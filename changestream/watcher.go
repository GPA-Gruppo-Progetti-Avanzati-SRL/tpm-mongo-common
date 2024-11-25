package changestream

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"sync"
	"time"
)

type watcherImpl struct {
	cfg       *Config
	chgStream *mongo.ChangeStream
	quitc     chan struct{}
	wg        *sync.WaitGroup
	listeners []Listener
}

var OutOfSequenceError = errors.New("resume token out of sequence")

func NewWatcher(cfg *Config, c chan error, wg *sync.WaitGroup, watcherOpts ...ConfigOption) (Watcher, error) {

	for _, o := range watcherOpts {
		o(cfg)
	}

	s := &watcherImpl{
		cfg:   cfg,
		quitc: make(chan struct{}),
		wg:    wg,
	}

	return s, nil
}

func (s *watcherImpl) Add(l Listener) error {
	const semLogContext = "watcher::add-listener"
	log.Info().Msg(semLogContext)
	s.listeners = append(s.listeners, l)
	return nil
}

func (s *watcherImpl) Close() {
	const semLogContext = "watcher::close"
	log.Info().Msg(semLogContext)
	close(s.quitc)
}

func (s *watcherImpl) Start() error {
	const semLogContext = "watcher::start"
	var err error

	log.Info().Msg(semLogContext)

	s.chgStream, err = s.newChangeStream()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	s.wg.Add(1)

	go s.workLoop()
	return nil
}

func (s *watcherImpl) newChangeStream() (*mongo.ChangeStream, error) {
	const semLogContext = "watcher::new-change-stream"

	opts, err := s.cfg.changeOptions()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	pipeline, err := s.cfg.Pipeline()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	lks, err := mongolks.GetLinkedService(context.Background(), s.cfg.MongoInstance)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	coll := lks.GetCollection(s.cfg.CollectionId, "")
	if coll == nil {
		err = errors.New("collection not found in config: " + s.cfg.CollectionId)
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	counter := 1
	log.Info().Err(err).Int("retry-num", counter).Msg(semLogContext)
	collStream, err := coll.Watch(context.TODO(), pipeline, &opts)
	for err != nil && counter < s.cfg.RetryCount {
		counter++
		log.Info().Err(err).Int("retry-num", counter).Msg(semLogContext)
		collStream, err = coll.Watch(context.TODO(), mongo.Pipeline{}, &opts)
	}

	if err != nil {
		log.Error().Err(err).Int("num-retries", counter).Msg(semLogContext)
		return nil, err
	}

	return collStream, err
}

func (s *watcherImpl) workLoop() {
	const semLogContext = "watcher::work-loop"

	defer s.wg.Done()

	var token checkpoint.ResumeToken
	var err error
	for {
		select {
		case <-s.quitc:
			log.Info().Msg(semLogContext + " terminating")
			err = s.chgStream.Close(context.Background())
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}
			return
		default:
			token, err = s.processChangeStream(token, 1000)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				if s.handleError(err) == OnErrorPolicyExit {
					log.Info().Msg(semLogContext + " exiting")
					return
				}
			}
		}
	}
}

func (s *watcherImpl) handleError(err error) string {
	const semLogContext = "watcher::on-error"

	if err == io.EOF {
		log.Info().Msg(semLogContext + " change stream closed")
	}

	err = s.chgStream.Close(context.Background())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	policy := s.cfg.onErrorPolicy()
	if policy == OnErrorPolicyContinue {
		s.chgStream, err = s.newChangeStream()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			policy = OnErrorPolicyExit
		}
	}

	return policy
}

func (s *watcherImpl) processChangeStream(token checkpoint.ResumeToken, batchSize int) (checkpoint.ResumeToken, error) {
	const semLogContext = "watcher::process-change-stream"
	numEvents := 0
	prevToken := token
	beginOf := time.Now()
	var g *promutil.Group
	for s.chgStream.TryNext(context.TODO()) {

		numEvents++
		g = s.setMetric(g, "cdc-events", 1, nil)

		var data bson.M
		if err := s.chgStream.Decode(&data); err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		// log.Warn().Interface("data", data).Msg(semLogContext)

		resumeToken, err := checkpoint.ParseResumeToken(s.chgStream.ResumeToken())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return prevToken, err
		}

		if s.cfg.VerifyOutOfSequenceError {
			if resumeToken.Value <= prevToken.Value {
				log.Error().Err(OutOfSequenceError).Str("current", resumeToken.Value).Str("prev", prevToken.Value).Msg(semLogContext + " - out-of-sequence token")
				g = s.setMetric(g, "cdc-event-errors", 1, nil)
				return prevToken, OutOfSequenceError
			}
		}

		evt, err := events.ParseEvent(data)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return prevToken, err
		}

		allSynchs := true
		for _, l := range s.listeners {
			var synchronous bool
			synchronous, err = l.Consume(resumeToken, evt)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return prevToken, err
			}

			allSynchs = allSynchs && synchronous
		}

		if allSynchs && s.cfg.checkPointSvc != nil {
			err = s.cfg.checkPointSvc.Store(s.cfg.Id, resumeToken)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return resumeToken, err
			}
		}

		prevToken = resumeToken
		if numEvents == batchSize {
			break
		}
	}

	if numEvents > 0 {
		log.Trace().Str("last-batch-token", prevToken.Value).Int("num-events", numEvents).Msg(semLogContext)
		g = s.setMetric(g, "cdc-event-duration", time.Since(beginOf).Seconds(), nil)
		g = s.setMetric(g, "cdc-events-batch-size", float64(numEvents), nil)
	}

	if s.chgStream.Err() != nil {
		log.Error().Err(s.chgStream.Err()).Msg(semLogContext)
		return prevToken, s.chgStream.Err()
	}

	if s.chgStream.ID() == 0 {
		return prevToken, io.EOF
	}

	return prevToken, nil
}

func (s *watcherImpl) processChangeStreamV1(token string, batchSize int) (string, error) {
	const semLogContext = "watcher::work-loop"

	// log.Info().Str("token", token).Msg(semLogContext + " - starting")
	var beginOf time.Time

	numEvents := 0
	prevToken := token
	var g *promutil.Group
	for s.chgStream.TryNext(context.TODO()) {

		numEvents++
		if numEvents == 1 {
			beginOf = time.Now()
		}

		g = s.setMetric(g, "cdc-events", 1, nil)

		var data bson.M
		if err := s.chgStream.Decode(&data); err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		resumeToken, err := checkpoint.ParseResumeToken(s.chgStream.ResumeToken())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		b, err := json.Marshal(data)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		log.Trace().Str("current-token", resumeToken.Value).Str("data", string(b)).Msg(semLogContext)
		if resumeToken.Value <= prevToken {
			log.Error().Str("current", resumeToken.Value).Str("prev", prevToken).Msg(semLogContext + " - out-of-sequence token")
			g = s.setMetric(g, "cdc-event-errors", 1, nil)
		}

		prevToken = resumeToken.Value
		if numEvents == batchSize {
			break
		}
	}

	if numEvents > 0 {
		log.Info().Str("last-batch-token", prevToken).Int("num-events", numEvents).Msg(semLogContext)
		g = s.setMetric(g, "cdc-event-duration", time.Since(beginOf).Seconds(), nil)
		g = s.setMetric(g, "cdc-events-batch-size", float64(numEvents), nil)
	}

	if s.chgStream.Err() != nil {
		log.Error().Err(s.chgStream.Err()).Msg(semLogContext)
		return prevToken, s.chgStream.Err()
	}

	if s.chgStream.ID() == 0 {
		return prevToken, io.EOF
	}

	return prevToken, nil
}

/*func (s *watcherImpl) ParseResumeToken(resumeToken bson.Raw) (string, error) {

	var data bson.M
	err := bson.Unmarshal(resumeToken, &data)
	if err != nil {
		panic(err)
	}
	dataId := data["_data"]


	if s, ok := dataId.(string); ok {
		return s, nil // s[:ResumeTokenTimestampSubStringLength], nil
	}

	return "", fmt.Errorf("invalid resumeToken of type %T", dataId)
}*/

func (s *watcherImpl) setMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "watcher::set-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(s.cfg.RefMetrics.GId)
		if err != nil {
			log.Warn().Err(err).Msg(semLogContext)
			return nil
		}

		metricGroup = &g
	}

	err = metricGroup.SetMetricValueById(metricId, value, labels)
	if err != nil {
		log.Warn().Err(err).Msg(semLogContext)
	}

	return metricGroup
}
