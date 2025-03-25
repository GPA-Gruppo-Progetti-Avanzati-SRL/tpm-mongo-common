package changestream

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"time"
)

const (
	MetricLabelName                            = "name"
	MetricChangeStreamHistoryLostCounter       = "cdc-cs-history-lost"
	MetricChangeStreamIdleTryNext              = "cdc-cs-idle-try-next"
	MetricChangeStreamMillisecondsBehindSource = "cdc-cs-milliseconds-behind-source"
	MetricChangeStreamNumEvents                = "cdc-cs-events"
)

type Consumer struct {
	cfg           *Config
	ServerVersion util.MongoDbVersion
	chgStream     *mongo.ChangeStream

	lastCommittedToken checkpoint.ResumeToken
	lastPolledToken    checkpoint.ResumeToken
	numEvents          int
	numIdlesTryNext    int

	metricsLabels prometheus.Labels
}

func NewConsumer(cfg *Config, watcherOpts ...ConfigOption) (*Consumer, error) {
	const semLogContext = "consumer::new"
	var err error

	for _, o := range watcherOpts {
		o(cfg)
	}

	s := &Consumer{
		cfg: cfg,
		metricsLabels: prometheus.Labels{
			MetricLabelName: cfg.Id,
		},
	}

	s.chgStream, err = s.newChangeStream()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return s, nil
}

func (s *Consumer) Close() {
	const semLogContext = "consumer::close"
	log.Info().Msg(semLogContext)
	err := s.chgStream.Close(context.Background())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}
}

func (s *Consumer) SynchPoint(rt checkpoint.ResumeToken) error {
	const semLogContext = "consumer::synch-point"

	if s.cfg.checkPointSvc == nil {
		err := errors.New("no checkpoint service configured to honour commit op")
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	err := s.cfg.checkPointSvc.Synch(s.cfg.Id, rt)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

func (s *Consumer) Commit() error {
	const semLogContext = "consumer::commit"

	if s.cfg.checkPointSvc == nil {
		err := errors.New("no checkpoint service configured to honour commit op")
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	if s.lastPolledToken.IsZero() {
		log.Warn().Msg(semLogContext + " - transaction is empty")
		return nil
	}

	err := s.cfg.checkPointSvc.Store(s.cfg.Id, s.lastPolledToken)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

func (s *Consumer) Abort() error {
	const semLogContext = "consumer::abort"
	return nil
}

func (s *Consumer) Poll() (*events.ChangeEvent, error) {
	const semLogContext = "consumer::process-change-stream"

	var g *promutil.Group
	if !s.chgStream.TryNext(context.TODO()) {

		s.numIdlesTryNext++
		g = s.setMetric(g, MetricChangeStreamIdleTryNext, float64(s.numIdlesTryNext), nil)

		// with no events assume there is no lag..... reset this gauge
		g = s.setMetric(g, MetricChangeStreamMillisecondsBehindSource, 0, nil)

		if s.chgStream.ID() == 0 {
			log.Warn().Msg(semLogContext + " - stream EOF")
			return nil, io.EOF
		}

		if s.chgStream.Err() != nil {
			ec, en := util.MongoError(s.chgStream.Err(), s.ServerVersion)
			if ec == util.MongoErrChangeStreamHistoryLost {
				var g *promutil.Group
				g = s.setMetric(g, MetricChangeStreamHistoryLostCounter, 1, nil)

				if s.cfg.checkPointSvc != nil {
					errHl := s.cfg.checkPointSvc.OnHistoryLost(s.cfg.Id)
					if errHl != nil {
						log.Error().Err(errHl).Msg(semLogContext + " - history lost")
					}
				}
			} else {
				log.Error().Err(s.chgStream.Err()).Int32("error-code", ec).Interface("error", en).Msg(semLogContext)
			}

			return nil, s.chgStream.Err()
		}

		if s.cfg.checkPointSvc != nil {
			idleResumeToken, err := checkpoint.DecodeResumeToken(s.chgStream.ResumeToken())
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}

			err = s.cfg.checkPointSvc.StoreIdle(s.cfg.Id, idleResumeToken)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}
		}

		return nil, nil
	}

	if s.cfg.checkPointSvc != nil {
		s.cfg.checkPointSvc.ClearIdle()
	}

	// clear the gauge
	s.numIdlesTryNext = 0
	g = s.setMetric(g, MetricChangeStreamIdleTryNext, 0, nil)

	/*
		    Errore fittizio generato per motivi di test
			fictitiousErr := errors.New("fictitious error")
			log.Error().Err(fictitiousErr).Msg(semLogContext)
			if fictitiousErr != nil {
				return nil, fictitiousErr
			}
	*/

	s.numEvents++
	g = s.setMetric(g, MetricChangeStreamNumEvents, 1, nil)

	var data bson.M
	if err := s.chgStream.Decode(&data); err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	evt, err := events.ParseEvent(data)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return &evt, err
	}

	/*
		resumeToken, err := checkpoint.DecodeResumeToken(s.chgStream.ResumeToken())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	*/

	if s.cfg.VerifyOutOfSequenceError {
		if evt.ResumeTok.Value <= s.lastPolledToken.Value {
			log.Error().Err(OutOfSequenceError).Str("current", evt.ResumeTok.Value).Str("prev", s.lastPolledToken.Value).Msg(semLogContext + " - out-of-sequence token")
			g = s.setMetric(g, "cdc-event-errors", 1, nil)
			return nil, OutOfSequenceError
		}
	}

	clusterTime := time.Unix(int64(evt.ClusterTime.T), 0)
	lag := time.Now().Sub(clusterTime)
	g = s.setMetric(g, MetricChangeStreamMillisecondsBehindSource, float64(lag.Milliseconds()), nil)

	s.lastPolledToken = evt.ResumeTok
	return &evt, nil
}

func (s *Consumer) newChangeStream() (*mongo.ChangeStream, error) {
	const semLogContext = "consumer::new-change-stream"

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

	s.ServerVersion, err = lks.ServerVersion()
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
		mongoCode, _ := util.MongoError(err, s.ServerVersion)
		// TODO add logic to retry with the start after time depending on config
		if mongoCode == util.MongoErrChangeStreamHistoryLost {
			var g *promutil.Group
			g = s.setMetric(g, MetricChangeStreamHistoryLostCounter, 1, nil)

			if s.cfg.checkPointSvc != nil {
				errHl := s.cfg.checkPointSvc.OnHistoryLost(s.cfg.Id)
				if errHl != nil {
					log.Error().Err(errHl).Msg(semLogContext + " - history lost")
				}
			}
			log.Error().Err(err).Msg(semLogContext + " - history lost")
			return nil, err
		}

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

func (s *Consumer) handleError(err error) string {
	const semLogContext = "consumer::on-error"

	if err == io.EOF {
		log.Info().Msg(semLogContext + " change stream closed")
	}

	err = s.chgStream.Close(context.Background())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	policy := s.cfg.onConsumerErrorPolicy()
	if policy == OnErrorPolicyContinue {
		s.chgStream, err = s.newChangeStream()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			policy = OnErrorPolicyExit
		}
	}

	return policy
}

func (s *Consumer) setMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "watcher::set-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(s.cfg.RefMetrics.GId)
		if err != nil {
			log.Trace().Err(err).Msg(semLogContext)
			return nil
		}

		metricGroup = &g
	}

	if labels == nil {
		labels = s.metricsLabels
	}

	err = metricGroup.SetMetricValueById(metricId, value, labels)
	if err != nil {
		log.Warn().Err(err).Msg(semLogContext)
	}

	return metricGroup
}
