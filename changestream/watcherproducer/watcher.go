package watcherproducer

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	MetricLabelName                            = "name"
	MetricChangeStreamHistoryLostCounter       = "cdc-cs-history-lost"
	MetricChangeStreamIdleTryNext              = "cdc-cs-idle-try-next"
	MetricChangeStreamMillisecondsBehindSource = "cdc-cs-milliseconds-behind-source"
	MetricChangeStreamNumEvents                = "cdc-cs-events"
	MetricCdcEventErrors                       = "cdc-event-errors"
)

type WatcherStatsInfo struct {
	HistoryLost              int
	IdlesTryNext             int
	MillisecondsBehindSource int64
	NumEvents                int
	CdcEventErrors           int

	HistoryLostCounterMetric            promutil.CollectorWithLabels
	IdlesTryNextGaugeMetric             promutil.CollectorWithLabels
	MillisecondsBehindSourceGaugeMetric promutil.CollectorWithLabels
	NumEventsCounterMetric              promutil.CollectorWithLabels
	CdcEventErrorsCounterMetric         promutil.CollectorWithLabels
	metricErrors                        bool
}

func (stat *WatcherStatsInfo) Clear() *WatcherStatsInfo {
	stat.HistoryLost = 0
	stat.IdlesTryNext = 0
	stat.MillisecondsBehindSource = 0
	stat.NumEvents = 0
	return stat
}

func (stat *WatcherStatsInfo) IncNumEvents() {
	stat.NumEvents++
	if !stat.metricErrors {
		stat.NumEventsCounterMetric.SetMetric(1)
	}
}

func (stat *WatcherStatsInfo) IncHistoryLost() {
	stat.HistoryLost++
	if !stat.metricErrors {
		stat.HistoryLostCounterMetric.SetMetric(1)
	}
}

func (stat *WatcherStatsInfo) IncIdlesTryNext() {
	stat.IdlesTryNext++
	if !stat.metricErrors {
		stat.IdlesTryNextGaugeMetric.SetMetric(float64(stat.IdlesTryNext))
	}
}

func (stat *WatcherStatsInfo) ResetIdlesTryNext() {
	stat.IdlesTryNext = 0
	if !stat.metricErrors {
		stat.IdlesTryNextGaugeMetric.SetMetric(0)
	}
}

func (stat *WatcherStatsInfo) IncCdcEventErrors() {
	stat.CdcEventErrors++
	if !stat.metricErrors {
		stat.CdcEventErrorsCounterMetric.SetMetric(1)
	}
}

func (stat *WatcherStatsInfo) SetMillisecondsBehindSource(ms int64) {
	stat.MillisecondsBehindSource = ms
	if !stat.metricErrors {
		stat.MillisecondsBehindSourceGaugeMetric.SetMetric(float64(ms))
	}
}

func (stat *WatcherStatsInfo) ResetMillisecondsBehindSource() {
	stat.MillisecondsBehindSource = 0
	if !stat.metricErrors {
		stat.MillisecondsBehindSourceGaugeMetric.SetMetric(0)
	}
}

func NewWatcherStatsInfo(whatcherId, metricGroupId string) *WatcherStatsInfo {
	stat := &WatcherStatsInfo{}
	mg, err := promutil.GetGroup(metricGroupId)
	if err != nil {
		stat.metricErrors = true
		return stat
	} else {
		stat.HistoryLostCounterMetric, err = mg.CollectorByIdWithLabels(MetricChangeStreamHistoryLostCounter, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.IdlesTryNextGaugeMetric, err = mg.CollectorByIdWithLabels(MetricChangeStreamIdleTryNext, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MillisecondsBehindSourceGaugeMetric, err = mg.CollectorByIdWithLabels(MetricChangeStreamMillisecondsBehindSource, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.NumEventsCounterMetric, err = mg.CollectorByIdWithLabels(MetricChangeStreamNumEvents, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}
	}

	return stat
}

type watcherImpl struct {
	cfg           *WatcherConfig
	ServerVersion util.MongoDbVersion
	chgStream     *mongo.ChangeStream

	lastCommittedToken checkpoint.ResumeToken
	lastPolledToken    checkpoint.ResumeToken

	quitc     chan struct{}
	wg        *sync.WaitGroup
	listeners []WatcherListener

	statsInfo *WatcherStatsInfo
}

func NewWatcher(cfg *WatcherConfig, c chan error, wg *sync.WaitGroup, watcherOpts ...WatcherConfigOption) (Watcher, error) {
	const semLogContext = "watcher::new"
	var err error

	for _, o := range watcherOpts {
		o(cfg)
	}

	s := &watcherImpl{
		cfg:       cfg,
		quitc:     make(chan struct{}),
		statsInfo: NewWatcherStatsInfo(cfg.Id, cfg.RefMetrics.GId),
		wg:        wg,
	}

	s.chgStream, err = s.newChangeStream()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return s, nil
}

func (s *watcherImpl) Add(l WatcherListener) error {
	const semLogContext = "watcher::add-listener"
	log.Trace().Msg(semLogContext)
	s.listeners = append(s.listeners, l)
	return nil
}

func (s *watcherImpl) Close() {
	const semLogContext = "watcher::close"
	log.Info().Msg(semLogContext)
	err := s.chgStream.Close(context.Background())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}
	close(s.quitc)
}

func (s *watcherImpl) CommitAt(rt checkpoint.ResumeToken, syncRequired bool) error {
	const semLogContext = "consumer::commit-at"

	if s.cfg.CheckPointSvc == nil {
		err := errors.New("no checkpoint service configured to honour commit op")
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	err := s.cfg.CheckPointSvc.CommitAt(s.cfg.Id, rt, syncRequired)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
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

	opts, err := s.cfg.ChangeStreamOptions()
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
	collStream, err := coll.Watch(context.TODO(), pipeline, opts)
	for err != nil && counter < s.cfg.RetryCount {
		mongoCode := util.MongoErrorCode(err, s.ServerVersion)
		// TODO add logic to retry with the start after time depending on config
		if mongoCode == util.MongoErrChangeStreamHistoryLost {
			s.statsInfo.IncHistoryLost()

			if s.cfg.CheckPointSvc != nil {
				errHl := s.cfg.CheckPointSvc.OnHistoryLost(s.cfg.Id)
				if errHl != nil {
					log.Error().Err(errHl).Msg(semLogContext + " - history lost")
				}
			}
			log.Error().Err(err).Msg(semLogContext + " - history lost")
			return nil, err
		}

		counter++
		log.Info().Err(err).Int("retry-num", counter).Msg(semLogContext)
		collStream, err = coll.Watch(context.TODO(), mongo.Pipeline{}, opts)
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

	policy := s.cfg.OnWatcherErrorPolicy()
	if policy == OnErrorPolicyContinue {
		s.chgStream, err = s.newChangeStream()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			policy = OnErrorPolicyExit
		}
	}

	return policy
}

func (s *watcherImpl) nextBatchOfEvents(batchSize int) ([]*events.ChangeEvent, error) {
	const semLogContext = "watcher::next-batch-of-events"
	batchOfEvents := make([]*events.ChangeEvent, 0, batchSize)
	numEvents := 0
	beginOf := time.Now()

	for numEvents < batchSize {
		evt, err := s.nextEvent()
		if err != nil {
			return nil, err
		}

		batchOfEvents = append(batchOfEvents, evt)
		numEvents++
	}

	log.Info().Int64("elapsed", time.Since(beginOf).Milliseconds()).Msg(semLogContext)
	return batchOfEvents, nil
}

func (s *watcherImpl) nextEvent() (*events.ChangeEvent, error) {
	const semLogContext = "watcher::next"
	log.Trace().Msg(semLogContext)

	ok := s.chgStream.Next(context.Background())
	if !ok {
		s.statsInfo.ResetMillisecondsBehindSource()

		if s.chgStream.ID() == 0 {
			log.Warn().Msg(semLogContext + " - stream EOF")
			return nil, io.EOF
		}

		if s.chgStream.Err() != nil {
			ec := util.MongoErrorCode(s.chgStream.Err(), s.ServerVersion)
			if ec == util.MongoErrChangeStreamHistoryLost {
				s.statsInfo.IncHistoryLost()
				if s.cfg.CheckPointSvc != nil {
					errHl := s.cfg.CheckPointSvc.OnHistoryLost(s.cfg.Id)
					if errHl != nil {
						log.Error().Err(errHl).Msg(semLogContext + " - history lost")
					}
				}
			} else {
				log.Error().Err(s.chgStream.Err()).Int32("error-code", ec).Interface("error", s.chgStream.Err()).Msg(semLogContext)
			}

			return nil, s.chgStream.Err()
		}

		return nil, errors.New("watcher undefined condition")
	}

	s.statsInfo.IncNumEvents()

	var data bson.M
	if err := s.chgStream.Decode(&data); err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	evt, err := events.ParseEvent(data)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return &evt, err
	}

	if s.cfg.VerifyOutOfSequenceError {
		if evt.ResumeTok.Value <= s.lastPolledToken.Value {
			log.Error().Err(events.OutOfSequenceError).Str("current", evt.ResumeTok.Value).Str("prev", s.lastPolledToken.Value).Msg(semLogContext + " - out-of-sequence token")
			s.statsInfo.IncCdcEventErrors()
			return nil, events.OutOfSequenceError
		}
	}

	clusterTime := time.Unix(int64(evt.ClusterTime.T), 0)
	lag := time.Now().Sub(clusterTime)
	s.statsInfo.SetMillisecondsBehindSource(lag.Milliseconds())

	allSynchs := true
	for _, l := range s.listeners {
		var synchronous bool
		synchronous, err = l.ConsumeEvent(evt)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}

		allSynchs = allSynchs && synchronous
	}

	if allSynchs && s.cfg.CheckPointSvc != nil {
		err = s.cfg.CheckPointSvc.CommitAt(s.cfg.Id, evt.ResumeTok, false)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	}

	s.lastPolledToken = evt.ResumeTok
	return &evt, nil
}

func (s *watcherImpl) processChangeStream(token checkpoint.ResumeToken, batchSize int) (checkpoint.ResumeToken, error) {
	const semLogContext = "watcher::process-change-stream"
	numEvents := 0
	prevToken := token
	beginOf := time.Now()
	var g *promutil.Group
	for s.chgStream.TryNext(context.TODO()) {

		numEvents++
		g = s.setMetric(g, MetricChangeStreamNumEvents, 1, nil)

		var data bson.M
		if err := s.chgStream.Decode(&data); err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		// log.Warn().Interface("data", data).Msg(semLogContext)
		evt, err := events.ParseEvent(data)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return prevToken, err
		}

		//resumeToken, err := checkpoint.DecodeResumeToken(s.chgStream.ResumeToken())
		//if err != nil {
		//	log.Error().Err(err).Msg(semLogContext)
		//	return prevToken, err
		//}

		if s.cfg.VerifyOutOfSequenceError {
			if evt.ResumeTok.Value <= prevToken.Value {
				log.Error().Err(events.OutOfSequenceError).Str("current", evt.ResumeTok.Value).Str("prev", prevToken.Value).Msg(semLogContext + " - out-of-sequence token")
				g = s.setMetric(g, "cdc-event-errors", 1, nil)
				return prevToken, events.OutOfSequenceError
			}
		}

		allSynchs := true
		for _, l := range s.listeners {
			var synchronous bool
			synchronous, err = l.ConsumeEvent(evt)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return prevToken, err
			}

			allSynchs = allSynchs && synchronous
		}

		if allSynchs && s.cfg.CheckPointSvc != nil {
			err = s.cfg.CheckPointSvc.CommitAt(s.cfg.Id, evt.ResumeTok, false)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return evt.ResumeTok, err
			}
		}

		prevToken = evt.ResumeTok
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

//func (s *watcherImpl) processChangeStreamV1(token string, batchSize int) (string, error) {
//	const semLogContext = "watcher::work-loop"
//
//	// log.Info().Str("token", token).Msg(semLogContext + " - starting")
//	var beginOf time.Time
//
//	numEvents := 0
//	prevToken := token
//	var g *promutil.Group
//	for s.chgStream.TryNext(context.TODO()) {
//
//		numEvents++
//		if numEvents == 1 {
//			beginOf = time.Now()
//		}
//
//		g = s.setMetric(g, "cdc-events", 1, nil)
//
//		var data bson.M
//		if err := s.chgStream.Decode(&data); err != nil {
//			log.Error().Err(err).Msg(semLogContext)
//		}
//
//		resumeToken, err := checkpoint.DecodeResumeToken(s.chgStream.ResumeToken())
//		if err != nil {
//			log.Error().Err(err).Msg(semLogContext)
//		}
//
//		b, err := json.Marshal(data)
//		if err != nil {
//			log.Error().Err(err).Msg(semLogContext)
//		}
//
//		log.Trace().Str("current-token", resumeToken.Value).Str("data", string(b)).Msg(semLogContext)
//		if resumeToken.Value <= prevToken {
//			log.Error().Str("current", resumeToken.Value).Str("prev", prevToken).Msg(semLogContext + " - out-of-sequence token")
//			g = s.setMetric(g, "cdc-event-errors", 1, nil)
//		}
//
//		prevToken = resumeToken.Value
//		if numEvents == batchSize {
//			break
//		}
//	}
//
//	if numEvents > 0 {
//		log.Info().Str("last-batch-token", prevToken).Int("num-events", numEvents).Msg(semLogContext)
//		g = s.setMetric(g, "cdc-event-duration", time.Since(beginOf).Seconds(), nil)
//		g = s.setMetric(g, "cdc-events-batch-size", float64(numEvents), nil)
//	}
//
//	if s.chgStream.Err() != nil {
//		log.Error().Err(s.chgStream.Err()).Msg(semLogContext)
//		return prevToken, s.chgStream.Err()
//	}
//
//	if s.chgStream.ID() == 0 {
//		return prevToken, io.EOF
//	}
//
//	return prevToken, nil
//}

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
			log.Trace().Err(err).Msg(semLogContext)
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
