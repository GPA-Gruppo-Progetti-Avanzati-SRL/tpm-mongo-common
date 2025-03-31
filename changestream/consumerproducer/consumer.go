package consumerproducer

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
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
	MetricCdcEventErrors                       = "cdc-event-errors"
)

type ConsumerStatsInfo struct {
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

func (stat *ConsumerStatsInfo) Clear() *ConsumerStatsInfo {
	stat.HistoryLost = 0
	stat.IdlesTryNext = 0
	stat.MillisecondsBehindSource = 0
	stat.NumEvents = 0
	return stat
}

func (stat *ConsumerStatsInfo) IncNumEvents() {
	stat.NumEvents++
	if !stat.metricErrors {
		stat.NumEventsCounterMetric.SetMetric(1)
	}
}

func (stat *ConsumerStatsInfo) IncHistoryLost() {
	stat.HistoryLost++
	if !stat.metricErrors {
		stat.HistoryLostCounterMetric.SetMetric(1)
	}
}

func (stat *ConsumerStatsInfo) IncIdlesTryNext() {
	stat.IdlesTryNext++
	if !stat.metricErrors {
		stat.IdlesTryNextGaugeMetric.SetMetric(float64(stat.IdlesTryNext))
	}
}

func (stat *ConsumerStatsInfo) ResetIdlesTryNext() {
	stat.IdlesTryNext = 0
	if !stat.metricErrors {
		stat.IdlesTryNextGaugeMetric.SetMetric(0)
	}
}

func (stat *ConsumerStatsInfo) IncCdcEventErrors() {
	stat.CdcEventErrors++
	if !stat.metricErrors {
		stat.CdcEventErrorsCounterMetric.SetMetric(1)
	}
}

func (stat *ConsumerStatsInfo) SetMillisecondsBehindSource(ms int64) {
	stat.MillisecondsBehindSource = ms
	if !stat.metricErrors {
		stat.MillisecondsBehindSourceGaugeMetric.SetMetric(float64(ms))
	}
}

func (stat *ConsumerStatsInfo) ResetMillisecondsBehindSource() {
	stat.MillisecondsBehindSource = 0
	if !stat.metricErrors {
		stat.MillisecondsBehindSourceGaugeMetric.SetMetric(0)
	}
}

func NewConsumerStatsInfo(whatcherId, metricGroupId string) *ConsumerStatsInfo {
	stat := &ConsumerStatsInfo{}
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

type Consumer struct {
	cfg           *ConsumerConfig
	ServerVersion util.MongoDbVersion
	chgStream     *mongo.ChangeStream

	lastCommittedToken checkpoint.ResumeToken
	lastPolledToken    checkpoint.ResumeToken

	statsInfo *ConsumerStatsInfo
}

func NewConsumer(cfg *ConsumerConfig, watcherOpts ...ConsumerConfigOption) (*Consumer, error) {
	const semLogContext = "consumer::new"
	var err error

	for _, o := range watcherOpts {
		o(cfg)
	}

	s := &Consumer{
		cfg:       cfg,
		statsInfo: NewConsumerStatsInfo(cfg.Id, cfg.RefMetrics.GId),
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
	s.statsInfo.Clear()
}

func (s *Consumer) CommitAt(rt checkpoint.ResumeToken, syncRequired bool) error {
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

func (s *Consumer) Commit() error {
	const semLogContext = "consumer::commit"

	if s.cfg.CheckPointSvc == nil {
		err := errors.New("no checkpoint service configured to honour commit op")
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	if s.lastPolledToken.IsZero() {
		log.Warn().Msg(semLogContext + " - transaction is empty")
		return nil
	}

	err := s.cfg.CheckPointSvc.CommitAt(s.cfg.Id, s.lastPolledToken, false)
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
	log.Trace().Msg(semLogContext)
	if !s.chgStream.TryNext(context.TODO()) {
		s.statsInfo.IncIdlesTryNext()
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

		if s.cfg.CheckPointSvc != nil {
			idleResumeToken, err := checkpoint.DecodeResumeToken(s.chgStream.ResumeToken())
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}

			err = s.cfg.CheckPointSvc.StoreIdle(s.cfg.Id, idleResumeToken)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}
		}

		return nil, nil
	}

	if s.cfg.CheckPointSvc != nil {
		s.cfg.CheckPointSvc.ClearIdle()
	}

	s.statsInfo.ResetIdlesTryNext()

	/*
		    Errore fittizio generato per motivi di test
			fictitiousErr := errors.New("fictitious error")
			log.Error().Err(fictitiousErr).Msg(semLogContext)
			if fictitiousErr != nil {
				return nil, fictitiousErr
			}
	*/

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

	/*
		resumeToken, err := checkpoint.DecodeResumeToken(s.chgStream.ResumeToken())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	*/

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

	s.lastPolledToken = evt.ResumeTok
	return &evt, nil
}

func (s *Consumer) newChangeStream() (*mongo.ChangeStream, error) {
	const semLogContext = "consumer::new-change-stream"

	opts, err := s.cfg.ChangeOptions()
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

/*
func (s *Consumer) setMetricBad(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
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
*/
