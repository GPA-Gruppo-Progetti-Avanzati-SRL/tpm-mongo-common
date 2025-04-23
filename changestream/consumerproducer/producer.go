package consumerproducer

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/factory"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

const (
	MetricRewindsCounter  = "cdc-rewinds"
	MetricBatchErrors     = "cdc-batch-errors"
	MetricBatches         = "cdc-batches"
	MetricBatchSize       = "cdc-batch-size"
	MetricBatchDuration   = "cdc-batch-duration"
	MetricMessageErrors   = "cdc-event-errors"
	MetricMessages        = "cdc-events"
	MetricMessageDuration = "cdc-event-duration"
)

type StatsInfo struct {
	Rewinds         int
	BatchErrors     int
	Batches         int
	BatchSize       int
	BatchDuration   float64
	MessageErrors   int
	Messages        int
	MessageDuration float64

	RewindsCounterMetric       promutil.CollectorWithLabels
	BatchesCounterMetric       promutil.CollectorWithLabels
	BatchErrorsCounterMetric   promutil.CollectorWithLabels
	BatchSizeGaugeMetric       promutil.CollectorWithLabels
	BatchDurationHistogram     promutil.CollectorWithLabels
	MessageErrorsCounterMetric promutil.CollectorWithLabels
	MessagesCounterMetric      promutil.CollectorWithLabels
	MessageDurationHistogram   promutil.CollectorWithLabels
	metricErrors               bool
}

func (stat *StatsInfo) Clear() *StatsInfo {
	stat.Rewinds = 0
	stat.BatchErrors = 0
	stat.Batches = 0
	stat.BatchSize = 0
	stat.BatchDuration = 0
	stat.MessageErrors = 0
	stat.Messages = 0
	stat.MessageDuration = 0
	return stat
}

func (stat *StatsInfo) IncRewinds() {
	stat.Rewinds++
	if !stat.metricErrors {
		stat.RewindsCounterMetric.SetMetric(1)
	}
}

func (stat *StatsInfo) IncBatchErrors() {
	stat.BatchErrors++
	if !stat.metricErrors {
		stat.BatchErrorsCounterMetric.SetMetric(1)
	}
}

func (stat *StatsInfo) IncBatches() {
	stat.Batches++
	if !stat.metricErrors {
		stat.BatchesCounterMetric.SetMetric(1)
	}
}

func (stat *StatsInfo) IncMessageErrors() {
	stat.MessageErrors++
	if !stat.metricErrors {
		stat.MessageErrorsCounterMetric.SetMetric(1)
	}
}

func (stat *StatsInfo) IncMessages() {
	stat.Messages++
	if !stat.metricErrors {
		stat.MessagesCounterMetric.SetMetric(1)
	}
}

func (stat *StatsInfo) SetBatchSize(sz int) {
	stat.BatchSize = sz
	if !stat.metricErrors {
		stat.BatchSizeGaugeMetric.SetMetric(float64(sz))
	}
}

func (stat *StatsInfo) SetBatchDuration(dur float64) {
	stat.BatchDuration = dur
	if !stat.metricErrors {
		stat.BatchDurationHistogram.SetMetric(dur)
	}
}

func (stat *StatsInfo) SetMessageDuration(dur float64) {
	stat.MessageDuration = dur
	if !stat.metricErrors {
		stat.MessageDurationHistogram.SetMetric(dur)
	}
}

func NewProducerStatsInfo(whatcherId, metricGroupId string) *StatsInfo {
	stat := &StatsInfo{}
	mg, err := promutil.GetGroup(metricGroupId)
	if err != nil {
		stat.metricErrors = true
		return stat
	} else {
		stat.RewindsCounterMetric, err = mg.CollectorByIdWithLabels(MetricRewindsCounter, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchErrorsCounterMetric, err = mg.CollectorByIdWithLabels(MetricBatchErrors, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchesCounterMetric, err = mg.CollectorByIdWithLabels(MetricBatches, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchSizeGaugeMetric, err = mg.CollectorByIdWithLabels(MetricBatchSize, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchDurationHistogram, err = mg.CollectorByIdWithLabels(MetricBatchDuration, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MessageErrorsCounterMetric, err = mg.CollectorByIdWithLabels(MetricMessageErrors, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MessagesCounterMetric, err = mg.CollectorByIdWithLabels(MetricMessages, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MessageDurationHistogram, err = mg.CollectorByIdWithLabels(MetricMessageDuration, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

	}

	return stat
}

type producerImpl struct {
	cfg *ProducerConfig

	wg           *sync.WaitGroup
	shutdownSync sync.Once
	quitc        chan struct{}

	parent                  Server
	numberOfMessages        int
	processor               Processor
	batchProcessedCbChannel chan BatchProcessedCbEvent
	checkpointSvc           checkpoint.ResumeTokenCheckpointSvc
	consumer                *Consumer
	statsInfo               *StatsInfo

	batchOfChangeEvents BatchOfChangeStreamEvents
}

func NewConsumerProducer(cfg *ProducerConfig, wg *sync.WaitGroup, processor Processor) (ConsumerProducer, error) {
	const semLogContext = "change-stream-cs-factory::new"

	if cfg.WorkMode != WorkModeBatch && cfg.WorkMode != WorkModeBatchFF {
		cfg.WorkMode = WorkModeMsg
	}

	t := producerImpl{
		cfg:       cfg,
		quitc:     make(chan struct{}),
		wg:        wg,
		statsInfo: NewProducerStatsInfo(cfg.Name, cfg.RefMetrics.GId),
	}

	if processor.IsProcessorDeferred() {
		processor.WithBatchProcessedErrorCallback(&t)
		processor.WithBatchProcessedCommitAtCallback(&t)
		if cfg.BatchProcessedCbChannelSize <= 0 {
			cfg.BatchProcessedCbChannelSize = 1
		}
		t.batchProcessedCbChannel = make(chan BatchProcessedCbEvent, cfg.BatchProcessedCbChannelSize)
	}
	t.processor = processor

	if cfg.WorkMode == WorkModeBatch || cfg.WorkMode == WorkModeBatchFF {
		if cfg.MaxBatchSize > 0 {
			cfg.TickInterval = 0
			log.Info().Int("max-batch-size", cfg.MaxBatchSize).Msg(semLogContext + " - working in batch-size mode")
		} else {
			if cfg.TickInterval == 0 {
				err := errors.New("max-batch-size or tick-interval have to be set")
				return nil, err
			}
			log.Info().Int64("tick-interval-ms", cfg.TickInterval.Milliseconds()).Msg(semLogContext + " - working in tick-interval mode")
		}
	}
	return &t, nil
}

func (tp *producerImpl) Name() string {
	return tp.cfg.Name
}

func (tp *producerImpl) SetParent(s Server) {
	tp.parent = s
}

func (tp *producerImpl) Start() error {
	const semLogContext = "change-stream-cp::start"
	var err error

	log.Info().Msg(semLogContext)

	/*
		var opts []changestream.ConfigOption
		if tp.cfg.CheckPointSvcConfig.Typ != "" {
			svc, err := factory.NewCheckPointSvc(tp.cfg.CheckPointSvcConfig)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}
			opts = append(opts, changestream.WithCheckpointSvc(svc))
		}

		tp.consumer, err = changestream.NewConsumer(&tp.cfg.Consumer, opts...)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}
	*/

	tp.consumer, err = tp.newConsumer()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	// Add to wait group
	if tp.wg != nil {
		tp.wg.Add(1)
	}

	tp.processor.StartProcessor()

	if tp.cfg.MaxBatchSize > 0 {
		go tp.maxBatchSizePollLoop()
	} else {
		go tp.tickIntervalPollLoop()
	}

	return nil
}

func (tp *producerImpl) newConsumer() (*Consumer, error) {
	const semLogContext = "change-stream-cp::new-consumer"

	var opts []ConsumerConfigOption
	if tp.cfg.CheckPointSvcConfig.Typ != "" {
		svc, err := factory.NewCheckPointSvc(tp.cfg.CheckPointSvcConfig)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
		opts = append(opts, WithCheckpointSvc(svc))

		if tp.processor != nil && tp.processor.IsProcessorDeferred() {
			tp.checkpointSvc = svc
		}
	}

	consumer, err := NewConsumer(&tp.cfg.Consumer, opts...)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return consumer, nil
}

func (tp *producerImpl) Close() error {
	const semLogContext = "change-stream-cp::close"
	log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " signalling shutdown transformer producer")
	close(tp.quitc)
	tp.processor.CloseProcessor()
	return nil
}

func (tp *producerImpl) onError(errIn error) error {
	const semLogContext = "change-stream-cp::on-error"
	log.Warn().Err(errIn).Msg(semLogContext)

	changeStreamFatal := util.IsMongoErrorHistoryLost(errIn, tp.consumer.ServerVersion)
	if changeStreamFatal || !tp.cfg.RewindEnabled() {
		log.Info().Bool("rewind-enabled", tp.cfg.RewindEnabled()).Bool("change-stream-fatal", changeStreamFatal).Msg(semLogContext + " non resumable error")
		return errIn
	}

	// Increment number of rewinds
	tp.statsInfo.IncRewinds()

	tp.consumer.Close()

	err := tp.processor.ResetProcessor()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	// Trying a restart. If not the policy is to exit
	tp.consumer, err = tp.newConsumer()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return err
}

func (tp *producerImpl) maxBatchSizePollLoop() {
	const semLogContext = "change-stream-cp::poll-loop"
	log.Info().Str("cs-prod-id", tp.cfg.Name).Int("batch-size", tp.cfg.MaxBatchSize).Msg(semLogContext + " starting polling loop")

	if tp.consumer == nil {
		tp.shutDown(errors.New("consumer not initialized"))
		return
	}

	var cbEvt BatchProcessedCbEvent
	var ok bool
	for {
		select {
		case <-tp.quitc:
			log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " terminating poll loop")

			tp.shutDown(nil)
			return

		case cbEvt, ok = <-tp.batchProcessedCbChannel:
			log.Trace().Msg(semLogContext + " received batch processed event")
			if ok {
				if cbEvt.Err != nil && tp.onError(cbEvt.Err) != nil {
					tp.shutDown(cbEvt.Err)
					return
				}
			}

		default:
			log.Trace().Msg(semLogContext + " doing default")
			isMsg, err := tp.poll()
			if err != nil {
				if tp.onError(err) != nil {
					tp.shutDown(err)
					return
				}
			}

			shouldProcessBatch := false
			switch {
			case tp.cfg.WorkMode == WorkModeBatch && ((isMsg && tp.processor.ProcessorBatchSize() == tp.cfg.MaxBatchSize) || !isMsg):
				shouldProcessBatch = true
			//case tp.cfg.WorkMode == WorkModeBatch && !isMsg:
			//	if tp.processor.ProcessorBatchSize() > 0 {
			//		shouldProcessBatch = true
			//	} else {
			//		tp.statsInfo.SetBatchSize(0)
			//	}
			case tp.cfg.WorkMode == WorkModeBatchFF && ((isMsg && len(tp.batchOfChangeEvents.Events) == tp.cfg.MaxBatchSize) || !isMsg):
				shouldProcessBatch = true
				// case !isMsg:
				// shouldProcessBatch = true
				//case tp.cfg.WorkMode == WorkModeBatchFF && !isMsg:
				//	if len(tp.batchOfChangeEvents.Events) > 0 {
				//		shouldProcessBatch = true
				//	} else {
				//		tp.statsInfo.SetBatchSize(0)
				//	}
			}

			if shouldProcessBatch {
				if tp.processor.IsProcessorDeferred() {
					err = tp.deferredProcessBatch(context.Background())
				} else {
					err = tp.processBatch(context.Background())
				}

				if err != nil && tp.onError(err) != nil {
					tp.shutDown(err)
					return
				}
			}
		}
	}
}

func (tp *producerImpl) tickIntervalPollLoop() {
	const semLogContext = "change-stream-cp::poll-loop"
	log.Info().Str("cs-prod-id", tp.cfg.Name).Float64("tick-interval", tp.cfg.TickInterval.Seconds()).Msg(semLogContext + " starting polling loop")

	if tp.consumer == nil {
		tp.shutDown(errors.New("consumer not initialized"))
		return
	}

	ticker := time.NewTicker(tp.cfg.TickInterval)

	var cbEvt BatchProcessedCbEvent
	var ok bool
	for {
		select {
		case <-ticker.C:

			if tp.cfg.WorkMode == WorkModeBatch || tp.cfg.WorkMode == WorkModeBatchFF {
				var err error
				if tp.processor.IsProcessorDeferred() {
					err = tp.deferredProcessBatch(context.Background())
				} else {
					err = tp.processBatch(context.Background())
				}

				if err != nil && tp.onError(err) != nil {
					ticker.Stop()
					tp.shutDown(err)
					return
				}
			}

		case cbEvt, ok = <-tp.batchProcessedCbChannel:
			if ok {
				if cbEvt.Err != nil && tp.onError(cbEvt.Err) != nil {
					tp.shutDown(cbEvt.Err)
					return
				}
			}

		case <-tp.quitc:
			log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " terminating poll loop")
			ticker.Stop()
			tp.shutDown(nil)
			return

		default:
			if isMsg, err := tp.poll(); err != nil {
				if tp.onError(err) != nil {
					ticker.Stop()
					tp.shutDown(err)
					return
				}
			} else if isMsg {
				tp.numberOfMessages++
			}
		}
	}
}

func (tp *producerImpl) BatchProcessedCommitAtCb(cbEvt BatchProcessedCbEvent) {
	const semLogContext = "change-stream-cp::batch-processed-commit-at"
	log.Trace().Msg(semLogContext + " - in")
	err := tp.checkpointSvc.CommitAt(tp.cfg.Consumer.Id, cbEvt.Rt, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}
	log.Trace().Msg(semLogContext + " - out")

}
func (tp *producerImpl) BatchProcessedErrorCb(cbEvt BatchProcessedCbEvent) {
	const semLogContext = "change-stream-cp::batch-processed-error-cb"
	log.Trace().Msg(semLogContext)

	if cbEvt.Err == nil {
		tp.statsInfo.IncBatches()
	} else {
		log.Error().Err(cbEvt.Err).Msg(semLogContext)
		tp.statsInfo.IncBatchErrors()
		if !cbEvt.Rt.IsZero() {
			log.Warn().Str("rt", cbEvt.Rt.String()).Msg(semLogContext + " last committable resume token is not zero - forcing a checkpoint save")
			_ = tp.consumer.CommitAt(cbEvt.Rt, true)
		} else {
			log.Warn().Msg(semLogContext + " no last committable resume token")
		}

		log.Warn().Str("ch", fmt.Sprintf("%v", tp.batchProcessedCbChannel)).Msg(semLogContext + " - writing to batc-processed-event-channel")
		tp.batchProcessedCbChannel <- cbEvt
		log.Warn().Msg(semLogContext + " - batch-processed-event-channel produced")
	}
}

func (tp *producerImpl) poll() (bool, error) {
	const semLogContext = "change-stream-cp::poll"
	log.Trace().Msg(semLogContext)

	var err error

	ev, err := tp.consumer.Poll()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return false, err
	}

	if ev == nil {
		return false, nil
	}

	switch tp.cfg.WorkMode {
	case WorkModeBatch:
		err = tp.addMessage2Batch(ev)
		tp.numberOfMessages++
		if err == nil {
			tp.statsInfo.IncMessages()
		}
	case WorkModeBatchFF:
		tp.numberOfMessages++
		tp.batchOfChangeEvents.Events = append(tp.batchOfChangeEvents.Events, ev)
		tp.statsInfo.IncMessages()
	default:
		err = tp.processMessage(ev)
		if err == nil {
			tp.numberOfMessages++
			err = tp.consumer.Commit()
		} else {
			// the error is anyway logged but the one propagated is the prev one.
			log.Warn().Msg(semLogContext + " synch on last committed message")
			_ = tp.consumer.CommitAt(checkpoint.ResumeToken{}, true)
		}
	}

	return true, err
}

func (tp *producerImpl) addMessage2Batch(km *events.ChangeEvent) error {
	const semLogContext = "change-stream-cp::add-message-2-batch"
	log.Trace().Msg(semLogContext)
	var err error

	spanName := tp.cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.cfg.Name
	}

	err = tp.processor.AddMessage2Batch(km)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		tp.statsInfo.IncMessageErrors()
		return err
	}

	// should not commit at this stage
	// err = tp.consumer.Commit()

	return nil
}

func (tp *producerImpl) deferredProcessBatch(ctx context.Context) error {
	const semLogContext = "change-stream-cp::deferred-process-batch"
	log.Trace().Msg(semLogContext)
	var err error

	var batchSize int
	switch tp.cfg.WorkMode {
	case WorkModeBatch:
		batchSize = tp.processor.ProcessorBatchSize()
	case WorkModeBatchFF:
		batchSize = len(tp.batchOfChangeEvents.Events)
	default:
	}

	tp.statsInfo.SetBatchSize(batchSize)
	if batchSize == 0 {
		return nil
	}

	defer func() {
		tp.batchOfChangeEvents = BatchOfChangeStreamEvents{}
	}()

	switch tp.cfg.WorkMode {
	case WorkModeBatch:
		_, err = tp.processor.ProcessBatch()
	case WorkModeBatchFF:
		_, err = tp.processor.ProcessBatchOfChangeStreamEvents(tp.batchOfChangeEvents)
	default:
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		tp.statsInfo.IncBatchErrors()
	}

	return err
}

func (tp *producerImpl) processBatch(ctx context.Context) error {
	const semLogContext = "change-stream-cp::process-batch"
	log.Trace().Msg(semLogContext)

	var batchSize int
	switch tp.cfg.WorkMode {
	case WorkModeBatch:
		batchSize = tp.processor.ProcessorBatchSize()
	case WorkModeBatchFF:
		batchSize = len(tp.batchOfChangeEvents.Events)
	default:
	}

	tp.statsInfo.SetBatchSize(batchSize)
	if batchSize == 0 {
		return nil
	}

	defer func() {
		tp.processor.ClearProcessor()
		tp.batchOfChangeEvents = BatchOfChangeStreamEvents{}
	}()

	beginOfProcessing := time.Now()

	var lastCommittableResumeToken checkpoint.ResumeToken
	var err error
	switch tp.cfg.WorkMode {
	case WorkModeBatch:
		lastCommittableResumeToken, err = tp.processor.ProcessBatch()
	case WorkModeBatchFF:
		lastCommittableResumeToken, err = tp.processor.ProcessBatchOfChangeStreamEvents(tp.batchOfChangeEvents)
	default:
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		tp.statsInfo.IncBatchErrors()
		if !lastCommittableResumeToken.IsZero() {
			log.Warn().Str("rt", lastCommittableResumeToken.String()).Msg(semLogContext + " last committable resume token is not zero - forcing a checkpoint save")
			_ = tp.consumer.CommitAt(lastCommittableResumeToken, true)
		} else {
			log.Warn().Msg(semLogContext + " no last committable resume token")
		}
	} else {
		if batchSize > 0 {
			tp.statsInfo.IncBatches()
			tp.statsInfo.SetBatchDuration(time.Since(beginOfProcessing).Seconds())
		}

		err = tp.consumer.Commit()
	}

	return err
}

func (tp *producerImpl) processMessage(e *events.ChangeEvent) error {
	const semLogContext = "change-stream-cp::process-message"
	log.Trace().Msg(semLogContext)
	var err error

	beginOfProcessing := time.Now()

	spanName := tp.cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.cfg.Name
	}

	err = tp.processor.ProcessMessage(e)
	if err != nil {
		log.Error().Err(err).Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " error processing message")
		tp.statsInfo.IncMessageErrors()
		return err
	}

	tp.statsInfo.IncMessages()
	tp.statsInfo.SetBatchDuration(time.Since(beginOfProcessing).Seconds())

	return nil
}

func (tp *producerImpl) shutDown(err error) {
	const semLogContext = "change-stream-cp::shutdown"
	log.Trace().Msg(semLogContext)
	tp.shutdownSync.Do(func() {

		if tp.wg != nil {
			tp.wg.Done()
		}

		if tp.consumer != nil {
			tp.consumer.Close()
		}
		tp.consumer = nil

		if tp.parent != nil {
			tp.parent.ConsumerProducerTerminated(err)
		} else {
			log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " parent has not been set....")
		}
	})

}

//func (tp *producerImpl) produceMetric2(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
//	const semLogContext = "change-stream-cp::produce-metric"
//
//	// Unconfigured...
//
//	if metricGroup == nil && tp.cfg.RefMetrics == nil {
//		log.Trace().Msg(semLogContext + " - un-configured metrics")
//		return nil
//	}
//
//	var err error
//	if metricGroup == nil {
//		g, err := promutil.GetGroup(tp.cfg.RefMetrics.GId)
//		if err != nil {
//			log.Trace().Err(err).Msg(semLogContext)
//			return nil
//		}
//
//		metricGroup = &g
//	}
//
//	err = metricGroup.SetMetricValueById(metricId, value, labels)
//	if err != nil {
//		log.Warn().Err(err).Msg(semLogContext)
//	}
//
//	return metricGroup
//}
