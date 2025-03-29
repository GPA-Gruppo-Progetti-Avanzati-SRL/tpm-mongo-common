package consumerproducer

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream"
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

type producerImpl struct {
	cfg *Config

	wg           *sync.WaitGroup
	shutdownSync sync.Once
	quitc        chan struct{}

	parent                  Server
	numberOfMessages        int
	processor               Processor
	batchProcessedCbChannel chan BatchProcessedCbEvent
	checkpointSvc           checkpoint.ResumeTokenCheckpointSvc
	consumer                *changestream.Consumer
	metricLabels            map[string]string
}

func NewConsumerProducer(cfg *Config, wg *sync.WaitGroup, processor *EchoConsumerProducer) (ConsumerProducer, error) {
	const semLogContext = "change-stream-cs-factory::new"

	if cfg.WorkMode != WorkModeBatch {
		cfg.WorkMode = WorkModeMsg
	}

	t := producerImpl{
		cfg:   cfg,
		quitc: make(chan struct{}),
		wg:    wg,
		metricLabels: map[string]string{
			"name": cfg.Name,
		},
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

	if cfg.WorkMode == WorkModeBatch {
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

func (tp *producerImpl) newConsumer() (*changestream.Consumer, error) {
	const semLogContext = "change-stream-cp::new-consumer"

	var opts []changestream.ConfigOption
	if tp.cfg.CheckPointSvcConfig.Typ != "" {
		svc, err := factory.NewCheckPointSvc(tp.cfg.CheckPointSvcConfig)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
		opts = append(opts, changestream.WithCheckpointSvc(svc))

		if tp.processor != nil && tp.processor.IsProcessorDeferred() {
			tp.checkpointSvc = svc
		}
	}

	consumer, err := changestream.NewConsumer(&tp.cfg.Consumer, opts...)
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
	_ = tp.produceMetric(nil, MetricRewindsCounter, 1, tp.metricLabels)

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
			if ok {
				if cbEvt.Err != nil && tp.onError(cbEvt.Err) != nil {
					tp.shutDown(cbEvt.Err)
					return
				}
			}

		default:
			isMsg, err := tp.poll()
			if err != nil {
				if tp.onError(err) != nil {
					tp.shutDown(err)
					return
				}
			}

			shouldProcessBatch := false
			if isMsg {
				tp.numberOfMessages++
				if tp.cfg.WorkMode == WorkModeBatch && tp.processor.ProcessorBatchSize() == tp.cfg.MaxBatchSize {
					shouldProcessBatch = true
				}
			} else {
				if tp.cfg.WorkMode == WorkModeBatch && tp.processor.ProcessorBatchSize() > 0 {
					shouldProcessBatch = true
				}
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
			if tp.cfg.WorkMode == WorkModeBatch {
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

	err := tp.checkpointSvc.CommitAt(tp.cfg.Consumer.Id, cbEvt.Rt, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

}
func (tp *producerImpl) BatchProcessedErrorCb(cbEvt BatchProcessedCbEvent) {
	const semLogContext = "change-stream-cp::batch-processed-error-cb"

	if cbEvt.Err == nil {
		_ = tp.produceMetric(nil, MetricBatches, 1, tp.metricLabels)
	} else {
		log.Error().Err(cbEvt.Err).Msg(semLogContext)
		_ = tp.produceMetric(nil, MetricBatchErrors, 1, tp.metricLabels)
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
	var err error

	ev, err := tp.consumer.Poll()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return false, err
	}

	if ev == nil {
		return false, nil
	}

	if tp.cfg.WorkMode == WorkModeBatch {
		err = tp.addMessage2Batch(ev)
	} else {
		err = tp.processMessage(ev)
		if err == nil {
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
	var err error

	spanName := tp.cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.cfg.Name
	}

	err = tp.processor.AddMessage2Batch(km)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		_ = tp.produceMetric(nil, MetricMessageErrors, 1, tp.metricLabels)
		return err
	}

	_ = tp.produceMetric(nil, MetricMessages, 1, tp.metricLabels)
	// should not commit at this stage
	// err = tp.consumer.Commit()

	return err
}

func (tp *producerImpl) deferredProcessBatch(ctx context.Context) error {
	const semLogContext = "change-stream-cp::deferred-process-batch"
	var err error

	batchSize := tp.processor.ProcessorBatchSize()
	if tp.cfg.WorkMode != WorkModeBatch || batchSize == 0 {
		return nil
	}

	metricGroup := tp.produceMetric(nil, MetricBatchSize, float64(batchSize), tp.metricLabels)

	_, err = tp.processor.ProcessBatch()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		_ = tp.produceMetric(metricGroup, MetricBatchErrors, 1, tp.metricLabels)
	}

	return err
}

func (tp *producerImpl) processBatch(ctx context.Context) error {
	const semLogContext = "change-stream-cp::process-batch"

	batchSize := tp.processor.ProcessorBatchSize()
	if tp.cfg.WorkMode != WorkModeBatch || tp.processor.ProcessorBatchSize() == 0 {
		return nil
	}

	defer tp.processor.ClearProcessor()

	beginOfProcessing := time.Now()
	metricGroup := tp.produceMetric(nil, MetricBatchSize, float64(batchSize), tp.metricLabels)

	lastCommittableResumeToken, err := tp.processor.ProcessBatch()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		_ = tp.produceMetric(nil, MetricBatchErrors, 1, tp.metricLabels)
		if !lastCommittableResumeToken.IsZero() {
			log.Warn().Str("rt", lastCommittableResumeToken.String()).Msg(semLogContext + " last committable resume token is not zero - forcing a checkpoint save")
			_ = tp.consumer.CommitAt(lastCommittableResumeToken, true)
		} else {
			log.Warn().Msg(semLogContext + " no last committable resume token")
		}
	} else {
		if batchSize > 0 {
			metricGroup = tp.produceMetric(metricGroup, MetricBatches, 1, tp.metricLabels)
			metricGroup = tp.produceMetric(metricGroup, MetricBatchDuration, time.Since(beginOfProcessing).Seconds(), tp.metricLabels)
		}

		err = tp.consumer.Commit()
	}

	return err
}

func (tp *producerImpl) processMessage(e *events.ChangeEvent) error {
	const semLogContext = "change-stream-cp::process-message"

	var err error

	beginOfProcessing := time.Now()

	spanName := tp.cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.cfg.Name
	}

	err = tp.processor.ProcessMessage(e)
	if err != nil {
		log.Error().Err(err).Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " error processing message")
		_ = tp.produceMetric(nil, MetricMessageErrors, 1, tp.metricLabels)
		return err
	}

	metricGroup := tp.produceMetric(nil, MetricMessages, 1, tp.metricLabels)
	metricGroup = tp.produceMetric(metricGroup, MetricMessageDuration, time.Since(beginOfProcessing).Seconds(), tp.metricLabels)

	return nil
}

func (tp *producerImpl) shutDown(err error) {
	const semLogContext = "change-stream-cp::shutdown"

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

func (tp *producerImpl) produceMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "change-stream-cp::produce-metric"

	// Unconfigured...

	if metricGroup == nil && tp.cfg.RefMetrics == nil {
		log.Trace().Msg(semLogContext + " - un-configured metrics")
		return nil
	}

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(tp.cfg.RefMetrics.GId)
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
