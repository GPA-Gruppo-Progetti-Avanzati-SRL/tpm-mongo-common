package producer

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

const (
	MetricBatchErrors     = "tprod-batch-errors"
	MetricBatches         = "tprod-batches"
	MetricBatchSize       = "tprod-batch-size"
	MetricBatchDuration   = "tprod-batch-duration"
	MetricMessageErrors   = "tprod-event-errors"
	MetricMessages        = "tprod-events"
	MetricMessageDuration = "tprod-event-duration"
)

type producerImpl struct {
	cfg *Config

	wg           *sync.WaitGroup
	shutdownSync sync.Once
	quitc        chan struct{}

	parent           Server
	numberOfMessages int
	processor        Processor

	consumer     *changestream.Consumer
	metricLabels map[string]string
}

func NewProducer(cfg *Config, wg *sync.WaitGroup, processor Processor) (Producer, error) {
	const semLogContext = "t-prod-factory::new"

	if cfg.WorkMode != WorkModeBatch {
		cfg.WorkMode = WorkModeMsg
	}

	t := producerImpl{
		cfg:       cfg,
		quitc:     make(chan struct{}),
		wg:        wg,
		processor: processor,
		metricLabels: map[string]string{
			"name": cfg.Name,
		},
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
	const semLogContext = "t-prod::start"
	var err error

	if tp.cfg.StartDelay > 0 {
		time.Sleep(time.Millisecond * time.Duration(tp.cfg.StartDelay))
	}

	log.Info().Msg(semLogContext)

	tp.consumer, err = changestream.NewConsumer(&tp.cfg.Consumer)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	// Add to wait group
	if tp.wg != nil {
		tp.wg.Add(1)
	}

	go tp.pollLoop()
	return nil
}

func (tp *producerImpl) Close() error {
	const semLogContext = "t-prod::close"
	log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " signalling shutdown transformer producer")
	close(tp.quitc)
	return nil
}

func (tp *producerImpl) pollLoop() {
	const semLogContext = "t-prod::poll-loop"
	log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " starting polling loop")

	ticker := time.NewTicker(tp.cfg.TickInterval)

	for {
		select {
		case <-ticker.C:
			if tp.cfg.WorkMode == WorkModeBatch {
				err := tp.processBatch(context.Background())
				if err != nil {
					ticker.Stop()
					tp.shutDown(err)
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
				ticker.Stop()
				tp.shutDown(err)
				return
			} else if isMsg {
				tp.numberOfMessages++
			}
		}
	}
}

func (tp *producerImpl) poll() (bool, error) {
	const semLogContext = "t-prod::poll"
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
	}

	return true, err
}

func (tp *producerImpl) addMessage2Batch(km *events.ChangeEvent) error {
	const semLogContext = "t-prod::add-message-2-batch"
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
	return nil
}

func (tp *producerImpl) processBatch(ctx context.Context) error {
	const semLogContext = "t-prod::process-batch"

	if tp.cfg.WorkMode != WorkModeBatch || tp.processor.BatchSize() == 0 {
		return nil
	}

	defer tp.processor.Clear()

	beginOfProcessing := time.Now()
	batchSize := tp.processor.BatchSize()

	err := tp.processor.ProcessBatch()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		_ = tp.produceMetric(nil, MetricBatchErrors, 1, tp.metricLabels)
	} else {
		if batchSize > 0 {
			metricGroup := tp.produceMetric(nil, MetricBatches, 1, tp.metricLabels)
			metricGroup = tp.produceMetric(metricGroup, MetricBatchSize, float64(batchSize), tp.metricLabels)
			metricGroup = tp.produceMetric(metricGroup, MetricBatchDuration, time.Since(beginOfProcessing).Seconds(), tp.metricLabels)
		}
	}

	return err
}

func (tp *producerImpl) processMessage(e *events.ChangeEvent) error {
	const semLogContext = "t-prod::process-message"

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
	const semLogContext = "t-prod::shutdown"

	tp.shutdownSync.Do(func() {

		if tp.wg != nil {
			tp.wg.Done()
		}

		if tp.consumer != nil {
			tp.consumer.Close()
		}
		tp.consumer = nil

		if tp.parent != nil {
			tp.parent.ProducerTerminated(err)
		} else {
			log.Info().Str("cs-prod-id", tp.cfg.Name).Msg(semLogContext + " parent has not been set....")
		}
	})

}

func (tp *producerImpl) produceMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "t-prod::produce-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(tp.cfg.RefMetrics.GId)
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
