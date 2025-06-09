package worker

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer/datasource"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/rs/zerolog/log"
	"sync"
	"time"
)

type BatchOfEvents struct {
	partition int32
	evts      []datasource.Event
}

func (b *BatchOfEvents) Add(event datasource.Event) {
	b.partition = event.Partition
	b.evts = append(b.evts, event)
}

func (b *BatchOfEvents) Size() int {
	return len(b.evts)
}

type Worker struct {
	Cfg    *Config
	Server *Server

	wg           *sync.WaitGroup
	shutdownSync sync.Once
	quitc        chan struct{}

	numberOfMessages        int
	processor               Processor
	batchProcessedCbChannel chan DeferredCbEvent
	consumer                *taskconsumer.Consumer
	statsInfo               *Metrics

	jobsColl      *mongo.Collection
	task          task.Task
	batchOfEvents BatchOfEvents
}

func NewWorker(jobsCollection *mongo.Collection, task task.Task, cfg *Config, wg *sync.WaitGroup, processor Processor) (*Worker, error) {
	const semLogContext = "worker-factory::new"

	if cfg.WorkMode != WorkModeBatch {
		cfg.WorkMode = WorkModeMsg
	}

	t := Worker{
		Cfg:       cfg,
		quitc:     make(chan struct{}),
		wg:        wg,
		statsInfo: NewProducerStatsInfo(cfg.Name, cfg.MetricsGId),
		jobsColl:  jobsCollection,
		task:      task,
	}

	err := processor.WithDeferredCallback(nil)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	t.processor = processor

	if cfg.TickInterval == 0 {
		cfg.TickInterval = time.Second
	}

	if cfg.WorkMode == WorkModeBatch {
		if cfg.MaxBatchSize <= 0 {
			cfg.MaxBatchSize = 100
		}
	} else {
		cfg.MaxBatchSize = -1
	}

	return &t, nil
}

func (tp *Worker) Start() error {
	const semLogContext = "worker::start"
	var err error

	log.Info().Msg(semLogContext)

	tp.consumer, err = tp.newConsumer()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	// Add to wait group
	if tp.wg != nil {
		tp.wg.Add(1)
	}

	err = tp.processor.Start()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	go tp.PollLoop()
	return nil

}

func (tp *Worker) Close() error {
	const semLogContext = "worker::close"
	log.Info().Str("name", tp.Cfg.Name).Msg(semLogContext + " signalling shutdown worker")
	close(tp.quitc)
	_ = tp.processor.Close()
	return nil
}

func (tp *Worker) newConsumer() (*taskconsumer.Consumer, error) {
	const semLogContext = "worker::new-consumer"

	var opts []taskconsumer.ConfigOption

	consumer, err := taskconsumer.NewConsumer(tp.jobsColl, tp.task, &tp.Cfg.Consumer, opts...)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return consumer, nil
}

func (tp *Worker) onError(errIn error) error {
	const semLogContext = "worker::on-error"

	if tp.Cfg.OnErrorPolicy == OnErrorContinue {
		log.Warn().Err(errIn).Msg(semLogContext + " - on error continue")
		return nil
	}

	return errIn
}

func (tp *Worker) PollLoop() {
	const semLogContext = "worker::poll-loop"
	var err error

	log.Info().Str("id", tp.Cfg.Name).Float64("tick-interval-ss", tp.Cfg.TickInterval.Seconds()).Int("max-batch-size", tp.Cfg.MaxBatchSize).Msg(semLogContext + " starting polling loop")
	if tp.consumer == nil {
		tp.shutDown(errors.New("consumer not initialized"))
		return
	}

	ticker := time.NewTicker(tp.Cfg.TickInterval)
	var cbEvt DeferredCbEvent
	var ok bool
	for {
		select {
		case <-ticker.C:
			_, err = tp.processor.OnTickEvent()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				if tp.onError(err) != nil {
					tp.shutDown(err)
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
			log.Info().Str("name", tp.Cfg.Name).Msg(semLogContext + " terminating poll loop")
			ticker.Stop()
			tp.shutDown(nil)
			return

		default:
			var evtTyp string
			evtTyp, err = tp.pollAndProcess()
			if err != nil && tp.onError(err) != nil {
				tp.shutDown(err)
				return
			}

			if evtTyp == datasource.EventTypeEof {
				ticker.Stop()
				tp.shutDown(nil)
				return
			}

			//}
			//
			//if evtTyp == datasource.EventTypeEof {
			//
			//} else {
			//	tp.numberOfMessages++
			//	if tp.Cfg.WorkMode == WorkModeBatch && tp.batchOfEvents.Size() == tp.Cfg.MaxBatchSize {
			//		err = tp.processBatch(context.Background())
			//	}
			//
			//	if err != nil && tp.onError(err) != nil {
			//		tp.shutDown(err)
			//		return
			//	}
			//}
		}
	}
}

func (tp *Worker) pollAndProcess() (string, error) {
	const semLogContext = "worker::do-poll"
	var err error
	var evt datasource.Event
	evt, err = tp.poll()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return datasource.EventTypeError, err
	}

	switch evt.Typ {
	case datasource.EventTypeError:
	case datasource.EventTypeEof:
		switch tp.Cfg.WorkMode {
		case WorkModeBatch:
			if tp.batchOfEvents.Size() > 0 {
				err = tp.processBatch(context.Background())
			}
		case WorkModeMsg:
			_, err = tp.processor.OnEofEvent()
		}
	case datasource.EventTypeEofPartition:
		switch tp.Cfg.WorkMode {
		case WorkModeBatch:
			if tp.batchOfEvents.Size() > 0 {
				err = tp.processBatch(context.Background())
			}
		case WorkModeMsg:
			_, err = tp.processor.OnEofPartitionEvent()
		}
	case datasource.EventTypeDocument:
		switch tp.Cfg.WorkMode {
		case WorkModeBatch:
			tp.numberOfMessages++
			tp.batchOfEvents.Add(evt)
			tp.statsInfo.IncMessages()
			if tp.batchOfEvents.Size() == tp.Cfg.MaxBatchSize {
				err = tp.processBatch(context.Background())
			}

		case WorkModeMsg:
			err = tp.processMessage(evt)
		}
	}

	return evt.Typ, err
}

func (tp *Worker) DeferredProcessingCallback(cbEvt DeferredCbEvent) {
	const semLogContext = "worker::batch-processed-commit-at"
	log.Trace().Msg(semLogContext + " - in")

	if cbEvt.Err != nil {
		log.Error().Err(cbEvt.Err).Msg(semLogContext)
		tp.statsInfo.IncBatchErrors()
		log.Warn().Str("ch", fmt.Sprintf("%v", tp.batchProcessedCbChannel)).Msg(semLogContext + " - writing to batch-processed-event-channel")
		tp.batchProcessedCbChannel <- cbEvt
		log.Warn().Msg(semLogContext + " - batch-processed-event-channel produced")
	} else {
		tp.statsInfo.IncBatches()
	}
}

func (tp *Worker) poll() (datasource.Event, error) {
	const semLogContext = "worker::poll"
	log.Trace().Msg(semLogContext)

	var err error

	ev, err := tp.consumer.Poll()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return datasource.ErrEvent, err
	}

	return ev, err
}

//func (tp *Worker) poll2() (string, error) {
//	const semLogContext = "worker::poll"
//	log.Trace().Msg(semLogContext)
//
//	var err error
//
//	ev, err := tp.consumer.Poll()
//	if err != nil {
//		log.Error().Err(err).Msg(semLogContext)
//		return datasource.EventTypeError, err
//	}
//	for ev.DataSourceType != datasource.EventTypeEof {
//		ev, err = tp.consumer.Poll()
//		if err != nil {
//			log.Error().Err(err).Msg(semLogContext)
//			return datasource.EventTypeError, err
//		}
//	}
//
//	if ev.IsBoundary() {
//		return datasource.EventTypeEof, nil
//	}
//
//	switch tp.Cfg.WorkMode {
//	case WorkModeBatch:
//		tp.numberOfMessages++
//		tp.batchOfEvents.Add(ev)
//		tp.statsInfo.IncMessages()
//	default:
//		err = tp.processMessage(ev)
//	}
//
//	return datasource.EventTypeDocument, err
//}

func (tp *Worker) processBatch(ctx context.Context) error {
	const semLogContext = "worker::process-batch"
	log.Trace().Msg(semLogContext)

	batchSize := tp.batchOfEvents.Size()
	if batchSize == 0 {
		return nil
	}
	tp.statsInfo.SetBatchSize(batchSize)

	defer func() {
		tp.batchOfEvents = BatchOfEvents{}
	}()

	beginOfProcessing := time.Now()
	resp, err := tp.processor.OnEvents(tp.batchOfEvents.evts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		tp.statsInfo.IncBatchErrors()
		err1 := tp.consumer.AbortPartial(datasource.NoEvent)
		if err1 != nil {
			log.Error().Err(err1).Msg(semLogContext)
		}
		return err
	}

	switch resp {
	case OnEventResponseProcessed:
		tp.statsInfo.IncBatches()
		tp.statsInfo.SetBatchDuration(time.Since(beginOfProcessing).Seconds())
		err = tp.consumer.Commit(true)
	case OnEventResponseDeferred:
	default:
		log.Error().Int("on-events-response", int(resp)).Msg(semLogContext)
	}

	return err
}

func (tp *Worker) processMessage(e datasource.Event) error {
	const semLogContext = "worker::process-message"
	log.Trace().Msg(semLogContext)
	var err error

	beginOfProcessing := time.Now()

	spanName := tp.Cfg.Tracing.SpanName
	if spanName == "" {
		spanName = tp.Cfg.Name
	}

	resp, err := tp.processor.OnEvent(e)
	if err != nil {
		log.Error().Err(err).Str("name", tp.Cfg.Name).Msg(semLogContext + " error processing message")
		tp.statsInfo.IncMessageErrors()
		err1 := tp.consumer.AbortPartial(datasource.NoEvent)
		if err1 != nil {
			log.Error().Err(err1).Msg(semLogContext)
		}

		return err
	}

	switch resp {
	case OnEventResponseProcessed:
		tp.statsInfo.IncMessages()
		tp.statsInfo.SetBatchDuration(time.Since(beginOfProcessing).Seconds())
		tp.numberOfMessages++
		err = tp.consumer.Commit(false)

	case OnEventResponseDeferred:
	case OnEventResponseQueued:
	}

	return nil
}

func (tp *Worker) shutDown(err error) {
	const semLogContext = "worker::shutdown"
	log.Trace().Msg(semLogContext)
	tp.shutdownSync.Do(func() {

		if tp.wg != nil {
			tp.wg.Done()
		}

		if tp.consumer != nil {
			_ = tp.consumer.Close(context.Background())
		}
		tp.consumer = nil

		if tp.Server != nil {
			tp.Server.WorkerTerminated(err)
		} else {
			log.Info().Str("name", tp.Cfg.Name).Msg(semLogContext + " parent has not been set....")
		}
	})

}
