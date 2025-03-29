package consumerproducer

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type EchoConsumerBatch struct {
	evts []*events.ChangeEvent
}

const deferredMode = true

type EchoConsumerProducer struct {
	numEvents    int
	numBatches   int
	currentBatch EchoConsumerBatch
	errorCb      BatchProcessedErrorCb
	commitAtCb   BatchProcessedCommitAtCb
	workChannel  chan EchoConsumerBatch
}

func NewEchoConsumerProducer() *EchoConsumerProducer {
	if deferredMode {
		return &EchoConsumerProducer{workChannel: make(chan EchoConsumerBatch, 10)}
	}

	return &EchoConsumerProducer{}
}

func (p *EchoConsumerProducer) StartProcessor() {
	const semLogContext = "echo-producer::start"
	log.Info().Msg(semLogContext)

	if deferredMode {
		go p.deferredBatchWorkLoop()
	}
}

func (p *EchoConsumerProducer) CloseProcessor() {
	const semLogContext = "echo-producer::close"
	log.Info().Msg(semLogContext)

	if deferredMode {
		close(p.workChannel)
	}
}

func (e *EchoConsumerProducer) ProcessMessage(evt *events.ChangeEvent) error {
	const semLogContext = "echo-producer::process-message"

	e.numEvents++
	log.Info().Interface("evt", evt).Int("num-evts", e.numEvents).Msg(semLogContext)
	if e.numEvents%4 == 0 {
		return errors.New("msg error")
	}
	return nil
}

func (e *EchoConsumerProducer) AddMessage2Batch(evt *events.ChangeEvent) error {
	const semLogContext = "echo-producer::add-message-2-batch"
	log.Trace().Interface("evt", evt).Msg(semLogContext)
	e.currentBatch.evts = append(e.currentBatch.evts, evt)
	return nil
}

func (e *EchoConsumerProducer) ProcessBatch() (checkpoint.ResumeToken, error) {
	const semLogContext = "echo-producer::process-batch"
	log.Info().Msg(semLogContext)
	if !deferredMode {
		return e.doProcessBatch(e.currentBatch)
	} else {
		e.workChannel <- e.currentBatch
		e.currentBatch = EchoConsumerBatch{}
	}

	return checkpoint.ResumeToken{}, nil
}

func (e *EchoConsumerProducer) deferredBatchWorkLoop() {
	const semLogContext = "echo-producer::deferred-batch-work-loop"
	log.Info().Msg(semLogContext + " - start")

	for {
		batch, ok := <-e.workChannel
		if !ok {
			break
		}
		rt, err := e.doProcessBatch(batch)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			e.errorCb.BatchProcessedErrorCb(BatchProcessedCbEvent{rt, err})
		}

		e.commitAtCb.BatchProcessedCommitAtCb(BatchProcessedCbEvent{rt, err})
	}

	log.Info().Msg(semLogContext + " - exited from loop")
}

func (e *EchoConsumerProducer) doProcessBatch(batch EchoConsumerBatch) (checkpoint.ResumeToken, error) {
	const semLogContext = "echo-producer::do-process-batch"

	e.numEvents += len(batch.evts)
	e.numBatches++
	log.Info().Interface("num-batches", e.numBatches).Int("num-evts", e.numEvents).Msg(semLogContext)
	if e.numBatches%4 == 0 {
		return checkpoint.ResumeToken{}, errors.New("currentBatch error")
	}

	rt := batch.evts[len(batch.evts)-1].ResumeTok
	return rt, nil
}

func (e *EchoConsumerProducer) ClearProcessor() {
	const semLogContext = "echo-producer::clear-batch"
	log.Info().Msg(semLogContext)
	e.currentBatch = EchoConsumerBatch{}
}

func (e *EchoConsumerProducer) ResetProcessor() error {
	const semLogContext = "echo-producer::reset"
	log.Info().Msg(semLogContext)
	e.currentBatch = EchoConsumerBatch{}
	e.numEvents = 0
	e.numBatches = 0
	return nil
}

func (e *EchoConsumerProducer) ProcessorBatchSize() int {
	const semLogContext = "echo-producer::batch-size"

	sz := len(e.currentBatch.evts)
	if sz > 0 {
		log.Info().Int("currentBatch-size", sz).Msg(semLogContext)
	}
	return sz
}

func (e *EchoConsumerProducer) IsProcessorDeferred() bool {
	const semLogContext = "echo-producer::is-deferred"
	return deferredMode
}

func (e *EchoConsumerProducer) WithBatchProcessedErrorCallback(errorCb BatchProcessedErrorCb) {
	e.errorCb = errorCb
}

func (e *EchoConsumerProducer) WithBatchProcessedCommitAtCallback(commitCb BatchProcessedCommitAtCb) {
	e.commitAtCb = commitCb
}
