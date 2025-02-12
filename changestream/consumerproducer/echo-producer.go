package consumerproducer

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type EchoConsumerProducer struct {
	numEvents  int
	numBatches int
	evts       []*events.ChangeEvent
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
	e.evts = append(e.evts, evt)
	return nil
}

func (e *EchoConsumerProducer) ProcessBatch() (checkpoint.ResumeToken, error) {
	const semLogContext = "echo-producer::process-batch"
	log.Info().Msg(semLogContext)
	e.numEvents += len(e.evts)
	e.numBatches++
	log.Info().Interface("num-batches", e.numBatches).Int("num-evts", e.numEvents).Msg(semLogContext)
	if e.numBatches%4 == 0 {
		return checkpoint.ResumeToken{}, errors.New("batch error")
	}
	return checkpoint.ResumeToken{}, nil
}

func (e *EchoConsumerProducer) Clear() {
	const semLogContext = "echo-producer::clear-batch"
	log.Info().Msg(semLogContext)
	e.evts = nil
}

func (e *EchoConsumerProducer) Reset() error {
	const semLogContext = "echo-producer::reset"
	log.Info().Msg(semLogContext)
	e.evts = nil
	e.numEvents = 0
	e.numBatches = 0
	return nil
}

func (e *EchoConsumerProducer) BatchSize() int {
	const semLogContext = "echo-producer::batch-size"

	sz := len(e.evts)
	if sz > 0 {
		log.Info().Int("batch-size", sz).Msg(semLogContext)
	}
	return sz
}
