package consumerproducer

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type EchoConsumerProducer struct {
	evts []*events.ChangeEvent
}

func (e *EchoConsumerProducer) ProcessMessage(evt *events.ChangeEvent) error {
	const semLogContext = "echo-producer::process-message"
	log.Trace().Interface("evt", evt).Msg(semLogContext)
	return nil
}

func (e *EchoConsumerProducer) AddMessage2Batch(evt *events.ChangeEvent) error {
	const semLogContext = "echo-producer::add-message-2-batch"
	log.Trace().Interface("evt", evt).Msg(semLogContext)
	e.evts = append(e.evts, evt)
	return nil
}

func (e *EchoConsumerProducer) ProcessBatch() error {
	const semLogContext = "echo-producer::process-batch"
	log.Trace().Msg(semLogContext)
	return nil
}

func (e *EchoConsumerProducer) Clear() {
	const semLogContext = "echo-producer::clear-batch"
	log.Trace().Msg(semLogContext)
	e.evts = nil
}

func (e *EchoConsumerProducer) BatchSize() int {
	const semLogContext = "echo-producer::batch-size"

	sz := len(e.evts)
	if sz > 0 {
		log.Info().Int("batch-size", sz).Msg(semLogContext)
	}
	return sz
}
