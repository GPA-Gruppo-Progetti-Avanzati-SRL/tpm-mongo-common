package producer

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type EchoProducer struct {
	UnimplementedTransformerProducerProcessor
}

func (e *EchoProducer) ProcessMessage(evt *events.ChangeEvent) error {
	const semLogContext = "echo-producer::process-message"
	log.Trace().Interface("evt", evt).Msg(semLogContext)
	return nil
}
