package producer

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type Producer interface {
	Start() error
	Close() error
	SetParent(s Server)
	Name() string
}

type Server interface {
	Close()
	Start()
	ProducerTerminated(err error)
}

type Processor interface {
	ProcessMessage(evt *events.ChangeEvent) error
	AddMessage2Batch(evt *events.ChangeEvent) error
	ProcessBatch() error
	Clear()
	BatchSize() int
}

type UnimplementedTransformerProducerProcessor struct {
}

func (b *UnimplementedTransformerProducerProcessor) ProcessMessage(m *events.ChangeEvent) error {
	const semLogContext = "t-prod-processor::process-message"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) AddMessage2Batch(m *events.ChangeEvent) error {
	const semLogContext = "t-prod-processor::add-to-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) ProcessBatch() error {
	const semLogContext = "t-prod-processor::process-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) BatchSize() int {
	const semLogContext = "t-prod-processor::batch-size"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedTransformerProducerProcessor) Clear() {
	const semLogContext = "t-prod-processor::clear"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}
