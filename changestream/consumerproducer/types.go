package consumerproducer

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type ConsumerProducer interface {
	Start() error
	Close() error
	SetParent(s Server)
	Name() string
}

type Server interface {
	Close()
	Start() error
	ConsumerProducerTerminated(err error)
}

type Processor interface {
	ProcessMessage(evt *events.ChangeEvent) error
	AddMessage2Batch(evt *events.ChangeEvent) error
	ProcessBatch() error
	Clear()
	BatchSize() int
}

type UnimplementedConsumerProducerProcessor struct {
}

func (b *UnimplementedConsumerProducerProcessor) ProcessMessage(m *events.ChangeEvent) error {
	const semLogContext = "t-prod-processor::process-message"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedConsumerProducerProcessor) AddMessage2Batch(m *events.ChangeEvent) error {
	const semLogContext = "t-prod-processor::add-to-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedConsumerProducerProcessor) ProcessBatch() error {
	const semLogContext = "t-prod-processor::process-batch"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedConsumerProducerProcessor) BatchSize() int {
	const semLogContext = "t-prod-processor::batch-size"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}

func (b *UnimplementedConsumerProducerProcessor) Clear() {
	const semLogContext = "t-prod-processor::clear"
	err := errors.New("not implemented")
	log.Error().Err(err).Msg(semLogContext)
	panic(err)
}
