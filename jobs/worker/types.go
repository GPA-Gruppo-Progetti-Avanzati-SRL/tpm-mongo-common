package worker

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer/datasource"
	"github.com/rs/zerolog/log"
)

type OnEventResponseStatus int

const (
	OnEventResponseUndefined OnEventResponseStatus = iota
	OnEventResponseQueued
	OnEventResponseProcessed
	OnEventResponseDeferred
	OnEventResponseSkipped
)

type DeferredCbEvent struct {
	Rt  string
	Err error
}

type OnEventResponse struct {
	Status int
}

type Processor interface {
	OnEvent(evt datasource.Event) (OnEventResponseStatus, error)
	OnEvents(evt []datasource.Event) (OnEventResponseStatus, error)
	OnTickEvent() (OnEventResponseStatus, error)
	OnEofEvent() (OnEventResponseStatus, error)
	OnEofPartitionEvent() (OnEventResponseStatus, error)
	NumberOfQueuedEvents() int

	Start() error
	Close() error
	Reset() error

	WithKafkaProducer(kp interface{})
	WithDeferredCallback(cb func(DeferredCbEvent)) error
}

type UnimplementedProcessor struct {
}

func (w *UnimplementedProcessor) OnEvent(evt datasource.Event) (OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-event"
	panic(semLogContext + " - implement me")
}

func (w *UnimplementedProcessor) OnEvents(evt []datasource.Event) (OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-events"
	panic(semLogContext + " - implement me")
}

func (w *UnimplementedProcessor) OnTickEvent() (OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-tick-event"
	panic(semLogContext + " - implement me")
}

func (w *UnimplementedProcessor) OnEofEvent() (OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-eof-event"
	panic(semLogContext + " - implement me")
}

func (w *UnimplementedProcessor) OnEofPartitionEvent() (OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-eof-partition-event"
	panic(semLogContext + " - implement me")
}

func (w *UnimplementedProcessor) NumberOfQueuedEvents() int {
	const semLogContext = "worker-processor::number-of-queued-events"
	panic(semLogContext + " - implement me")
}

func (w *UnimplementedProcessor) Start() error {
	const semLogContext = "worker-processor::start"
	log.Info().Msg(semLogContext)
	return nil
}

func (w *UnimplementedProcessor) Close() error {
	const semLogContext = "worker-processor::close"
	log.Info().Msg(semLogContext)
	return nil
}

func (w *UnimplementedProcessor) Reset() error {
	const semLogContext = "worker-processor::reset"
	log.Info().Msg(semLogContext)
	return nil
}

func (w *UnimplementedProcessor) WithKafkaProducer(kp interface{}) {
	const semLogContext = "worker-processor::with-kafka-producer"
	log.Info().Msg(semLogContext)
}

func (w *UnimplementedProcessor) WithDeferredCallback(cb func(DeferredCbEvent)) error {
	const semLogContext = "worker-processor::with-deferred-callback"
	log.Info().Msg(semLogContext)
	return nil
}
