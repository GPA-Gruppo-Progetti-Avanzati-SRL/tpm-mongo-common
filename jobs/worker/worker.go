package worker

import (
	"errors"
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
)

var registry = map[string]Factory{}

func RegisterFactory(n string, workerFactory Factory) {
	registry[n] = workerFactory
}

func GetRegisteredWorker(task task.Task, opts ...Option) (Worker, error) {
	const semLogContext = "worker::get-registered-factory"
	f, ok := registry[task.Name]
	if !ok {
		err := errors.New("task not registered")
		log.Error().Err(err).Str("task", task.Name).Msg(semLogContext)
		return nil, err
	}

	w, err := f(task, opts...)
	if err != nil {
		log.Error().Err(err).Str("task", task.Name).Msg(semLogContext)
		return nil, err
	}

	return w, nil
}

type Options struct {
	wg              *sync.WaitGroup
	taskLogStoreRef mongolks.StoreReference
	taskStoreRef    mongolks.StoreReference
}

type Option func(fo *Options)

func WithWaitGroup(wg *sync.WaitGroup) Option {
	return func(fo *Options) {
		fo.wg = wg
	}
}

func WithTaskStoreReference(sr mongolks.StoreReference) Option {
	return func(fo *Options) {
		fo.taskStoreRef = sr
	}
}

func WithTaskLogStoreReference(sr mongolks.StoreReference) Option {
	return func(fo *Options) {
		fo.taskLogStoreRef = sr
	}
}

type Factory func(task task.Task, opts ...Option) (Worker, error)

type Worker interface {
	Start() error
	Stop() error
}

type PartitionWorker interface {
	Work(aTask task.Task, partitionNumber int) error
}
