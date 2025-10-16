package worker

import (
	"errors"
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

var registry = map[string]Factory{}

func RegisterFactory(n string, workerFactory Factory) {
	registry[n] = workerFactory
}

func GetRegisteredFactory(taskColl *mongo.Collection, task task.Task, wg *sync.WaitGroup) (Worker, error) {
	const semLogContext = "worker::get-registered-factory"
	f, ok := registry[task.Name]
	if !ok {
		err := errors.New("task not registered")
		log.Error().Err(err).Str("task", task.Name).Msg(semLogContext)
		return nil, err
	}

	w, err := f(taskColl, task, wg)
	if err != nil {
		log.Error().Err(err).Str("task", task.Name).Msg(semLogContext)
		return nil, err
	}

	return w, nil
}

type Factory func(taskColl *mongo.Collection, task task.Task, wg *sync.WaitGroup) (Worker, error)

type Worker interface {
	Start() error
	Stop() error
}

type PartitionWorker interface {
	Work(aTask task.Task, partitionNumber int) error
}
