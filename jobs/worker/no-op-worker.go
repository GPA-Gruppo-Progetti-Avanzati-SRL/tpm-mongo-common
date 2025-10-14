package worker

import (
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type NoOpPartitionWorker struct {
}

func NewNoOpWorker(taskColl *mongo.Collection, task task.Task, wg *sync.WaitGroup) (Worker, error) {
	return NewWorker(taskColl, task, &NoOpPartitionWorker{}, wg)
}

func (w *NoOpPartitionWorker) Work(p beans.Partition) error {
	const semLogContext = "no-op-partition-worker::work"
	log.Info().Interface("p", p).Msg(semLogContext)
	return nil
}
