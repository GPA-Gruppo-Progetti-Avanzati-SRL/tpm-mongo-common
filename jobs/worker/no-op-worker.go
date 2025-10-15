package worker

import (
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type NoOpPartitionWorker struct {
}

func NewNoOpWorker(taskColl *mongo.Collection, task task.Task, wg *sync.WaitGroup) (Worker, error) {
	return NewWorker(taskColl, task, &NoOpPartitionWorker{}, wg)
}

func (w *NoOpPartitionWorker) Work(aTask task.Task, partitionNumber int) error {
	const semLogContext = "no-op-partition-worker::work"
	log.Info().Str("task", aTask.Bid).Str("job", aTask.JobId).Int("partition", partitionNumber).Msg(semLogContext)

	for n, v := range aTask.Properties {
		log.Trace().Str("task-prop-name", n).Interface("task-prop-value", v).Msg(semLogContext)
	}
	for n, v := range aTask.Partitions[partitionNumber-1].Properties {
		log.Trace().Str("partition-prop-name", n).Interface("partition-prop-value", v).Msg(semLogContext)
	}
	return nil
}
