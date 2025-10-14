package worker

import (
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Factory func(taskColl *mongo.Collection, task task.Task, wg *sync.WaitGroup) (Worker, error)

type Worker interface {
	Start() error
	Stop() error
}

type PartitionWorker interface {
	Work(p beans.Partition) error
}
