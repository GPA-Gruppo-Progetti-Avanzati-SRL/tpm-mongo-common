package worker

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker/logger"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
)

type NoOpPartitionWorker struct {
	wrkLogger   *logger.WorkerLogger
	logStoreRef mongolks.StoreReference
}

const NoOPWorkerName = "snoop"

func NewNoOpWorker(task task.Task, opts ...Option) (Worker, error) {

	options := &Options{}
	for _, opt := range opts {
		opt(options)
	}

	return NewWorker(task, &NoOpPartitionWorker{logStoreRef: options.taskLogStoreRef}, opts...)
}

func (w *NoOpPartitionWorker) Work(aTask task.Task, partitionNumber int) error {
	const semLogContext = "no-op-partition-worker::work"
	w.wrkLogger = logger.NewWorkerLogger(aTask, int32(partitionNumber), w.logStoreRef)
	w.wrkLogger.Logger.Info().Int("partition", partitionNumber).Msg(semLogContext)

	for n, v := range aTask.Properties {
		w.wrkLogger.Logger.Trace().Str("task-prop-name", n).Interface("task-prop-value", v).Msg(semLogContext)
	}
	for n, v := range aTask.Partitions[partitionNumber-1].Properties {
		w.wrkLogger.Logger.Trace().Str("partition-prop-name", n).Interface("partition-prop-value", v).Msg(semLogContext)
	}

	w.wrkLogger.Flush()
	return nil
}
