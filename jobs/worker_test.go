package jobs_test

import (
	"context"
	"sync"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/tasklog"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
)

func TestCaseInitialization(t *testing.T) {

}

func TestNewWorker(t *testing.T) {

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	jobs, err := job.FindJobsByGroupAndStatus(taskColl, []string{job.GroupAny}, job.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(jobs) > 0 }, "expected jobs to be available")

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobs[0].Bid, task.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(tasks) > 0 }, "expected tasks to be available")

	var wg sync.WaitGroup
	wrk, err := worker.NewNoOpWorker(tasks[0],
		worker.WithTaskStoreReference(mongolks.StoreReference{
			InstanceName: JobsInstanceId,
			CollectionId: JobsCollectionId,
		}),
		worker.WithTaskLogStoreReference(mongolks.StoreReference{
			InstanceName: JobsInstanceId,
			CollectionId: JobsLogsCollectionId,
		}),
		worker.WithWaitGroup(&wg),
	)
	require.NoError(t, err)

	err = wrk.Start()
	require.NoError(t, err)

	/*
		time.Sleep(60 * time.Second)
		wrk.Close()
	*/
	log.Info().Msg("waiting for completion")
	wg.Wait()
	log.Info().Msg("terminated")
}

func TestScheduler(t *testing.T) {

	cfg := driver.Config{
		ExitOnIdle:             true,
		ExitAfterMaxIterations: 1,
		JobsStore: mongolks.StoreReference{
			InstanceName: JobsInstanceId,
			CollectionId: JobsCollectionId,
		},
		JobsLogsStore: mongolks.StoreReference{
			InstanceName: JobsInstanceId,
			CollectionId: JobsLogsCollectionId,
		},
	}

	var wg sync.WaitGroup

	worker.RegisterFactory(worker.NoOPWorkerName, worker.NewNoOpWorker)

	/*
		factory := func(task task.Task, opts ...worker.Option) (worker.Worker, error) {
			return worker.NewNoOpWorker(task, opts...)
		}
	*/

	m, err := driver.NewDriver(&cfg, &wg)
	require.NoError(t, err)

	err = m.Start(nil)
	require.NoError(t, err)

	wg.Wait()
	log.Info().Msg("waiting for completion")
}

func TestParseLogEntry(t *testing.T) {
	s := "2025-10-19T21:07:56+02:00 INF no-op-partition-worker::work job-id=my-job-id partition=4 tsk-id=my-job-id-t1 tsk-name=snoop"

	_, _, err := tasklog.ParseLogLine(s)
	require.NoError(t, err)
}
