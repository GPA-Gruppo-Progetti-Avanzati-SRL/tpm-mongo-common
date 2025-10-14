package jobs_test

import (
	"context"
	"sync"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

func TestCaseInitialization(t *testing.T) {

}

func TestNewWorker(t *testing.T) {

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	jobs, err := job.FindJobsByAmbitAndStatus(taskColl, []string{job.AmbitAny}, job.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(jobs) > 0 }, "expected jobs to be available")

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobs[0].Bid, task.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(tasks) > 0 }, "expected tasks to be available")

	var wg sync.WaitGroup
	wrk, err := worker.NewNoOpWorker(taskColl, tasks[0], &wg)
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
		Store: driver.StoreReference{
			InstanceName: JobsInstanceId,
			CollectionId: JobsCollectionId,
		},
	}

	var wg sync.WaitGroup
	factory := func(taskColl *mongo.Collection, task task.Task, wg *sync.WaitGroup) (worker.Worker, error) {
		return worker.NewNoOpWorker(taskColl, task, wg)
	}

	m, err := driver.NewDriver(&cfg, factory, &wg)
	require.NoError(t, err)

	err = m.Start(nil)
	require.NoError(t, err)

	wg.Wait()
	log.Info().Msg("waiting for completion")
}
