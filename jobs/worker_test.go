package jobs_test

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/monitor"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestCaseInitialization(t *testing.T) {

}

func TestNewWorkerMessage(t *testing.T) {

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	jobs, err := job.FindJobsByTypeAndStatus(taskColl, []string{job.TypeAny}, job.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(jobs) > 0 }, "expected jobs to be available")

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobs[0].Bid, task.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(tasks) > 0 }, "expected tasks to be available")

	wrkCfg := worker.Config{
		Name:          "my-worker",
		WorkMode:      worker.WorkModeMsg,
		OnErrorPolicy: worker.OnErrorContinue,
		Consumer: taskconsumer.Config{
			Id:               "my-worker-consumer",
			MetricsGId:       "wrk-consumer-qmetrics",
			OnPartitionError: "",
		},
		MetricsGId:   "wrk-qmetrics",
		TickInterval: 0,
		MaxBatchSize: 0,
		Tracing:      worker.TracingCfg{},
		Processor:    &worker.MessageDummyProcessor{ErrorsStride: WithOnEventErrorsStride},
	}

	var wg sync.WaitGroup
	wrk, err := worker.NewWorker(taskColl, tasks[0], &wrkCfg, &wg)
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

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	cfg := monitor.Config{
		WorkersConfig: []worker.Config{
			{
				Name:          "my-worker",
				WorkMode:      worker.WorkModeMsg,
				OnErrorPolicy: worker.OnErrorContinue,
				Consumer: taskconsumer.Config{
					Id:               "my-worker-consumer",
					MetricsGId:       "wrk-consumer-qmetrics",
					OnPartitionError: "",
				},
				TickInterval: 0,
				MaxBatchSize: 0,
				Tracing:      worker.TracingCfg{},
				Processor:    &worker.MessageDummyProcessor{ErrorsStride: 0},
			},
		},
	}

	var wg sync.WaitGroup
	m, err := monitor.NewMonitor(taskColl, &cfg, &wg)
	require.NoError(t, err)

	err = m.Start()
	require.NoError(t, err)

	wg.Wait()
	log.Info().Msg("waiting for completion")
}

func TestNewWorkerBatch(t *testing.T) {

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobId, task.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(tasks) > 0 }, "expected tasks to be available")

	wrkCfg := worker.Config{
		Name:          "my-worker",
		WorkMode:      worker.WorkModeBatch,
		OnErrorPolicy: worker.OnErrorContinue,
		Consumer: taskconsumer.Config{
			Id:               "my-worker-consumer",
			MetricsGId:       "wrk-consumer-qmetrics",
			OnPartitionError: "",
		},
		MetricsGId:   "wrk-qmetrics",
		TickInterval: 0,
		MaxBatchSize: 0,
		Tracing:      worker.TracingCfg{},
		Processor:    &worker.BatchDummyProcessor{ErrorsStride: WithOnEventErrorsStride},
	}

	var wg sync.WaitGroup
	wrk, err := worker.NewWorker(taskColl, tasks[0], &wrkCfg, &wg)
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
