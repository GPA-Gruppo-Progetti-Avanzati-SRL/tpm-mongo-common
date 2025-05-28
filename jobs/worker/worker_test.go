package worker_test

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer/datasource"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"sync"
	"testing"
)

func TestNewWorkerMessage(t *testing.T) {

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobId, task.StatusAvailable)
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
	}

	var wg sync.WaitGroup
	wrk, err := worker.NewWorker(taskColl, tasks[0], &wrkCfg, &wg, &MessageWorkerListener{})
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

type MessageWorkerListener struct {
	worker.UnimplementedProcessor

	numEvts int
}

func (w *MessageWorkerListener) OnEvent(evt datasource.Event) (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-event"
	log.Info().Msg(semLogContext)
	w.numEvts++
	if w.numEvts == 14 {
		err := errors.New("error condition materialized")
		log.Error().Err(err).Msg(semLogContext)
		return worker.OnEventResponseUndefined, err
	}

	return worker.OnEventResponseProcessed, nil
}

func (w *MessageWorkerListener) OnTickEvent() (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-tick-event"
	log.Info().Msg(semLogContext)
	return worker.OnEventResponseSkipped, nil
}

func (w *MessageWorkerListener) OnEofEvent() (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-eof-event"
	log.Info().Msg(semLogContext)
	return worker.OnEventResponseSkipped, nil
}

func (w *MessageWorkerListener) OnEofPartitionEvent() (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-eof-partition-event"
	log.Info().Msg(semLogContext)
	return worker.OnEventResponseSkipped, nil
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
	}

	var wg sync.WaitGroup
	wrk, err := worker.NewWorker(taskColl, tasks[0], &wrkCfg, &wg, &BatchWorkerListener{})
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

type BatchWorkerListener struct {
	worker.UnimplementedProcessor
	numEvts int
}

func (w *BatchWorkerListener) OnEvent(evt datasource.Event) (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-event"
	panic(semLogContext + " - implement me")
}

func (w *BatchWorkerListener) OnTickEvent() (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-tick-event"
	log.Info().Msg(semLogContext)
	return worker.OnEventResponseSkipped, nil
}

func (w *BatchWorkerListener) OnEofEvent() (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-eof-event"
	log.Info().Msg(semLogContext)
	return worker.OnEventResponseSkipped, nil
}

func (w *BatchWorkerListener) OnEofPartitionEvent() (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-eof-partition-event"
	log.Info().Msg(semLogContext)
	return worker.OnEventResponseSkipped, nil
}

func (w *BatchWorkerListener) OnEvents(evt []datasource.Event) (worker.OnEventResponseStatus, error) {
	const semLogContext = "worker-processor::on-events"
	log.Info().Msg(semLogContext)
	w.numEvts += len(evt)
	if w.numEvts > 14 {
		return worker.OnEventResponseUndefined, errors.New("error condition materialized")
	}

	return worker.OnEventResponseProcessed, nil
}
