package monitor

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
)

type Monitor struct {
	cfg *Config

	jobsColl    *mongo.Collection
	wg          *sync.WaitGroup
	workersWg   *sync.WaitGroup
	workersDone chan struct{}
	quitc       chan struct{}
}

func NewMonitor(coll *mongo.Collection, cfg *Config, wg *sync.WaitGroup) (*Monitor, error) {
	m := &Monitor{
		cfg:       cfg,
		wg:        wg,
		workersWg: &sync.WaitGroup{},
		jobsColl:  coll,
		quitc:     make(chan struct{}),
	}

	if len(cfg.WorkersConfig) == 0 {
		return nil, errors.New("must specify at least one worker configuration")
	}

	if cfg.TickInterval == 0 {
		cfg.TickInterval = time.Second
	}
	return m, nil
}

func (m *Monitor) Start() error {
	const semLogContext = "monitor::start"
	m.wg.Add(1)
	go m.workLoop()
	return nil
}

func (m *Monitor) Close() error {
	const semLogContext = "monitor::close"
	close(m.quitc)
	return nil
}

func (m *Monitor) workLoop() {
	const semLogContext = "monitor::work-loop"

	startedTasks := m.findAndStartTasks()
	log.Info().Int("num-started-tasks", len(startedTasks)).Msg(semLogContext)

	ticker := time.NewTicker(m.cfg.TickInterval)
	log.Info().Float64("tick-interval-ss", m.cfg.TickInterval.Seconds()).Msg(semLogContext + " starting scheduler loop")

	// Use select to wait on the done channel OR a timeout
	var terminate bool
	for !terminate {
		select {
		case <-ticker.C:
			log.Info().Msg(semLogContext + " tick")
		case <-m.workersDone:
			log.Info().Msg(semLogContext + " - tasks ended normally.")
			// In here should decide what to do...
			m.updateTasksStatus(startedTasks)
			terminate = true
		case <-m.quitc:
			log.Info().Msg(semLogContext + " quit")
			terminate = true
		}
	}

	m.wg.Done()
	log.Info().Msg(semLogContext + " - exiting from scheduler loop")
}

func (m *Monitor) findAndStartTasks() []beans.TaskReference {
	const semLogContext = "monitor::find-and-start-tasks"

	tasks, err := m.FindTasks(m.jobsColl)
	if err != nil {
		log.Fatal().Err(err).Msg(semLogContext)
		return nil
	}

	var startedTasks []beans.TaskReference
	if len(tasks) > 0 {
		startedTasks, err = m.startTasks(m.jobsColl, tasks, m.cfg.WorkersConfig)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		log.Info().Int("numStarted", len(startedTasks)).Msg(semLogContext)
	}

	return startedTasks
}

func (m *Monitor) FindTasks(jobsColl *mongo.Collection) ([]task.Task, error) {
	const semLogContext = "monitor::find-tasks"
	var tasks []task.Task

	jobs, err := job.FindJobsByTypeAndStatus(jobsColl, m.cfg.JobTypes, job.StatusAvailable)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	for _, j := range jobs {
		tsks, err := j.GetTasks(jobsColl)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			continue
		}

		tasks = append(tasks, tsks...)
	}

	return tasks, nil
}

func (m *Monitor) startTasks(jobsColl *mongo.Collection, tasks []task.Task, workersConfigs []worker.Config) ([]beans.TaskReference, error) {
	const semLogContext = "monitor::execute-tasks"

	var startedTasks []beans.TaskReference
	for _, tsk := range tasks {

		var prc worker.Config
		var ok bool
		if tsk.ProcessorId == task.TypeAny {
			prc = workersConfigs[0]
			ok = true
		} else {
			for _, wc := range workersConfigs {
				if tsk.ProcessorId == wc.Name {
					prc = wc
					ok = true
					break
				}
			}
		}

		if !ok {
			err := errors.New("processor not found in processorGroup")
			log.Error().Err(err).Str("processor-id", tsk.ProcessorId).Msg(semLogContext)
			// keep processing others....
			continue
		}

		wrk, err := worker.NewWorker(jobsColl, tsk, &prc, m.workersWg)
		if err != nil {
			log.Error().Err(err).Str("processor-id", tsk.ProcessorId).Msg(semLogContext)
			// keep processing others....
			continue
		}

		err = wrk.Start()
		if err != nil {
			log.Error().Err(err).Str("processor-id", tsk.ProcessorId).Msg(semLogContext)
			// keep processing others....
			continue
		}

		startedTasks = append(startedTasks, beans.TaskReference{
			Id:             tsk.Bid,
			JobId:          tsk.JobId,
			Status:         tsk.Status,
			DataSourceType: tsk.DataSourceType,
			StreamType:     tsk.StreamType,
		})
	}

	if len(startedTasks) > 0 {
		m.workersDone = make(chan struct{})
		go func() {
			m.workersWg.Wait()
			close(m.workersDone)
		}()
	}

	return startedTasks, nil
}

func (m *Monitor) updateTasksStatus(tasks []beans.TaskReference) error {
	const semLogContext = "monitor::update-tasks-status"

	for _, tsk := range tasks {

		if tsk.StreamType == task.DataStreamTypeInfinite {
			continue
		}

		lh, ok, err := lease.AcquireLease(m.jobsColl, "all", tsk.JobId, true)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		if ok {
			j, err := job.FindById(m.jobsColl, tsk.JobId)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}

			tsk, err := task.FindById(m.jobsColl, tsk.Id)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}
			if tsk.IsEOF() {
				err = tsk.UpdateStatus(m.jobsColl, tsk.Bid, task.StatusDone)
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
					return err
				}

				err = j.UpdateTaskStatus(m.jobsColl, tsk.Bid, task.StatusDone)
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
					return err
				}
			}

			err = lh.Release()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}
		} else {
			// TODO gestire una contention
		}
	}

	return nil
}
