package driver

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
)

type Driver struct {
	cfg *Config

	numIterations int
	jobsColl      *mongo.Collection
	wg            *sync.WaitGroup
	workersWg     *sync.WaitGroup
	workersDone   chan struct{}
	quitc         chan struct{}

	shutdownChannel chan error // channel used to quit the application. triggered when driver wants to exit.
}

func NewDriver(cfg *Config, wg *sync.WaitGroup) (*Driver, error) {

	const semLogContext = "driver::new"

	coll, err := mongolks.GetCollection(context.Background(), cfg.Store.InstanceName, cfg.Store.CollectionId)
	if err != nil {
		log.Error().Err(err).Str("store", cfg.Store.InstanceName).Str("collection-id", cfg.Store.CollectionId).Msg("Failed to get collection")
		return nil, err
	}

	m := &Driver{
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

func (m *Driver) Start(sh chan error) error {
	const semLogContext = "monitor::start"
	m.shutdownChannel = sh
	m.wg.Add(1)
	go m.workLoop()
	return nil
}

func (m *Driver) Close() error {
	const semLogContext = "monitor::close"
	close(m.quitc)
	return nil
}

func (m *Driver) workLoop() {
	const semLogContext = "monitor::work-loop"

	startedTasks := m.findAndStartTasks()
	hasTasks := len(startedTasks) > 0
	log.Info().Int("num-started-tasks", len(startedTasks)).Msg(semLogContext)

	if !hasTasks && m.cfg.ExitOnIdle {
		log.Info().Msg(semLogContext + " no tasks available... exiting")
		m.wg.Done()
		if m.shutdownChannel != nil {
			m.shutdownChannel <- fmt.Errorf("exiting from driver workloop")
		}
		return
	}

	m.numIterations++
	ticker := time.NewTicker(m.cfg.TickInterval)
	log.Info().Float64("tick-interval-ss", m.cfg.TickInterval.Seconds()).Msg(semLogContext + " starting scheduler loop")

	// Use select to wait on the done channel OR a timeout
	var terminate bool
	for !terminate {
		select {
		case <-ticker.C:
			log.Info().Msg(semLogContext + " tick")
			if !hasTasks && m.cfg.ExitOnIdle {
				log.Info().Msg(semLogContext + " no tasks available... exiting")
				terminate = true
			}

		case <-m.workersDone:
			log.Info().Msg(semLogContext + " - tasks ended normally.")
			// In here should decide what to do...
			err := m.onWorkersDone(startedTasks)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				terminate = true
			}

			if m.cfg.ExitAfterMaxIterations > 0 && m.numIterations == m.cfg.ExitAfterMaxIterations {
				log.Info().Msg(semLogContext + " max-iterations reached... exiting")
				terminate = true
			} else {
				startedTasks = m.findAndStartTasks()
				hasTasks = len(startedTasks) > 0
				log.Info().Int("num-started-tasks", len(startedTasks)).Msg(semLogContext)

				if !hasTasks && m.cfg.ExitOnIdle {
					log.Info().Msg(semLogContext + " no tasks available... exiting")
					terminate = true
				}
			}
		case <-m.quitc:
			log.Info().Msg(semLogContext + " quit")
			terminate = true
		}
	}

	if terminate && m.shutdownChannel != nil {
		m.shutdownChannel <- fmt.Errorf("exiting from driver workloop")
	}

	m.wg.Done()
	if m.workersDone != nil {
		close(m.workersDone)
	}
	log.Info().Msg(semLogContext + " - exiting from scheduler loop")
}

func (m *Driver) findAndStartTasks() []beans.TaskReference {
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

func (m *Driver) FindTasks(jobsColl *mongo.Collection) ([]task.Task, error) {
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

func (m *Driver) startTasks(jobsColl *mongo.Collection, tasks []task.Task, workersConfigs []worker.Config) ([]beans.TaskReference, error) {
	const semLogContext = "monitor::execute-tasks"

	var startedTasks []beans.TaskReference
	for _, tsk := range tasks {

		var prc worker.Config
		var ok bool
		if tsk.ProcessorId == task.ProcessorIdAny {
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
		if m.workersDone == nil {
			m.workersDone = make(chan struct{})
		}
		go func() {
			m.workersWg.Wait()
			m.workersDone <- struct{}{}
		}()
	}

	return startedTasks, nil
}

func (m *Driver) onWorkersDone(tasks []beans.TaskReference) error {
	const semLogContext = "monitor::on-workers-done"

	for _, tsk := range tasks {

		if tsk.StreamType == task.DataStreamTypeInfinite {
			continue
		}

		// issue.... in here a contention skips the job..... that doesn't get updated ...
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

				ndx, err := j.UpdateTaskStatus(m.jobsColl, tsk.Bid, task.StatusDone)
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
					return err
				}

				if ndx == (len(j.Tasks) - 1) {
					err = j.UpdateStatus(m.jobsColl, j.Bid, task.StatusDone)
					if err != nil {
						log.Error().Err(err).Msg(semLogContext)
						return err
					}
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
