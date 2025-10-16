package driver

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type Driver struct {
	cfg *Config

	workerFactory worker.Factory
	numIterations int
	jobsColl      *mongo.Collection
	wg            *sync.WaitGroup
	workersWg     *sync.WaitGroup
	workersDone   chan struct{}
	quitc         chan struct{}

	shutdownChannel chan error // channel used to quit the application. triggered when driver wants to exit.
}

func NewDriver(cfg *Config, workerFactory worker.Factory, wg *sync.WaitGroup) (*Driver, error) {

	const semLogContext = "driver::new"

	coll, err := mongolks.GetCollection(context.Background(), cfg.Store.InstanceName, cfg.Store.CollectionId)
	if err != nil {
		log.Error().Err(err).Str("store", cfg.Store.InstanceName).Str("collection-id", cfg.Store.CollectionId).Msg("Failed to get collection")
		return nil, err
	}

	m := &Driver{
		cfg:           cfg,
		wg:            wg,
		workerFactory: workerFactory,
		workersWg:     &sync.WaitGroup{},
		jobsColl:      coll,
		quitc:         make(chan struct{}),
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
			if !hasTasks {
				if m.cfg.ExitOnIdle {
					log.Info().Msg(semLogContext + " no tasks available... exiting")
					terminate = true
				}

				startedTasks = m.findAndStartTasks()
				hasTasks = len(startedTasks) > 0
				log.Info().Int("num-started-tasks", len(startedTasks)).Msg(semLogContext)

				if hasTasks {
					m.numIterations++
				}
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

				if hasTasks {
					m.numIterations++
				} else if m.cfg.ExitOnIdle {
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
	const semLogContext = "driver::find-and-start-tasks"

	tasks, err := m.FindTasks(m.jobsColl)
	if err != nil {
		log.Fatal().Err(err).Msg(semLogContext)
		return nil
	}

	var startedTasks []beans.TaskReference
	if len(tasks) > 0 {
		startedTasks, err = m.startTasks(m.jobsColl, tasks, m.workerFactory)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		log.Info().Int("numStarted", len(startedTasks)).Msg(semLogContext)
	}

	return startedTasks
}

func (m *Driver) FindTasks(jobsColl *mongo.Collection) ([]task.Task, error) {
	const semLogContext = "driver::find-tasks"
	var tasks []task.Task

	jobs, err := job.FindJobsByGroupAndStatus(jobsColl, m.cfg.JobTypes, job.StatusAvailable)
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

		if len(tsks) == 0 {
			log.Info().Str("job-id", j.Bid).Msg(semLogContext + " no tasks available")
		} else {
			tasks = append(tasks, tsks...)
		}
	}

	return tasks, nil
}

func (m *Driver) startTasks(coll *mongo.Collection, tasks []task.Task, workerFactory worker.Factory) ([]beans.TaskReference, error) {
	const semLogContext = "driver::execute-tasks"

	var startedTasks []beans.TaskReference
	for _, tsk := range tasks {

		wrk, err := workerFactory(coll, tsk, m.workersWg)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			// keep processing others....
			continue
		}

		err = wrk.Start()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			// keep processing others....
			continue
		}

		startedTasks = append(startedTasks, beans.TaskReference{
			Id:     tsk.Bid,
			JobId:  tsk.JobId,
			Status: tsk.Status,
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
	const semLogContext = "driver::on-workers-done"

	for _, tsk := range tasks {

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
