package duedatetrigger

import (
	"context"
	"sync"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/ddtcheckpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type JobTrigger struct {
	Cfg          *Config
	LastExecDate string

	wg    *sync.WaitGroup
	quitc chan struct{}
}

func NewJobTrigger(cfg *Config, wg *sync.WaitGroup) *JobTrigger {
	const semLogContext = "job-trigger::new"

	jt := &JobTrigger{Cfg: cfg, wg: wg}
	if cfg.Mode == ExecuteLoop {
		if cfg.TickInterval == 0 {
			cfg.TickInterval = time.Minute
		}
		jt.quitc = make(chan struct{})
	}

	if cfg.CheckPointId == "" {
		jt.Cfg.CheckPointId = cfg.Filter.JobGroup
		log.Warn().Str("chk-id", jt.Cfg.CheckPointId).Msg(semLogContext + " - checkpoint Id not initialized using filter ambit, please do")
	}

	return jt
}

func (t *JobTrigger) Start() error {
	const semLogContext = "job-trigger::start"

	ddt, err := t.getStartDueDate(nil)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	err = t.Execute(ddt)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	if t.Cfg.Mode == ExecuteLoop {
		log.Info().Msg(semLogContext)
		t.wg.Add(1)
		go t.workLoop()
	}

	return nil
}

func (t *JobTrigger) Stop() {
	const semLogContext = "job-trigger::stop"
	if t.quitc != nil {
		log.Info().Msg(semLogContext)
		close(t.quitc)
	}
}

func (t *JobTrigger) getStartDueDate(coll *mongo.Collection) (string, error) {
	const semLogContext = "job-trigger::retrieve-due-date-checkpoint"
	var err error
	ddt := t.LastExecDate
	if ddt == "" {
		if coll == nil {
			coll, err = mongolks.GetCollection(context.Background(), t.Cfg.Store.InstanceName, t.Cfg.Store.CollectionId)
			if err != nil {
				log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Msg(semLogContext + " - Failed to get collection")
				return "", err
			}
		}

		ddt, err = ddtcheckpoint.RetrieveDueDate(coll, t.Cfg.CheckPointId, t.Cfg.StartDate)
		if err != nil {
			log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Str("chk-id", t.Cfg.CheckPointId).Msg(semLogContext)
			return "", err
		}
	}

	return ddt, nil
}

func (t *JobTrigger) storeDueDateCheckPoint(coll *mongo.Collection, dueDate string) error {
	const semLogContext = "job-trigger::store-due-date-checkpoint"
	var err error

	if coll == nil {
		coll, err = mongolks.GetCollection(context.Background(), t.Cfg.Store.InstanceName, t.Cfg.Store.CollectionId)
		if err != nil {
			log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Msg(semLogContext + " - Failed to get collection")
			return err
		}
	}

	err = ddtcheckpoint.UpdateDueDate(coll, t.Cfg.CheckPointId, dueDate)
	if err != nil {
		log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Str("chk-id", t.Cfg.CheckPointId).Msg(semLogContext)
		return err
	}

	return nil
}

func (t *JobTrigger) workLoop() {
	const semLogContext = "job-trigger::work-loop"

	log.Info().Float64("tick-interval-ss", t.Cfg.TickInterval.Seconds()).Msg(semLogContext + " starting trigger loop")
	ticker := time.NewTicker(t.Cfg.TickInterval)

	// Use select to wait on the done channel OR a timeout
	var terminate bool
	for !terminate {
		select {
		case <-ticker.C:
			log.Info().Msg(semLogContext + " tick")
			ddt, err := t.getStartDueDate(nil)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}
			err = t.Execute(ddt)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}

		case <-t.quitc:
			log.Info().Msg(semLogContext + " quit")
			terminate = true
		}
	}

	t.wg.Done()
	log.Info().Msg(semLogContext + " - exiting from trigger loop")
}

func (t *JobTrigger) Execute(withStartDueDate string) error {
	const semLogContext = "job-trigger::execute"

	today := time.Now().Format(ddtcheckpoint.DueDateLayout)
	if withStartDueDate > today {
		log.Info().Str("exec-start-date", withStartDueDate).Msg(semLogContext + " - no execution")
		return nil
	}

	/*	if t.LastExecDate == today {
		log.Info().Str("exec-date", t.LastExecDate).Msg(semLogContext + " - date already processed")
		return nil
	}*/

	/*
		var execDate string
		if t.LastExecDate == "" {
			execDate = withStartDueDate
		} else {
			tm, err := time.Parse(ddtcheckpoint.DueDateLayout, t.LastExecDate)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}

			execDate = tm.AddDate(0, 0, 1).Format(ddtcheckpoint.DueDateLayout)
		}
	*/

	f := job.Filter{}
	f.Or().
		AndEtEqTo(job.EType).
		AndStatusEqTo(t.Cfg.Filter.JobStatus).
		AndGroupEqTo(t.Cfg.Filter.JobGroup).
		AndDueDateBetween(withStartDueDate, today)

	log.Info().Str("filter-from-date", withStartDueDate).Str("filter-to", today).Msg(semLogContext)

	coll, err := mongolks.GetCollection(context.Background(), t.Cfg.Store.InstanceName, t.Cfg.Store.CollectionId)
	if err != nil {
		log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Msg(semLogContext + " - Failed to get collection")
		return err
	}

	_, err = job.UpdateManyStatus(coll, &f, t.Cfg.Update.JobStatus)
	if err != nil {
		log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Msg(semLogContext)
		return err
	}

	t.LastExecDate, _ = ddtcheckpoint.IncDueDate(today)
	err = t.storeDueDateCheckPoint(coll, t.LastExecDate)
	if err != nil {
		log.Error().Err(err).Str("store", t.Cfg.Store.InstanceName).Str("collection-id", t.Cfg.Store.CollectionId).Msg(semLogContext)
		return err
	}

	log.Info().Str("next-due-date", t.LastExecDate).Msg(semLogContext)
	return nil
}

/*
func UpdateManyStatus(jobsColl *mongo.Collection, f *job.Filter, st string) error {
	const semLogContext = "job::update-many-status"

	updOpts := job.UpdateOptions{
		job.UpdateWithStatus(st),
	}

	f.Or().AndEtEqTo(job.EType)

	updDoc := job.GetUpdateDocumentFromOptions(updOpts...)
	resp, err := jobsColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)
	return nil
}
*/
