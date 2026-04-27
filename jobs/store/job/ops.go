package job

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func FindById(coll *mongo.Collection, jobId string) (Job, error) {
	const semLogContext = "job::find-by-id"

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(jobId)
	opts := options.FindOne()

	var job Job
	err := coll.FindOne(context.Background(), f.Build(), opts).Decode(&job)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return Job{}, err
	}

	return job, nil
}

func FindJobsByGroupAndStatus(coll *mongo.Collection, groups []string, status ...string) ([]Job, error) {
	const semLogContext = "job::find-all-by-type-and-status"

	f := Filter{}
	ca := f.Or().AndEtEqTo(EType)
	switch len(status) {
	case 0:
		return nil, fmt.Errorf("at least one status must be provided")
	case 1:
		ca.AndStatusEqTo(status[0])
	default:
		ca.AndStatusIn(status)
	}

	if len(groups) > 1 || len(groups) == 1 && groups[0] != GroupAny {
		log.Info().Str("types", strings.Join(groups, ",")).Msg(semLogContext + " - filtering by types")
		ca.AndGroupIn(groups)
	} else {
		log.Info().Str("types", strings.Join(groups, ",")).Msg(semLogContext + " - accepting all job types")
	}

	opts := options.Find()

	crs, err := coll.Find(context.Background(), f.Build(), opts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var jobs []Job
	err = crs.All(context.Background(), &jobs)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	log.Info().Int("num-jobs", len(jobs)).Msg(semLogContext)
	return jobs, nil
}

func (j Job) GetTasks(coll *mongo.Collection) ([]task.Task, error) {

	const semLogContext = "job::get-tasks"

	var tsks []task.Task
	for _, refTsk := range j.Tasks {

		if refTsk.Status == task.StatusAvailable || refTsk.Status == task.StatusRetry {
			tsk, err := task.FindById(coll, refTsk.Id)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return nil, err
			}

			tsks = append(tsks, tsk)
			return tsks, nil
		}
	}

	return tsks, nil
}

func (j Job) UpdateStatus(jobsColl *mongo.Collection, jobId string, st string) error {
	const semLogContext = "job::update-status"

	updOpts := UpdateOptions{
		UpdateWithStatus(st),
		UpdateWithModifiedAt(bson.NewDateTimeFromTime(time.Now()), st),
	}

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(jobId)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := jobsColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)
	return nil
}

func UpdateManyStatus(jobsColl *mongo.Collection, f *Filter, st string) (int64, error) {
	const semLogContext = "job::update-many-status"

	updOpts := UpdateOptions{
		UpdateWithStatus(st),
		UpdateWithModifiedAt(bson.NewDateTimeFromTime(time.Now()), st),
	}

	filterDocument := f.Build()
	log.Info().Str("filter", util.MustToExtendedJsonString(filterDocument, false, false)).Msg(semLogContext)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := jobsColl.UpdateMany(context.Background(), filterDocument, updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return -1, err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)
	return resp.ModifiedCount, nil
}

func (j Job) UpdateTaskStatus(taskColl *mongo.Collection, taskId string, st string) (int, error) {
	const semLogContext = semLogContextPackage + "update-task-status"

	taskNdx := -1
	for i, t := range j.Tasks {
		if t.Id == taskId {
			taskNdx = i
			break
		}
	}

	if taskNdx == -1 {
		err := errors.New("task not found")
		log.Error().Err(err).Msg(semLogContext)
		return taskNdx, err
	}
	updOpts := UpdateOptions{
		UpdateWithTaskStatus(int32(taskNdx), st),
	}

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(j.Bid)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := taskColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return taskNdx, err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)
	return taskNdx, nil
}

func (j Job) Restart() error {
	const semLogContext = semLogContextPackage + "restart"

	lks, err := mongolks.GetLinkedService(context.Background(), mongolks.MongoDbDefaultInstanceName)
	if err != nil {
		return err
	}

	jobCollection := lks.GetCollection(CollectionId, "")
	if jobCollection == nil {
		err = errors.New("cannot find collection by id")
		return err
	}

	taskCollection := lks.GetCollection(task.CollectionId, "")
	if taskCollection == nil {
		err = errors.New("cannot find collection by id")
		return err
	}

	for t := 0; t < len(j.Tasks); t++ {
		// Status of the task as in the job.
		j.Tasks[t].Status = task.StatusAvailable

		tskRef := j.Tasks[t]
		tsk, _, err := task.FindByPk(taskCollection, j.Domain, j.Site, tskRef.JobId, tskRef.Id, true, options.FindOne())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		for p := 0; p < len(tsk.Partitions); p++ {
			tsk.Partitions[p].Status = task.StatusAvailable
		}

		tsk.Status = task.StatusAvailable
		tskUpdate := task.GetUpdateDocumentFromOptions(
			task.UpdateWithPartitions(tsk.Partitions),
			task.UpdateWithStatus(tsk.Status),
		)
		tUd := tskUpdate.Build()
		tFilter := task.Filter{}
		tFilter.Or().AndDomainEqTo(j.Domain).AndSiteEqTo(j.Site).AndEtEqTo(task.EType).AndJobIdEqTo(tskRef.JobId).AndBidEqTo(tskRef.Id)
		tFd := tFilter.Build()

		log.Info().
			Str("update-filter", util.MustToExtendedJsonString(tFd, false, false)).
			Str("update-document", util.MustToExtendedJsonString(tUd, false, false)).
			Msg(semLogContext)

		resp, err := taskCollection.UpdateOne(context.Background(), tFd, tUd, options.UpdateOne())
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		if resp.MatchedCount == 0 {
			err = errors.New("no matched documents")
			log.Error().Err(err).Msg(semLogContext)
			return err
		}
	}

	j.Status = StatusAvailable
	jobUpdate := GetUpdateDocumentFromOptions(
		UpdateWithTasks(j.Tasks),
		UpdateWithStatus(j.Status),
	)
	jUd := jobUpdate.Build()
	jFilter := Filter{}
	jFilter.Or().AndDomainEqTo(j.Domain).AndSiteEqTo(j.Site).AndEtEqTo(EType).AndBidEqTo(j.Bid)
	jFd := jFilter.Build()

	log.Info().
		Str("update-filter", util.MustToExtendedJsonString(jFd, false, false)).
		Str("update-document", util.MustToExtendedJsonString(jUd, false, false)).
		Msg(semLogContext)

	jResp, err := jobCollection.UpdateOne(context.Background(), jFd, jUd, options.UpdateOne())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	if jResp.MatchedCount == 0 {
		err = errors.New("no matched documents")
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}
