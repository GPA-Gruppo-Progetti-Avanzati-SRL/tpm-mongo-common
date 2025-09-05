package job

import (
	"context"
	"errors"
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FindById(coll *mongo.Collection, jobId string) (Job, error) {
	const semLogContext = "job::find-by-id"

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(jobId)
	opts := options.FindOneOptions{}

	var job Job
	err := coll.FindOne(context.Background(), f.Build(), &opts).Decode(&job)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return Job{}, err
	}

	return job, nil
}

func FindJobsByAmbitAndStatus(coll *mongo.Collection, ambits []string, status string) ([]Job, error) {
	const semLogContext = "job::find-all-by-type-and-status"

	f := Filter{}
	ca := f.Or().AndEtEqTo(EType).AndStatusEqTo(status)
	if len(ambits) > 1 || len(ambits) == 1 && ambits[0] != AmbitAny {
		log.Info().Str("types", strings.Join(ambits, ",")).Msg(semLogContext + " - filtering by types")
		ca.AndAmbitIn(ambits)
	} else {
		log.Info().Str("types", strings.Join(ambits, ",")).Msg(semLogContext + " - accepting all job types")
	}

	opts := options.FindOptions{}

	crs, err := coll.Find(context.Background(), f.Build(), &opts)
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
		if refTsk.DataSourceType != task.TypeQMongo {
			return tsks, nil
		}

		if refTsk.Status == task.StatusAvailable {
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
	}

	filterDocument := f.Build()
	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := jobsColl.UpdateMany(context.Background(), filterDocument, updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return -1, err
	}

	log.Info().Str("filter", util.MustToExtendedJsonString(filterDocument, false, false)).Interface("resp", resp).Msg(semLogContext)
	return resp.ModifiedCount, nil
}

func (j Job) UpdateTaskStatus(taskColl *mongo.Collection, taskId string, st string) (int, error) {
	const semLogContext = "job::update-task-status"

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
