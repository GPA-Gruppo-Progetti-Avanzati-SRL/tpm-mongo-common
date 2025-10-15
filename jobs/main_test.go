package jobs_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	NumPartitions           = 5
	NumDocumentPerPartition = 20

	jobId  = "my-job-id"
	taskId = jobId + "-t1"

	JobsInstanceId     = "default"
	JobsCollectionId   = "jobs-collection"
	JobsCollectionName = "jobs"

	Host   = "mongodb://localhost:27017"
	DbName = "tpm_mongo_common"

	WithPopulateTasks = true
	WithClearData     = false
)

var cfg = mongolks.Config{
	Name:          "default",
	Host:          Host,
	DbName:        DbName,
	User:          "",
	Pwd:           "",
	AuthMechanism: "",
	AuthSource:    "",
	Pool: mongolks.PoolConfig{
		MinConn: 1,
		MaxConn: 20,
		//MaxWaitQueueSize:      1000,
		ConnectTimeout:        1 * time.Second,
		MaxConnectionIdleTime: 30 * time.Second,
		//MaxConnectionLifeTime: 6000000,
	},
	//BulkWriteOrdered: true,
	WriteConcern: "majority",
	//WriteTimeout: "120s",
	Collections: []mongolks.CollectionCfg{
		{
			Id:   JobsCollectionId,
			Name: JobsCollectionName,
		},
	},
	SecurityProtocol: "PLAIN",
	TLS:              mongolks.TLSConfig{SkipVerify: true},
}

func TestMain(m *testing.M) {

	_, err := mongolks.Initialize([]mongolks.Config{cfg})
	if err != nil {
		panic(err)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.TraceLevel)

	clearJobAndData()

	if WithPopulateTasks {
		err = initJob()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to populate tasks")
		}
	}

	exitVal := m.Run()
	if WithClearData {
		defer clearJobAndData()
	}

	os.Exit(exitVal)
}

func initJob() error {
	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		return err
	}

	_, err = populateJob(taskColl, jobId, taskId)
	if err != nil {
		return err
	}

	_, err = populateTask(taskColl, jobId, taskId)
	if err != nil {
		return err
	}

	return nil
}

func populateJob(taskColl *mongo.Collection, aJobId string, aTaskId string) (job.Job, error) {

	aJob := job.Job{
		Bid:    aJobId,
		Et:     job.EType,
		Ambit:  "test-mongo-common",
		Status: job.StatusAvailable,
		Tasks: []beans.TaskReference{
			{
				Id:     aTaskId,
				Status: task.StatusAvailable,
			},
		},
	}

	insertResp, err := taskColl.InsertOne(context.Background(), aJob)
	if err != nil {
		return job.Job{}, err
	}

	log.Info().Interface("resp", insertResp).Msgf("inserted job")
	return aJob, err
}

func populateTask(taskColl *mongo.Collection, aJobId string, aTaskId string) (task.Task, error) {

	aTask := task.Task{
		Bid:    aTaskId,
		Et:     task.EType,
		JobId:  aJobId,
		Ambit:  "test-mongo-common",
		Status: task.StatusAvailable,
		Properties: bson.M{
			"task-property-1": "task-property-1-value",
		},
	}

	for j := 1; j <= NumPartitions; j++ {
		aTask.Partitions = append(aTask.Partitions,
			beans.Partition{
				Bid:             beans.PartitionId(aTaskId, int32(j)),
				Gid:             "",
				Et:              beans.PartitionEType,
				PartitionNumber: int32(j),
				Status:          beans.PartitionStatusAvailable,
				Etag:            0,
				Properties: bson.M{
					fmt.Sprintf("prt-%d-property-1", j): fmt.Sprintf("prt-%d-property-1-value", j),
				},
			},
		)
	}

	insertResp, err := taskColl.InsertOne(context.Background(), aTask)
	if err != nil {
		return task.Task{}, err
	}

	log.Info().Interface("resp", insertResp).Msgf("inserted task")
	return aTask, err
}

func clearJobAndData() {
	const semLogContext = "main::clear-task-and-data"

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

	err = clearTask(taskColl, jobId, taskId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

	err = clearJob(taskColl, jobId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

}

func clearJob(taskColl *mongo.Collection, aJobId string) error {
	filter := bson.M{
		"_bid": aJobId,
		"_et":  job.EType,
	}

	resp, err := taskColl.DeleteOne(context.Background(), filter)
	if err != nil {
		return err
	}
	log.Info().Interface("resp", resp).Msgf("deleted job")

	return nil
}

func clearTask(taskColl *mongo.Collection, aJobId string, aTaskId string) error {

	filter := bson.M{
		"_bid":   aTaskId,
		"_et":    task.EType,
		"job_id": aJobId,
	}

	resp, err := taskColl.DeleteOne(context.Background(), filter)
	if err != nil {
		return err
	}
	log.Info().Interface("resp", resp).Msgf("deleted task")

	filter = bson.M{
		"_et": lease.EntityType,
	}

	resp, err = taskColl.DeleteMany(context.Background(), filter, nil)
	if err != nil {
		return err
	}
	log.Info().Interface("resp", resp).Msgf("deleted lease")

	return nil
}
