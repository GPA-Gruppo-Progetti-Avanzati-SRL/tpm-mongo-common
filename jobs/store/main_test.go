package store_test

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"

	"go.mongodb.org/mongo-driver/v2/bson"

	"os"
	"testing"
)

const (
	JobsInstanceId     = "default"
	JobsCollectionId   = "jobs-collection"
	JobsCollectionName = "jobs"

	QueryInstanceId     = "default"
	QueryCollectionId   = "query-collection"
	QueryCollectionName = "tpm_mongo_common"

	Host   = "mongodb://localhost:27017"
	DbName = "tpm_morphia"

	jobBid        = "hello-job"
	numPartitions = 5
	clearOnEnd    = false
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
		ConnectTimeout:        1000,
		MaxConnectionIdleTime: 30000,
		//MaxConnectionLifeTime: 6000000,
	},
	//BulkWriteOrdered: true,
	WriteConcern: "majority",
	//WriteTimeout:     "120s",
	Collections: []mongolks.CollectionCfg{
		{
			Id:   QueryCollectionId,
			Name: QueryCollectionName,
		},
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
	panicOnError(err)

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	err = populateJobs()
	panicOnError(err)

	err = populateTasks(jobBid, 1)
	panicOnError(err)

	exitVal := m.Run()

	if clearOnEnd {
		err = clear()
		panicOnError(err)
	}
	os.Exit(exitVal)
}

func panicOnError(err error) {
	if err != nil {
		panic(err)
	}
}

func populateJobs() error {
	const semLogContext = "populate-jobs"
	coll, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get jobs collection")
		return err
	}

	aJob := job.Job{
		Bid:    jobBid,
		Et:     job.EType,
		Status: job.StatusAvailable,
		Ambit:  job.AmbitAny,
		Info:   beans.JobInfo{},
		Tasks: []beans.TaskReference{
			{
				Id:             "hello-job-t1",
				Status:         task.StatusAvailable,
				DataSourceType: task.TypeQMongo,
			},
		},
	}

	insertResp, err := coll.InsertOne(context.Background(), aJob)
	if err != nil {
		log.Error().Err(err).Msg("Failed to get jobs collection")
		return err
	}

	log.Info().Interface("insertResp", insertResp).Msg(semLogContext)
	return nil
}

func populateTasks(jobId string, numTasks int) error {
	const semLogContext = "populate-tasks"

	coll, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	for i := 1; i <= numTasks; i++ {
		taskId := fmt.Sprintf("%s-t%d", jobId, i)
		aTask := task.Task{
			Bid:            taskId,
			Et:             task.EType,
			JobId:          jobBid,
			Status:         task.StatusAvailable,
			DataSourceType: task.TypeQMongo,
			Info: beans.TaskInfo{
				MdbInstance:   QueryInstanceId,
				MdbCollection: QueryCollectionId,
			},
		}

		for j := 1; j <= numPartitions; j++ {
			aTask.Partitions = append(aTask.Partitions,
				partition.Partition{
					Bid:             partition.Id(taskId, int32(j)),
					Gid:             "",
					Et:              partition.EType,
					PartitionNumber: int32(j),
					Status:          partition.StatusAvailable,
					Etag:            0,
					Info: beans.PartitionInfo{
						MdbFilter: fmt.Sprintf(`{ "$and": [ {"%s": %d }, { "_id": { "$gt": { "$oid": "{resumeObjectId}" } } }  ] }`, partition.QueryDocumentPartitionFieldName, j),
					},
				},
			)
		}

		insertResp, err := coll.InsertOne(context.Background(), aTask)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		log.Info().Interface("insertResp", insertResp).Msg(semLogContext)
	}

	return nil
}

func clear() error {
	const semLogContext = "clear"

	coll, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	f := bson.D{
		{
			Key: "_et", Value: task.EType,
		},
		{
			Key: "jobBid", Value: jobBid,
		},
	}
	deleteResp, err := coll.DeleteMany(context.Background(), f)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}
	log.Info().Interface("resp", deleteResp).Msg(semLogContext + " - task deleted")

	f = bson.D{
		{
			Key: "_et", Value: job.EType,
		},
		{
			Key: "_bid", Value: jobBid,
		},
	}

	deleteResp, err = coll.DeleteMany(context.Background(), f)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}
	log.Info().Interface("resp", deleteResp).Msg(semLogContext + " - jobs deleted")

	return nil
}

/*
func populatePartitionedDocuments(t *testing.T, coll *mongo.Collection) {
	// last partition is left empty
	for i := 1; i <= NumPartitions-1; i++ {
		doc := bson.M{
			partition.QueryDocumentPartitionFieldName: int32(i),
			"name": fmt.Sprintf("name-%d", i),
		}

		insertResp, err := coll.InsertOne(context.Background(), doc)
		require.NoError(t, err)
		t.Log(insertResp)
	}
}

func clearPartitionedDocuments(t *testing.T, coll *mongo.Collection) {
	for i := 1; i <= NumPartitions; i++ {
		filter := bson.M{
			"_np": int32(i),
		}
		resp, err := coll.DeleteMany(context.Background(), filter)
		require.NoError(t, err)
		t.Log(resp)
	}

}
*/
