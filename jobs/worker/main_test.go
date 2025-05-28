package worker_test

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"testing"
)

const (
	NumPartitions           = 5
	NumDocumentPerPartition = 10

	jobId  = "my-job-id"
	taskId = "jobId-t1"

	QueryInstanceId     = "default"
	QueryCollectionId   = "query-collection"
	QueryCollectionName = "task_query_docs"

	JobsInstanceId     = "default"
	JobsCollectionId   = "jobs-collection"
	JobsCollectionName = "jobs"

	Host   = "mongodb://localhost:27017"
	DbName = "tpm_morphia"

	WithPopulateTasks = true
	WithPopulateData  = false
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
		MinConn:               1,
		MaxConn:               20,
		MaxWaitQueueSize:      1000,
		MaxWaitTime:           1000,
		MaxConnectionIdleTime: 30000,
		MaxConnectionLifeTime: 6000000,
	},
	BulkWriteOrdered: true,
	WriteConcern:     "majority",
	WriteTimeout:     "120s",
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
	if err != nil {
		panic(err)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	if WithPopulateTasks {
		err = populateTasks()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to populate tasks")
		}
	}

	if WithPopulateData {
		err = populateData()
		if err != nil {
			log.Fatal().Err(err).Msg("Failed to populate data")
		}
	}

	exitVal := m.Run()
	if WithClearData {
		defer clearTaskAndData()
	}

	os.Exit(exitVal)
}

func populatePartitionedDocuments(coll *mongo.Collection) error {

	// it populates all partitions but one.
	for i := 1; i <= NumPartitions-1; i++ {
		for j := 1; j <= NumDocumentPerPartition; j++ {
			doc := bson.M{
				partition.QueryDocumentPartitionFieldName: int32(i),
				"name": fmt.Sprintf("name-%d-%d", i, j),
			}

			insertResp, err := coll.InsertOne(context.Background(), doc)
			if err != nil {
				return err
			}
			log.Info().Interface("resp", insertResp).Msgf("inserted documents")
		}
	}
	return nil
}

func populateTasks() error {
	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		return err
	}

	_, err = populateTask(taskColl)
	if err != nil {
		return err
	}

	return nil
}

func populateData() error {
	qcoll, err := mongolks.GetCollection(context.Background(), QueryInstanceId, QueryCollectionId)
	if err != nil {
		return err
	}

	err = populatePartitionedDocuments(qcoll)
	if err != nil {
		return err
	}

	return nil
}

func clearPartitionedDocuments(coll *mongo.Collection) error {
	log.Info().Msg("clear partitioned documents")
	for i := 1; i <= NumPartitions; i++ {
		filter := bson.M{
			"_np": int32(i),
		}
		resp, err := coll.DeleteMany(context.Background(), filter)
		if err != nil {
			log.Error().Err(err).Msg("error deleting partitioned documents")
			return err
		}
		log.Info().Interface("resp", resp).Msgf("deleted documents")
	}
	return nil
}

func clearTaskAndData() {
	const semLogContext = "main::clear-task-amd-data"
	qcoll, err := mongolks.GetCollection(context.Background(), QueryInstanceId, QueryCollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

	err = clearPartitionedDocuments(qcoll)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

	err = clearTask(taskColl)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return
	}

}

func populateTask(taskColl *mongo.Collection) (task.Task, error) {

	aTask := task.Task{
		Bid:            taskId,
		Et:             task.EType,
		JobBid:         jobId,
		Status:         task.StatusAvailable,
		Typ:            task.TypeQMongo,
		DataStreamType: task.DataStreamTypeInfinite,
		Info: beans.TaskInfo{
			MdbInstance:   JobsInstanceId,
			MdbCollection: QueryCollectionId,
		},
	}

	for j := 1; j <= NumPartitions; j++ {
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

	insertResp, err := taskColl.InsertOne(context.Background(), aTask)
	if err != nil {
		return task.Task{}, err
	}

	log.Info().Interface("resp", insertResp).Msgf("inserted documents")
	return aTask, err
}

func clearTask(taskColl *mongo.Collection) error {
	filter := bson.M{
		"_bid": taskId,
		"_et":  task.EType,
	}

	resp, err := taskColl.DeleteOne(context.Background(), filter)
	if err != nil {
		return err
	}
	log.Info().Interface("resp", resp).Msgf("deleted documents")

	return nil
}
