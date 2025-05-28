package taskconsumer_test

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"testing"
)

const (
	QueryInstanceId     = "default"
	QueryCollectionId   = "query-collection"
	QueryCollectionName = "task_query_docs"
	JobsInstanceId      = "default"
	JobsCollectionId    = "jobs-collection"
	JobsCollectionName  = "jobs"
	Host                = "mongodb://localhost:27017"
	DbName              = "tpm_morphia"
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
	exitVal := m.Run()
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

//
//func populateTasks(jobId string, numTasks int) error {
//	const semLogContext = "populate-tasks"
//
//	coll, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
//	if err != nil {
//		log.Error().Err(err).Msg(semLogContext)
//		return err
//	}
//
//	for i := 1; i <= numTasks; i++ {
//		taskId := fmt.Sprintf("%s-t%d", jobId, i)
//		aTask := task.Task{
//			Bid:    taskId,
//			Et:     task.EType,
//			JobBid: jobId,
//			Status: task.StatusAvailable,
//			Typ:    task.TypeQMongo,
//			Info: beans.TaskInfo{
//				MdbInstance:   QueryInstanceId,
//				MdbCollection: QueryCollectionId,
//			},
//		}
//
//		for j := 1; j <= 5; j++ {
//			aTask.Partitions = append(aTask.Partitions,
//				partition.Partition{
//					Bid:             partition.Id(taskId, int32(j)),
//					Gid:             "",
//					Et:              partition.EType,
//					PartitionNumber: int32(j),
//					Status:          partition.StatusAvailable,
//					Etag:            0,
//					Info: beans.PartitionInfo{
//						MdbFilter: fmt.Sprintf(`{ "$and": [ {"%s": %d }, { "_id": { "$gt": { "$oid": "{resumeObjectId}" } } }  ] }`, partition.QueryDocumentPartitionFieldName, j),
//					},
//				},
//			)
//		}
//
//		insertResp, err := coll.InsertOne(context.Background(), aTask)
//		if err != nil {
//			log.Error().Err(err).Msg(semLogContext)
//			return err
//		}
//
//		log.Info().Interface("insertResp", insertResp).Msg(semLogContext)
//	}
//
//	return nil
//}
