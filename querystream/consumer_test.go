package querystream_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/querystream"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"testing"
)

const (
	NumPartitions           = 5
	NumDocumentPerPartition = 10
	TotalDocs               = (NumPartitions - 1) * NumDocumentPerPartition
	jobId                   = "my-job-id"
	taskId                  = "jobId-t1"
)

func TestNewStreamBatch(t *testing.T) {
	coll, err := mongolks.GetCollection(context.Background(), "default", QueryCollectionId)
	require.NoError(t, err)

	batch := querystream.NewBatch(coll, 20)
	err = batch.Query(querystream.NewResumableFilter(`{ "_id": { "$gt": { "$oid": "{resumeObjectId}" } } }`, primitive.NilObjectID.Hex()))
	require.NoError(t, err)

	numDocs := 0
	doc, err := batch.Next()
	for err == nil {
		t.Log(doc)
		numDocs++
		doc, err = batch.Next()
	}
	require.Equal(t, true, err == io.EOF)
	t.Logf("num documents : %d", numDocs)
}

const (
	WithPopulateData = false
	WithClearData    = false
)

func TestNewQueryConsumer(t *testing.T) {

	if WithPopulateData {
		populateTaskAndData(t)
	}

	if WithClearData {
		defer clearTaskAndData(t)
	}

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobId, task.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(tasks) > 0 }, "expected tasks to be available")

	consumerCfg := querystream.Config{
		Id: "my-consumer-group",
		// to-do: configure a metric. This generates an error in trace mode.
		RefMetrics: &promutil.MetricsConfigReference{
			GId: "qstream-events",
		},
		OnErrorPolicy: querystream.OnErrorPolicyExit,
	}

	qs, err := querystream.NewQueryConsumer(&consumerCfg, tasks[0], taskColl)
	require.NoError(t, err)
	defer qs.Close(context.Background())

	numDocs := 0
	evt, err := qs.Poll()
	for !evt.Eof && err == nil {
		if evt.IsDocument() {
			numDocs++
			err = qs.Commit()
			require.NoError(t, err)
		}
		t.Log(evt)
		evt, err = qs.Poll()
	}

	t.Logf("num-documents: %d", numDocs)
	if err != nil {
		require.Condition(t, func() bool { return errors.Is(err, io.EOF) }, "expected EOF but found %s", err.Error())
	}

}

func populateTaskAndData(t *testing.T) {
	qcoll, err := mongolks.GetCollection(context.Background(), QueryInstanceId, QueryCollectionId)
	if err != nil {
		panic(err)
	}

	err = populatePartitionedDocuments(qcoll)
	if err != nil {
		panic(err)
	}

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	_, err = populateTask(t, taskColl)
	require.NoError(t, err)
}

func clearTaskAndData(t *testing.T) {
	qcoll, err := mongolks.GetCollection(context.Background(), QueryInstanceId, QueryCollectionId)
	require.NoError(t, err)

	err = clearPartitionedDocuments(qcoll)
	require.NoError(t, err)

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	err = clearTask(t, taskColl)
	require.NoError(t, err)
}

func populateTask(t *testing.T, taskColl *mongo.Collection) (task.Task, error) {

	aTask := task.Task{
		Bid:    taskId,
		Et:     task.EType,
		JobBid: jobId,
		Status: task.StatusAvailable,
		Typ:    task.TypeQMongo,
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
	require.NoError(t, err)
	t.Log(insertResp)
	return aTask, err
}

func clearTask(t *testing.T, taskColl *mongo.Collection) error {
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
