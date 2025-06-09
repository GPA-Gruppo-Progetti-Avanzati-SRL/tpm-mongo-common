package taskconsumer_test

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer/datasource"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
)

const (
	NumPartitions           = 5
	NumDocumentPerPartition = 10
	TotalDocs               = (NumPartitions - 1) * NumDocumentPerPartition
	jobId                   = "my-job-id"
	taskId                  = "jobId-t1"
)

const (
	WithPopulateTasks = true
	WithPopulateData  = true
	WithClearData     = true
)

func TestNewConsumer(t *testing.T) {

	if WithPopulateTasks {
		populateTasks(t)
	}

	if WithPopulateData {
		populateData(t)
	}

	if WithClearData {
		defer clearTaskAndData(t)
	}

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	tasks, err := task.FindByJobBidAndStatus(taskColl, jobId, task.StatusAvailable)
	require.NoError(t, err)
	require.Condition(t, func() bool { return len(tasks) > 0 }, "expected tasks to be available")

	consumerCfg := taskconsumer.Config{
		Id: "my-consumer-group",
		// to-do: configure a metric. This generates an error in trace mode.
		MetricsGId: "qstream-events",
		//OnErrorPolicy: querystream.OnErrorPolicyExit,
	}

	qs, err := taskconsumer.NewConsumer(taskColl, tasks[0], &consumerCfg)
	require.NoError(t, err)
	defer qs.Close(context.Background())

	numDocs := 0
	evt, err := qs.Poll()
	for evt.Typ != datasource.EventTypeEof && err == nil {
		if evt.IsDocument() {
			numDocs++
			err = qs.Commit(false)
			require.NoError(t, err)
		}
		t.Log(evt)
		evt, err = qs.Poll()
	}

	t.Logf("num-documents: %d", numDocs)
	require.NoError(t, err)
	require.Condition(t, func() bool { return evt.Typ == datasource.EventTypeEof }, "expected EOF but found %v", evt)

	//if err != nil {
	//	require.Condition(t, func() bool { return errors.Is(err, io.EOF) }, "expected EOF but found %s", err.Error())
	//}
}

func populateTasks(t *testing.T) {
	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	_, err = populateTask(t, taskColl)
	require.NoError(t, err)
}

func populateData(t *testing.T) {
	qcoll, err := mongolks.GetCollection(context.Background(), QueryInstanceId, QueryCollectionId)
	if err != nil {
		panic(err)
	}

	err = populatePartitionedDocuments(qcoll)
	if err != nil {
		panic(err)
	}
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
		Bid:            taskId,
		Et:             task.EType,
		JobId:          jobId,
		Status:         task.StatusAvailable,
		DataSourceType: task.TypeQMongo,
		StreamType:     task.DataStreamTypeInfinite,
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
