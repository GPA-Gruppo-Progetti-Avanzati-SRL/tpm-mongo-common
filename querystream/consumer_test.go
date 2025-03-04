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
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"testing"
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

func TestNewQueryConsumer(t *testing.T) {

	consumerCfg := querystream.Config{
		Id: "my-consumer-group",
		// to-do: configure a metric. This generates an error in trace mode.
		RefMetrics: &promutil.MetricsConfigReference{
			GId: "qstream-events",
		},
		OnErrorPolicy: querystream.OnErrorPolicyExit,
	}

	jobId := "my-job-id"
	taskId := fmt.Sprintf("%s-t%d", jobId, 1)
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

	for j := 1; j <= 5; j++ {
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

	taskColl, err := mongolks.GetCollection(context.Background(), JobsInstanceId, JobsCollectionId)
	require.NoError(t, err)

	qs, err := querystream.NewQueryConsumer(&consumerCfg, aTask, taskColl)
	require.NoError(t, err)
	defer qs.Close(context.Background())

	numDocs := 0
	evt, err := qs.Poll()
	for err == nil {
		numDocs++
		t.Log(evt)
		evt, err = qs.Next()
	}

	t.Logf("num-documents: %d", numDocs)
	require.Equal(t, true, errors.Is(err, io.EOF))
}
