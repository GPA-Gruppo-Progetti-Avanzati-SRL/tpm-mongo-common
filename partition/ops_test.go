package partition_test

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/partition"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
)

const (
	PartitionsInstanceId     = "default"
	PartitionsCollectionId   = "partitions"
	PartitionsCollectionName = "partitions"

	NumPartitions = 5
)

func TestOps(t *testing.T) {
	coll, err := mongolks.GetCollection(context.Background(), PartitionsInstanceId, PartitionsCollectionId)
	require.NoError(t, err)

	populatePartitions(t, coll)
	defer clearPartitions(t, coll)

	qcoll, err := mongolks.GetCollection(context.Background(), QueryInstanceId, QueryCollectionId)
	require.NoError(t, err)

	populatePartitionedDocuments(t, qcoll)
	defer clearPartitionedDocuments(t, qcoll)

	prts, err := partition.FindAllPartitionsByTypeAndPartitionGroupAndStatus(coll, partition.MongoPartitionType, QueryCollectionId, partition.StatusAvailable /*, true, primitive.NilObjectID */)
	require.NoError(t, err)
	t.Log(prts)

}

func populatePartitions(t *testing.T, coll *mongo.Collection) {
	for i := 1; i <= NumPartitions; i++ {
		prt := partition.Partition{
			Bid:             partition.Id(QueryCollectionId, int32(i)),
			Gid:             QueryCollectionId,
			Et:              partition.MongoPartitionType,
			PartitionNumber: 1,
			Status:          partition.StatusAvailable,
			Etag:            0,
			Mongo: partition.MongoPartition{
				//Instance:   QueryInstanceId,
				//Collection: QueryCollectionId,
				Filter: fmt.Sprintf(`{ "$and": [ {"%s": %d }, { "_id": { "$gt": { "$oid": "{resumeObjectId}" } } }  ] }`, partition.QueryDocumentPartitionFieldName, i),
			},
		}

		insertResp, err := coll.InsertOne(context.Background(), prt)
		require.NoError(t, err)
		t.Log(insertResp)
	}
}

func clearPartitions(t *testing.T, coll *mongo.Collection) {
	f := partition.Filter{}
	f.Or().AndEtEqTo(partition.MongoPartitionType).AndGidEqTo(QueryCollectionId).AndStatusEqTo(partition.StatusAvailable)
	deleteResp, err := coll.DeleteMany(context.Background(), f.Build())
	require.NoError(t, err)
	t.Log(deleteResp)
}

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
