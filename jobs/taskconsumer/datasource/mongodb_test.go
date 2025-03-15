package datasource_test

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer/datasource"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"io"
	"testing"
)

func TestNewStreamBatch(t *testing.T) {
	coll, err := mongolks.GetCollection(context.Background(), "default", QueryCollectionId)
	require.NoError(t, err)

	ds, err := datasource.NewMongoDbConnector(coll, 20)
	require.NoError(t, err)

	err = ds.Query(datasource.NewResumableFilter(`{ "_id": { "$gt": { "$oid": "{resumeObjectId}" } } }`, 1, primitive.NilObjectID.Hex()))
	require.NoError(t, err)

	numDocs := 0
	doc, err := ds.Next()
	for err == nil {
		t.Log(doc)
		numDocs++
		doc, err = ds.Next()
	}
	require.Equal(t, true, err == io.EOF)
	t.Logf("num documents : %d", numDocs)
}
