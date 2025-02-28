package querystream_test

import (
	"context"
	"errors"
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
	err = batch.Query(`{ "_id": { "$gt": { "$oid": "{resumeObjectId}" } } }`, primitive.NilObjectID.Hex())
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

func TestNewQueryStream(t *testing.T) {
	cfg := querystream.StreamOptions{
		Consumer: querystream.ResourceGroup{
			Pid:          "test-partition-group",
			Gid:          "test-consumer-group",
			Instance:     "default",
			CollectionId: PartitionsCollectionId,
		},
		QuerySource: querystream.QuerySource{
			Instance:     "default",
			CollectionId: QueryCollectionId,
			BatchSize:    20,
		},
	}

	qs, err := querystream.NewQueryStream(cfg)
	require.NoError(t, err)
	defer qs.Close(context.Background())

	numDocs := 0
	evt, err := qs.Next()
	for err == nil {
		numDocs++
		t.Log(evt)
		evt, err = qs.Next()
	}

	t.Logf("num-documents: %d", numDocs)
	if err != nil {
		require.Equal(t, true, errors.Is(err, io.EOF))
	}
}
