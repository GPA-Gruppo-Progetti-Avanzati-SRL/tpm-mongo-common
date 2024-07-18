package jsonops_test

import (
	"context"
	_ "embed"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jsonops"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/stretchr/testify/require"
	"testing"
)

var findOneQueryTest = []byte(`{ "year": 1939 }`)
var findOneProjectionTest = []byte(`{ "year": 1 }`)
var findSortTest = []byte(`{ "title": 1 }`)
var aggregateTest = []byte(`[{ "$match": { "year": 1939 }}, { "$project": { "year": 1, "title": 1 }}]`)

func TestFindOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, body, err := jsonops.FindOne(lks, CollectionId, findOneQueryTest, findOneProjectionTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))
}

func TestFind(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, items, err := jsonops.Find(lks, CollectionId, findOneQueryTest, findSortTest, findOneProjectionTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, len(items))
	for i, el := range items {
		t.Log("item:", i, string(el))
	}
}

func TestAggregateOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, body, err := jsonops.AggregateOne(lks, CollectionId, aggregateTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))
}

func TestAggregate(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, items, err := jsonops.Aggregate(lks, CollectionId, aggregateTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, len(items))
	for i, el := range items {
		t.Log("item:", i, string(el))
	}
}

var updateOneTestFilter = []byte(`{ "year": 1939 }`)
var updateOneTestUpdate = []byte(`{ "$set": { "year-new": 1939 }}`)
var updateOneTestOpts = []byte(`{ "upsert": true }`)

func TestUpdateOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.UpdateOne(lks, CollectionId, updateOneTestFilter, updateOneTestUpdate, updateOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}
