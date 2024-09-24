package jsonops_test

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jsonops"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/feliixx/mongoextjson"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"testing"
	"time"
)

var findOneQueryTest = []byte(`{ "year": 1939 }`)
var findOneProjectionTest = []byte(`{ "year": 1 }`)
var findSortTest = []byte(`{ "title": 1 }`)

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

var aggregateTest = []byte(`[{ "$match": { "year": 1939 }}, { "$project": { "year": 1, "title": 1 }}]`)

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
var updateOneTestUpdateWithArray = []byte(`[{ "$set": { "year-new": 1939, "update-mode-array": "done", "aggregate-field" : { "$cond": [ false, "condition is true", "condition is false" ] } }}]`)
var updateOneTestUpdateWithMap = []byte(`{ "$set": { "year-new": 1939, "update-mode-map": "done" }}`)
var updateOneTestOpts = []byte(`{ "upsert": true }`)

func TestUpdateOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.UpdateOne(lks, CollectionId, updateOneTestFilter, updateOneTestUpdateWithMap, updateOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))

	sc, resp, err = jsonops.UpdateOne(lks, CollectionId, updateOneTestFilter, updateOneTestUpdateWithArray, updateOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

var replaceOneTestFilter = []byte(`{ "year": 1939 }`)
var replaceOneTestReplacement = []byte(`{ "year": 1939, "year-new": 1939, "my_date": {"$date":"2019-08-11T17:54:14.692Z"}}`)
var replaceOneTestOpts = []byte(`{ "upsert": true }`)

func TestReplaceOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.ReplaceOne(lks, CollectionId, replaceOneTestFilter, replaceOneTestReplacement, replaceOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

var deleteOneTestFilter = []byte(`{ "year": 1939 }`)
var deleteOneTestOpts = []byte(`{}`)

func TestDeleteOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.DeleteOne(lks, CollectionId, deleteOneTestFilter, deleteOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

var insertOneTestDocument = []byte(`{ "year": 2030, "my_date": {"$date":"2019-08-11T17:54:14.692Z"}}`)
var insertOneTestOpts = []byte(`{  }`)

func TestInsertOne(t *testing.T) {
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.InsertOne(lks, CollectionId, insertOneTestDocument, insertOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

func TestExampleUnmarshal(t *testing.T) {

	doc := bson.M{}
	data := []byte(`
	{
		"_id":        ObjectId("5a934e000102030405000000"),
		"binary":     BinData(2,"YmluYXJ5"),
		"date":       ISODate("2016-05-15T01:02:03.004Z"),
		"decimal128": NumberDecimal("1.8446744073709551617E-6157"),
		"double":     2.2,
		"false":      false,
		"int32":      32,
		"int64":      NumberLong(64),
		"string":     "string",
		"timestamp":  Timestamp(12,0),
		"true":       true,
		"undefined":  undefined,
		unquoted:     "keys can be unquoted"
	}`)
	err := mongoextjson.Unmarshal(data, &doc)
	if err != nil {
		fmt.Printf("fail to unmarshal %+v: %v", data, err)
	}
	fmt.Printf("%+v", doc)

	// Output:
	//map[_id:ObjectID("5a934e000102030405000000") binary:{Subtype:2 Data:[98 105 110 97 114 121]} date:2016-05-15 01:02:03.004 +0000 UTC decimal128:1.8446744073709551617E-6157 double:2.2 false:false int32:32 int64:64 string:string timestamp:{T:12 I:0} true:true undefined:{} unquoted:keys can be unquoted]
}

func TestExtJson(t *testing.T) {
	data := []byte(`{"name":"Cereal Rounds","sku":"AB12345","price_cents":{"$numberLong":"399"}, "my_date": {"$date":"2019-08-11T17:54:14.692Z"}, "number_value": 8120000000}`)

	// Create a Decoder that reads the Extended JSON document and use it to
	// unmarshal the document into a Product struct.
	vr, err := bsonrw.NewExtJSONValueReader(bytes.NewReader(data), false)
	require.NoError(t, err)

	decoder, err := bson.NewDecoder(vr)
	require.NoError(t, err)

	type Product struct {
		Name  string    `bson:"name"`
		SKU   string    `bson:"sku"`
		Price int64     `bson:"price_cents"`
		ADate time.Time `bson:"my_date"`
	}

	var res Product
	err = decoder.Decode(&res)
	require.NoError(t, err)

	vr, err = bsonrw.NewExtJSONValueReader(bytes.NewReader(data), false)
	require.NoError(t, err)

	decoder, err = bson.NewDecoder(vr)
	require.NoError(t, err)

	var resMap map[string]interface{}
	err = decoder.Decode(&resMap)
	require.NoError(t, err)

	fmt.Printf("%+v\n", resMap)
	for n, v := range resMap {
		t.Logf("jey: %s, value: %v of type %T", n, v, v)
	}

	vr, err = bsonrw.NewExtJSONValueReader(bytes.NewReader(data), false)
	require.NoError(t, err)
	vr.ReadCodeWithScope()
}
