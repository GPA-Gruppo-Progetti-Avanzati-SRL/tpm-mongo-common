package jsonops_test

import (
	"bytes"
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jsonops"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/feliixx/mongoextjson"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/bsonrw"
	"go.mongodb.org/mongo-driver/mongo"
	"testing"
	"time"
)

var insertOneTestDocument1 = []byte(`{ "year": 2030, "title": "the 2030 movie", "summary": "the 2030 movie summary", "my_date": {"$date":"2019-08-11T17:54:14.692Z"}}`)
var insertOneTestDocument2 = []byte(`{ "year": 1939, "title": "the 1939 1st movie", "summary": "the 1939 1st movie summary", "my_date": {"$date":"2019-08-11T17:54:14.692Z"}}`)
var insertOneTestDocument3 = []byte(`{ "year": 1939, "title": "the 1939 2nd movie", "summary": "the 1939 2nd movie summary", "my_date": {"$date":"2019-08-11T17:54:14.692Z"}}`)
var insertOneTestOpts = []byte(`{  }`)

func TestAll(t *testing.T) {
	TestDeleteAll(t)
	TestInsertOne(t)
	TestFindOne(t)
	TestFindOneAndUpdate(t)
	TestFind(t)
	TestUpdateOne(t)
	TestFind(t)
	TestReplaceOne(t)
	TestFind(t)
	TestAggregateOne(t)
	TestAggregate(t)
	TestDeleteOne(t)
	TestFind(t)
}

func TestInsertOne(t *testing.T) {
	log.Info().Msg("test-insert-one")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.InsertOne(lks, CollectionId, insertOneTestDocument1, insertOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))

	sc, resp, err = jsonops.InsertOne(lks, CollectionId, insertOneTestDocument2, insertOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))

	sc, resp, err = jsonops.InsertOne(lks, CollectionId, insertOneTestDocument3, insertOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

var findOneQueryTest = []byte(`{ "year": 1939 }`)
var findOneProjectionTest = []byte(`{ "year": 1, "summary": 1 }`)
var findOneSortTest = []byte(`{ "title": 1 }`)

func TestFindOne(t *testing.T) {
	log.Info().Msg("test-find-one")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, body, err := jsonops.FindOne(lks, CollectionId, findOneQueryTest, findOneProjectionTest, findOneSortTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))
}

var findOneAndUpdateQueryTest = []byte(`{ "year": 1939 }`)
var findOneAndUpdateProjectionTest = []byte(`{ "year": 1, "summary": 1 }`)
var findOneAndUpdateSortAscTest = []byte(`{ "title": 1 }`)
var findOneAndUpdateSortDescTest = []byte(`{ "title": -1 }`)
var findOneAndUpdateUpdateWithArray = []byte(`[{ "$set": { "year": 1940, "find-one-and-update-mode-array": "done", "aggregate-field" : { "$cond": [ false, "condition is true", "condition is false" ] } }}]`)
var findOneAndUpdateUpdateWithMap = []byte(`{ "$set": { "year": 1941, "find-one-and-update-update-mode-map": "done" }}`)

var findOneAndUpdateWithUpsertQueryTest = []byte(`{ "year": 2025 }`)
var findOneAndUpdateWithUpsertUpdateWithMap = []byte(`{ "$set": { "year": 2025, "find-one-and-update-update-mode-map": "done" }}`)
var findOneAndUpdateWithUpsertOptionsTest = []byte(`{ "upsert": true, "returnDocument": "before" }`)

func TestFindOneAndUpdate(t *testing.T) {
	log.Info().Msg("test-find-one-and-update")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, body, err := jsonops.FindOneAndUpdate(lks, CollectionId, findOneAndUpdateQueryTest, findOneAndUpdateProjectionTest, findOneAndUpdateSortAscTest, findOneAndUpdateUpdateWithArray, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))

	sc, body, err = jsonops.FindOneAndUpdate(lks, CollectionId, findOneAndUpdateQueryTest, findOneAndUpdateProjectionTest, findOneAndUpdateSortDescTest, findOneAndUpdateUpdateWithMap, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))

	sc, body, err = jsonops.FindOneAndUpdate(lks, CollectionId, findOneAndUpdateWithUpsertQueryTest, nil, nil, findOneAndUpdateWithUpsertUpdateWithMap, findOneAndUpdateWithUpsertOptionsTest)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))
}

var findQueryTest = []byte(`{}`)
var findProjectionTest = []byte(`{}`)
var findSortTest = []byte(`{ }`)

func TestFind(t *testing.T) {
	log.Info().Msg("test-find")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, body, err := jsonops.Find(lks, CollectionId, findQueryTest, findSortTest, findProjectionTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))
}

var aggregateTest = []byte(`[{ "$match": { "year": 1939 }}, { "$project": { "year": 1, "title": 1 }}]`)

func TestAggregateOne(t *testing.T) {
	log.Info().Msg("test-aggregate-one")

	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, body, err := jsonops.AggregateOne(lks, CollectionId, aggregateTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, string(body))
}

func TestAggregate(t *testing.T) {
	log.Info().Msg("test-aggregate")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, items, err := jsonops.Aggregate(lks, CollectionId, aggregateTest, nil)
	require.NoError(t, err)
	t.Log("status code:", sc, len(items))
	for i, el := range items {
		t.Log("item:", i, string(el))
	}
}

var updateOneTestFilterWithArray = []byte(`{ "year": 1940 }`)
var updateOneTestUpdateWithArray = []byte(`[{ "$set": { "year": 1942, "update-mode-array": "done", "aggregate-field" : { "$cond": [ false, "condition is true", "condition is false" ] } }}]`)
var updateOneTestFilterWithMap = []byte(`{ "year": 1941 }`)
var updateOneTestUpdateWithMap = []byte(`{ "$set": { "year": 1943, "update-mode-map": "done" }}`)
var updateOneTestOpts = []byte(`{ "upsert": true }`)

func TestUpdateOne(t *testing.T) {
	log.Info().Msg("test-update-one")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.UpdateOne(lks, CollectionId, updateOneTestFilterWithArray, updateOneTestUpdateWithArray, updateOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))

	sc, resp, err = jsonops.UpdateOne(lks, CollectionId, updateOneTestFilterWithMap, updateOneTestUpdateWithMap, updateOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))

}

var replaceOneTestFilter = []byte(`{ "year": 1943 }`)
var replaceOneTestReplacement = []byte(`{ "year": 1939, "update-mode-map": "done", "my_date": {"$date":"2019-08-11T17:54:14.692Z"}}`)
var replaceOneTestOpts = []byte(`{ "upsert": true }`)

func TestReplaceOne(t *testing.T) {
	log.Info().Msg("test-replace-one")

	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.ReplaceOne(lks, CollectionId, replaceOneTestFilter, replaceOneTestReplacement, replaceOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

var deleteOneTestFilter = []byte(`{ "year": 1942 }`)
var deleteOneTestOpts = []byte(`{}`)

func TestDeleteOne(t *testing.T) {
	log.Info().Msg("test-delete-one")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.DeleteOne(lks, CollectionId, deleteOneTestFilter, deleteOneTestOpts)
	require.NoError(t, err)
	t.Log("status code:", sc, string(resp))
}

var deleteAllTestFilter = []byte(`{}`)
var deleteAllTestOpts = []byte(`{}`)

func TestDeleteAll(t *testing.T) {
	log.Info().Msg("test-delete-all")
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	sc, resp, err := jsonops.DeleteMany(lks, CollectionId, deleteAllTestFilter, deleteAllTestOpts)
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

func TestBoh(t *testing.T) {
	p := getBsonD("20240701", bson.M{"operazotre": "whatever"})
	b, err := json.Marshal(p)
	require.NoError(t, err)

	fmt.Println(string(b))
}

func getUpdatePipeline(operazioneData string, obj interface{}) mongo.Pipeline {

	return mongo.Pipeline{
		bson.D{{
			Key: "$set", Value: bson.M{
				"opLiquidazioni": bson.M{
					"$filter": bson.M{
						"input": bson.M{
							"$ifNull": bson.A{"$opLiquidazioni", bson.A{}},
						},
						"as": "item",
						"cond": bson.M{
							"$ne": bson.A{"$$item.operazione.data", operazioneData},
						},
					},
				},
			},
		}},

		bson.D{{
			Key: "$set", Value: bson.M{
				"opLiquidazioni": bson.M{
					"$concatArrays": bson.A{
						"$opLiquidazioni",
						bson.A{
							obj,
						},
					},
				},
			},
		}},

		{ // Second stage: Sort the opLiquidazioni array by operazione.data
			{Key: "$set", Value: bson.M{
				"opLiquidazioni": bson.M{
					"$sortArray": bson.M{
						"input": "$opLiquidazioni",
						"sortBy": bson.M{
							"operazione.data": 1, // Sorting in ascending order
						},
					},
				},
			}},
		},
	}
}

func getBsonD(operazioneData string, obj interface{}) bson.A {

	return bson.A{
		bson.D{{
			Key: "$set", Value: bson.M{
				"opLiquidazioni": bson.M{
					"$filter": bson.M{
						"input": bson.M{
							"$ifNull": bson.A{"$opLiquidazioni", bson.A{}},
						},
						"as": "item",
						"cond": bson.M{
							"$ne": bson.A{"$$item.operazione.data", operazioneData},
						},
					},
				},
			},
		}},

		bson.D{{
			Key: "$set", Value: bson.M{
				"opLiquidazioni": bson.M{
					"$concatArrays": bson.A{
						"$opLiquidazioni",
						bson.A{
							obj,
						},
					},
				},
			},
		}},

		bson.D{ // Second stage: Sort the opLiquidazioni array by operazione.data
			{Key: "$set", Value: bson.M{
				"opLiquidazioni": bson.M{
					"$sortArray": bson.M{
						"input": "$opLiquidazioni",
						"sortBy": bson.M{
							"operazione.data": 1, // Sorting in ascending order
						},
					},
				},
			}},
		},
	}

}
