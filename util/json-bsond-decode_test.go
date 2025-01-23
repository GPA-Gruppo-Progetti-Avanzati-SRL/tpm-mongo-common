package util_test

import (
	_ "embed"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/stretchr/testify/require"
	"testing"
)

var testJson = []byte(`{ "sort": {"f2": 1, "f1": 1 } }`)

func TestDecodeJson(t *testing.T) {
	d, err := util.UnmarshalJson2BsonD(testJson, true)
	require.NoError(t, err)
	t.Log(d)
}

var aggregateTestJson = []byte(`[{ "$match": { "year": 1939, "the_date": {"$date":"2019-08-11T17:54:14.692Z"} }}, { "$project": { "year": 1, "title": 1 }}]`)

func TestUnmarshalJson2ArrayOfBsonD(t *testing.T) {
	docs, err := util.UnmarshalJson2ArrayOfBsonD(aggregateTestJson, true)
	require.NoError(t, err)
	for _, d := range docs {
		t.Log(d)
	}
}

var conversionTestJson1 = []byte(`{ "$match": { "year": 1939, "myOid": {"$oid": "65a6773b79d056df01e93e42"}, "the_date": {"$date":"2019-08-11T17:54:14.692Z"} }}`)

//go:embed extended.json
var conversionTestJson []byte

func TestConvertJsonExtended2Json(t *testing.T) {

	b, err := util.JsonExtended2JsonConv(conversionTestJson)
	require.NoError(t, err)

	t.Log(string(b))
}
