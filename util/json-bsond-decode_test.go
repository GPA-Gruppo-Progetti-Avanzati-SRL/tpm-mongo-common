package util_test

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
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

var extendedTest = []byte(`
{
          "legati": [
            {
              "dataCensimento": "20200102",
              "ndg:": "1234"
            },
            {
              "dataCensimento": "20200102",
              "ndg:": "0123"
            }
          ],
  "$filter": {
    "_bid": "000000000000002",
    "_et": "MOVIMENTI-DR",
    "infoTecniche.ccid": "000000000000002__SEQNO000000000000002"
  },
  "$update": {
    "$set": {
      "_bid": "000000000000002",
      "_et": "MOVIMENTI-DR",
      "evento": {
        "Codice_Causale": "1136",
        "Data_Contabile": "20241105",
        "Descrizione": "Versamento arrotondamento  per salvadanaio da PPAY *9100",
        "Identificativo_Movimento": "000000000000002",
        "Importo_Movimento": "410",
        "Numero_Rapporto": "000987654321",
        "Settoriale_Mittente": "DR",
        "Tipo_Movimento": "consolidato"
      },
      "infoTecniche": {
        "ccid": "000000000000002__SEQNO000000000000002",
        "dataAlimentazione": "2024-11-05  11:29:25.929921000000",
        "dataOperazione": "2024-11-05 11:29:24.081786000000",
        "sourceTopic": "rhapsody-movim-dr-src",
        "tipoOperazione": "I"
      },
      "informazioniAddizionali": {
        "_bid": "000987654321",
        "_et": "RAP",
        "_id": "67efc9a76e1f2ff4b79cb842",
        "categoria": "1210",
        "dataApertura": "20250320",
        "dataIntestazione": "20250320",
        "filiale": "38095",
        "infoTecniche": {
          "dataOra": "20250320163910",
          "dataOraAlimentazione": "2025-03-21 00:39:13.441472000000",
          "tipoOperazione": "I"
        },
        "ndg": "09876543",
        "ndgInfo": {
          "_bid": "09876543",
          "_et": "NDG",
          "_id": "67dd4558bf8d8d94af26ec18",
          "dataCensimento": "20221213",
          "dataNascita": "19800101",
          "legati": [
            {
              "dataCensimento": "20200102",
              "ndg": "1234"
            },
            {
              "dataCensimento": "20200102",
              "ndg": "0123"
            }
          ],
          "maxDataCensimento": "20200102",
          "natura": "PF"
        },
        "servizio": "DR"
      }
    }
  },
  "$opts": {
    "upsert": true
  }
}
`)

func TestDecodeExtendedTest2Json(t *testing.T) {

	m := make(map[string]interface{})
	err := json.Unmarshal(extendedTest, &m)
	require.NoError(t, err)
	t.Log(m)

	m = make(map[string]interface{})
	err = util.UnmarshalMongoJson(extendedTest, &m)
	require.NoError(t, err)
	t.Log(m)

	t.Logf("%T", m["legati"])
	if a, ok := m["legati"].(primitive.A); ok {
		t.Logf("%T", a[0])
	}

	d, err := util.UnmarshalJson2BsonD(extendedTest, true)
	require.NoError(t, err)
	first := fmt.Sprintln(d)

	t.Log(first)

	/*
		for i := 0; i < 1; i++ {
			d, err := util.UnmarshalJson2BsonD(extendedTest, true)
			require.NoError(t, err)
			current := fmt.Sprintln(d)
			require.Equal(t, first, current)
		}
	*/
}
