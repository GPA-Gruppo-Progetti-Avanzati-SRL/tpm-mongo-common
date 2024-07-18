package jsonopsutil

import (
	"github.com/feliixx/mongoextjson"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

func UnmarshalJSON2BsonM(b []byte) (bson.M, error) {
	const semLogContext = "json-ops-util::unmarshal-2-bson-m"
	var err error

	if len(b) == 0 {
		return nil, nil
	}
	var m map[string]interface{}
	err = mongoextjson.Unmarshal(b, &m)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return bson.M(m), nil

}

func UnmarshalJSON2ArrayOfBsonM(b []byte) ([]bson.M, error) {
	const semLogContext = "json-ops-util::unmarshal-2-bson-d"
	var err error

	if len(b) == 0 {
		return nil, nil
	}
	var d []bson.M
	err = mongoextjson.Unmarshal(b, &d)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return d, nil

}

func UnmarshalJSONMap2BsonD(b []byte) (bson.D, error) {
	const semLogContext = "json-ops-util::unmarshal-map-2-bson-d"
	var err error

	if len(b) == 0 {
		return nil, nil
	}

	var o = NewMap2BsonDAdapter()
	err = mongoextjson.Unmarshal(b, &o)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return o.GetDocument()
}
