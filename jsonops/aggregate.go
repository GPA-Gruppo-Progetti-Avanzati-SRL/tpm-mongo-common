package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
)

func AggregateOne(lks *mongolks.LinkedService, collectionId string, pipeline []byte, opts []byte) (int, []byte, error) {
	const semLogContext = "json-ops::aggregate-one"
	sc, items, err := Aggregate(lks, collectionId, pipeline, opts)
	if err != nil {
		return sc, nil, err
	}

	if len(items) == 0 {
		return http.StatusNotFound, nil, nil
	}

	/*
		b, err := json.Marshal(items[0])
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}
	*/

	return http.StatusOK, items[0], nil
}

func Aggregate(lks *mongolks.LinkedService, collectionId string, pipeline []byte, opts []byte) (int, [][]byte, error) {
	const semLogContext = "json-ops::aggregate"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	statementQuery, err := util.UnmarshalJson2ArrayOfBsonD(pipeline)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	fo := options.AggregateOptions{}

	sc, resp, err := executeAggregateOp(c, statementQuery, &fo)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	if sc == http.StatusOK {
		//b, err := json.Marshal(body)
		//if err != nil {
		//	log.Error().Err(err).Msg(semLogContext)
		//	return http.StatusInternalServerError, nil, err
		//}

		return sc, resp, nil
	}

	return sc, nil, nil
}

func executeAggregateOp(c *mongo.Collection, pipeline interface{}, fo *options.AggregateOptions) (int, [][]byte, error) {
	const semLogContext = "mongo-operation::execute-aggregate-op"

	crs, err := c.Aggregate(context.Background(), pipeline, fo)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	var resp [][]byte
	for crs.Next(context.TODO()) {
		var el bson.M
		err = crs.Decode(&el)
		if err = crs.Decode(&el); err != nil {
			return http.StatusInternalServerError, nil, err
		}

		var b []byte
		b, err := json.Marshal(el)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}

		resp = append(resp, b)
	}

	if err = crs.Err(); err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, resp, nil
}
