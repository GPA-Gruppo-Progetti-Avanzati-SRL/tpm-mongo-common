package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jsonops/jsonopsutil"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
)

func Find(lks *mongolks.LinkedService, collectionId string, query []byte, sort []byte, projection []byte, opts []byte) (int, [][]byte, error) {
	const semLogContext = "json-ops::find"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	statementQuery, err := jsonopsutil.UnmarshalJSON2BsonM(query)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	fo := options.FindOptions{}
	srt, err := jsonopsutil.UnmarshalJSONMap2BsonD(sort)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	if len(srt) > 0 {
		fo.SetSort(srt)
	}

	prj, err := jsonopsutil.UnmarshalJSON2BsonM(projection)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	fo.SetLimit(10)

	sc, resp, err := executeFindOp(c, statementQuery, &fo)
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

func executeFindOp(c *mongo.Collection, query bson.M, fo *options.FindOptions) (int, [][]byte, error) {
	const semLogContext = "mongo-operation::execute-find-op"

	crs, err := c.Find(context.Background(), query, fo)
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
