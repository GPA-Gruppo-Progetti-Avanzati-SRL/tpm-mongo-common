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

func FindOne(lks *mongolks.LinkedService, collectionId string, query []byte, projection []byte, opts []byte) (int, []byte, error) {
	const semLogContext = "json-ops::find-one"
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

	fo := options.FindOneOptions{}
	prj, err := jsonopsutil.UnmarshalJSON2BsonM(projection)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	sc, body, err := executeFindOneOp(c, statementQuery, &fo)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	if sc == http.StatusOK {
		b, err := json.Marshal(body)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}

		return sc, b, nil
	}

	return sc, nil, nil
}

func executeFindOneOp(c *mongo.Collection, query bson.M, fo *options.FindOneOptions) (int, bson.M, error) {
	const semLogContext = "mongo-operation::execute-find-one-op"

	result := c.FindOne(context.Background(), query, fo)
	if errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return http.StatusNotFound, nil, nil
	}

	if result.Err() != nil {
		log.Error().Err(result.Err()).Msg(semLogContext)
		return http.StatusInternalServerError, nil, result.Err()
	}

	var body bson.M
	err := result.Decode(&body)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, body, nil
}
