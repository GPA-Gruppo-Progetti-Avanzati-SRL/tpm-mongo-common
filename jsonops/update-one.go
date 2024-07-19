package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
)

func UpdateOne(lks *mongolks.LinkedService, collectionId string, filter []byte, update []byte, opts []byte) (int, []byte, error) {
	const semLogContext = "json-ops::update-one"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	statementFilter, err := util.UnmarshalJson2BsonD(filter)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	statementUpdate, err := util.UnmarshalJson2BsonD(update)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	uo := options.UpdateOptions{}
	if len(opts) > 0 {
		err = json.Unmarshal(opts, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}
	}
	res, err := c.UpdateOne(context.Background(), statementFilter, statementUpdate, &uo)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	var b []byte
	b, err = json.Marshal(res)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, b, nil
}
