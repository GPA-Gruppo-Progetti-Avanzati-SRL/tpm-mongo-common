package util

import (
	"errors"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
)

func MongoError(err error) (bool, mongo.CommandError) {
	const semLogContext = "mongo::error"

	var srvErr mongo.ServerError
	if errors.Is(err, srvErr) {
		log.Error().Msg(semLogContext)
	}

	var mongoErr mongo.CommandError
	switch {
	case errors.As(err, &mongoErr):
		log.Error().Err(mongoErr).Msg(semLogContext + " command")
	case errors.Is(err, srvErr):
		log.Error().Err(srvErr).Msg(semLogContext + " srv")
	}

	return false, mongo.CommandError{}
}
