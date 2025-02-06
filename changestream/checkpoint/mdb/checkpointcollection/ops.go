package checkpointcollection

import (
	"context"
	"errors"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FindOneByBidAndStatus(coll *mongo.Collection, watcherId string, status string) (*Document, error) {
	const semLogContext = "checkpoint-collection::find-by-bid-and-status"
	var doc Document

	f := Filter{}
	f.Or().AndBidEqTo(watcherId).AndStatusEqTo(status)
	opts := options.FindOneOptions{}
	err := coll.FindOne(context.Background(), f.Build(), &opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Info().Str("watcher-id", watcherId).Msg(semLogContext + " - no active checkpoint found")
			return nil, nil
		} else {
			log.Error().Err(err).Str("watcher-id", watcherId).Msg(semLogContext)
		}
	}

	log.Info().Err(err).Interface("chk", doc).Msg(semLogContext)
	return &doc, nil
}
