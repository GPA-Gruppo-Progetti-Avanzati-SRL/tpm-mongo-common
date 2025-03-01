package job

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FindJobsByTypeAndStatus(coll *mongo.Collection, typ string, status string) ([]Job, error) {
	const semLogContext = "job::find-all-by-type-and-status"

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndStatusEqTo(status)
	opts := options.FindOptions{}

	crs, err := coll.Find(context.Background(), f.Build(), &opts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var jobs []Job
	err = crs.All(context.Background(), &jobs)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	log.Info().Int("num-jobs", len(jobs)).Str("type", typ).Msg(semLogContext)
	return jobs, nil
}
