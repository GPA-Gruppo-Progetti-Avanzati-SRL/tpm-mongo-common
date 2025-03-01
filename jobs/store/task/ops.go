package task

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FindByJobBidAndStatus(coll *mongo.Collection, jobBid string, status string) ([]Task, error) {
	const semLogContext = "task::find-by-jobBid-and-status"

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndJobBidEqTo(jobBid).AndStatusEqTo(status)
	opts := options.FindOptions{}

	crs, err := coll.Find(context.Background(), f.Build(), &opts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var tasks []Task
	err = crs.All(context.Background(), &tasks)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return tasks, nil
}
