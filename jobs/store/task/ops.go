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
	f.Or().AndEtEqTo(EType).AndJobIdEqTo(jobBid).AndStatusEqTo(status)
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

func (tsk *Task) UpdatePartitionStatus(taskColl *mongo.Collection, taskId string, prtNdx int32, st string, withErrors bool) error {
	const semLogContext = "task::update-partition-status"

	updOpts := UpdateOptions{
		UpdateWithIncPartitionAcquisitions(prtNdx),
	}

	if st != "" {
		updOpts = append(updOpts, UpdateWithPartitionStatus(prtNdx, st))
	}

	if withErrors {
		updOpts = append(updOpts, UpdateWithIncPartitionErrors(prtNdx))
	}

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(taskId).AndEtEqTo(EType)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := taskColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)

	return nil
}
