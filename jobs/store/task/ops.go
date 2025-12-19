package task

import (
	"context"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func FindById(coll *mongo.Collection, taskId string) (Task, error) {
	const semLogContext = "task::find-by-id"

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(taskId)
	opts := options.FindOne()

	var tsk Task
	err := coll.FindOne(context.Background(), f.Build(), opts).Decode(&tsk)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return Task{}, err
	}

	return tsk, nil
}

func FindByJobBidAndStatus(coll *mongo.Collection, jobBid string, status string) ([]Task, error) {
	const semLogContext = "task::find-by-jobBid-and-status"

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndJobIdEqTo(jobBid).AndStatusEqTo(status)
	opts := options.Find()

	crs, err := coll.Find(context.Background(), f.Build(), opts)
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

func (tsk Task) UpdateStatus(taskColl *mongo.Collection, taskId string, st string) error {
	const semLogContext = "task::update-status"

	updOpts := UpdateOptions{
		UpdateWithStatus(st),
		UpdateWithModifiedAt(bson.NewDateTimeFromTime(time.Now()), st),
	}

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(taskId)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := taskColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)

	return nil
}

func (tsk Task) UpdatePartitionStatus(taskColl *mongo.Collection, prtNdx int32, st string) error {
	const semLogContext = "task::update-partition-status"

	updOpts := UpdateOptions{
		UpdateWithIncPartitionAcquisitions(prtNdx),
	}

	if st != "" {
		updOpts = append(updOpts, UpdateWithPartitionStatus(prtNdx, st))
		updOpts = append(updOpts, UpdateWithPartitionModifiedAt(prtNdx, bson.NewDateTimeFromTime(time.Now()), st == beans.PartitionStatusEOF))
	}

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(tsk.Bid)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := taskColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)

	return nil
}

func (tsk Task) UpdatePartitionStatusOnError(taskColl *mongo.Collection, prtNdx int32, statusInError bool) error {
	const semLogContext = "task::update-partition-status"

	updOpts := UpdateOptions{
		UpdateWithIncPartitionAcquisitions(prtNdx),
		UpdateWithIncPartitionErrors(prtNdx),
		UpdateWithIncErrors(),
	}

	if statusInError {
		updOpts = append(updOpts, UpdateWithPartitionStatus(prtNdx, StatusError))
		updOpts = append(updOpts, UpdateWithPartitionModifiedAt(prtNdx, bson.NewDateTimeFromTime(time.Now()), false))
	}

	f := Filter{}
	f.Or().AndEtEqTo(EType).AndBidEqTo(tsk.Bid)

	updDoc := GetUpdateDocumentFromOptions(updOpts...)
	resp, err := taskColl.UpdateOne(context.Background(), f.Build(), updDoc.Build())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)

	return nil
}
