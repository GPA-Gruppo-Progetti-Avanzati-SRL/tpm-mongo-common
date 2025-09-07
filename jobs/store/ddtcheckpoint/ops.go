package ddtcheckpoint

import (
	"context"
	"errors"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func RetrieveDueDate(coll *mongo.Collection, bid string, startDueDate string) (string, error) {
	const semLogContext = "ddt-checkpoint::retrieve"

	var doc DueDateTriggerCheckPoint

	f := Filter{}
	f.Or().AndBidEqTo(bid).AndEtEqTo(EType)
	opts := options.FindOneOptions{}
	err := coll.FindOne(context.Background(), f.Build(), &opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			log.Info().Str("bid", bid).Msg(semLogContext + " - no active checkpoint found")
			return startDueDate, nil
		} else {
			log.Info().Str("bid", bid).Msg(semLogContext + " - no active checkpoint found")
			return "", err
		}
	}

	return doc.DueDate, nil
}

func UpdateDueDate(coll *mongo.Collection, bid string, dueDate string) error {
	const semLogContext = "ddt-checkpoint::update-due-date"

	f := Filter{}
	f.Or().AndBidEqTo(bid).AndEtEqTo(EType)

	upd := GetUpdateDocumentFromOptions(UpdateWith_et(EType), UpdateWith_bid(bid), UpdateWithDue_date(dueDate))
	opts := options.UpdateOptions{}
	opts.SetUpsert(true)
	resp, err := coll.UpdateOne(context.Background(), f.Build(), upd.Build(), &opts)
	if err != nil {
		log.Error().Err(err).Str("bid", bid).Str("due_date", dueDate).Msg(semLogContext)
		return err
	}

	log.Info().Str("bid", bid).Str("due_date", dueDate).Interface("update-op", resp).Msg(semLogContext)
	if resp.MatchedCount > 1 {
		log.Warn().Msg(semLogContext + " - more than one trigger found")
	}

	return nil
}
