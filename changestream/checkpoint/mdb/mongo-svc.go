package mdb

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/mdb/checkpointcollection"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type CheckpointSvcConfig struct {
	Instance     string `yaml:"instance,omitempty" mapstructure:"instance,omitempty" json:"instance,omitempty"`
	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
	Stride       int    `yaml:"stride,omitempty" mapstructure:"stride,omitempty" json:"stride,omitempty"`
}

type CheckpointSvc struct {
	cfg           CheckpointSvcConfig
	LastSaved     checkpoint.ResumeToken
	NumberOfTicks int

	coll *mongo.Collection
}

func NewCheckpointSvc(cfg CheckpointSvcConfig) (checkpoint.ResumeTokenCheckpointSvc, error) {
	const semLogContext = "mongodb-checkpoint-svc::new"

	coll, err := mongolks.GetCollection(context.Background(), cfg.Instance, cfg.CollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return &CheckpointSvc{
		cfg:           cfg,
		NumberOfTicks: -1,
		coll:          coll,
	}, nil

}

func (svc *CheckpointSvc) Retrieve(watcherId string) (checkpoint.ResumeToken, error) {
	const semLogContext = "mongodb-checkpoint::retrieve"

	var err error
	var token checkpoint.ResumeToken

	var doc checkpointcollection.Document

	f := checkpointcollection.Filter{}
	f.Or().AndBidEqTo(watcherId)
	opts := options.FindOneOptions{}
	err = svc.coll.FindOne(context.Background(), f.Build(), &opts).Decode(&doc)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return token, nil
		}
	}

	token.Value = doc.ResumeToken
	token.At = doc.At
	return token, nil
}

func (f *CheckpointSvc) Store(tokenId string, token checkpoint.ResumeToken) error {
	var err error

	doSave := false
	if f.NumberOfTicks < 0 {
		doSave = true
		f.NumberOfTicks = 0
	} else {
		if (f.NumberOfTicks + 1) >= f.cfg.Stride {
			doSave = true
		}
	}

	f.NumberOfTicks++
	if doSave {
		err = f.save(tokenId, token)
		if err == nil {
			f.NumberOfTicks = 0
			f.LastSaved = token
		}
	}

	return err
}

func (svc *CheckpointSvc) save(watcherId string, token checkpoint.ResumeToken) error {
	const semLogContext = "mongodb-checkpoint::save"
	var err error

	info, err := token.Parse()
	if err != nil {
		return err
	}

	f := checkpointcollection.Filter{}
	f.Or().AndBidEqTo(watcherId)
	opts := options.UpdateOptions{}
	opts.SetUpsert(true)

	ud := checkpointcollection.GetUpdateDocumentFromOptions(
		checkpointcollection.UpdateWith_bid(watcherId),
		checkpointcollection.UpdateWithResume_token(token.Value),
		checkpointcollection.UpdateWithAt(token.At),
		checkpointcollection.UpdateWithShort_token(token.ShortVersion()),
		checkpointcollection.UpdateWithTxn_opn_index(info.TxnOpIndex),
	)

	resp, err := svc.coll.UpdateOne(context.Background(), f.Build(), ud.Build(), &opts)
	if err != nil {
		return err
	}

	log.Trace().Interface("resp", resp).Msg(semLogContext)
	return nil
}
