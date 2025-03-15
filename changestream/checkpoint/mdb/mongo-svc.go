package mdb

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/mdb/checkpointcollection"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type CheckpointSvcConfig struct {
	Instance           string `yaml:"instance,omitempty" mapstructure:"instance,omitempty" json:"instance,omitempty"`
	CollectionId       string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
	Stride             int    `yaml:"stride,omitempty" mapstructure:"stride,omitempty" json:"stride,omitempty"`
	ClearOnHistoryLost bool   `yaml:"clear-on-history-lost,omitempty" mapstructure:"clear-on-history-lost,omitempty" json:"clear-on-history-lost,omitempty"`
}

type CheckpointSvc struct {
	cfg           CheckpointSvcConfig
	LastSaved     checkpoint.ResumeToken
	LastCommitted checkpoint.ResumeToken
	LastIdle      checkpoint.ResumeToken
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

	doc, err := checkpointcollection.FindOneByBidAndStatus(svc.coll, watcherId, checkpointcollection.CheckPointStatusActive)
	if err != nil {
		log.Error().Err(err).Str("watcher-id", watcherId).Msg(semLogContext)
		return token, err
	}

	if doc == nil {
		return token, nil
	}
	/*
		f := checkpointcollection.Filter{}
		f.Or().AndBidEqTo(watcherId).AndStatusEqTo(checkpointcollection.CheckPointStatusActive)
		opts := options.FindOneOptions{}
		err = svc.coll.FindOne(context.Background(), f.Build(), &opts).Decode(&doc)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				log.Info().Err(err).Str("watcher-id", watcherId).Msg(semLogContext)
				return token, nil
			} else {
				log.Error().Err(err).Str("watcher-id", watcherId).Msg(semLogContext)
			}
		}
	*/

	log.Info().Err(err).Interface("chk", doc).Msg(semLogContext)
	token.Value = doc.ResumeToken
	token.At = doc.At
	return token, nil
}

func (f *CheckpointSvc) Synch(watcherId string, rt checkpoint.ResumeToken) error {
	const semLogContext = "mongodb-checkpoint::synch"
	var err error

	// level is warn since this is called in cases when you want to force the update of synchpoint in case of errors
	if !rt.IsZero() {
		log.Warn().Str("rt", rt.String()).Msg(semLogContext + " synch on last-delivered")
		err = f.save(watcherId, rt)
	} else {
		if !f.LastCommitted.IsZero() {
			log.Warn().Str("rt", f.LastCommitted.String()).Msg(semLogContext + " synch on last-committed")
			err = f.save(watcherId, f.LastCommitted)
		}
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

const (
	LastIdle5MinsStoreInterval   = 1 * time.Minute
	LastIdle24HoursStoreInterval = 24 * time.Hour
	LastIdle1HourStoreInterval   = 1 * time.Hour
)

func (f *CheckpointSvc) ClearIdle() {
	f.LastIdle = checkpoint.ResumeToken{}
}

func (f *CheckpointSvc) StoreIdle(tokenId string, token checkpoint.ResumeToken) error {
	const semLogContext = "mongodb-checkpoint::store-idle"
	var err error

	if f.LastIdle.IsZero() {
		f.LastIdle = token
		return nil
	}

	if f.LastIdle.Value != token.Value {
		log.Info().Str("", f.LastIdle.Value).Str("", token.Value).Msg(semLogContext + " - last idle changed")
	}

	// Check if a save happened in the last hour (either for an event or for an idle resume token.
	elapsed := LastIdle24HoursStoreInterval.Seconds()
	var lastIdleTm time.Time
	lastIdleTm, err = time.Parse(time.RFC3339Nano, f.LastIdle.At)
	if err == nil {
		elapsed = time.Now().Sub(lastIdleTm).Seconds()
	} else {
		log.Error().Err(err).Msg(semLogContext)
	}

	if elapsed > LastIdle5MinsStoreInterval.Seconds() {
		err = f.save(tokenId, token)
		if err == nil {
			f.NumberOfTicks = 0
			f.LastIdle = token
		}
	}

	return err
}

func (f *CheckpointSvc) Store(tokenId string, token checkpoint.ResumeToken) error {
	const semLogContext = "mongodb-checkpoint::store"
	var err error

	// last committed contains the last token that's been stored or not.
	f.LastCommitted = token

	doSave := false
	if f.NumberOfTicks < 0 {
		doSave = true
		f.NumberOfTicks = 0
	} else {
		if (f.NumberOfTicks + 1) >= f.cfg.Stride {
			doSave = true
		}

		lastSavedTm, err1 := time.Parse(time.RFC3339Nano, f.LastSaved.At)
		currentTokenTm, err2 := time.Parse(time.RFC3339Nano, token.At)
		if err1 == nil && err2 == nil {
			if currentTokenTm.Sub(lastSavedTm) > 1*time.Minute {
				log.Info().Float64("interval", currentTokenTm.Sub(lastSavedTm).Seconds()).Msg(semLogContext)
				doSave = true
			}
		} else {
			if err1 != nil {
				log.Error().Err(err1).Msg(semLogContext)
			}

			if err2 != nil {
				log.Error().Err(err2).Msg(semLogContext)
			}
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
	f.Or().AndBidEqTo(watcherId).AndStatusEqTo(checkpointcollection.CheckPointStatusActive)
	opts := options.UpdateOptions{}
	opts.SetUpsert(true)

	ud := checkpointcollection.GetUpdateDocumentFromOptions(
		checkpointcollection.UpdateWith_bid(watcherId),
		checkpointcollection.UpdateWithResume_token(token.Value),
		checkpointcollection.UpdateWithAt(token.At),
		checkpointcollection.UpdateWithShort_token(token.ShortVersion()),
		checkpointcollection.UpdateWithTxn_opn_index(info.TxnOpIndex),
		checkpointcollection.UpdateWithStatus(checkpointcollection.CheckPointStatusActive),
	)

	resp, err := svc.coll.UpdateOne(context.Background(), f.Build(), ud.Build(), &opts)
	if err != nil {
		return err
	}

	log.Info().Interface("resp", resp).Str("token", token.Value).Msg(semLogContext)
	return nil
}

func (svc *CheckpointSvc) Clear(watcherId string) error {
	const semLogContext = "mongodb-checkpoint::clear"
	var err error

	doc, err := checkpointcollection.FindOneByBidAndStatus(svc.coll, watcherId, checkpointcollection.CheckPointStatusActive)
	if err != nil {
		return err
	}

	if doc.IsZero() {
		log.Info().Msg(semLogContext + " no checkpoint to clear")
		return nil
	}

	rt := checkpoint.ResumeToken{
		Value: doc.ResumeToken,
		At:    doc.At,
	}

	rtInfo, err := rt.Parse()
	if err == nil {
		log.Warn().Interface("rt-info", rtInfo).Msg(semLogContext + " checkpoint to clear")
	} else {
		log.Error().Err(err).Msg(semLogContext)
	}

	f := checkpointcollection.Filter{}
	f.Or().AndBidEqTo(doc.Bid).AndStatusEqTo(doc.Status)
	opts := options.UpdateOptions{}
	ud := checkpointcollection.GetUpdateDocumentFromOptions(
		checkpointcollection.UpdateWithStatus(checkpointcollection.CheckPointStatusCleared),
	)

	resp, err := svc.coll.UpdateOne(context.Background(), f.Build(), ud.Build(), &opts)
	if err != nil {
		return err
	}

	log.Info().Interface("resp", resp).Msg(semLogContext)
	return nil
}

func (svc *CheckpointSvc) OnHistoryLost(watcherId string) error {
	if svc.cfg.ClearOnHistoryLost {
		return svc.Clear(watcherId)
	}

	return nil
}
