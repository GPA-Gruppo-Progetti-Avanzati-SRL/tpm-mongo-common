package file

import (
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/fileutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/rs/zerolog/log"
	"os"
	"time"
)

type Document struct {
	Bid         string `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et          string `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	ResumeToken string `json:"resume_token,omitempty" bson:"resume_token,omitempty" yaml:"resume_token,omitempty"`
	At          string `json:"at,omitempty" bson:"at,omitempty" yaml:"at,omitempty"`
	ShortToken  string `json:"short_token,omitempty" bson:"short_token,omitempty" yaml:"short_token,omitempty"`
	TxnOpnIndex string `json:"txn_opn_index,omitempty" bson:"txn_opn_index,omitempty" yaml:"txn_opn_index,omitempty"`
}

type CheckpointSvcConfig struct {
	Fn                 string `yaml:"file-name,omitempty" mapstructure:"file-name,omitempty" json:"file-name,omitempty"`
	Stride             int    `yaml:"stride,omitempty" mapstructure:"stride,omitempty" json:"stride,omitempty"`
	ClearOnHistoryLost bool   `yaml:"clear-on-history-lost,omitempty" mapstructure:"clear-on-history-lost,omitempty" json:"clear-on-history-lost,omitempty"`
}

type CheckpointSvc struct {
	cfg           CheckpointSvcConfig
	LastCommitted checkpoint.ResumeToken
	LastSaved     checkpoint.ResumeToken
	NumberOfTicks int
}

func NewCheckpointSvc(cfg CheckpointSvcConfig) checkpoint.ResumeTokenCheckpointSvc {
	return &CheckpointSvc{
		cfg:           cfg,
		NumberOfTicks: -1,
	}

}

func (f *CheckpointSvc) Retrieve(watcherId string) (checkpoint.ResumeToken, error) {
	const semLogContext = "file-checkpoint::retrieve"

	var err error
	var token checkpoint.ResumeToken

	if !fileutil.FileExists(f.cfg.Fn) {
		err = errors.New("file does not exist")
		log.Warn().Err(err).Str("fn", f.cfg.Fn).Msg(semLogContext)
		return token, nil
	}

	b, err := os.ReadFile(f.cfg.Fn)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return token, err
	}

	d := Document{}
	err = json.Unmarshal(b, &d)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return token, err
	}

	token.At = d.At
	token.Value = d.ResumeToken
	return token, nil
}

//func (f *CheckpointSvc) CommitAt2(watcherId string, rt checkpoint.ResumeToken, syncRequired bool) error {
//	const semLogContext = "file-checkpoint::synch"
//	var err error
//
//	// level is warn since this is called in cases when you want to force the update of synchpoint in case of errors
//	if !rt.IsZero() {
//		log.Warn().Str("rt", rt.String()).Msg(semLogContext + " synch on last-delivered")
//		err = f.save(watcherId, rt)
//	} else {
//		if !f.LastCommitted.IsZero() {
//			log.Warn().Str("rt", f.LastCommitted.String()).Msg(semLogContext + " synch on last-committed")
//			err = f.save(watcherId, f.LastCommitted)
//		}
//	}
//
//	if err != nil {
//		log.Error().Err(err).Msg(semLogContext)
//		return err
//	}
//
//	return nil
//}

func (f *CheckpointSvc) StoreIdle(watcherId string, token checkpoint.ResumeToken) error {
	return errors.New("still not implemented for file checkpoint svc")
}

func (f *CheckpointSvc) ClearIdle() {
	const semLogContext = "file-checkpoint::clear-idle"
	err := errors.New("still not implemented for file checkpoint svc")
	log.Error().Err(err).Msg(semLogContext)
}

func (f *CheckpointSvc) CommitAt(watcherId string, token checkpoint.ResumeToken, syncRequired bool) error {
	var err error

	// last committed contains the last token that's been stored or not.
	if !token.IsZero() {
		f.LastCommitted = token
	}

	doSave := syncRequired
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
		err = f.save(watcherId, token)
		if err == nil {
			f.NumberOfTicks = 0
			f.LastSaved = token
		}
	}

	return err
}

func (f *CheckpointSvc) save(watcherId string, token checkpoint.ResumeToken) error {
	const semLogContext = "file-checkpoint::save"

	log.Trace().Str("fn", f.cfg.Fn).Msg(semLogContext)

	info, err := token.Parse()
	if err != nil {
		return err
	}

	d := Document{
		Bid:         watcherId,
		Et:          "",
		ResumeToken: token.Value,
		At:          time.Now().Format(time.RFC3339),
		ShortToken:  token.ShortVersion(),
		TxnOpnIndex: info.TxnOpIndex,
	}

	b, err := json.Marshal(d)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	err = os.WriteFile(f.cfg.Fn, b, os.ModePerm)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

func (f *CheckpointSvc) Clear(watcherId string) error {
	const semLogContext = "file-checkpoint::clear"
	err := errors.New("clearing not implemented on file checkpoint svc")
	log.Warn().Err(err).Msg(semLogContext)
	return nil
}

func (f *CheckpointSvc) OnHistoryLost(watcherId string) error {
	if f.cfg.ClearOnHistoryLost {
		return f.Clear(watcherId)
	}

	return nil
}
