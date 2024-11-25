package file

import (
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/fileutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/rs/zerolog/log"
	"os"
)

type CheckpointSvcConfig struct {
	Fn           string `yaml:"file-name,omitempty" mapstructure:"file-name,omitempty" json:"file-name,omitempty"`
	TickInterval int    `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
}

type CheckpointSvc struct {
	cfg           CheckpointSvcConfig
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

	err = json.Unmarshal(b, &token)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return token, err
	}

	return token, nil
}

func (f *CheckpointSvc) Store(watcherId string, token checkpoint.ResumeToken) error {
	var err error

	doSave := false
	if f.NumberOfTicks < 0 {
		doSave = true
		f.NumberOfTicks = 0
	} else {
		if (f.NumberOfTicks + 1) >= f.cfg.TickInterval {
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

	b, err := json.Marshal(token)
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
