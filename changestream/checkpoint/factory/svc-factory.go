package factory

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/file"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/mdb"
	"github.com/rs/zerolog/log"
)

const (
	SvcProviderMongo = "mongo"
	SvcProviderFile  = "file"
)

type Config struct {
	Typ               string `yaml:"type,omitempty" mapstructure:"type,omitempty" json:"type,omitempty"`
	Fn                string `yaml:"file-name,omitempty" mapstructure:"file-name,omitempty" json:"file-name,omitempty"`
	Stride            int    `yaml:"stride,omitempty" mapstructure:"stride,omitempty" json:"stride,omitempty"`
	MongoInstance     string `yaml:"mongo-db-instance,omitempty" mapstructure:"mongo-db-instance,omitempty" json:"mongo-db-instance,omitempty"`
	MongoCollectionId string `yaml:"mongo-db-collection-id,omitempty" mapstructure:"mongo-db-collection-id,omitempty" json:"mongo-db-collection-id,omitempty"`
}

func NewCheckPointSvc(config Config) (checkpoint.ResumeTokenCheckpointSvc, error) {
	const semLogContext = "checkpoint-svc::new"
	var err error
	var svc checkpoint.ResumeTokenCheckpointSvc

	switch config.Typ {
	case SvcProviderMongo:
		svc, err = mdb.NewCheckpointSvc(mdb.CheckpointSvcConfig{
			Instance:     config.MongoInstance,
			CollectionId: config.MongoCollectionId,
			Stride:       config.Stride,
		})
	case SvcProviderFile:
		svc = file.NewCheckpointSvc(file.CheckpointSvcConfig{
			Fn:     config.Fn,
			Stride: config.Stride,
		})
	default:
		err = fmt.Errorf("unknown checkpoint service type: %s", config.Typ)
		log.Error().Err(err).Msg(semLogContext)
	}

	return svc, err
}
