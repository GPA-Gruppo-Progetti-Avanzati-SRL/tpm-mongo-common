package jsonops_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"os"
	"testing"
)

const (
	CollectionId   = "tpm-mongo-common"
	CollectionName = "tpm_mongo_common"
	Host           = "mongodb://localhost:27017"
	DbName         = "tpm_morphia"
)

var cfg = mongolks.Config{
	Name:          "default",
	Host:          Host,
	DbName:        DbName,
	User:          "",
	Pwd:           "",
	AuthMechanism: "",
	AuthSource:    "",
	Pool: mongolks.PoolConfig{
		MinConn: 1,
		MaxConn: 20,
		//MaxWaitQueueSize:      1000,
		ConnectTimeout:        1000,
		MaxConnectionIdleTime: 30000,
		//MaxConnectionLifeTime: 6000000,
	},
	//BulkWriteOrdered: true,
	WriteConcern: "majority",
	//WriteTimeout:     "120s",
	Collections: []mongolks.CollectionCfg{
		{
			Id:   CollectionId,
			Name: CollectionName,
		},
	},
	SecurityProtocol: "PLAIN",
	TLS:              mongolks.TLSConfig{SkipVerify: true},
}

func TestMain(m *testing.M) {

	_, err := mongolks.Initialize([]mongolks.Config{cfg})
	if err != nil {
		panic(err)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)
	exitVal := m.Run()
	os.Exit(exitVal)
}
