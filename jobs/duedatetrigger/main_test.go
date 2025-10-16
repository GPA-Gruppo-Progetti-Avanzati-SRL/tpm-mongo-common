package duedatetrigger_test

import (
	"os"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

const (
	JobsCollectionId   = "jobs-collection"
	JobsCollectionName = "jobs"

	Host   = "mongodb://localhost:27017"
	DbName = "tubamirum"
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
		MinConn:               1,
		MaxConn:               20,
		MaxWaitTime:           1000,
		MaxConnectionIdleTime: 30000,
	},
	WriteConcern: "majority",
	Collections: []mongolks.CollectionCfg{
		{
			Id:   JobsCollectionId,
			Name: JobsCollectionName,
		},
	},
	SecurityProtocol: "PLAIN",
	TLS:              mongolks.TLSConfig{SkipVerify: true},
}

func TestMain(m *testing.M) {
	const semLogContext = "due-date-trigger-test::setup"

	_, err := mongolks.Initialize([]mongolks.Config{cfg})
	if err != nil {
		log.Fatal().Err(err).Msg(semLogContext)
	}

	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.InfoLevel)

	exitVal := m.Run()
	os.Exit(exitVal)
}
