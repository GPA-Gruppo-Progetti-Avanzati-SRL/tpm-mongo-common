package changestream_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"os"
	"strconv"
	"testing"
)

const (
	CollectionId               = "diaries"
	CollectionName             = "diaries"
	DbName                     = "test"
	HostEnvVarName             = "MONGODB_HOST_ENV"
	DbEnvVarName               = "MONGODB_DB_ENV"
	UserEnvVarName             = "MONGODB_USR_ENV"
	PwdEnvVarName              = "MONGODB_PWD_ENV"
	AuthSourceEnvVarName       = "MONGODB_AUTHSRC_ENV"
	SkpVerifEnvVarName         = "MONGODB_SKPVER_ENV"
	SecurityProtocolEnvVarName = "MONGODB_SECURITY_PROTOCOL"
)

var cfg = mongolks.Config{
	Name:          "default",
	Host:          os.Getenv(HostEnvVarName),
	DbName:        DbName, // os.Getenv(DbEnvVarName),
	User:          os.Getenv(UserEnvVarName),
	Pwd:           os.Getenv(PwdEnvVarName),
	AuthMechanism: "",
	AuthSource:    os.Getenv(AuthSourceEnvVarName),
	Pool: mongolks.PoolConfig{
		MinConn:               1,
		MaxConn:               20,
		MaxWaitQueueSize:      1000,
		MaxWaitTime:           1000,
		MaxConnectionIdleTime: 30000,
		MaxConnectionLifeTime: 6000000,
	},
	BulkWriteOrdered: true,
	WriteConcern:     "majority",
	WriteTimeout:     "120s",
	Collections: []mongolks.CollectionCfg{
		{
			Id:   CollectionId,
			Name: CollectionName,
		},
	},
	SecurityProtocol: os.Getenv(SecurityProtocolEnvVarName),
	TLS:              mongolks.TLSConfig{SkipVerify: false},
}

func TestMain(m *testing.M) {

	skp, err := strconv.ParseBool(os.Getenv(SkpVerifEnvVarName))
	if err != nil {
		panic(err)
	}

	cfg.TLS.SkipVerify = skp

	_, err = mongolks.Initialize([]mongolks.Config{cfg})
	if err != nil {
		panic(err)
	}

	exitVal := m.Run()
	os.Exit(exitVal)
}
