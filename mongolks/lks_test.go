package mongolks_test

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"testing"
	"time"
)

func TestConfig(t *testing.T) {

	cfg := mongolks.Config{
		Name:          "default",
		Host:          "${MONGODB_HOST_ENV}",
		DbName:        "${MONGODB_DB_ENV}",
		User:          "${MONGODB_USR_ENV}",
		Pwd:           "${MONGODB_PWD_ENV}",
		AuthMechanism: "${MONGODB_AUTHSRC_ENV}",
		AuthSource:    "${MONGODB_SECURITY_PROTOCOL}",
		Pool: mongolks.PoolConfig{
			MinConn:               1,
			MaxConn:               20,
			ConnectTimeout:        1 * time.Millisecond,
			MaxConnectionIdleTime: 30 * time.Millisecond,
			MaxConnecting:         20,
			MetricConfig: &mongolks.PoolConfigMetrics{
				Name: "tpm-mongo-common",
			},
		},
		WriteConcern:     "majority",
		ReadConcern:      "majority",
		OperationTimeout: 0,
		SecurityProtocol: "${MONGODB_SECURITY_PROTOCOL}",
		TLS: mongolks.TLSConfig{
			CaLocation: "",
			SkipVerify: true,
		},
		HeartbeatInterval:      10 * time.Second,
		ServerSelectionTimeout: 30 * time.Second,
		RetryWrites:            "true",
		RetryReads:             "true",
		Compressor:             []string{},
		ZlibLevel:              "-1",
		ZstdLevel:              "6",
		Collections: []mongolks.CollectionCfg{
			{
				Id:   "collectionId",
				Name: "collectionName",
			},
		},
	}

	b, err := yaml.Marshal(cfg)
	require.NoError(t, err)

	fmt.Println(string(b))
}
