package lease_test

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
	"time"
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

const (
	LeaseGroupId  = "lease-gid"
	LeaseObjectId = "leased-oid"
)

func TestLease(t *testing.T) {

	coll, err := mongolks.GetCollection(context.Background(), "default", CollectionId)
	require.NoError(t, err)

	t.Logf("acquire lease on object %s", LeaseObjectId)
	lh, ok, err := lease.AcquireLease(coll, LeaseGroupId, LeaseObjectId, false)
	require.NoError(t, err)
	require.EqualValues(t, true, ok)

	lh.SetLeaseData("my-user-property", "My-User-Specific-data")
	err = lh.RenewLease(false)
	require.NoError(t, err)

	t.Logf("release lease on object %s", LeaseObjectId)
	err = lh.Release()
	require.NoError(t, err)

	t.Logf("acquire lease on object %s", LeaseObjectId)
	lh, ok, err = lease.AcquireLease(coll, LeaseGroupId, LeaseObjectId, false)
	require.NoError(t, err)
	require.EqualValues(t, true, ok)

	t.Log("waiting 10 secs for not expired test...")
	time.Sleep(time.Second * 10)
	t.Logf("try to acquire lease on object %s.... and should fail", LeaseObjectId)
	_, ok, err = lease.AcquireLease(coll, LeaseGroupId, LeaseObjectId, false)
	require.NoError(t, err)
	require.EqualValues(t, false, ok)

	t.Log("waiting 70 secs for lease expiration...")
	time.Sleep(time.Second * 70)
	t.Logf("acquire lease on object %s", LeaseObjectId)
	lh, ok, err = lease.AcquireLease(coll, LeaseGroupId, LeaseObjectId, true)
	require.NoError(t, err)
	require.EqualValues(t, true, ok)

	lh.SetLeaseData("my-user-property-2", "My-User-Specific-data")
	err = lh.RenewLease(true) // added withErrors flag
	require.NoError(t, err)

	t.Log("waiting 100 secs before release...")
	time.Sleep(time.Second * 100)
	t.Logf("release lease on object %s", LeaseObjectId)
	err = lh.Release()
	require.NoError(t, err)
}
