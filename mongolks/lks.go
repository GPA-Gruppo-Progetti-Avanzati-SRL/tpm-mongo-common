package mongolks

import (
	"context"
	"crypto/tls"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"time"
)

var DefaultWriteConcern = writeconcern.New(writeconcern.WMajority())
var DefaultWriteTimeout = 60 * time.Second

type LinkedService struct {
	cfg          Config
	mongoClient  *mongo.Client
	db           *mongo.Database
	writeConcern *writeconcern.WriteConcern
	writeTimeout time.Duration
}

func (lks *LinkedService) Name() string {
	return lks.cfg.Name
}

func (lks *LinkedService) IsConnected() bool {
	return lks.mongoClient != nil
}

func (lks *LinkedService) WriteTimeout() time.Duration {
	return lks.writeTimeout
}

func NewLinkedServiceWithConfig(cfg Config) (*LinkedService, error) {
	lks := LinkedService{cfg: cfg}
	return &lks, nil
}

func (mdb *LinkedService) Connect(ctx context.Context) error {

	const semLogContext = "mongo-lks::connect"

	var mongoOptions = options.Client().ApplyURI(mdb.cfg.Host).
		SetMinPoolSize(uint64(mdb.cfg.Pool.MinConn)).
		SetMaxPoolSize(uint64(mdb.cfg.Pool.MaxConn)).
		SetMaxConnIdleTime(time.Duration(mdb.cfg.Pool.MaxConnectionIdleTime) * time.Millisecond).
		SetConnectTimeout(time.Duration(mdb.cfg.Pool.MaxWaitTime) * time.Millisecond)

	switch mdb.cfg.SecurityProtocol {
	case "TLS":
		log.Info().Bool("skip-verify", mdb.cfg.TLS.SkipVerify).Msg(semLogContext + " security-protocol set to TLS....")
		tlsCfg := &tls.Config{
			InsecureSkipVerify: mdb.cfg.TLS.SkipVerify,
		}
		mongoOptions.SetTLSConfig(tlsCfg)
	case "PLAIN":
		log.Info().Str("security-protocol", mdb.cfg.SecurityProtocol).Msg(semLogContext + " security-protocol set to PLAIN....nothing to do")
	default:
		log.Info().Str("security-protocol", mdb.cfg.SecurityProtocol).Msg(semLogContext + " skipping mongo security-protocol settings")
	}

	/*
	 * Simple User/password authentication
	 */
	if mdb.cfg.User != "" {
		authMechanism := "" // "SCRAM-SHA-256"
		if mdb.cfg.AuthMechanism != "" {
			authMechanism = mdb.cfg.AuthMechanism
		}
		authSource := mdb.cfg.DbName
		if mdb.cfg.AuthSource != "" {
			authSource = mdb.cfg.AuthSource
		}
		mongoOptions.SetAuth(options.Credential{
			AuthSource: authSource, Username: mdb.cfg.User, Password: mdb.cfg.Pwd, AuthMechanism: authMechanism,
		})
	}

	connTimeout := 10 * time.Second
	if mdb.cfg.ConnectTimeout > 0 {
		connTimeout = mdb.cfg.ConnectTimeout
	}
	log.Trace().Dur("connect-timeout", connTimeout).Msg(semLogContext)

	deadline := time.Now().Add(connTimeout)
	ctx, cancelCtx := context.WithDeadline(ctx, deadline)
	defer cancelCtx()
	client, err := mongo.Connect(ctx, mongoOptions)

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	/*
		err = client.Connect(ctx)
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}
	*/

	deadline = time.Now().Add(connTimeout)
	pingCtx, pingCancelCtx := context.WithDeadline(ctx, deadline)
	defer pingCancelCtx()

	err = client.Ping(pingCtx, readpref.Primary())
	if err != nil {
		return err
	}

	mdb.mongoClient = client
	mdb.db = client.Database(mdb.cfg.DbName)
	mdb.writeConcern = EvalWriteConcern(mdb.cfg.WriteConcern)
	mdb.writeTimeout = DefaultWriteTimeout

	if mdb.cfg.WriteTimeout != "" {
		mdb.writeTimeout = util.ParseDuration(mdb.cfg.WriteTimeout, DefaultWriteTimeout)
	}

	return nil
}

func (m *LinkedService) Disconnect(ctx context.Context) {
	if m.mongoClient != nil {
		defer m.mongoClient.Disconnect(ctx)
	}
}

func (m *LinkedService) GetCollection(aCollectionId string, wcStr string) *mongo.Collection {

	w := m.writeConcern
	if wcStr != "" {
		w = EvalWriteConcern(wcStr)
	}

	for _, c := range m.cfg.Collections {
		if c.Id == aCollectionId {
			return m.db.Collection(c.Name, &options.CollectionOptions{WriteConcern: w})
		}
	}

	return nil
}
