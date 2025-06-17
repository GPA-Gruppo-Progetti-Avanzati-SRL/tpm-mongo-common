package mongolks

import (
	"context"
	"go.mongodb.org/mongo-driver/mongo/readconcern"

	mongoUtil "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"time"
)

var DefaultWriteConcern = writeconcern.Majority()
var DefaultWriteTimeout = 60 * time.Second
var DefaultReadConcern = readconcern.Majority()

const DefaultAuthMechanism = "SCRAM-SHA-256"

type LinkedService struct {
	cfg          Config
	version      mongoUtil.MongoDbVersion
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

	mongoOptions := mdb.cfg.getOptions(nil)

	/*
		var mongoOptions = options.Client().ApplyURI(mdb.cfg.Host).
			SetMinPoolSize(uint64(mdb.cfg.Pool.MinConn)).
			SetMaxPoolSize(uint64(mdb.cfg.Pool.MaxConn)).
			SetMaxConnIdleTime(time.Duration(mdb.cfg.Pool.MaxConnectionIdleTime) * time.Millisecond).
			SetConnectTimeout(time.Duration(mdb.cfg.Pool.ConnectTimeout) * time.Millisecond)
	*/

	connTimeout := 10 * time.Second
	if mdb.cfg.Pool.ConnectTimeout > 0 {
		connTimeout = mdb.cfg.Pool.ConnectTimeout
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

	//if mdb.cfg.WriteTimeout != "" {
	//	mdb.writeTimeout = util.ParseDuration(mdb.cfg.WriteTimeout, DefaultWriteTimeout)
	//}

	mdb.version, err = mdb.ServerVersion()
	if err != nil {
		return err
	}

	//buildInfoCmd := bson.D{bson.E{Key: "buildInfo", Value: 1}}
	//var buildInfoDoc bson.M
	//if err := mdb.db.RunCommand(ctx, buildInfoCmd).Decode(&buildInfoDoc); err != nil {
	//	log.Error().Err(err).Msg(semLogContext)
	//	return err
	//}
	//log.Info().Interface("mongo-db-version", buildInfoDoc["version"]).Msg(semLogContext)

	return nil
}

func (mdb *LinkedService) ServerVersion() (mongoUtil.MongoDbVersion, error) {
	const semLogContext = "mongo-lks::version"

	if !mdb.version.IsZero() {
		return mdb.version, nil
	}

	buildInfoCmd := bson.D{bson.E{Key: "buildInfo", Value: 1}}
	var buildInfoDoc bson.M
	if err := mdb.db.RunCommand(context.Background(), buildInfoCmd).Decode(&buildInfoDoc); err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return mongoUtil.MongoDbVersion{}, err
	}

	v := buildInfoDoc["version"]
	log.Info().Interface("mongo-db-version", v).Msg(semLogContext)
	return mongoUtil.NewMongoDbVersion(v), nil
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
