package mongolks

import (
	"context"
	"fmt"

	"go.mongodb.org/mongo-driver/mongo/readconcern"

	"time"

	mongoUtil "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
)

var DefaultWriteConcern = writeconcern.Majority()
var DefaultWriteTimeout = 60 * time.Second
var DefaultReadConcern = readconcern.Majority()

const DefaultAuthMechanism = "SCRAM-SHA-256"

type LinkedService struct {
	cfg               Config
	collectionsCfgMap map[string]CollectionCfg
	version           mongoUtil.MongoDbVersion
	mongoClient       *mongo.Client
	db                *mongo.Database
	writeConcern      *writeconcern.WriteConcern
	writeTimeout      time.Duration
}

func (lks *LinkedService) Name() string {
	return lks.cfg.Name
}

func (lks *LinkedService) Db() *mongo.Database {
	return lks.db
}

func (lks *LinkedService) IsConnected() bool {
	return lks.mongoClient != nil
}

func (lks *LinkedService) WriteTimeout() time.Duration {
	return lks.writeTimeout
}

func NewLinkedServiceWithConfig(cfg Config) (*LinkedService, error) {
	lks := LinkedService{cfg: cfg}

	if len(cfg.Collections) > 0 {
		lks.collectionsCfgMap = make(map[string]CollectionCfg)
		for _, collCfg := range cfg.Collections {
			lks.collectionsCfgMap[collCfg.Name] = collCfg
		}
	}

	return &lks, nil
}

func (lks *LinkedService) Connect(ctx context.Context) error {

	const semLogContext = "mongo-lks::connect"

	mongoOptions := lks.cfg.getOptions(nil)

	/*
		var mongoOptions = options.Client().ApplyURI(mdb.cfg.Host).
			SetMinPoolSize(uint64(mdb.cfg.Pool.MinConn)).
			SetMaxPoolSize(uint64(mdb.cfg.Pool.MaxConn)).
			SetMaxConnIdleTime(time.Duration(mdb.cfg.Pool.MaxConnectionIdleTime) * time.Millisecond).
			SetConnectTimeout(time.Duration(mdb.cfg.Pool.ConnectTimeout) * time.Millisecond)
	*/

	connTimeout := 10 * time.Second
	if lks.cfg.Pool.ConnectTimeout > 0 {
		connTimeout = lks.cfg.Pool.ConnectTimeout
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

	lks.mongoClient = client
	lks.db = client.Database(lks.cfg.DbName)
	lks.writeConcern = EvalWriteConcern(lks.cfg.WriteConcern)
	lks.writeTimeout = DefaultWriteTimeout

	//if mdb.cfg.WriteTimeout != "" {
	//	mdb.writeTimeout = util.ParseDuration(mdb.cfg.WriteTimeout, DefaultWriteTimeout)
	//}

	lks.version, err = lks.ServerVersion()
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

func (lks *LinkedService) ServerVersion() (mongoUtil.MongoDbVersion, error) {
	const semLogContext = "mongo-lks::version"

	if !lks.version.IsZero() {
		return lks.version, nil
	}

	buildInfoCmd := bson.D{bson.E{Key: "buildInfo", Value: 1}}
	var buildInfoDoc bson.M
	if err := lks.db.RunCommand(context.Background(), buildInfoCmd).Decode(&buildInfoDoc); err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return mongoUtil.MongoDbVersion{}, err
	}

	v := buildInfoDoc["version"]
	log.Info().Interface("mongo-db-version", v).Msg(semLogContext)
	return mongoUtil.NewMongoDbVersion(v), nil
}

func (lks *LinkedService) Disconnect(ctx context.Context) {
	if lks.mongoClient != nil {
		defer lks.mongoClient.Disconnect(ctx)
	}
}

func (lks *LinkedService) GetCollection(aCollectionId string, wcStr string) *mongo.Collection {
	const semLogContext = "mongo-lks::get-collection"

	w := lks.writeConcern
	if wcStr != "" {
		w = EvalWriteConcern(wcStr)
	}

	for _, c := range lks.cfg.Collections {
		if c.Id == aCollectionId {
			return lks.db.Collection(c.Name, &options.CollectionOptions{WriteConcern: w})
		}
	}

	err := fmt.Errorf("cannot find collection by id %s", aCollectionId)
	log.Error().Err(err).Str("id", aCollectionId).Str("instance", lks.cfg.Name).Msg(semLogContext)
	return nil
}

func (lks *LinkedService) GetCollectionName(aCollectionId string) string {

	for _, c := range lks.cfg.Collections {
		if c.Id == aCollectionId {
			return c.Name
		}
	}

	return ""
}

func (lks *LinkedService) GetCollectionsCfg() map[string]CollectionCfg {
	return lks.collectionsCfgMap
}
