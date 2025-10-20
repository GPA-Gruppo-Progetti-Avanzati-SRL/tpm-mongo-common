package mongolks

import (
	"crypto/tls"
	"strconv"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
	"go.mongodb.org/mongo-driver/v2/mongo/readconcern"
	"go.mongodb.org/mongo-driver/v2/mongo/writeconcern"
)

const MongoDbDefaultInstanceName = "default"

type StoreReference struct {
	InstanceName string `yaml:"lks,omitempty" mapstructure:"lks,omitempty" json:"lks,omitempty"`
	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
}

type CollectionCfg struct {
	Id   string
	Name string
}

type CollectionsCfg []CollectionCfg

type TLSConfig struct {
	CaLocation string `json:"ca-location" mapstructure:"ca-location" yaml:"ca-location"`
	SkipVerify bool   `json:"skip-verify" mapstructure:"skip-verify" yaml:"skip-verify"`
}

type Config struct {
	Name                   string
	Host                   string
	DbName                 string         `mapstructure:"db-name,omitempty" json:"db-name,omitempty" yaml:"db-name,omitempty"`
	User                   string         `mapstructure:"user,omitempty" json:"user,omitempty" yaml:"user,omitempty"`
	Pwd                    string         `mapstructure:"pwd,omitempty" json:"pwd,omitempty" yaml:"pwd,omitempty"`
	AuthMechanism          string         `mapstructure:"authMechanism,omitempty" json:"authMechanism,omitempty" yaml:"authMechanism,omitempty"`
	AuthSource             string         `mapstructure:"authSource,omitempty" json:"authSource,omitempty" yaml:"authSource,omitempty"`
	Pool                   PoolConfig     `mapstructure:"pool,omitempty" json:"pool,omitempty" yaml:"pool,omitempty"`
	WriteConcern           string         `mapstructure:"write-concern,omitempty" json:"write-concern,omitempty" yaml:"write-concern,omitempty"`
	ReadConcern            string         `mapstructure:"read-concern" json:"read-concern" yaml:"read-concern"`
	OperationTimeout       time.Duration  `mapstructure:"operation-timeout" json:"operation-timeout" yaml:"operation-timeout"`
	SecurityProtocol       string         `mapstructure:"security-protocol,omitempty" json:"security-protocol,omitempty" yaml:"security-protocol,omitempty"`
	TLS                    TLSConfig      `json:"tls" mapstructure:"tls" yaml:"tls"`
	HeartbeatInterval      time.Duration  `mapstructure:"heartbeat-interval" json:"heartbeat-interval" yaml:"heartbeat-interval"`
	ServerSelectionTimeout time.Duration  `mapstructure:"server-selection-timeout" json:"server-selection-timeout" yaml:"server-selection-timeout"`
	RetryWrites            string         `mapstructure:"retry-writes" json:"retry-writes" yaml:"retry-writes"`
	RetryReads             string         `mapstructure:"retry-reads" json:"retry-reads" yaml:"retry-reads"`
	Compressor             []string       `mapstructure:"compressor" json:"compressor" yaml:"compressor"`
	ZlibLevel              string         `mapstructure:"zlib-level" json:"zlib-level" yaml:"zlib-level"`
	ZstdLevel              string         `mapstructure:"zstd-level" json:"zstd-level" yaml:"zstd-level"`
	Collections            CollectionsCfg `mapstructure:"collections,omitempty" json:"collections,omitempty" yaml:"collections,omitempty"`
	// WriteTimeout           string         `mapstructure:"write-timeout,omitempty" json:"write-timeout,omitempty" yaml:"write-timeout,omitempty"`
	// BulkWriteOrdered bool           `mapstructure:"bulk-write-ordered,omitempty" json:"bulk-write-ordered,omitempty" yaml:"bulk-write-ordered,omitempty"`
}

func (cfg *Config) getOptions(opts *options.ClientOptions) *options.ClientOptions {
	const semLogContext = "mongo-lks::get-options"

	if opts == nil {
		opts = options.Client()
	}

	opts.ApplyURI(cfg.Host)
	opts = cfg.Pool.getOptions(opts)
	opts = cfg.getAuthOptions(opts)

	writeConcern := DefaultWriteConcern
	if cfg.WriteConcern != "" {
		writeConcern = EvalWriteConcern(cfg.WriteConcern)
	}
	opts.SetWriteConcern(writeConcern)

	if cfg.OperationTimeout != 0 {
		opts.SetTimeout(cfg.OperationTimeout)
	}

	if cfg.HeartbeatInterval != 0 {
		opts.SetHeartbeatInterval(cfg.HeartbeatInterval)
	}

	if cfg.ServerSelectionTimeout != 0 {
		opts.SetServerSelectionTimeout(cfg.ServerSelectionTimeout)
	}

	if cfg.RetryReads != "" {
		b, err := strconv.ParseBool(cfg.RetryReads)
		if err != nil {
			log.Error().Err(err).Str("retry-reads", cfg.RetryReads).Msg(semLogContext + " invalid value for retry-reads")
		}
		opts.SetRetryReads(b)
	}

	if cfg.RetryWrites != "" {
		b, err := strconv.ParseBool(cfg.RetryWrites)
		if err != nil {
			log.Error().Err(err).Str("retry-writes", cfg.RetryWrites).Msg(semLogContext + " invalid value for retry-writes")
		}
		opts.SetRetryWrites(b)
	}

	if len(cfg.Compressor) > 0 {
		opts.SetCompressors(cfg.Compressor)
	}

	if cfg.ZlibLevel != "" {
		i, err := strconv.Atoi(cfg.ZlibLevel)
		if err != nil {
			log.Error().Err(err).Str("zlib-level", cfg.ZlibLevel).Msg(semLogContext + " invalid value for zlib-level")
		}
		opts.SetZlibLevel(i)
	}

	if cfg.ZstdLevel != "" {
		i, err := strconv.Atoi(cfg.ZstdLevel)
		if err != nil {
			log.Error().Err(err).Str("zstd-level", cfg.ZstdLevel).Msg(semLogContext + " invalid value for zstd-level")
		}
		opts.SetZstdLevel(i)
	}

	readConcern := DefaultReadConcern
	if cfg.ReadConcern != "" {
		readConcern = &readconcern.ReadConcern{Level: cfg.ReadConcern}
	}
	opts.SetReadConcern(readConcern)

	return opts
}

func (cfg *Config) getAuthOptions(opts *options.ClientOptions) *options.ClientOptions {
	const semLogContext = "mongo-lks::config-set-auth-options"

	if opts == nil {
		opts = options.Client()
	}

	switch cfg.SecurityProtocol {
	case "TLS":
		log.Info().Bool("skip-verify", cfg.TLS.SkipVerify).Msg(semLogContext + " security-protocol set to TLS....")
		tlsCfg := &tls.Config{
			InsecureSkipVerify: cfg.TLS.SkipVerify,
		}
		opts.SetTLSConfig(tlsCfg)
	case "PLAIN":
		log.Info().Str("security-protocol", cfg.SecurityProtocol).Msg(semLogContext + " security-protocol set to PLAIN....nothing to do")
	default:
		log.Info().Str("security-protocol", cfg.SecurityProtocol).Msg(semLogContext + " skipping mongo security-protocol settings")
	}

	/*
	 * Simple User/password authentication
	 */
	if cfg.User != "" {
		authMechanism := "" // DefaultAuthMechanism // "SCRAM-SHA-256"
		if cfg.AuthMechanism != "" {
			authMechanism = cfg.AuthMechanism
		}
		authSource := cfg.DbName
		if cfg.AuthSource != "" {
			authSource = cfg.AuthSource
		}
		opts.SetAuth(options.Credential{
			AuthSource: authSource, Username: cfg.User, Password: cfg.Pwd, AuthMechanism: authMechanism,
		})
	}

	return opts
}

/*
 * GetConfigDefaults not applicable in an array type of config.

func GetConfigDefaults() []configuration.VarDefinition {
	return []configuration.VarDefinition{
		{"config.mongo.host", "mongodb://localhost:27017", "host reference"},
		{"config.mongo.db-name", "cpxstore", "database name"},
		{"config.mongo.bulk-write-ordered", true, "bulk write ordered"},
		{"config.mongo.write-concern", "majority", "write concern"},
		{"config.mongo.write-timeout", "120s", "write timeout"},
		{"config.mongo.pool.min-conn", "1", "min conn"},
		{"config.mongo.pool.max-conn", "20", "max conn"},
		{"config.mongo.pool.max-wait-queue-size", "1000", "max wait queue size"},
		{"config.mongo.pool.max-wait-time", "1000", "max wait time"},
		{"config.mongo.pool.max-conn-idle-time", "30000", "max conn idle time"},
		{"config.mongo.pool.max-conn-life-time", "6000000", "max conn life time"},
	}
}
*/

func EvalWriteConcern(wstr string) *writeconcern.WriteConcern {

	w := DefaultWriteConcern
	if wstr != "" {
		switch wstr {
		case "majority":
			w = writeconcern.Majority()
		case "1":
			w = &writeconcern.WriteConcern{W: 1}
		default:
			if i, err := strconv.Atoi(wstr); err == nil {
				w = &writeconcern.WriteConcern{W: i}
			}
		}
	}

	return w

}

func (c *Config) PostProcess() error {
	return nil
}
