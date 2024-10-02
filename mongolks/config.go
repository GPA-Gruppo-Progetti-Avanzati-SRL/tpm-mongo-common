package mongolks

import (
	"go.mongodb.org/mongo-driver/mongo/writeconcern"
	"strconv"
	"time"
)

const MongoDbDefaultInstanceName = "default"

type CollectionCfg struct {
	Id   string
	Name string
}

type CollectionsCfg []CollectionCfg

type TLSConfig struct {
	CaLocation string `json:"ca-location" mapstructure:"ca-location" yaml:"ca-location"`
	SkipVerify bool   `json:"skip-verify" mapstructure:"skip-verify" yaml:"skip-verify"`
}

type PoolConfig struct {
	MinConn               int `mapstructure:"min-conn" json:"min-conn" yaml:"min-conn"`
	MaxConn               int `mapstructure:"max-conn" json:"max-conn" yaml:"max-conn"`
	MaxWaitQueueSize      int `mapstructure:"max-wait-queue-size" json:"max-wait-queue-size" yaml:"max-wait-queue-size"`
	MaxWaitTime           int `mapstructure:"max-wait-time" json:"max-wait-time" yaml:"max-wait-time"`
	MaxConnectionIdleTime int `mapstructure:"max-conn-idle-time" json:"max-conn-idle-time" yaml:"max-conn-idle-time"`
	MaxConnectionLifeTime int `mapstructure:"max-conn-life-time" json:"max-conn-life-time" yaml:"max-conn-life-time"`
}

type Config struct {
	Name             string
	Host             string
	DbName           string         `mapstructure:"db-name,omitempty" json:"db-name,omitempty" yaml:"db-name,omitempty"`
	User             string         `mapstructure:"user,omitempty" json:"user,omitempty" yaml:"user,omitempty"`
	Pwd              string         `mapstructure:"pwd,omitempty" json:"pwd,omitempty" yaml:"pwd,omitempty"`
	AuthMechanism    string         `mapstructure:"authMechanism,omitempty" json:"authMechanism,omitempty" yaml:"authMechanism,omitempty"`
	AuthSource       string         `mapstructure:"authSource,omitempty" json:"authSource,omitempty" yaml:"authSource,omitempty"`
	Pool             PoolConfig     `mapstructure:"pool,omitempty" json:"pool,omitempty" yaml:"pool,omitempty"`
	BulkWriteOrdered bool           `mapstructure:"bulk-write-ordered,omitempty" json:"bulk-write-ordered,omitempty" yaml:"bulk-write-ordered,omitempty"`
	WriteConcern     string         `mapstructure:"write-concern,omitempty" json:"write-concern,omitempty" yaml:"write-concern,omitempty"`
	WriteTimeout     string         `mapstructure:"write-timeout,omitempty" json:"write-timeout,omitempty" yaml:"write-timeout,omitempty"`
	Collections      CollectionsCfg `mapstructure:"collections,omitempty" json:"collections,omitempty" yaml:"collections,omitempty"`
	SecurityProtocol string         `mapstructure:"security-protocol,omitempty" json:"security-protocol,omitempty" yaml:"security-protocol,omitempty"`
	TLS              TLSConfig      `json:"tls" mapstructure:"tls" yaml:"tls"`
	ConnectTimeout   time.Duration  `mapstructure:"connect-timeout,omitempty" json:"connect-timeout,omitempty" yaml:"connect-timeout,omitempty"`
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
			writeconcern.New(writeconcern.WMajority())
		case "1":
			w = writeconcern.New(writeconcern.W(1))
		default:
			if i, err := strconv.Atoi(wstr); err == nil {
				w = writeconcern.New(writeconcern.W(i))
			}
		}
	}

	return w

}

func (c *Config) PostProcess() error {
	return nil
}
