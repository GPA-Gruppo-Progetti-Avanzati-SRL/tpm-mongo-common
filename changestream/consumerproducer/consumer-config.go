package consumerproducer

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

const (
	OnErrorPolicyExit     = "exit"
	OnErrorPolicyContinue = "continue"
)

// ChangeStreamOptions derived from mongo/options/mongooptions.go but with some modifications to make it suitable to define in a yaml file.
// not options are used.
type ChangeStreamOptions struct {
	// The maximum number of documents to be included in each batch returned by the server.
	BatchSize *int32 `yaml:"batch-size,omitempty" mapstructure:"batch-size,omitempty" json:"batch-size,omitempty"`

	// Specifies how the updated document should be returned in change notifications for update operations. The default
	// is options.Default, which means that only partial update deltas will be included in the change notification.
	FullDocument *options.FullDocument `yaml:"full-document,omitempty" mapstructure:"full-document,omitempty" json:"full-document,omitempty"`

	// Specifies how the pre-update document should be returned in change notifications for update operations. The default
	// is options.Off, which means that the pre-update document will not be included in the change notification.
	FullDocumentBeforeChange *options.FullDocument `yaml:"before-full-document,omitempty" mapstructure:"before-full-document,omitempty" json:"before-full-document,omitempty"`

	// The maximum amount of time that the server should wait for new documents to satisfy a tailable cursor query.
	MaxAwaitTime *time.Duration `yaml:"max-await-time,omitempty" mapstructure:"max-await-time,omitempty" json:"max-await-time,omitempty"`

	// ShowExpandedEvents specifies whether the server will return an expanded list of change stream events. Additional
	// events include: createIndexes, dropIndexes, modify, create, shardCollection, reshardCollection and
	// refineCollectionShardKey. This option is only valid for MongoDB versions >= 6.0.
	ShowExpandedEvents *bool `yaml:"show-expanded-events,omitempty" mapstructure:"show-expanded-events,omitempty" json:"show-expanded-events,omitempty"`

	// If specified, the change stream will only return changes that occurred at or after the given timestamp. This
	// option is only valid for MongoDB versions >= 4.0. If this is specified, ResumeAfter and StartAfter must not be
	// set.
	StartAtOperationTime string `yaml:"start-at-operation-time,omitempty" mapstructure:"start-at-operation-time,omitempty" json:"start-at-operation-time,omitempty"`

	// Custom options to be added to the $changeStream stage in the initial aggregate. Key-value pairs of the BSON map should
	// correlate with desired option names and values. Values must be Marshalable. Custom pipeline options bypass client-side
	// validation. Prefer using non-custom options where possible.
	Pl string `yaml:"pipeline,omitempty" mapstructure:"pipeline,omitempty" json:"pipeline,omitempty"`
}

// CheckPointServiceCfg     *factory.Config                  `yaml:"checkpoint-svc,omitempty"  mapstructure:"checkpoint-svc,omitempty"  json:"checkpoint-svc,omitempty"`

type ConsumerConfig struct {
	Id                       string                              `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	MongoInstance            string                              `yaml:"lks-name,omitempty" mapstructure:"lks-name,omitempty" json:"lks-name,omitempty"`
	CollectionId             string                              `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
	RefMetrics               *promutil.MetricsConfigReference    `yaml:"ref-metrics"  mapstructure:"ref-metrics"  json:"ref-metrics,omitempty"`
	VerifyOutOfSequenceError bool                                `yaml:"out-of-seq-error"  mapstructure:"out-of-seq-error"  json:"out-of-seq-error"`
	OnErrorPolicy            string                              `yaml:"on-error-policy,omitempty"  mapstructure:"on-error-policy,omitempty"  json:"on-error-policy,omitempty"`
	RetryCount               int                                 `yaml:"retry-count,omitempty"  mapstructure:"retry-count,omitempty"  json:"retry-count,omitempty"`
	ChangeStream             ChangeStreamOptions                 `yaml:"change-stream-opts,omitempty"  mapstructure:"change-stream-opts,omitempty"  json:"change-stream-opts,omitempty"`
	CheckPointSvc            checkpoint.ResumeTokenCheckpointSvc `yaml:"-"  mapstructure:"-"  json:"-"`
}

type ConsumerConfigOption func(*ConsumerConfig)

func WithRetryCount(retryCount int) ConsumerConfigOption {
	return func(options *ConsumerConfig) {
		options.RetryCount = retryCount
	}
}

func WithCheckpointSvc(svc checkpoint.ResumeTokenCheckpointSvc) ConsumerConfigOption {
	return func(options *ConsumerConfig) {
		options.CheckPointSvc = svc
	}
}

func (cfg *ConsumerConfig) ChangeOptions() (options.ChangeStreamOptions, error) {
	const semLogContext = "watcher::config-change-options"

	var err error
	var token checkpoint.ResumeToken

	opts := options.ChangeStreamOptions{
		BatchSize:                cfg.ChangeStream.BatchSize,
		Collation:                nil,
		Comment:                  nil,
		FullDocument:             cfg.ChangeStream.FullDocument,
		FullDocumentBeforeChange: cfg.ChangeStream.FullDocumentBeforeChange,
		MaxAwaitTime:             cfg.ChangeStream.MaxAwaitTime,
		ResumeAfter:              nil,
		ShowExpandedEvents:       cfg.ChangeStream.ShowExpandedEvents,
		StartAtOperationTime:     nil,
		StartAfter:               nil,
		Custom:                   nil,
		CustomPipeline:           nil,
	}

	if cfg.CheckPointSvc != nil {
		token, err = cfg.CheckPointSvc.Retrieve(cfg.Id)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return opts, err
		}
		if !token.IsZero() {
			opts.SetResumeAfter(token.MustToBsonM())
		}
	}

	return opts, nil
}

func (cfg *ConsumerConfig) Pipeline() (mongo.Pipeline, error) {
	const semLogContext = "watcher::config-pipeline"

	if cfg.ChangeStream.Pl == "" {
		return mongo.Pipeline{}, nil
	}

	pl, err := util.UnmarshalJson2ArrayOfBsonD([]byte(cfg.ChangeStream.Pl), true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return mongo.Pipeline{}, err
	}

	return pl, err
}

func (cfg *ConsumerConfig) OnWatcherErrorPolicy() string {
	if cfg.OnErrorPolicy == "" {
		return OnErrorPolicyContinue
	}
	return cfg.OnErrorPolicy
}

func (cfg *ConsumerConfig) onConsumerErrorPolicy() string {
	if cfg.OnErrorPolicy == "" {
		return OnErrorPolicyContinue
	}
	return cfg.OnErrorPolicy
}

var defaultChangeOptions = options.ChangeStreamOptions{
	BatchSize:                nil,
	Collation:                nil,
	Comment:                  nil,
	FullDocument:             nil,
	FullDocumentBeforeChange: nil,
	MaxAwaitTime:             nil,
	ResumeAfter:              nil,
	ShowExpandedEvents:       nil,
	StartAtOperationTime:     nil,
	StartAfter:               nil,
	Custom:                   nil,
	CustomPipeline:           nil,
}
