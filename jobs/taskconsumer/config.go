package taskconsumer

const (
	OnPartitionEofMarkLeaseAsDone    = "mark-lease-as-done"
	OnPartitionErrorMarkLeaseAsError = "mark-lease-as-error"
)

type Config struct {
	Id         string `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	MetricsGId string `yaml:"metrics-gid"  mapstructure:"metrics-gid"  json:"metrics-gid,omitempty"`
	// OnPartitionEof   string                           `yaml:"on-partition-eof,omitempty"  mapstructure:"on-partition-eof,omitempty"  json:"on-partition-eof,omitempty"`
	OnPartitionError string `yaml:"on-partition-fail,omitempty"  mapstructure:"on-partition-fail,omitempty"  json:"on-partition-fail,omitempty"`
	// OnErrorPolicy string                           `yaml:"on-error-policy,omitempty"  mapstructure:"on-error-policy,omitempty"  json:"on-error-policy,omitempty"`
}

type ConfigOption func(*Config)

/*
   func WithRetryCount(retryCount int) ConfigOption {
	return func(options *Config) {
		options.RetryCount = retryCount
	}
}*/

//func (cfg *Config) onErrorPolicy() string {
//	if cfg.OnErrorPolicy == "" {
//		return OnErrorPolicyExit
//	}
//	return cfg.OnErrorPolicy
//}
