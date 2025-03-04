package querystream

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
)

const (
	OnErrorPolicyExit     = "exit"
	OnErrorPolicyContinue = "continue"
)

type Config struct {
	Id            string                           `yaml:"id,omitempty" mapstructure:"id,omitempty" json:"id,omitempty"`
	RefMetrics    *promutil.MetricsConfigReference `yaml:"ref-metrics"  mapstructure:"ref-metrics"  json:"ref-metrics,omitempty"`
	OnErrorPolicy string                           `yaml:"on-error-policy,omitempty"  mapstructure:"on-error-policy,omitempty"  json:"on-error-policy,omitempty"`
}

type ConfigOption func(*Config)

/*
   func WithRetryCount(retryCount int) ConfigOption {
	return func(options *Config) {
		options.RetryCount = retryCount
	}
}*/

func (cfg *Config) onErrorPolicy() string {
	if cfg.OnErrorPolicy == "" {
		return OnErrorPolicyContinue
	}
	return cfg.OnErrorPolicy
}
