package consumerproducer

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/factory"
	"github.com/rs/zerolog/log"
	"time"
)

const (
	OnErrorLevelFatal  = "fatal"
	OnErrorLevelSystem = "system"
	OnErrorLevelError  = "error"

	OnErrorExit   = "exit"
	OnErrorRewind = "rewind"

	WorkModeMsg   = "msg-mode"
	WorkModeBatch = "batch-mode"
)

type ServerConfig struct {
	OnWorkerTerminated string `yaml:"on-worker-terminated,omitempty" mapstructure:"on-worker-terminated,omitempty" json:"on-worker-terminated,omitempty"` // Possible values: dead-letter, exit
	StartDelay         int    `yaml:"start-delay-ms" mapstructure:"start-delay-ms" json:"start-delay-ms"`
}

type TracingCfg struct {
	SpanName string `yaml:"span-name" mapstructure:"span-name" json:"span-name"`
}

type Config struct {
	Name                string                           `yaml:"name,omitempty" mapstructure:"name,omitempty" json:"name,omitempty"`
	WorkMode            string                           `yaml:"work-mode,omitempty" mapstructure:"work-mode,omitempty" json:"work-mode,omitempty"`
	Consumer            changestream.Config              `yaml:"consumer,omitempty" mapstructure:"consumer,omitempty" json:"consumer,omitempty"`
	CheckPointSvcConfig factory.Config                   `yaml:"checkpoint-svc,omitempty" mapstructure:"checkpoint-svc,omitempty" json:"checkpoint-svc,omitempty"`
	RefMetrics          *promutil.MetricsConfigReference `yaml:"ref-metrics,omitempty"  mapstructure:"ref-metrics,omitempty"  json:"ref-metrics,omitempty"`
	TickInterval        time.Duration                    `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
	MaxBatchSize        int                              `yaml:"batch-size,omitempty" mapstructure:"batch-size,omitempty" json:"batch-size,omitempty"`
	Tracing             TracingCfg                       `yaml:"tracing,omitempty" mapstructure:"tracing,omitempty" json:"tracing,omitempty"`
}

func (c *Config) RewindEnabled() bool {
	const semLogContext = "cs-consumer-producer::config"
	switch c.Consumer.OnErrorPolicy {
	case OnErrorRewind:
		return true
	case OnErrorExit:
		return false
	case "":
		log.Warn().Msg(semLogContext + " please explicitly assign the on-error parameter (rewind, exit)")
		return false
	default:
		log.Error().Str("on-error", c.Consumer.OnErrorPolicy).Msg(semLogContext + " - invalid on error parameter (rewind, exit)")
		return false
	}
}
