package worker

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer"
	"time"
)

const (
	OnErrorLevelFatal  = "fatal"
	OnErrorLevelSystem = "system"
	OnErrorLevelError  = "error"

	OnErrorExit     = "exit"
	OnErrorContinue = "continue"

	WorkModeOnEvent  = "on-event-mode"
	WorkModeOnEvents = "on-events-mode"
)

type ServerConfig struct {
	OnWorkerTerminated string `yaml:"on-worker-terminated,omitempty" mapstructure:"on-worker-terminated,omitempty" json:"on-worker-terminated,omitempty"` // Possible values: dead-letter, exit
	StartDelay         int    `yaml:"start-delay-ms" mapstructure:"start-delay-ms" json:"start-delay-ms"`
}

type TracingCfg struct {
	SpanName string `yaml:"span-name" mapstructure:"span-name" json:"span-name"`
}

type Config struct {
	Name          string              `yaml:"name,omitempty" mapstructure:"name,omitempty" json:"name,omitempty"`
	WorkMode      string              `yaml:"work-mode,omitempty" mapstructure:"work-mode,omitempty" json:"work-mode,omitempty"`
	OnErrorPolicy string              `yaml:"on-error-policy,omitempty"  mapstructure:"on-error-policy,omitempty"  json:"on-error-policy,omitempty"`
	Consumer      taskconsumer.Config `yaml:"consumer,omitempty" mapstructure:"consumer,omitempty" json:"consumer,omitempty"`
	MetricsGId    string              `yaml:"metrics-gid"  mapstructure:"metrics-gid"  json:"metrics-gid,omitempty"`
	TickInterval  time.Duration       `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
	MaxBatchSize  int                 `yaml:"max-batch-size,omitempty" mapstructure:"max-batch-size,omitempty" json:"max-batch-size,omitempty"`
	Tracing       TracingCfg          `yaml:"tracing,omitempty" mapstructure:"tracing,omitempty" json:"tracing,omitempty"`

	Processor Processor `yaml:"-" mapstructure:"-" json:"-"`
}
