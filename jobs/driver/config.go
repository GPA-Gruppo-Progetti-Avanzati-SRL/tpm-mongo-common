package driver

import (
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
)

// ExecMode      string          `yaml:"exec-mode,omitempty" mapstructure:"exec-mode,omitempty" json:"exec-mode,omitempty"`

type Config struct {
	JobsStore              mongolks.StoreReference `yaml:"jobs-store,omitempty" mapstructure:"jobs-store,omitempty" json:"jobs-store,omitempty"`
	JobsLogsStore          mongolks.StoreReference `yaml:"jobs-logs-store,omitempty" mapstructure:"jobs-logs-store,omitempty" json:"jobs-logs-store,omitempty"`
	JobTypes               []string                `yaml:"job-types,omitempty" mapstructure:"job-types,omitempty" json:"job-types,omitempty"`
	ExitOnIdle             bool                    `yaml:"exit-on-idle,omitempty" mapstructure:"exit-on-idle,omitempty" json:"exit-on-idle,omitempty"`
	ExitAfterMaxIterations int                     `yaml:"exit-after-max-iterations,omitempty" mapstructure:"exit-after-max-iterations,omitempty" json:"exit-after-max-iterations,omitempty"`
	TickInterval           time.Duration           `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
}
