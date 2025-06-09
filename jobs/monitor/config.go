package monitor

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"time"
)

type Config struct {
	JobTypes      []string        `yaml:"job-types,omitempty" mapstructure:"job-types,omitempty" json:"job-types,omitempty"`
	ExecMode      string          `yaml:"exec-mode,omitempty" mapstructure:"exec-mode,omitempty" json:"exec-mode,omitempty"`
	TickInterval  time.Duration   `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
	WorkersConfig []worker.Config `yaml:"workers-config,omitempty" mapstructure:"workers-config,omitempty" json:"workers-config,omitempty"`
}
