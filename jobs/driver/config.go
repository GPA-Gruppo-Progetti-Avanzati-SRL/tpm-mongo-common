package driver

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/worker"
	"time"
)

type StoreReference struct {
	InstanceName string `yaml:"lks,omitempty" mapstructure:"lks,omitempty" json:"lks,omitempty"`
	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
}

// ExecMode      string          `yaml:"exec-mode,omitempty" mapstructure:"exec-mode,omitempty" json:"exec-mode,omitempty"`

type Config struct {
	Store                  StoreReference  `yaml:"store,omitempty" mapstructure:"store,omitempty" json:"store,omitempty"`
	JobTypes               []string        `yaml:"job-types,omitempty" mapstructure:"job-types,omitempty" json:"job-types,omitempty"`
	ExitOnIdle             bool            `yaml:"exit-on-idle,omitempty" mapstructure:"exit-on-idle,omitempty" json:"exit-on-idle,omitempty"`
	ExitAfterMaxIterations int             `yaml:"exit-after-max-iterations,omitempty" mapstructure:"exit-after-max-iterations,omitempty" json:"exit-after-max-iterations,omitempty"`
	TickInterval           time.Duration   `yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty" json:"tick-interval,omitempty"`
	WorkersConfig          []worker.Config `yaml:"workers-config,omitempty" mapstructure:"workers-config,omitempty" json:"workers-config,omitempty"`
}
