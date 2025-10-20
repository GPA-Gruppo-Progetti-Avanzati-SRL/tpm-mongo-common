package duedatetrigger

import (
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
)

const (
	ExecuteOnce = "once"
	ExecuteLoop = "loop"
)

type Filter struct {
	JobGroup  string `json:"job-group,omitempty" yaml:"job-group,omitempty" mapstructure:"job-group,omitempty"`
	JobStatus string `json:"job-status,omitempty" yaml:"job-status,omitempty" mapstructure:"job-status,omitempty"`
}

type Update struct {
	JobStatus string `json:"job-status,omitempty" yaml:"job-status,omitempty" mapstructure:"job-status,omitempty"`
}

type Config struct {
	StartDate    string                  `json:"start-date,omitempty" yaml:"start-date,omitempty" mapstructure:"start-date,omitempty"`
	Mode         string                  `json:"mode,omitempty" yaml:"mode,omitempty" mapstructure:"mode,omitempty"`
	Store        mongolks.StoreReference `json:"store,omitempty" yaml:"store,omitempty" mapstructure:"store,omitempty"`
	Filter       Filter                  `json:"filter,omitempty" yaml:"filter,omitempty" mapstructure:"filter,omitempty"`
	Update       Update                  `json:"update,omitempty" yaml:"update,omitempty" mapstructure:"update,omitempty"`
	TickInterval time.Duration           `json:"tick-interval,omitempty" yaml:"tick-interval,omitempty" mapstructure:"tick-interval,omitempty"`
	CheckPointId string                  `json:"checkpoint-id,omitempty" yaml:"checkpoint-id,omitempty" mapstructure:"checkpoint-id,omitempty"`
}
