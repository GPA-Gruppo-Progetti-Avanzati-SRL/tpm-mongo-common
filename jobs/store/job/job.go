package job

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"

// @tpm-schematics:start-region("top-file-section")

const (
	EType           = "job"
	TypeAny         = "any"
	StatusAvailable = "available"
	StatusClosed    = "closed"
)

// @tpm-schematics:end-region("top-file-section")

type Job struct {
	Bid    string                `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et     string                `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Typ    string                `json:"typ,omitempty" bson:"typ,omitempty" yaml:"typ,omitempty"`
	Status string                `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Info   beans.JobInfo         `json:"info,omitempty" bson:"info,omitempty" yaml:"info,omitempty"`
	Tasks  []beans.TaskReference `json:"tasks,omitempty" bson:"tasks,omitempty" yaml:"tasks,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Job) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.Typ == "" && s.Status == "" && s.Info.IsZero() && len(s.Tasks) == 0
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
