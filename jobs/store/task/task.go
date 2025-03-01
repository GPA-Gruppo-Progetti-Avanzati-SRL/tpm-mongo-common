package task

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"

// @tpm-schematics:start-region("top-file-section")

const (
	EType           = "task"
	TypeAny         = "any"
	TypeQMongo      = "q-mongo"
	StatusAvailable = "available"
	StatusClosed    = "closed"
)

// @tpm-schematics:end-region("top-file-section")

type Task struct {
	Bid        string                `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et         string                `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Status     string                `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Typ        string                `json:"typ,omitempty" bson:"typ,omitempty" yaml:"typ,omitempty"`
	JobBid     string                `json:"jobBid,omitempty" bson:"jobBid,omitempty" yaml:"jobBid,omitempty"`
	Info       beans.TaskInfo        `json:"info,omitempty" bson:"info,omitempty" yaml:"info,omitempty"`
	Partitions []partition.Partition `json:"partitions,omitempty" bson:"partitions,omitempty" yaml:"partitions,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Task) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.Status == "" && s.Typ == "" && s.JobBid == "" && s.Info.IsZero() && len(s.Partitions) == 0
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
