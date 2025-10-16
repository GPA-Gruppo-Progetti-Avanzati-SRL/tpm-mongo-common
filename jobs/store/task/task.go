package task

import "go.mongodb.org/mongo-driver/v2/bson"
import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"

// @tpm-schematics:start-region("top-file-section")

const (
	EType           = "task"
	AmbitAny        = "any"
	StatusAvailable = "available"
	StatusReady     = "ready"
	StatusDone      = "done"
)

// @tpm-schematics:end-region("top-file-section")

type Task struct {
	Bid        string            `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et         string            `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Status     string            `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Group      string            `json:"group,omitempty" bson:"group,omitempty" yaml:"group,omitempty"`
	Name       string            `json:"name,omitempty" bson:"name,omitempty" yaml:"name,omitempty"`
	JobId      string            `json:"job_id,omitempty" bson:"job_id,omitempty" yaml:"job_id,omitempty"`
	Properties bson.M            `json:"properties,omitempty" bson:"properties,omitempty" yaml:"properties,omitempty"`
	Partitions []beans.Partition `json:"partitions,omitempty" bson:"partitions,omitempty" yaml:"partitions,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Task) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.Status == "" && s.Group == "" && s.Name == "" && s.JobId == "" && len(s.Properties) == 0 && len(s.Partitions) == 0
}

type QueryResult struct {
	Records int    `json:"records,omitempty" bson:"records,omitempty" yaml:"records,omitempty"`
	Data    []Task `json:"data,omitempty" bson:"data,omitempty" yaml:"data,omitempty"`
}

// @tpm-schematics:start-region("bottom-file-section")

func (s Task) IsEOF() bool {
	for _, p := range s.Partitions {
		if p.Status != beans.PartitionStatusEOF {
			return false
		}
	}

	return true
}

// @tpm-schematics:end-region("bottom-file-section")
