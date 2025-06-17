package task

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"

// @tpm-schematics:start-region("top-file-section")

const (
	EType          = "task"
	AmbitAny       = "any"
	ProcessorIdAny = "any"
	//TypeAny                = "any"
	TypeQMongo             = "q-mongo"
	StatusAvailable        = "available"
	StatusReady            = "ready"
	StatusDone             = "done"
	DataStreamTypeInfinite = "infinite"
	DataStreamTypeFinite   = "finite"
)

// @tpm-schematics:end-region("top-file-section")

type Task struct {
	Bid            string                `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et             string                `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Status         string                `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Ambit          string                `json:"ambit,omitempty" bson:"ambit,omitempty" yaml:"ambit,omitempty"`
	DataSourceType string                `json:"data_source_type,omitempty" bson:"data_source_type,omitempty" yaml:"data_source_type,omitempty"`
	StreamType     string                `json:"stream_type,omitempty" bson:"stream_type,omitempty" yaml:"stream_type,omitempty"`
	ProcessorId    string                `json:"processor_id,omitempty" bson:"processor_id,omitempty" yaml:"processor_id,omitempty"`
	JobId          string                `json:"job_id,omitempty" bson:"job_id,omitempty" yaml:"job_id,omitempty"`
	Info           beans.TaskInfo        `json:"info,omitempty" bson:"info,omitempty" yaml:"info,omitempty"`
	Partitions     []partition.Partition `json:"partitions,omitempty" bson:"partitions,omitempty" yaml:"partitions,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Task) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.Status == "" && s.Ambit == "" && s.DataSourceType == "" && s.StreamType == "" && s.ProcessorId == "" && s.JobId == "" && s.Info.IsZero() && len(s.Partitions) == 0
}

// @tpm-schematics:start-region("bottom-file-section")

func (s Task) IsEOF() bool {
	for _, p := range s.Partitions {
		if p.Status != partition.StatusEOF {
			return false
		}
	}

	return true
}

// @tpm-schematics:end-region("bottom-file-section")
