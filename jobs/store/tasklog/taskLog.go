package tasklog

import "go.mongodb.org/mongo-driver/v2/bson"

// @tpm-schematics:start-region("top-file-section")

const (
	EType        = "task-log"
	CollectionId = "jobs-logs"
)

// @tpm-schematics:end-region("top-file-section")

type TaskLog struct {
	OId         bson.ObjectID `json:"_id,omitempty" bson:"_id,omitempty" yaml:"_id,omitempty"`
	Domain      string        `json:"domain,omitempty" bson:"domain,omitempty" yaml:"domain,omitempty"`
	Site        string        `json:"site,omitempty" bson:"site,omitempty" yaml:"site,omitempty"`
	Bid         string        `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et          string        `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Name        string        `json:"name,omitempty" bson:"name,omitempty" yaml:"name,omitempty"`
	TaskId      string        `json:"task_id,omitempty" bson:"task_id,omitempty" yaml:"task_id,omitempty"`
	Partition   int32         `json:"partition,omitempty" bson:"partition,omitempty" yaml:"partition,omitempty"`
	JobId       string        `json:"job_id,omitempty" bson:"job_id,omitempty" yaml:"job_id,omitempty"`
	BlockNumber int32         `json:"block_number,omitempty" bson:"block_number,omitempty" yaml:"block_number,omitempty"`
	Entries     []LogEntry    `json:"entries,omitempty" bson:"entries,omitempty" yaml:"entries,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s TaskLog) IsZero() bool {
	return s.OId == bson.NilObjectID && s.Domain == "" && s.Site == "" && s.Bid == "" && s.Et == "" && s.Name == "" && s.TaskId == "" && s.Partition == 0 && s.JobId == "" && s.BlockNumber == 0 && len(s.Entries) == 0
}

type QueryResult struct {
	Records int       `json:"records,omitempty" bson:"records,omitempty" yaml:"records,omitempty"`
	Data    []TaskLog `json:"data,omitempty" bson:"data,omitempty" yaml:"data,omitempty"`
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
