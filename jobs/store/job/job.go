package job

import "go.mongodb.org/mongo-driver/v2/bson"
import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"

// @tpm-schematics:start-region("top-file-section")

const (
	EType           = "job"
	CollectionId    = "jobs"
	GroupAny        = "any"
	StatusAvailable = "available"
	StatusWaiting   = "waiting"
	StatusDone      = "done"
	DueDateLayout   = "20060102"
)

// @tpm-schematics:end-region("top-file-section")

type Job struct {
	Domain     string                `json:"domain,omitempty" bson:"domain,omitempty" yaml:"domain,omitempty"`
	Site       string                `json:"site,omitempty" bson:"site,omitempty" yaml:"site,omitempty"`
	Bid        string                `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et         string                `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Group      string                `json:"group,omitempty" bson:"group,omitempty" yaml:"group,omitempty"`
	Name       string                `json:"name,omitempty" bson:"name,omitempty" yaml:"name,omitempty"`
	Status     string                `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	DueDate    string                `json:"due_date,omitempty" bson:"due_date,omitempty" yaml:"due_date,omitempty"`
	Properties bson.M                `json:"properties,omitempty" bson:"properties,omitempty" yaml:"properties,omitempty"`
	Tasks      []beans.TaskReference `json:"tasks,omitempty" bson:"tasks,omitempty" yaml:"tasks,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Job) IsZero() bool {
	return s.Domain == "" && s.Site == "" && s.Bid == "" && s.Et == "" && s.Group == "" && s.Name == "" && s.Status == "" && s.DueDate == "" && len(s.Properties) == 0 && len(s.Tasks) == 0
}

type QueryResult struct {
	Records int   `json:"records,omitempty" bson:"records,omitempty" yaml:"records,omitempty"`
	Data    []Job `json:"data,omitempty" bson:"data,omitempty" yaml:"data,omitempty"`
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
