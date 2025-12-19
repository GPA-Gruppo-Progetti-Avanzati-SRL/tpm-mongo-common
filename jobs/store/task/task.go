package task

import (
	"fmt"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
)
import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"

// @tpm-schematics:start-region("top-file-section")

const (
	EType           = "task"
	CollectionId    = "jobs"
	AmbitAny        = "any"
	StatusAvailable = "available"
	StatusReady     = "ready"
	StatusDone      = "done"
	StatusError     = "error"

	SystemPropertyMaxRestarts = "_max_restarts"
)

// @tpm-schematics:end-region("top-file-section")

type Task struct {
	OId        bson.ObjectID     `json:"_id,omitempty" bson:"_id,omitempty" yaml:"_id,omitempty"`
	Domain     string            `json:"domain,omitempty" bson:"domain,omitempty" yaml:"domain,omitempty"`
	Site       string            `json:"site,omitempty" bson:"site,omitempty" yaml:"site,omitempty"`
	Bid        string            `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et         string            `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Status     string            `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Group      string            `json:"group,omitempty" bson:"group,omitempty" yaml:"group,omitempty"`
	Name       string            `json:"name,omitempty" bson:"name,omitempty" yaml:"name,omitempty"`
	JobId      string            `json:"job_id,omitempty" bson:"job_id,omitempty" yaml:"job_id,omitempty"`
	Properties bson.M            `json:"properties,omitempty" bson:"properties,omitempty" yaml:"properties,omitempty"`
	Partitions []beans.Partition `json:"partitions,omitempty" bson:"partitions,omitempty" yaml:"partitions,omitempty"`
	SysInfo    beans.SysInfo     `json:"sys_info,omitempty" bson:"sys_info,omitempty" yaml:"sys_info,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Task) IsZero() bool {
	return s.OId == bson.NilObjectID && s.Domain == "" && s.Site == "" && s.Bid == "" && s.Et == "" && s.Status == "" && s.Group == "" && s.Name == "" && s.JobId == "" && len(s.Properties) == 0 && len(s.Partitions) == 0 && s.SysInfo.IsZero()
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

func (s Task) IsError() bool {
	var withErrors bool
	for _, p := range s.Partitions {
		if p.Status == beans.PartitionStatusError {
			withErrors = true
		}

		if p.Status != beans.PartitionStatusError && p.Status != beans.PartitionStatusEOF {
			return false
		}
	}

	return withErrors
}

func (s Task) MaxRestarts() int32 {
	const semLogContext = "task::max-restarts"
	if len(s.Properties) > 0 {
		if v, ok := s.Properties[SystemPropertyMaxRestarts]; ok {
			if iv, ok := v.(int32); ok {
				return iv
			} else {
				log.Warn().Interface(SystemPropertyMaxRestarts, v).Str(SystemPropertyMaxRestarts+"-type", fmt.Sprintf("%T", v)).Msg(semLogContext)
			}
		}
	}

	return 0
}

func (s Task) RestartableOnError(prtNdx int) bool {
	maxRetries := s.MaxRestarts()
	numPartitionErrors := s.Partitions[prtNdx-1].SysInfo.Errors

	if maxRetries > numPartitionErrors {
		return true
	}

	return false
}

// @tpm-schematics:end-region("bottom-file-section")
