package partition

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
)

// @tpm-schematics:start-region("top-file-section")

const (
	EType                           = "task"
	TypeAny                         = "any"
	TypeQMongo                      = "q-mongo"
	StatusAvailable                 = "available"
	StatusClosed                    = "closed"
	StatusEOF                       = "EOF"
	QueryDocumentPartitionFieldName = "_np"
)

// @tpm-schematics:end-region("top-file-section")

type Partition struct {
	Bid             string              `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Gid             string              `json:"_gid,omitempty" bson:"_gid,omitempty" yaml:"_gid,omitempty"`
	Et              string              `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	PartitionNumber int32               `json:"partitionNumber,omitempty" bson:"partitionNumber,omitempty" yaml:"partitionNumber,omitempty"`
	Status          string              `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Etag            int64               `json:"etag" bson:"etag" yaml:"etag"`
	Info            beans.PartitionInfo `json:"info,omitempty" bson:"info,omitempty" yaml:"info,omitempty"`
	Errors          int32               `json:"errors,omitempty" bson:"errors,omitempty" yaml:"errors,omitempty"`
	Acquisitions    int32               `json:"acquisitions,omitempty" bson:"acquisitions,omitempty" yaml:"acquisitions,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Partition) IsZero() bool {
	return s.Bid == "" && s.Gid == "" && s.Et == "" && s.PartitionNumber == 0 && s.Status == "" && s.Etag == 0 && s.Info.IsZero() && s.Errors == 0 && s.Acquisitions == 0
}

// @tpm-schematics:start-region("bottom-file-section")

func Id(partitionGroup string, partitionNumber int32) string {
	return fmt.Sprintf("%s:%03d", partitionGroup, partitionNumber)
}

// @tpm-schematics:end-region("bottom-file-section")
