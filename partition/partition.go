package partition

import "fmt"

// @tpm-schematics:start-region("top-file-section")

const (
	StatusAvailable = "available"
	StatusClosed    = "closed"

	QueryDocumentPartitionFieldName = "_np"
)

// @tpm-schematics:end-region("top-file-section")

type Partition struct {
	Bid             string         `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Gid             string         `json:"_gid,omitempty" bson:"_gid,omitempty" yaml:"_gid,omitempty"`
	Et              string         `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	PartitionNumber int32          `json:"partitionNumber,omitempty" bson:"partitionNumber,omitempty" yaml:"partitionNumber,omitempty"`
	Status          string         `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Etag            int64          `json:"etag" bson:"etag" yaml:"etag"`
	Mongo           MongoPartition `json:"mongo,omitempty" bson:"mongo,omitempty" yaml:"mongo,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Partition) IsZero() bool {
	return s.Bid == "" && s.Gid == "" && s.Et == "" && s.PartitionNumber == 0 && s.Status == "" && s.Etag == 0 && s.Mongo.IsZero()
}

// @tpm-schematics:start-region("bottom-file-section")

func Id(collectionName string, partitionNumber int32) string {
	return fmt.Sprintf("%s:%03d", collectionName, partitionNumber)
}

// @tpm-schematics:end-region("bottom-file-section")
