package beans

// @tpm-schematics:start-region("top-file-section")

const (
	PartitionTypeMongo = "mongo-partition"
)

// @tpm-schematics:end-region("top-file-section")

type PartitionInfo struct {
	MdbFilter string `json:"mdbFilter,omitempty" bson:"mdbFilter,omitempty" yaml:"mdbFilter,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s PartitionInfo) IsZero() bool {
	return s.MdbFilter == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
