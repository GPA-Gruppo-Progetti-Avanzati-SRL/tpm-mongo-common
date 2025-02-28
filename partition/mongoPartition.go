package partition

// @tpm-schematics:start-region("top-file-section")

const (
	MongoPartitionType = "mongo-partition"
)

// @tpm-schematics:end-region("top-file-section")

type MongoPartition struct {
	//Instance   string `json:"instance,omitempty" bson:"instance,omitempty" yaml:"instance,omitempty"`
	//Collection string `json:"collection,omitempty" bson:"collection,omitempty" yaml:"collection,omitempty"`
	Filter string `json:"filter,omitempty" bson:"filter,omitempty" yaml:"filter,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s MongoPartition) IsZero() bool {
	return /* s.Instance == "" && s.Collection == "" && */ s.Filter == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
