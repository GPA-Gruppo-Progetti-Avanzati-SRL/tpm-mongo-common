package beans

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type TaskInfo struct {
	MdbInstance   string `json:"mdbInstance,omitempty" bson:"mdbInstance,omitempty" yaml:"mdbInstance,omitempty"`
	MdbCollection string `json:"mdbCollection,omitempty" bson:"mdbCollection,omitempty" yaml:"mdbCollection,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s TaskInfo) IsZero() bool {
	return s.MdbInstance == "" && s.MdbCollection == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
