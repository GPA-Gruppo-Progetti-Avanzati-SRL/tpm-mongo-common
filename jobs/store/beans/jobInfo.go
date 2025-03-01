package beans

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type JobInfo struct {
	Reserved string `json:"reserved,omitempty" bson:"reserved,omitempty" yaml:"reserved,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s JobInfo) IsZero() bool {
	return s.Reserved == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
