package beans

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type TaskReference struct {
	Id     string `json:"id,omitempty" bson:"id,omitempty" yaml:"id,omitempty"`
	Status string `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	Typ    string `json:"typ,omitempty" bson:"typ,omitempty" yaml:"typ,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s TaskReference) IsZero() bool {
	return s.Id == "" && s.Status == "" && s.Typ == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
