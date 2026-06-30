package beans

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type TaskReference struct {
	Id     string `json:"id,omitempty" bson:"id,omitempty" yaml:"id,omitempty"`
	Status string `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	JobId  string `json:"job_id,omitempty" bson:"job_id,omitempty" yaml:"job_id,omitempty"`
	Domain string `json:"domain,omitempty" bson:"domain,omitempty" yaml:"domain,omitempty"`
	Site   string `json:"site,omitempty" bson:"site,omitempty" yaml:"site,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s TaskReference) IsZero() bool {
	return s.Id == "" && s.Status == "" && s.JobId == "" && s.Domain == "" && s.Site == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
