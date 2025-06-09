package beans

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type TaskReference struct {
	Id             string `json:"id,omitempty" bson:"id,omitempty" yaml:"id,omitempty"`
	Status         string `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	DataSourceType string `json:"data_source_type,omitempty" bson:"data_source_type,omitempty" yaml:"data_source_type,omitempty"`
	StreamType     string `json:"stream_type,omitempty" bson:"stream_type,omitempty" yaml:"stream_type,omitempty"`
	JobId          string `json:"job_id,omitempty" bson:"job_id,omitempty" yaml:"job_id,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s TaskReference) IsZero() bool {
	return s.Id == "" && s.Status == "" && s.DataSourceType == "" && s.StreamType == "" && s.JobId == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
