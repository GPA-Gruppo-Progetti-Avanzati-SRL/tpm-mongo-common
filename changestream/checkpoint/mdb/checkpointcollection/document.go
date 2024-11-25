package checkpointcollection

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

type Document struct {
	Bid         string `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et          string `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	ResumeToken string `json:"resume_token,omitempty" bson:"resume_token,omitempty" yaml:"resume_token,omitempty"`
	At          string `json:"at,omitempty" bson:"at,omitempty" yaml:"at,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Document) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.ResumeToken == "" && s.At == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
