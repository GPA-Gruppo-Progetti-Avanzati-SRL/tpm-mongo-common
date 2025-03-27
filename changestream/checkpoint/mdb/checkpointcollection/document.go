package checkpointcollection

// @tpm-schematics:start-region("top-file-section")
const (
	CheckPointStatusActive  = "active"
	CheckPointStatusCleared = "cleared"
)

// @tpm-schematics:end-region("top-file-section")

type Document struct {
	Bid         string `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et          string `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	ResumeToken string `json:"resume_token,omitempty" bson:"resume_token,omitempty" yaml:"resume_token,omitempty"`
	At          string `json:"at,omitempty" bson:"at,omitempty" yaml:"at,omitempty"`
	ShortToken  string `json:"short_token,omitempty" bson:"short_token,omitempty" yaml:"short_token,omitempty"`
	TxnOpnIndex string `json:"txn_opn_index,omitempty" bson:"txn_opn_index,omitempty" yaml:"txn_opn_index,omitempty"`
	Status      string `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	OpCount     int32  `json:"op_count,omitempty" bson:"op_count,omitempty" yaml:"op_count,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s Document) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.ResumeToken == "" && s.At == "" && s.ShortToken == "" && s.TxnOpnIndex == "" && s.Status == "" && s.OpCount == 0
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
