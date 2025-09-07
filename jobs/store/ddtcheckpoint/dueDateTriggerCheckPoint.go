package ddtcheckpoint

// @tpm-schematics:start-region("top-file-section")

const (
	EType = "ddtchk"
)

// @tpm-schematics:end-region("top-file-section")

type DueDateTriggerCheckPoint struct {
	Bid     string `json:"_bid,omitempty" bson:"_bid,omitempty" yaml:"_bid,omitempty"`
	Et      string `json:"_et,omitempty" bson:"_et,omitempty" yaml:"_et,omitempty"`
	Ambit   string `json:"ambit,omitempty" bson:"ambit,omitempty" yaml:"ambit,omitempty"`
	Status  string `json:"status,omitempty" bson:"status,omitempty" yaml:"status,omitempty"`
	DueDate string `json:"due_date,omitempty" bson:"due_date,omitempty" yaml:"due_date,omitempty"`

	// @tpm-schematics:start-region("struct-section")
	// @tpm-schematics:end-region("struct-section")
}

func (s DueDateTriggerCheckPoint) IsZero() bool {
	return s.Bid == "" && s.Et == "" && s.Ambit == "" && s.Status == "" && s.DueDate == ""
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
