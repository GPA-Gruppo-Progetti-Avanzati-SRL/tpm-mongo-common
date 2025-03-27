package checkpointcollection

import (
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

func UpdateMethodsGoInfo() string {
	i := fmt.Sprintf("tpm_morphia query filter support generated for %s package on %s", "author", time.Now().String())
	return i
}

type UnsetMode int64

const (
	UnSpecified     UnsetMode = 0
	KeepCurrent               = 1
	UnsetData                 = 2
	SetData2Default           = 3
)

type UnsetOption func(uopt *UnsetOptions)

type UnsetOptions struct {
	DefaultMode UnsetMode
	Bid         UnsetMode
	Et          UnsetMode
	ResumeToken UnsetMode
	At          UnsetMode
	ShortToken  UnsetMode
	TxnOpnIndex UnsetMode
	Status      UnsetMode
	OpCount     UnsetMode
}

func (uo *UnsetOptions) ResolveUnsetMode(um UnsetMode) UnsetMode {
	if um == UnSpecified {
		um = uo.DefaultMode
	}

	return um
}

func WithDefaultUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.DefaultMode = m
	}
}
func WithBidUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Bid = m
	}
}
func WithEtUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Et = m
	}
}
func WithResumeTokenUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.ResumeToken = m
	}
}
func WithAtUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.At = m
	}
}
func WithShortTokenUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.ShortToken = m
	}
}
func WithTxnOpnIndexUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.TxnOpnIndex = m
	}
}
func WithStatusUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Status = m
	}
}
func WithOpCountUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.OpCount = m
	}
}

type UpdateOption func(ud *UpdateDocument)
type UpdateOptions []UpdateOption

// GetUpdateDocumentFromOptions convenience method to create an update document from single updates instead of a whole object
func GetUpdateDocumentFromOptions(opts ...UpdateOption) UpdateDocument {
	ud := UpdateDocument{}
	for _, o := range opts {
		o(&ud)
	}

	return ud
}

// GetUpdateDocument
// Convenience method to create an Update Document from the values of the top fields of the object. The convenience is in the handling
// the unset because if I pass an empty struct to the update it generates an empty object anyway in the db. Handling the unset eliminates
// the issue and delete an existing value without creating an empty struct.
func GetUpdateDocument(obj *Document, opts ...UnsetOption) UpdateDocument {

	uo := &UnsetOptions{DefaultMode: KeepCurrent}
	for _, o := range opts {
		o(uo)
	}

	ud := UpdateDocument{}
	ud.setOrUnset_bid(obj.Bid, uo.ResolveUnsetMode(uo.Bid))
	ud.setOrUnset_et(obj.Et, uo.ResolveUnsetMode(uo.Et))
	ud.setOrUnsetResume_token(obj.ResumeToken, uo.ResolveUnsetMode(uo.ResumeToken))
	ud.setOrUnsetAt(obj.At, uo.ResolveUnsetMode(uo.At))
	ud.setOrUnsetShort_token(obj.ShortToken, uo.ResolveUnsetMode(uo.ShortToken))
	ud.setOrUnsetTxn_opn_index(obj.TxnOpnIndex, uo.ResolveUnsetMode(uo.TxnOpnIndex))
	ud.setOrUnsetStatus(obj.Status, uo.ResolveUnsetMode(uo.Status))
	ud.setOrUnsetOp_count(obj.OpCount, uo.ResolveUnsetMode(uo.OpCount))

	return ud
}

// Set_bid No Remarks
func (ud *UpdateDocument) Set_bid(p string) *UpdateDocument {
	mName := fmt.Sprintf(BidFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// Unset_bid No Remarks
func (ud *UpdateDocument) Unset_bid() *UpdateDocument {
	mName := fmt.Sprintf(BidFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnset_bid No Remarks
func (ud *UpdateDocument) setOrUnset_bid(p string, um UnsetMode) {
	if p != "" {
		ud.Set_bid(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.Unset_bid()
		case SetData2Default:
			ud.Unset_bid()
		}
	}
}

func UpdateWith_bid(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.Set_bid(p)
		} else {
			ud.Unset_bid()
		}
	}
}

// @tpm-schematics:start-region("-bid-field-update-section")
// @tpm-schematics:end-region("-bid-field-update-section")

// Set_et No Remarks
func (ud *UpdateDocument) Set_et(p string) *UpdateDocument {
	mName := fmt.Sprintf(EtFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// Unset_et No Remarks
func (ud *UpdateDocument) Unset_et() *UpdateDocument {
	mName := fmt.Sprintf(EtFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnset_et No Remarks
func (ud *UpdateDocument) setOrUnset_et(p string, um UnsetMode) {
	if p != "" {
		ud.Set_et(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.Unset_et()
		case SetData2Default:
			ud.Unset_et()
		}
	}
}

func UpdateWith_et(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.Set_et(p)
		} else {
			ud.Unset_et()
		}
	}
}

// @tpm-schematics:start-region("-et-field-update-section")
// @tpm-schematics:end-region("-et-field-update-section")

// SetResume_token No Remarks
func (ud *UpdateDocument) SetResume_token(p string) *UpdateDocument {
	mName := fmt.Sprintf(ResumeTokenFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetResume_token No Remarks
func (ud *UpdateDocument) UnsetResume_token() *UpdateDocument {
	mName := fmt.Sprintf(ResumeTokenFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetResume_token No Remarks
func (ud *UpdateDocument) setOrUnsetResume_token(p string, um UnsetMode) {
	if p != "" {
		ud.SetResume_token(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetResume_token()
		case SetData2Default:
			ud.UnsetResume_token()
		}
	}
}

func UpdateWithResume_token(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetResume_token(p)
		} else {
			ud.UnsetResume_token()
		}
	}
}

// @tpm-schematics:start-region("resume-token-field-update-section")
// @tpm-schematics:end-region("resume-token-field-update-section")

// SetAt No Remarks
func (ud *UpdateDocument) SetAt(p string) *UpdateDocument {
	mName := fmt.Sprintf(AtFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetAt No Remarks
func (ud *UpdateDocument) UnsetAt() *UpdateDocument {
	mName := fmt.Sprintf(AtFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetAt No Remarks
func (ud *UpdateDocument) setOrUnsetAt(p string, um UnsetMode) {
	if p != "" {
		ud.SetAt(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetAt()
		case SetData2Default:
			ud.UnsetAt()
		}
	}
}

func UpdateWithAt(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetAt(p)
		} else {
			ud.UnsetAt()
		}
	}
}

// @tpm-schematics:start-region("at-field-update-section")
// @tpm-schematics:end-region("at-field-update-section")

// SetShort_token No Remarks
func (ud *UpdateDocument) SetShort_token(p string) *UpdateDocument {
	mName := fmt.Sprintf(ShortTokenFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetShort_token No Remarks
func (ud *UpdateDocument) UnsetShort_token() *UpdateDocument {
	mName := fmt.Sprintf(ShortTokenFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetShort_token No Remarks
func (ud *UpdateDocument) setOrUnsetShort_token(p string, um UnsetMode) {
	if p != "" {
		ud.SetShort_token(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetShort_token()
		case SetData2Default:
			ud.UnsetShort_token()
		}
	}
}

func UpdateWithShort_token(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetShort_token(p)
		} else {
			ud.UnsetShort_token()
		}
	}
}

// @tpm-schematics:start-region("short-token-field-update-section")
// @tpm-schematics:end-region("short-token-field-update-section")

// SetTxn_opn_index No Remarks
func (ud *UpdateDocument) SetTxn_opn_index(p string) *UpdateDocument {
	mName := fmt.Sprintf(TxnOpnIndexFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetTxn_opn_index No Remarks
func (ud *UpdateDocument) UnsetTxn_opn_index() *UpdateDocument {
	mName := fmt.Sprintf(TxnOpnIndexFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetTxn_opn_index No Remarks
func (ud *UpdateDocument) setOrUnsetTxn_opn_index(p string, um UnsetMode) {
	if p != "" {
		ud.SetTxn_opn_index(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetTxn_opn_index()
		case SetData2Default:
			ud.UnsetTxn_opn_index()
		}
	}
}

func UpdateWithTxn_opn_index(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetTxn_opn_index(p)
		} else {
			ud.UnsetTxn_opn_index()
		}
	}
}

// @tpm-schematics:start-region("txn-opn-index-field-update-section")
// @tpm-schematics:end-region("txn-opn-index-field-update-section")

// SetStatus No Remarks
func (ud *UpdateDocument) SetStatus(p string) *UpdateDocument {
	mName := fmt.Sprintf(StatusFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetStatus No Remarks
func (ud *UpdateDocument) UnsetStatus() *UpdateDocument {
	mName := fmt.Sprintf(StatusFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetStatus No Remarks
func (ud *UpdateDocument) setOrUnsetStatus(p string, um UnsetMode) {
	if p != "" {
		ud.SetStatus(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetStatus()
		case SetData2Default:
			ud.UnsetStatus()
		}
	}
}

func UpdateWithStatus(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetStatus(p)
		} else {
			ud.UnsetStatus()
		}
	}
}

// @tpm-schematics:start-region("status-field-update-section")
// @tpm-schematics:end-region("status-field-update-section")

// SetOp_count No Remarks
func (ud *UpdateDocument) SetOp_count(p int32) *UpdateDocument {
	mName := fmt.Sprintf(OpCountFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetOp_count No Remarks
func (ud *UpdateDocument) UnsetOp_count() *UpdateDocument {
	mName := fmt.Sprintf(OpCountFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetOp_count No Remarks
func (ud *UpdateDocument) setOrUnsetOp_count(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetOp_count(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetOp_count()
		case SetData2Default:
			ud.UnsetOp_count()
		}
	}
}

func UpdateWithOp_count(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetOp_count(p)
		} else {
			ud.UnsetOp_count()
		}
	}
}

// @tpm-schematics:start-region("op-count-field-update-section")

func (ud *UpdateDocument) IncOp_count(p int32) *UpdateDocument {
	mName := fmt.Sprintf(OpCountFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: "$inc", Value: bson.E{Key: mName, Value: p}}
	})
	return ud
}

func UpdateWithIncrementOp_count(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		ud.IncOp_count(p)
	}
}

// @tpm-schematics:end-region("op-count-field-update-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
