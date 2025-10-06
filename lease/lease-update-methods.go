package lease

import (
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
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
	DefaultMode  UnsetMode
	Bid          UnsetMode
	Et           UnsetMode
	Gid          UnsetMode
	LeaseId      UnsetMode
	Data         UnsetMode
	Status       UnsetMode
	Etag         UnsetMode
	Duration     UnsetMode
	Ts           UnsetMode
	Ttl          UnsetMode
	Errors       UnsetMode
	Acquisitions UnsetMode
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
func WithGidUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Gid = m
	}
}
func WithLeaseIdUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.LeaseId = m
	}
}
func WithDataUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Data = m
	}
}
func WithStatusUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Status = m
	}
}
func WithEtagUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Etag = m
	}
}
func WithDurationUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Duration = m
	}
}
func WithTsUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Ts = m
	}
}
func WithTtlUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Ttl = m
	}
}
func WithErrorsUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Errors = m
	}
}
func WithAcquisitionsUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Acquisitions = m
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
func GetUpdateDocument(obj *Lease, opts ...UnsetOption) UpdateDocument {

	uo := &UnsetOptions{DefaultMode: KeepCurrent}
	for _, o := range opts {
		o(uo)
	}

	ud := UpdateDocument{}
	ud.setOrUnset_bid(obj.Bid, uo.ResolveUnsetMode(uo.Bid))
	ud.setOrUnset_et(obj.Et, uo.ResolveUnsetMode(uo.Et))
	ud.setOrUnset_gid(obj.Gid, uo.ResolveUnsetMode(uo.Gid))
	ud.setOrUnsetLeaseId(obj.LeaseId, uo.ResolveUnsetMode(uo.LeaseId))
	ud.setOrUnsetData(obj.Data, uo.ResolveUnsetMode(uo.Data))
	ud.setOrUnsetStatus(obj.Status, uo.ResolveUnsetMode(uo.Status))
	ud.setOrUnsetEtag(obj.Etag, uo.ResolveUnsetMode(uo.Etag))
	ud.setOrUnsetDuration(obj.Duration, uo.ResolveUnsetMode(uo.Duration))
	ud.setOrUnsetTs(obj.Ts, uo.ResolveUnsetMode(uo.Ts))
	ud.setOrUnsetTtl(obj.Ttl, uo.ResolveUnsetMode(uo.Ttl))
	ud.setOrUnsetErrors(obj.Errors, uo.ResolveUnsetMode(uo.Errors))
	ud.setOrUnsetAcquisitions(obj.Acquisitions, uo.ResolveUnsetMode(uo.Acquisitions))

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

// Set_gid No Remarks
func (ud *UpdateDocument) Set_gid(p string) *UpdateDocument {
	mName := fmt.Sprintf(GidFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// Unset_gid No Remarks
func (ud *UpdateDocument) Unset_gid() *UpdateDocument {
	mName := fmt.Sprintf(GidFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnset_gid No Remarks
func (ud *UpdateDocument) setOrUnset_gid(p string, um UnsetMode) {
	if p != "" {
		ud.Set_gid(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.Unset_gid()
		case SetData2Default:
			ud.Unset_gid()
		}
	}
}

func UpdateWith_gid(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.Set_gid(p)
		} else {
			ud.Unset_gid()
		}
	}
}

// @tpm-schematics:start-region("-gid-field-update-section")
// @tpm-schematics:end-region("-gid-field-update-section")

// SetLeaseId No Remarks
func (ud *UpdateDocument) SetLeaseId(p string) *UpdateDocument {
	mName := fmt.Sprintf(LeaseIdFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetLeaseId No Remarks
func (ud *UpdateDocument) UnsetLeaseId() *UpdateDocument {
	mName := fmt.Sprintf(LeaseIdFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetLeaseId No Remarks
func (ud *UpdateDocument) setOrUnsetLeaseId(p string, um UnsetMode) {
	if p != "" {
		ud.SetLeaseId(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetLeaseId()
		case SetData2Default:
			ud.UnsetLeaseId()
		}
	}
}

func UpdateWithLeaseId(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetLeaseId(p)
		} else {
			ud.UnsetLeaseId()
		}
	}
}

// @tpm-schematics:start-region("lease-id-field-update-section")
// @tpm-schematics:end-region("lease-id-field-update-section")

// SetData No Remarks
func (ud *UpdateDocument) SetData(p bson.M) *UpdateDocument {
	mName := fmt.Sprintf(DataFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetData No Remarks
func (ud *UpdateDocument) UnsetData() *UpdateDocument {
	mName := fmt.Sprintf(DataFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetData No Remarks
func (ud *UpdateDocument) setOrUnsetData(p bson.M, um UnsetMode) {
	if len(p) != 0 {
		ud.SetData(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetData()
		case SetData2Default:
			ud.UnsetData()
		}
	}
}

func UpdateWithData(p bson.M) UpdateOption {
	return func(ud *UpdateDocument) {
		if len(p) != 0 {
			ud.SetData(p)
		} else {
			ud.UnsetData()
		}
	}
}

// @tpm-schematics:start-region("data-field-update-section")
// @tpm-schematics:end-region("data-field-update-section")

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

// SetEtag No Remarks
func (ud *UpdateDocument) SetEtag(p int64) *UpdateDocument {
	mName := fmt.Sprintf(EtagFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetEtag No Remarks
func (ud *UpdateDocument) UnsetEtag() *UpdateDocument {
	mName := fmt.Sprintf(EtagFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetEtag No Remarks
func (ud *UpdateDocument) setOrUnsetEtag(p int64, um UnsetMode) {
	if p != 0 {
		ud.SetEtag(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetEtag()
		case SetData2Default:
			ud.UnsetEtag()
		}
	}
}

func UpdateWithEtag(p int64) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetEtag(p)
		} else {
			ud.UnsetEtag()
		}
	}
}

// @tpm-schematics:start-region("etag-field-update-section")
// @tpm-schematics:end-region("etag-field-update-section")

// SetDuration No Remarks
func (ud *UpdateDocument) SetDuration(p int32) *UpdateDocument {
	mName := fmt.Sprintf(DurationFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetDuration No Remarks
func (ud *UpdateDocument) UnsetDuration() *UpdateDocument {
	mName := fmt.Sprintf(DurationFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetDuration No Remarks
func (ud *UpdateDocument) setOrUnsetDuration(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetDuration(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetDuration()
		case SetData2Default:
			ud.UnsetDuration()
		}
	}
}

func UpdateWithDuration(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetDuration(p)
		} else {
			ud.UnsetDuration()
		}
	}
}

// @tpm-schematics:start-region("duration-field-update-section")
// @tpm-schematics:end-region("duration-field-update-section")

// SetTs No Remarks
func (ud *UpdateDocument) SetTs(p string) *UpdateDocument {
	mName := fmt.Sprintf(TsFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetTs No Remarks
func (ud *UpdateDocument) UnsetTs() *UpdateDocument {
	mName := fmt.Sprintf(TsFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetTs No Remarks
func (ud *UpdateDocument) setOrUnsetTs(p string, um UnsetMode) {
	if p != "" {
		ud.SetTs(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetTs()
		case SetData2Default:
			ud.UnsetTs()
		}
	}
}

func UpdateWithTs(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetTs(p)
		} else {
			ud.UnsetTs()
		}
	}
}

// @tpm-schematics:start-region("ts-field-update-section")
// @tpm-schematics:end-region("ts-field-update-section")

// SetTtl No Remarks
func (ud *UpdateDocument) SetTtl(p int32) *UpdateDocument {
	mName := fmt.Sprintf(TtlFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetTtl No Remarks
func (ud *UpdateDocument) UnsetTtl() *UpdateDocument {
	mName := fmt.Sprintf(TtlFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetTtl No Remarks
func (ud *UpdateDocument) setOrUnsetTtl(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetTtl(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetTtl()
		case SetData2Default:
			ud.UnsetTtl()
		}
	}
}

func UpdateWithTtl(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetTtl(p)
		} else {
			ud.UnsetTtl()
		}
	}
}

// @tpm-schematics:start-region("ttl-field-update-section")
// @tpm-schematics:end-region("ttl-field-update-section")

// SetErrors No Remarks
func (ud *UpdateDocument) SetErrors(p int32) *UpdateDocument {
	mName := fmt.Sprintf(ErrorsFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetErrors No Remarks
func (ud *UpdateDocument) UnsetErrors() *UpdateDocument {
	mName := fmt.Sprintf(ErrorsFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetErrors No Remarks
func (ud *UpdateDocument) setOrUnsetErrors(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetErrors(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetErrors()
		case SetData2Default:
			ud.UnsetErrors()
		}
	}
}

func UpdateWithErrors(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetErrors(p)
		} else {
			ud.UnsetErrors()
		}
	}
}

// @tpm-schematics:start-region("errors-field-update-section")

func (ud *UpdateDocument) IncErrors(p int32) *UpdateDocument {
	mName := fmt.Sprintf(ErrorsFieldName)
	ud.Inc().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

func UpdateWithIncErrors(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.IncErrors(p)
		}
	}
}

// @tpm-schematics:end-region("errors-field-update-section")

// SetAcquisitions No Remarks
func (ud *UpdateDocument) SetAcquisitions(p int32) *UpdateDocument {
	mName := fmt.Sprintf(AcquisitionsFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetAcquisitions No Remarks
func (ud *UpdateDocument) UnsetAcquisitions() *UpdateDocument {
	mName := fmt.Sprintf(AcquisitionsFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetAcquisitions No Remarks
func (ud *UpdateDocument) setOrUnsetAcquisitions(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetAcquisitions(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetAcquisitions()
		case SetData2Default:
			ud.UnsetAcquisitions()
		}
	}
}

func UpdateWithAcquisitions(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetAcquisitions(p)
		} else {
			ud.UnsetAcquisitions()
		}
	}
}

// @tpm-schematics:start-region("acquisitions-field-update-section")
// @tpm-schematics:end-region("acquisitions-field-update-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
