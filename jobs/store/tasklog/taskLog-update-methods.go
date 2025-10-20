package tasklog

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
	DefaultMode UnsetMode
	Domain      UnsetMode
	Site        UnsetMode
	Bid         UnsetMode
	Et          UnsetMode
	Name        UnsetMode
	TaskId      UnsetMode
	Partition   UnsetMode
	JobId       UnsetMode
	BlockNumber UnsetMode
	Entries     UnsetMode
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
func WithDomainUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Domain = m
	}
}
func WithSiteUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Site = m
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
func WithNameUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Name = m
	}
}
func WithTaskIdUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.TaskId = m
	}
}
func WithPartitionUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Partition = m
	}
}
func WithJobIdUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.JobId = m
	}
}
func WithBlockNumberUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.BlockNumber = m
	}
}
func WithEntriesUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Entries = m
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
func GetUpdateDocument(obj *TaskLog, opts ...UnsetOption) UpdateDocument {

	uo := &UnsetOptions{DefaultMode: KeepCurrent}
	for _, o := range opts {
		o(uo)
	}

	ud := UpdateDocument{}
	ud.setOrUnsetDomain(obj.Domain, uo.ResolveUnsetMode(uo.Domain))
	ud.setOrUnsetSite(obj.Site, uo.ResolveUnsetMode(uo.Site))
	ud.setOrUnset_bid(obj.Bid, uo.ResolveUnsetMode(uo.Bid))
	ud.setOrUnset_et(obj.Et, uo.ResolveUnsetMode(uo.Et))
	ud.setOrUnsetName(obj.Name, uo.ResolveUnsetMode(uo.Name))
	ud.setOrUnsetTask_id(obj.TaskId, uo.ResolveUnsetMode(uo.TaskId))
	ud.setOrUnsetPartition(obj.Partition, uo.ResolveUnsetMode(uo.Partition))
	ud.setOrUnsetJob_id(obj.JobId, uo.ResolveUnsetMode(uo.JobId))
	ud.setOrUnsetBlock_number(obj.BlockNumber, uo.ResolveUnsetMode(uo.BlockNumber))
	ud.setOrUnsetEntries(obj.Entries, uo.ResolveUnsetMode(uo.Entries))

	return ud
}

// SetDomain No Remarks
func (ud *UpdateDocument) SetDomain(p string) *UpdateDocument {
	mName := fmt.Sprintf(DomainFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetDomain No Remarks
func (ud *UpdateDocument) UnsetDomain() *UpdateDocument {
	mName := fmt.Sprintf(DomainFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetDomain No Remarks
func (ud *UpdateDocument) setOrUnsetDomain(p string, um UnsetMode) {
	if p != "" {
		ud.SetDomain(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetDomain()
		case SetData2Default:
			ud.UnsetDomain()
		}
	}
}

func UpdateWithDomain(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetDomain(p)
		} else {
			ud.UnsetDomain()
		}
	}
}

// @tpm-schematics:start-region("domain-field-update-section")
// @tpm-schematics:end-region("domain-field-update-section")

// SetSite No Remarks
func (ud *UpdateDocument) SetSite(p string) *UpdateDocument {
	mName := fmt.Sprintf(SiteFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetSite No Remarks
func (ud *UpdateDocument) UnsetSite() *UpdateDocument {
	mName := fmt.Sprintf(SiteFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetSite No Remarks
func (ud *UpdateDocument) setOrUnsetSite(p string, um UnsetMode) {
	if p != "" {
		ud.SetSite(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetSite()
		case SetData2Default:
			ud.UnsetSite()
		}
	}
}

func UpdateWithSite(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetSite(p)
		} else {
			ud.UnsetSite()
		}
	}
}

// @tpm-schematics:start-region("site-field-update-section")
// @tpm-schematics:end-region("site-field-update-section")

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

// SetName No Remarks
func (ud *UpdateDocument) SetName(p string) *UpdateDocument {
	mName := fmt.Sprintf(NameFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetName No Remarks
func (ud *UpdateDocument) UnsetName() *UpdateDocument {
	mName := fmt.Sprintf(NameFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetName No Remarks
func (ud *UpdateDocument) setOrUnsetName(p string, um UnsetMode) {
	if p != "" {
		ud.SetName(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetName()
		case SetData2Default:
			ud.UnsetName()
		}
	}
}

func UpdateWithName(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetName(p)
		} else {
			ud.UnsetName()
		}
	}
}

// @tpm-schematics:start-region("name-field-update-section")
// @tpm-schematics:end-region("name-field-update-section")

// SetTask_id No Remarks
func (ud *UpdateDocument) SetTask_id(p string) *UpdateDocument {
	mName := fmt.Sprintf(TaskIdFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetTask_id No Remarks
func (ud *UpdateDocument) UnsetTask_id() *UpdateDocument {
	mName := fmt.Sprintf(TaskIdFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetTask_id No Remarks
func (ud *UpdateDocument) setOrUnsetTask_id(p string, um UnsetMode) {
	if p != "" {
		ud.SetTask_id(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetTask_id()
		case SetData2Default:
			ud.UnsetTask_id()
		}
	}
}

func UpdateWithTask_id(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetTask_id(p)
		} else {
			ud.UnsetTask_id()
		}
	}
}

// @tpm-schematics:start-region("task-id-field-update-section")
// @tpm-schematics:end-region("task-id-field-update-section")

// SetPartition No Remarks
func (ud *UpdateDocument) SetPartition(p int32) *UpdateDocument {
	mName := fmt.Sprintf(PartitionFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetPartition No Remarks
func (ud *UpdateDocument) UnsetPartition() *UpdateDocument {
	mName := fmt.Sprintf(PartitionFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetPartition No Remarks
func (ud *UpdateDocument) setOrUnsetPartition(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetPartition(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetPartition()
		case SetData2Default:
			ud.UnsetPartition()
		}
	}
}

func UpdateWithPartition(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetPartition(p)
		} else {
			ud.UnsetPartition()
		}
	}
}

// @tpm-schematics:start-region("partition-field-update-section")
// @tpm-schematics:end-region("partition-field-update-section")

// SetJob_id No Remarks
func (ud *UpdateDocument) SetJob_id(p string) *UpdateDocument {
	mName := fmt.Sprintf(JobIdFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetJob_id No Remarks
func (ud *UpdateDocument) UnsetJob_id() *UpdateDocument {
	mName := fmt.Sprintf(JobIdFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetJob_id No Remarks
func (ud *UpdateDocument) setOrUnsetJob_id(p string, um UnsetMode) {
	if p != "" {
		ud.SetJob_id(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetJob_id()
		case SetData2Default:
			ud.UnsetJob_id()
		}
	}
}

func UpdateWithJob_id(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetJob_id(p)
		} else {
			ud.UnsetJob_id()
		}
	}
}

// @tpm-schematics:start-region("job-id-field-update-section")
// @tpm-schematics:end-region("job-id-field-update-section")

// SetBlock_number No Remarks
func (ud *UpdateDocument) SetBlock_number(p int32) *UpdateDocument {
	mName := fmt.Sprintf(BlockNumberFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetBlock_number No Remarks
func (ud *UpdateDocument) UnsetBlock_number() *UpdateDocument {
	mName := fmt.Sprintf(BlockNumberFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetBlock_number No Remarks
func (ud *UpdateDocument) setOrUnsetBlock_number(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetBlock_number(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetBlock_number()
		case SetData2Default:
			ud.UnsetBlock_number()
		}
	}
}

func UpdateWithBlock_number(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetBlock_number(p)
		} else {
			ud.UnsetBlock_number()
		}
	}
}

// @tpm-schematics:start-region("block-number-field-update-section")
// @tpm-schematics:end-region("block-number-field-update-section")

// SetEntries No Remarks
func (ud *UpdateDocument) SetEntries(p []TaskLogEntry) *UpdateDocument {
	mName := fmt.Sprintf(EntriesFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetEntries No Remarks
func (ud *UpdateDocument) UnsetEntries() *UpdateDocument {
	mName := fmt.Sprintf(EntriesFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetEntries No Remarks - here2
func (ud *UpdateDocument) setOrUnsetEntries(p []TaskLogEntry, um UnsetMode) {
	if len(p) > 0 {
		ud.SetEntries(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetEntries()
		case SetData2Default:
			ud.UnsetEntries()
		}
	}
}

func UpdateWithEntries(p []TaskLogEntry) UpdateOption {
	return func(ud *UpdateDocument) {
		if len(p) > 0 {
			ud.SetEntries(p)
		} else {
			ud.UnsetEntries()
		}
	}
}

// @tpm-schematics:start-region("entries-field-update-section")
// @tpm-schematics:end-region("entries-field-update-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
