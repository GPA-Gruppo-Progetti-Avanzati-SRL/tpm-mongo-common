package task

import (
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
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
	DefaultMode    UnsetMode
	Bid            UnsetMode
	Et             UnsetMode
	Status         UnsetMode
	Ambit          UnsetMode
	DataSourceType UnsetMode
	StreamType     UnsetMode
	ProcessorId    UnsetMode
	JobId          UnsetMode
	Info           UnsetMode
	Partitions     UnsetMode
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
func WithStatusUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Status = m
	}
}
func WithAmbitUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Ambit = m
	}
}
func WithDataSourceTypeUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.DataSourceType = m
	}
}
func WithStreamTypeUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.StreamType = m
	}
}
func WithProcessorIdUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.ProcessorId = m
	}
}
func WithJobIdUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.JobId = m
	}
}
func WithInfoUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Info = m
	}
}
func WithPartitionsUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Partitions = m
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
func GetUpdateDocument(obj *Task, opts ...UnsetOption) UpdateDocument {

	uo := &UnsetOptions{DefaultMode: KeepCurrent}
	for _, o := range opts {
		o(uo)
	}

	ud := UpdateDocument{}
	ud.setOrUnset_bid(obj.Bid, uo.ResolveUnsetMode(uo.Bid))
	ud.setOrUnset_et(obj.Et, uo.ResolveUnsetMode(uo.Et))
	ud.setOrUnsetStatus(obj.Status, uo.ResolveUnsetMode(uo.Status))
	ud.setOrUnsetAmbit(obj.Ambit, uo.ResolveUnsetMode(uo.Ambit))
	ud.setOrUnsetData_source_type(obj.DataSourceType, uo.ResolveUnsetMode(uo.DataSourceType))
	ud.setOrUnsetStream_type(obj.StreamType, uo.ResolveUnsetMode(uo.StreamType))
	ud.setOrUnsetProcessor_id(obj.ProcessorId, uo.ResolveUnsetMode(uo.ProcessorId))
	ud.setOrUnsetJob_id(obj.JobId, uo.ResolveUnsetMode(uo.JobId))
	ud.setOrUnsetInfo(&obj.Info, uo.ResolveUnsetMode(uo.Info))
	ud.setOrUnsetPartitions(obj.Partitions, uo.ResolveUnsetMode(uo.Partitions))

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

// SetAmbit No Remarks
func (ud *UpdateDocument) SetAmbit(p string) *UpdateDocument {
	mName := fmt.Sprintf(AmbitFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetAmbit No Remarks
func (ud *UpdateDocument) UnsetAmbit() *UpdateDocument {
	mName := fmt.Sprintf(AmbitFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetAmbit No Remarks
func (ud *UpdateDocument) setOrUnsetAmbit(p string, um UnsetMode) {
	if p != "" {
		ud.SetAmbit(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetAmbit()
		case SetData2Default:
			ud.UnsetAmbit()
		}
	}
}

func UpdateWithAmbit(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetAmbit(p)
		} else {
			ud.UnsetAmbit()
		}
	}
}

// @tpm-schematics:start-region("ambit-field-update-section")
// @tpm-schematics:end-region("ambit-field-update-section")

// SetData_source_type No Remarks
func (ud *UpdateDocument) SetData_source_type(p string) *UpdateDocument {
	mName := fmt.Sprintf(DataSourceTypeFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetData_source_type No Remarks
func (ud *UpdateDocument) UnsetData_source_type() *UpdateDocument {
	mName := fmt.Sprintf(DataSourceTypeFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetData_source_type No Remarks
func (ud *UpdateDocument) setOrUnsetData_source_type(p string, um UnsetMode) {
	if p != "" {
		ud.SetData_source_type(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetData_source_type()
		case SetData2Default:
			ud.UnsetData_source_type()
		}
	}
}

func UpdateWithData_source_type(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetData_source_type(p)
		} else {
			ud.UnsetData_source_type()
		}
	}
}

// @tpm-schematics:start-region("data-source-type-field-update-section")
// @tpm-schematics:end-region("data-source-type-field-update-section")

// SetStream_type No Remarks
func (ud *UpdateDocument) SetStream_type(p string) *UpdateDocument {
	mName := fmt.Sprintf(StreamTypeFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetStream_type No Remarks
func (ud *UpdateDocument) UnsetStream_type() *UpdateDocument {
	mName := fmt.Sprintf(StreamTypeFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetStream_type No Remarks
func (ud *UpdateDocument) setOrUnsetStream_type(p string, um UnsetMode) {
	if p != "" {
		ud.SetStream_type(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetStream_type()
		case SetData2Default:
			ud.UnsetStream_type()
		}
	}
}

func UpdateWithStream_type(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetStream_type(p)
		} else {
			ud.UnsetStream_type()
		}
	}
}

// @tpm-schematics:start-region("stream-type-field-update-section")
// @tpm-schematics:end-region("stream-type-field-update-section")

// SetProcessor_id No Remarks
func (ud *UpdateDocument) SetProcessor_id(p string) *UpdateDocument {
	mName := fmt.Sprintf(ProcessorIdFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetProcessor_id No Remarks
func (ud *UpdateDocument) UnsetProcessor_id() *UpdateDocument {
	mName := fmt.Sprintf(ProcessorIdFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetProcessor_id No Remarks
func (ud *UpdateDocument) setOrUnsetProcessor_id(p string, um UnsetMode) {
	if p != "" {
		ud.SetProcessor_id(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetProcessor_id()
		case SetData2Default:
			ud.UnsetProcessor_id()
		}
	}
}

func UpdateWithProcessor_id(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetProcessor_id(p)
		} else {
			ud.UnsetProcessor_id()
		}
	}
}

// @tpm-schematics:start-region("processor-id-field-update-section")
// @tpm-schematics:end-region("processor-id-field-update-section")

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

// SetInfo No Remarks
func (ud *UpdateDocument) SetInfo(p *beans.TaskInfo) *UpdateDocument {
	mName := fmt.Sprintf(InfoFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetInfo No Remarks
func (ud *UpdateDocument) UnsetInfo() *UpdateDocument {
	mName := fmt.Sprintf(InfoFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetInfo No Remarks - here2
func (ud *UpdateDocument) setOrUnsetInfo(p *beans.TaskInfo, um UnsetMode) {
	if p != nil && !p.IsZero() {
		ud.SetInfo(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetInfo()
		case SetData2Default:
			ud.UnsetInfo()
		}
	}
}

func UpdateWithInfo(p *beans.TaskInfo) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != nil && !p.IsZero() {
			ud.SetInfo(p)
		} else {
			ud.UnsetInfo()
		}
	}
}

// @tpm-schematics:start-region("info-field-update-section")
// @tpm-schematics:end-region("info-field-update-section")

// SetPartitions No Remarks
func (ud *UpdateDocument) SetPartitions(p []partition.Partition) *UpdateDocument {
	mName := fmt.Sprintf(PartitionsFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetPartitions No Remarks
func (ud *UpdateDocument) UnsetPartitions() *UpdateDocument {
	mName := fmt.Sprintf(PartitionsFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetPartitions No Remarks - here2
func (ud *UpdateDocument) setOrUnsetPartitions(p []partition.Partition, um UnsetMode) {
	if len(p) > 0 {
		ud.SetPartitions(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetPartitions()
		case SetData2Default:
			ud.UnsetPartitions()
		}
	}
}

func UpdateWithPartitions(p []partition.Partition) UpdateOption {
	return func(ud *UpdateDocument) {
		if len(p) > 0 {
			ud.SetPartitions(p)
		} else {
			ud.UnsetPartitions()
		}
	}
}

// @tpm-schematics:start-region("partitions-field-update-section")

func UpdateWithPartitionStatus(prt int32, status string) UpdateOption {
	return func(ud *UpdateDocument) {
		// partitions are numbered from 1 but array is indexed from 0.
		mName := fmt.Sprintf(PartitionsIStatusFieldName, prt-1)
		ud.Set().Add(func() bson.E {
			return bson.E{Key: mName, Value: status}
		})
	}
}

func UpdateWithIncPartitionAcquisitions(prt int32) UpdateOption {
	return func(ud *UpdateDocument) {
		// partitions are numbered from 1 but array is indexed from 0.
		mName := fmt.Sprintf(PartitionsIAcquisitionsFieldName, prt-1)
		ud.Inc().Add(func() bson.E {
			return bson.E{Key: mName, Value: 1}
		})
	}
}

func UpdateWithIncPartitionErrors(prt int32) UpdateOption {
	return func(ud *UpdateDocument) {
		// partitions are numbered from 1 but array is indexed from 0.
		mName := fmt.Sprintf(PartitionsIErrorsFieldName, prt-1)
		ud.Inc().Add(func() bson.E {
			return bson.E{Key: mName, Value: 1}
		})
	}
}

// @tpm-schematics:end-region("partitions-field-update-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
