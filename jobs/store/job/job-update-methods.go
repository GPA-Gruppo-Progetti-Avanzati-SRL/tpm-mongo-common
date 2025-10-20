package job

import (
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
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
	Group       UnsetMode
	Name        UnsetMode
	Status      UnsetMode
	DueDate     UnsetMode
	Properties  UnsetMode
	Tasks       UnsetMode
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
func WithGroupUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Group = m
	}
}
func WithNameUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Name = m
	}
}
func WithStatusUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Status = m
	}
}
func WithDueDateUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.DueDate = m
	}
}
func WithPropertiesUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Properties = m
	}
}
func WithTasksUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Tasks = m
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
func GetUpdateDocument(obj *Job, opts ...UnsetOption) UpdateDocument {

	uo := &UnsetOptions{DefaultMode: KeepCurrent}
	for _, o := range opts {
		o(uo)
	}

	ud := UpdateDocument{}
	ud.setOrUnsetDomain(obj.Domain, uo.ResolveUnsetMode(uo.Domain))
	ud.setOrUnsetSite(obj.Site, uo.ResolveUnsetMode(uo.Site))
	ud.setOrUnset_bid(obj.Bid, uo.ResolveUnsetMode(uo.Bid))
	ud.setOrUnset_et(obj.Et, uo.ResolveUnsetMode(uo.Et))
	ud.setOrUnsetGroup(obj.Group, uo.ResolveUnsetMode(uo.Group))
	ud.setOrUnsetName(obj.Name, uo.ResolveUnsetMode(uo.Name))
	ud.setOrUnsetStatus(obj.Status, uo.ResolveUnsetMode(uo.Status))
	ud.setOrUnsetDue_date(obj.DueDate, uo.ResolveUnsetMode(uo.DueDate))
	ud.setOrUnsetProperties(obj.Properties, uo.ResolveUnsetMode(uo.Properties))
	ud.setOrUnsetTasks(obj.Tasks, uo.ResolveUnsetMode(uo.Tasks))

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

// SetGroup No Remarks
func (ud *UpdateDocument) SetGroup(p string) *UpdateDocument {
	mName := fmt.Sprintf(GroupFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetGroup No Remarks
func (ud *UpdateDocument) UnsetGroup() *UpdateDocument {
	mName := fmt.Sprintf(GroupFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetGroup No Remarks
func (ud *UpdateDocument) setOrUnsetGroup(p string, um UnsetMode) {
	if p != "" {
		ud.SetGroup(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetGroup()
		case SetData2Default:
			ud.UnsetGroup()
		}
	}
}

func UpdateWithGroup(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetGroup(p)
		} else {
			ud.UnsetGroup()
		}
	}
}

// @tpm-schematics:start-region("group-field-update-section")
// @tpm-schematics:end-region("group-field-update-section")

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

// SetDue_date No Remarks
func (ud *UpdateDocument) SetDue_date(p string) *UpdateDocument {
	mName := fmt.Sprintf(DueDateFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetDue_date No Remarks
func (ud *UpdateDocument) UnsetDue_date() *UpdateDocument {
	mName := fmt.Sprintf(DueDateFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetDue_date No Remarks
func (ud *UpdateDocument) setOrUnsetDue_date(p string, um UnsetMode) {
	if p != "" {
		ud.SetDue_date(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetDue_date()
		case SetData2Default:
			ud.UnsetDue_date()
		}
	}
}

func UpdateWithDue_date(p string) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != "" {
			ud.SetDue_date(p)
		} else {
			ud.UnsetDue_date()
		}
	}
}

// @tpm-schematics:start-region("due-date-field-update-section")
// @tpm-schematics:end-region("due-date-field-update-section")

// SetProperties No Remarks
func (ud *UpdateDocument) SetProperties(p bson.M) *UpdateDocument {
	mName := fmt.Sprintf(PropertiesFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetProperties No Remarks
func (ud *UpdateDocument) UnsetProperties() *UpdateDocument {
	mName := fmt.Sprintf(PropertiesFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetProperties No Remarks
func (ud *UpdateDocument) setOrUnsetProperties(p bson.M, um UnsetMode) {
	if len(p) != 0 {
		ud.SetProperties(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetProperties()
		case SetData2Default:
			ud.UnsetProperties()
		}
	}
}

func UpdateWithProperties(p bson.M) UpdateOption {
	return func(ud *UpdateDocument) {
		if len(p) != 0 {
			ud.SetProperties(p)
		} else {
			ud.UnsetProperties()
		}
	}
}

// @tpm-schematics:start-region("properties-field-update-section")
// @tpm-schematics:end-region("properties-field-update-section")

// SetTasks No Remarks
func (ud *UpdateDocument) SetTasks(p []beans.TaskReference) *UpdateDocument {
	mName := fmt.Sprintf(TasksFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetTasks No Remarks
func (ud *UpdateDocument) UnsetTasks() *UpdateDocument {
	mName := fmt.Sprintf(TasksFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetTasks No Remarks - here2
func (ud *UpdateDocument) setOrUnsetTasks(p []beans.TaskReference, um UnsetMode) {
	if len(p) > 0 {
		ud.SetTasks(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetTasks()
		case SetData2Default:
			ud.UnsetTasks()
		}
	}
}

func UpdateWithTasks(p []beans.TaskReference) UpdateOption {
	return func(ud *UpdateDocument) {
		if len(p) > 0 {
			ud.SetTasks(p)
		} else {
			ud.UnsetTasks()
		}
	}
}

// @tpm-schematics:start-region("tasks-field-update-section")

func UpdateWithTaskStatus(tskNdx int32, status string) UpdateOption {
	return func(ud *UpdateDocument) {
		// partitions are numbered from 1 but array is indexed from 0.
		mName := fmt.Sprintf(Tasks_IFieldName_status, tskNdx)
		ud.Set().Add(func() bson.E {
			return bson.E{Key: mName, Value: status}
		})
	}
}

// @tpm-schematics:end-region("tasks-field-update-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
