package partition

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
	DefaultMode     UnsetMode
	Bid             UnsetMode
	Gid             UnsetMode
	Et              UnsetMode
	PartitionNumber UnsetMode
	Status          UnsetMode
	Etag            UnsetMode
	Mongo           UnsetMode
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
func WithGidUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Gid = m
	}
}
func WithEtUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Et = m
	}
}
func WithPartitionNumberUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.PartitionNumber = m
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
func WithMongoUnsetMode(m UnsetMode) UnsetOption {
	return func(uopt *UnsetOptions) {
		uopt.Mongo = m
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
func GetUpdateDocument(obj *Partition, opts ...UnsetOption) UpdateDocument {

	uo := &UnsetOptions{DefaultMode: KeepCurrent}
	for _, o := range opts {
		o(uo)
	}

	ud := UpdateDocument{}
	ud.setOrUnset_bid(obj.Bid, uo.ResolveUnsetMode(uo.Bid))
	ud.setOrUnset_gid(obj.Gid, uo.ResolveUnsetMode(uo.Gid))
	ud.setOrUnset_et(obj.Et, uo.ResolveUnsetMode(uo.Et))
	ud.setOrUnsetPartitionNumber(obj.PartitionNumber, uo.ResolveUnsetMode(uo.PartitionNumber))
	ud.setOrUnsetStatus(obj.Status, uo.ResolveUnsetMode(uo.Status))
	ud.setOrUnsetEtag(obj.Etag, uo.ResolveUnsetMode(uo.Etag))
	ud.setOrUnsetMongo(&obj.Mongo, uo.ResolveUnsetMode(uo.Mongo))

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

// SetPartitionNumber No Remarks
func (ud *UpdateDocument) SetPartitionNumber(p int32) *UpdateDocument {
	mName := fmt.Sprintf(PartitionNumberFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetPartitionNumber No Remarks
func (ud *UpdateDocument) UnsetPartitionNumber() *UpdateDocument {
	mName := fmt.Sprintf(PartitionNumberFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetPartitionNumber No Remarks
func (ud *UpdateDocument) setOrUnsetPartitionNumber(p int32, um UnsetMode) {
	if p != 0 {
		ud.SetPartitionNumber(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetPartitionNumber()
		case SetData2Default:
			ud.UnsetPartitionNumber()
		}
	}
}

func UpdateWithPartitionNumber(p int32) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != 0 {
			ud.SetPartitionNumber(p)
		} else {
			ud.UnsetPartitionNumber()
		}
	}
}

// @tpm-schematics:start-region("partition-number-field-update-section")
// @tpm-schematics:end-region("partition-number-field-update-section")

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

// SetMongo No Remarks
func (ud *UpdateDocument) SetMongo(p *MongoPartition) *UpdateDocument {
	mName := fmt.Sprintf(MongoFieldName)
	ud.Set().Add(func() bson.E {
		return bson.E{Key: mName, Value: p}
	})
	return ud
}

// UnsetMongo No Remarks
func (ud *UpdateDocument) UnsetMongo() *UpdateDocument {
	mName := fmt.Sprintf(MongoFieldName)
	ud.Unset().Add(func() bson.E {
		return bson.E{Key: mName, Value: ""}
	})
	return ud
}

// setOrUnsetMongo No Remarks - here2
func (ud *UpdateDocument) setOrUnsetMongo(p *MongoPartition, um UnsetMode) {
	if p != nil && !p.IsZero() {
		ud.SetMongo(p)
	} else {
		switch um {
		case KeepCurrent:
		case UnsetData:
			ud.UnsetMongo()
		case SetData2Default:
			ud.UnsetMongo()
		}
	}
}

func UpdateWithMongo(p *MongoPartition) UpdateOption {
	return func(ud *UpdateDocument) {
		if p != nil && !p.IsZero() {
			ud.SetMongo(p)
		} else {
			ud.UnsetMongo()
		}
	}
}

// @tpm-schematics:start-region("mongo-field-update-section")
// @tpm-schematics:end-region("mongo-field-update-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
