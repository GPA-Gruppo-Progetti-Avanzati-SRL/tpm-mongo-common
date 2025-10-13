package lease

import "go.mongodb.org/mongo-driver/v2/bson"

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

var emptyUpdate = bson.D{}

type UpdateOperator string

const (
	Set         UpdateOperator = "$set"
	Unset       UpdateOperator = "$unset"
	Inc         UpdateOperator = "$inc"
	CurrentDate UpdateOperator = "$currentDate"
	AddToSet    UpdateOperator = "$addToSet"
	Pull        UpdateOperator = "$pull"
)

type Update func() bson.E

type UpdateDocument struct {
	Ops map[UpdateOperator]*Updates
}

func (ud *UpdateDocument) Set() *Updates {
	return ud.op(Set)
}

func (ud *UpdateDocument) Unset() *Updates {
	return ud.op(Unset)
}

func (ud *UpdateDocument) CurrentDate() *Updates {
	return ud.op(CurrentDate)
}

func (ud *UpdateDocument) Inc() *Updates {
	return ud.op(Inc)
}

func (ud *UpdateDocument) AddToSet() *Updates {
	return ud.op(AddToSet)
}

func (ud *UpdateDocument) Pull() *Updates {
	return ud.op(Pull)
}

func (ud *UpdateDocument) op(op UpdateOperator) *Updates {

	if len(ud.Ops) == 0 {
		ud.Ops = make(map[UpdateOperator]*Updates)
	}

	if u, ok := ud.Ops[op]; ok {
		return u
	}

	uopi := &Updates{operator: string(op)}
	ud.Ops[op] = uopi

	return uopi
}

func (ud *UpdateDocument) Build() bson.D {
	if len(ud.Ops) == 0 {
		return emptyFilter
	}

	docAll := bson.D{}
	for _, cas := range ud.Ops {
		doc := bson.D{}
		for _, c := range cas.updates {
			if u := c(); u.Key != "" {
				doc = append(doc, u)
			}
		}
		if len(doc) > 0 {
			opu := bson.E{Key: cas.operator, Value: doc}
			docAll = append(docAll, opu)
		}
	}

	return docAll
}

type Updates struct {
	operator string
	updates  []Update
}

func (ui *Updates) Add(fu Update) {
	ui.updates = append(ui.updates, fu)
}

func (ud *UpdateDocument) Add(op UpdateOperator, fu Update) {
	ud.op(op).Add(fu)
}

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
