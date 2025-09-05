package job

import (
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

// @tpm-schematics:start-region("top-file-section")
// @tpm-schematics:end-region("top-file-section")

func FilterMethodsGoInfo() string {
	i := fmt.Sprintf("tpm_morphia query filter support generated for %s package on %s", "author", time.Now().String())
	return i
}

// to be able to succesfully call this method you have to define a text index on the collection. The $text operator has some additional fields that are not supported yet.
func (ca *Criteria) AndTextSearch(ssearch string) *Criteria {
	if ssearch == "" {
		return ca
	}

	c := func() bson.E {
		const TextOperator = "$text"
		return bson.E{Key: TextOperator, Value: bson.E{Key: "$search", Value: ssearch}}
	}
	*ca = append(*ca, c)
	return ca
}

/*
 * filter-string template: _bid
 */

// AndBidEqTo No Remarks
func (ca *Criteria) AndBidEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(BidFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndBidIsNullOrUnset No Remarks
func (ca *Criteria) AndBidIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(BidFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndBidIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(BidFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("-bid-field-filter-section")
// @tpm-schematics:end-region("-bid-field-filter-section")

/*
 * filter-string template: _et
 */

// AndEtEqTo No Remarks
func (ca *Criteria) AndEtEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(EtFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndEtIsNullOrUnset No Remarks
func (ca *Criteria) AndEtIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(EtFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndEtIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(EtFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("-et-field-filter-section")
// @tpm-schematics:end-region("-et-field-filter-section")

/*
 * filter-string template: ambit
 */

// AndAmbitEqTo No Remarks
func (ca *Criteria) AndAmbitEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(AmbitFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndAmbitIsNullOrUnset No Remarks
func (ca *Criteria) AndAmbitIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(AmbitFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndAmbitIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(AmbitFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("ambit-field-filter-section")
// @tpm-schematics:end-region("ambit-field-filter-section")

/*
 * filter-string template: status
 */

// AndStatusEqTo No Remarks
func (ca *Criteria) AndStatusEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(StatusFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndStatusIsNullOrUnset No Remarks
func (ca *Criteria) AndStatusIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(StatusFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndStatusIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(StatusFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("status-field-filter-section")
// @tpm-schematics:end-region("status-field-filter-section")

/*
 * filter-string template: due_date
 */

// AndDueDateEqTo No Remarks
func (ca *Criteria) AndDueDateEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(DueDateFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndDueDateIsNullOrUnset No Remarks
func (ca *Criteria) AndDueDateIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(DueDateFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndDueDateIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(DueDateFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("due-date-field-filter-section")

func (ca *Criteria) AndDueDateBetween(p1, p2 string) *Criteria {

	if p1 == "" && p2 == "" {
		return ca
	}

	mName := fmt.Sprintf(DueDateFieldName)

	if p1 == "" || p2 == "" {
		if p1 != "" {
			c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$gte", p1}}} }
			*ca = append(*ca, c)
		}

		if p2 != "" {
			c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$lte", p2}}} }
			*ca = append(*ca, c)
		}
	} else {
		c := func() bson.E {
			return bson.E{Key: "$and", Value: bson.A{
				bson.E{Key: mName, Value: bson.D{{"$gte", p1}}},
				bson.E{Key: mName, Value: bson.D{{"$lte", p2}}},
			}}
		}

		*ca = append(*ca, c)
	}

	return ca
}

// @tpm-schematics:end-region("due-date-field-filter-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
