package task

import (
	"fmt"
	"go.mongodb.org/mongo-driver/v2/bson"
	"time"
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
 * filter-object-id template: oId
 */

// AndOIdEqTo No Remarks
func (ca *Criteria) AndOIdEqTo(oId bson.ObjectID) *Criteria {

	if oId == bson.NilObjectID {
		return ca
	}

	mName := fmt.Sprintf(OIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: oId} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndOIdIn(p []bson.ObjectID) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(OIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("o-id-field-filter-section")
// @tpm-schematics:end-region("o-id-field-filter-section")

/*
 * filter-string template: domain
 */

// AndDomainEqTo No Remarks
func (ca *Criteria) AndDomainEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(DomainFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndDomainIsNullOrUnset No Remarks
func (ca *Criteria) AndDomainIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(DomainFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndDomainIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(DomainFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("domain-field-filter-section")
// @tpm-schematics:end-region("domain-field-filter-section")

/*
 * filter-string template: site
 */

// AndSiteEqTo No Remarks
func (ca *Criteria) AndSiteEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(SiteFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndSiteIsNullOrUnset No Remarks
func (ca *Criteria) AndSiteIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(SiteFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndSiteIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(SiteFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("site-field-filter-section")
// @tpm-schematics:end-region("site-field-filter-section")

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
 * filter-string template: group
 */

// AndGroupEqTo No Remarks
func (ca *Criteria) AndGroupEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(GroupFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndGroupIsNullOrUnset No Remarks
func (ca *Criteria) AndGroupIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(GroupFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndGroupIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(GroupFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("group-field-filter-section")
// @tpm-schematics:end-region("group-field-filter-section")

/*
 * filter-string template: job_id
 */

// AndJobIdEqTo No Remarks
func (ca *Criteria) AndJobIdEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(JobIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndJobIdIsNullOrUnset No Remarks
func (ca *Criteria) AndJobIdIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(JobIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndJobIdIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(JobIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("job-id-field-filter-section")
// @tpm-schematics:end-region("job-id-field-filter-section")

// @tpm-schematics:start-region("bottom-file-section")
// @tpm-schematics:end-region("bottom-file-section")
