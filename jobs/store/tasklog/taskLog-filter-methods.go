package tasklog

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
 * filter-string template: task_id
 */

// AndTaskIdEqTo No Remarks
func (ca *Criteria) AndTaskIdEqTo(p string) *Criteria {

	if p == "" {
		return ca
	}

	mName := fmt.Sprintf(TaskIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

// AndTaskIdIsNullOrUnset No Remarks
func (ca *Criteria) AndTaskIdIsNullOrUnset() *Criteria {

	mName := fmt.Sprintf(TaskIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: nil} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndTaskIdIn(p []string) *Criteria {

	if len(p) == 0 {
		return ca
	}

	mName := fmt.Sprintf(TaskIdFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$in", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("task-id-field-filter-section")
// @tpm-schematics:end-region("task-id-field-filter-section")

/*
 * filter-int template: partition
 */

// AndPartitionEqTo No Remarks
func (ca *Criteria) AndPartitionEqTo(p int, nullValue ...int) *Criteria {

	if len(nullValue) > 0 && p == nullValue[0] {
		return ca
	}

	mName := fmt.Sprintf(PartitionFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: p} }
	*ca = append(*ca, c)
	return ca
}

func (ca *Criteria) AndPartitionGt(p int, nullValue ...int) *Criteria {

	if len(nullValue) > 0 && p == nullValue[0] {
		return ca
	}

	mName := fmt.Sprintf(PartitionFieldName)
	c := func() bson.E { return bson.E{Key: mName, Value: bson.D{{"$gt", p}}} }
	*ca = append(*ca, c)
	return ca
}

// @tpm-schematics:start-region("partition-field-filter-section")
// @tpm-schematics:end-region("partition-field-filter-section")

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
