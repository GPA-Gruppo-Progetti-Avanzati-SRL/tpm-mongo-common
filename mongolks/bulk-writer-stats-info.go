package mongolks

import (
	"fmt"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

type BulkWriterStatsInfo struct {
	ModifiedCount  int64
	InsertedCount  int64
	UpsertedCount  int64
	MatchedCount   int64
	DeletedCount   int64
	ErrsCount      int64
	DiscardedCount int64
	BulkSize       int

	WriteModifiedCountMetric promutil.CollectorWithLabels
	WriteInsertedCountMetric promutil.CollectorWithLabels
	WriteUpsertedCountMetric promutil.CollectorWithLabels
	WriteMatchedCountMetric  promutil.CollectorWithLabels
	WriteDeletedCountMetric  promutil.CollectorWithLabels
	WriteErrsCountMetric     promutil.CollectorWithLabels
	WriteBulkSizeGaugeMetric promutil.CollectorWithLabels
	SinkDiscardedCountMetric promutil.CollectorWithLabels
	WriteDurationHistMetric  promutil.CollectorWithLabels
	metricErrors             bool
}

func (stat *BulkWriterStatsInfo) Clear() *BulkWriterStatsInfo {
	stat.ModifiedCount = 0
	stat.InsertedCount = 0
	stat.UpsertedCount = 0
	stat.MatchedCount = 0
	stat.DeletedCount = 0
	stat.ErrsCount = 0
	stat.BulkSize = 0
	stat.DiscardedCount = 0
	return stat
}

func (stat *BulkWriterStatsInfo) String() string {
	stat.ModifiedCount = 0
	stat.InsertedCount = 0
	stat.UpsertedCount = 0
	stat.MatchedCount = 0
	stat.DeletedCount = 0
	stat.ErrsCount = 0
	stat.BulkSize = 0
	stat.DiscardedCount = 0
	return fmt.Sprintf("modified: %d, inserted: %d, upserted: %d, matched: %d, deleted: %d, errs: %d",
		stat.ModifiedCount,
		stat.InsertedCount,
		stat.UpsertedCount,
		stat.MatchedCount,
		stat.DeletedCount,
		stat.ErrsCount)
}

const (
	WriteDurationHistogramMetric = "mdb-write-duration"
	WriteBulkOpsMetric           = "mdb-writes-ops"
	WriteErrsMetric              = "mdb-write-errors"
	WriteBulkSizeGaugeMetric     = "mdb-write-bulk-size"
	SinkDiscardedCountMetric     = "mdb-sink-discarded-count"
	MetricLabelContextId         = "sub-context-id"
	MetricLabelSubContextId      = "context-id"
	MetricLabelOpCounter         = "oper"
)

func NewBulkWriterStatsInfo(metricGroupId string, contextName, subContextName string) *BulkWriterStatsInfo {
	const semLogContext = "mongo-sink::mew-stats-info"

	stat := &BulkWriterStatsInfo{}
	mg, err := promutil.GetGroup(metricGroupId)
	if err != nil {
		stat.metricErrors = true
		log.Error().Err(err).Str("stageId", subContextName).Msg(semLogContext)
		return stat
	} else {

		stat.WriteInsertedCountMetric, err = mg.CollectorByIdWithLabels(WriteBulkOpsMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "inserted",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteUpsertedCountMetric, err = mg.CollectorByIdWithLabels(WriteBulkOpsMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "upserted",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteModifiedCountMetric, err = mg.CollectorByIdWithLabels(WriteBulkOpsMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "modified",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteMatchedCountMetric, err = mg.CollectorByIdWithLabels(WriteBulkOpsMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "matched",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteDeletedCountMetric, err = mg.CollectorByIdWithLabels(WriteBulkOpsMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "deleted",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteErrsCountMetric, err = mg.CollectorByIdWithLabels(WriteErrsMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "errors",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteBulkSizeGaugeMetric, err = mg.CollectorByIdWithLabels(WriteBulkSizeGaugeMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
			MetricLabelOpCounter:    "bulk-size",
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.SinkDiscardedCountMetric, err = mg.CollectorByIdWithLabels(SinkDiscardedCountMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.WriteDurationHistMetric, err = mg.CollectorByIdWithLabels(WriteDurationHistogramMetric, map[string]string{
			MetricLabelSubContextId: contextName,
			MetricLabelContextId:    subContextName,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}
	}

	return stat
}

func (stat *BulkWriterStatsInfo) SetBulkSize(bulkSize int) {

	stat.BulkSize = bulkSize
	if !stat.metricErrors {
		stat.WriteBulkSizeGaugeMetric.SetMetric(float64(bulkSize))
	}
}

func (stat *BulkWriterStatsInfo) IncDiscarded(n int64) {
	stat.DiscardedCount = n
	if !stat.metricErrors {
		stat.SinkDiscardedCountMetric.SetMetric(float64(n))
	}
}

func (stat *BulkWriterStatsInfo) IncErrors(errs int64) {

	stat.ErrsCount += errs
	if !stat.metricErrors {
		stat.WriteErrsCountMetric.SetMetric(float64(errs))
	}
}

func (stat *BulkWriterStatsInfo) Update(result *mongo.BulkWriteResult, writeDuration time.Duration) {
	stat.DeletedCount += result.DeletedCount
	stat.InsertedCount += result.InsertedCount
	stat.MatchedCount += result.MatchedCount
	stat.ModifiedCount += result.ModifiedCount
	stat.UpsertedCount += result.UpsertedCount

	if !stat.metricErrors {
		stat.WriteMatchedCountMetric.SetMetric(float64(result.MatchedCount))
		stat.WriteModifiedCountMetric.SetMetric(float64(result.ModifiedCount))
		stat.WriteInsertedCountMetric.SetMetric(float64(result.InsertedCount))
		stat.WriteUpsertedCountMetric.SetMetric(float64(result.UpsertedCount))
		stat.WriteDeletedCountMetric.SetMetric(float64(result.DeletedCount))
		stat.WriteDurationHistMetric.SetMetric(writeDuration.Seconds())
	}
}
