package worker

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"

const (
	MetricLabelName       = "name"
	MetricRewindsCounter  = "cdc-rewinds"
	MetricBatchErrors     = "cdc-batch-errors"
	MetricBatches         = "cdc-batches"
	MetricBatchSize       = "cdc-batch-size"
	MetricBatchDuration   = "cdc-batch-duration"
	MetricMessageErrors   = "cdc-event-errors"
	MetricMessages        = "cdc-events"
	MetricMessageDuration = "cdc-event-duration"
)

type Metrics struct {
	Rewinds         int
	BatchErrors     int
	Batches         int
	BatchSize       int
	BatchDuration   float64
	MessageErrors   int
	Messages        int
	MessageDuration float64

	RewindsCounterMetric       promutil.CollectorWithLabels
	BatchesCounterMetric       promutil.CollectorWithLabels
	BatchErrorsCounterMetric   promutil.CollectorWithLabels
	BatchSizeGaugeMetric       promutil.CollectorWithLabels
	BatchDurationHistogram     promutil.CollectorWithLabels
	MessageErrorsCounterMetric promutil.CollectorWithLabels
	MessagesCounterMetric      promutil.CollectorWithLabels
	MessageDurationHistogram   promutil.CollectorWithLabels
	metricErrors               bool
}

func (stat *Metrics) Clear() *Metrics {
	stat.Rewinds = 0
	stat.BatchErrors = 0
	stat.Batches = 0
	stat.BatchSize = 0
	stat.BatchDuration = 0
	stat.MessageErrors = 0
	stat.Messages = 0
	stat.MessageDuration = 0
	return stat
}

func (stat *Metrics) IncRewinds() {
	stat.Rewinds++
	if !stat.metricErrors {
		stat.RewindsCounterMetric.SetMetric(1)
	}
}

func (stat *Metrics) IncBatchErrors() {
	stat.BatchErrors++
	if !stat.metricErrors {
		stat.BatchErrorsCounterMetric.SetMetric(1)
	}
}

func (stat *Metrics) IncBatches() {
	stat.Batches++
	if !stat.metricErrors {
		stat.BatchesCounterMetric.SetMetric(1)
	}
}

func (stat *Metrics) IncMessageErrors() {
	stat.MessageErrors++
	if !stat.metricErrors {
		stat.MessageErrorsCounterMetric.SetMetric(1)
	}
}

func (stat *Metrics) IncMessages() {
	stat.Messages++
	if !stat.metricErrors {
		stat.MessagesCounterMetric.SetMetric(1)
	}
}

func (stat *Metrics) SetBatchSize(sz int) {
	stat.BatchSize = sz
	if !stat.metricErrors {
		stat.BatchSizeGaugeMetric.SetMetric(float64(sz))
	}
}

func (stat *Metrics) SetBatchDuration(dur float64) {
	stat.BatchDuration = dur
	if !stat.metricErrors {
		stat.BatchDurationHistogram.SetMetric(dur)
	}
}

func (stat *Metrics) SetMessageDuration(dur float64) {
	stat.MessageDuration = dur
	if !stat.metricErrors {
		stat.MessageDurationHistogram.SetMetric(dur)
	}
}

func NewProducerStatsInfo(whatcherId, metricGroupId string) *Metrics {
	stat := &Metrics{}
	mg, err := promutil.GetGroup(metricGroupId)
	if err != nil {
		stat.metricErrors = true
		return stat
	} else {
		stat.RewindsCounterMetric, err = mg.CollectorByIdWithLabels(MetricRewindsCounter, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchErrorsCounterMetric, err = mg.CollectorByIdWithLabels(MetricBatchErrors, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchesCounterMetric, err = mg.CollectorByIdWithLabels(MetricBatches, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchSizeGaugeMetric, err = mg.CollectorByIdWithLabels(MetricBatchSize, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchDurationHistogram, err = mg.CollectorByIdWithLabels(MetricBatchDuration, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MessageErrorsCounterMetric, err = mg.CollectorByIdWithLabels(MetricMessageErrors, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MessagesCounterMetric, err = mg.CollectorByIdWithLabels(MetricMessages, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.MessageDurationHistogram, err = mg.CollectorByIdWithLabels(MetricMessageDuration, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

	}

	return stat
}
