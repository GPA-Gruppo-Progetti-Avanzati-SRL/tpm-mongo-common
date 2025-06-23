package worker

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"

const (
	MetricLabelName = "name"

	MetricEvents                = "wrk-events"
	MetricEventProcessErrors    = "wrk-process-errors"
	MetricEventProcessGroups    = "wrk-process-groups"
	MetricEventProcessGroupSize = "wrk-process-size"
	MetricEventProcessDuration  = "wrk-process-duration"
)

type Metrics struct {
	Events        int
	BatchErrors   int
	Batches       int
	BatchSize     int
	BatchDuration float64

	EventsCounterMetric      promutil.CollectorWithLabels
	BatchesCounterMetric     promutil.CollectorWithLabels
	BatchErrorsCounterMetric promutil.CollectorWithLabels
	BatchSizeGaugeMetric     promutil.CollectorWithLabels
	BatchDurationHistogram   promutil.CollectorWithLabels
	metricErrors             bool
}

func (stat *Metrics) Clear() *Metrics {
	stat.Events = 0
	stat.BatchErrors = 0
	stat.Batches = 0
	stat.BatchSize = 0
	stat.BatchDuration = 0
	return stat
}

func (stat *Metrics) IncEvents() {
	stat.Events++
	if !stat.metricErrors {
		stat.EventsCounterMetric.SetMetric(1)
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

func NewWorkerMetrics(whatcherId, metricGroupId string) *Metrics {
	stat := &Metrics{}
	mg, err := promutil.GetGroup(metricGroupId)
	if err != nil {
		stat.metricErrors = true
		return stat
	} else {
		stat.EventsCounterMetric, err = mg.CollectorByIdWithLabels(MetricEvents, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchErrorsCounterMetric, err = mg.CollectorByIdWithLabels(MetricEventProcessErrors, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchesCounterMetric, err = mg.CollectorByIdWithLabels(MetricEventProcessGroups, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchSizeGaugeMetric, err = mg.CollectorByIdWithLabels(MetricEventProcessGroupSize, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

		stat.BatchDurationHistogram, err = mg.CollectorByIdWithLabels(MetricEventProcessDuration, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}

	}

	return stat
}
