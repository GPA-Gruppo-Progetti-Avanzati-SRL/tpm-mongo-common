package taskconsumer

import "github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"

const (
	MetricLabelName = "name"
	MetricNumEvents = "qmdb-events"
)

type Metrics struct {
	NumEvents              int
	NumEventsCounterMetric promutil.CollectorWithLabels
	metricErrors           bool
}

func (stat *Metrics) Clear() *Metrics {
	stat.NumEvents = 0
	return stat
}

func (stat *Metrics) IncNumEvents() {
	stat.NumEvents++
	if !stat.metricErrors {
		stat.NumEventsCounterMetric.SetMetric(1)
	}
}

func NewMetrics(whatcherId, metricGroupId string) *Metrics {
	stat := &Metrics{}
	mg, err := promutil.GetGroup(metricGroupId)
	if err != nil {
		stat.metricErrors = true
		return stat
	} else {
		stat.NumEventsCounterMetric, err = mg.CollectorByIdWithLabels(MetricNumEvents, map[string]string{
			MetricLabelName: whatcherId,
		})
		if err != nil {
			stat.metricErrors = true
			return stat
		}
	}

	return stat
}
