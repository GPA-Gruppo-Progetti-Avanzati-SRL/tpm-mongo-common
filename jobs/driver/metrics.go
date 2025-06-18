package driver

import (
	_ "embed"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/rs/zerolog/log"
	"gopkg.in/yaml.v3"
)

//go:embed jobs-metrics.yaml
var jobMetrics []byte

const JobMetricsGroupId = "jobs"

func JobMetrics(jobMetricsGroupId string) (promutil.MetricGroupConfig, error) {
	var cfg promutil.MetricGroupConfig
	err := yaml.Unmarshal(jobMetrics, &cfg)
	if err != nil {
		return promutil.MetricGroupConfig{}, err
	}

	cfg.GroupId = jobMetricsGroupId
	return cfg, nil
}

func MustJobMetrics(jobMetricsGroupId string) promutil.MetricGroupConfig {
	const semLogContext = "driver::job-metrics"
	cfg, err := JobMetrics(jobMetricsGroupId)
	if err != nil {
		log.Fatal().Err(err).Str("jobMetricsGroupId", jobMetricsGroupId).Msg(semLogContext)
	}

	return cfg
}
