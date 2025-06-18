package driver

import (
	_ "embed"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"gopkg.in/yaml.v3"
)

//go:embed jobs-metrics.yaml
var jobMetrics []byte

func JobMetrics(jobMetricsGroupId string) (promutil.MetricGroupConfig, error) {
	var cfg promutil.MetricGroupConfig
	err := yaml.Unmarshal(jobMetrics, &cfg)
	if err != nil {
		return promutil.MetricGroupConfig{}, err
	}

	cfg.GroupId = jobMetricsGroupId
	return cfg, nil
}
