package querystream

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/rs/zerolog/log"
	"io"
)

type Consumer struct {
	cfg       *Config
	qs        *QueryStream
	numEvents int
}

func NewConsumer(cfg *Config, opts ...ConfigOption) (*Consumer, error) {
	const semLogContext = "consumer::new"
	var err error

	for _, o := range opts {
		o(cfg)
	}

	s := &Consumer{
		cfg: cfg,
	}

	s.qs, err = NewQueryStream(cfg.QueryStream)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	return s, err
}

func (s *Consumer) Close() {
	const semLogContext = "consumer::close"
	log.Info().Msg(semLogContext)
}

func (s *Consumer) Commit() error {
	const semLogContext = "consumer::commit"
	return nil
}

func (s *Consumer) Abort() error {
	const semLogContext = "consumer::abort"
	return nil
}

func (s *Consumer) Poll() (Event, error) {
	const semLogContext = "consumer::process-change-stream"

	evt, err := s.qs.Next()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return ZeroEvent, err
	}

	var g *promutil.Group

	s.numEvents++
	g = s.setMetric(g, "cdc-events", 1, nil)

	return evt, nil
}

func (s *Consumer) handleError(err error) string {
	const semLogContext = "consumer::on-error"

	if err == io.EOF {
		log.Info().Msg(semLogContext + " query stream closed")
	}

	err = s.qs.Close(context.Background())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	policy := s.cfg.onErrorPolicy()
	if policy == OnErrorPolicyContinue {
		s.qs, err = NewQueryStream(s.cfg.QueryStream)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			policy = OnErrorPolicyExit
		}
	}

	return policy
}

func (s *Consumer) setMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "watcher::set-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(s.cfg.RefMetrics.GId)
		if err != nil {
			log.Trace().Err(err).Msg(semLogContext)
			return nil
		}

		metricGroup = &g
	}

	err = metricGroup.SetMetricValueById(metricId, value, labels)
	if err != nil {
		log.Warn().Err(err).Msg(semLogContext)
	}

	return metricGroup
}
