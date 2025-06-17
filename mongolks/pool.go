package mongolks

import (
	"context"
	mongoprom "github.com/globocom/mongo-go-prometheus"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/event"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.opentelemetry.io/contrib/instrumentation/go.mongodb.org/mongo-driver/mongo/otelmongo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"time"
)

type PoolConfigMetrics struct {
	Name                      string
	ConnectionTimeReady       []float64 `mapstructure:"connection-time-ready,omitempty" json:"connection-time-ready,omitempty" yaml:"connection-time-ready,omitempty"`
	ConnectionPoolTimeAcquire []float64 `mapstructure:"connection-pool-time-acquire,omitempty" json:"connection-pool-time-acquire,omitempty" yaml:"connection-pool-time-acquire,omitempty"`
}

var DefaultTimeToReadyConnectionBuckets = []float64{100, 1000, 10_000, 100_000, 200_000, 500_000, 1_000_000, 2_000_000}
var DefaultPoolTimeAcquireBucket = []float64{0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000}

type poolMetric struct {
	AliveConnection metric.Int64UpDownCounter
	UsedConnection  metric.Int64UpDownCounter

	TimeToReadyConnection   metric.Int64Histogram
	TotalCloseConnection    metric.Int64Counter
	TotalReturnedConnection metric.Int64Counter

	TimeToAcquireConnection      metric.Int64Histogram
	TotalFailedAcquireConnection metric.Int64Counter
}

func (poolMetric *poolMetric) getPoolMonitor() *event.PoolMonitor {

	return &event.PoolMonitor{
		Event: func(e *event.PoolEvent) {
			//log.Debug().Str("type", e.Type).Str("duration", e.Duration.String()).Str("address", e.Address).Str("id", fmt.Sprint(e.ConnectionID)).Msg("event from mongo pool")

			attributes := attribute.String("address", e.Address)

			attributesSet := attribute.NewSet(attributes)

			switch e.Type {
			// Created when an operation successfully acquires a connection for execution.
			// Have duration
			case event.GetSucceeded:
				poolMetric.TimeToAcquireConnection.Record(context.Background(), e.Duration.Microseconds(), metric.WithAttributeSet(attributesSet))
				poolMetric.UsedConnection.Add(context.Background(), 1, metric.WithAttributeSet(attributesSet))
				break

			// Created when a connection is checked back into the pool after an operation is executed.
			// Do not have duration
			case event.ConnectionReturned:
				poolMetric.TotalReturnedConnection.Add(context.Background(), 1, metric.WithAttributeSet(attributesSet))
				poolMetric.UsedConnection.Add(context.Background(), -1, metric.WithAttributeSet(attributesSet))
				break

			// Created when a connection is created, but not necessarily when it is used for an operation.
			// Do not have duration
			case event.ConnectionCreated:
				// Connections created can be closed even if they do not reach the 'ready' state."
				poolMetric.AliveConnection.Add(context.Background(), 1, metric.WithAttributeSet(attributesSet))
				break

			// Created after a connection completes a handshake and is ready to be used for operations.
			// Have duration
			case event.ConnectionReady:
				poolMetric.TimeToReadyConnection.Record(context.Background(), e.Duration.Microseconds(), metric.WithAttributeSet(attributesSet))
				break

			// Created when a connection is closed.
			case event.ConnectionClosed:
				poolMetric.TotalCloseConnection.Add(context.Background(), 1, metric.WithAttributeSet(attributesSet))
				poolMetric.AliveConnection.Add(context.Background(), -1, metric.WithAttributeSet(attributesSet))
				break
			// Created when a connection pool is ready.
			// No connection seems to be created before this event
			case event.PoolReady:
				break

			// Created when an operation cannot acquire a connection for execution.
			case event.GetFailed:
				poolMetric.TotalFailedAcquireConnection.Add(context.Background(), 1, metric.WithAttributeSet(attributesSet))
				// ConnectionCheckOutStarted -> ConnectionCheckOutFailed quindi non serve se ascoltiamo ConnectionCheckedOut e ConnectionCheckedIn
				//poolMetric.UsedConnection.Add(context.Background(), -1, metric.WithAttributeSet(attributesSet))

				log.Error().Msg("Mongo Get Failed")
				break

			}
		},
	}
}

type PoolConfig struct {
	MinConn               uint64             `mapstructure:"min-conn,omitempty" json:"min-conn,omitempty" yaml:"min-conn,omitempty"`
	MaxConn               uint64             `mapstructure:"max-conn,omitempty" json:"max-conn,omitempty" yaml:"max-conn,omitempty"`
	MaxWaitTime           int64              `mapstructure:"max-wait-time,omitempty" json:"max-wait-time,omitempty" yaml:"max-wait-time,omitempty"`
	ConnectTimeout        time.Duration      `mapstructure:"connect-timeout,omitempty" json:"connect-timeout,omitempty" yaml:"connect-timeout,omitempty"`
	MaxConnectionIdleTime time.Duration      `mapstructure:"max-conn-idle-time,omitempty" json:"max-conn-idle-time,omitempty" yaml:"max-conn-idle-time,omitempty"`
	MaxConnecting         uint64             `mapstructure:"max-connecting,omitempty" json:"max-connecting,omitempty" yaml:"max-connecting,omitempty"`
	MetricConfig          *PoolConfigMetrics `mapstructure:"metrics" json:"metrics" yaml:"metrics"`
	//MaxConnectionLifeTime int `mapstructure:"max-conn-life-time" json:"max-conn-life-time" yaml:"max-conn-life-time"`
	// MaxWaitQueueSize      int           `mapstructure:"max-wait-queue-size" json:"max-wait-queue-size" yaml:"max-wait-queue-size"`
}

func (cfg *PoolConfig) getOptions(opts *options.ClientOptions) *options.ClientOptions {
	const semLogContext = "mongo-lks::poll-get-options"

	if opts == nil {
		opts = options.Client()
	}

	opts.SetMinPoolSize(cfg.MinConn)
	if cfg.MaxConn != 0 {
		opts.SetMaxPoolSize(cfg.MaxConn)
	}

	if cfg.MaxConnectionIdleTime != 0 {
		opts.SetMaxConnIdleTime(cfg.MaxConnectionIdleTime)
	}

	if cfg.MaxWaitTime > 0 {
		log.Warn().Msg(semLogContext + " pool max-wait-time deprecated use connect-timeout instead")
		if cfg.ConnectTimeout == 0 {
			cfg.ConnectTimeout = time.Duration(cfg.MaxWaitTime) * time.Millisecond
		}
	}

	if cfg.ConnectTimeout != 0 {
		opts.SetConnectTimeout(cfg.ConnectTimeout)
	}

	if cfg.MaxConnecting != 0 {
		opts.SetMaxConnecting(cfg.MaxConnecting)
	}

	opts.Monitor = combineMonitors(
		otelmongo.NewMonitor(otelmongo.WithTracerProvider(otel.GetTracerProvider())),
		mongoprom.NewCommandMonitor(
			mongoprom.WithInstanceName(""),
			mongoprom.WithNamespace(""),
		),
	)

	pm := cfg.newPoolMetrics()
	opts.SetPoolMonitor(pm.getPoolMonitor())

	return opts
}

func (cfg *PoolConfig) newPoolMetrics() *poolMetric {

	pm := &poolMetric{}

	if cfg.MetricConfig.Name == "" {
		cfg.MetricConfig.Name = "tpm-mongo-common"
	}

	otelMeter := otel.Meter(cfg.MetricConfig.Name)

	ConnectionPoolTimeAcquireBucket := DefaultPoolTimeAcquireBucket
	if cfg.MetricConfig.ConnectionPoolTimeAcquire != nil {
		ConnectionPoolTimeAcquireBucket = cfg.MetricConfig.ConnectionPoolTimeAcquire
	}

	TimeToReadyConnectionBuckets := DefaultTimeToReadyConnectionBuckets
	if cfg.MetricConfig.ConnectionTimeReady != nil {
		TimeToReadyConnectionBuckets = cfg.MetricConfig.ConnectionTimeReady
	}

	pm.AliveConnection, _ = otelMeter.Int64UpDownCounter(
		"mongo.connection.alive",
		metric.WithDescription("Total alive connection in the pool"))

	pm.UsedConnection, _ = otelMeter.Int64UpDownCounter(
		"mongo.connection.used",
		metric.WithDescription("Total used connections successfully obtained from the connection pool"))

	pm.TimeToReadyConnection, _ = otelMeter.Int64Histogram(
		"mongo.connection.time.ready",
		metric.WithUnit("us"),
		metric.WithExplicitBucketBoundaries(TimeToReadyConnectionBuckets...),
		metric.WithDescription("Time for a connection completes a handshake and is ready to be used for operations"))

	pm.TotalCloseConnection, _ = otelMeter.Int64Counter(
		"mongo.connection.close",
		metric.WithDescription("Total closed connection"))
	pm.TotalReturnedConnection, _ = otelMeter.Int64Counter(
		"mongo.connection.pool.returned",
		metric.WithDescription("Incremented when a connection is checked back into the pool after an operation is executed"))

	pm.TimeToAcquireConnection, _ = otelMeter.Int64Histogram(
		"mongo.connection.pool.time.acquire",
		metric.WithUnit("us"),
		metric.WithDescription("Time for an operation successfully acquires a connection for execution"),
		metric.WithExplicitBucketBoundaries(ConnectionPoolTimeAcquireBucket...))

	pm.TotalFailedAcquireConnection, _ = otelMeter.Int64Counter(
		"mongo.connection.pool.acquire.failed",
		metric.WithDescription("Total operation cannot acquire a connection for execution"))

	return pm
}

func combineMonitors(monitors ...*event.CommandMonitor) *event.CommandMonitor {
	return &event.CommandMonitor{
		Started: func(ctx context.Context, evt *event.CommandStartedEvent) {
			for _, monitor := range monitors {
				if monitor != nil && monitor.Started != nil {
					monitor.Started(ctx, evt)
				}
			}
		},
		Succeeded: func(ctx context.Context, evt *event.CommandSucceededEvent) {
			for _, monitor := range monitors {
				if monitor != nil && monitor.Succeeded != nil {
					monitor.Succeeded(ctx, evt)
				}
			}
		},
		Failed: func(ctx context.Context, evt *event.CommandFailedEvent) {
			for _, monitor := range monitors {
				if monitor != nil && monitor.Failed != nil {
					monitor.Failed(ctx, evt)
				}
			}
		},
	}
}
