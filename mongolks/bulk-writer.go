package mongolks

import (
	"context"
	"time"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

type BulkWriterOptions struct {
	Ordered        bool   `yaml:"ordered,omitempty" mapstructure:"ordered,omitempty" json:"ordered,omitempty"`
	Size           int    `yaml:"size,omitempty" mapstructure:"size,omitempty" json:"size,omitempty"`
	MetricsGid     string `yaml:"metrics-gid,omitempty" mapstructure:"metrics-gid,omitempty" json:"metrics-gid,omitempty"`
	PrimaryLabel   string `yaml:"metrics-primary-label,omitempty" mapstructure:"metrics-primary-label,omitempty" json:"metrics-primary-label,omitempty"`
	SecondaryLabel string `yaml:"metrics-secondary-label,omitempty" mapstructure:"metrics-secondary-label,omitempty" json:"metrics-secondary-label,omitempty"`
}

type BulkWriterOption func(*BulkWriterOptions)

func BulkWriterWithOrdered(b bool) BulkWriterOption {
	return func(o *BulkWriterOptions) {
		o.Ordered = b
	}
}

func BulkWriterWithSize(sz int) BulkWriterOption {
	return func(o *BulkWriterOptions) {
		o.Size = sz
	}
}

func BulkWriterWithMetrics(groupId, primary, secondary string) BulkWriterOption {
	return func(o *BulkWriterOptions) {
		o.PrimaryLabel = primary
		o.SecondaryLabel = secondary
	}
}

type BulkWriter struct {
	coll  *mongo.Collection
	opts  BulkWriterOptions
	batch []mongo.WriteModel
	stats *BulkWriterStatsInfo
}

func NewBulkWriter(coll *mongo.Collection, opts ...BulkWriterOption) *BulkWriter {
	options := BulkWriterOptions{Size: 100, Ordered: false}
	for _, opt := range opts {
		opt(&options)
	}

	return &BulkWriter{coll: coll,
		opts:  options,
		batch: make([]mongo.WriteModel, 0, options.Size),
		stats: NewBulkWriterStatsInfo(options.MetricsGid, options.PrimaryLabel, options.SecondaryLabel)}
}

func (w *BulkWriter) String() string {
	return w.stats.String()
}

func (w *BulkWriter) Stats() *BulkWriterStatsInfo {
	return w.stats
}

func (w *BulkWriter) Flush() error {
	const semLogContext = "bulk-writer::flush"
	if len(w.batch) > 0 {
		begin := time.Now()
		blkOpts := options.BulkWrite()
		blkOpts.SetOrdered(w.opts.Ordered)
		resp, err := w.coll.BulkWrite(context.Background(), w.batch, blkOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			w.batch = w.batch[:0]
			w.stats.IncErrors(1)
			return err
		} else {
			w.stats.Update(resp, time.Since(begin))
		}

		log.Info().Interface("resp", resp).Msg(semLogContext)
		w.batch = w.batch[:0]
	}

	return nil
}

func (w *BulkWriter) Write(wm mongo.WriteModel) error {
	const semLogContext = "bulk-writer::write"
	w.batch = append(w.batch, wm)
	if len(w.batch) >= w.opts.Size {
		return w.Flush()
	}

	return nil
}

func (w *BulkWriter) Insert(item interface{}) error {
	const semLogContext = "bulk-writer::insert"

	wm := mongo.NewInsertOneModel().SetDocument(item)
	w.batch = append(w.batch, wm)
	if len(w.batch) >= w.opts.Size {
		return w.Flush()
	}

	return nil
}

func (w *BulkWriter) Update(filter interface{}, updateDoc interface{}, withUpsert bool) error {
	wm := mongo.NewUpdateOneModel().SetUpdate(updateDoc).SetUpsert(withUpsert).SetFilter(filter)
	w.batch = append(w.batch, wm)
	if len(w.batch) >= w.opts.Size {
		return w.Flush()
	}

	return nil
}
