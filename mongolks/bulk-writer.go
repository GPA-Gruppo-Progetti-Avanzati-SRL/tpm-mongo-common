package mongolks

import (
	"context"
	"errors"
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

func NewBulkWriter(instanceName, collId string, opts ...BulkWriterOption) (*BulkWriter, error) {
	const semLogContext = "bulk-writer::new"
	coll, err := GetCollection(context.Background(), instanceName, collId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	wrtOptions := BulkWriterOptions{Size: 100, Ordered: false}
	for _, opt := range opts {
		opt(&wrtOptions)
	}

	return &BulkWriter{coll: coll,
		opts:  wrtOptions,
		batch: make([]mongo.WriteModel, 0, wrtOptions.Size),
		stats: NewBulkWriterStatsInfo(wrtOptions.MetricsGid, wrtOptions.PrimaryLabel, wrtOptions.SecondaryLabel)}, nil
}

func (w *BulkWriter) String() string {
	return w.stats.String()
}

func (w *BulkWriter) Stats() *BulkWriterStatsInfo {
	return w.stats
}

func (w *BulkWriter) Flush() (int, error) {
	const semLogContext = "bulk-writer::flush"

	sz := len(w.batch)
	if sz > 0 {
		begin := time.Now()
		blkOpts := options.BulkWrite()
		blkOpts.SetOrdered(w.opts.Ordered)
		resp, err := w.coll.BulkWrite(context.Background(), w.batch, blkOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			w.batch = w.batch[:0]
			w.stats.IncErrors(1)
			return sz, err
		}

		w.stats.Update(resp, time.Since(begin))
		log.Info().Interface("resp", resp).Msg(semLogContext)
		w.batch = w.batch[:0]
	}

	return sz, nil
}

func (w *BulkWriter) Write(wm mongo.WriteModel) (int, error) {
	const semLogContext = "bulk-writer::write"
	w.batch = append(w.batch, wm)
	if w.opts.Size > 0 && len(w.batch) >= w.opts.Size {
		return w.Flush()
	}

	return 0, nil
}

func (w *BulkWriter) Insert(item interface{}) (int, error) {
	const semLogContext = "bulk-writer::insert"

	wm := mongo.NewInsertOneModel().SetDocument(item)
	w.batch = append(w.batch, wm)
	if w.opts.Size > 0 && len(w.batch) >= w.opts.Size {
		return w.Flush()
	}

	return 0, nil
}

func (w *BulkWriter) Update(filter interface{}, updateDoc interface{}, withUpsert bool) (int, error) {
	wm := mongo.NewUpdateOneModel().SetUpdate(updateDoc).SetUpsert(withUpsert).SetFilter(filter)
	w.batch = append(w.batch, wm)
	if w.opts.Size > 0 && len(w.batch) >= w.opts.Size {
		return w.Flush()
	}

	return 0, nil
}

type BulkWriterSet struct {
	opts        BulkWriterOptions
	writers     map[string]*BulkWriter
	currentSize int
}

func NewBulkWriterSet(opts ...BulkWriterOption) *BulkWriterSet {
	wrtOptions := BulkWriterOptions{Size: 100, Ordered: false}
	for _, opt := range opts {
		opt(&wrtOptions)
	}

	return &BulkWriterSet{
		opts:    wrtOptions,
		writers: make(map[string]*BulkWriter),
	}
}

func (b *BulkWriterSet) Add(instanceName, collId string, opts ...BulkWriterOption) error {
	const semLogContext = "bulk-writer-set::add"

	if _, ok := b.writers[collId]; ok {
		err := errors.New("bulk-writer already in set")
		log.Error().Err(err).Str("name", collId).Msg(semLogContext)
		return err
	}

	blkWrt, err := NewBulkWriter(instanceName, collId, opts...)
	if err != nil {
		log.Error().Err(err).Str("name", collId).Msg(semLogContext)
		return err
	}

	blkWrt.opts.Size = 0
	b.writers[collId] = blkWrt
	return nil
}

func (b *BulkWriterSet) Size() int {
	const semLogContext = "bulk-writer-set::size"

	size := 0
	for _, wrt := range b.writers {
		size += len(wrt.batch)
	}

	return size
}

func (b *BulkWriterSet) Write(nm string, wm mongo.WriteModel) (int, error) {
	const semLogContext = "bulk-writer-set::write"

	wrt, ok := b.writers[nm]
	if !ok {
		err := errors.New("bulk-writer not present")
		log.Error().Err(err).Str("name", nm).Msg(semLogContext)
		return 0, err
	}
	sz, err := wrt.Write(wm)
	if err != nil {
		log.Error().Err(err).Str("name", nm).Msg(semLogContext)
		return sz, err
	}

	flushedSize := 0
	b.currentSize += 1
	if b.opts.Size > 0 && b.currentSize >= b.opts.Size {
		for nm, wrt = range b.writers {
			sz, err = wrt.Flush()
			flushedSize += sz
			b.currentSize += len(wrt.batch)
			if err != nil {
				log.Error().Err(err).Str("name", nm).Int("flushed", sz).Msg(semLogContext)
				return sz, err
			}

			log.Info().Err(err).Str("name", nm).Int("flushed", sz).Msg(semLogContext)
		}
	}

	return flushedSize, nil
}

func (b *BulkWriterSet) Insert(nm string, item interface{}) (int, error) {
	const semLogContext = "bulk-writer-set::insert"
	wm := mongo.NewInsertOneModel().SetDocument(item)
	return b.Write(nm, wm)
}

func (b *BulkWriterSet) Update(nm string, filter interface{}, updateDoc interface{}, withUpsert bool) (int, error) {
	const semLogContext = "bulk-writer-set::update"
	wm := mongo.NewUpdateOneModel().SetUpdate(updateDoc).SetUpsert(withUpsert).SetFilter(filter)
	return b.Write(nm, wm)
}

func (b *BulkWriterSet) Flush() (int, error) {
	const semLogContext = "bulk-writer-set::flush"

	flushedSize := 0
	b.currentSize = 0
	for nm, wrt := range b.writers {
		sz, err := wrt.Flush()
		flushedSize += sz
		b.currentSize += len(wrt.batch)
		if err != nil {
			log.Error().Err(err).Str("name", nm).Int("flushed", sz).Msg(semLogContext)
			return flushedSize, err
		}

		log.Info().Err(err).Str("name", nm).Int("flushed", sz).Msg(semLogContext)
	}

	log.Info().Int("flushed-size", flushedSize).Msg(semLogContext)
	return flushedSize, nil
}
