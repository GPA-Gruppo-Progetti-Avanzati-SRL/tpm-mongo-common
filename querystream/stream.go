package querystream

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/partition"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"math/rand"
)

type ResourceGroup struct {
	Pid          string `yaml:"pid,omitempty" mapstructure:"pid,omitempty" json:"pid,omitempty"`
	Gid          string `yaml:"gid,omitempty" mapstructure:"gid,omitempty" json:"gid,omitempty"`
	Instance     string `yaml:"instance,omitempty" mapstructure:"batch-size,omitempty" json:"batch-size,omitempty"`
	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
}

type QuerySource struct {
	Instance     string `yaml:"instance,omitempty" mapstructure:"instance,omitempty" json:"instance,omitempty"`
	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
	BatchSize    int32  `yaml:"batch-size,omitempty" mapstructure:"batch-size,omitempty" json:"batch-size,omitempty"`
}

type StreamOptions struct {
	Consumer    ResourceGroup `yaml:"consumer-group,omitempty" mapstructure:"consumer-group,omitempty" json:"consumer-group,omitempty"`
	QuerySource QuerySource   `yaml:"query-source,omitempty" mapstructure:"query-source,omitempty" json:"query-source,omitempty"`
}

type QueryStream struct {
	cfg    StreamOptions
	cgColl *mongo.Collection
	qColl  *mongo.Collection
	prts   []partition.Partition

	curPrtNdx    int
	curPrtIsEof  bool
	eventBuffer  *StreamBatch
	leaseHandler *lease.Handler
}

func NewQueryStream(cfg StreamOptions) (*QueryStream, error) {
	const semLogContext = "query-stream::new"

	cgColl, err := mongolks.GetCollection(context.Background(), cfg.Consumer.Instance, cfg.Consumer.CollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	qColl, err := mongolks.GetCollection(context.Background(), cfg.QuerySource.Instance, cfg.QuerySource.CollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	prts, err := partition.FindAllPartitionsByTypeAndPartitionGroupAndStatus(cgColl, partition.MongoPartitionType, cfg.Consumer.Pid, partition.StatusAvailable /*, false, primitive.NilObjectID */)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	if len(prts) == 0 {
		err = errors.New("no partitions found")
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	// Shuffle the partitions in order for different query stream starts from a different position
	rand.Shuffle(len(prts), func(i, j int) { prts[i], prts[j] = prts[j], prts[i] })

	return &QueryStream{cfg: cfg, cgColl: cgColl, prts: prts, qColl: qColl, curPrtNdx: -1, curPrtIsEof: true, eventBuffer: NewBatch(qColl, int64(cfg.QuerySource.BatchSize))}, nil
}

func (qs *QueryStream) Close(cts context.Context) error {
	const semLogContext = "query-stream::close"
	if qs.leaseHandler != nil {
		log.Info().Str("leased-obj", qs.leaseHandler.Lease.Bid).Msg(semLogContext + " - releasing lease")
		qs.leaseHandler.Release()
	}
	return nil
}

func (qs *QueryStream) EOF() bool {
	return false
}

func (qs *QueryStream) Next() (Event, error) {
	const semLogContext = "query-stream::next"
	var err error
	var evt Event
	if !qs.curPrtIsEof {
		evt, err = qs.eventBuffer.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Info().Msg(semLogContext + " - reached eof partition")
				qs.curPrtIsEof = true
			} else {
				log.Error().Err(err).Msg(semLogContext)
				return evt, err
			}
		} else {
			return evt, nil
		}
	}

	if qs.curPrtIsEof {
		if qs.leaseHandler != nil {
			qs.leaseHandler.Release()
		}

		qs.curPrtNdx, err = qs.acquirePartition()
		if err != nil {
			return ZeroEvent, err
		}

		qs.curPrtIsEof = false
	}

	evt, err = qs.eventBuffer.Next()
	return evt, err
}

func (qs *QueryStream) acquirePartition() (int, error) {
	const semLogContext = "query-stream::acquire-partition"

	prtNdx := qs.curPrtNdx
	for {
		if (prtNdx + 1) >= len(qs.prts) {
			log.Info().Msg(semLogContext + " - partitions exhausted")
			return -1, io.EOF
		}

		prtNdx = prtNdx + 1
		prt := qs.prts[prtNdx]
		lh, ok, err := lease.AcquireLease(qs.cgColl, qs.cfg.Consumer.Gid, partition.Id(qs.cfg.Consumer.Pid, prt.PartitionNumber), false)
		if ok {
			log.Info().Str("partition-id", partition.Id(qs.cfg.Consumer.Pid, prt.PartitionNumber)).Msg(semLogContext + " - acquired partition")
			err = qs.eventBuffer.Query(prt.Mongo.Filter, lh.Lease.Data)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				_ = lh.Release()
				return -1, err
			}
			if qs.eventBuffer.isEof {
				log.Info().Int32("partition-number", prt.PartitionNumber).Msg(semLogContext + " - partition is eof")
				_ = lh.Release()
			}

			qs.leaseHandler = lh
			return prtNdx, nil
		} else {
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}
		}
	}

}
