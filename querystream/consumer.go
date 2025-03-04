package querystream

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"io"
	"math/rand"
)

//type ResourceGroup struct {
//	Pid          string `yaml:"pid,omitempty" mapstructure:"pid,omitempty" json:"pid,omitempty"`
//	Gid          string `yaml:"gid,omitempty" mapstructure:"gid,omitempty" json:"gid,omitempty"`
//	Instance     string `yaml:"instance,omitempty" mapstructure:"batch-size,omitempty" json:"batch-size,omitempty"`
//	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
//}
//
//type QuerySource struct {
//	Instance     string `yaml:"instance,omitempty" mapstructure:"instance,omitempty" json:"instance,omitempty"`
//	CollectionId string `yaml:"collection-id,omitempty" mapstructure:"collection-id,omitempty" json:"collection-id,omitempty"`
//	BatchSize    int32  `yaml:"batch-size,omitempty" mapstructure:"batch-size,omitempty" json:"batch-size,omitempty"`
//}
//
//type StreamOptions struct {
//	Consumer    ResourceGroup `yaml:"consumer-group,omitempty" mapstructure:"consumer-group,omitempty" json:"consumer-group,omitempty"`
//	QuerySource QuerySource   `yaml:"query-source,omitempty" mapstructure:"query-source,omitempty" json:"query-source,omitempty"`
//}

type Consumer struct {
	cfg                      *Config
	task                     task.Task
	taskCollection           *mongo.Collection
	qColl                    *mongo.Collection
	shuffledPartitionIndexes []int
	// cfg                      StreamOptions
	// cgColl                   *mongo.Collection

	// prts []partition.Partition

	curPrtNdx    int
	curPrtIsEof  bool
	eventBuffer  *QueryStream
	leaseHandler *lease.Handler

	numEvents int
}

func NewQueryConsumer(cfg *Config, task task.Task, taskColl *mongo.Collection, opts ...ConfigOption) (*Consumer, error) {
	const semLogContext = "query-stream::new"
	var err error

	for _, o := range opts {
		o(cfg)
	}

	queryLksInstance := task.Info.MdbInstance
	queryCollectionId := task.Info.MdbCollection
	if queryLksInstance == "" || queryCollectionId == "" {
		err = errors.New("missing or empty query-collection-instance/query-collection-id")
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	qColl, err := mongolks.GetCollection(context.Background(), queryLksInstance, queryCollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	if len(task.Partitions) == 0 {
		err = errors.New("no partitions available in task")
		log.Error().Err(err).Str("task-id", task.Bid).Msg(semLogContext)
		return nil, err
	}

	shuffledPartitionIndexes := make([]int, len(task.Partitions))
	for i := 0; i < len(shuffledPartitionIndexes); i++ {
		shuffledPartitionIndexes[i] = i
	}
	// Shuffle the partitions in order for different query stream starts from a different position
	rand.Shuffle(len(shuffledPartitionIndexes), func(i, j int) {
		shuffledPartitionIndexes[i], shuffledPartitionIndexes[j] = shuffledPartitionIndexes[j], shuffledPartitionIndexes[i]
	})

	return &Consumer{
		cfg:                      cfg,
		task:                     task,
		taskCollection:           taskColl,
		qColl:                    qColl,
		shuffledPartitionIndexes: shuffledPartitionIndexes,
		curPrtNdx:                -1,
		curPrtIsEof:              true,
		eventBuffer:              NewBatch(qColl, 10),
		leaseHandler:             nil,
	}, nil
}

func (s *Consumer) Commit() error {
	const semLogContext = "consumer::commit"
	return nil
}

func (s *Consumer) Abort() error {
	const semLogContext = "consumer::abort"
	return nil
}

func (qs *Consumer) Close(cts context.Context) error {
	const semLogContext = "query-stream::close"
	if qs.leaseHandler != nil {
		log.Info().Str("leased-obj", qs.leaseHandler.Lease.Bid).Msg(semLogContext + " - releasing lease")
		qs.leaseHandler.Release()
	}
	return nil
}

func (qs *Consumer) EOF() bool {
	return false
}

func (s *Consumer) Poll() (Event, error) {
	const semLogContext = "consumer::process-change-stream"

	evt, err := s.Next()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return evt, err
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

	err = s.Close(context.Background())
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	policy := s.cfg.onErrorPolicy()
	if policy == OnErrorPolicyContinue {
		// s.qs, err = NewQueryStream(s.cfg.Consumer)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			policy = OnErrorPolicyExit
		}
	}

	return policy
}

func (qs *Consumer) Next() (Event, error) {
	const semLogContext = "query-stream::next"
	var err error
	var evt Event
	if !qs.curPrtIsEof {
		evt, err = qs.eventBuffer.Next()
		evt.Partition = qs.task.Partitions[qs.shuffledPartitionIndexes[qs.curPrtNdx]].PartitionNumber
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Info().Interface("evt", evt).Msg(semLogContext + " - reached eof partition")
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
			_ = qs.leaseHandler.Release()
		}

		qs.curPrtNdx, err = qs.acquirePartition()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return EofEvent, err
			}
			return ErrEvent, err
		}

		qs.curPrtIsEof = false
	}

	evt, err = qs.eventBuffer.Next()
	evt.Partition = qs.task.Partitions[qs.shuffledPartitionIndexes[qs.curPrtNdx]].PartitionNumber
	return evt, err
}

func (qs *Consumer) acquirePartition() (int, error) {
	const semLogContext = "query-stream::acquire-partition"

	prtNdx := qs.curPrtNdx
	for {
		if (prtNdx + 1) >= len(qs.shuffledPartitionIndexes) {
			log.Info().Msg(semLogContext + " - partitions exhausted")
			return -1, io.EOF
		}

		prtNdx = prtNdx + 1
		prt := qs.task.Partitions[qs.shuffledPartitionIndexes[prtNdx]]
		lh, ok, err := lease.AcquireLease(qs.taskCollection, qs.cfg.Id, partition.Id(qs.task.Bid, prt.PartitionNumber), false)
		if ok {
			log.Info().Str("partition-id", partition.Id(qs.task.Bid, prt.PartitionNumber)).Msg(semLogContext + " - acquired partition")
			err = qs.eventBuffer.Query(NewResumableFilter(prt.Info.MdbFilter, lh.Lease.Data))
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
