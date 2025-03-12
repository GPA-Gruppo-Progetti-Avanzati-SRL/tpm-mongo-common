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
	"go.mongodb.org/mongo-driver/bson/primitive"
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

const (
	LeaseDataResumeId        = "resume_id"
	LeaseDataPartitionStatus = "partition_status"
)

type Consumer struct {
	cfg                      *Config
	task                     task.Task
	taskCollection           *mongo.Collection
	qColl                    *mongo.Collection
	shuffledPartitionIndexes []int
	// cfg                      StreamOptions
	// cgColl                   *mongo.Collection

	// prts []partition.Partition

	curPrtNdx       int
	curPrtIsEof     bool
	curPrtWithFails int
	qStream         *QueryStream
	leaseHandler    *lease.Handler

	uncommittedEvents  int
	lastCommittedEvent Event
	lastPolledEvent    Event

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
		qStream:                  NewQueryStream(qColl, 10),
		leaseHandler:             nil,
		lastPolledEvent:          NoEvent,
		lastCommittedEvent:       NoEvent,
	}, nil
}

func (s *Consumer) UpdateLeaseData(evt Event, status string) error {
	const semLogContext = "consumer::update-lease"
	var err error

	if s.leaseHandler != nil {
		renew := false
		if evt.Key != primitive.NilObjectID {
			if s.leaseHandler.Lease.Bid == partition.Id(s.task.Bid, evt.Partition) {
				s.lastCommittedEvent = evt
				s.leaseHandler.SetLeaseData(LeaseDataResumeId, evt.Key.Hex())
				s.uncommittedEvents = 0
				renew = true
			} else {
				err = errors.New("lease not acquired by consumer")
				log.Warn().Err(err).Interface("evt", evt).Msg(semLogContext)
				return nil
			}
		}

		if status != "" {
			s.leaseHandler.SetLeaseData(LeaseDataPartitionStatus, status)
			renew = true
		}

		if renew {
			err = s.leaseHandler.RenewLease()
		}
	} else {
		log.Warn().Msg(semLogContext + " - cannot update not owned lease")
	}

	return err
}

func (s *Consumer) CommitEvent(evt Event, forceSync bool) error {
	const semLogContext = "consumer::commit"
	var err error

	if s.curPrtWithFails > 0 {
		log.Info().Err(err).Interface("fails", s.curPrtWithFails).Msg(semLogContext + " - current partition with fails")
		return nil
	}

	if evt.Key != primitive.NilObjectID {
		if s.leaseHandler != nil && s.leaseHandler.Lease.Bid == partition.Id(s.task.Bid, evt.Partition) {
			s.lastCommittedEvent = evt
			s.uncommittedEvents++
			if s.uncommittedEvents == 3 || forceSync {
				err = s.UpdateLeaseData(evt, "")
				s.uncommittedEvents = 0
			}
		} else {
			err = errors.New("lease not acquired by consumer")
			log.Warn().Err(err).Interface("evt", evt).Msg(semLogContext)
		}
	} else {
		err = errors.New("commit called on nil object event")
		log.Warn().Err(err).Interface("evt", evt).Msg(semLogContext)
	}

	return err
}

func (s *Consumer) Commit() error {
	return s.CommitEvent(s.lastPolledEvent, false)
	//const semLogContext = "consumer::commit"
	//var err error
	//if s.lastPolledEvent.Key != primitive.NilObjectID {
	//	if s.leaseHandler != nil && s.leaseHandler.Lease.Bid == partition.Id(s.task.Bid, s.lastPolledEvent.Partition) {
	//		log.Info().Msg(semLogContext + ": commit lease")
	//		err = s.leaseHandler.SetLeaseData("resume_id", s.lastPolledEvent.Key.Hex(), true)
	//	} else {
	//		err = errors.New("lease not acquired by consumer")
	//		log.Warn().Err(err).Interface("evt", s.lastPolledEvent).Msg(semLogContext)
	//	}
	//} else {
	//	err = errors.New("commit called on nil object event")
	//	log.Warn().Err(err).Interface("evt", s.lastPolledEvent).Msg(semLogContext)
	//}
	//
	//return err
}

func (s *Consumer) CommitsPending() bool {
	return s.uncommittedEvents > 0
}

func (s *Consumer) Abort() error {
	const semLogContext = "consumer::abort"
	return s.AbortPartial(NoEvent)
}

func (s *Consumer) AbortPartial(lastCommittableEvent Event) error {
	const semLogContext = "consumer::abort-partial"
	var err error

	if lastCommittableEvent.IsDocument() {
		err = s.CommitEvent(lastCommittableEvent, true)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}
	} else if s.CommitsPending() {
		err = s.CommitEvent(s.lastCommittedEvent, true)
	}

	s.curPrtWithFails++
	return err
}

func (qs *Consumer) Close(cts context.Context) error {
	const semLogContext = "consumer::close"
	if qs.leaseHandler != nil {
		_ = qs.leaseHandler.Release()
	}
	return nil
}

func (qs *Consumer) EOF() bool {
	return false
}

func (s *Consumer) Poll() (Event, error) {
	const semLogContext = "consumer::poll"

	var evt Event
	var err error
	if s.curPrtIsEof {
		if s.leaseHandler != nil {
			err = s.leaseHandler.Release()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return ErrEvent, err
			}
		}
		evt, err = s.NextPartition()
	} else {
		evt, err = s.Next()
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return evt, err
	}

	if evt.IsDocument() {
		s.lastPolledEvent = evt
	} else {
		err = s.handleBoundaryEvent(evt)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}
	}

	//} else if evt.EofPartition && !s.lastCommittedEvent.IsZero() {
	//	err = s.CommitEvent("", s.lastCommittedEvent, true)
	//	if err != nil {
	//		log.Error().Err(err).Msg(semLogContext)
	//	}
	//}

	var g *promutil.Group

	switch {
	case evt.IsDocument():
		log.Info().Interface("evt", evt).Msg(semLogContext + " - document")
		s.numEvents++
		g = s.setMetric(g, "cdc-events", 1, nil)
	case evt.IsBoundary():
		log.Info().Interface("evt", evt).Msg(semLogContext + " - boundary")

	case evt.IsErr:
		log.Error().Interface("evt", evt).Msg(semLogContext + " - err")
	default:
		log.Info().Interface("evt", evt).Msg(semLogContext + " - zero?")
	}

	return evt, nil
}

func (s *Consumer) handleBoundaryEvent(evt Event) error {
	const semLogContext = "consumer::handle-boundary-event"

	var err error
	if evt.EofPartition && s.curPrtWithFails == 0 {
		var st string
		if s.task.DataStreamType == task.DataStreamTypeFinite {
			st = "EOF"
		}

		if s.uncommittedEvents > 0 || st != "" {
			evt1 := NoEvent
			if s.lastCommittedEvent.Key != primitive.NilObjectID {
				if s.leaseHandler != nil && s.leaseHandler.Lease.Bid == partition.Id(s.task.Bid, s.lastCommittedEvent.Partition) {
					evt1 = s.lastCommittedEvent
				} else {
					err = errors.New("lease not acquired by consumer")
					log.Error().Err(err).Interface("evt", evt).Msg(semLogContext)
					return err
				}
			}

			err = s.UpdateLeaseData(evt1, st)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}

			s.uncommittedEvents = 0
		}

		if s.task.DataStreamType == task.DataStreamTypeFinite {
			err = s.task.UpdatePartitionStatus(s.taskCollection, s.task.Bid, evt.Partition, "EOF")
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return err
			}
		}
	}

	return err
}

func (s *Consumer) CommitOnEof() error {
	const semLogContext = "consumer::commit"
	var err error

	if s.curPrtWithFails > 0 {
		log.Info().Err(err).Interface("fails", s.curPrtWithFails).Msg(semLogContext + " - current partition with fails")
		return nil
	}

	return err
}

//func (s *Consumer) handleError(err error) string {
//	const semLogContext = "consumer::handle-error"
//
//	if err == io.EOF {
//		log.Info().Msg(semLogContext + " query stream closed")
//	}
//
//	err = s.Close(context.Background())
//	if err != nil {
//		log.Error().Err(err).Msg(semLogContext)
//	}
//
//	policy := s.cfg.onErrorPolicy()
//	if policy == OnErrorPolicyContinueCurrentPartition {
//		// s.qs, err = NewQueryStream(s.cfg.Consumer)
//		if err != nil {
//			log.Error().Err(err).Msg(semLogContext)
//			policy = OnErrorPolicyExit
//		}
//	}
//
//	return policy
//}

func (qs *Consumer) Next() (Event, error) {
	const semLogContext = "consumer::next"
	var err error
	var evt Event

	if !qs.curPrtIsEof {
		evt, err = qs.qStream.Next()
		evt.Partition = qs.task.Partitions[qs.shuffledPartitionIndexes[qs.curPrtNdx]].PartitionNumber
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Info().Interface("evt", evt).Msg(semLogContext + " - reached eof partition")
				qs.curPrtIsEof = true
				err = nil
			} else {
				log.Error().Err(err).Msg(semLogContext)
			}
		}

		return evt, nil
	}

	return ErrEvent, errors.New("partition is already eof-ed")
}

func (qs *Consumer) NextPartition() (Event, error) {
	const semLogContext = "consumer::next-partition"
	var err error
	var evt Event

	qs.curPrtNdx, err = qs.acquirePartition()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return EofEvent, err
		}
		return ErrEvent, err
	}

	qs.curPrtIsEof = false
	qs.curPrtWithFails = 0
	evt, err = qs.qStream.Next()
	evt.Partition = qs.task.Partitions[qs.shuffledPartitionIndexes[qs.curPrtNdx]].PartitionNumber
	return evt, err
}

//func (qs *Consumer) Next3() (Event, error) {
//	const semLogContext = "query-stream::next"
//	var err error
//	var evt Event
//	if !qs.curPrtIsEof {
//		evt, err = qs.qStream.Next()
//		evt.Partition = qs.task.Partitions[qs.shuffledPartitionIndexes[qs.curPrtNdx]].PartitionNumber
//		if err != nil {
//			if errors.Is(err, io.EOF) {
//				log.Info().Interface("evt", evt).Msg(semLogContext + " - reached eof partition")
//				qs.curPrtIsEof = true
//			} else {
//				log.Error().Err(err).Msg(semLogContext)
//				return evt, err
//			}
//		} else {
//			return evt, nil
//		}
//	}
//
//	if qs.curPrtIsEof {
//		if qs.leaseHandler != nil {
//			_ = qs.leaseHandler.Release()
//		}
//
//		qs.curPrtNdx, err = qs.acquirePartition()
//		if err != nil {
//			if errors.Is(err, io.EOF) {
//				return EofEvent, err
//			}
//			return ErrEvent, err
//		}
//
//		qs.curPrtIsEof = false
//	}
//
//	evt, err = qs.qStream.Next()
//	evt.Partition = qs.task.Partitions[qs.shuffledPartitionIndexes[qs.curPrtNdx]].PartitionNumber
//	return evt, err
//}

func (qs *Consumer) acquirePartition() (int, error) {
	const semLogContext = "consumer::acquire-partition"

	prtNdx := qs.curPrtNdx
	for {
		if (prtNdx + 1) >= len(qs.shuffledPartitionIndexes) {
			log.Info().Msg(semLogContext + " - partitions exhausted")
			return -1, io.EOF
		}

		prtNdx = prtNdx + 1
		prt := qs.task.Partitions[qs.shuffledPartitionIndexes[prtNdx]]
		if prt.Status == partition.StatusAvailable {
			lh, ok, err := lease.AcquireLease(qs.taskCollection, qs.cfg.Id, partition.Id(qs.task.Bid, prt.PartitionNumber), false)
			if ok {
				log.Info().Str("partition-id", partition.Id(qs.task.Bid, prt.PartitionNumber)).Msg(semLogContext + " - acquired partition")
				err = qs.qStream.Query(NewResumableFilter(prt.Info.MdbFilter, prt.PartitionNumber, lh.GetLeaseStringData(LeaseDataResumeId, "")))
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
					_ = lh.Release()
					return -1, err
				}

				if qs.qStream.isEof {
					log.Info().Int32("partition-number", prt.PartitionNumber).Msg(semLogContext + " - partition is eof")
					_ = lh.Release()
				} else {
					qs.leaseHandler = lh
					return prtNdx, nil
				}
			} else {
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
				}
			}
		}
	}

}

func (s *Consumer) setMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "consumer::set-metric"

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
