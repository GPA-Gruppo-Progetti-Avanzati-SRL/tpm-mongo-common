package taskconsumer

import (
	"context"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util/promutil"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/consumerproducer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/partition"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/taskconsumer/datasource"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
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

type DataSource interface {
	Next() (datasource.Event, error)
	Query(filter datasource.ResumableFilter) error
	IsEOF() bool
}

type Consumer struct {
	cfg                      *Config
	task                     task.Task
	taskCollection           *mongo.Collection
	metrics                  *Metrics
	shuffledPartitionIndexes []int
	curPrtNdx                int
	curPrtIsEof              bool
	curPrtWithFails          int
	dataSource               DataSource // *QueryStream
	leaseHandler             *lease.Handler
	uncommittedEvents        int
	lastCommittedEvent       datasource.Event
	lastPolledEvent          datasource.Event
	numEvents                int
}

func NewConsumer(taskColl *mongo.Collection, task task.Task, cfg *Config, opts ...ConfigOption) (*Consumer, error) {
	const semLogContext = "query-stream::new"
	var err error

	for _, o := range opts {
		o(cfg)
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

	ds, err := datasource.NewMongoDbConnectorForTask(task.Info)
	if err != nil {
		log.Error().Err(err).Str("task-id", task.Bid).Msg(semLogContext)
		return nil, err
	}

	return &Consumer{
		cfg:                      cfg,
		task:                     task,
		taskCollection:           taskColl,
		metrics:                  NewMetrics(cfg.Id, cfg.MetricsGId),
		shuffledPartitionIndexes: shuffledPartitionIndexes,
		curPrtNdx:                -1,
		curPrtIsEof:              true,
		dataSource:               ds, // mdbds.NewQueryStream(qColl, 10),
		leaseHandler:             nil,
		lastPolledEvent:          datasource.NoEvent,
		lastCommittedEvent:       datasource.NoEvent,
	}, nil
}

func (c *Consumer) UpdateLeaseData(evt datasource.Event, status string, withErrors bool) error {
	const semLogContext = "consumer::update-lease"
	var err error

	if c.leaseHandler != nil {
		renew := false
		if evt.Key != primitive.NilObjectID {
			if c.leaseHandler.Lease.Bid == partition.Id(c.task.Bid, evt.Partition) {
				c.lastCommittedEvent = evt
				c.leaseHandler.SetLeaseData(LeaseDataResumeId, evt.Key.Hex())
				c.uncommittedEvents = 0
				renew = true
			} else {
				err = errors.New("lease not acquired by consumer")
				log.Warn().Err(err).Interface("evt", evt).Msg(semLogContext)
				return nil
			}
		}

		if status != "" {
			c.leaseHandler.SetLeaseData(LeaseDataPartitionStatus, status)
			renew = true
		}

		if renew || withErrors {
			err = c.leaseHandler.RenewLease(withErrors)
		}
	} else {
		log.Warn().Msg(semLogContext + " - cannot update not owned lease")
	}

	return err
}

func (c *Consumer) CommitEvent(evt datasource.Event, forceSync bool, withErrors bool) error {
	const semLogContext = "consumer::commit"
	var err error

	if c.curPrtWithFails > 0 {
		log.Info().Err(err).Interface("fails", c.curPrtWithFails).Msg(semLogContext + " - current partition with fails")
		return nil
	}

	if evt.Key != primitive.NilObjectID {
		if c.leaseHandler != nil && c.leaseHandler.Lease.Bid == partition.Id(c.task.Bid, evt.Partition) {
			c.lastCommittedEvent = evt
			c.uncommittedEvents++
			if c.uncommittedEvents == 3 || forceSync {
				err = c.UpdateLeaseData(evt, "", withErrors)
				c.uncommittedEvents = 0
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

func (c *Consumer) Commit(forceSync bool) error {
	return c.CommitEvent(c.lastPolledEvent, forceSync, false)
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

func (c *Consumer) CommitsPending() bool {
	return c.uncommittedEvents > 0
}

func (c *Consumer) Abort() error {
	const semLogContext = "consumer::abort"
	return c.AbortPartial(datasource.NoEvent)
}

func (c *Consumer) AbortPartial(lastCommittableEvent datasource.Event) error {
	const semLogContext = "consumer::abort-partial"
	var err error

	if lastCommittableEvent.IsDocument() {
		err = c.CommitEvent(lastCommittableEvent, true, true)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}
	} else if c.CommitsPending() {
		err = c.CommitEvent(c.lastCommittedEvent, true, true)
	} else {
		err = c.UpdateLeaseData(datasource.NoEvent, "", true)
	}

	c.curPrtWithFails++
	return err
}

func (c *Consumer) Close(cts context.Context) error {
	const semLogContext = "consumer::close"
	if c.leaseHandler != nil {
		_ = c.leaseHandler.Release()
	}
	return nil
}

func (c *Consumer) EOF() bool {
	return false
}

func (c *Consumer) Poll() (datasource.Event, error) {
	const semLogContext = "consumer::poll"

	var evt datasource.Event
	var err error
	if c.curPrtIsEof {
		if c.leaseHandler != nil {
			err = c.leaseHandler.Release()
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return datasource.ErrEvent, err
			}
		}
		ok, err := c.NextPartition()
		if !ok {
			if err == nil {
				return datasource.EofEvent, nil
			} else {
				log.Error().Err(err).Msg(semLogContext)
				return datasource.ErrEvent, err
			}
		}
	}

	evt, err = c.Next()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return evt, err
	}

	if evt.IsDocument() {
		c.lastPolledEvent = evt
		c.metrics.IncNumEvents()
	} else {
		err = c.handleBoundaryEvent(evt)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return evt, err
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
		c.numEvents++
		g = c.setMetric(g, consumerproducer.MetricChangeStreamNumEvents, 1, nil)
	case evt.IsBoundary():
		log.Info().Interface("evt", evt).Msg(semLogContext + " - boundary")

	case evt.Typ == datasource.EventTypeError:
		log.Error().Interface("evt", evt).Msg(semLogContext + " - err")
	default:
		log.Info().Interface("evt", evt).Msg(semLogContext + " - zero?")
	}

	return evt, nil
}

func (c *Consumer) handleBoundaryEvent(evt datasource.Event) error {
	const semLogContext = "consumer::handle-boundary-event"

	log.Info().Interface("evt", evt).Str("lease-bid", c.leaseHandler.Lease.Bid).Str("evt-partition", partition.Id(c.task.Bid, c.lastCommittedEvent.Partition)).Int32("acqs", c.leaseHandler.Lease.Acquisitions).Int32("errs", c.leaseHandler.Lease.Errors).Msg(semLogContext)

	defer func() {
		c.uncommittedEvents = 0
	}()

	var err error
	if evt.Typ == datasource.EventTypeEofPartition {

		var st string
		if c.curPrtWithFails == 0 {
			if c.task.StreamType == task.DataStreamTypeFinite {
				st = partition.StatusEOF
			}

			evt1 := datasource.NoEvent
			if c.uncommittedEvents > 0 && c.lastCommittedEvent.Key != primitive.NilObjectID {
				if c.leaseHandler != nil && c.leaseHandler.Lease.Bid == partition.Id(c.task.Bid, c.lastCommittedEvent.Partition) {
					evt1 = c.lastCommittedEvent
				} else {
					err = errors.New("lease not acquired by consumer")
					log.Error().Err(err).Interface("evt", evt).Str("lease-bid", c.leaseHandler.Lease.Bid).Str("evt-partition", partition.Id(c.task.Bid, c.lastCommittedEvent.Partition)).Msg(semLogContext)
					return err
				}
			}

			if c.uncommittedEvents > 0 || st != "" {
				err = c.UpdateLeaseData(evt1, st, false)
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
					return err
				}
			}
		}

		err = c.task.UpdatePartitionStatus(c.taskCollection, c.task.Bid, evt.Partition, st, c.curPrtWithFails != 0)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

	}

	return err
}

/*
func (s *Consumer) CommitOnEof() error {
	const semLogContext = "consumer::commit"
	var err error

	if s.curPrtWithFails > 0 {
		log.Info().Err(err).Interface("fails", s.curPrtWithFails).Msg(semLogContext + " - current partition with fails")
		return nil
	}

	return err
}
*/

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

func (c *Consumer) Next() (datasource.Event, error) {
	const semLogContext = "consumer::next"
	var err error
	var evt datasource.Event

	if !c.curPrtIsEof {
		evt, err = c.dataSource.Next()
		evt.Partition = c.task.Partitions[c.shuffledPartitionIndexes[c.curPrtNdx]].PartitionNumber
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Info().Interface("evt", evt).Msg(semLogContext + " - reached eof partition")
				c.curPrtIsEof = true
				err = nil
			} else {
				log.Error().Err(err).Msg(semLogContext)
			}
		}

		return evt, nil
	}

	return datasource.ErrEvent, errors.New("partition is already eof-ed")
}

func (c *Consumer) NextPartition() (bool, error) {
	const semLogContext = "consumer::next-partition"
	var err error

	c.curPrtNdx, err = c.acquirePartition()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return false, nil
		}
		return false, err
	}

	c.curPrtIsEof = false
	c.curPrtWithFails = 0

	// evt, err = c.dataSource.Next()
	// evt.Partition = c.task.Partitions[c.shuffledPartitionIndexes[c.curPrtNdx]].PartitionNumber
	return true, nil
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

func (c *Consumer) acquirePartition() (int, error) {
	const semLogContext = "consumer::acquire-partition"

	prtNdx := c.curPrtNdx
	for {
		if (prtNdx + 1) >= len(c.shuffledPartitionIndexes) {
			log.Info().Msg(semLogContext + " - partitions exhausted")
			return -1, io.EOF
		}

		prtNdx = prtNdx + 1
		prt := c.task.Partitions[c.shuffledPartitionIndexes[prtNdx]]
		if prt.Status == partition.StatusAvailable {
			lh, ok, err := lease.AcquireLease(c.taskCollection, c.cfg.Id, partition.Id(c.task.Bid, prt.PartitionNumber), false)
			if ok {
				log.Info().Str("partition-id", partition.Id(c.task.Bid, prt.PartitionNumber)).Msg(semLogContext + " - acquired partition")
				err = c.dataSource.Query(datasource.NewResumableFilter(prt.Info.MdbFilter, prt.PartitionNumber, lh.GetLeaseStringData(LeaseDataResumeId, "")))
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
					_ = lh.Release()
					return -1, err
				}

				c.leaseHandler = lh
				return prtNdx, nil

				//if c.dataSource.IsEOF() {
				//	log.Info().Int32("partition-number", prt.PartitionNumber).Msg(semLogContext + " - partition is eof")
				//	_ = lh.Release()
				//} else {
				//	c.leaseHandler = lh
				//	return prtNdx, nil
				//}
			} else {
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
				}
			}
		}
	}

}

func (c *Consumer) setMetric(metricGroup *promutil.Group, metricId string, value float64, labels map[string]string) *promutil.Group {
	const semLogContext = "consumer::set-metric"

	var err error
	if metricGroup == nil {
		g, err := promutil.GetGroup(c.cfg.MetricsGId)
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
