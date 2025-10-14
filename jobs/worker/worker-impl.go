package worker

import (
	"context"
	"errors"
	"io"
	"math/rand"
	"sync"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/beans"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/lease"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

const (
	LeaseDataResumeId        = "resume_id"
	LeaseDataPartitionStatus = "partition_status"
)

type workerImpl struct {
	task                     task.Task
	taskCollection           *mongo.Collection
	shuffledPartitionIndexes []int
	curPrtNdx                int
	leaseHandler             *lease.Handler

	wg              *sync.WaitGroup
	shutdownSync    sync.Once
	quitc           chan struct{}
	workerId        string
	partitionWorker PartitionWorker
}

func NewWorker(taskColl *mongo.Collection, task task.Task, partitionWorker PartitionWorker, wg *sync.WaitGroup) (Worker, error) {
	w := &workerImpl{wg: wg, partitionWorker: partitionWorker}
	err := w.init(taskColl, task)
	return w, err
}

func (worker *workerImpl) init(taskColl *mongo.Collection, task task.Task) error {
	const semLogContext = "worker-impl::new"
	var err error

	if len(task.Partitions) == 0 {
		err = errors.New("no partitions available in task")
		log.Error().Err(err).Str("task-id", task.Bid).Msg(semLogContext)
		return err
	}

	shuffledPartitionIndexes := make([]int, len(task.Partitions))
	for i := 0; i < len(shuffledPartitionIndexes); i++ {
		shuffledPartitionIndexes[i] = i
	}
	// Shuffle the partitions in order for different query stream starts from a different position
	rand.Shuffle(len(shuffledPartitionIndexes), func(i, j int) {
		shuffledPartitionIndexes[i], shuffledPartitionIndexes[j] = shuffledPartitionIndexes[j], shuffledPartitionIndexes[i]
	})

	worker.task = task
	worker.taskCollection = taskColl
	worker.shuffledPartitionIndexes = shuffledPartitionIndexes
	worker.curPrtNdx = -1
	worker.workerId = util.NewUUID()
	return nil
}

func (w *workerImpl) Start() error {
	const semLogContext = "worker-impl::start"
	log.Info().Msg(semLogContext)
	if w.wg != nil {
		w.wg.Add(1)
	}

	err := w.work()
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}
	return nil
}

func (w *workerImpl) work() error {
	const semLogContext = "worker-impl::do-work"
	log.Info().Msg(semLogContext)

	p, ok, err := w.nextPartition()
	for ok && err == nil {
		err = w.partitionWorker.Work(p)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		err = w.task.UpdatePartitionStatus(w.taskCollection, w.task.Bid, p.PartitionNumber, beans.PartitionStatusEOF, false)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}

		err = w.leaseHandler.Release()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}
		p, ok, err = w.nextPartition()
	}

	if err != nil {
		w.Terminate(err)
		return err
	}

	w.Terminate(nil)
	return nil
}

func (w *workerImpl) Terminate(err error) {
	const semLogContext = "worker-impl::shutdown"

	log.Trace().Msg(semLogContext)
	w.shutdownSync.Do(func() {
		if w.wg != nil {
			w.wg.Done()
		}
	})
}

func (w *workerImpl) Stop() error {
	const semLogContext = "worker-impl::stop"
	log.Info().Msg(semLogContext)
	close(w.quitc)
	return nil
}

func (c *workerImpl) updateLeaseData(status string, withErrors bool) error {
	const semLogContext = "worker-impl::update-lease"
	var err error

	if c.leaseHandler != nil {
		renew := false
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

func (c *workerImpl) close(cts context.Context) error {
	const semLogContext = "worker-impl::close"
	if c.leaseHandler != nil {
		_ = c.leaseHandler.Release()
	}
	return nil
}

func (c *workerImpl) nextPartition() (beans.Partition, bool, error) {
	const semLogContext = "worker-impl::next-partition"
	var err error

	c.curPrtNdx, err = c.acquirePartition()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return beans.Partition{}, false, nil
		}
		return beans.Partition{}, false, err
	}

	return c.task.Partitions[c.shuffledPartitionIndexes[c.curPrtNdx]], true, nil
}

func (c *workerImpl) acquirePartition() (int, error) {
	const semLogContext = "worker-impl::acquire-partition"

	prtNdx := c.curPrtNdx
	for {
		if (prtNdx + 1) >= len(c.shuffledPartitionIndexes) {
			log.Info().Msg(semLogContext + " - partitions exhausted")
			return -1, io.EOF
		}

		prtNdx = prtNdx + 1
		prt := c.task.Partitions[c.shuffledPartitionIndexes[prtNdx]]
		if prt.Status == beans.PartitionStatusAvailable {
			lh, ok, err := lease.AcquireLease(c.taskCollection, c.workerId, beans.PartitionId(c.task.Bid, prt.PartitionNumber), false)
			if ok {
				log.Info().Str("partition-id", beans.PartitionId(c.task.Bid, prt.PartitionNumber)).Msg(semLogContext + " - acquired partition")
				c.leaseHandler = lh
				return prtNdx, nil
			} else {
				if err != nil {
					log.Error().Err(err).Msg(semLogContext)
				}
			}
		}
	}

}
