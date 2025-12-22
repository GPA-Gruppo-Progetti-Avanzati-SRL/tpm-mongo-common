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
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
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

func NewWorker(task task.Task, partitionWorker PartitionWorker, opts ...Option) (Worker, error) {
	const semLogContext = "worker-impl::new"
	var err error

	options := Options{}
	for _, opt := range opts {
		opt(&options)
	}

	jobsColl, err := mongolks.GetCollection(context.Background(), options.taskStore.InstanceName, options.taskStore.CollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	w := &workerImpl{
		task:            task,
		taskCollection:  jobsColl,
		curPrtNdx:       -1,
		workerId:        util.NewUUID(),
		wg:              options.wg,
		partitionWorker: partitionWorker}

	if len(task.Partitions) == 0 {
		err = errors.New("no partitions available in task")
		log.Error().Err(err).Str("task-id", task.Bid).Msg(semLogContext)
		return w, err
	}

	shuffledPartitionIndexes := make([]int, len(task.Partitions))
	for i := 0; i < len(shuffledPartitionIndexes); i++ {
		shuffledPartitionIndexes[i] = i
	}
	// Shuffle the partitions in order for different query stream starts from a different position
	rand.Shuffle(len(shuffledPartitionIndexes), func(i, j int) {
		shuffledPartitionIndexes[i], shuffledPartitionIndexes[j] = shuffledPartitionIndexes[j], shuffledPartitionIndexes[i]
	})

	w.shuffledPartitionIndexes = shuffledPartitionIndexes
	return w, nil
}

func (w *workerImpl) Start() error {
	const semLogContext = "worker-impl::start"
	log.Info().Msg(semLogContext)
	if w.wg != nil {
		w.wg.Add(1)
	}

	/*
		err := w.work()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return err
		}
	*/
	go w.work()

	return nil
}

func (w *workerImpl) work() /* error */ {
	const semLogContext = "worker-impl::do-work"
	log.Info().Msg(semLogContext)

	partitionNumber, err := w.nextPartitionNumber()
	for partitionNumber >= 0 && err == nil {
		err = w.partitionWorker.Work(w.task, partitionNumber)
		if err == nil {
			err = w.task.UpdatePartitionStatus(w.taskCollection, int32(partitionNumber), beans.PartitionStatusEOF)
		} else {
			var updErr error
			if PartitionWorkErrorLevel(err) == PartitionWorkErrorRetriable && w.task.RestartableOnError(partitionNumber) {
				updErr = w.task.UpdatePartitionStatusOnError(w.taskCollection, int32(partitionNumber), false)
			} else {
				updErr = w.task.UpdatePartitionStatusOnError(w.taskCollection, int32(partitionNumber), true)
			}
			if updErr != nil {
				log.Error().Err(updErr).Msg(semLogContext)
			}
		}

		errLease := w.leaseHandler.Release()
		if errLease != nil {
			log.Error().Err(err).Msg(semLogContext)
		}

		if err == nil {
			partitionNumber, err = w.nextPartitionNumber()
		} else {
			log.Error().Err(err).Msg(semLogContext)
		}
	}

	if err != nil {
		w.Terminate(err)
		// return err
	}

	w.Terminate(nil)
	// return nil
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

func (c *workerImpl) nextPartitionNumber() (int, error) {
	const semLogContext = "worker-impl::next-partition"
	var err error

	c.curPrtNdx, err = c.acquirePartition()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return -1, nil
		}
		return -1, err
	}

	return c.shuffledPartitionIndexes[c.curPrtNdx] + 1, nil
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
