package logger

import (
	"context"
	"os"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/task"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/tasklog"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
)

type WorkerLogger struct {
	Logger         *zerolog.Logger
	stdOutLogLevel zerolog.Level
	logBean        tasklog.TaskLog
	StoreReference mongolks.StoreReference
}

func (t *WorkerLogger) Flush() (int, error) {
	const semLogContext = "worker-logger::flush"
	if len(t.logBean.Entries) > 0 {
		err := t.WriteEntry(&t.logBean)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
		}
		t.logBean.Entries = nil
	}
	return 0, nil
}

func (t *WorkerLogger) Write(p []byte) (int, error) {
	const semLogContext = "worker-logger::write"
	text := string(p)

	lev, e, err := tasklog.ParseLogLine(text)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	if lev >= zerolog.InfoLevel {
		t.logBean.Entries = append(t.logBean.Entries, e)
		if len(t.logBean.Entries) == 10 {
			err := t.WriteEntry(&t.logBean)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
			}
			t.logBean.Entries = nil
			t.logBean.BlockNumber++
		}
	}

	return os.Stdout.Write(p)
}

func (t *WorkerLogger) WriteEntry(bean *tasklog.TaskLog) error {
	const semLogContext = "worker-logger::write-entry"
	coll, err := mongolks.GetCollection(context.Background(), t.StoreReference.InstanceName, t.StoreReference.CollectionId)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	_, err = coll.InsertOne(context.Background(), bean)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	return nil
}

func NewWorkerLogger(aTask task.Task, aPartition int32, taskLogsStoreRef mongolks.StoreReference) *WorkerLogger {
	tl := &WorkerLogger{
		StoreReference: taskLogsStoreRef,
		stdOutLogLevel: zerolog.GlobalLevel(),
		logBean: tasklog.TaskLog{
			Domain:      aTask.Domain,
			Site:        aTask.Site,
			Bid:         aTask.Partitions[aPartition].Bid,
			Et:          tasklog.EType,
			TaskId:      aTask.Bid,
			Name:        aTask.Name,
			JobId:       aTask.JobId,
			Partition:   aPartition,
			BlockNumber: 1,
			Entries:     nil,
		},
	}

	// The logger is configured at least from Info even if the global value is Error (example).
	// In this way I receive all the events. Those that are Info and Upper are written to MongoDB whereas
	// I write to std-out only those that match the global level.
	lev := zerolog.GlobalLevel()
	if lev > zerolog.InfoLevel {
		lev = zerolog.InfoLevel
	}
	lg := zerolog.New(zerolog.ConsoleWriter{Out: tl, NoColor: true, TimeFormat: time.RFC3339}).Level(lev).
		With().Str("job-id", aTask.JobId).Str("tsk-id", aTask.Bid).Str("tsk-name", aTask.Name).Timestamp().
		Logger()
	tl.Logger = &lg

	return tl
}
