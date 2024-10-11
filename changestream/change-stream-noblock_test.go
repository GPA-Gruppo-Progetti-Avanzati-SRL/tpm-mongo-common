package changestream_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
)

func TestNonBlockingChangeStream(t *testing.T) {
	const semLogContext = "test-nonblocking-change-stream"
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	coll := lks.GetCollection(CollectionId, "")

	opt := options.ChangeStreamOptions{
		BatchSize:                nil,
		Collation:                nil,
		Comment:                  nil,
		FullDocument:             nil,
		FullDocumentBeforeChange: nil,
		MaxAwaitTime:             nil,
		ResumeAfter:              nil,
		ShowExpandedEvents:       nil,
		StartAtOperationTime:     nil,
		StartAfter:               nil,
		Custom:                   nil,
		CustomPipeline:           nil,
	}

	var resumeTokenMap bson.M
	if ResumeTokenValueJson != "" {
		err = json.Unmarshal([]byte(ResumeTokenValueJson), &resumeTokenMap)
		require.NoError(t, err)
		opt.SetResumeAfter(resumeTokenMap)
	} else {
		if ResumeTokenValue != "" {
			var tok bson.M
			tok = bson.M{"_data": ResumeTokenValue}
			require.NoError(t, err)
			if len(tok) != 0 {
				opt.SetResumeAfter(tok)
			}
		} else {
			startTs := primitive.Timestamp{T: uint32(time.Now().Add(-2 * time.Hour).Unix())}
			opt.SetStartAtOperationTime(&startTs)
		}
	}

	log.Info().Msg(semLogContext + " enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	collStream, err := coll.Watch(context.TODO(), mongo.Pipeline{}, &opt)
	require.NoError(t, err)

	var wg sync.WaitGroup
	go processNonBlockingChangeStream(collStream, &wg)

	sig := <-shutdownChannel
	log.Debug().Interface("signal", sig).Msg(semLogContext + " got termination signal")
	closeChangeStream = true
	wg.Wait()
	collStream.Close(context.TODO())
	log.Debug().Interface("signal", sig).Msg(semLogContext + " terminating")
	time.Sleep(2 * time.Second)
}

var closeChangeStream bool

func processNonBlockingChangeStream(chgStream *mongo.ChangeStream, wg *sync.WaitGroup) {
	const semLogContext = "process-non-blocking-change-stream"

	log.Info().Msg(semLogContext + " - starting")
	wg.Add(1)
	defer wg.Done()

	numEvents := 0
	var beginOf time.Time

	warmUpTryNextTimeout := 60 * time.Second
	tryNextTimeout := 10 * time.Second
	evt, err := poll(chgStream, warmUpTryNextTimeout)
	if closeChangeStream {
		return
	}

	for err == nil {

		if evt != nil {
			var data bson.M

			numEvents++
			if numEvents == 1 {
				beginOf = time.Now()
			}

			err = bson.Unmarshal(evt, &data)
			if err != nil {
				return
			}

			if numEvents%batchSize == 0 {
				savedResumeToken = chgStream.ResumeToken()
				savedResumeTokenJson = savedResumeToken.String()

				fmt.Println(savedResumeToken.String())
				log.Info().Int("numEvents", numEvents).Interface("data", data).Str("resume-tok", savedResumeToken.String()).Float64("elapsed", time.Since(beginOf).Seconds()).Msg(semLogContext)
			}
		} else {
			tryNextTimeout = tryNextTimeout + 10*time.Second
			log.Error().Err(err).Dur("try-next", tryNextTimeout).Msg(semLogContext)
		}

		evt, err = poll(chgStream, tryNextTimeout)
		if closeChangeStream {
			return
		}
	}

	log.Error().Err(err).Int("num-events", numEvents).Msg(semLogContext + " - end")
	log.Info().Msg(semLogContext + " - ending")
}

func poll(chgStream *mongo.ChangeStream, tmout time.Duration) (bson.Raw, error) {
	const semLogContext = "change-stream::poll"

	/*
		ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(tmout))
		defer cancel()
	*/
	if chgStream.TryNext(context.Background()) {
		return chgStream.Current, nil
	}
	err := chgStream.Err()

	actualErr := err
	if err != nil {
		msg := ""
		if errors.Is(err, context.DeadlineExceeded) {
			msg = " - context deadline exceeded"
			time.Sleep(10 * time.Second)
			actualErr = nil
		} else if errors.Is(err, context.Canceled) {
			msg = " - context canceled"
		} else {
			var cmdErr mongo.CommandError
			if errors.As(err, &cmdErr) {
				msg = fmt.Sprintf("mongo command error %v", cmdErr)
			}
		}
		log.Error().Err(err).Msg(semLogContext + msg)
	} else {
		log.Info().Msg(semLogContext + " - No Data")
	}

	return nil, actualErr
}
