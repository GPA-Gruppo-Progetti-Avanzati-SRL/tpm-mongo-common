package changestream_test

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/mdb"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/listeners"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
)

var yamlConfig = []byte(`
id: my-watcher
lks-name: default
collection-id: watch-collection
out-of-seq-error: true
ref-metrics:
   group-id: "change-stream"
   counter-id: "cdc-events"
   histogram-id: "cdc-event-duration"
change-stream-opts:
   # batch-size: 500
   # full-document: default
   # before-full-document: whenAvailable
   # could result in a blocking call
   # max-await-time: 1m
   # show-expanded-events: true
   # start-at-operation-time: now
   pipeline: >
     [{ "$match": { "operationType": "insert" } }]
`)

func TestChangeStream(t *testing.T) {
	cfg := changestream.Config{}
	err := yaml.Unmarshal(yamlConfig, &cfg)
	require.NoError(t, err)

	var wg sync.WaitGroup

	log.Info().Msg("enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	// svc := file.NewSvc(file.CheckpointSvcConfig{Fn: "resume-token-checkpoint.json", TickInterval: 10})
	svc, err := mdb.NewMongoDbSvc(mdb.CheckpointSvcConfig{
		Instance:     "default",
		CollectionId: CheckpointCollectionId,
		TickInterval: 10,
	})
	require.NoError(t, err)
	w, err := changestream.NewWatcher(&cfg, shutdownChannel, &wg, changestream.WithRetryCount(5), changestream.WithCheckpointSvc(svc))
	require.NoError(t, err)

	_ = w.Add(&listeners.DefaultListener{})
	err = w.Start()
	require.NoError(t, err)

	sig := <-shutdownChannel
	log.Debug().Interface("signal", sig).Msg("got termination signal")
	w.Close()
	wg.Wait()
}
