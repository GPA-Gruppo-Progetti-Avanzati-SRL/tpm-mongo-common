package watcherproducer_test

import (
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/factory"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/watcherproducer"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/watcherproducer/listeners"

	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"

	"gopkg.in/yaml.v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
)

var yamlWatcherConfig = []byte(`
id: cdc-tx-mario
lks-name: default
collection-id: watch-collection
out-of-seq-error: true
retry-count: 4
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

var yamlCheckPointSvcConfig = []byte(`
# mongo, file
type: mongo
file-name: resume-token-checkpoint.json
stride: 10
mongo-db-instance: default
mongo-db-collection-id: checkpoint-collection
`)

func TestWatcher(t *testing.T) {
	cfg := watcherproducer.WatcherConfig{}
	err := yaml.Unmarshal(yamlWatcherConfig, &cfg)
	require.NoError(t, err)

	chkSvcCfg := factory.Config{}
	err = yaml.Unmarshal(yamlCheckPointSvcConfig, &chkSvcCfg)
	require.NoError(t, err)

	var wg sync.WaitGroup

	log.Info().Msg("enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	var opts []watcherproducer.WatcherConfigOption
	svc, err := factory.NewCheckPointSvc(chkSvcCfg)
	require.NoError(t, err)
	opts = append(opts, watcherproducer.WithCheckpointSvc(svc))

	w, err := watcherproducer.NewWatcher(&cfg, shutdownChannel, &wg, opts...)
	require.NoError(t, err)

	_ = w.Add(&listeners.DefaultListener{})
	err = w.Start()
	require.NoError(t, err)

	sig := <-shutdownChannel
	log.Debug().Interface("signal", sig).Msg("got termination signal")
	w.Close()
	wg.Wait()
}
