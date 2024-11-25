package changestream_test

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/factory"
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

const (
	TestDataSize     = 1000000
	testDataTemplate = `{ "name": "hello world", "item": %d }`
)

func TestPrepareData2Load(t *testing.T) {
	f, err := os.Create("../local-files/change-stream-test-data.json")
	require.NoError(t, err)
	defer f.Close()

	_, err = f.WriteString("[")
	for i := 0; i < TestDataSize; i++ {
		if i > 0 {
			_, err = f.WriteString(",")
		}
		_, err = f.WriteString(fmt.Sprintf(testDataTemplate, i))
		require.NoError(t, err)
	}
	_, err = f.WriteString("]")
}

var yamlWatcherConfig = []byte(`
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

var yamlCheckpointConfig = []byte(`
# mongo, file
type: mongo
file-name: resume-token-checkpoint.json
tick-interval: 10
mongo-db-instance: default
mongo-db-collection-id: checkpoint-collection
`)

func TestChangeStream(t *testing.T) {
	cfg := changestream.Config{}
	err := yaml.Unmarshal(yamlWatcherConfig, &cfg)
	require.NoError(t, err)

	chkCfg := factory.Config{}
	err = yaml.Unmarshal(yamlCheckpointConfig, &chkCfg)
	require.NoError(t, err)

	var wg sync.WaitGroup

	log.Info().Msg("enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	svc, err := factory.NewCheckPointSvc(chkCfg)
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
