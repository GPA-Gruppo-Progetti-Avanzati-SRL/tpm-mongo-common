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
	TestDataSize     = 20000
	testDataTemplate = `{ "name": "hello world", "item": %d }`
)

func TestPrepareDataArray2Load(t *testing.T) {
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

func TestPrepareData2Load(t *testing.T) {
	f, err := os.Create("../local-files/change-stream-test-data.json")
	require.NoError(t, err)
	defer f.Close()

	for i := 0; i < TestDataSize; i++ {
		_, err = f.WriteString(fmt.Sprintf(testDataTemplate+"\n", i))
		require.NoError(t, err)
	}
}

var yamlWatcherConfig = []byte(`
id: my-watcher
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
tick-interval: 10
mongo-db-instance: default
mongo-db-collection-id: checkpoint-collection
`)

func TestWatcher(t *testing.T) {
	cfg := changestream.Config{}
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

	var opts []changestream.ConfigOption
	svc, err := factory.NewCheckPointSvc(chkSvcCfg)
	require.NoError(t, err)
	opts = append(opts, changestream.WithCheckpointSvc(svc))

	w, err := changestream.NewWatcher(&cfg, shutdownChannel, &wg, opts...)
	require.NoError(t, err)

	_ = w.Add(&listeners.DefaultListener{})
	err = w.Start()
	require.NoError(t, err)

	sig := <-shutdownChannel
	log.Debug().Interface("signal", sig).Msg("got termination signal")
	w.Close()
	wg.Wait()
}

func TestConsumer(t *testing.T) {
	const semLogContext = "change-stream::consumer"

	cfg := changestream.Config{}
	err := yaml.Unmarshal(yamlWatcherConfig, &cfg)
	require.NoError(t, err)

	chkSvcCfg := factory.Config{}
	err = yaml.Unmarshal(yamlCheckPointSvcConfig, &chkSvcCfg)
	require.NoError(t, err)

	var opts []changestream.ConfigOption
	svc, err := factory.NewCheckPointSvc(chkSvcCfg)
	require.NoError(t, err)
	opts = append(opts, changestream.WithCheckpointSvc(svc))

	c, err := changestream.NewConsumer(&cfg, opts...)
	require.NoError(t, err)

	log.Info().Msg("enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	for {
		select {
		case <-shutdownChannel:
			log.Info().Msg(semLogContext + " terminating")
			c.Close()
			require.NoError(t, err)
			return
		default:
			evt, err := c.Poll()
			require.NoError(t, err)
			if evt != nil {
				t.Log((*evt).String())
			}
		}
	}
}
