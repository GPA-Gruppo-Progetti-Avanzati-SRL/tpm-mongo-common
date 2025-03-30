package changestream_test

import (
	"context"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint/factory"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/listeners"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"gopkg.in/yaml.v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
	"time"
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
				err = c.Commit()
				require.NoError(t, err)
			} else {

			}
		}
	}
}

func TestConsumerTryNext(t *testing.T) {
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

	var mu sync.Mutex
	ticker := time.NewTicker(500 * time.Millisecond)
	numEvts := 0
	begin := time.Now()
	for {
		select {
		case <-ticker.C:
			//mu.Lock()
			//fmt.Printf("numEvts: %d\n", numEvts)
			//numEvts = 0
			//mu.Unlock()
		case <-shutdownChannel:
			log.Info().Msg(semLogContext + " terminating")
			c.Close()
			require.NoError(t, err)
			return
		default:
			evt, err := c.Poll()
			require.NoError(t, err)
			if evt != nil {
				mu.Lock()
				numEvts++
				if numEvts%20000 == 0 {
					fmt.Printf("elapsed: %d\n", time.Since(begin).Milliseconds())
				}
				mu.Unlock()
			}
		}
	}
}

func TestConsumerNext(t *testing.T) {
	const semLogContext = "change-stream::consumer"

	opts := options.ChangeStreamOptions{
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

	var tok bson.M
	tok = bson.M{"_data": "8267E73748000000222B042C01002C03E66E5A1004F3ACB4AA4DE147A0B0F5F541701E4310463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006467E7374793A5A7D34207F732000004"}
	opts.SetResumeAfter(tok)

	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	coll := lks.GetCollection(WatchCollectionId, "")
	if coll == nil {
		err = errors.New("collection not found in config: " + WatchCollectionId)
		log.Fatal().Err(err).Msg(semLogContext)
	}

	collStream, err := coll.Watch(context.TODO(), mongo.Pipeline{}, &opts)
	require.NoError(t, err)

	log.Info().Msg("enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	var mu sync.Mutex
	ticker := time.NewTicker(500 * time.Millisecond)
	numEvts := 0
	go func() {
		for {
			select {
			case <-ticker.C:
				mu.Lock()
				//fmt.Printf("numEvts: %d\n", numEvts)
				//numEvts = 0
				mu.Unlock()
			}
		}
	}()

	begin := time.Now()
	for collStream.Next(context.TODO()) {
		var event map[string]interface{}
		if errDecode := collStream.Decode(&event); errDecode != nil {
			log.Printf("error decoding: %s", errDecode)
		}
		//fmt.Println(numEvts)
		mu.Lock()
		numEvts++
		if numEvts%20000 == 0 {
			fmt.Printf("evts: %d - elapsed: %d\n", numEvts, time.Since(begin).Milliseconds())
		}
		mu.Unlock()
	}
}
