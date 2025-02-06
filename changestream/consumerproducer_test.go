package changestream_test

import (
	_ "embed"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/consumerproducer"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"testing"
)

//go:embed cp-config.yml
var yamlConsumerProducerConfig []byte

func TestConsumerProducer(t *testing.T) {
	const semLogContext = "change-stream::consumer-producer"

	cfg := consumerproducer.Config{}
	err := yaml.Unmarshal(yamlConsumerProducerConfig, &cfg)
	require.NoError(t, err)

	log.Info().Msg("enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	var wg sync.WaitGroup
	cp, err := consumerproducer.NewConsumerProducer(&cfg, &wg, &consumerproducer.EchoConsumerProducer{})
	require.NoError(t, err)

	cps, err := consumerproducer.NewServer(
		&consumerproducer.ServerConfig{
			OnWorkerTerminated: "exit",
			StartDelay:         0,
		},
		[]consumerproducer.ConsumerProducer{cp},
		nil)
	require.NoError(t, err)
	err = cps.Start()
	require.NoError(t, err)

	log.Info().Msg(semLogContext + " wait 4 termination signal")
	sig := <-shutdownChannel
	log.Warn().Interface("signal", sig).Msg(semLogContext + " got termination signal")
	cps.Close()
	wg.Wait()

	log.Info().Msg(semLogContext + " terminated...")
}
