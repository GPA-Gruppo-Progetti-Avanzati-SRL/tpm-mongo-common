package consumerproducer

import (
	"errors"
	"github.com/rs/zerolog/log"
	"time"
)

type server struct {
	cfg                     *ServerConfig
	producers               []ConsumerProducer
	numberOfActiveProducers int
	quitc                   chan error
}

func NewServer(cfg *ServerConfig, tps []ConsumerProducer, c chan error) (Server, error) {
	s := &server{
		cfg:   cfg,
		quitc: c,
	}

	for _, tp := range tps {
		s.Add(tp)
		tp.SetParent(s)
	}

	s.numberOfActiveProducers = len(s.producers)
	return s, nil
}

func (s *server) Close() {
	const semLogContext = "change-stream-cp-srv::close"
	if len(s.producers) > 0 {
		log.Info().Msg(semLogContext + " closing transformer producer")
		for _, tp := range s.producers {
			_ = tp.Close()
		}

		// This is a wait to allow the producers to exit from the loop.
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *server) Add(tp ConsumerProducer) {
	s.producers = append(s.producers, tp)
	s.numberOfActiveProducers++
}

func (s *server) Start() {
	const semLogContext = "change-stream-cp-srv::start"

	var startDelay time.Duration
	if s.cfg.StartDelay > 0 {
		startDelay = time.Millisecond * time.Duration(s.cfg.StartDelay)
	}

	log.Info().Msg(semLogContext)
	for i, tp := range s.producers {
		startDelay = time.Millisecond * time.Duration(s.cfg.StartDelay*(i+1))
		log.Info().Dur("delay", startDelay).Str("processor", tp.Name()).Msg(semLogContext + " - starting processor...")
		if startDelay > 0 {
			time.Sleep(startDelay)
		}

		tp1 := tp
		go tp1.Start()
	}
}

func (s *server) ConsumerProducerTerminated(err error) {
	const semLogContext = "change-stream-cp-srv::producer-terminated"
	log.Info().Msg(semLogContext)
	if err == nil {
		return
	}

	s.numberOfActiveProducers--

	if s.numberOfActiveProducers == 0 {
		log.Info().Msg(semLogContext + " no more producers.... server not operative")
		s.quitc <- errors.New("kafka consumer server not operative")
	} else {
		if s.cfg.OnWorkerTerminated == "exit" {
			log.Error().Err(err).Msg(semLogContext + " on worker terminated.... server shutting down")
			s.quitc <- errors.New("tprod-server shutting down")
		}
	}
}
