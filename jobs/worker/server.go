package worker

import (
	"errors"
	"github.com/rs/zerolog/log"
	"time"
)

type Server struct {
	cfg                   *ServerConfig
	workers               []*Worker
	numberOfActiveWorkers int
	quitc                 chan error
}

func NewServer(cfg *ServerConfig, tps []*Worker, c chan error) (*Server, error) {
	s := &Server{
		cfg:   cfg,
		quitc: c,
	}

	for _, tp := range tps {
		s.Add(tp)
	}

	s.numberOfActiveWorkers = len(s.workers)
	return s, nil
}

func (s *Server) Close() {
	const semLogContext = "task-worker-srv::close"
	if len(s.workers) > 0 {
		log.Info().Msg(semLogContext + " closing worker")
		for _, tp := range s.workers {
			_ = tp.Close()
		}

		// This is a wait to allow the workers to exit from the loop.
		time.Sleep(500 * time.Millisecond)
	}
}

func (s *Server) Add(tp *Worker) {
	s.workers = append(s.workers, tp)
	tp.Server = s
	s.numberOfActiveWorkers++
}

func (s *Server) Start() error {
	const semLogContext = "task-worker-srv::start"

	var startDelay time.Duration
	if s.cfg.StartDelay > 0 {
		startDelay = time.Millisecond * time.Duration(s.cfg.StartDelay)
	}

	log.Info().Msg(semLogContext)
	for i, tp := range s.workers {
		startDelay = time.Millisecond * time.Duration(s.cfg.StartDelay*(i+1))
		log.Info().Dur("delay", startDelay).Str("processor", tp.Cfg.Name).Msg(semLogContext + " - starting processor...")
		if startDelay > 0 {
			time.Sleep(startDelay)
		}

		tp1 := tp
		err := tp1.Start()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext + " on worker not started.... server shutting down")
			return err
		}
	}

	return nil
}

func (s *Server) WorkerTerminated(err error) {
	const semLogContext = "task-worker-srv::worker-terminated"
	log.Info().Msg(semLogContext)
	if err == nil {
		return
	}

	s.numberOfActiveWorkers--

	if s.numberOfActiveWorkers == 0 {
		log.Info().Msg(semLogContext + " no more workers.... server not operative")
		s.quitc <- errors.New("kafka consumer server not operative")
	} else {
		if s.cfg.OnWorkerTerminated == "exit" {
			log.Error().Err(err).Msg(semLogContext + " on worker terminated.... server shutting down")
			s.quitc <- errors.New("tprod-server shutting down")
		}
	}
}
