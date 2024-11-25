package listeners

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog/log"
)

type DefaultListener struct {
}

func (l *DefaultListener) Consume(resumeToken checkpoint.ResumeToken, evt events.ChangeEvent) (bool, error) {
	const semLogContext = "change-stream-default-listener::consume"

	log.Trace().Str("current-token", resumeToken.Value).Str("event", evt.String()).Msg(semLogContext)
	fmt.Println(evt.String())
	return true, nil
}
