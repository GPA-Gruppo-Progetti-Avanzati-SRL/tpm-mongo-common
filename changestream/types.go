package changestream

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
)

const (
	ResumeTokenTimestampSubStringLength = 18
)

type Watcher interface {
	Close()
	Start() error
	Add(l Listener) error
}

type Listener interface {
	Consume(changeEvent events.ChangeEvent) (bool, error)
}
