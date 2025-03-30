package watcherproducer

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
)

const (
	ResumeTokenTimestampSubStringLength = 18
)

type Watcher interface {
	Close()
	Start() error
	Add(l WatcherListener) error
}

type WatcherListener interface {
	ConsumeEvent(changeEvent events.ChangeEvent) (bool, error)
	ConsumeBatch(changeEvents []events.ChangeEvent) (bool, error)
}
