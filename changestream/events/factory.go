package events

import (
	"encoding/json"
	"errors"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
)

const (
	OperationTypeInsert  = "insert"
	OperationTypeUpdate  = "update"
	OperationTypeDelete  = "delete"
	OperationTypeReplace = "replace"
)

var UnsupportedOperationType = errors.New("unsupported operation type")

type EventId struct {
	Data string `yaml:"_data,omitempty" mapstructure:"_data,omitempty" json:"_data,omitempty"`
}

type ChangeEvent interface {
	String() string
}

func ParseEvent(m bson.M) (ChangeEvent, error) {
	const semLogContext = "event-factory::parse-event"

	opType, err := getString(m, "operationType", true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var evt ChangeEvent
	switch opType {
	case OperationTypeInsert:
		evt, err = parseInsertOperationType(m)
	case OperationTypeDelete:
		evt, err = parseDeleteOperationType(m)
	case OperationTypeReplace:
		evt, err = parseReplaceOperationType(m)
	case OperationTypeUpdate:
		evt, err = parseUpdateOperationType(m)
	default:
		log.Warn().Str("op-type", opType).Msg(semLogContext + " - unsupported operation type")
		err = UnsupportedOperationType
	}

	if err != nil {
		b, jsonErr := json.Marshal(m)
		if jsonErr != nil {
			log.Error().Err(err).Msg(semLogContext)
		}
		log.Error().Err(err).Str("data", string(b)).Msg(semLogContext)
	}

	return evt, err
}
