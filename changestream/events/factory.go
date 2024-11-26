package events

import (
	"encoding/json"
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
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
	ResumeToken() checkpoint.ResumeToken
	String() string
	IsZero() bool
}

type changeEventImpl struct {
	t checkpoint.ResumeToken
}

func (ce *changeEventImpl) ResumeToken() checkpoint.ResumeToken {
	return ce.t
}

func ParseEvent(tok checkpoint.ResumeToken, m bson.M) (ChangeEvent, error) {
	const semLogContext = "event-factory::parse-event"

	/*
		j, err := json.Marshal(m)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
		fmt.Println(string(j))
	*/

	opType, err := getString(m, "operationType", true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var evt ChangeEvent
	switch opType {
	case OperationTypeInsert:
		evt, err = parseInsertOperationType(tok, m)
	case OperationTypeDelete:
		evt, err = parseDeleteOperationType(tok, m)
	case OperationTypeReplace:
		evt, err = parseReplaceOperationType(tok, m)
	case OperationTypeUpdate:
		evt, err = parseUpdateOperationType(tok, m)
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
