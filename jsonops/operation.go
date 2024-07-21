package jsonops

import (
	"errors"
	"github.com/rs/zerolog/log"
)

const (
	FindOneOperationType = "find-one"
)

type Operation interface {
	OpType() string
	ToString() string
}

func NewOperation(opType string, m map[string][]byte) (Operation, error) {
	const semLogContext = "json-ops::new-operation"
	var op Operation
	var err error

	switch opType {
	case FindOneOperationType:
		op, err = NewFindOneOperation(m)
	default:
		err = errors.New("invalid op-type " + opType)
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	return op, err
}
