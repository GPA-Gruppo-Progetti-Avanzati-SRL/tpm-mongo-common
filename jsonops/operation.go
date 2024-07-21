package jsonops

import (
	"errors"
	"github.com/rs/zerolog/log"
)

type MongoJsonOperationType string
type MongoJsonOperationStatementPart string

const (
	FindOneOperationType MongoJsonOperationType = "find-one"
)

type Operation interface {
	OpType() MongoJsonOperationType
	ToString() string
}

func NewOperation(opType MongoJsonOperationType, m map[MongoJsonOperationStatementPart][]byte) (Operation, error) {
	const semLogContext = "json-ops::new-operation"
	var op Operation
	var err error

	switch opType {
	case FindOneOperationType:
		op, err = NewFindOneOperation(m)
	default:
		err = errors.New("invalid op-type " + string(opType))
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	return op, err
}
