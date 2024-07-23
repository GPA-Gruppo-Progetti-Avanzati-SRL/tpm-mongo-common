package jsonops

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
)

type MongoJsonOperationType string
type MongoJsonOperationStatementPart string

const (
	FindOneOperationType      MongoJsonOperationType = "find-one"
	ReplaceOneOperationType   MongoJsonOperationType = "replace-one"
	AggregateOneOperationType MongoJsonOperationType = "aggregate-one"
)

type Operation interface {
	OpType() MongoJsonOperationType
	ToString() string
	Execute(lks *mongolks.LinkedService, collectionId string) (int, []byte, error)
}

func NewOperation(opType MongoJsonOperationType, m map[MongoJsonOperationStatementPart][]byte) (Operation, error) {
	const semLogContext = "json-ops::new-operation"
	var op Operation
	var err error

	switch opType {
	case FindOneOperationType:
		op, err = NewFindOneOperation(m)
	case ReplaceOneOperationType:
		op, err = NewReplaceOneOperation(m)
	case AggregateOneOperationType:
		op, err = NewAggregateOneOperation(m)
	default:
		err = errors.New("invalid op-type " + string(opType))
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	return op, err
}
