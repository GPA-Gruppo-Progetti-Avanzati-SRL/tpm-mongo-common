package jsonops

import (
	"errors"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"net/http"
)

type MongoJsonOperationType string
type MongoJsonOperationStatementPart string

const (
	FindOneOperationType          MongoJsonOperationType = "find-one"
	FindOneAndUpdateOperationType MongoJsonOperationType = "find-one-and-update"
	ReplaceOneOperationType       MongoJsonOperationType = "replace-one"
	AggregateOneOperationType     MongoJsonOperationType = "aggregate-one"
	UpdateOneOperationType        MongoJsonOperationType = "update-one"
	DeleteOneOperationType        MongoJsonOperationType = "delete-one"
	InsertOneOperationType        MongoJsonOperationType = "insert-one"
	UpdateManyOperationType       MongoJsonOperationType = "update-many"
	DeleteManyOperationType       MongoJsonOperationType = "delete-many"
	FindManyOperationType         MongoJsonOperationType = "find"
)

type Operation interface {
	OpType() MongoJsonOperationType
	ToString() string
	Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error)
	NewWriteModel() (mongo.WriteModel, error)
}

func NewOperation(opType MongoJsonOperationType, m map[MongoJsonOperationStatementPart][]byte) (Operation, error) {
	const semLogContext = "json-ops::new-operation"
	var op Operation
	var err error

	switch opType {
	case FindManyOperationType:
		op, err = NewFindOperation(m)
	case FindOneOperationType:
		op, err = NewFindOneOperation(m)
	case FindOneAndUpdateOperationType:
		op, err = NewFindOneAndUpdateOperation(m)
	case ReplaceOneOperationType:
		op, err = NewReplaceOneOperation(m)
	case UpdateOneOperationType:
		op, err = NewUpdateOneOperation(m)
	case AggregateOneOperationType:
		op, err = NewAggregateOneOperation(m)
	case DeleteOneOperationType:
		op, err = NewDeleteOneOperation(m)
	case InsertOneOperationType:
		op, err = NewInsertOneOperation(m)
	case UpdateManyOperationType:
		op, err = NewUpdateManyOperation(m)
	case DeleteManyOperationType:
		op, err = NewDeleteManyOperation(m)
	default:
		err = errors.New("invalid op-type " + string(opType))
	}

	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
	}

	return op, err
}

type OperationResult struct {
	StatusCode    int
	MatchedCount  int64 // The number of documents matched by the filter.
	ModifiedCount int64 // The number of documents modified by the operation.
	UpsertedCount int64 // The number of documents upserted by the operation.
	DeletedCount  int64
	ObjectID      interface{} // The _id field of the upserted document, or nil if no upsert was done.
}

func OperationResultFromUpdateResult(ur *mongo.UpdateResult) OperationResult {
	return OperationResult{
		StatusCode:    http.StatusOK,
		MatchedCount:  ur.MatchedCount,
		ModifiedCount: ur.ModifiedCount,
		UpsertedCount: ur.UpsertedCount,
		ObjectID:      ur.UpsertedID,
	}
}

func OperationResultFromInsertOneResult(ur *mongo.InsertOneResult) OperationResult {
	return OperationResult{
		StatusCode: http.StatusOK,
		ObjectID:   ur.InsertedID,
	}
}

func OperationResultFromDeleteResult(ur *mongo.DeleteResult) OperationResult {
	return OperationResult{
		StatusCode:   http.StatusOK,
		DeletedCount: ur.DeletedCount,
	}
}
