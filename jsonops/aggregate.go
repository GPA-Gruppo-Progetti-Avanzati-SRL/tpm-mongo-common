package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
)

const (
	MongoActivityAggregateOneOpProperty       MongoJsonOperationStatementPart = "$op"
	MongoActivityAggregateOnePipelineProperty MongoJsonOperationStatementPart = "$pipeline"
	MongoActivityAggregateOneOptsProperty     MongoJsonOperationStatementPart = "$opts"
)

type AggregateOneOperation struct {
	Filter   []byte `yaml:"filter,omitempty" json:"filter,omitempty" mapstructure:"filter,omitempty"`
	Pipeline []byte `yaml:"pipeline,omitempty" json:"pipeline,omitempty" mapstructure:"pipelineP,omitempty"`
	Options  []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *AggregateOneOperation) OpType() MongoJsonOperationType {
	return AggregateOneOperationType
}

func (op *AggregateOneOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")

	if len(op.Pipeline) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityAggregateOnePipelineProperty))
		sb.WriteString(string(op.Pipeline))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityAggregateOneOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewAggregateOneOperation(m map[MongoJsonOperationStatementPart][]byte) (*AggregateOneOperation, error) {
	foStmt, err := NewAggregateOneStatementConfigFromJson(m[MongoActivityAggregateOneOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityAggregateOnePipelineProperty]; ok {
		foStmt.Pipeline = data
	}

	if data, ok := m[MongoActivityAggregateOneOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewAggregateOneStatementConfigFromJson(data []byte) (AggregateOneOperation, error) {

	if len(data) == 0 {
		return AggregateOneOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return AggregateOneOperation{}, err
	}

	fo := AggregateOneOperation{
		Pipeline: m[MongoActivityAggregateOnePipelineProperty],
		Options:  m[MongoActivityAggregateOneOptsProperty],
	}

	return fo, nil
}

func (op *AggregateOneOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := AggregateOne(lks, collectionId, op.Pipeline, op.Options)
	return sc, resp, err
}

func AggregateOne(lks *mongolks.LinkedService, collectionId string, pipeline []byte, opts []byte) (OperationResult, []byte, error) {
	const semLogContext = "json-ops::aggregate-one"
	sc, items, err := Aggregate(lks, collectionId, pipeline, opts)
	if err != nil {
		return sc, nil, err
	}

	if len(items) == 0 {
		return OperationResult{StatusCode: http.StatusNotFound}, nil, nil
	}

	/*
		b, err := json.Marshal(items[0])
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}
	*/

	return OperationResult{StatusCode: http.StatusOK}, items[0], nil
}

func Aggregate(lks *mongolks.LinkedService, collectionId string, pipeline []byte, opts []byte) (OperationResult, [][]byte, error) {
	const semLogContext = "json-ops::aggregate"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	statementQuery, err := util.UnmarshalJson2ArrayOfBsonD(pipeline, true)
	if err != nil {
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	fo := options.AggregateOptions{}

	sc, resp, err := executeAggregateOp(c, statementQuery, &fo)
	if err != nil {
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	if sc.StatusCode == http.StatusOK {
		//b, err := json.Marshal(body)
		//if err != nil {
		//	log.Error().Err(err).Msg(semLogContext)
		//	return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		//}

		return sc, resp, nil
	}

	return sc, nil, nil
}

func executeAggregateOp(c *mongo.Collection, pipeline interface{}, fo *options.AggregateOptions) (OperationResult, [][]byte, error) {
	const semLogContext = "mongo-operation::execute-aggregate-op"

	crs, err := c.Aggregate(context.Background(), pipeline, fo)
	if err != nil {
		mongoErrorCode := util.MongoErrorCode(err, util.MongoDbVersion{})
		log.Error().Err(err).Int32("mongo-error", mongoErrorCode).Msg(semLogContext)
		return OperationResult{StatusCode: int(-mongoErrorCode)}, nil, err
	}

	var resp [][]byte
	for crs.Next(context.TODO()) {
		var el bson.M
		err = crs.Decode(&el)
		if err = crs.Decode(&el); err != nil {
			return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		}

		var b []byte
		b, err := json.Marshal(el)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		}

		resp = append(resp, b)
	}

	if err = crs.Err(); err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	return OperationResult{StatusCode: http.StatusOK}, resp, nil
}

func (op *AggregateOneOperation) NewWriteModel() (mongo.WriteModel, error) {
	panic("new write model not supported in aggregation queries operations")
}
