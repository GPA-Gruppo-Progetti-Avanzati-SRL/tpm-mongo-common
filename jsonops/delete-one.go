package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
	"strings"
)

const (
	MongoActivityDeleteOneOpProperty     MongoJsonOperationStatementPart = "$op"
	MongoActivityDeleteOneFilterProperty MongoJsonOperationStatementPart = "$filter"
	MongoActivityDeleteOneOptsProperty   MongoJsonOperationStatementPart = "$opts"
)

type DeleteOneOperation struct {
	Filter  []byte `yaml:"filter,omitempty" json:"filter,omitempty" mapstructure:"filter,omitempty"`
	Options []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *DeleteOneOperation) OpType() MongoJsonOperationType {
	return DeleteOneOperationType
}

func (op *DeleteOneOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Filter) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityDeleteOneFilterProperty))
		sb.WriteString(string(op.Filter))
	}

	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityDeleteOneOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewDeleteOneOperation(m map[MongoJsonOperationStatementPart][]byte) (*DeleteOneOperation, error) {
	foStmt, err := NewDeleteOneStatementConfigFromJson(m[MongoActivityDeleteOneOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityDeleteOneFilterProperty]; ok {
		foStmt.Filter = data
	}

	if data, ok := m[MongoActivityDeleteOneOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewDeleteOneStatementConfigFromJson(data []byte) (DeleteOneOperation, error) {

	if len(data) == 0 {
		return DeleteOneOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return DeleteOneOperation{}, err
	}

	fo := DeleteOneOperation{
		Filter:  m[MongoActivityDeleteOneFilterProperty],
		Options: m[MongoActivityDeleteOneOptsProperty],
	}

	return fo, nil
}

func (op *DeleteOneOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := DeleteOne(lks, collectionId, op.Filter, op.Options)
	return sc, resp, err
}

func DeleteOne(lks *mongolks.LinkedService, collectionId string, filter []byte, opts []byte) (OperationResult, []byte, error) {
	const semLogContext = "json-ops::delete-one"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	opFilter, err := util.UnmarshalJson2BsonD(filter, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	uo := options.DeleteOptions{}
	if len(opts) > 0 {
		err = json.Unmarshal(opts, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		}
	}
	res, err := c.DeleteOne(context.Background(), opFilter, &uo)
	if err != nil {
		mongoErrorCode := util.MongoErrorCode(err, util.MongoDbVersion{})
		log.Error().Err(err).Int32("mongo-error", mongoErrorCode).Msg(semLogContext)
		return OperationResult{StatusCode: int(-mongoErrorCode)}, nil, err
	}

	var b []byte
	b, err = json.Marshal(res)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	return OperationResultFromDeleteResult(res), b, nil
}

func (op *DeleteOneOperation) NewWriteModel() (mongo.WriteModel, error) {
	const semLogContext = "json-ops::new-delete-one-model"

	statementFilter, err := util.UnmarshalJson2BsonD(op.Filter, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	uo := options.DeleteOptions{}
	if len(op.Options) > 0 {
		err = json.Unmarshal(op.Options, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	}

	return mongo.NewDeleteOneModel().SetFilter(statementFilter), nil
}
