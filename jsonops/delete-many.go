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
	MongoActivityDeleteManyOpProperty     MongoJsonOperationStatementPart = "$op"
	MongoActivityDeleteManyFilterProperty MongoJsonOperationStatementPart = "$filter"
	MongoActivityDeleteManyOptsProperty   MongoJsonOperationStatementPart = "$opts"
)

type DeleteManyOperation struct {
	Filter  []byte `yaml:"filter,omitempty" json:"filter,omitempty" mapstructure:"filter,omitempty"`
	Options []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *DeleteManyOperation) OpType() MongoJsonOperationType {
	return DeleteManyOperationType
}

func (op *DeleteManyOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Filter) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityDeleteManyFilterProperty))
		sb.WriteString(string(op.Filter))
	}

	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityDeleteManyOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewDeleteManyOperation(m map[MongoJsonOperationStatementPart][]byte) (*DeleteManyOperation, error) {
	foStmt, err := NewDeleteManyStatementConfigFromJson(m[MongoActivityDeleteManyOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityDeleteManyFilterProperty]; ok {
		foStmt.Filter = data
	}

	if data, ok := m[MongoActivityDeleteManyOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewDeleteManyStatementConfigFromJson(data []byte) (DeleteManyOperation, error) {

	if len(data) == 0 {
		return DeleteManyOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return DeleteManyOperation{}, err
	}

	fo := DeleteManyOperation{
		Filter:  m[MongoActivityDeleteManyFilterProperty],
		Options: m[MongoActivityDeleteManyOptsProperty],
	}

	return fo, nil
}

func (op *DeleteManyOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := DeleteMany(lks, collectionId, op.Filter, op.Options)
	return sc, resp, err
}

func DeleteMany(lks *mongolks.LinkedService, collectionId string, filter []byte, opts []byte) (OperationResult, []byte, error) {
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
	//if opFilter == nil {
	//	opFilter = bson.D{}
	//}

	uo := options.DeleteOptions{}
	if len(opts) > 0 {
		err = json.Unmarshal(opts, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		}
	}
	res, err := c.DeleteMany(context.Background(), opFilter, &uo)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	var b []byte
	b, err = json.Marshal(res)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	return OperationResultFromDeleteResult(res), b, nil
}

func (op *DeleteManyOperation) NewWriteModel() (mongo.WriteModel, error) {
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

	return mongo.NewDeleteManyModel().SetFilter(statementFilter), nil
}
