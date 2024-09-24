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
	MongoActivityInsertOneOpProperty       MongoJsonOperationStatementPart = "$op"
	MongoActivityInsertOneDocumentProperty MongoJsonOperationStatementPart = "$document"
	MongoActivityInsertOneOptsProperty     MongoJsonOperationStatementPart = "$opts"
)

type InsertOneOperation struct {
	Document []byte `yaml:"document,omitempty" json:"document,omitempty" mapstructure:"document,omitempty"`
	Options  []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *InsertOneOperation) OpType() MongoJsonOperationType {
	return InsertOneOperationType
}

func (op *InsertOneOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")

	if len(op.Document) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityInsertOneDocumentProperty))
		sb.WriteString(string(op.Document))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityInsertOneOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewInsertOneOperation(m map[MongoJsonOperationStatementPart][]byte) (*InsertOneOperation, error) {
	foStmt, err := NewInsertOneStatementConfigFromJson(m[MongoActivityInsertOneOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityInsertOneDocumentProperty]; ok {
		foStmt.Document = data
	}

	if data, ok := m[MongoActivityInsertOneOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewInsertOneStatementConfigFromJson(data []byte) (InsertOneOperation, error) {

	if len(data) == 0 {
		return InsertOneOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return InsertOneOperation{}, err
	}

	fo := InsertOneOperation{
		Document: m[MongoActivityInsertOneDocumentProperty],
		Options:  m[MongoActivityInsertOneOptsProperty],
	}

	return fo, nil
}

func (op *InsertOneOperation) Execute(lks *mongolks.LinkedService, collectionId string) (int, []byte, error) {
	sc, resp, err := InsertOne(lks, collectionId, op.Document, op.Options)
	return sc, resp, err
}

func InsertOne(lks *mongolks.LinkedService, collectionId string, document []byte, opts []byte) (int, []byte, error) {
	const semLogContext = "json-ops::insert-one"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	opDocument, err := util.UnmarshalJson2BsonD(document)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	uo := options.InsertOneOptions{}
	if len(opts) > 0 {
		err = json.Unmarshal(opts, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}
	}
	res, err := c.InsertOne(context.Background(), opDocument, &uo)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	var b []byte
	b, err = json.Marshal(res)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, b, nil
}

func (op *InsertOneOperation) NewWriteModel() (mongo.WriteModel, error) {
	const semLogContext = "json-ops::new-insert-one-model"

	statementDocument, err := util.UnmarshalJson2BsonD(op.Document)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	uo := options.InsertOneOptions{}
	if len(op.Options) > 0 {
		err = json.Unmarshal(op.Options, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	}

	return mongo.NewInsertOneModel().SetDocument(statementDocument), nil
}
