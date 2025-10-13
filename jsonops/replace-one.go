package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util/mdboptions"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

const (
	MongoActivityReplaceOneOpProperty          MongoJsonOperationStatementPart = "$op"
	MongoActivityReplaceOneFilterProperty      MongoJsonOperationStatementPart = "$filter"
	MongoActivityReplaceOneReplacementProperty MongoJsonOperationStatementPart = "$replacement"
	MongoActivityReplaceOneOptsProperty        MongoJsonOperationStatementPart = "$opts"
)

type ReplaceOneOperation struct {
	Filter      []byte `yaml:"filter,omitempty" json:"filter,omitempty" mapstructure:"filter,omitempty"`
	Replacement []byte `yaml:"replacement,omitempty" json:"replacement,omitempty" mapstructure:"replacement,omitempty"`
	Options     []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *ReplaceOneOperation) OpType() MongoJsonOperationType {
	return ReplaceOneOperationType
}

func (op *ReplaceOneOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Filter) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityReplaceOneFilterProperty))
		sb.WriteString(string(op.Filter))
	}
	if len(op.Replacement) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityReplaceOneReplacementProperty))
		sb.WriteString(string(op.Replacement))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityReplaceOneOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewReplaceOneOperation(m map[MongoJsonOperationStatementPart][]byte) (*ReplaceOneOperation, error) {
	foStmt, err := NewReplaceOneStatementConfigFromJson(m[MongoActivityReplaceOneOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityReplaceOneFilterProperty]; ok {
		foStmt.Filter = data
	}

	if data, ok := m[MongoActivityReplaceOneReplacementProperty]; ok {
		foStmt.Replacement = data
	}

	if data, ok := m[MongoActivityReplaceOneOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewReplaceOneStatementConfigFromJson(data []byte) (ReplaceOneOperation, error) {

	if len(data) == 0 {
		return ReplaceOneOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return ReplaceOneOperation{}, err
	}

	fo := ReplaceOneOperation{
		Filter:      m[MongoActivityReplaceOneFilterProperty],
		Replacement: m[MongoActivityReplaceOneReplacementProperty],
		Options:     m[MongoActivityReplaceOneOptsProperty],
	}

	return fo, nil
}

func (op *ReplaceOneOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := ReplaceOne(lks, collectionId, op.Filter, op.Replacement, op.Options)
	return sc, resp, err
}

func ReplaceOne(lks *mongolks.LinkedService, collectionId string, filter []byte, replacement []byte, opts []byte) (OperationResult, []byte, error) {
	const semLogContext = "json-ops::replace-one"
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

	opReplacement, err := util.UnmarshalJson2BsonD(replacement, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	uo, err := mdboptions.ReplaceOptionsFromJson(opts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	res, err := c.ReplaceOne(context.Background(), opFilter, opReplacement, uo)
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

	return OperationResultFromUpdateResult(res), b, nil
}

func (op *ReplaceOneOperation) NewWriteModel() (mongo.WriteModel, error) {
	const semLogContext = "json-ops::new-update-one-model"

	statementFilter, err := util.UnmarshalJson2BsonD(op.Filter, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	statementReplacement, err := util.UnmarshalJson2BsonD(op.Replacement, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	upsert := false
	uo := options.ReplaceOptions{}
	if len(op.Options) > 0 {
		err = json.Unmarshal(op.Options, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}

		if uo.Upsert != nil {
			upsert = *uo.Upsert
		}
	}

	return mongo.NewReplaceOneModel().SetFilter(statementFilter).SetReplacement(statementReplacement).SetUpsert(upsert), nil
}
