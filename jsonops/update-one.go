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
	MongoActivityUpdateOneOpProperty     MongoJsonOperationStatementPart = "$op"
	MongoActivityUpdateOneFilterProperty MongoJsonOperationStatementPart = "$filter"
	MongoActivityUpdateOneUpdateProperty MongoJsonOperationStatementPart = "$update"
	MongoActivityUpdateOneOptsProperty   MongoJsonOperationStatementPart = "$opts"
)

type UpdateOneOperation struct {
	Filter  []byte `yaml:"filter,omitempty" json:"filter,omitempty" mapstructure:"filter,omitempty"`
	Update  []byte `yaml:"update,omitempty" json:"update,omitempty" mapstructure:"update,omitempty"`
	Options []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *UpdateOneOperation) OpType() MongoJsonOperationType {
	return UpdateOneOperationType
}

func (op *UpdateOneOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Filter) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityUpdateOneFilterProperty))
		sb.WriteString(string(op.Filter))
	}
	if len(op.Update) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityUpdateOneUpdateProperty))
		sb.WriteString(string(op.Update))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityUpdateOneOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewUpdateOneOperation(m map[MongoJsonOperationStatementPart][]byte) (*UpdateOneOperation, error) {
	foStmt, err := NewUpdateOneStatementConfigFromJson(m[MongoActivityUpdateOneOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityUpdateOneFilterProperty]; ok {
		foStmt.Filter = data
	}

	if data, ok := m[MongoActivityUpdateOneUpdateProperty]; ok {
		foStmt.Update = data
	}

	if data, ok := m[MongoActivityUpdateOneOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewUpdateOneStatementConfigFromJson(data []byte) (UpdateOneOperation, error) {

	if len(data) == 0 {
		return UpdateOneOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return UpdateOneOperation{}, err
	}

	fo := UpdateOneOperation{
		Filter:  m[MongoActivityUpdateOneFilterProperty],
		Update:  m[MongoActivityUpdateOneUpdateProperty],
		Options: m[MongoActivityUpdateOneOptsProperty],
	}

	return fo, nil
}

func (op *UpdateOneOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := UpdateOne(lks, collectionId, op.Filter, op.Update, op.Options)
	return sc, resp, err
}

func UpdateOne(lks *mongolks.LinkedService, collectionId string, filter []byte, update []byte, opts []byte) (OperationResult, []byte, error) {
	const semLogContext = "json-ops::update-one"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	statementFilter, err := util.UnmarshalJson2BsonD(filter, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	statementUpdate, err := util.UnmarshalJson2Bson(update, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	uo := options.UpdateOptions{}
	if len(opts) > 0 {
		err = json.Unmarshal(opts, &uo)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		}
	}
	res, err := c.UpdateOne(context.Background(), statementFilter, statementUpdate, &uo)
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

	return OperationResultFromUpdateResult(res), b, nil
}

func (op *UpdateOneOperation) NewWriteModel() (mongo.WriteModel, error) {
	const semLogContext = "json-ops::new-update-one-model"

	statementFilter, err := util.UnmarshalJson2BsonD(op.Filter, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	statementUpdate, err := util.UnmarshalJson2Bson(op.Update, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	upsert := false
	uo := options.UpdateOptions{}
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

	return mongo.NewUpdateOneModel().SetFilter(statementFilter).SetUpdate(statementUpdate).SetUpsert(upsert), nil
}
