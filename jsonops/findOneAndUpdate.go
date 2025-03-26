package jsonops

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"net/http"
	"strings"
)

const (
	MongoActivityFindOneAndUpdateOpProperty         MongoJsonOperationStatementPart = "$op"
	MongoActivityFindOneAndUpdateQueryProperty      MongoJsonOperationStatementPart = "$query"
	MongoActivityFindOneAndUpdateUpdateProperty     MongoJsonOperationStatementPart = "$update"
	MongoActivityFindOneAndUpdateSortProperty       MongoJsonOperationStatementPart = "$sort"
	MongoActivityFindOneAndUpdateProjectionProperty MongoJsonOperationStatementPart = "$projection"
	MongoActivityFindOneAndUpdateOptsProperty       MongoJsonOperationStatementPart = "$opts"
)

type FindOneAndUpdateOperation struct {
	Query      []byte `yaml:"query,omitempty" json:"query,omitempty" mapstructure:"query,omitempty"`
	Sort       []byte `yaml:"sort,omitempty" json:"sort,omitempty" mapstructure:"sort,omitempty"`
	Projection []byte `yaml:"projection,omitempty" json:"projection,omitempty" mapstructure:"projection,omitempty"`
	Update     []byte `yaml:"update,omitempty" json:"update,omitempty" mapstructure:"update,omitempty"`
	Options    []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *FindOneAndUpdateOperation) OpType() MongoJsonOperationType {
	return FindOneAndUpdateOperationType
}

func (op *FindOneAndUpdateOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Query) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneAndUpdateQueryProperty))
		sb.WriteString(string(op.Query))
	}
	if len(op.Sort) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneAndUpdateSortProperty))
		sb.WriteString(string(op.Sort))
	}
	if len(op.Projection) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneAndUpdateProjectionProperty))
		sb.WriteString(string(op.Projection))
	}
	if len(op.Update) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneAndUpdateUpdateProperty))
		sb.WriteString(string(op.Update))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneAndUpdateOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewFindOneAndUpdateOperation(m map[MongoJsonOperationStatementPart][]byte) (*FindOneAndUpdateOperation, error) {
	foStmt, err := NewFindOneAndUpdateStatementConfigFromJson(m[MongoActivityFindOneAndUpdateOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityFindOneAndUpdateQueryProperty]; ok {
		foStmt.Query = data
	}

	if data, ok := m[MongoActivityFindOneAndUpdateSortProperty]; ok {
		foStmt.Sort = data
	}

	if data, ok := m[MongoActivityFindOneAndUpdateProjectionProperty]; ok {
		foStmt.Projection = data
	}

	if data, ok := m[MongoActivityFindOneAndUpdateOptsProperty]; ok {
		foStmt.Options = data
	}

	if data, ok := m[MongoActivityFindOneAndUpdateUpdateProperty]; ok {
		foStmt.Update = data
	}

	return &foStmt, nil
}

func NewFindOneAndUpdateStatementConfigFromJson(data []byte) (FindOneAndUpdateOperation, error) {

	if len(data) == 0 {
		return FindOneAndUpdateOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return FindOneAndUpdateOperation{}, err
	}

	fo := FindOneAndUpdateOperation{
		Query:      m[MongoActivityFindOneAndUpdateQueryProperty],
		Sort:       m[MongoActivityFindOneAndUpdateSortProperty],
		Projection: m[MongoActivityFindOneAndUpdateProjectionProperty],
		Options:    m[MongoActivityFindOneAndUpdateOptsProperty],
		Update:     m[MongoActivityFindOneAndUpdateUpdateProperty],
	}

	return fo, nil
}

func (op *FindOneAndUpdateOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := FindOneAndUpdate(lks, collectionId, op.Query, op.Projection, op.Sort, op.Update, op.Options)
	return sc, resp, err
}

func newFindOneAndUpdateOptions(opts []byte) (options.FindOneAndUpdateOptions, error) {
	const semLogContext = "json-ops::new-find-one-and-update-options"
	fo := options.FindOneAndUpdateOptions{}
	if len(opts) > 0 {
		var m map[string]interface{}
		err := json.Unmarshal(opts, &m)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return fo, err
		}

		if upsert, ok := m["upsert"]; ok {
			if b, ok := upsert.(bool); ok {
				fo.SetUpsert(b)
			} else {
				err = errors.New("unrecognized upsert flag")
				log.Error().Msg(semLogContext)
			}
		}

		if rd, ok := m["returnDocument"]; ok {
			if s, ok := rd.(string); ok {
				switch s {
				case "before":
					fo.SetReturnDocument(options.Before)
				case "after":
					fo.SetReturnDocument(options.After)
				default:
					err = errors.New("unrecognized returnDocument")
					log.Error().Err(err).Str("returnDocument", s).Msg(semLogContext)
				}
			}
		}
	}

	return fo, nil
}

func FindOneAndUpdate(lks *mongolks.LinkedService, collectionId string, query []byte, projection []byte, sort []byte, update []byte, opts []byte) (OperationResult, []byte, error) {
	const semLogContext = "json-ops::find-one-and-update"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	statementQuery, err := util.UnmarshalJson2BsonD(query, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	statementUpdate, err := util.UnmarshalJson2Bson(update, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	fo, err := newFindOneAndUpdateOptions(opts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	srt, err := util.UnmarshalJson2BsonD(sort, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	if len(srt) > 0 {
		fo.SetSort(srt)
	}

	prj, err := util.UnmarshalJson2BsonD(projection, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	sc, body, err := executeFindOneAndUpdateOp(c, statementQuery, statementUpdate, &fo)
	if err != nil {
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	if sc.StatusCode == http.StatusOK {
		b, err := json.Marshal(body)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		}

		return sc, b, nil
	}

	return sc, nil, nil
}

func executeFindOneAndUpdateOp(c *mongo.Collection, query bson.D, update any, fo *options.FindOneAndUpdateOptions) (OperationResult, bson.M, error) {
	const semLogContext = "mongo-operation::execute-find-one-and-update-op"

	result := c.FindOneAndUpdate(context.Background(), query, update, fo)
	if errors.Is(result.Err(), mongo.ErrNoDocuments) {
		if fo.Upsert != nil && *fo.Upsert {
			return OperationResult{StatusCode: http.StatusNoContent}, nil, nil
		}
		return OperationResult{StatusCode: http.StatusNotFound}, nil, nil
	}

	if result.Err() != nil {
		err := result.Err()
		mongoErrorCode := util.MongoErrorCode(err, util.MongoDbVersion{})
		log.Error().Err(err).Int32("mongo-error", mongoErrorCode).Msg(semLogContext)
		return OperationResult{StatusCode: int(-mongoErrorCode)}, nil, err
	}

	var body bson.M
	err := result.Decode(&body)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	return OperationResult{StatusCode: http.StatusOK}, body, nil
}

func (op *FindOneAndUpdateOperation) NewWriteModel() (mongo.WriteModel, error) {
	panic("new write model not supported in find operations")
}
