package jsonops

import (
	"bytes"
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
	MongoActivityFindOpProperty         MongoJsonOperationStatementPart = "$op"
	MongoActivityFindQueryProperty      MongoJsonOperationStatementPart = "$query"
	MongoActivityFindSortProperty       MongoJsonOperationStatementPart = "$sort"
	MongoActivityFindProjectionProperty MongoJsonOperationStatementPart = "$projection"
	MongoActivityFindOptsProperty       MongoJsonOperationStatementPart = "$opts"
)

type FindOperation struct {
	Query      []byte `yaml:"query,omitempty" json:"query,omitempty" mapstructure:"query,omitempty"`
	Sort       []byte `yaml:"sort,omitempty" json:"sort,omitempty" mapstructure:"sort,omitempty"`
	Projection []byte `yaml:"projection,omitempty" json:"projection,omitempty" mapstructure:"projection,omitempty"`
	Options    []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *FindOperation) OpType() MongoJsonOperationType {
	return FindManyOperationType
}

func (op *FindOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Query) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindQueryProperty))
		sb.WriteString(string(op.Query))
	}
	if len(op.Sort) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindSortProperty))
		sb.WriteString(string(op.Sort))
	}
	if len(op.Projection) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindProjectionProperty))
		sb.WriteString(string(op.Projection))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewFindOperation(m map[MongoJsonOperationStatementPart][]byte) (*FindOperation, error) {
	foStmt, err := NewFindStatementConfigFromJson(m[MongoActivityFindOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityFindQueryProperty]; ok {
		foStmt.Query = data
	}

	if data, ok := m[MongoActivityFindSortProperty]; ok {
		foStmt.Sort = data
	}

	if data, ok := m[MongoActivityFindProjectionProperty]; ok {
		foStmt.Projection = data
	}

	if data, ok := m[MongoActivityFindOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewFindStatementConfigFromJson(data []byte) (FindOperation, error) {

	if len(data) == 0 {
		return FindOperation{}, nil
	}

	var m map[MongoJsonOperationStatementPart]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return FindOperation{}, err
	}

	fo := FindOperation{
		Query:      m[MongoActivityFindQueryProperty],
		Sort:       m[MongoActivityFindSortProperty],
		Projection: m[MongoActivityFindProjectionProperty],
		Options:    m[MongoActivityFindOptsProperty],
	}

	return fo, nil
}

func (op *FindOperation) Execute(lks *mongolks.LinkedService, collectionId string) (OperationResult, []byte, error) {
	sc, resp, err := Find(lks, collectionId, op.Query, op.Projection, op.Sort, op.Options)
	return sc, resp, err
}

func Find(lks *mongolks.LinkedService, collectionId string, query []byte, projection []byte, sort []byte, opts []byte) (OperationResult, []byte, error) {
	const semLogContext = "json-ops::find-one"
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

	fo, err := newFindOptions(opts)
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

	sc, body, err := executeFindOp(c, statementQuery, &fo)
	if err != nil {
		return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	}

	if sc.StatusCode == http.StatusOK {
		//b, err := json.Marshal(body)
		//if err != nil {
		//	log.Error().Err(err).Msg(semLogContext)
		//	return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
		//}
		var bb bytes.Buffer
		bb.WriteString(`{ "to_array": [`)
		for i, b := range body {
			if i > 0 {
				bb.WriteString(",")
			}
			bb.Write(b)
		}
		bb.WriteString(`]}`)

		return sc, bb.Bytes(), nil
	}

	return sc, nil, nil
}

func newFindOptions(opts []byte) (options.FindOptions, error) {
	const semLogContext = "json-ops::new-find-options"
	fo := options.FindOptions{}
	if len(opts) > 0 {
		var m map[string]interface{}
		err := json.Unmarshal(opts, &m)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return fo, err
		}

		if limitOpt, ok := m["limit"]; ok {
			switch limitValue := limitOpt.(type) {
			case float64:
				fo.SetLimit(int64(limitValue))
				log.Trace().Float64("float-limit", limitValue).Msg(semLogContext)
			case int:
				fo.SetLimit(int64(limitValue))
				log.Trace().Int("int-limit", limitValue).Msg(semLogContext)
			default:
				err = errors.New("unrecognized limit value")
				log.Error().Err(err).Str("param-type", fmt.Sprintf("%T", limitOpt)).Msg(semLogContext)
			}
		}
	}

	return fo, nil
}

func executeFindOp(c *mongo.Collection, query bson.D, fo *options.FindOptions) (OperationResult, [][]byte, error) {
	const semLogContext = "mongo-operation::execute-find-op"

	crs, err := c.Find(context.Background(), query, fo)
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

	return OperationResult{StatusCode: http.StatusOK, MatchedCount: int64(len(resp))}, resp, nil
}

func (op *FindOperation) NewWriteModel() (mongo.WriteModel, error) {
	panic("new write model not supported in find operations")
}
