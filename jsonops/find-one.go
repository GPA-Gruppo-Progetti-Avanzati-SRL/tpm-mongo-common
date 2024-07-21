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
	MongoActivityFindOneOpProperty         = "$op"
	MongoActivityFindOneQueryProperty      = "$query"
	MongoActivityFindOneSortProperty       = "$sort"
	MongoActivityFindOneProjectionProperty = "$projection"
	MongoActivityFindOneOptsProperty       = "$opts"
)

type FindOneOperation struct {
	Query      []byte `yaml:"query,omitempty" json:"query,omitempty" mapstructure:"query,omitempty"`
	Sort       []byte `yaml:"sort,omitempty" json:"sort,omitempty" mapstructure:"sort,omitempty"`
	Projection []byte `yaml:"projection,omitempty" json:"projection,omitempty" mapstructure:"projection,omitempty"`
	Options    []byte `yaml:"options,omitempty" json:"options,omitempty" mapstructure:"options,omitempty"`
}

func (op *FindOneOperation) OpType() string {
	return FindOneOperationType
}

func (op *FindOneOperation) ToString() string {
	var sb strings.Builder
	numberOfElements := 0
	sb.WriteString("{")
	if len(op.Query) > 0 {
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneQueryProperty))
		sb.WriteString(string(op.Query))
	}
	if len(op.Sort) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneSortProperty))
		sb.WriteString(string(op.Sort))
	}
	if len(op.Projection) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneProjectionProperty))
		sb.WriteString(string(op.Projection))
	}
	if len(op.Options) > 0 {
		if numberOfElements > 0 {
			sb.WriteString(",")
		}
		numberOfElements++
		sb.WriteString(fmt.Sprintf("\"%s\": ", MongoActivityFindOneOptsProperty))
		sb.WriteString(string(op.Options))
	}

	sb.WriteString("}")
	return sb.String()
}

func NewFindOneOperation(m map[string][]byte) (*FindOneOperation, error) {
	foStmt, err := NewFindOneStatementConfigFromJson(m[MongoActivityFindOneOpProperty])
	if err != nil {
		return nil, err
	}

	if data, ok := m[MongoActivityFindOneQueryProperty]; ok {
		foStmt.Query = data
	}

	if data, ok := m[MongoActivityFindOneSortProperty]; ok {
		foStmt.Sort = data
	}

	if data, ok := m[MongoActivityFindOneProjectionProperty]; ok {
		foStmt.Projection = data
	}

	if data, ok := m[MongoActivityFindOneOptsProperty]; ok {
		foStmt.Options = data
	}

	return &foStmt, nil
}

func NewFindOneStatementConfigFromJson(data []byte) (FindOneOperation, error) {

	if len(data) == 0 {
		return FindOneOperation{}, nil
	}

	var m map[string]json.RawMessage
	err := json.Unmarshal(data, &m)
	if err != nil {
		return FindOneOperation{}, err
	}

	fo := FindOneOperation{
		Query:      m[MongoActivityFindOneQueryProperty],
		Sort:       m[MongoActivityFindOneSortProperty],
		Projection: m[MongoActivityFindOneProjectionProperty],
		Options:    m[MongoActivityFindOneOptsProperty],
	}

	return fo, nil
}

func FindOne(lks *mongolks.LinkedService, collectionId string, query []byte, projection []byte, opts []byte) (int, []byte, error) {
	const semLogContext = "json-ops::find-one"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	statementQuery, err := util.UnmarshalJson2BsonD(query)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	fo := options.FindOneOptions{}
	prj, err := util.UnmarshalJson2BsonD(projection)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	sc, body, err := executeFindOneOp(c, statementQuery, &fo)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	if sc == http.StatusOK {
		b, err := json.Marshal(body)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return http.StatusInternalServerError, nil, err
		}

		return sc, b, nil
	}

	return sc, nil, nil
}

func executeFindOneOp(c *mongo.Collection, query bson.D, fo *options.FindOneOptions) (int, bson.M, error) {
	const semLogContext = "mongo-operation::execute-find-one-op"

	result := c.FindOne(context.Background(), query, fo)
	if errors.Is(result.Err(), mongo.ErrNoDocuments) {
		return http.StatusNotFound, nil, nil
	}

	if result.Err() != nil {
		log.Error().Err(result.Err()).Msg(semLogContext)
		return http.StatusInternalServerError, nil, result.Err()
	}

	var body bson.M
	err := result.Decode(&body)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	return http.StatusOK, body, nil
}
