package datasource

import (
	"encoding/json"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
)

const (
	EventTypeDocument     = "document"
	EventTypeError        = "error"
	EventTypeEofPartition = "eof_partition"
	EventTypeEof          = "eof"
)

var NoEvent = Event{Key: bson.NilObjectID}
var ErrEvent = Event{Key: bson.NilObjectID /* IsErr: true,*/, Typ: EventTypeError}
var EofPartition = Event{Key: bson.NilObjectID /*Eof: false, EofPartition: true, EofQueryBatch: true,*/, Typ: EventTypeEofPartition}
var EofEvent = Event{Key: bson.NilObjectID /*Eof: true, EofPartition: true, EofQueryBatch: true,*/, Typ: EventTypeEof}

type Event struct {
	Key       bson.ObjectID     `yaml:"key,omitempty" mapstructure:"key,omitempty" json:"key,omitempty"`
	Partition int32             `yaml:"partition,omitempty" mapstructure:"partition,omitempty" json:"partition,omitempty"`
	Document  bson.M            `yaml:"document,omitempty" mapstructure:"document,omitempty" json:"document,omitempty"`
	Span      opentracing.Span  `yaml:"-" mapstructure:"-" json:"-"`
	Headers   map[string]string `yaml:"-" mapstructure:"-" json:"-"`
	// EofPartition  bool               `yaml:"eof-partition,omitempty" mapstructure:"eof-partition,omitempty" json:"eof-partition,omitempty"`
	// Eof           bool               `yaml:"eof,omitempty" mapstructure:"eof,omitempty" json:"eof,omitempty"`
	// EofQueryBatch bool               `yaml:"eof-batch,omitempty" mapstructure:"eof-batch,omitempty" json:"eof-batch,omitempty"`
	// IsErr         bool               `yaml:"is-err,omitempty" mapstructure:"is-err,omitempty" json:"is-err,omitempty"`
	Typ string `yaml:"type,omitempty" mapstructure:"type,omitempty" json:"type,omitempty"`
}

func NewEvent(m bson.M, isEofBatch bool) Event {
	key := m["_id"].(bson.ObjectID)
	return Event{Key: key, Document: m, Typ: EventTypeDocument /*, EofQueryBatch: isEofBatch*/}
}

func (evt Event) IsZero() bool {
	return evt.Key == bson.NilObjectID && evt.Document == nil && evt.Typ == "" /*!evt.Eof && !evt.EofQueryBatch && !evt.IsErr && !evt.EofPartition*/ && evt.Partition == 0
}

func (evt Event) IsDocument() bool {
	return evt.Key != bson.NilObjectID
}

func (evt Event) IsBoundary() bool {
	b := evt.Key == bson.NilObjectID && evt.Document == nil && evt.Typ != EventTypeDocument
	return b
}

func (evt Event) String() string {
	const semLogContext = "query-event::string"

	b, err := json.Marshal(evt)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return ""
	}

	return string(b)
}

func (e Event) AsExtendedJson(canonical bool) string {
	const semLogContext = "query-event::as-extended-json"

	b, err := bson.MarshalExtJSON(e, canonical, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return ""
	}

	return string(b)
}

func (e Event) DocumentKeyAsString() (string, bool) {
	var docKey string
	docKey = e.Key.Hex()

	return docKey, true
}
