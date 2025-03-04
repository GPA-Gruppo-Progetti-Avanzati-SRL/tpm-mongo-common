package querystream

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var ErrEvent = Event{Key: primitive.NilObjectID, IsErr: true}
var EofPartition = Event{Key: primitive.NilObjectID, Eof: false, EofPartition: true, EofQueryBatch: true}
var EofEvent = Event{Key: primitive.NilObjectID, Eof: true, EofPartition: true, EofQueryBatch: true}

type Event struct {
	Key           primitive.ObjectID `yaml:"key,omitempty" mapstructure:"key,omitempty" json:"key,omitempty"`
	Partition     int32              `yaml:"partition,omitempty" mapstructure:"partition,omitempty" json:"partition,omitempty"`
	Document      bson.M             `yaml:"document,omitempty" mapstructure:"document,omitempty" json:"document,omitempty"`
	EofPartition  bool               `yaml:"eof-partition,omitempty" mapstructure:"eof-partition,omitempty" json:"eof-partition,omitempty"`
	Eof           bool               `yaml:"eof,omitempty" mapstructure:"eof,omitempty" json:"eof,omitempty"`
	EofQueryBatch bool               `yaml:"eof-batch,omitempty" mapstructure:"eof-batch,omitempty" json:"eof-batch,omitempty"`
	IsErr         bool               `yaml:"is-err,omitempty" mapstructure:"is-err,omitempty" json:"is-err,omitempty"`
}

func NewEvent(m bson.M, isEofBatch bool) Event {
	key := m["_id"].(primitive.ObjectID)
	return Event{Key: key, Document: m, EofQueryBatch: isEofBatch}
}

func (evt Event) IsZero() bool {
	return evt.Key == primitive.NilObjectID && evt.Document == nil
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
