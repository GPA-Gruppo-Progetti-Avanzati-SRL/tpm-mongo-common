package querystream

import (
	"encoding/json"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

var ZeroEvent = Event{Key: primitive.NilObjectID}

type Event struct {
	Key      primitive.ObjectID `yaml:"key,omitempty" mapstructure:"key,omitempty" json:"key,omitempty"`
	Document bson.M             `yaml:"document,omitempty" mapstructure:"document,omitempty" json:"document,omitempty"`
}

func NewEvent(m bson.M) Event {
	key := m["_id"].(primitive.ObjectID)
	return Event{Key: key, Document: m}
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
