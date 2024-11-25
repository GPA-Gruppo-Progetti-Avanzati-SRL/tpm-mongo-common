package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/keystring"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type ResumeToken struct {
	Value string `yaml:"token,omitempty"  mapstructure:"token,omitempty"  json:"token,omitempty"`
	At    string `yaml:"at,omitempty"     mapstructure:"at,omitempty"  json:"at,omitempty"`
}

func (t ResumeToken) TraceLog(semLogContext string, verbose bool) bool {

	if zerolog.GlobalLevel() > zerolog.DebugLevel {
		return false
	}

	evt := log.Trace()
	if verbose {
		evt.Str("resume-token", t.Value)
	} else {
		evt.Str("resume-token", t.Value[:18])
	}

	ks, err := keystring.NewKeyStringFromString(keystring.KeyStringVersionV1, t.Value)
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	}

	val, err := ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	} else if verbose {
		evt.Interface("timestamp", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	} else if verbose {
		evt.Interface("version", val)
	}
	version := fmt.Sprintf("%v", val)

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	} else if verbose {
		evt.Interface("token-type", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	} else {
		evt.Interface("txn-op-index", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	} else if verbose {
		evt.Interface("from-invalidate", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	} else if verbose {
		evt.Str("uuid", fmt.Sprintf("%x", val.(primitive.Binary).Data))
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", t.Value).Msg(semLogContext)
		evt.Err(err).Msg(semLogContext)
		return true
	}

	if verbose {
		lbl := "eventIdentifier"
		if version != "2" {
			lbl = "documentKey"
		}
		evt.Str(lbl, fmt.Sprintf("%v", val))
	}

	evt.Msg(semLogContext)
	return true
}

func (t ResumeToken) IsZero() bool {
	return t.Value == ""
}

func (t ResumeToken) String() string {
	const semLogContext = "resume-token::string"

	b, err := json.Marshal(t)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return ""
	}
	return string(b)
}

func (t ResumeToken) ToBsonM() (bson.M, error) {
	if t.IsZero() {
		return nil, errors.New("token is empty")
	}

	var tok bson.M
	tok = bson.M{"_data": t.Value}
	return tok, nil
}

func (t ResumeToken) MustToBsonM() bson.M {
	const semLogContext = "resume-token::must-to-bson-m"
	if t.IsZero() {
		err := errors.New("token is empty")
		log.Fatal().Err(err).Msg(semLogContext + " - token is empty")
		return nil
	}

	var tok bson.M
	tok = bson.M{"_data": t.Value}
	return tok
}

func ParseResumeToken(resumeToken bson.Raw) (ResumeToken, error) {

	const semLogContext = "resume-token::parse-resume-token"
	var data bson.M
	err := bson.Unmarshal(resumeToken, &data)
	if err != nil {
		panic(err)
	}
	dataId := data["_data"]
	/*
		if dataIdMap, ok := dataId.(bson.M); ok {
			fmt.Printf("%T\n", dataIdMap["_data"])
		}
	*/

	if s, ok := dataId.(string); ok {
		token := ResumeToken{
			Value: s,
			At:    time.Now().Format(time.RFC3339Nano),
		}
		token.TraceLog(semLogContext, false)
		return token, nil
	}

	err = fmt.Errorf("invalid resumeToken of type %T", dataId)
	log.Error().Err(err).Msg(semLogContext)
	return ResumeToken{}, err
}
