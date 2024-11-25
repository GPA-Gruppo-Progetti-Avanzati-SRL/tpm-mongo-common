package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"time"
)

type ResumeToken struct {
	Value string `yaml:"token,omitempty"  mapstructure:"token,omitempty"  json:"token,omitempty"`
	At    string `yaml:"at,omitempty"     mapstructure:"at,omitempty"  json:"at,omitempty"`
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
		return ResumeToken{
			Value: s,
			At:    time.Now().Format(time.RFC3339Nano),
		}, nil
	}

	err = fmt.Errorf("invalid resumeToken of type %T", dataId)
	log.Error().Err(err).Msg(semLogContext)
	return ResumeToken{}, err
}
