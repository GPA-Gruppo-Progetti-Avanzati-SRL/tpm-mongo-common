package checkpoint

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/keystring"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/bson"
	"time"
)

type ResumeToken struct {
	Value string `yaml:"token,omitempty"  mapstructure:"token,omitempty"  json:"token,omitempty"`
	At    string `yaml:"at,omitempty"     mapstructure:"at,omitempty"  json:"at,omitempty"`
}

type ResumeTokenInfo struct {
	Version        string `yaml:"version,omitempty"  mapstructure:"version,omitempty"  json:"version,omitempty"`
	Timestamp      string `yaml:"timestamp,omitempty"  mapstructure:"timestamp,omitempty"  json:"timestamp,omitempty"`
	TokenType      string `yaml:"token-type,omitempty"  mapstructure:"token-type,omitempty"  json:"token-type,omitempty"`
	TxnOpIndex     string `yaml:"txn-op-index,omitempty"  mapstructure:"txn-op-index,omitempty"  json:"txn-op-index,omitempty"`
	FromInvalidate string `yaml:"from-invalidate,omitempty"  mapstructure:"from-invalidate,omitempty"  json:"from-invalidate,omitempty"`
	UUID           string `yaml:"uuid,omitempty"  mapstructure:"uuid,omitempty"  json:"uuid,omitempty"`
	DocumentKey    string `yaml:"document-key,omitempty"  mapstructure:"document-key,omitempty"  json:"document-key,omitempty"`
}

func (rt ResumeToken) ShortVersion() string {
	return rt.Value[:18]
}

func (rt ResumeToken) Parse() (ResumeTokenInfo, error) {
	const semLogContext = "resume-token::parse"

	tok := rt.Value

	var info ResumeTokenInfo
	ks, err := keystring.NewKeyStringFromString(keystring.KeyStringVersionV1, tok)
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}

	val, err := ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}
	if ts, ok := val.(bson.Timestamp); ok {
		info.Timestamp = fmt.Sprintf("%v", ts)
	} else {
		info.Timestamp = fmt.Sprintf("%v", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}
	if vs, ok := val.(int64); ok {
		info.Version = fmt.Sprintf("%v", vs)
	} else {
		info.Version = fmt.Sprintf("%v", val)
	}
	version := fmt.Sprintf("%v", val)

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}
	if tt, ok := val.(int64); ok {
		info.TokenType = fmt.Sprintf("%v", tt)
	} else {
		info.TokenType = fmt.Sprintf("%v", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}
	if op, ok := val.(int); ok {
		info.TxnOpIndex = fmt.Sprintf("%v", op)
	} else {
		info.TxnOpIndex = fmt.Sprintf("%v", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}
	if fi, ok := val.(bool); ok {
		info.FromInvalidate = fmt.Sprintf("%v", fi)
	} else {
		info.FromInvalidate = fmt.Sprintf("%v", val)
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}
	if val != nil {
		if bin, ok := val.(bson.Binary); ok {
			info.UUID = fmt.Sprintf("%x", bin.Data)
		} else {
			info.UUID = fmt.Sprintf("%v", val)
		}
	}

	val, err = ks.ToSingleValueBsonPartial()
	if err != nil {
		log.Error().Err(err).Str("resume-token", tok).Msg(semLogContext)
		return info, err
	}

	if val != nil {
		lbl := "eventIdentifier"
		if version != "2" {
			lbl = "documentKey"
		}
		if m, ok := val.(map[string]interface{}); ok {
			key, ok := m[lbl]
			if ok {
				info.DocumentKey = fmt.Sprintf("%v", key)
			} else {
				info.DocumentKey = fmt.Sprintf("%v", val)
			}
		} else {
			info.DocumentKey = fmt.Sprintf("%v", val)
		}
	}

	return info, nil
}

func (rt ResumeToken) TraceLog(semLogContext string, verbose bool) bool {

	if zerolog.GlobalLevel() > zerolog.DebugLevel {
		return false
	}

	info, err := rt.Parse()
	if err != nil {
		log.Error().Err(err).Str("resume-token", rt.Value).Msg(semLogContext)
	} else {
		if verbose {
			log.Trace().Interface("resume-token-info", info).Msg(semLogContext)
		} else {
			log.Trace().Str("resume-token-short", rt.ShortVersion()).Str("txn-op-index", info.TxnOpIndex).Msg(semLogContext)
		}
	}

	return true
}

func (rt ResumeToken) IsZero() bool {
	return rt.Value == ""
}

func (rt ResumeToken) String() string {
	const semLogContext = "resume-token::string"

	b, err := json.Marshal(rt)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return ""
	}
	return string(b)
}

func (rt ResumeToken) ToBsonM() (bson.M, error) {
	if rt.IsZero() {
		return nil, errors.New("token is empty")
	}

	var tok bson.M
	tok = bson.M{"_data": rt.Value}
	return tok, nil
}

func (rt ResumeToken) MustToBsonM() bson.M {
	const semLogContext = "resume-token::must-to-bson-m"
	if rt.IsZero() {
		err := errors.New("token is empty")
		log.Fatal().Err(err).Msg(semLogContext + " - token is empty")
		return nil
	}

	var tok bson.M
	tok = bson.M{"_data": rt.Value}
	return tok
}

func DecodeResumeToken(resumeToken bson.Raw) (ResumeToken, error) {

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
