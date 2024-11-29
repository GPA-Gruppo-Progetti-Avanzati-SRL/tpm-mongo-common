package events

import (
	"encoding/json"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"github.com/opentracing/opentracing-go"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type EventId struct {
	Data string `yaml:"_data,omitempty" mapstructure:"_data,omitempty" json:"_data,omitempty"`
}

type ChangeEvent struct {
	ResumeTok                checkpoint.ResumeToken `yaml:"resumeToken,omitempty" mapstructure:"resumeToken,omitempty" json:"resumeToken,omitempty"`
	Span                     opentracing.Span       `yaml:"-" mapstructure:"-" json:"-"`
	Headers                  map[string]string      `yaml:"-" mapstructure:"-" json:"-"`
	OpType                   string                 `yaml:"operationType,omitempty" mapstructure:"operationType,omitempty" json:"operationType,omitempty"`
	Id                       EventId                `yaml:"_id,omitempty" mapstructure:"_id,omitempty" json:"_id,omitempty"`
	ClusterTime              primitive.Timestamp    `yaml:"clusterTime,omitempty" mapstructure:"clusterTime,omitempty" json:"clusterTime,omitempty"`
	DocumentKey              primitive.M            `yaml:"documentKey,omitempty" mapstructure:"documentKey,omitempty" json:"documentKey,omitempty"`
	OperationDescription     primitive.M            `yaml:"operationDescription,omitempty" mapstructure:"operationDescription,omitempty" json:"operationDescription,omitempty"`
	Lsid                     primitive.M            `yaml:"lsid,omitempty" mapstructure:"lsid,omitempty" json:"lsid,omitempty"`
	Ns                       Namespace              `yaml:"ns,omitempty" mapstructure:"ns,omitempty" json:"ns,omitempty"`
	TxnNumber                int64                  `bson:"txnNumber,omitempty" mapstructure:"txnNumber,omitempty" json:"txnNumber,omitempty"`
	WallTime                 primitive.DateTime     `bson:"wall-time,omitempty" mapstructure:"wall-time,omitempty" json:"wall-time,omitempty"`
	FullDocument             primitive.M            `yaml:"fullDocument,omitempty" mapstructure:"fullDocument,omitempty" json:"fullDocument,omitempty"`
	FullDocumentBeforeChange primitive.M            `yaml:"fullDocumentBeforeChange,omitempty" mapstructure:"fullDocumentBeforeChange,omitempty" json:"fullDocumentBeforeChange,omitempty"`
	UpdateDescription        primitive.M            `yaml:"updateDescription,omitempty" mapstructure:"updateDescription,omitempty" json:"updateDescription,omitempty"`
}

func (e *ChangeEvent) String() string {
	const semLogContext = "change-event::string"

	b, err := json.Marshal(e)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return ""
	}

	return string(b)
}

func (e *ChangeEvent) DocumentKeyAsString() string {
	var docKey string
	if e.DocumentKey != nil {
		if k, ok := e.DocumentKey["_id"]; ok {
			switch tk := k.(type) {
			case primitive.ObjectID:
				docKey = tk.Hex()
			default:
				docKey = fmt.Sprint(tk)
			}
		}
	}

	return docKey
}

func (e *ChangeEvent) ClusterTimeAsString() string {
	tm := time.Unix(int64(e.ClusterTime.T), 0)
	return fmt.Sprintf("%s/%d", tm.Format(time.RFC3339), e.ClusterTime.I)
}
