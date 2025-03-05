package events

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/checkpoint"
	"go.mongodb.org/mongo-driver/bson"
)

// https://www.mongodb.com/docs/manual/reference/change-events/delete/
// TODO: missing field collectionUUID

//type DeleteEvent struct {
//	changeEventImpl
//	OpType               string              `yaml:"operationType,omitempty" mapstructure:"operationType,omitempty" json:"operationType,omitempty"`
//	Id                   EventId             `yaml:"_id,omitempty" mapstructure:"_id,omitempty" json:"_id,omitempty"`
//	ClusterTime          primitive.Timestamp `yaml:"clusterTime,omitempty" mapstructure:"clusterTime,omitempty" json:"clusterTime,omitempty"`
//	DocumentKey          primitive.M         `yaml:"documentKey,omitempty" mapstructure:"documentKey,omitempty" json:"documentKey,omitempty"`
//	OperationDescription primitive.M         `yaml:"operationDescription,omitempty" mapstructure:"operationDescription,omitempty" json:"operationDescription,omitempty"`
//	Lsid                 primitive.M         `yaml:"lsid,omitempty" mapstructure:"lsid,omitempty" json:"lsid,omitempty"`
//	Ns                   Namespace           `yaml:"ns,omitempty" mapstructure:"ns,omitempty" json:"ns,omitempty"`
//	TxnNumber            int64               `bson:"txnNumber,omitempty" mapstructure:"txnNumber,omitempty" json:"txnNumber,omitempty"`
//	WallTime             primitive.DateTime  `bson:"wall-time,omitempty" mapstructure:"wall-time,omitempty" json:"wall-time,omitempty"`
//}
//
//func (e *DeleteEvent) String() string {
//	const semLogContext = "delete-event::string"
//
//	b, err := json.Marshal(e)
//	if err != nil {
//		log.Error().Err(err).Msg(semLogContext)
//		return ""
//	}
//
//	return string(b)
//}
//
//func (e *DeleteEvent) IsZero() bool {
//	return e.OpType == ""
//}

func parseDeleteOperationType(m bson.M) (ChangeEvent, error) {
	const semLogContext = "delete-event::parse"

	var err error
	e := ChangeEvent{
		OpType: OperationTypeDelete,
	}

	id, err := getDocument(m, "_id", true)
	if err == nil {
		var data string
		data, err = getString(id, "_data", true)
		if err == nil {
			e.Id.Data = data
			e.ResumeTok = checkpoint.ResumeToken{Value: data}
		}
	}

	if err == nil {
		e.ClusterTime, err = getTimestamp(m, "clusterTime", true)
	}

	if err == nil {
		e.DocumentKey, err = getDocument(m, "documentKey", true)
	}

	if err == nil {
		e.Lsid, err = getDocument(m, "lsid", false)
	}

	if err == nil {
		e.OperationDescription, err = getDocument(m, "operationDescription", false)
	}

	if err == nil {
		e.Ns, err = getNamespace(m, true)
	}

	if err == nil {
		e.TxnNumber, err = getNumberLong(m, "txnNumber", false)
	}

	if err == nil {
		e.WallTime, err = getDateTime(m, "wallTime", false)
	}

	return e, err
}
