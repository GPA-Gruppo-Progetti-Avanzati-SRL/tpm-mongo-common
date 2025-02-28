package querystream

import (
	"context"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"io"
	"strings"
)

type StreamBatch struct {
	coll      *mongo.Collection
	batchSize int64

	filter   string
	resumeId string

	docs       []bson.M
	currentDoc int
	isEof      bool
}

func NewBatch(coll *mongo.Collection, maxSize int64) *StreamBatch {
	return &StreamBatch{coll: coll, batchSize: maxSize, resumeId: primitive.NilObjectID.Hex()}
}

func (sb *StreamBatch) Query(filter string, resumeId string) error {
	sb.filter = filter
	if resumeId == "" {
		sb.resumeId = primitive.NilObjectID.Hex()
	} else {
		sb.resumeId = resumeId
	}

	sb.isEof = false
	return sb.loadPage()
}

func (sb *StreamBatch) loadPage() error {
	const semLogContext = "document-batch::load"

	filter := strings.Replace(sb.filter, "{resumeObjectId}", sb.resumeId, -1)
	filterBsonObj, err := util.UnmarshalJson2Bson([]byte(filter), true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	findOpts := options.Find()
	findOpts.SetLimit(sb.batchSize)
	findOpts.Sort = bson.D{{"_id", 1}}
	crs, err := sb.coll.Find(context.Background(), filterBsonObj, findOpts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	var docs []bson.M
	err = crs.All(context.Background(), &docs)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return err
	}

	sb.docs = docs
	sb.currentDoc = -1
	if len(docs) > 0 {
		lastDoc := docs[len(docs)-1]
		sb.resumeId = (lastDoc["_id"].(primitive.ObjectID)).Hex()
	} else {
		sb.isEof = true
	}
	return nil
}

func (sb *StreamBatch) Next() (Event, error) {
	const semLogContext = "document-batch::next"
	var err error

	if sb.isEof {
		return ZeroEvent, io.EOF
	}

	sb.currentDoc++
	if sb.currentDoc > len(sb.docs)-1 {
		err = sb.loadPage()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return ZeroEvent, err
		}

		if sb.isEof {
			return ZeroEvent, io.EOF
		}

		sb.currentDoc++
	}

	return NewEvent(sb.docs[sb.currentDoc]), nil
}
