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

type ResumableFilter struct {
	filter    string
	prtNumber int32
	resumeId  string
}

func NewResumableFilter(filter string, prtNumber int32, resumeId string) ResumableFilter {
	if resumeId == "" {
		resumeId = primitive.NilObjectID.Hex()
	}

	return ResumableFilter{filter, prtNumber, resumeId}
}

func (f ResumableFilter) Filter(resumeId string) string {
	if resumeId == "" {
		resumeId = primitive.NilObjectID.Hex()
	}

	return strings.Replace(f.filter, "{resumeObjectId}", resumeId, -1)
}

func (f ResumableFilter) UpdateResumeId(resumeId string) ResumableFilter {
	if resumeId == "" {
		resumeId = primitive.NilObjectID.String()
	}

	f.resumeId = resumeId
	return f
}

type QueryStream struct {
	coll      *mongo.Collection
	batchSize int64

	filter     ResumableFilter
	docs       []bson.M
	currentDoc int
	isEof      bool
}

func NewQueryStream(coll *mongo.Collection, maxSize int64) *QueryStream {
	return &QueryStream{coll: coll, batchSize: maxSize}
}

func (sb *QueryStream) Query(filter ResumableFilter) error {
	sb.filter = filter
	sb.isEof = false
	return sb.loadPage()
}

func (sb *QueryStream) loadPage() error {
	const semLogContext = "query-stream::load-page"

	filter := sb.filter.Filter(sb.filter.resumeId)
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
		sb.filter = sb.filter.UpdateResumeId((lastDoc["_id"].(primitive.ObjectID)).Hex())
	} else {
		sb.isEof = true
	}
	return nil
}

func (sb *QueryStream) Next() (Event, error) {
	const semLogContext = "query-stream::next"
	var err error

	if sb.isEof {
		return EofPartition, io.EOF
	}

	sb.currentDoc++
	if sb.currentDoc > len(sb.docs)-1 {
		err = sb.loadPage()
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return ErrEvent, err
		}

		if sb.isEof {
			return EofPartition, io.EOF
		}
		sb.currentDoc++
	}

	isEofBatch := false
	if sb.currentDoc == len(sb.docs)-1 {
		isEofBatch = true
	}
	return NewEvent(sb.docs[sb.currentDoc], isEofBatch), nil
}
