package partition

import (
	"context"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func FindAllPartitionsByTypeAndPartitionGroupAndStatus(coll *mongo.Collection, typ string, partitionGroupId string, status string /*, filterOutEmpty bool, resumeObjectId primitive.ObjectID */) ([]Partition, error) {
	const semLogContext = "partition::find-all-by-type-and-collection"

	f := Filter{}
	f.Or().AndEtEqTo(typ).AndGidEqTo(partitionGroupId).AndStatusEqTo(status)
	opts := options.FindOptions{}

	crs, err := coll.Find(context.Background(), f.Build(), &opts)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	var partitions []Partition
	err = crs.All(context.Background(), &partitions)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	//if filterOutEmpty {
	//	if typ == MongoPartitionType {
	//		filteredPartitions = nil
	//		for _, part := range partitions {
	//			qcoll, err := mongolks.GetCollection(context.Background(), part.Mongo.Instance, part.Mongo.Collection)
	//			if err != nil {
	//				log.Error().Err(err).Msg(semLogContext)
	//				return nil, err
	//			}
	//
	//			f := strings.Replace(part.Mongo.Filter, "{resumeObjectId}", resumeObjectId.Hex(), -1)
	//			bsonObj, err := util.UnmarshalJson2Bson([]byte(f), true)
	//			if err != nil {
	//				log.Error().Err(err).Msg(semLogContext)
	//				return nil, err
	//			}
	//
	//			cnt, err := qcoll.CountDocuments(context.Background(), bsonObj)
	//			if err != nil {
	//				log.Error().Err(err).Msg(semLogContext)
	//				return nil, err
	//			}
	//
	//			log.Info().Int64("num-documents", cnt).Str("applied-filter", f).Msg(semLogContext)
	//			if cnt > 0 {
	//				filteredPartitions = append(filteredPartitions, part)
	//			}
	//		}
	//	} else {
	//		log.Warn().Msg(semLogContext + " filter-out-empty partitions is available only on mongo partitions")
	//	}
	//}

	log.Info().Int("num-partitions", len(partitions)).Str("type", typ).Str("group", partitionGroupId).Msg(semLogContext)
	return partitions, nil
}
