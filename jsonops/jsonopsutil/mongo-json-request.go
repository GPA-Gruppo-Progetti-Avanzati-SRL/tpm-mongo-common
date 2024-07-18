package jsonopsutil

import (
	"github.com/feliixx/mongoextjson"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
)

/*
type MongoJsonRequest struct {
	mapRequest map[string]interface{}
}
*/

type MongoJsonRequest struct {
	Query          bson.M          `json:"query"`
	FindOneOptions *FindOneOptions `json:"find-one-options"`
}

type FindOneOptions struct {
	Projection bson.M `json:"projection"`
}

func ParseMongoJsonRequest(body []byte) (MongoJsonRequest, error) {
	const semLogContext = "mongo-json-request::parse"
	var err error

	r := MongoJsonRequest{}
	err = mongoextjson.Unmarshal(body, &r)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return r, err
	}

	/*	err = mongoextjson.Unmarshal(body, &r)
			// err := bson.Unmarshal(body, &m)
			if err != nil {
				log.Error().Err(err).Msg(semLogContext)
				return r, err
			}

		err = json.Unmarshal(body, &m)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return r, err
		}

		var ok bool
		r.mapRequest, ok = m.(map[string]interface{})
		if !ok {
			err = errors.New("request is not a map")
			log.Error().Err(err).Str("request-type", fmt.Sprintf("%T", m)).Msg(semLogContext)
			return r, err
		}
	*/
	return r, nil
}

func (r MongoJsonRequest) GetQuery() (bson.M, error) {
	if r.Query == nil {
		return make(bson.M), nil
	}

	/*
		var m bson.M
		err := mongoextjson.Unmarshal(r.Query, &m)
	*/
	return r.Query, nil
}

func (r MongoJsonRequest) GetFindOneOptions() (*options.FindOneOptions, error) {
	const semLogContext = "mongo-json-request::parse"
	//var err error

	var fo options.FindOneOptions
	if r.FindOneOptions == nil {
		return &fo, nil
	}

	//if r.FindOneOptions.Sort != nil {
	//	var a bson.D
	//	iter := r.FindOneOptions.Sort.EntriesIter()
	//	for {
	//		pair, ok := iter()
	//		if !ok {
	//			break
	//		}
	//		a = append(a, bson.E{pair.Key, pair.Value})
	//		fmt.Printf("%-12s: %v\n", pair.Key, pair.Value)
	//	}
	//
	//	if len(a) > 0 {
	//		fo.SetSort(a)
	//	}
	//}

	if len(r.FindOneOptions.Projection) > 0 {
		fo.SetProjection(r.FindOneOptions.Projection)
	}

	/*
		var opts FindOneOptions
		err := mongoextjson.Unmarshal(r.FindOneOptions, &opts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return &fo, err
		}
	*/

	/*
		var a bson.D
		err = mongoextjson.Unmarshal(r.FindOneOptions.Sort, &a)
		if err != nil {
			return nil, err
		}

		fo.SetSort(a)
	*/
	return &fo, nil
}

/*
func (r MongoJsonRequest) Get(n string) (bson.M, error) {
	const semLogContext = "mongo-operation::get-statement-component"
	v := util.GetProperty(r.mapRequest, n) // r.mapRequest[n]
	if v != nil {
		switch tv := v.(type) {
		case map[string]interface{}:
			return bson.M(tv), nil
		default:
			err := errors.New("request is not a map")
			log.Error().Err(err).Str("request-type", fmt.Sprintf("%T", r.mapRequest)).Msg(semLogContext)
			return nil, err
		}
	}

	return bson.M{}, nil
}
*/
