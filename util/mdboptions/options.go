package mdboptions

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/util"
	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

func FindOptionsFromJson(opts []byte, sort, projection []byte) (*options.FindOptionsBuilder, error) {
	const semLogContext = "mongo-options::new-find-options"
	fo := options.Find()
	if len(opts) > 0 {
		var m map[string]interface{}
		err := json.Unmarshal(opts, &m)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return fo, err
		}

		if limitOpt, ok := m["limit"]; ok {
			switch limitValue := limitOpt.(type) {
			case float64:
				fo.SetLimit(int64(limitValue))
				log.Trace().Float64("float-limit", limitValue).Msg(semLogContext)
			case int:
				fo.SetLimit(int64(limitValue))
				log.Trace().Int("int-limit", limitValue).Msg(semLogContext)
			default:
				err = errors.New("unrecognized limit value")
				log.Error().Err(err).Str("param-type", fmt.Sprintf("%T", limitOpt)).Msg(semLogContext)
			}
		}
	}

	srt, err := util.UnmarshalJson2BsonD(sort, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	if len(srt) > 0 {
		fo.SetSort(srt)
	}

	prj, err := util.UnmarshalJson2BsonD(projection, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	return fo, nil
}

func FindOneAndUpdateOptionsFromJson(opts []byte, sort, projection []byte) (*options.FindOneAndUpdateOptionsBuilder, bool, error) {
	const semLogContext = "mongo-options::new-find-one-and-update-options"
	var upsert bool

	fo := options.FindOneAndUpdate()
	if len(opts) > 0 {
		var m map[string]interface{}
		err := json.Unmarshal(opts, &m)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return fo, false, err
		}

		if upsert, ok := m["upsert"]; ok {
			if b, ok := upsert.(bool); ok {
				fo.SetUpsert(b)
				upsert = b
			} else {
				err = errors.New("unrecognized upsert flag")
				log.Error().Msg(semLogContext)
			}
		}

		if rd, ok := m["returnDocument"]; ok {
			if s, ok := rd.(string); ok {
				switch s {
				case "before":
					fo.SetReturnDocument(options.Before)
				case "after":
					fo.SetReturnDocument(options.After)
				default:
					err = errors.New("unrecognized returnDocument")
					log.Error().Err(err).Str("returnDocument", s).Msg(semLogContext)
				}
			}
		}
	}

	//fo, err := newFindOneAndUpdateOptions(opts)
	//if err != nil {
	//	log.Error().Err(err).Msg(semLogContext)
	//	return OperationResult{StatusCode: http.StatusInternalServerError}, nil, err
	//}

	srt, err := util.UnmarshalJson2BsonD(sort, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, false, err
	}

	if len(srt) > 0 {
		fo.SetSort(srt)
	}

	prj, err := util.UnmarshalJson2BsonD(projection, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return nil, false, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	return fo, upsert, nil
}

func ReplaceOptionsFromJson(opts []byte) (*options.ReplaceOptionsBuilder, error) {
	const semLogContext = "mongo-options::replace-options-from-json"
	var isUpsert bool
	uo := options.Replace()
	if len(opts) > 0 {
		delOpts := options.ReplaceOptions{}
		err := json.Unmarshal(opts, &delOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
		if delOpts.Upsert != nil {
			isUpsert = *delOpts.Upsert
			uo.SetUpsert(isUpsert)
		}
	}

	return uo, nil
}

func InsertOneOptionsFromJson(opts []byte) (*options.InsertOneOptionsBuilder, error) {
	const semLogContext = "mongo-options::insert-one-options-from-json"
	uo := options.InsertOne()
	if len(opts) > 0 {
		delOpts := options.InsertOneOptions{}
		err := json.Unmarshal(opts, &delOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	}

	return uo, nil
}

func UpdateOneOptionsFromJson(opts []byte) (*options.UpdateOneOptionsBuilder, bool, error) {
	const semLogContext = "mongo-options::update-one-options-from-json"
	uo := options.UpdateOne()

	var isUpsert bool
	if len(opts) > 0 {
		delOpts := options.UpdateOneOptions{}
		err := json.Unmarshal(opts, &delOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, isUpsert, err
		}

		if delOpts.Upsert != nil {
			isUpsert = *delOpts.Upsert
			uo.SetUpsert(isUpsert)
		}
	}

	return uo, isUpsert, nil
}

func UpdateManyOptionsFromJson(opts []byte) (*options.UpdateManyOptionsBuilder, bool, error) {
	const semLogContext = "mongo-options::update-many-options-from-json"
	uo := options.UpdateMany()

	var isUpsert bool
	if len(opts) > 0 {
		delOpts := options.UpdateManyOptions{}
		err := json.Unmarshal(opts, &delOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, isUpsert, err
		}

		if delOpts.Upsert != nil {
			isUpsert = *delOpts.Upsert
			uo.SetUpsert(isUpsert)
		}
	}

	return uo, isUpsert, nil
}

func DeleteManyOptionsFromJson(opts []byte) (*options.DeleteManyOptionsBuilder, error) {
	const semLogContext = "mongo-options::delete-many-options-from-json"
	uo := options.DeleteMany()
	if len(opts) > 0 {
		delOpts := options.DeleteManyOptions{}
		err := json.Unmarshal(opts, &delOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	}

	return uo, nil
}

func DeleteOneOptionsFromJson(opts []byte) (*options.DeleteOneOptionsBuilder, error) {
	const semLogContext = "mongo-options::delete-one-options-from-json"
	uo := options.DeleteOne()
	if len(opts) > 0 {
		delOpts := options.DeleteOneOptions{}
		err := json.Unmarshal(opts, &delOpts)
		if err != nil {
			log.Error().Err(err).Msg(semLogContext)
			return nil, err
		}
	}

	return uo, nil
}
