package mongolks

import (
	"context"
	"errors"
	"fmt"

	"github.com/rs/zerolog/log"
	"go.mongodb.org/mongo-driver/mongo"
)

type LinkedServices []*LinkedService

var theRegistry LinkedServices

func Initialize(cfgs []Config) (LinkedServices, error) {

	const semLogContext = "mongo-lks-registry::initialize"
	if len(cfgs) == 0 {
		log.Info().Msg(semLogContext + " no config provided....skipping")
		return nil, nil
	}

	if len(theRegistry) != 0 {
		log.Warn().Msg(semLogContext + " registry already configured.. overwriting")
	}

	log.Info().Int("no-linked-services", len(cfgs)).Msg(semLogContext)

	var r LinkedServices
	for _, kcfg := range cfgs {
		lks, err := NewLinkedServiceWithConfig(kcfg)
		if err != nil {
			return nil, err
		}

		r = append(r, lks)
		log.Info().Str("name", kcfg.Name).Msg(semLogContext + " mongodb instance configured")

	}

	theRegistry = r
	return r, nil
}

func GetLinkedService(ctx context.Context, stgName string) (*LinkedService, error) {
	const semLogContext = "mongo-lks-registry::get-lks"
	for _, stg := range theRegistry {
		if stg.Name() == stgName {
			if !stg.IsConnected() {
				err := stg.Connect(ctx)
				if err != nil {
					return nil, err
				}
			}
			return stg, nil
		}
	}

	err := errors.New("mongo linked service not found by name " + stgName)
	log.Error().Err(err).Str("name", stgName).Msg(semLogContext)
	return nil, err
}

func GetCollection(ctx context.Context, instanceName string, collectionId string) (*mongo.Collection, error) {
	const semLogContext = "mongo-lks-registry::get-collection"
	lks, err := GetLinkedService(ctx, instanceName)
	if err != nil {
		log.Error().Err(err).Str("name", collectionId).Str("instance", instanceName).Msg(semLogContext)
		return nil, err
	}

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = fmt.Errorf("cannot find collection by id %s", collectionId)
		log.Error().Err(err).Str("name", collectionId).Str("instance", instanceName).Msg(semLogContext)
		return c, err
	}

	return c, nil
}

func GetCollectionName(ctx context.Context, instanceName string, collectionId string) (string, error) {
	const semLogContext = "mongo-lks-registry::get-collection-name"
	lks, err := GetLinkedService(ctx, instanceName)
	if err != nil {
		log.Error().Err(err).Str("name", collectionId).Str("instance", instanceName).Msg(semLogContext)
		return "", err
	}

	c := lks.GetCollectionName(collectionId)
	if c == "" {
		err = fmt.Errorf("cannot find collection by id %s", collectionId)
		log.Error().Err(err).Str("name", collectionId).Str("instance", instanceName).Msg(semLogContext)
		return c, err
	}

	return c, nil
}
