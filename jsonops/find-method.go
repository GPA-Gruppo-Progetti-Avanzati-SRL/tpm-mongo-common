package jsonops

/*
func Find(lks *mongolks.LinkedService, collectionId string, query []byte, sort []byte, projection []byte, opts []byte) (int, [][]byte, error) {
	const semLogContext = "json-ops::find"
	var err error

	c := lks.GetCollection(collectionId, "")
	if c == nil {
		err = errors.New("cannot find requested collection")
		log.Error().Err(err).Str("collection", collectionId).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	statementQuery, err := util.UnmarshalJson2BsonD(query, true)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	fo := options.FindOptions{}
	srt, err := util.UnmarshalJson2BsonD(sort, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	if len(srt) > 0 {
		fo.SetSort(srt)
	}

	prj, err := util.UnmarshalJson2BsonD(projection, false)
	if err != nil {
		log.Error().Err(err).Msg(semLogContext)
		return http.StatusInternalServerError, nil, err
	}

	if len(prj) > 0 {
		fo.SetProjection(prj)
	}

	fo.SetLimit(10)

	sc, resp, err := executeFindOp(c, statementQuery, &fo)
	if err != nil {
		return http.StatusInternalServerError, nil, err
	}

	if sc == http.StatusOK {
		//b, err := json.Marshal(body)
		//if err != nil {
		//	log.Error().Err(err).Msg(semLogContext)
		//	return http.StatusInternalServerError, nil, err
		//}

		return sc, resp, nil
	}

	return sc, nil, nil
}
*/
