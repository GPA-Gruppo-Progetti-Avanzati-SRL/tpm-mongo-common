package util

import (
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

type MongoDbVersion struct {
	v string
}

func NewMongoDbVersion(serverVersion interface{}) MongoDbVersion {
	s := fmt.Sprint(serverVersion)
	return MongoDbVersion{v: s}
}

func (mv MongoDbVersion) IsVersion4() bool {
	return strings.HasPrefix(mv.v, "4.")
}

func (mv MongoDbVersion) CommandErrorCode(err mongo.CommandError) int32 {
	code := err.Code
	if code == MongoErrChangeStreamFatalError && mv.IsVersion4() {
		code = MongoErrChangeStreamHistoryLost
	}

	return code
}
