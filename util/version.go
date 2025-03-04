package util

import (
	"fmt"
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
