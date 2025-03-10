package util

import (
	"fmt"
	"strings"
)

type MongoDbVersion struct {
	V string
}

func NewMongoDbVersion(serverVersion interface{}) MongoDbVersion {
	s := fmt.Sprint(serverVersion)
	return MongoDbVersion{V: s}
}

func (mv MongoDbVersion) IsVersion4() bool {
	return strings.HasPrefix(mv.V, "4.")
}

func (mv MongoDbVersion) String() string {
	return mv.V
}
