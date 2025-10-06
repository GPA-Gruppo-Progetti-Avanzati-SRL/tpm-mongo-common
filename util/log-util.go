package util

import (
	"go.mongodb.org/mongo-driver/v2/bson"
)

func MustToExtendedJsonString(document bson.D, canonical, escapeHtml bool) string {
	j, err := ToExtendedJsonString(document, canonical, escapeHtml)
	if err != nil {
		panic(err)
	}

	return j
}

func ToExtendedJsonString(document bson.D, canonical, escapeHtml bool) (string, error) {
	b, err := bson.MarshalExtJSON(document, canonical, escapeHtml)
	if err != nil {
		return "", err
	}

	return string(b), nil
}
