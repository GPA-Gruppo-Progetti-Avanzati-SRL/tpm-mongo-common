package jsonopsutil_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jsonops/jsonopsutil"
	"github.com/stretchr/testify/require"
	"testing"
)

var findSortTest = []byte(`{ "title": 1, "sub-map": { "sub-item": "whatever" } }`)

func TestMap2BsonDAdapter(t *testing.T) {
	srt, err := jsonopsutil.UnmarshalJSONMap2BsonD(findSortTest)
	require.NoError(t, err)

	t.Log(srt)
}
