package keystring_test

import (
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/keystring"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"testing"
)

/*
* 82 - 612E851300000001
* 2B - 02
* 2C - 0100
* 29 -
* 6E -
* 5A - Binary: 1004A5093ABB38FE4B9EA67F01BB1A96D812
* 46 - Object: 3C5F6964003C5F5F5F78000004
 */
var resumeToken = "82612E8513000000012B022C0100296E5A1004A5093ABB38FE4B9EA67F01BB1A96D812463C5F6964003C5F5F5F78000004"
var resumeToken1 = "826701016B0000008F2B042C0100296E5A100414EA8E417E0548D5BEDFA8AFC33F974B463C6F7065726174696F6E54797065003C64656C6574650046646F63756D656E744B65790046645F69640064670100EF3FD4D1958758731D000004"

func TestSingleValueKeyStringToBsonPartial(t *testing.T) {

	ks, err := keystring.NewKeyStringFromString(keystring.KeyStringVersionV1, resumeToken)
	require.NoError(t, err)

	val, err := ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	t.Log("timestamp: ", val)

	val, err = ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	t.Log("version: ", val)
	version := fmt.Sprintf("%v", val)

	val, err = ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	t.Log("tokenType: ", val)

	val, err = ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	t.Log("txnOpIndex: ", val)

	val, err = ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	t.Log("fromInvalidate: ", val)

	val, err = ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	t.Log("uuid: ", fmt.Sprintf("%x", val.(primitive.Binary).Data))

	val, err = ks.ToSingleValueBsonPartial()
	require.NoError(t, err)
	lbl := "eventIdentifier"
	if version != "2" {
		lbl = "documentKey"
	}
	t.Log(lbl, ": ", fmt.Sprintf("%v", val))
}
