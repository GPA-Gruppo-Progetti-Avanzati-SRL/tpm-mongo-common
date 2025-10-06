package events_test

import (
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/changestream/events"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/v2/bson"

	"os"
	"testing"
)

var testData = bson.M{"_id": bson.M{"_data": "8267E7317E000000082B042C0100296E5A1004F3ACB4AA4DE147A0B0F5F541701E4310463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006467E7317E7035AAACEE6B5091000004"}, "clusterTime": bson.Timestamp{T: 0x67e7317e, I: 0x8}, "documentKey": bson.M{"_id": bson.ObjectID{0x67, 0xe7, 0x31, 0x7e, 0x70, 0x35, 0xaa, 0xac, 0xee, 0x6b, 0x50, 0x91}}, "fullDocument": bson.M{"_id": bson.ObjectID{0x67, 0xe7, 0x31, 0x7e, 0x70, 0x35, 0xaa, 0xac, 0xee, 0x6b, 0x50, 0x91}, "item": 0, "name": "hello world"}, "ns": bson.M{"coll": "diaries", "db": "test"}, "operationType": "insert", "wallTime": bson.DateTime(1743204734233)}

func TestParse01(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	for i := 1; i < 100000; i++ {
		ce, err := events.ParseEvent(testData)
		require.NoError(t, err)
		_ = ce.String()
	}
}

func TestParse02(t *testing.T) {
	log.Logger = log.Output(zerolog.ConsoleWriter{Out: os.Stderr})
	zerolog.SetGlobalLevel(zerolog.WarnLevel)
	for i := 1; i < 100000; i++ {
		ce, err := events.ParseEvent(testData)
		require.NoError(t, err)
		_ = []byte(ce.AsExtendedJson(true))
	}
}
