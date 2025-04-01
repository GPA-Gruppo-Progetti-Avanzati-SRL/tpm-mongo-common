package changestream_test

import (
	"context"
	"fmt"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/mongolks"
	"github.com/rs/zerolog/log"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"os/signal"
	"syscall"
	"testing"
	"time"
)

/*
	 {
	  "_id": {
	    "$oid": "67a36015842596c3bbb9ef27"
	  },
	  "_bid": "cdc-movimenti",
	  "at": "2025-02-05T14:24:53.964946+01:00",
	  "resume_token": "8267A366A6000000042B042C0100296E5A1004F3ACB4AA4DE147A0B0F5F541701E4310463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006467A366A55D8CA4F13DDE878D000004",
	  "short_token": "8267A366A600000004",
	  "txn_opn_index": "0"
	}
*/

const (
	ResumeTokenValue     = "8267015CC7000004682B042C0100296E5A1004F3ACB4AA4DE147A0B0F5F541701E4310463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006467015CC64C1EBED2B638951D000004"
	ResumeTokenValueJson = `{"_data": "8267015CC80000045C2B042C0100296E5A1004F3ACB4AA4DE147A0B0F5F541701E4310463C6F7065726174696F6E54797065003C696E736572740046646F63756D656E744B65790046645F6964006467015CC74C1EBED2B6389905000004"}`

	WatchCollectionId = "watch-collection"
)

func TestChangeStreamOld(t *testing.T) {
	const semLogContext = "test-change-stream"
	lks, err := mongolks.GetLinkedService(context.Background(), "default")
	require.NoError(t, err)

	coll := lks.GetCollection(WatchCollectionId, "")

	opt := options.ChangeStreamOptions{
		BatchSize:                nil,
		Collation:                nil,
		Comment:                  nil,
		FullDocument:             nil,
		FullDocumentBeforeChange: nil,
		MaxAwaitTime:             nil,
		ResumeAfter:              nil,
		ShowExpandedEvents:       nil,
		StartAtOperationTime:     nil,
		StartAfter:               nil,
		Custom:                   nil,
		CustomPipeline:           nil,
	}

	/*
		var resumeTokenMap bson.M
		if ResumeTokenValueJson != "" {
			err = json.Unmarshal([]byte(ResumeTokenValueJson), &resumeTokenMap)
			require.NoError(t, err)
			opt.SetResumeAfter(resumeTokenMap)
		} else {
			if ResumeTokenValue != "" {
				var tok bson.M
				tok = bson.M{"_data": ResumeTokenValue}
				require.NoError(t, err)
				if len(tok) != 0 {
					opt.SetResumeAfter(tok)
				}
			} else {
				startTs := primitive.Timestamp{T: uint32(time.Now().Add(-2 * time.Hour).Unix())}
				opt.SetStartAtOperationTime(&startTs)
			}
		}
	*/

	log.Info().Msg(semLogContext + " enabling SIGINT e SIGTERM")
	shutdownChannel := make(chan error)
	go func() {
		c := make(chan os.Signal)
		signal.Notify(c, syscall.SIGINT, syscall.SIGTERM)
		shutdownChannel <- fmt.Errorf("signal received: %v", <-c)
	}()

	collStream, err := coll.Watch(context.TODO(), mongo.Pipeline{}, &opt)
	require.NoError(t, err)
	defer collStream.Close(context.TODO())

	go processChangeStream(collStream)

	sig := <-shutdownChannel
	log.Debug().Interface("signal", sig).Msg(semLogContext + " got termination signal")
	time.Sleep(10 * time.Second)
}

var savedResumeToken bson.Raw
var savedResumeTokenJson string

const batchSize = 1000

func processChangeStream(chgStream *mongo.ChangeStream) {
	const semLogContext = "process-change-stream"

	log.Info().Msg(semLogContext + " - starting")

	numEvents := 0
	var beginOf time.Time
	for chgStream.Next(context.TODO()) {
		var data bson.M

		numEvents++
		if numEvents == 1 {
			beginOf = time.Now()
		}

		if err := chgStream.Decode(&data); err != nil {
			panic(err)
		}

		if numEvents%batchSize == 0 {
			savedResumeToken = chgStream.ResumeToken()
			savedResumeTokenJson = savedResumeToken.String()

			fmt.Println(savedResumeToken.String())
			/*
				var data2 bson.M
				err := bson.Unmarshal(savedResumeToken, &data2)
				if err != nil {
					panic(err)
				}
				dataId := data["_id"]
				if dataIdMap, ok := dataId.(bson.M); ok {
					fmt.Printf("%T\n", dataIdMap["_data"])
				}
			*/
			log.Info().Int("numEvents", numEvents).Interface("data", data).Str("resume-tok", savedResumeToken.String()).Float64("elapsed", time.Since(beginOf).Seconds()).Msg(semLogContext)
		}
	}

	log.Info().Msg(semLogContext + " - ending")
}
