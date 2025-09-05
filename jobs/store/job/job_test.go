package job_test

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/store/job"
	"github.com/stretchr/testify/require"
	"go.mongodb.org/mongo-driver/bson"
)

func TestJobFilter(t *testing.T) {
	f := job.Filter{}
	f.Or().
		AndEtEqTo(job.EType).
		AndStatusEqTo("waiting").
		AndAmbitEqTo("ja").
		AndDueDateBetween("20250901", "20250905")

	document := f.Build()
	b, err := json.Marshal(document)
	require.NoError(t, err)
	fmt.Println(string(b))

	b, err = bson.MarshalExtJSON(document, false, false)
	require.NoError(t, err)
	fmt.Println(string(b))

}
