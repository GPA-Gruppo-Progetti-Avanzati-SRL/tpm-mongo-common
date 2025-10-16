package duedatetrigger_test

import (
	"sync"
	"testing"
	"time"

	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/driver"
	"github.com/GPA-Gruppo-Progetti-Avanzati-SRL/tpm-mongo-common/jobs/duedatetrigger"
	"github.com/stretchr/testify/require"
)

const jobAmbit = "pipeline-hpyb"

var triggerConfig = duedatetrigger.Config{
	StartDate:    "20250901",
	Mode:         duedatetrigger.ExecuteLoop,
	TickInterval: 2 * time.Hour,
	CheckPointId: "puffo",
	Store: driver.StoreReference{
		InstanceName: "default",
		CollectionId: "jobs-collection",
	},
	Filter: duedatetrigger.Filter{
		JobAmbit:  jobAmbit,
		JobStatus: "waiting",
	},
	Update: duedatetrigger.Update{
		JobStatus: "available",
	},
}

func TestDueDateJobTrigger(t *testing.T) {
	wg := sync.WaitGroup{}
	trg := duedatetrigger.NewJobTrigger(&triggerConfig, &wg)
	require.NotNil(t, trg)

	err := trg.Start()
	require.NoError(t, err)

	t.Log("waiting...")
	wg.Wait()
	t.Log("done")
}
