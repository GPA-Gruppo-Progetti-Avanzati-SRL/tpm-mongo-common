package util_test

import (
	"fmt"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

const (
	TestDataSize     = 20000
	testDataTemplate = `{ "name": "hello world", "item": %d }`
)

func TestPrepareData2Load(t *testing.T) {
	f, err := os.Create("../local-files/change-stream-test-data.json")
	require.NoError(t, err)
	defer f.Close()

	for i := 0; i < TestDataSize; i++ {
		_, err = f.WriteString(fmt.Sprintf(testDataTemplate+"\n", i))
		require.NoError(t, err)
	}
}
