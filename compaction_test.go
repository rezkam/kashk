package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestSuccessfulCompaction(t *testing.T) {
	// Setup test environment
	tempDir, err := os.MkdirTemp("", "successful_compaction_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) // clean up

	// Initialize Engine with a very small max file size
	verySmallMaxLogSize := int64(256) // 256 bytes
	engine, err := NewEngine(tempDir, WithMaxLogSize(verySmallMaxLogSize))
	require.NoError(t, err)

	// Mock Data: Populate the engine with more test data
	for i := 0; i < 50; i++ { // Increased number of keys
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := engine.Put(key, value)
		require.NoError(t, err)
	}

	// Update keys to create a need for compaction
	for i := 0; i < 50; i++ { // Update a subset of keys
		key := fmt.Sprintf("key%d", i)
		newValue := fmt.Sprintf("update_value%d", i)
		err := engine.Put(key, newValue)
		require.NoError(t, err)
	}

	for i := 0; i < 25; i++ { // Update a subset of keys
		key := fmt.Sprintf("key%d", i)
		newValue := fmt.Sprintf("new_value%d", i)
		err := engine.Put(key, newValue)
		require.NoError(t, err)
	}

	for i := 25; i < 50; i++ { // Update a subset of keys
		key := fmt.Sprintf("key%d", i)
		newValue := fmt.Sprintf("value%d", i)
		err := engine.Put(key, newValue)
		require.NoError(t, err)
	}

	startFiles, err := os.ReadDir(tempDir)

	// Run Compaction
	err = engine.compact()
	require.NoError(t, err)

	// Assertions
	// Check if the compaction resulted in multiple log files
	compactFiles, err := os.ReadDir(tempDir)
	require.NoError(t, err)

	assert.Less(t, len(compactFiles), len(startFiles), "Expected less files after compaction")

	// Verify that the updated values are correct and old values are not present
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		expectedValue := fmt.Sprintf("value%d", i)
		if i < 25 {
			expectedValue = fmt.Sprintf("new_value%d", i) // updated values
		}
		value, err := engine.Get(key)
		require.NoError(t, err)
		assert.Equal(t, expectedValue, value, "Mismatched value after compaction for key: "+key)
	}

	// Close the engine at the end of the test
	require.NoError(t, engine.Close())
}
