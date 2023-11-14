package storage

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestSuccessfulCompactionWithUpdates(t *testing.T) {
	// Setup test environment
	tempDir, err := os.MkdirTemp("", "successful_compaction_with_update_test")
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

func TestSuccessfulCompactionWithDeletions(t *testing.T) {
	// Setup test environment
	tempDir, err := os.MkdirTemp("", "successful_compaction_with_deletion_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir) // clean up

	// Initialize Engine with a very small max file size
	verySmallMaxLogSize := int64(256) // 256 bytes
	engine, err := NewEngine(tempDir, WithMaxLogSize(verySmallMaxLogSize))
	require.NoError(t, err)

	// Populate the engine with test data
	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := engine.Put(key, value)
		require.NoError(t, err)
	}

	// Delete a subset of keys
	for i := 0; i < 25; i++ {
		key := fmt.Sprintf("key%d", i)
		err := engine.Delete(key)
		require.NoError(t, err)
	}

	err = engine.closeWriteLog()
	require.NoError(t, err)

	// Run Compaction
	err = engine.compact()
	require.NoError(t, err)

	// Get a list of compacted files
	compactFiles, err := extractDatafiles(tempDir)
	require.NoError(t, err)

	// Read each compacted file and check for deleted keys
	for _, filePath := range compactFiles {
		keys, err := extractKeysFromDataFile(filePath)
		require.NoError(t, err)

		// Check that none of the deleted keys are present
		for _, key := range keys {
			if isDeletedKey(key) {
				t.Errorf("Deleted key %s found in compacted file %s", key, filePath)
			}
		}
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		if !isDeletedKey(key) {
			value, err := engine.Get(key)
			require.NoError(t, err)
			assert.Equal(t, "value"+strconv.Itoa(i), value)
		} else {
			_, err := engine.Get(key)
			assert.Error(t, err)
		}
	}
}

// isDeletedKey checks if the key is one of the deleted keys
func isDeletedKey(key string) bool {
	keyNum, err := strconv.Atoi(strings.TrimPrefix(key, "key"))
	if err != nil {
		return false
	}
	return keyNum < 25 // since we deleted keys from key0 to key24
}
