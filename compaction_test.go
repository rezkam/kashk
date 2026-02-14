package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	require.NoError(t, err)

	// Run Compaction
	err = engine.compact()
	require.NoError(t, err)

	// Assertions
	// Check if the compaction resulted in fewer files
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
	// Note: engine.Close() is not called here because closeWriteLog() above
	// already closed the write log file, leaving the writeLog in a state
	// where Close() would fail trying to sync an already-closed file.
}

func TestCompactionNoBackupDirectoryAccumulation(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_no_backup_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine, err := NewEngine(tempDir, WithMaxLogSize(256))
	require.NoError(t, err)

	// Write enough data to create multiple log files
	for i := 0; i < 30; i++ {
		require.NoError(t, engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	// Run compaction multiple times
	for round := 0; round < 3; round++ {
		require.NoError(t, engine.compact())
	}

	// Verify no compaction_backup directories were created
	entries, err := os.ReadDir(tempDir)
	require.NoError(t, err)
	for _, entry := range entries {
		assert.False(t, entry.IsDir() && strings.HasPrefix(entry.Name(), "compaction_backup"),
			"Found leftover backup directory: %s", entry.Name())
	}

	// Verify no compaction temp directory left behind
	for _, entry := range entries {
		assert.False(t, entry.IsDir() && entry.Name() == "compaction",
			"Found leftover compaction directory")
	}

	// Verify data integrity after multiple compactions
	for i := 0; i < 30; i++ {
		value, err := engine.Get(fmt.Sprintf("key%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%d", i), value)
	}

	require.NoError(t, engine.Close())
}

func TestCompactionWithEnabledOption(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_enabled_option_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create engine with compaction enabled — the compaction engine should NOT
	// inherit the compaction enabled flag and start its own background goroutine.
	engine, err := NewEngine(tempDir,
		WithMaxLogSize(256),
		WithCompactionEnabled(),
		WithCompactionInterval(1*time.Hour),
	)
	require.NoError(t, err)

	for i := 0; i < 30; i++ {
		require.NoError(t, engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	// This should succeed without issues from nested compaction engines
	require.NoError(t, engine.compact())

	// Verify data integrity
	for i := 0; i < 30; i++ {
		value, err := engine.Get(fmt.Sprintf("key%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%d", i), value)
	}

	require.NoError(t, engine.Close())
}

func TestCompactionWritesDuringCompaction(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_writes_during_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine, err := NewEngine(tempDir, WithMaxLogSize(256))
	require.NoError(t, err)

	// Write initial data
	for i := 0; i < 50; i++ {
		require.NoError(t, engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	// Run compaction
	require.NoError(t, engine.compact())

	// Write new data after compaction (these go to the active write log, not compacted)
	for i := 50; i < 75; i++ {
		require.NoError(t, engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	// Update some compacted keys
	for i := 0; i < 10; i++ {
		require.NoError(t, engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("updated_value%d", i)))
	}

	// Verify all data is accessible
	for i := 0; i < 75; i++ {
		value, err := engine.Get(fmt.Sprintf("key%d", i))
		require.NoError(t, err)
		if i < 10 {
			assert.Equal(t, fmt.Sprintf("updated_value%d", i), value)
		} else {
			assert.Equal(t, fmt.Sprintf("value%d", i), value)
		}
	}

	require.NoError(t, engine.Close())
}

func TestCompactionCleanupOnError(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "compaction_cleanup_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	engine, err := NewEngine(tempDir, WithMaxLogSize(256))
	require.NoError(t, err)

	for i := 0; i < 20; i++ {
		require.NoError(t, engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)))
	}

	// Simulate a leftover compaction directory
	compactionPath := filepath.Join(tempDir, "compaction")
	require.NoError(t, os.MkdirAll(compactionPath, 0755))

	// Compaction should fail because the directory already exists
	err = engine.compact()
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already in progress")

	// Clean up the directory and try again — it should succeed
	require.NoError(t, os.RemoveAll(compactionPath))
	require.NoError(t, engine.compact())

	// Verify data
	for i := 0; i < 20; i++ {
		value, err := engine.Get(fmt.Sprintf("key%d", i))
		require.NoError(t, err)
		assert.Equal(t, fmt.Sprintf("value%d", i), value)
	}

	require.NoError(t, engine.Close())
}

func TestFilterCompactionOptions(t *testing.T) {
	options := []OptionSetter{
		WithMaxLogSize(512),
		WithCompactionEnabled(),
		WithCompactionInterval(5 * time.Minute),
		WithMaxKeySize(2 * KB),
	}

	filtered := filterCompactionOptions(options)

	// Should only keep non-compaction options (MaxLogSize and MaxKeySize)
	assert.Len(t, filtered, 2)

	// Verify the filtered options work correctly
	dummy := &Engine{
		compactionManager: &compactionManager{},
	}
	for _, opt := range filtered {
		require.NoError(t, opt(dummy))
	}
	assert.Equal(t, int64(512), dummy.maxLogBytes)
	assert.Equal(t, int64(2*KB), dummy.maxKeyBytes)
	assert.False(t, dummy.compactionManager.enabled)
}

// isDeletedKey checks if the key is one of the deleted keys
func isDeletedKey(key string) bool {
	keyNum, err := strconv.Atoi(strings.TrimPrefix(key, "key"))
	if err != nil {
		return false
	}
	return keyNum < 25 // since we deleted keys from key0 to key24
}
