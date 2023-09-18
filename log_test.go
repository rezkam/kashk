package storage

import (
	"encoding/binary"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExtractReadLog(t *testing.T) {
	// Setup
	tmpFile, err := os.CreateTemp("", "test-log")
	require.NoError(t, err)

	defer os.Remove(tmpFile.Name())

	// Write binary-formatted key-value pairs to the file
	writeKeyValue := func(key, value string) error {
		keySize := uint32(len(key))
		valueSize := uint32(len(value))

		err := binary.Write(tmpFile, binary.LittleEndian, keySize)
		require.NoError(t, err)

		_, err = tmpFile.Write([]byte(key))
		require.NoError(t, err)

		err = binary.Write(tmpFile, binary.LittleEndian, valueSize)
		require.NoError(t, err)

		_, err = tmpFile.Write([]byte(value))
		require.NoError(t, err)

		return nil
	}

	err = writeKeyValue("key1", "value1")
	require.NoError(t, err, "Failed to write key-value pair")

	err = writeKeyValue("key2", "value2")
	require.NoError(t, err, "Failed to write key-value pair")

	err = writeKeyValue("key3", "value3")
	require.NoError(t, err, "Failed to write key-value pair")
	tmpFile.Close()

	// Run function
	readLog, err := extractReadLog(tmpFile.Name())
	require.NoError(t, err)

	// Validate results
	assert.Equal(t, readLog.path, tmpFile.Name())

	require.NotNil(t, readLog.index, "Expected non-nil index")

	expectedOffsets := map[string]int64{
		"key1": 8,
		"key2": 26,
		"key3": 44,
	}

	for k, expected := range expectedOffsets {
		actual, found := readLog.index[k]
		require.True(t, found, "Key %s not found in index", k)
		assert.Equal(t, expected, actual, "Expected offset %d, got %d", expected, actual)
	}
}
