package storage

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test for basic AppendKeyValue and GetValue functionality
func TestBasicFunctionality(t *testing.T) {
	dataPath := "test_basic/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath)
	require.NoError(t, err)

	key, value := "name", "gopher"
	err = engine.Put(key, value)
	require.NoError(t, err)

	readValue, err := engine.Get(key)
	require.NoError(t, err)

	assert.Equal(t, value, readValue)

	require.NoError(t, engine.Close())
}

// Test for key collisions and overwrite behavior
func TestKeyCollision(t *testing.T) {
	dataPath := "test_key_collision/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath)
	require.NoError(t, err)

	key, value1, value2 := "name", "gopher", "badger"
	require.NoError(t, engine.Put(key, value1))

	require.NoError(t, engine.Put(key, value2))

	readValue, err := engine.Get(key)
	require.NoError(t, err)

	assert.Equal(t, value2, readValue)

	require.NoError(t, engine.Close())
}

func TestGetValueFromSecondLog(t *testing.T) {
	dataPath := "test_get_value_from_second_log/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath, WithMaxLogSize(1)) // size = 1 byte
	require.NoError(t, err)

	require.NoError(t, engine.Put("key1", "1"))
	require.NoError(t, engine.Put("key2", "2"))

	// try to get value from the first log file
	value, err := engine.Get("key1")
	require.NoError(t, err)
	assert.Equal(t, "1", value)

	// try to get value from the second log file
	value, err = engine.Get("key2")
	require.NoError(t, err)
	assert.Equal(t, "2", value)

	require.NoError(t, engine.Close())
}

// Test for deleting a key-value pair using tombstone value
func TestDelete(t *testing.T) {
	dataPath := "test_delete/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath)
	require.NoError(t, err)

	key, value := "name", "gopher"
	require.NoError(t, engine.Put(key, value))

	readValue, err := engine.Get(key)
	require.NoError(t, err)
	assert.Equal(t, value, readValue)

	require.NoError(t, engine.Delete(key))

	_, err = engine.Get(key)
	require.Error(t, err)

	require.NoError(t, engine.Close())
}

// Test for empty key
func TestEmptyKey(t *testing.T) {
	dataPath := "test_empty_key/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath)
	require.NoError(t, err)

	require.Error(t, engine.Put("", "value"))

	require.NoError(t, engine.Close())
}

// Test for empty value
func TestEmptyValue(t *testing.T) {
	dataPath := "test_empty_value/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath)
	require.NoError(t, err)

	require.NoError(t, engine.Put("key", ""))

	readValue, err := engine.Get("key")
	require.NoError(t, err)
	assert.Equal(t, "", readValue)

	require.NoError(t, engine.Close())
}

// Test for large key and value
func TestLargeKeyValue(t *testing.T) {
	dataPath := "test_large_key_value/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath, WithMaxKeySize(1*KB), WithMaxLogSize(10*KB))
	require.NoError(t, err)

	largeKey := string(make([]byte, 2*KB))
	require.Error(t, engine.Put(largeKey, "value"))

	largeValue := string(make([]byte, 20*KB))
	require.Error(t, engine.Put("key", largeValue))

	require.NoError(t, engine.Close())
}

// Test for deleting a non-existent key
func TestDeleteNonExistentKey(t *testing.T) {
	dataPath := "test_delete_non_existent_key/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath)
	require.NoError(t, err)

	require.NoError(t, engine.Delete("non_existent_key"))

	require.NoError(t, engine.Close())
}

// Test for key size and value size validation
func TestKeyAndValueSizeValidation(t *testing.T) {
	dataPath := "test_key_value_size_validation/"
	require.NoError(t, removeDir(dataPath))

	engine, err := NewEngine(dataPath, WithMaxKeySize(10), WithMaxLogSize(10))
	require.NoError(t, err)

	require.Error(t, engine.Put("veryLongKeyForThis", "value"))
	require.Error(t, engine.Put("key", "veryLongValueForThis"))

	require.NoError(t, engine.Close())
}

func removeDir(dirname string) error {
	if err := os.RemoveAll(dirname); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
