package storage

import (
	"os"
	"testing"
)

// Test for basic AppendKeyValue and GetValue functionality
func TestBasicFunctionality(t *testing.T) {
	fileName := "test_basic.dat"
	// Remove the test file if it exists
	if err := removeFile(fileName); err != nil {
		t.Fatal("Failed to remove test file:", err)
	}

	engine, err := NewEngine("test_basic.dat", 1*KB)
	if err != nil {
		t.Fatal("Failed to create engine:", err)
	}

	// Test appending a key-value pair and reading it back
	key, value := "name", "gopher"
	if err := engine.AppendKeyValue(key, value); err != nil {
		t.Fatal("Failed to append key-value:", err)
	}

	readValue, err := engine.GetValue(key)
	if err != nil {
		t.Fatal("Failed to get value:", err)
	}

	if readValue != value {
		t.Fatalf("Expected value '%s', got '%s'", value, readValue)
	}
}

// Test for key collisions and overwrite behavior

func TestKeyCollision(t *testing.T) {
	fileName := "test_key_collision.dat"
	// Remove the test file if it exists
	if err := removeFile(fileName); err != nil {
		t.Fatal("Failed to remove test file:", err)
	}

	engine, err := NewEngine("test_key_collision.dat", 1*KB)
	if err != nil {
		t.Fatal("Failed to create engine:", err)
	}

	key, value1, value2 := "name", "gopher", "badger"
	if err := engine.AppendKeyValue(key, value1); err != nil {
		t.Fatal("Failed to append first key-value:", err)
	}

	// Overwrite the value for the same key
	if err := engine.AppendKeyValue(key, value2); err != nil {
		t.Fatal("Failed to append second key-value:", err)
	}

	readValue, err := engine.GetValue(key)
	if err != nil {
		t.Fatal("Failed to get value:", err)
	}

	if readValue != value2 {
		t.Fatalf("Expected value '%s', got '%s'", value2, readValue)
	}
}

func TestGetValueFromSecondLog(t *testing.T) {
	fileName := "test_get_value_from_second_log.dat"
	// Remove the test file if it exists
	if err := removeFile(fileName); err != nil {
		t.Fatal("Failed to remove test file:", err)
	}

	engine, err := NewEngine(fileName, 1)
	if err != nil {
		t.Fatal("Failed to create engine:", err)
	}

	if err := engine.AppendKeyValue("key1", "value1"); err != nil {
		t.Fatalf("Failed to append key-value: %v", err)
	}

	if err := engine.AppendKeyValue("key2", "value2"); err != nil {
		t.Fatalf("Failed to append key-value: %v", err)
	}

	// try to get value from the first log file
	value, err := engine.GetValue("key1")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if value != "value1" {
		t.Fatalf("Expected value 'value1', got '%s'", value)
	}

	// try to get value from the second log file
	value, err = engine.GetValue("key2")
	if err != nil {
		t.Fatalf("Failed to get value: %v", err)
	}

	if value != "value2" {
		t.Fatalf("Expected value 'value2', got '%s'", value)
	}
}

func removeFile(filename string) error {
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}
