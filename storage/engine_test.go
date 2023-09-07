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

	engine, err := NewEngine("test_basic.dat")
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
	engine, err := NewEngine("test_key_collision.dat")
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

func removeFile(filename string) error {
	if err := os.Remove(filename); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}
