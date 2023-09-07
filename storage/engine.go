// storage package for key-value storage engine
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// Engine struct represents the storage engine
type Engine struct {
	file       *os.File         // The file used for storing key-value pairs
	index      map[string]int64 // Index to quickly locate the position of keys in the file
	lock       sync.RWMutex     // Read-Write mutex for concurrent access
	currentPos int64            // Current write position in the file
}

// NewEngine initializes a new storage engine
func NewEngine(filename string) (*Engine, error) {
	// Open or create the file for writing, appending, and reading
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	// Initialize and return the Engine struct
	return &Engine{
		file:  file,
		index: make(map[string]int64),
	}, nil
}

// AppendKeyValue appends a key-value pair to the file
func (e *Engine) AppendKeyValue(key, value string) error {
	e.lock.Lock() // Lock for exclusive write access
	defer e.lock.Unlock()

	// Convert key to bytes and write it to the file
	keyBytes := []byte(key)
	if _, err := e.file.Write(keyBytes); err != nil {
		return err
	}

	// Find the current write position in the file
	currentPos, err := e.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Convert value to bytes
	valueBytes := []byte(value)
	valueSize := uint32(len(valueBytes))
	sizeBuffer := make([]byte, 4)

	// Write the size of the value to the file
	binary.LittleEndian.PutUint32(sizeBuffer, valueSize)
	if _, err := e.file.Write(sizeBuffer); err != nil {
		return err
	}
	// Write the actual value to the file
	if _, err := e.file.Write(valueBytes); err != nil {
		return err
	}

	// Update the index with the current write position
	e.index[key] = currentPos
	return nil
}

// GetValue retrieves the value of a given key from the file
func (e *Engine) GetValue(key string) (string, error) {
	e.lock.RLock() // Lock for read access
	position, ok := e.index[key]
	e.lock.RUnlock()

	// Check if the key exists in the index
	if !ok {
		return "", fmt.Errorf("key %s not found", key)
	}

	// Open the file for reading
	readFile, err := os.OpenFile(e.file.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer readFile.Close()

	// Seek to the position of the key in the file
	if _, err := readFile.Seek(position, io.SeekStart); err != nil {
		return "", err
	}

	// Read the size of the value
	var sizeBuffer = make([]byte, 4)
	if _, err := readFile.Read(sizeBuffer); err != nil {
		return "", err
	}

	// Convert the size from bytes to uint32
	valueSize := binary.LittleEndian.Uint32(sizeBuffer)

	// Read the actual value from the file
	valueBuffer := make([]byte, valueSize)
	if _, err := readFile.Read(valueBuffer); err != nil {
		return "", err
	}

	// Convert the value to string and return
	return string(valueBuffer), nil
}
