// Package storage for key-value storage engine
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

const (
	_  = iota // ignore first value by assigning to blank identifier
	KB = 1 << (10 * iota)
	MB
	GB
)

// log represents the data and index for the storage engine
type log struct {
	file  *os.File
	index map[string]int64
	size  int64
}

// Engine represents the storage engine for key-value storage
// It's safe for concurrent use by multiple goroutines
type Engine struct {
	logs    []*log
	lock    sync.RWMutex
	maxSize int64
}

// NewEngine initializes a new storage engine and returns a pointer to it
// creates the first log storage file with a given filename and size in bytes
func NewEngine(filename string, maxSize int64) (*Engine, error) {
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &Engine{
		logs: []*log{
			{
				file:  file,
				index: make(map[string]int64)},
		},
		maxSize: maxSize,
	}, nil
}

// AppendKeyValue appends a key-value pair to the file
func (e *Engine) AppendKeyValue(key, value string) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Get the last log file
	currentLog := e.logs[len(e.logs)-1]
	if currentLog.size >= e.maxSize {
		newFileName := fmt.Sprintf("%d.dat", len(e.logs))

		newFile, err := os.OpenFile(newFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		if err != nil {
			return err
		}

		err = currentLog.file.Close()
		if err != nil {
			return err
		}

		currentLog = &log{file: newFile, index: make(map[string]int64)}
		e.logs = append(e.logs, currentLog)
	}

	// Convert key to bytes and write it to the file
	keyBytes := []byte(key)

	written, err := currentLog.file.Write(keyBytes)
	if err != nil {
		return err
	}

	currentLog.size += int64(written)

	// Find the current write position in the file
	currentPos, err := currentLog.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	// Convert value to bytes
	valueBytes := []byte(value)
	valueSize := uint32(len(valueBytes))
	sizeBuffer := make([]byte, 4)

	// Write the size of the value to the file
	binary.LittleEndian.PutUint32(sizeBuffer, valueSize)

	written, err = currentLog.file.Write(sizeBuffer)
	if err != nil {
		return err
	}

	currentLog.size += int64(written)

	// Write the actual value to the file
	written, err = currentLog.file.Write(valueBytes)
	if err != nil {
		return err
	}

	currentLog.size += int64(written)

	// Update the index with the current write position
	currentLog.index[key] = currentPos

	return nil
}

// GetValue returns the value for a given key searching from the latest log file
// to the oldest log file available in the storage engine
func (e *Engine) GetValue(key string) (string, error) {
	e.lock.RLock()
	currentLog := e.logs[len(e.logs)-1]
	position, ok := currentLog.index[key]
	e.lock.RUnlock()
	if ok {
		return e.readValueFromFile(position, currentLog)
	}

	for i := len(e.logs) - 2; i >= 0; i-- {
		l := e.logs[i]

		p, exists := l.index[key]
		if exists {
			return e.readValueFromFile(p, l)
		}
	}

	return "", fmt.Errorf("key %s not found", key)
}

// readValueFromFile reads the value from the file at a given position by opening and seeking to the position
func (e *Engine) readValueFromFile(position int64, l *log) (string, error) {
	readFile, err := os.OpenFile(l.file.Name(), os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer readFile.Close()

	// Seek to the position of the key in the file
	if _, err := readFile.Seek(position, io.SeekStart); err != nil {
		return "", err
	}

	var sizeBuffer = make([]byte, 4)
	if _, err := readFile.Read(sizeBuffer); err != nil {
		return "", err
	}

	// Convert the size from bytes to uint32
	valueSize := binary.LittleEndian.Uint32(sizeBuffer)

	valueBuffer := make([]byte, valueSize)
	if _, err := readFile.Read(valueBuffer); err != nil {
		return "", err
	}

	return string(valueBuffer), nil
}
