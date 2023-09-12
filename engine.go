// Package storage for key-value storage engine
package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"

	"golang.org/x/sys/unix"
)

const (
	_  = iota // ignore first value by assigning to blank identifier
	KB = 1 << (10 * iota)
	MB
	GB
)

const (
	defaultTombstone = "tombstone-jbc46-q42fd-pggmc-kp38y-6mqd8"
	DefaultLogSize   = 10 * MB
	defaultKeySize   = 1 * KB
	firstLogFileName = "1.dat"
)

// log represents the data and index for the storage engine
type log struct {
	file  *os.File
	index map[string]int64
	size  int64
}

// Engine represents the storage engine for key-value storage
// TODO: Add garbage collector and compaction process
type Engine struct {
	// logs represents the list of log file and index for the storage engine
	logs []*log
	lock sync.RWMutex
	// maxLogBytes represents the max size of the log file in bytes if the log file exceeds this size
	// a new log file will be created and this file will be closed for writing.
	// the smaller the size the more log files will be created and the more time it will take to read a key specially
	// if the key is not found in the storage we have to recursively search all the log files to find the key from the
	// latest to the oldest log file so inorder to reduce the number of log files we can increase the max log size
	maxLogBytes int64
	// maxKeyBytes represents the max size of the key in bytes if the key exceeds this size an error will be returned
	// and the state of the storage engine will not be changed. Since all the keys are stored in the in-memory index
	// it's better to keep the key size small to reduce the memory footprint of the storage engine and practically have
	// more keys in the storage engine
	maxKeyBytes int64
	// represents the tombstone value for the storage engine which a special value used to mark a key as deleted
	// the key will still be part of the index and the value will be set to the tombstone value which later will be
	// picked up by the garbage collector and removed from the index also the compaction process will remove the key
	// from all the other log files
	tombStone string
	// represents the path where the data files will be stored if the path doesn't exist it will be created
	dataPath string
	// represents the file used to lock the storage engine for writing
	// this lock makes sure only one process can write to the storage engine at a time
	lockFile *os.File
}

type OptionSetter func(*Engine) error

// NewEngine creates a new Engine instance with default settings
// which can be overridden with optional settings
// path is where the data files will be stored if the path doesn't exist it will be created
// the user should have write access to the path otherwise an error will be returned
func NewEngine(path string, options ...OptionSetter) (*Engine, error) {
	if err := validateDataPath(path); err != nil {
		return nil, err
	}
	if exists, err := dataFileExists(path); err != nil {
		return nil, err
	} else if exists {
		return nil, fmt.Errorf("data file exists we have to index it")
	}

	lockFile, err := createFlock(path)
	if err != nil {
		return nil, err
	}

	file, err := os.OpenFile(path+firstLogFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		maxLogBytes: DefaultLogSize,
		maxKeyBytes: defaultKeySize,
		tombStone:   defaultTombstone,
		dataPath:    path,
		lockFile:    lockFile,
		logs:        []*log{{file: file, index: make(map[string]int64)}},
	}

	for _, option := range options {
		if err := option(engine); err != nil {
			return nil, err
		}
	}

	return engine, nil
}

func (e *Engine) Close() error {
	currentLog := e.logs[len(e.logs)-1]

	if err := currentLog.file.Sync(); err != nil {
		return err
	}
	if err := currentLog.file.Close(); err != nil {
		return err
	}

	if err := unix.Flock(int(e.lockFile.Fd()), unix.LOCK_UN); err != nil {
		return nil
	}

	return nil
}

// WithMaxLogSize sets the max size of the log file
func WithMaxLogSize(size int64) OptionSetter {
	return func(e *Engine) error {
		if size <= 0 {
			return fmt.Errorf("invalid max log size")
		}
		e.maxLogBytes = size

		return nil
	}
}

// WithMaxKeySize sets the max size of the key
func WithMaxKeySize(size int64) OptionSetter {
	return func(e *Engine) error {
		if size <= 0 {
			return fmt.Errorf("invalid max key size")
		}
		e.maxKeyBytes = size

		return nil
	}
}

// WithTombStone sets the tombstone value
func WithTombStone(value string) OptionSetter {
	return func(engine *Engine) error {
		if value == "" {
			return fmt.Errorf("invalid tombstone value")
		}
		engine.tombStone = value

		return nil
	}
}

// Put set a key-value pair in the storage engine
// key and value are strings
func (e *Engine) Put(key, value string) error {
	return e.putKeyValue(key, value)
}

// Get returns the value for a given key from the storage engine
func (e *Engine) Get(key string) (string, error) {
	return e.getValue(key)
}

// Delete deletes a key-value pair from the storage engine
// Internally it sets the value to a tombstone value and then garbage collector will remove it
func (e *Engine) Delete(key string) error {
	return e.deleteKey(key)
}

// deleteKey validates the key and then appends the key-value pair to the storage engine
func (e *Engine) deleteKey(key string) error {
	if err := e.validateKey(key); err != nil {
		return err
	}
	return e.appendKeyValue(key, e.tombStone)
}

// putKeyValue validates the key and value and then appends the key-value pair to the storage engine
func (e *Engine) putKeyValue(key, value string) error {
	if err := e.validateKey(key); err != nil {
		return err
	}
	if err := e.validateValue(value); err != nil {
		return err
	}
	return e.appendKeyValue(key, value)
}

// appendKeyValue appends a key-value pair to the file
func (e *Engine) appendKeyValue(key, value string) error {
	e.lock.Lock()
	defer e.lock.Unlock()

	// Get the last log file
	currentLog := e.logs[len(e.logs)-1]
	if currentLog.size >= e.maxLogBytes {
		newFileName := fmt.Sprintf("%d.dat", len(e.logs)+1)

		newFile, err := os.OpenFile(e.dataPath+newFileName, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
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

// getValue returns the value for a given key searching from the latest log file
// to the oldest log file available in the storage engine
func (e *Engine) getValue(key string) (string, error) {
	if err := e.validateKey(key); err != nil {
		return "", err
	}
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
	defer func(readFile *os.File) {
		_ = readFile.Close()
	}(readFile)

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
	if string(valueBuffer) == e.tombStone {
		return "", fmt.Errorf("key not found")
	}
	return string(valueBuffer), nil
}

func (e *Engine) validateKey(key string) error {
	if key == "" {
		return fmt.Errorf("key cannot be empty")
	}
	if int64(len([]byte(key))) > e.maxKeyBytes {
		return fmt.Errorf("key cannot be longer than %d bytes", e.maxKeyBytes)
	}
	return nil
}

func (e *Engine) validateValue(value string) error {
	if value == e.tombStone {
		return fmt.Errorf("value cannot be tombstone")
	}
	// value size should be less than the max size of the log file
	if int64(len([]byte(value))) > e.maxLogBytes {
		return fmt.Errorf("value cannot be longer than %d bytes", e.maxLogBytes)
	}
	return nil
}
