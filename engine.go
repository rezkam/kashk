// Package storage for key-value storage engine
package storage

import (
	"encoding/binary"
	"fmt"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"sync"
)

const (
	_  = iota // ignore first value (0) by assigning to blank identifier
	KB = 1 << (10 * iota)
	MB
	GB
)

// default internal constants for the storage engine
// can be overridden by the user with provided options
const (
	defaultTombstone = "tombstone-jbc46-q42fd-pggmc-kp38y-6mqd8"
	defaultLogSize   = 10 * MB
	defaultKeySize   = 1 * KB
)

// Engine represents the storage engine for key-value storage
// TODO: Add garbage collector and compaction process
type Engine struct {
	// logs represents the list of log file and index for the storage engine
	// TODO: let's see if we can change this to a []log and what's the benefit of using a slice of pointers
	readLogs []*readLog
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
	// represents the lock for the storage engine to ensure only one process can write to the storage engine at a time
	lock sync.RWMutex
	// writeLog represents the current log file and index for the storage engine
	writeLog *writeLog
}

// NewEngine creates a new Engine instance with default settings which can be overridden with optional settings
// path is where the data files will be stored if the path doesn't exist it will be created
// the user should have write access to the path otherwise an error will be returned
func NewEngine(path string, options ...OptionSetter) (*Engine, error) {
	if err := validateDataPath(path); err != nil {
		return nil, err
	}

	lockFile, err := createFlock(path)
	if err != nil {
		return nil, err
	}

	dataFiles, err := extractDatafiles(path)
	if err != nil {
		return nil, err
	}

	readLogs, err := initReadLogs(dataFiles)
	if err != nil {
		return nil, err
	}

	engine := &Engine{
		maxLogBytes: defaultLogSize,
		maxKeyBytes: defaultKeySize,
		tombStone:   defaultTombstone,
		dataPath:    path,
		lockFile:    lockFile,
		readLogs:    readLogs,
	}

	file, err := engine.createNewFile()
	if err != nil {
		return nil, err
	}

	engine.writeLog = &writeLog{file: file, index: make(map[string]int64)}

	for _, option := range options {
		if err := option(engine); err != nil {
			return nil, err
		}
	}

	return engine, nil
}

type OptionSetter func(*Engine) error

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

func (e *Engine) Close() error {

	if err := e.writeLog.file.Sync(); err != nil {
		return err
	}
	if err := e.writeLog.file.Close(); err != nil {
		return err
	}

	if err := unix.Flock(int(e.lockFile.Fd()), unix.LOCK_UN); err != nil {
		return nil
	}

	return nil
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

	if e.writeLog.size >= e.maxLogBytes {
		e.readLogs = append(e.readLogs, &readLog{path: e.writeLog.file.Name(), index: e.writeLog.index})
		file, err := e.createNewFile()
		if err != nil {
			return err
		}

		err = e.writeLog.file.Close()
		if err != nil {
			return err
		}

		e.writeLog = &writeLog{file: file, index: make(map[string]int64), size: 0}
	}

	keyBytes := []byte(key)
	keySize := uint32(len(keyBytes))
	sizeBuffer := make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuffer, keySize)

	written, err := e.writeLog.file.Write(sizeBuffer)
	if err != nil {
		return err
	}

	e.writeLog.size += int64(written)

	written, err = e.writeLog.file.Write(keyBytes)
	if err != nil {
		return err
	}

	e.writeLog.size += int64(written)

	// Find the current write position in the file
	// Current position is the position that we write the value size
	currentPos, err := e.writeLog.file.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

	valueBytes := []byte(value)
	valueSize := uint32(len(valueBytes))
	sizeBuffer = make([]byte, 4)
	binary.LittleEndian.PutUint32(sizeBuffer, valueSize)
	written, err = e.writeLog.file.Write(sizeBuffer)
	if err != nil {
		return err
	}

	e.writeLog.size += int64(written)

	written, err = e.writeLog.file.Write(valueBytes)
	if err != nil {
		return err
	}

	e.writeLog.size += int64(written)

	// Update the index with the current write position
	e.writeLog.index[key] = currentPos

	return nil
}

// getValue returns the value for a given key searching from the latest log file
// to the oldest log file available in the storage engine
func (e *Engine) getValue(key string) (string, error) {
	if err := e.validateKey(key); err != nil {
		return "", err
	}
	e.lock.RLock()
	position, ok := e.writeLog.index[key]
	e.lock.RUnlock()
	if ok {
		return readValue(e.writeLog.file.Name(), position, e.tombStone)
	}

	for i := len(e.readLogs) - 1; i >= 0; i-- {
		currentLog := e.readLogs[i]

		position, exists := currentLog.index[key]
		if exists {
			return readValue(currentLog.path, position, e.tombStone)
		}
	}

	return "", fmt.Errorf("key %s not found", key)
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

func (e *Engine) createNewFile() (*os.File, error) {
	fileName := fmt.Sprintf("%d%s", len(e.readLogs)+1, dataFileFormatSuffix)
	dataFilePath := filepath.Join(e.dataPath, fileName)
	file, err := os.OpenFile(dataFilePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	return file, nil
}
