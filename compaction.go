package storage

import (
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type compactionManager struct {
	enabled  bool
	interval time.Duration
	ticker   *time.Ticker
	lock     sync.Mutex
}

// compact orchestrates the compaction process for the storage engine.
// It ensures that only one compaction process can run at a time and manages the creation,
// execution, and cleanup of the compaction environment.
func (e *Engine) compact() error {
	// Acquire a lock to ensure single execution of the compaction process
	e.compactionManager.lock.Lock()
	defer e.compactionManager.lock.Unlock()

	// Define the path for the compaction directory
	compactionPath := filepath.Join(e.dataPath, "compaction")
	compactionPath = ensureTrailingSlash(compactionPath)

	// Check if the compaction directory already exists as a sign of problematic or incomplete compaction process
	if _, err := os.Stat(compactionPath); err == nil {
		return fmt.Errorf("compaction process already in progress or previous compaction was not properly cleaned up")
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("failed to check compaction directory: %w", err)
	}

	// Create the compaction directory
	if err := os.MkdirAll(compactionPath, 0755); err != nil {
		return fmt.Errorf("failed to create compaction directory: %w", err)
	}

	// cleanup compaction path
	defer func() {
		// Cleanup compaction directory after compaction, regardless of success or failure
		if cleanupErr := os.RemoveAll(compactionPath); cleanupErr != nil {
			slog.Warn("failed to clean up compaction directory", "err", cleanupErr)
		}
	}()

	// Create a new engine instance for the compaction process
	// compaction engine should have the same settings and options as the main engine
	cEngine, err := NewEngine(compactionPath, e.options...)
	if err != nil {
		return err
	}

	// Take a snapshot of the current read logs for processing
	snapshotReadLogs := make([]*readLog, len(e.readLogs))
	copy(snapshotReadLogs, e.readLogs)

	// Map to track the keys that have been deleted
	deletedKeys := make(map[string]struct{})

	// Iterate through each log in the snapshot and compact the data
	for i := len(snapshotReadLogs) - 1; i >= 0; i-- {
		currentLog := snapshotReadLogs[i]
		for key, offset := range currentLog.index {
			if _, ok := deletedKeys[key]; ok {
				continue // Skip this key as it's already deleted
			}

			// Try to get the key from the compaction engine. If it exists, no need to re-add it.
			if _, err := cEngine.Get(key); err != nil {
				// If the key doesn't exist in the compaction engine, read its value
				value, err := e.readValueFromFile(currentLog.path, offset)
				if err != nil {
					return fmt.Errorf("failed to read value for key %s: %w", key, err)
				}

				// Check if the current value is a tombstone, indicating the key is deleted
				if value == e.tombStone {
					deletedKeys[key] = struct{}{}
					continue // Skip adding this key-value pair to the compaction engine
				}

				// Add the key-value pair to the compaction engine
				if err := cEngine.Put(key, value); err != nil {
					return fmt.Errorf("failed to put key-value pair in compaction engine: %w", err)
				}
			}
		}
	}

	// Close the write log of the compaction engine to finalize the current log
	err = cEngine.closeWriteLog()
	if err != nil {
		return err
	}

	// Replace the compacted logs in the original engine
	err = e.replaceCompactedLogs(snapshotReadLogs, cEngine)
	if err != nil {
		return err
	}

	return nil
}

// replaceCompactedLogs handles the final steps of the compaction process.
// It moves the old log files to a backup directory and updates the engine's read logs
// with the new compacted logs from the compaction engine.
func (e *Engine) replaceCompactedLogs(snapshotReadLogs []*readLog, cEngine *Engine) error {
	// Ensure exclusive access to the engine during the replacement process
	e.lock.Lock()
	defer e.lock.Unlock()

	// Create a backup directory with a timestamp to store old logs
	backupPath := filepath.Join(e.dataPath, "compaction_backup", time.Now().Format("20060102150405"))
	if err := os.MkdirAll(backupPath, 0755); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	// Move each old log file to the backup directory
	for _, log := range snapshotReadLogs {
		backupFilePath := filepath.Join(backupPath, filepath.Base(log.path))
		if err := os.Rename(log.path, backupFilePath); err != nil {
			return fmt.Errorf("failed to move old file %s to backup: %w", log.path, err)
		}
	}

	// Move compacted files from the compaction directory to the main directory
	compactionFiles, err := extractDatafiles(cEngine.dataPath)
	if err != nil {
		return fmt.Errorf("failed to read compaction directory: %w", err)
	}
	for _, path := range compactionFiles {
		newPath := filepath.Join(e.dataPath, filepath.Base(path))
		if err := os.Rename(path, newPath); err != nil {
			return fmt.Errorf("failed to move compacted file %s to %s: %w", path, newPath, err)
		}
	}

	// Update the file paths in the read logs of the compaction engine to reflect their new location
	for _, log := range cEngine.readLogs {
		fileName := filepath.Base(log.path)
		log.path = filepath.Join(e.dataPath, fileName)
	}

	// Combine the new compacted logs with the remaining original logs
	newReadLogs := make([]*readLog, len(cEngine.readLogs))
	copy(newReadLogs, cEngine.readLogs)

	for _, log := range e.readLogs {
		if !isLogInSnapshot(log, snapshotReadLogs) {
			newReadLogs = append(newReadLogs, log)
		}
	}

	e.readLogs = newReadLogs

	return nil
}

func isLogInSnapshot(log *readLog, snapshotReadLogs []*readLog) bool {
	for _, snapLog := range snapshotReadLogs {
		if log.path == snapLog.path {
			return true
		}
	}
	return false
}

func (e *Engine) startBackgroundCompaction() error {
	if !e.compactionManager.enabled {
		return fmt.Errorf("compaction is not enabled")
	}

	e.compactionManager.ticker = time.NewTicker(e.compactionManager.interval)
	go func() {
		for range e.compactionManager.ticker.C {
			if err := e.compact(); err != nil {
				slog.Warn("failed to run compaction", "err", err)
			}
		}
	}()
	return nil
}
