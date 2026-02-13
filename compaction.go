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
	done     chan struct{}
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

	// Create a new engine instance for the compaction process.
	// Filter out compaction-related options to prevent the compaction engine from
	// starting its own background compaction goroutine/ticker.
	compactionOpts := filterCompactionOptions(e.options)
	cEngine, err := NewEngine(compactionPath, compactionOpts...)
	if err != nil {
		return err
	}
	defer cEngine.Close()

	// Take a snapshot of the current read logs for processing.
	// Hold the read lock to prevent concurrent modification of readLogs slice.
	e.lock.RLock()
	snapshotReadLogs := make([]*readLog, len(e.readLogs))
	copy(snapshotReadLogs, e.readLogs)
	e.lock.RUnlock()

	// Use an in-memory set to track which keys have already been processed,
	// avoiding expensive disk-based lookups via cEngine.Get().
	processedKeys := make(map[string]struct{})

	// Map to track the keys that have been deleted
	deletedKeys := make(map[string]struct{})

	// Iterate through each log in the snapshot from newest to oldest.
	// This ensures that when a key exists in multiple logs, the newest value is kept.
	for i := len(snapshotReadLogs) - 1; i >= 0; i-- {
		currentLog := snapshotReadLogs[i]
		for key, offset := range currentLog.index {
			// Skip keys already processed (from a newer log)
			if _, ok := processedKeys[key]; ok {
				continue
			}

			// Skip keys already known to be deleted
			if _, ok := deletedKeys[key]; ok {
				continue
			}

			// Read the value from disk
			value, err := e.readValueFromFile(currentLog.path, offset)
			if err != nil {
				return fmt.Errorf("failed to read value for key %s: %w", key, err)
			}

			// Mark key as processed regardless of whether it's a tombstone
			processedKeys[key] = struct{}{}

			// Check if the current value is a tombstone, indicating the key is deleted
			if value == e.tombStone {
				deletedKeys[key] = struct{}{}
				continue
			}

			// Add the key-value pair to the compaction engine
			if err := cEngine.Put(key, value); err != nil {
				return fmt.Errorf("failed to put key-value pair in compaction engine: %w", err)
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

// filterCompactionOptions returns a copy of options with compaction-related
// options removed, so a compaction engine doesn't start its own background compaction.
func filterCompactionOptions(options []OptionSetter) []OptionSetter {
	filtered := make([]OptionSetter, 0, len(options))
	for _, opt := range options {
		// Apply each option to a dummy engine to check if it enables compaction.
		// We reconstruct safe options that only set non-compaction fields.
		dummy := &Engine{
			compactionManager: &compactionManager{},
		}
		_ = opt(dummy)
		if dummy.compactionManager.enabled || dummy.compactionManager.interval != 0 {
			continue
		}
		filtered = append(filtered, opt)
	}
	return filtered
}

// replaceCompactedLogs handles the final steps of the compaction process.
// It removes the old log files and updates the engine's read logs
// with the new compacted logs from the compaction engine.
func (e *Engine) replaceCompactedLogs(snapshotReadLogs []*readLog, cEngine *Engine) error {
	// Ensure exclusive access to the engine during the replacement process
	e.lock.Lock()
	defer e.lock.Unlock()

	// Remove old log files from disk. On Unix, open file descriptors remain valid
	// after unlink, so any in-flight reads on old handles will complete safely.
	// The file handles in snapshotReadLogs are intentionally not closed here to
	// avoid disrupting concurrent readers; they will be closed when garbage collected.
	for _, log := range snapshotReadLogs {
		if err := os.Remove(log.path); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("failed to remove old file %s: %w", log.path, err)
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

	// Update the file paths in the read logs of the compaction engine to reflect their new location.
	// Also reopen the files from their new paths so the file handles are valid.
	for _, log := range cEngine.readLogs {
		if log.file != nil {
			log.file.Close()
		}
		fileName := filepath.Base(log.path)
		newPath := filepath.Join(e.dataPath, fileName)
		log.path = newPath
		file, err := os.OpenFile(newPath, os.O_RDONLY, 0644)
		if err != nil {
			return fmt.Errorf("failed to reopen compacted log file %s: %w", newPath, err)
		}
		log.file = file
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
	e.compactionManager.done = make(chan struct{})
	go func() {
		for {
			select {
			case <-e.compactionManager.done:
				return
			case <-e.compactionManager.ticker.C:
				if err := e.compact(); err != nil {
					slog.Warn("failed to run compaction", "err", err)
				}
			}
		}
	}()
	return nil
}
