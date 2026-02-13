package storage

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"testing/synctest"
	"time"
)

// countDatFiles counts .dat files in the given directory.
func countDatFiles(dir string) int {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return 0
	}
	count := 0
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == dataFileFormatSuffix {
			count++
		}
	}
	return count
}

func TestBackgroundCompactionTriggersOnInterval(t *testing.T) {
	synctest.Run(func() {
		tempDir, err := os.MkdirTemp("", "bg_compaction_interval_test")
		if err != nil {
			t.Error("MkdirTemp:", err)
			return
		}
		defer os.RemoveAll(tempDir)

		interval := 1 * time.Hour
		engine, err := NewEngine(tempDir,
			WithMaxLogSize(256),
			WithCompactionEnabled(),
			WithCompactionInterval(interval),
		)
		if err != nil {
			t.Error("NewEngine:", err)
			return
		}

		// Write data and then overwrite all keys to create duplicates across log files.
		// This ensures compaction has redundant entries to eliminate.
		for i := 0; i < 50; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("old_value%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}
		for i := 0; i < 50; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}

		startCount := countDatFiles(tempDir)

		// Advance fake time past the compaction interval to trigger background compaction
		time.Sleep(interval)
		synctest.Wait()

		afterCount := countDatFiles(tempDir)
		if afterCount >= startCount {
			t.Errorf("expected fewer data files after background compaction, got %d (was %d)", afterCount, startCount)
		}

		// Verify all data is still accessible and correct
		for i := 0; i < 50; i++ {
			value, err := engine.Get(fmt.Sprintf("key%d", i))
			if err != nil {
				t.Errorf("Get key%d: %v", i, err)
				continue
			}
			expected := fmt.Sprintf("value%d", i)
			if value != expected {
				t.Errorf("key%d = %q, want %q", i, value, expected)
			}
		}

		if err := engine.Close(); err != nil {
			t.Error("Close:", err)
		}
	})
}

func TestBackgroundCompactionStopsAfterClose(t *testing.T) {
	synctest.Run(func() {
		tempDir, err := os.MkdirTemp("", "bg_compaction_stop_test")
		if err != nil {
			t.Error("MkdirTemp:", err)
			return
		}
		defer os.RemoveAll(tempDir)

		interval := 1 * time.Hour
		engine, err := NewEngine(tempDir,
			WithMaxLogSize(256),
			WithCompactionEnabled(),
			WithCompactionInterval(interval),
		)
		if err != nil {
			t.Error("NewEngine:", err)
			return
		}

		for i := 0; i < 30; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}

		// Close the engine — ticker should stop, background goroutine should exit
		if err := engine.Close(); err != nil {
			t.Error("Close:", err)
			return
		}

		countAfterClose := countDatFiles(tempDir)

		// Advance time well past the interval — no compaction should run
		time.Sleep(3 * interval)
		synctest.Wait()

		countLater := countDatFiles(tempDir)
		if countLater != countAfterClose {
			t.Errorf("file count changed after Close: was %d, now %d", countAfterClose, countLater)
		}
	})
}

func TestBackgroundCompactionMultipleCycles(t *testing.T) {
	synctest.Run(func() {
		tempDir, err := os.MkdirTemp("", "bg_compaction_multi_test")
		if err != nil {
			t.Error("MkdirTemp:", err)
			return
		}
		defer os.RemoveAll(tempDir)

		interval := 30 * time.Minute
		engine, err := NewEngine(tempDir,
			WithMaxLogSize(256),
			WithCompactionEnabled(),
			WithCompactionInterval(interval),
		)
		if err != nil {
			t.Error("NewEngine:", err)
			return
		}

		// Write initial data
		for i := 0; i < 40; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}

		// Trigger first compaction cycle
		time.Sleep(interval)
		synctest.Wait()

		// Write more data and update existing keys between compaction cycles
		for i := 40; i < 60; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}
		for i := 0; i < 15; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("updated_%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}

		// Trigger second compaction cycle
		time.Sleep(interval)
		synctest.Wait()

		// Delete some keys between cycles
		for i := 55; i < 60; i++ {
			if err := engine.Delete(fmt.Sprintf("key%d", i)); err != nil {
				t.Error("Delete:", err)
				return
			}
		}

		// Trigger third compaction cycle
		time.Sleep(interval)
		synctest.Wait()

		// Verify all data integrity after three compaction cycles
		for i := 0; i < 60; i++ {
			key := fmt.Sprintf("key%d", i)
			value, err := engine.Get(key)

			if i >= 55 {
				// These were deleted
				if err == nil {
					t.Errorf("expected error for deleted %s, got value %q", key, value)
				}
				continue
			}

			if err != nil {
				t.Errorf("Get %s: %v", key, err)
				continue
			}

			var expected string
			if i < 15 {
				expected = fmt.Sprintf("updated_%d", i)
			} else {
				expected = fmt.Sprintf("value%d", i)
			}
			if value != expected {
				t.Errorf("%s = %q, want %q", key, value, expected)
			}
		}

		if err := engine.Close(); err != nil {
			t.Error("Close:", err)
		}
	})
}

func TestNoNestedCompactionTickers(t *testing.T) {
	synctest.Run(func() {
		tempDir, err := os.MkdirTemp("", "no_nested_tickers_test")
		if err != nil {
			t.Error("MkdirTemp:", err)
			return
		}
		defer os.RemoveAll(tempDir)

		interval := 30 * time.Minute
		engine, err := NewEngine(tempDir,
			WithMaxLogSize(256),
			WithCompactionEnabled(),
			WithCompactionInterval(interval),
		)
		if err != nil {
			t.Error("NewEngine:", err)
			return
		}

		for i := 0; i < 30; i++ {
			if err := engine.Put(fmt.Sprintf("key%d", i), fmt.Sprintf("value%d", i)); err != nil {
				t.Error("Put:", err)
				return
			}
		}

		// Trigger compaction. Without filterCompactionOptions, the compaction engine
		// would start its own background goroutine with a ticker that never stops,
		// causing synctest.Run to deadlock.
		time.Sleep(interval)
		synctest.Wait()

		// Advance time again — if nested tickers leaked, they would try to compact
		// the (now deleted) compaction directory and potentially panic or error.
		time.Sleep(interval)
		synctest.Wait()

		// A third cycle to be thorough
		time.Sleep(interval)
		synctest.Wait()

		// Verify the engine still works correctly
		for i := 0; i < 30; i++ {
			value, err := engine.Get(fmt.Sprintf("key%d", i))
			if err != nil {
				t.Errorf("Get key%d: %v", i, err)
				continue
			}
			expected := fmt.Sprintf("value%d", i)
			if value != expected {
				t.Errorf("key%d = %q, want %q", i, value, expected)
			}
		}

		if err := engine.Close(); err != nil {
			t.Error("Close:", err)
		}
	})
}
