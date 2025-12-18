package storage

import (
	"fmt"
	"testing"
)

// BenchmarkGetBytes measures GetBytes performance (returns []byte, no string conversion)
func BenchmarkGetBytes(b *testing.B) {
	dataPath := "bench_get_bytes/"
	if err := removeDir(dataPath); err != nil {
		b.Fatal(err)
	}
	defer removeDir(dataPath)

	engine, err := NewEngine(dataPath, WithMaxLogSize(100*MB))
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := string(make([]byte, 1*KB))
	for i := 0; i < 10000; i++ {
		if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
			b.Fatal(err)
		}
	}

	targetKey := "key5000"

	for b.Loop() {
		_, _ = engine.GetBytes(targetKey)
	}
}

// BenchmarkGetByDataSize measures how Get latency degrades as the dataset grows
func BenchmarkGetByDataSize(b *testing.B) {
	sizes := []int{100, 1_000, 10_000, 100_000, 500_000}

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("keys_%d", numKeys), func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_get_size_%d/", numKeys)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(500*MB))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			value := string(make([]byte, 1*KB))
			for i := 0; i < numKeys; i++ {
				if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
					b.Fatal(err)
				}
			}

			targetKey := fmt.Sprintf("key%d", numKeys/2)

			for b.Loop() {
				_, _ = engine.Get(targetKey)
			}
		})
	}
}

// BenchmarkGetByLogDepth measures how Get latency increases when searching through multiple log files
func BenchmarkGetByLogDepth(b *testing.B) {
	depths := []int{1, 5, 10, 25, 50, 100}

	for _, numLogs := range depths {
		b.Run(fmt.Sprintf("logs_%d", numLogs), func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_get_depth_%d/", numLogs)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(10*KB))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			targetKey := "target_key"
			if err := engine.Put(targetKey, "target_value"); err != nil {
				b.Fatal(err)
			}

			value := string(make([]byte, 5*KB))
			for i := 0; i < numLogs; i++ {
				for j := 0; j < 3; j++ {
					if err := engine.Put(fmt.Sprintf("filler_%d_%d", i, j), value); err != nil {
						b.Fatal(err)
					}
				}
			}

			for b.Loop() {
				_, _ = engine.Get(targetKey)
			}
		})
	}
}

// BenchmarkGetMiss measures lookup time for keys that don't exist (single log file)
func BenchmarkGetMiss(b *testing.B) {
	sizes := []int{100, 1_000, 10_000, 100_000, 500_000}

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("keys_%d", numKeys), func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_get_miss_%d/", numKeys)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(500*MB))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			value := string(make([]byte, 512))
			for i := 0; i < numKeys; i++ {
				if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				_, _ = engine.Get("nonexistent_key")
			}
		})
	}
}

// BenchmarkGetMissWithManyLogs measures worst-case missing key lookup across many log files
func BenchmarkGetMissWithManyLogs(b *testing.B) {
	logCounts := []int{5, 10, 25, 50, 100, 200}

	for _, targetLogs := range logCounts {
		b.Run(fmt.Sprintf("logs_%d", targetLogs), func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_get_miss_logs_%d/", targetLogs)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			// Tiny log size to create many files
			engine, err := NewEngine(dataPath, WithMaxLogSize(1*KB))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			// Create many log files with real data
			value := string(make([]byte, 512))
			for i := 0; i < targetLogs*2; i++ {
				if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				// Search for a key that doesn't exist - must check all log files
				_, _ = engine.Get("nonexistent_key_that_forces_full_search")
			}
		})
	}
}

// BenchmarkPutUniqueKeys measures Put throughput with growing index
func BenchmarkPutUniqueKeys(b *testing.B) {
	dataPath := "bench_put_unique/"
	if err := removeDir(dataPath); err != nil {
		b.Fatal(err)
	}
	defer removeDir(dataPath)

	engine, err := NewEngine(dataPath, WithMaxLogSize(1000*MB))
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := string(make([]byte, 4*KB))
	i := 0

	for b.Loop() {
		_ = engine.Put(fmt.Sprintf("key%d", i), value)
		i++
	}
}

// BenchmarkIndexReconstruction measures startup time with existing data
func BenchmarkIndexReconstruction(b *testing.B) {
	sizes := []int{1_000, 10_000, 50_000, 100_000, 250_000}

	for _, numKeys := range sizes {
		b.Run(fmt.Sprintf("keys_%d", numKeys), func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_index_rebuild_%d/", numKeys)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}

			engine, err := NewEngine(dataPath, WithMaxLogSize(500*MB))
			if err != nil {
				b.Fatal(err)
			}

			value := string(make([]byte, 256))
			for i := 0; i < numKeys; i++ {
				if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
					b.Fatal(err)
				}
			}
			engine.Close()

			for b.Loop() {
				eng, err := NewEngine(dataPath, WithMaxLogSize(500*MB))
				if err != nil {
					b.Fatal(err)
				}
				eng.Close()
			}

			removeDir(dataPath)
		})
	}
}

// BenchmarkLargeValues measures write performance with increasingly large values
func BenchmarkLargeValues(b *testing.B) {
	sizes := []int{256, 1 * KB, 16 * KB, 64 * KB, 256 * KB, 1 * MB, 4 * MB}

	for _, size := range sizes {
		name := fmt.Sprintf("value_%dB", size)
		if size >= KB {
			name = fmt.Sprintf("value_%dKB", size/KB)
		}
		if size >= MB {
			name = fmt.Sprintf("value_%dMB", size/MB)
		}

		b.Run(name, func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_large_val_%d/", size)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(int64(size*200)))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			value := string(make([]byte, size))
			i := 0

			for b.Loop() {
				_ = engine.Put(fmt.Sprintf("key%d", i), value)
				i++
			}
		})
	}
}

// BenchmarkManySmallLogs measures overhead of having many log files
func BenchmarkManySmallLogs(b *testing.B) {
	logCounts := []int{5, 10, 25, 50, 100, 200}

	for _, targetLogs := range logCounts {
		b.Run(fmt.Sprintf("logs_%d", targetLogs), func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_many_logs_%d/", targetLogs)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(1*KB))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			value := string(make([]byte, 512))
			for i := 0; i < targetLogs*2; i++ {
				if err := engine.Put(fmt.Sprintf("setup_key%d", i), value); err != nil {
					b.Fatal(err)
				}
			}

			for b.Loop() {
				_, _ = engine.Get("setup_key0")
			}
		})
	}
}

// BenchmarkReadLargeValues measures Get performance with large stored values
func BenchmarkReadLargeValues(b *testing.B) {
	sizes := []int{256, 1 * KB, 16 * KB, 64 * KB, 256 * KB, 1 * MB, 4 * MB}

	for _, size := range sizes {
		name := fmt.Sprintf("value_%dB", size)
		if size >= KB {
			name = fmt.Sprintf("value_%dKB", size/KB)
		}
		if size >= MB {
			name = fmt.Sprintf("value_%dMB", size/MB)
		}

		b.Run(name, func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_read_large_%d/", size)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(int64(size*10)))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			value := string(make([]byte, size))
			if err := engine.Put("largekey", value); err != nil {
				b.Fatal(err)
			}

			for b.Loop() {
				_, _ = engine.Get("largekey")
			}
		})
	}
}

// BenchmarkConcurrentReads measures read performance under concurrent load
func BenchmarkConcurrentReads(b *testing.B) {
	dataPath := "bench_concurrent_read/"
	if err := removeDir(dataPath); err != nil {
		b.Fatal(err)
	}
	defer removeDir(dataPath)

	engine, err := NewEngine(dataPath, WithMaxLogSize(100*MB))
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := string(make([]byte, 1*KB))
	for i := 0; i < 10000; i++ {
		if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
			b.Fatal(err)
		}
	}

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_, _ = engine.Get(fmt.Sprintf("key%d", i%10000))
			i++
		}
	})
}

// BenchmarkConcurrentWrites measures write performance under concurrent load
func BenchmarkConcurrentWrites(b *testing.B) {
	dataPath := "bench_concurrent_write/"
	if err := removeDir(dataPath); err != nil {
		b.Fatal(err)
	}
	defer removeDir(dataPath)

	engine, err := NewEngine(dataPath, WithMaxLogSize(1000*MB))
	if err != nil {
		b.Fatal(err)
	}
	defer engine.Close()

	value := string(make([]byte, 1*KB))

	b.RunParallel(func(pb *testing.PB) {
		i := 0
		for pb.Next() {
			_ = engine.Put(fmt.Sprintf("key%d", i), value)
			i++
		}
	})
}

// BenchmarkMixedReadWrite measures how writes impact concurrent read performance
func BenchmarkMixedReadWrite(b *testing.B) {
	// Test different read/write ratios
	ratios := []struct {
		name        string
		readPercent int
	}{
		{"reads_100pct", 100},
		{"reads_90pct", 90},
		{"reads_75pct", 75},
		{"reads_50pct", 50},
		{"reads_25pct", 25},
	}

	for _, ratio := range ratios {
		b.Run(ratio.name, func(b *testing.B) {
			dataPath := fmt.Sprintf("bench_mixed_%s/", ratio.name)
			if err := removeDir(dataPath); err != nil {
				b.Fatal(err)
			}
			defer removeDir(dataPath)

			engine, err := NewEngine(dataPath, WithMaxLogSize(1000*MB))
			if err != nil {
				b.Fatal(err)
			}
			defer engine.Close()

			// Populate initial data for reads
			value := string(make([]byte, 1*KB))
			for i := 0; i < 10000; i++ {
				if err := engine.Put(fmt.Sprintf("key%d", i), value); err != nil {
					b.Fatal(err)
				}
			}

			b.RunParallel(func(pb *testing.PB) {
				i := 0
				for pb.Next() {
					// Decide whether to read or write based on ratio
					if (i % 100) < ratio.readPercent {
						// Read operation
						_, _ = engine.Get(fmt.Sprintf("key%d", i%10000))
					} else {
						// Write operation
						_ = engine.Put(fmt.Sprintf("key%d", i%10000), value)
					}
					i++
				}
			})
		})
	}
}
