package storage

import (
	"io"
	"os"
	"sort"
)

// log represents the data and index for the storage engine
type readLog struct {
	path  string
	index map[string]int64
}

type writeLog struct {
	file  *os.File
	index map[string]int64
	size  int64
}

func initReadLogs(paths []string) ([]*readLog, error) {
	sort.Slice(paths, func(i, j int) bool {
		return extractFileNumber(paths[i]) < extractFileNumber(paths[j])
	})
	logs := make([]*readLog, 0, len(paths))
	for _, path := range paths {
		log, err := extractReadLog(path)
		if err != nil {
			return nil, err
		}
		logs = append(logs, log)
	}

	return logs, nil
}

func extractReadLog(path string) (*readLog, error) {
	log := &readLog{
		path:  path,
		index: make(map[string]int64),
	}

	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	for {
		key, err := readDataFile(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		endOffset, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}
		log.index[key] = endOffset

		// Intentionally reading value to move the file cursor to the next key
		_, err = readDataFile(file)
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
	}
	return log, nil
}
