package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

const (
	dataFileFormatSuffix = ".dat"
)

func validatePathFormat(path string) error {
	if path == "" || path[len(path)-1] != '/' {
		return fmt.Errorf("path is mandatory and should end with a /")
	}
	return nil
}

func ensureDataDirectoryExists(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.MkdirAll(path, 0o755); err != nil {
				return err
			} else {
				return nil
			}
		} else {
			return err
		}
	}
	if !stat.IsDir() {
		return fmt.Errorf("path is not a directory")
	}
	return nil
}

func validateWriteAccess(path string) error {
	testPath := filepath.Join(path, "test-access-file")
	testFile, err := os.OpenFile(testPath, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}

	_, err = testFile.WriteString("test")
	if err != nil {
		return err
	}

	err = testFile.Close()
	if err != nil {
		return err
	}

	err = os.Remove(testPath)
	if err != nil {
		return err
	}

	return nil
}

func validateDataPath(path string) error {
	if err := validatePathFormat(path); err != nil {
		return err
	}

	if err := ensureDataDirectoryExists(path); err != nil {
		return err
	}

	if err := validateWriteAccess(path); err != nil {
		return err
	}

	return nil
}

func extractDatafiles(path string) ([]string, error) {
	var dataFiles []string
	err := filepath.WalkDir(path, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if info, err := d.Info(); err != nil {
			return err
		} else {
			if info.Size() > 0 && filepath.Ext(path) == dataFileFormatSuffix {
				dataFiles = append(dataFiles, path)
			}
		}
		return nil
	})

	return dataFiles, err
}

func extractFileNumber(filename string) int {
	filepath.Base(filename)
	strings.TrimSuffix(filename, dataFileFormatSuffix)
	num, err := strconv.Atoi(filename)
	if err == nil {
		return num
	}

	return -1
}

func loadLogsFromDataFiles(filePaths []string) ([]*log, error) {
	sort.Slice(filePaths, func(i, j int) bool {
		return extractFileNumber(filePaths[i]) < extractFileNumber(filePaths[j])
	})
	logs := make([]*log, 0, len(filePaths))

	for _, dataFile := range filePaths {
		file, err := os.OpenFile(dataFile, os.O_RDONLY, 0o644)
		currentLog := &log{file: file, index: make(map[string]int64)}
		logs = append(logs, currentLog)
		if err != nil {
			return nil, err
		}

		keySizeBuffer := make([]byte, 4)
		if _, err := file.Read(keySizeBuffer); err != nil {
			return nil, err
		}

		keySize := binary.LittleEndian.Uint32(keySizeBuffer)
		keyBuffer := make([]byte, keySize)
		if _, err := file.Read(keyBuffer); err != nil {
			return nil, err
		}

		currentPosition, err := file.Seek(0, io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		currentLog.index[string(keyBuffer)] = currentPosition

		valueSizeBuffer := make([]byte, 4)
		if _, err := file.Read(valueSizeBuffer); err != nil {
			return nil, err
		}
		valueSize := binary.LittleEndian.Uint32(valueSizeBuffer)
		_, err = file.Seek(int64(valueSize), io.SeekCurrent)
		if err != nil {
			return nil, err
		}

		err = file.Close()
		if err != nil {
			return nil, err
		}
	}

	return logs, nil
}
