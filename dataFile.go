package storage

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
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

func ensureTrailingSlash(path string) string {
	return filepath.Clean(path) + string(filepath.Separator)
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

// extractDatafiles returns a list of data files in the given path
// it's not recursive, it only returns the files in the given path
func extractDatafiles(path string) ([]string, error) {
	var dataFiles []string
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}

	// Iterate over each entry and append if it's a data file
	for _, entry := range entries {
		if entry.IsDir() {
			continue // skip directories
		}
		// Check if the file is a data file
		if info, err := entry.Info(); err != nil {
			return nil, err
		} else {
			if info.Size() > 0 && filepath.Ext(entry.Name()) == dataFileFormatSuffix {
				dataFiles = append(dataFiles, filepath.Join(path, entry.Name()))
			}
		}
	}

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

func readDataFile(file *os.File) (string, error) {
	var size uint32
	err := binary.Read(file, binary.LittleEndian, &size)
	if err != nil {
		return "", err
	}

	dataBuffer := make([]byte, size)
	_, err = io.ReadFull(file, dataBuffer)
	if err != nil {
		return "", err
	}

	return string(dataBuffer), nil
}

func readAtDataFile(file *os.File, offset int64) (string, error) {
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}
	return readDataFile(file)
}

func openAndReadAtDataFile(path string, offset int64) (string, error) {
	file, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	value, err := readAtDataFile(file, offset)
	if err != nil {
		return "", err
	}

	return value, nil
}

func extractKeysFromDataFile(filePath string) ([]string, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	var keys []string
	for {
		// Read key size and key
		key, err := readDataFile(file)
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			return nil, fmt.Errorf("error reading key: %w", err)
		}

		keys = append(keys, key)

		// Read value size and skip the value
		_, err = readDataFile(file)
		if err == io.EOF {
			break // End of file reached
		}
		if err != nil {
			return nil, fmt.Errorf("error reading value: %w", err)
		}
	}
	return keys, nil
}
