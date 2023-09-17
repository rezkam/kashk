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

func readKey(file *os.File, offset int64) (string, error) {
	_, err := file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	keySizeBuffer := make([]byte, 4)
	_, err = file.Read(keySizeBuffer)
	if err != nil {
		return "", err
	}

	keySize := binary.LittleEndian.Uint32(keySizeBuffer)
	keyBuffer := make([]byte, keySize)
	_, err = file.Read(keyBuffer)
	if err != nil {
		return "", err
	}

	return string(keyBuffer), nil
}

func readValue(filePath string, offset int64, tombStone string) (string, error) {
	file, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return "", err
	}
	defer file.Close()

	_, err = file.Seek(offset, io.SeekStart)
	if err != nil {
		return "", err
	}

	valueSizeBuffer := make([]byte, 4)
	_, err = file.Read(valueSizeBuffer)
	if err != nil {
		return "", err
	}

	keySize := binary.LittleEndian.Uint32(valueSizeBuffer)
	valueBuffer := make([]byte, keySize)
	_, err = file.Read(valueBuffer)
	if err != nil {
		return "", err
	}

	value := string(valueBuffer)

	if value == tombStone {
		return "", fmt.Errorf("key not found")
	}

	return value, nil
}
