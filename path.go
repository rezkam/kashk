package storage

import (
	"fmt"
	"os"
	"path/filepath"
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

func dataFileExists(path string) (bool, error) {
	exists := false
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
				exists = true
				return filepath.SkipDir
			}
		}
		return nil
	})
	return exists, err
}
