package storage

import (
	"golang.org/x/sys/unix"
	"os"
)

const (
	lockFileName       = ".lock"
	testAccessFileName = "test-access-file"
)

func createFlock(path string) (*os.File, error) {
	lockFile, err := os.OpenFile(path+lockFileName, os.O_CREATE|os.O_RDONLY, 0o644)
	if err != nil {
		return nil, err
	}

	err = unix.Flock(int(lockFile.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err != nil {
		return nil, err
	}

	return lockFile, nil
}

func validateDataDirAccess(path string) error {
	testFile, err := os.OpenFile(path+testAccessFileName, os.O_CREATE|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	err = testFile.Close()
	if err != nil {
		return err
	}
	err = os.Remove(path + testAccessFileName)
	if err != nil {
		return err
	}
	return nil
}
