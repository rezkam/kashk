package storage

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidatePathFormat(t *testing.T) {
	tests := []struct {
		path   string
		hasErr bool
	}{
		{"", true},
		{"path", true},
		{"path/", false},
	}

	for _, test := range tests {
		err := validatePathFormat(test.path)
		if test.hasErr {
			assert.Error(t, err, "Expected an error for path '%s'", test.path)
		} else {
			assert.NoError(t, err, "Expected no error for path '%s'", test.path)
		}
	}
}

func TestEnsureDataDirectoryExists(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_storage")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	path := filepath.Join(tempDir, "data/")
	err = ensureDataDirectoryExists(path)
	require.NoError(t, err, "Failed to ensure directory exists: %v", err)

	_, err = os.Stat(path)
	require.NoError(t, err, "Directory was not created: %v", err)

	assert.True(t, isDir(path), "Path is not a directory")
}

func TestValidateWriteAccess(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_storage")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	err = validateWriteAccess(tempDir + "/")
	assert.NoError(t, err, "Failed to test write access: %v", err)
}

func TestDataFileExists(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_storage")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dataFilePath := filepath.Join(tempDir, "data"+dataFileFormatSuffix)
	dataFile, err := os.OpenFile(dataFilePath, os.O_CREATE|os.O_WRONLY, 0o644)
	_, err = dataFile.Write([]byte("test"))
	require.NoError(t, err, "Failed to write to test .dat file: %v", err)

	exists, err := dataFileExists(tempDir)
	require.NoError(t, err, "Failed to check if data file exists: %v", err)
	assert.True(t, exists, "Expected data file to exist")
}

func TestDataFileNotExists(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "test_storage")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	dataFilePath := filepath.Join(tempDir, "data"+dataFileFormatSuffix)
	_, err = os.Create(dataFilePath)
	require.NoError(t, err, "Failed to create test .dat file: %v", err)

	exists, err := dataFileExists(tempDir)
	require.NoError(t, err, "Failed to check if data file exists: %v", err)
	assert.False(t, exists, "Expected data file to exist")
}

func isDir(path string) bool {
	stat, err := os.Stat(path)
	if err != nil {
		return false
	}
	return stat.IsDir()
}
