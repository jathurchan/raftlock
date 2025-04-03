package storage

import (
	"io"
	"os"
)

// FileSystem is an interface for file system operations to enable mocking in tests
type FileSystem interface {
	Open(name string) (File, error)
	Exists(name string) (bool, error)
	Truncate(name string, size int64) error
}

// File is an interface for file operations to enable mocking in tests
type File interface {
	io.Reader
	io.Closer
	Seek(offset int64, whence int) (int64, error)
}

// OsFileSystem implements FileSystem using the OS's file operations
type OsFileSystem struct{}

// Open opens the named file
func (fs OsFileSystem) Open(name string) (File, error) {
	return os.Open(name)
}

// Exists checks if a file exists
func (fs OsFileSystem) Exists(name string) (bool, error) {
	_, err := os.Stat(name)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

// Truncate truncates the named file to size
func (fs OsFileSystem) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

// OsFile wraps *os.File to implement the File interface
type OsFile struct {
	*os.File
}
