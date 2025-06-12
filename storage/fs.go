package storage

import (
	"errors"
	"io"
	"os"
	"path/filepath"
)

type fileStatFunc func(name string) (os.FileInfo, error)

// fileSystem defines an interface for file system operations.
// It allows mocking during testing by abstracting underlying file I/O operations.
type fileSystem interface {
	Stat(name string) (os.FileInfo, error)
	ReadFile(name string) ([]byte, error)
	Open(name string) (file, error)
	Exists(name string) (bool, error)
	Truncate(name string, size int64) error
	WriteFile(name string, data []byte, perm os.FileMode) error
	Rename(oldPath, newPath string) error
	MkdirAll(path string, perm os.FileMode) error
	Dir(path string) string
	Remove(name string) error
	IsNotExist(err error) bool
	Glob(pattern string) ([]string, error)
	Join(elem ...string) string
	Path(dirPath, name string) string
	TempPath(path string) string
	AtomicWrite(path string, data []byte, perm os.FileMode) error
	WriteMaybeAtomic(path string, data []byte, perm os.FileMode, atomic bool) error
	AppendFile(name string) (file, error)
}

// file defines an interface for file-level operations.
// It extends io.Reader and io.Closer, adding methods for more complete file reading functionality.
type file interface {
	io.Reader
	io.Closer
	Seek(offset int64, whence int) (int64, error)
	Write([]byte) (int, error)
	Sync() error
	ReadFull(buf []byte) (int, error)
	ReadAll() ([]byte, error)
}

// defaultFileSystem provides a concrete implementation of fileSystem
// using the standard library's os and filepath packages.
type defaultFileSystem struct {
	statFunc fileStatFunc
}

// newFileSystem returns a new defaultFileSystem using os.Stat.
func newFileSystem() fileSystem {
	return newFileSystemWithStat(os.Stat)
}

// newFileSystemWithStat returns a defaultFileSystem using the provided stat function.
// If the stat function is nil, os.Stat is used as the default.
func newFileSystemWithStat(stat fileStatFunc) fileSystem {
	if stat == nil {
		stat = os.Stat
	}
	return defaultFileSystem{statFunc: stat}
}

func (fs defaultFileSystem) Stat(name string) (os.FileInfo, error) {
	return fs.statFunc(name)
}

func (fs defaultFileSystem) Path(dirPath, name string) string { return fs.Join(dirPath, name) }
func (fs defaultFileSystem) TempPath(path string) string {
	return path + tmpSuffix
}

func (fs defaultFileSystem) WriteMaybeAtomic(
	path string,
	data []byte,
	perm os.FileMode,
	atomic bool,
) error {
	if atomic {
		return fs.AtomicWrite(path, data, perm)
	}
	return fs.WriteFile(path, data, perm)
}

// atomicWriteFile writes data to a temporary file and then atomically renames it to the target path.
// This ensures the file is never in a partially-written state.
func (fs defaultFileSystem) AtomicWrite(path string, data []byte, perm os.FileMode) error {
	return atomicWrite(fs, path, data, perm)
}

// ReadFile reads the contents of the specified file and returns the data.
func (fs defaultFileSystem) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

// Open opens the specified file and wraps it in a file interface.
func (fs defaultFileSystem) Open(name string) (file, error) {
	f, err := os.Open(name)
	if err != nil {
		return nil, err
	}
	return &defaultFile{File: f}, nil
}

// Exists reports whether the specified file exists.
func (fs defaultFileSystem) Exists(name string) (bool, error) {
	_, err := fs.statFunc(name)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	return false, err
}

// Truncate resizes the specified file to the given size.
func (fs defaultFileSystem) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

// WriteFile writes data to the specified file with the given permissions.
func (fs defaultFileSystem) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}

// Rename renames (moves) a file from oldPath to newPath.
func (fs defaultFileSystem) Rename(oldPath, newPath string) error {
	return os.Rename(oldPath, newPath)
}

// MkdirAll creates a directory and all necessary parents with the given permissions.
func (fs defaultFileSystem) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

// Dir returns the directory component of the given path.
func (fs defaultFileSystem) Dir(path string) string {
	return filepath.Dir(path)
}

// Remove deletes the specified file or directory.
func (fs defaultFileSystem) Remove(name string) error {
	return os.Remove(name)
}

// IsNotExist reports whether the error indicates a file or directory does not exist.
func (fs defaultFileSystem) IsNotExist(err error) bool {
	return errors.Is(err, os.ErrNotExist)
}

// Glob returns the names of all files matching the pattern.
func (fs defaultFileSystem) Glob(pattern string) ([]string, error) {
	return filepath.Glob(pattern)
}

// Join joins any number of path elements into a single path.
func (fs defaultFileSystem) Join(elem ...string) string {
	return filepath.Join(elem...)
}

// AppendFile opens the specified file in append mode (creating it if it does not exist)
// and returns a handle that implements the file interface. Data written will be appended
// to the end of the file.
func (fs defaultFileSystem) AppendFile(name string) (file, error) {
	f, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_APPEND, ownRWOthR)
	if err != nil {
		return nil, err
	}
	return &defaultFile{File: f}, nil
}

// defaultFile wraps *os.File to implement the file interface.
type defaultFile struct {
	*os.File
}

// ReadFull reads exactly len(buf) bytes into buf.
func (f *defaultFile) ReadFull(buf []byte) (int, error) {
	return io.ReadFull(f.File, buf)
}

// ReadAll reads the entire contents of the file.
func (f *defaultFile) ReadAll() ([]byte, error) {
	return io.ReadAll(f.File)
}
