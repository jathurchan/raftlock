package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"

	"github.com/jathurchan/raftlock/types"
)

// mockFile implements the `file` interface for testing.
var _ file = (*mockFile)(nil)

// mockFileSystem implements the `fileSystem` for testing.
var _ fileSystem = (*mockFileSystem)(nil)

// failingReader implements `file` interface for testing.
var _ file = (*failingReader)(nil)

// mockSerializer implements the `serializer` for testing.
var _ serializer = (*mockSerializer)(nil)

// mockLogEntryReader implements the `logEntryReader` for testing.
var _ logEntryReader = (*mockLogEntryReader)(nil)

// mockIndexService implements the `indexService` for testing.
var _ indexService = (*mockIndexService)(nil)

type mockFile struct {
	*bytes.Reader
}

func (r *mockFile) ReadFull(buf []byte) (int, error) { return io.ReadFull(r.Reader, buf) }
func (r *mockFile) Close() error                     { return nil }
func (r *mockFile) ReadAll() ([]byte, error)         { return io.ReadAll(r.Reader) }

// mockFileSystem implements fileSystem for testing
type mockFileSystem struct {
	mu            sync.Mutex
	files         map[string][]byte
	openErr       error
	existsErr     error
	truncateErr   error
	writeFileErr  error
	renameErr     error
	mkdirErr      error
	removeErr     error
	globErr       error
	isNotExistErr bool  // Flag to simulate os.ErrNotExist on Exists error
	statErr       error // General stat error if needed separate from existsErr

	truncatedPath string
	truncatedSize int64
}

func newMockFileSystem() *mockFileSystem {
	return &mockFileSystem{
		files: make(map[string][]byte),
	}
}

func (mfs *mockFileSystem) ReadFile(name string) ([]byte, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mfs.openErr != nil {
		return nil, mfs.openErr
	}

	data, ok := mfs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	return data, nil
}

func (mfs *mockFileSystem) Open(name string) (file, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mfs.openErr != nil {
		return nil, mfs.openErr
	}
	data, ok := mfs.files[name]
	if !ok {
		return nil, os.ErrNotExist
	}

	return &mockFile{Reader: bytes.NewReader(data)}, nil
}

func (mfs *mockFileSystem) Exists(name string) (bool, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mfs.existsErr != nil {
		if mfs.isNotExistErr {
			return false, os.ErrNotExist // Simulate specific error type
		}
		return false, mfs.existsErr
	}
	_, exists := mfs.files[name]
	return exists, nil
}

const errInvalidArgument = "invalid argument" // for clarity

func (mfs *mockFileSystem) Truncate(name string, size int64) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	mfs.truncatedPath = name
	mfs.truncatedSize = size

	if mfs.truncateErr != nil {
		return mfs.truncateErr
	}

	data, ok := mfs.files[name]
	if !ok {
		return os.ErrNotExist
	}

	if size < 0 {
		return errors.New(errInvalidArgument)
	}

	if size > int64(len(data)) {
		diff := int(size) - len(data)
		mfs.files[name] = append(data, make([]byte, diff)...)
	} else {
		mfs.files[name] = data[:size]
	}
	return nil
}

func (mfs *mockFileSystem) WriteFile(name string, data []byte, perm os.FileMode) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if mfs.writeFileErr != nil {
		return mfs.writeFileErr
	}

	mfs.files[name] = data
	return nil
}

func (mfs *mockFileSystem) Remove(name string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if mfs.removeErr != nil {
		return mfs.removeErr
	}

	_, exists := mfs.files[name]
	if !exists {
		return os.ErrNotExist
	}

	delete(mfs.files, name)
	return nil
}

func (mfs *mockFileSystem) Rename(oldPath, newPath string) error {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if mfs.renameErr != nil {
		return mfs.renameErr
	}

	data, exists := mfs.files[oldPath]
	if !exists {
		return os.ErrNotExist
	}

	mfs.files[newPath] = data
	delete(mfs.files, oldPath)
	return nil
}

// Implement other methods as needed or with panics if unused
func (mfs *mockFileSystem) MkdirAll(path string, perm os.FileMode) error { return mfs.mkdirErr }
func (mfs *mockFileSystem) Dir(path string) string                       { return path } // Simplistic
func (mfs *mockFileSystem) IsNotExist(err error) bool                    { return errors.Is(err, os.ErrNotExist) }
func (mfs *mockFileSystem) Glob(pattern string) ([]string, error) {
	if mfs.globErr != nil {
		return nil, mfs.globErr
	}
	// This can be made smarter if needed
	return []string{}, nil
}
func (mfs *mockFileSystem) Join(elem ...string) string {
	panic("mockFileSystem.Join not implemented")
}

// Helper to reset truncation tracking
func (mfs *mockFileSystem) resetTruncate() {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	mfs.truncatedPath = ""
	mfs.truncatedSize = -1 // Use -1 to indicate not called
}

// failingReader simulates a reader that returns an error after a limited number of bytes.
type failingReader struct {
	reader      io.Reader
	err         error
	bytesToRead int
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.bytesToRead <= 0 {
		return 0, r.err
	}
	toRead := len(p)
	if toRead > r.bytesToRead {
		toRead = r.bytesToRead
	}
	n, err := r.reader.Read(p[:toRead])
	r.bytesToRead -= n
	if r.bytesToRead <= 0 {
		return n, r.err
	}
	return n, err
}

func (r *failingReader) Close() error { return nil }
func (r *failingReader) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek not implemented")
}
func (r *failingReader) ReadFull(buf []byte) (int, error) { return io.ReadFull(r, buf) }
func (r *failingReader) ReadAll() ([]byte, error)         { return io.ReadAll(r) }

type mockSerializer struct {
	unmarshalFunc       func([]byte) (types.LogEntry, error)
	marshalMetadataFunc func(logMetadata) ([]byte, error)
	calledWith          []byte
}

func (m *mockSerializer) UnmarshalLogEntry(data []byte) (types.LogEntry, error) {
	m.calledWith = data
	if m.unmarshalFunc != nil {
		return m.unmarshalFunc(data)
	}
	return types.LogEntry{}, errors.New("mockSerializer: unmarshalFunc not set")
}

func (m *mockSerializer) MarshalMetadata(metadata logMetadata) ([]byte, error) {
	if m.marshalMetadataFunc != nil {
		return m.marshalMetadataFunc(metadata)
	}
	return NewJsonSerializer().MarshalMetadata(metadata)
}

// All unused methods stubbed
func (m *mockSerializer) MarshalLogEntry(types.LogEntry) ([]byte, error)     { return nil, nil }
func (m *mockSerializer) UnmarshalMetadata([]byte) (logMetadata, error)      { return logMetadata{}, nil }
func (m *mockSerializer) MarshalState(types.PersistentState) ([]byte, error) { return nil, nil }
func (m *mockSerializer) UnmarshalState([]byte) (types.PersistentState, error) {
	return types.PersistentState{}, nil
}
func (m *mockSerializer) MarshalSnapshotMetadata(types.SnapshotMetadata) ([]byte, error) {
	return nil, nil
}
func (m *mockSerializer) UnmarshalSnapshotMetadata([]byte) (types.SnapshotMetadata, error) {
	return types.SnapshotMetadata{}, nil
}

// mockLogEntryReader implements logEntryReader for testing
type mockLogEntryReader struct {
	mu          sync.Mutex
	entries     []types.LogEntry
	readErrors  []error // Errors to return after corresponding entry/EOF
	bytesRead   []int64 // Bytes read for each corresponding entry
	readCallIdx int     // Tracks which entry/error to return next
}

func newMockLogEntryReader() *mockLogEntryReader {
	return &mockLogEntryReader{
		readCallIdx: 0,
	}
}

// AddEntry adds a successful entry read simulation
func (m *mockLogEntryReader) AddEntry(entry types.LogEntry, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, nil) // nil error for success
}

// AddError adds an error simulation for a read attempt
func (m *mockLogEntryReader) AddError(err error, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Add a dummy entry, it won't be used when error is non-nil
	m.entries = append(m.entries, types.LogEntry{})
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, err)
}

func (m *mockLogEntryReader) ReadNext(f file) (types.LogEntry, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.readCallIdx >= len(m.entries) {
		// If we've exhausted programmed entries/errors, assume EOF
		return types.LogEntry{}, 0, io.EOF
	}

	entry := m.entries[m.readCallIdx]
	bytes := m.bytesRead[m.readCallIdx]
	err := m.readErrors[m.readCallIdx]

	m.readCallIdx++

	// If we return an actual error (not nil, not EOF), the entry data is usually ignored by caller
	if err != nil && err != io.EOF {
		return types.LogEntry{}, bytes, err
	}

	// Return the programmed entry/EOF
	return entry, bytes, err
}

func createTestData(entryData []byte, prefixSize int) []byte {
	prefix := make([]byte, prefixSize)
	binary.BigEndian.PutUint32(prefix, uint32(len(entryData)))
	return append(prefix, entryData...)
}

type mockIndexService struct {
	getBoundsResult boundsResult
}

func (m *mockIndexService) Build(logPath string) (buildResult, error) {
	panic("mockIndexService.Build not implemented")
}

func (m *mockIndexService) VerifyConsistency(indexMap []types.IndexOffsetPair) error {
	panic("mockIndexService.VerifyConsistency not implemented")
}

func (m *mockIndexService) GetBounds(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
	return m.getBoundsResult
}
