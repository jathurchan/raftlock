package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// mockMetadataService implements the `metadataService` for testing.
var _ metadataService = (*mockMetadataService)(nil)

// mockSystemInfo implements the `systemInfo` for testing.
var _ systemInfo = (*mockSystemInfo)(nil)

type mockFile struct {
	*bytes.Reader

	ReadAllFunc func() ([]byte, error)
	WriteFunc   func([]byte) (int, error)
	SyncFunc    func() error
}

func (r *mockFile) ReadFull(buf []byte) (int, error) {
	return io.ReadFull(r.Reader, buf)
}

func (r *mockFile) ReadAll() ([]byte, error) {
	if r.ReadAllFunc != nil {
		return r.ReadAllFunc()
	}

	return io.ReadAll(r.Reader)
}

func (r *mockFile) Seek(offset int64, whence int) (int64, error) {
	return r.Reader.Seek(offset, whence)
}

func (r *mockFile) Close() error {
	return nil
}

func (r *mockFile) Write(p []byte) (int, error) {
	if r.WriteFunc != nil {
		return r.WriteFunc(p)
	}
	// default: pretend write succeeded but don't store data
	return len(p), nil
}

func (r *mockFile) Sync() error {
	if r.SyncFunc != nil {
		return r.SyncFunc()
	}
	return nil
}

// mockFileSystem implements fileSystem for testing
type mockFileSystem struct {
	mu             sync.Mutex
	files          map[string][]byte
	openErr        error
	existsErr      error
	truncateErr    error
	writeFileErr   error
	renameErr      error
	mkdirAllErr    error
	removeErr      error
	globErr        error
	isNotExistErr  bool
	atomicWriteErr error

	truncatedPath string
	truncatedSize int64

	ExistsFunc           func(string) (bool, error)
	OpenFunc             func(string) (file, error)
	ReadFileFunc         func(string) ([]byte, error)
	TruncateFunc         func(name string, size int64) error
	WriteFileFunc        func(string, []byte, os.FileMode) error
	RemoveFunc           func(string) error
	RenameFunc           func(string, string) error
	MkdirAllFunc         func(string, os.FileMode) error
	JoinFunc             func(...string) string
	IsNotExistFunc       func(error) bool
	GlobFunc             func(string) ([]string, error)
	TempPathFunc         func(string) string
	AtomicWriteFunc      func(path string, data []byte, perm os.FileMode) error
	WriteMaybeAtomicFunc func(path string, data []byte, perm os.FileMode, atomic bool) error
	AppendFileFunc       func(name string) (file, error)
}

func newMockFileSystem() *mockFileSystem {
	return &mockFileSystem{
		files: make(map[string][]byte),
	}
}

func (mfs *mockFileSystem) WriteMaybeAtomic(path string, data []byte, perm os.FileMode, atomic bool) error {
	if mfs.WriteMaybeAtomicFunc != nil {
		return mfs.WriteMaybeAtomicFunc(path, data, perm, atomic)
	}
	if atomic {
		if mfs.atomicWriteErr != nil {
			return mfs.atomicWriteErr
		}
		mfs.files[path] = data
		return nil
	} else {
		return mfs.WriteFile(path, data, perm)
	}
}

func (mfs *mockFileSystem) Path(dirPath, name string) string {
	return mfs.Join(dirPath, name)
}

func (mfs *mockFileSystem) TempPath(path string) string {
	if mfs.TempPathFunc != nil {
		return mfs.TempPathFunc(path)
	}
	return path + ".tmp"
}

func (mfs *mockFileSystem) AtomicWrite(path string, data []byte, perm os.FileMode) error {
	if mfs.AtomicWriteFunc != nil {
		return mfs.AtomicWriteFunc(path, data, perm)
	}

	return atomicWrite(mfs, path, data, perm)
}

func (mfs *mockFileSystem) ReadFile(name string) ([]byte, error) {
	if mfs.ReadFileFunc != nil {
		return mfs.ReadFileFunc(name)
	}
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
	if mfs.OpenFunc != nil {
		return mfs.OpenFunc(name)
	}
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
	if mfs.ExistsFunc != nil {
		return mfs.ExistsFunc(name)
	}
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mfs.existsErr != nil {
		if mfs.isNotExistErr {
			return false, os.ErrNotExist
		}
		return false, mfs.existsErr
	}
	_, exists := mfs.files[name]
	return exists, nil
}

func (mfs *mockFileSystem) Truncate(name string, size int64) error {
	if mfs.TruncateFunc != nil {
		return mfs.TruncateFunc(name, size)
	}
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
		return errors.New("invalid argument")
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
	if mfs.WriteFileFunc != nil {
		return mfs.WriteFileFunc(name, data, perm)
	}
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mfs.writeFileErr != nil {
		return mfs.writeFileErr
	}
	if mfs.files == nil {
		return errors.New("mockFileSystem.files map is nil — use newMockFileSystem()")
	}
	mfs.files[name] = data
	return nil
}

func (mfs *mockFileSystem) Remove(name string) error {
	if mfs.RemoveFunc != nil {
		return mfs.RemoveFunc(name)
	}
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
	if mfs.RenameFunc != nil {
		return mfs.RenameFunc(oldPath, newPath)
	}
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

func (mfs *mockFileSystem) MkdirAll(path string, perm os.FileMode) error {
	if mfs.MkdirAllFunc != nil {
		return mfs.MkdirAllFunc(path, perm)
	}
	return mfs.mkdirAllErr
}

func (mfs *mockFileSystem) Dir(path string) string { return path }

func (mfs *mockFileSystem) IsNotExist(err error) bool {
	if mfs.IsNotExistFunc != nil {
		return mfs.IsNotExistFunc(err)
	}
	return errors.Is(err, os.ErrNotExist)
}

func (mfs *mockFileSystem) Glob(pattern string) ([]string, error) {
	if mfs.GlobFunc != nil {
		return mfs.GlobFunc(pattern)
	}
	if mfs.globErr != nil {
		return nil, mfs.globErr
	}
	var matches []string
	for name := range mfs.files {
		if strings.Contains(name, pattern) {
			matches = append(matches, name)
		}
	}
	return matches, nil
}

func (mfs *mockFileSystem) Join(elem ...string) string {
	if mfs.JoinFunc != nil {
		return mfs.JoinFunc(elem...)
	}
	return filepath.Join(elem...)
}

func (mfs *mockFileSystem) resetTruncate() {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	mfs.truncatedPath = ""
	mfs.truncatedSize = -1
}

func (mfs *mockFileSystem) AppendFile(name string) (file, error) {
	if mfs.AppendFileFunc != nil {
		return mfs.AppendFileFunc(name)
	}
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	data := mfs.files[name] // returns nil if not exists, which is okay
	writer := &bytes.Buffer{}
	if data != nil {
		writer.Write(data)
	}

	mockF := &mockFile{
		Reader: bytes.NewReader(data),
		WriteFunc: func(p []byte) (int, error) {
			n, err := writer.Write(p)
			mfs.files[name] = writer.Bytes()
			return n, err
		},
	}

	return mockF, nil
}

// failingReader simulates a reader that returns an error after a limited number of bytes.
type failingReader struct {
	reader      io.Reader
	err         error
	bytesToRead int
	failReadAll bool
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

func (r *failingReader) Close() error {
	return nil
}
func (r *failingReader) Seek(offset int64, whence int) (int64, error) {
	return 0, errors.New("seek not implemented")
}
func (r *failingReader) ReadFull(buf []byte) (int, error) {
	return io.ReadFull(r, buf)
}
func (r *failingReader) ReadAll() ([]byte, error) {
	if r.failReadAll {
		return nil, r.err
	}
	return io.ReadAll(r.reader) // Use r.reader to avoid recursive calls
}

func (r *failingReader) Write(p []byte) (int, error) {
	return 0, errors.New("write not supported")
}

func (r *failingReader) Sync() error {
	return nil // no-op for test
}

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
	return newJsonSerializer().MarshalMetadata(metadata)
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

	m.entries = append(m.entries, types.LogEntry{})
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, err)
}

func (m *mockLogEntryReader) ReadNext(f file) (types.LogEntry, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.readCallIdx >= len(m.entries) {
		return types.LogEntry{}, 0, io.EOF
	}

	entry := m.entries[m.readCallIdx]
	bytes := m.bytesRead[m.readCallIdx]
	err := m.readErrors[m.readCallIdx]

	m.readCallIdx++

	if err != nil && err != io.EOF {
		return types.LogEntry{}, bytes, err
	}

	return entry, bytes, err
}

func (m *mockLogEntryReader) ReadAtOffset(file file, offset int64, expectedIndex types.Index) (types.LogEntry, int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if offset < 0 || int(offset) >= len(m.entries) {
		return types.LogEntry{}, 0, io.EOF
	}

	entry := m.entries[offset]
	if expectedIndex != 0 && entry.Index != expectedIndex {
		return types.LogEntry{}, m.bytesRead[offset], fmt.Errorf("%w: index mismatch (expected %d, got %d)", ErrCorruptedLog, expectedIndex, entry.Index)
	}

	return entry, m.bytesRead[offset], m.readErrors[offset]
}

func (m *mockLogEntryReader) ScanRange(ctx context.Context, file file, start, end types.Index) ([]types.LogEntry, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []types.LogEntry
	for _, entry := range m.entries {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}

		if entry.Index >= start && entry.Index < end {
			result = append(result, entry)
		}
	}

	return result, nil
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

func (m *mockIndexService) ReadInRange(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
	panic("mockIndexService.ReadInRange not implemented")
}

func (m *mockIndexService) VerifyConsistency(indexMap []types.IndexOffsetPair) error {
	panic("mockIndexService.VerifyConsistency not implemented")
}

func (m *mockIndexService) GetBounds(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
	return m.getBoundsResult
}

func (m *mockIndexService) Append(base, additions []types.IndexOffsetPair) []types.IndexOffsetPair {
	panic("mockIndexService.AppendEntries not implemented")
}

func (m *mockIndexService) TruncateLast(indexMap []types.IndexOffsetPair, count int) []types.IndexOffsetPair {
	panic("mockIndexService.TruncateLastEntries not implemented")
}

func (m *mockIndexService) FindFirstIndexAtOrAfter(indexMap []types.IndexOffsetPair, target types.Index) int {
	panic("mockIndexService.FindFirstIndexAtOrAfter not implemented")
}

func (m *mockIndexService) TruncateAfter(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
	panic("mockIndexService.TruncateAfter not implemented")
}

func (m *mockIndexService) TruncateBefore(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
	panic("mockIndexService.TruncateBefore not implemented")
}

type mockMetadataService struct {
	loadError        error
	saveError        error
	syncError        error
	validateError    error
	savedPath        string
	savedMetadata    logMetadata
	savedAtomicWrite bool
	saveCalled       bool

	SaveMetadataFunc func(path string, metadata logMetadata, useAtomicWrite bool) error
}

func (mms *mockMetadataService) LoadMetadata(path string) (logMetadata, error) {
	return logMetadata{}, mms.loadError
}
func (mms *mockMetadataService) SaveMetadata(path string, metadata logMetadata, useAtomicWrite bool) error {
	if mms.SaveMetadataFunc != nil {
		return mms.SaveMetadataFunc(path, metadata, useAtomicWrite)
	}

	mms.saveCalled = true
	mms.savedPath = path
	mms.savedMetadata = metadata
	mms.savedAtomicWrite = useAtomicWrite
	return mms.saveError
}
func (mms *mockMetadataService) SyncMetadataFromIndexMap(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error) {
	return 0, 0, mms.syncError
}
func (mms *mockMetadataService) ValidateMetadataRange(firstIndex, lastIndex types.Index) error {
	return mms.validateError
}

type mockSystemInfo struct {
	PIDFunc          func() int
	HostnameFunc     func() string
	NowUnixMilliFunc func() int64
}

func (m *mockSystemInfo) PID() int            { return m.PIDFunc() }
func (m *mockSystemInfo) Hostname() string    { return m.HostnameFunc() }
func (m *mockSystemInfo) NowUnixMilli() int64 { return m.NowUnixMilliFunc() }
