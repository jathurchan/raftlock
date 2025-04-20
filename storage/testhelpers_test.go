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
	"time"

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
	reader       io.Reader
	WriteFunc    func([]byte) (int, error)
	SyncFunc     func() error
	ReadFunc     func([]byte) (int, error)
	SeekFunc     func(int64, int) (int64, error)
	CloseFunc    func() error
	ReadFullFunc func([]byte) (int, error)
	ReadAllFunc  func() ([]byte, error)
}

func (r *mockFile) Read(p []byte) (int, error) {
	if r.ReadFunc != nil {
		return r.ReadFunc(p)
	}
	if r.Reader != nil {
		return r.Reader.Read(p)
	}
	return 0, errors.New("mockFile: no reader available")
}

func (r *mockFile) ReadFull(buf []byte) (int, error) {
	if r.ReadFullFunc != nil {
		return r.ReadFullFunc(buf)
	}
	return io.ReadFull(r.Reader, buf)
}

func (r *mockFile) ReadAll() ([]byte, error) {
	if r.ReadAllFunc != nil {
		return r.ReadAllFunc()
	}

	return io.ReadAll(r.Reader)
}

func (r *mockFile) Seek(offset int64, whence int) (int64, error) {
	if r.SeekFunc != nil {
		return r.SeekFunc(offset, whence)
	}
	if r.Reader != nil {
		return r.Reader.Seek(offset, whence)
	}
	return 0, errors.New("mockFile: no seeker available")
}

func (r *mockFile) Close() error {
	if r.CloseFunc != nil {
		return r.CloseFunc()
	}
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
	dir            string
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
	TruncateFunc         func(string, int64) error
	WriteFileFunc        func(string, []byte, os.FileMode) error
	RemoveFunc           func(string) error
	RenameFunc           func(string, string) error
	MkdirAllFunc         func(string, os.FileMode) error
	JoinFunc             func(...string) string
	IsNotExistFunc       func(error) bool
	GlobFunc             func(string) ([]string, error)
	TempPathFunc         func(string) string
	AtomicWriteFunc      func(string, []byte, os.FileMode) error
	WriteMaybeAtomicFunc func(string, []byte, os.FileMode, bool) error
	AppendFileFunc       func(string) (file, error)
	StatFunc             func(string) (os.FileInfo, error)
	DirFunc              func(string) string
}

func newMockFileSystem() *mockFileSystem {
	return &mockFileSystem{
		files: make(map[string][]byte),
	}
}

// defaultStat provides a basic Stat implementation for the mock.
func (mfs *mockFileSystem) defaultStat(name string) (os.FileInfo, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()

	if name == mfs.dir {
		return &mockFileInfo{nameVal: filepath.Base(name), isDir: true}, nil
	}

	data, ok := mfs.files[name]
	if ok {
		return &mockFileInfo{nameVal: filepath.Base(name), sizeVal: int64(len(data)), isDir: false}, nil
	}

	dirPrefix := name
	if !strings.HasSuffix(dirPrefix, string(filepath.Separator)) {
		dirPrefix += string(filepath.Separator)
	}
	for p := range mfs.files {
		if strings.HasPrefix(p, dirPrefix) {
			return &mockFileInfo{nameVal: filepath.Base(name), isDir: true}, nil
		}
	}

	return nil, os.ErrNotExist
}

func (mfs *mockFileSystem) Stat(name string) (os.FileInfo, error) {
	if mfs.StatFunc != nil {
		return mfs.StatFunc(name)
	}
	return mfs.defaultStat(name)
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
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy, nil
}

// defaultOpen provides a basic Open implementation for the mock.
// It creates the file in the map if it doesn't exist.
func (mfs *mockFileSystem) defaultOpen(name string) (file, error) {
	mfs.mu.Lock()
	defer mfs.mu.Unlock()
	if mfs.openErr != nil {
		return nil, mfs.openErr
	}
	data, ok := mfs.files[name]
	if !ok {
		mfs.files[name] = []byte{}
		data = mfs.files[name]
		ok = true
	}

	reader := bytes.NewReader(data)
	return &mockFile{
		Reader: reader,
		reader: reader,
		WriteFunc: func(p []byte) (int, error) {
			currentData := mfs.files[name]
			newData := append(currentData, p...)
			mfs.files[name] = newData
			n := len(p)
			return n, nil
		},
		ReadFunc: func(p []byte) (int, error) {
			return reader.Read(p)
		},
		SeekFunc: func(offset int64, whence int) (int64, error) {
			return reader.Seek(offset, whence)
		},
		SyncFunc:  func() error { return nil },
		CloseFunc: func() error { return nil },
	}, nil
}

func (mfs *mockFileSystem) Open(name string) (file, error) {
	if mfs.OpenFunc != nil {
		return mfs.OpenFunc(name)
	}
	return mfs.defaultOpen(name)
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
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	mfs.files[name] = dataCopy
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
		isDir := false
		dirPrefix := name
		if !strings.HasSuffix(dirPrefix, string(filepath.Separator)) {
			dirPrefix += string(filepath.Separator)
		}
		filesToRemove := []string{}
		for p := range mfs.files {
			if strings.HasPrefix(p, dirPrefix) {
				isDir = true
				filesToRemove = append(filesToRemove, p)
			}
		}
		if !isDir {
			return os.ErrNotExist
		}
		for _, f := range filesToRemove {
			delete(mfs.files, f)
		}
	} else {
		delete(mfs.files, name)
	}
	return nil
}

// defaultRename provides a basic Rename implementation for the mock.
func (mfs *mockFileSystem) defaultRename(oldPath, newPath string) error {
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

func (mfs *mockFileSystem) Rename(oldPath, newPath string) error {
	if mfs.RenameFunc != nil {
		return mfs.RenameFunc(oldPath, newPath)
	}
	return mfs.defaultRename(oldPath, newPath)
}

func (mfs *mockFileSystem) MkdirAll(path string, perm os.FileMode) error {
	if mfs.MkdirAllFunc != nil {
		return mfs.MkdirAllFunc(path, perm)
	}
	return mfs.mkdirAllErr
}

func (mfs *mockFileSystem) Dir(path string) string {
	if mfs.DirFunc != nil {
		return mfs.DirFunc(path)
	}
	return path
}

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
	marshalLogEntryFunc           func(types.LogEntry) ([]byte, error)
	unmarshalLogEntryFunc         func([]byte) (types.LogEntry, error)
	marshalMetadataFunc           func(logMetadata) ([]byte, error)
	unmarshalMetadataFunc         func([]byte) (logMetadata, error)
	marshalStateFunc              func(types.PersistentState) ([]byte, error)
	unmarshalStateFunc            func([]byte) (types.PersistentState, error)
	marshalSnapshotMetadataFunc   func(types.SnapshotMetadata) ([]byte, error)
	unmarshalSnapshotMetadataFunc func([]byte) (types.SnapshotMetadata, error)
	calledWith                    []byte
}

func (m *mockSerializer) MarshalLogEntry(entry types.LogEntry) ([]byte, error) {
	if m.marshalLogEntryFunc != nil {
		return m.marshalLogEntryFunc(entry)
	}
	return nil, errors.New("mockSerializer: marshalLogEntryFunc not set")
}

func (m *mockSerializer) UnmarshalLogEntry(data []byte) (types.LogEntry, error) {
	m.calledWith = data
	if m.unmarshalLogEntryFunc != nil {
		return m.unmarshalLogEntryFunc(data)
	}
	return types.LogEntry{}, errors.New("mockSerializer: unmarshalLogEntryFunc not set")
}

func (m *mockSerializer) MarshalMetadata(metadata logMetadata) ([]byte, error) {
	if m.marshalMetadataFunc != nil {
		return m.marshalMetadataFunc(metadata)
	}
	return newJsonSerializer().MarshalMetadata(metadata)
}

func (m *mockSerializer) UnmarshalMetadata(data []byte) (logMetadata, error) {
	if m.unmarshalMetadataFunc != nil {
		return m.unmarshalMetadataFunc(data)
	}
	return logMetadata{}, errors.New("mockSerializer: unmarshalMetadataFunc not set")
}

func (m *mockSerializer) MarshalState(state types.PersistentState) ([]byte, error) {
	if m.marshalStateFunc != nil {
		return m.marshalStateFunc(state)
	}
	return nil, errors.New("mockSerializer: marshalStateFunc not set")
}

func (m *mockSerializer) UnmarshalState(data []byte) (types.PersistentState, error) {
	if m.unmarshalStateFunc != nil {
		return m.unmarshalStateFunc(data)
	}
	return types.PersistentState{}, errors.New("mockSerializer: unmarshalStateFunc not set")
}

func (m *mockSerializer) MarshalSnapshotMetadata(metadata types.SnapshotMetadata) ([]byte, error) {
	if m.marshalSnapshotMetadataFunc != nil {
		return m.marshalSnapshotMetadataFunc(metadata)
	}
	return nil, errors.New("mockSerializer: marshalSnapshotMetadataFunc not set")
}

func (m *mockSerializer) UnmarshalSnapshotMetadata(data []byte) (types.SnapshotMetadata, error) {
	if m.unmarshalSnapshotMetadataFunc != nil {
		return m.unmarshalSnapshotMetadataFunc(data)
	}
	return types.SnapshotMetadata{}, errors.New("mockSerializer: unmarshalSnapshotMetadataFunc not set")
}

// mockLogEntryReader implements logEntryReader for testing
type mockLogEntryReader struct {
	mu          sync.Mutex
	entries     []types.LogEntry
	offsets     []int64
	bytesRead   []int64
	readErrors  []error
	readCallIdx int
}

func newMockLogEntryReader() *mockLogEntryReader {
	return &mockLogEntryReader{
		readCallIdx: 0,
	}
}

func (m *mockLogEntryReader) AddEntry(entry types.LogEntry, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, nil)
}

func (m *mockLogEntryReader) AddEntryWithOffset(entry types.LogEntry, bytes int64, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, entry)
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, nil)
	m.offsets = append(m.offsets, offset)
}

func (m *mockLogEntryReader) AddError(err error, bytes int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = append(m.entries, types.LogEntry{})
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, err)
}

func (m *mockLogEntryReader) AddErrorAtOffset(err error, bytes int64, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.entries = append(m.entries, types.LogEntry{})
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, err)
	m.offsets = append(m.offsets, offset)
}

func (m *mockLogEntryReader) AddEOF(entry types.LogEntry, bytes int64, offset int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.entries = append(m.entries, entry)
	m.bytesRead = append(m.bytesRead, bytes)
	m.readErrors = append(m.readErrors, io.EOF)
	m.offsets = append(m.offsets, offset)
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

	for i, off := range m.offsets {
		if off == offset {
			entry := m.entries[i]
			if expectedIndex != 0 && entry.Index != expectedIndex {
				return types.LogEntry{}, m.bytesRead[i], fmt.Errorf("%w: index mismatch (expected %d, got %d)", ErrCorruptedLog, expectedIndex, entry.Index)
			}
			return entry, m.bytesRead[i], m.readErrors[i]
		}
	}

	return types.LogEntry{}, 0, io.EOF // simulate missing offset
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

// mockIndexService implements indexService for testing.
type mockIndexService struct {
	BuildFunc             func(string) (buildResult, error)
	ReadInRangeFunc       func(context.Context, string, []types.IndexOffsetPair, types.Index, types.Index) ([]types.LogEntry, int64, error)
	VerifyConsistencyFunc func([]types.IndexOffsetPair) error
	GetBoundsFunc         func([]types.IndexOffsetPair, types.Index, types.Index) boundsResult
	AppendFunc            func([]types.IndexOffsetPair, []types.IndexOffsetPair) []types.IndexOffsetPair
	TruncateLastFunc      func([]types.IndexOffsetPair, int) []types.IndexOffsetPair
	FindFirstIndexFunc    func([]types.IndexOffsetPair, types.Index) int
	TruncateAfterFunc     func([]types.IndexOffsetPair, types.Index) []types.IndexOffsetPair
	TruncateBeforeFunc    func([]types.IndexOffsetPair, types.Index) []types.IndexOffsetPair

	getBoundsResult boundsResult
}

func (m *mockIndexService) Build(logPath string) (buildResult, error) {
	if m.BuildFunc != nil {
		return m.BuildFunc(logPath)
	}
	panic("mockIndexService.Build not implemented")
}

func (m *mockIndexService) ReadInRange(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
	if m.ReadInRangeFunc != nil {
		return m.ReadInRangeFunc(ctx, logPath, indexMap, start, end)
	}
	panic("mockIndexService.ReadInRange not implemented")
}

func (m *mockIndexService) VerifyConsistency(indexMap []types.IndexOffsetPair) error {
	if m.VerifyConsistencyFunc != nil {
		return m.VerifyConsistencyFunc(indexMap)
	}
	return nil
}

func (m *mockIndexService) GetBounds(indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index) boundsResult {
	if m.GetBoundsFunc != nil {
		return m.GetBoundsFunc(indexMap, currentFirst, currentLast)
	}
	return m.getBoundsResult
}

func (m *mockIndexService) Append(base, additions []types.IndexOffsetPair) []types.IndexOffsetPair {
	if m.AppendFunc != nil {
		return m.AppendFunc(base, additions)
	}
	return append(base, additions...)
}

func (m *mockIndexService) TruncateLast(indexMap []types.IndexOffsetPair, count int) []types.IndexOffsetPair {
	if m.TruncateLastFunc != nil {
		return m.TruncateLastFunc(indexMap, count)
	}
	if count <= 0 {
		return indexMap
	}
	if count >= len(indexMap) {
		return []types.IndexOffsetPair{}
	}
	return indexMap[:len(indexMap)-count]
}

func (m *mockIndexService) FindFirstIndexAtOrAfter(indexMap []types.IndexOffsetPair, target types.Index) int {
	if m.FindFirstIndexFunc != nil {
		return m.FindFirstIndexFunc(indexMap, target)
	}
	panic("mockIndexService.FindFirstIndexAtOrAfter not implemented")
}

func (m *mockIndexService) TruncateAfter(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
	if m.TruncateAfterFunc != nil {
		return m.TruncateAfterFunc(indexMap, target)
	}
	panic("mockIndexService.TruncateAfter not implemented")
}

func (m *mockIndexService) TruncateBefore(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
	if m.TruncateBeforeFunc != nil {
		return m.TruncateBeforeFunc(indexMap, target)
	}
	panic("mockIndexService.TruncateBefore not implemented")
}

// mockMetadataService implements metadataService for testing.
type mockMetadataService struct {
	loadError        error
	saveError        error
	syncError        error
	validateError    error
	savedPath        string
	savedMetadata    logMetadata
	savedAtomicWrite bool
	saveCalled       bool

	LoadMetadataFunc             func(path string) (logMetadata, error)
	SaveMetadataFunc             func(path string, metadata logMetadata, useAtomicWrite bool) error
	SyncMetadataFromIndexMapFunc func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error)
	ValidateMetadataRangeFunc    func(firstIndex, lastIndex types.Index) error
}

func (mms *mockMetadataService) LoadMetadata(path string) (logMetadata, error) {
	if mms.LoadMetadataFunc != nil {
		return mms.LoadMetadataFunc(path)
	}
	return logMetadata{}, mms.loadError
}
func (mms *mockMetadataService) SaveMetadata(path string, metadata logMetadata, useAtomicWrite bool) error {
	mms.saveCalled = true
	mms.savedPath = path
	mms.savedMetadata = metadata
	mms.savedAtomicWrite = useAtomicWrite
	if mms.SaveMetadataFunc != nil {
		return mms.SaveMetadataFunc(path, metadata, useAtomicWrite)
	}
	return mms.saveError
}
func (mms *mockMetadataService) SyncMetadataFromIndexMap(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error) {
	if mms.SyncMetadataFromIndexMapFunc != nil {
		return mms.SyncMetadataFromIndexMapFunc(path, indexMap, currentFirst, currentLast, context, useAtomicWrite)
	}
	return 0, 0, mms.syncError
}
func (mms *mockMetadataService) ValidateMetadataRange(firstIndex, lastIndex types.Index) error {
	if mms.ValidateMetadataRangeFunc != nil {
		return mms.ValidateMetadataRangeFunc(firstIndex, lastIndex)
	}
	return mms.validateError
}

// mockSystemInfo implements systemInfo for testing.
type mockSystemInfo struct {
	PIDFunc          func() int
	HostnameFunc     func() string
	NowUnixMilliFunc func() int64
}

func (m *mockSystemInfo) PID() int {
	if m.PIDFunc != nil {
		return m.PIDFunc()
	}
	return 12345
}
func (m *mockSystemInfo) Hostname() string {
	if m.HostnameFunc != nil {
		return m.HostnameFunc()
	}
	return "mock-host"
}
func (m *mockSystemInfo) NowUnixMilli() int64 {
	if m.NowUnixMilliFunc != nil {
		return m.NowUnixMilliFunc()
	}
	return 1678886400000 // Default fixed time (approx Mar 15 2023)
}

type mockFileInfo struct {
	nameVal string
	sizeVal int64
	modeVal os.FileMode
	modtVal time.Time
	isDir   bool
	sysVal  any
}

func (m *mockFileInfo) Name() string       { return m.nameVal }
func (m *mockFileInfo) Size() int64        { return m.sizeVal }
func (m *mockFileInfo) Mode() os.FileMode  { return m.modeVal }
func (m *mockFileInfo) ModTime() time.Time { return m.modtVal }
func (m *mockFileInfo) IsDir() bool        { return m.isDir }
func (m *mockFileInfo) Sys() any           { return m.sysVal }
