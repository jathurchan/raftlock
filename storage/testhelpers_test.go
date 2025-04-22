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
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
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

// mockRecoveryService implements the `recoveryService` for testing.
var _ recoveryService = (*mockRecoveryService)(nil)

// mockLogAppender implements the `logAppender` for testing.
var _ logAppender = (*mockLogAppender)(nil)

// mockLogRewriter implements the `logRewriter` for testing.
var _ logRewriter = (*mockLogRewriter)(nil)

// mockSnapshotWriter implements the `snapshotWriter` for testing.
var _ snapshotWriter = (*mockSnapshotWriter)(nil)

// mockSnapshotReader implements the `snapshotReader` for testing.
var _ snapshotReader = (*mockSnapshotReader)(nil)

// mockRWOperationLocker implements the `rwOperationLocker` for testing.
var _ rwOperationLocker = (*mockRWOperationLocker)(nil)

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
	if mfs.mkdirAllErr != nil {
		return mfs.mkdirAllErr
	}
	return nil
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
	toRead := min(len(p), r.bytesToRead)
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
	mu            sync.Mutex
	entries       []types.LogEntry
	offsets       []int64
	bytesRead     []int64
	readErrors    []error
	readCallIdx   int
	ScanRangeFunc func(context.Context, file, types.Index, types.Index) ([]types.LogEntry, error)
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
	if m.ScanRangeFunc != nil {
		return m.ScanRangeFunc(ctx, file, start, end)
	}
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

// mockRecoveryService implements recoveryService for testing
type mockRecoveryService struct {
	checkForMarkersErr           error
	performRecoveryErr           error
	createMarkerErr              error
	cleanupTempFilesErr          error
	removeMarkerErr              error
	createSnapshotMarkerErr      error
	updateSnapshotMarkerErr      error
	removeSnapshotMarkerErr      error
	recoveryNeeded               bool
	checkForMarkersCalled        bool
	performRecoveryCalled        bool
	createMarkerCalled           bool
	cleanupTempFilesCalled       bool
	removeMarkerCalled           bool
	createSnapshotMarkerCalled   bool
	updateSnapshotMarkerCalled   bool
	removeSnapshotMarkerCalled   bool
	lastUpdateSnapshotMarkerArgs string
	lastSnapshotMetadata         types.SnapshotMetadata
}

func (m *mockRecoveryService) CheckForRecoveryMarkers() (bool, error) {
	m.checkForMarkersCalled = true
	return m.recoveryNeeded, m.checkForMarkersErr
}

func (m *mockRecoveryService) PerformRecovery() error {
	m.performRecoveryCalled = true
	return m.performRecoveryErr
}

func (m *mockRecoveryService) CreateRecoveryMarker() error {
	m.createMarkerCalled = true
	return m.createMarkerErr
}

func (m *mockRecoveryService) CleanupTempFiles() error {
	m.cleanupTempFilesCalled = true
	return m.cleanupTempFilesErr
}

func (m *mockRecoveryService) RemoveRecoveryMarker() error {
	m.removeMarkerCalled = true
	return m.removeMarkerErr
}

func (m *mockRecoveryService) CreateSnapshotRecoveryMarker(metadata types.SnapshotMetadata) error {
	m.createSnapshotMarkerCalled = true
	m.lastSnapshotMetadata = metadata
	return m.createSnapshotMarkerErr
}

func (m *mockRecoveryService) UpdateSnapshotMarkerStatus(status string) error {
	m.updateSnapshotMarkerCalled = true
	m.lastUpdateSnapshotMarkerArgs = status
	return m.updateSnapshotMarkerErr
}

func (m *mockRecoveryService) RemoveSnapshotRecoveryMarker() error {
	m.removeSnapshotMarkerCalled = true
	return m.removeSnapshotMarkerErr
}

// mockLogAppender implements logAppender for testing
type mockLogAppender struct {
	appendErr  error
	appendArgs struct {
		ctx          context.Context
		entries      []types.LogEntry
		currentLast  types.Index
		callCount    int
		lastEntries  []types.LogEntry
		lastCurrent  types.Index
		allCalledCtx []context.Context
	}
	appendResult appendResult
}

func (m *mockLogAppender) Append(ctx context.Context, entries []types.LogEntry, currentLast types.Index) (appendResult, error) {
	m.appendArgs.ctx = ctx
	m.appendArgs.callCount++
	m.appendArgs.lastEntries = entries
	m.appendArgs.lastCurrent = currentLast
	m.appendArgs.allCalledCtx = append(m.appendArgs.allCalledCtx, ctx)
	return m.appendResult, m.appendErr
}

// mockLogRewriter implements logRewriter for testing
type mockLogRewriter struct {
	rewriteFunc func(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error)
	rewriteCall struct {
		ctx       context.Context
		entries   []types.LogEntry
		callCount int
	}
}

func (m *mockLogRewriter) Rewrite(ctx context.Context, entries []types.LogEntry) ([]types.IndexOffsetPair, error) {
	m.rewriteCall.ctx = ctx
	m.rewriteCall.entries = entries
	m.rewriteCall.callCount++

	if m.rewriteFunc != nil {
		return m.rewriteFunc(ctx, entries)
	}

	// Default: return one offset pair per entry
	pairs := make([]types.IndexOffsetPair, len(entries))
	for i, entry := range entries {
		pairs[i] = types.IndexOffsetPair{
			Index:  entry.Index,
			Offset: int64(i * 100), // Mock offset
		}
	}
	return pairs, nil
}

// mockSnapshotWriter implements snapshotWriter for testing
type mockSnapshotWriter struct {
	writeErr  error
	writeCall struct {
		ctx       context.Context
		metadata  types.SnapshotMetadata
		data      []byte
		callCount int
	}
}

func (m *mockSnapshotWriter) Write(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	m.writeCall.ctx = ctx
	m.writeCall.metadata = metadata
	m.writeCall.data = data
	m.writeCall.callCount++
	return m.writeErr
}

// mockSnapshotReader implements snapshotReader for testing
type mockSnapshotReader struct {
	readErr    error
	readResult struct {
		metadata types.SnapshotMetadata
		data     []byte
	}
	readCall struct {
		ctx       context.Context
		callCount int
	}
}

func (m *mockSnapshotReader) Read(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	m.readCall.ctx = ctx
	m.readCall.callCount++
	return m.readResult.metadata, m.readResult.data, m.readErr
}

// mockStorageDependencies holds all the mock dependencies needed for FileStorage tests
type mockStorageDependencies struct {
	fs             *mockFileSystem
	serializer     *mockSerializer
	logAppender    *mockLogAppender
	logReader      *mockLogEntryReader
	logRewriter    *mockLogRewriter
	indexSvc       *mockIndexService
	metadataSvc    *mockMetadataService
	recoverySvc    *mockRecoveryService
	snapshotWriter *mockSnapshotWriter
	snapshotReader *mockSnapshotReader
	logger         *logger.NoOpLogger
}

// newMockStorageDependencies creates a new set of mock dependencies for testing
func newMockStorageDependencies() *mockStorageDependencies {
	return &mockStorageDependencies{
		fs:             newMockFileSystem(),
		serializer:     &mockSerializer{},
		logAppender:    &mockLogAppender{},
		logReader:      newMockLogEntryReader(),
		logRewriter:    &mockLogRewriter{},
		indexSvc:       &mockIndexService{},
		metadataSvc:    &mockMetadataService{},
		recoverySvc:    &mockRecoveryService{},
		snapshotWriter: &mockSnapshotWriter{},
		snapshotReader: &mockSnapshotReader{},
		logger:         &logger.NoOpLogger{},
	}
}

// createFileStorageWithMocks creates a new FileStorage with the provided mock dependencies
func createFileStorageWithMocks(cfg StorageConfig, options FileStorageOptions, deps *mockStorageDependencies) (*FileStorage, error) {
	s := &FileStorage{
		dir:            cfg.Dir,
		options:        options,
		fileSystem:     deps.fs,
		serializer:     deps.serializer,
		logAppender:    deps.logAppender,
		logReader:      deps.logReader,
		logRewriter:    deps.logRewriter,
		indexSvc:       deps.indexSvc,
		metadataSvc:    deps.metadataSvc,
		recoverySvc:    deps.recoverySvc,
		snapshotWriter: deps.snapshotWriter,
		snapshotReader: deps.snapshotReader,
		logger:         deps.logger,
	}

	// Initialize atomic values
	s.status.Store(storageStatusReady)

	// Initialize mutexes
	s.logLocker = newRWOperationLocker(
		&s.logMu,
		deps.logger,
		options,
		&s.metrics.slowOperations,
	)

	s.snapshotLocker = newRWOperationLocker(
		&s.snapshotMu,
		deps.logger,
		options,
		&s.metrics.slowOperations,
	)

	return s, nil
}

// checkStorage validates FileStorage behavior in tests
func checkStorage(t *testing.T, s *FileStorage, expectedFirst, expectedLast types.Index, expectedStatus storageStatus) {
	t.Helper()

	actualFirst := s.FirstLogIndex()
	actualLast := s.LastLogIndex()
	actualStatus := s.status.Load().(storageStatus)

	testutil.AssertEqual(t, expectedFirst, actualFirst, "FirstLogIndex mismatch")
	testutil.AssertEqual(t, expectedLast, actualLast, "LastLogIndex mismatch")
	testutil.AssertEqual(t, expectedStatus, actualStatus, "Storage status mismatch")
}

// verifyAppendCall checks if Append was called correctly
func verifyAppendCall(t *testing.T, mock *mockLogAppender, expectedEntries []types.LogEntry, expectedLastIndex types.Index) {
	t.Helper()

	testutil.AssertTrue(t, mock.appendArgs.callCount > 0, "Append should have been called")
	testutil.AssertEqual(t, expectedLastIndex, mock.appendArgs.lastCurrent, "Last index mismatch in Append call")

	if expectedEntries != nil {
		testutil.AssertEqual(t, len(expectedEntries), len(mock.appendArgs.lastEntries), "Entry count mismatch in Append call")

		for i := range expectedEntries {
			testutil.AssertEqual(t, expectedEntries[i].Index, mock.appendArgs.lastEntries[i].Index,
				"Entry index mismatch at position %d", i)
			testutil.AssertEqual(t, expectedEntries[i].Term, mock.appendArgs.lastEntries[i].Term,
				"Entry term mismatch at position %d", i)
		}
	}
}

// mockRWOperationLocker is a simplified implementation of rwOperationLocker for testing
type mockRWOperationLocker struct {
	mu sync.RWMutex
}

func (m *mockRWOperationLocker) DoWrite(ctx context.Context, operation func() error) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	return operation()
}

func (m *mockRWOperationLocker) DoRead(ctx context.Context, operation func() error) error {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return operation()
}

type testStorageBuilder struct {
	t     *testing.T
	cfg   StorageConfig
	opts  FileStorageOptions
	deps  *mockStorageDependencies
	first types.Index
	last  types.Index
}

func newTestStorageBuilder(t *testing.T) *testStorageBuilder {
	deps := newMockStorageDependencies()

	deps.metadataSvc.ValidateMetadataRangeFunc = func(firstIndex, lastIndex types.Index) error { return nil }
	deps.indexSvc.BuildFunc = func(path string) (buildResult, error) {
		return buildResult{
			IndexMap:       []types.IndexOffsetPair{{Index: 1, Offset: 0}},
			Truncated:      false,
			LastValidIndex: 1,
		}, nil
	}
	deps.indexSvc.TruncateAfterFunc = func(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
		var result []types.IndexOffsetPair
		for _, pair := range indexMap {
			if pair.Index <= target {
				result = append(result, pair)
			}
		}
		return result
	}
	deps.indexSvc.TruncateBeforeFunc = func(indexMap []types.IndexOffsetPair, target types.Index) []types.IndexOffsetPair {
		var result []types.IndexOffsetPair
		for _, pair := range indexMap {
			if pair.Index >= target {
				result = append(result, pair)
			}
		}
		return result
	}
	deps.indexSvc.ReadInRangeFunc = func(ctx context.Context, logPath string, indexMap []types.IndexOffsetPair, start, end types.Index) ([]types.LogEntry, int64, error) {
		return []types.LogEntry{}, 0, nil
	}
	deps.metadataSvc.SyncMetadataFromIndexMapFunc = func(path string, indexMap []types.IndexOffsetPair, currentFirst, currentLast types.Index, context string, useAtomicWrite bool) (types.Index, types.Index, error) {
		return 1, 1, nil
	}
	deps.logAppender.appendResult = appendResult{
		FirstIndex: 1,
		LastIndex:  1,
		Offsets:    []types.IndexOffsetPair{{Index: 1, Offset: 0}},
	}
	deps.indexSvc.getBoundsResult = boundsResult{NewFirst: 1, NewLast: 1, Changed: false, WasReset: false}
	deps.snapshotReader.readResult = struct {
		metadata types.SnapshotMetadata
		data     []byte
	}{
		metadata: types.SnapshotMetadata{LastIncludedIndex: 1, LastIncludedTerm: 1},
		data:     []byte("test-snapshot"),
	}

	return &testStorageBuilder{
		t:     t,
		cfg:   StorageConfig{Dir: "/test"},
		opts:  DefaultFileStorageOptions(),
		deps:  deps,
		first: 1,
		last:  1,
	}
}

func (b *testStorageBuilder) WithIndexBounds(first, last types.Index) *testStorageBuilder {
	b.first = first
	b.last = last
	return b
}

func (b *testStorageBuilder) WithOptions(opts FileStorageOptions) *testStorageBuilder {
	b.opts = opts
	return b
}

func (b *testStorageBuilder) WithDeps(configure func(*mockStorageDependencies)) *testStorageBuilder {
	configure(b.deps)
	return b
}

func (b *testStorageBuilder) Build() (*FileStorage, *mockStorageDependencies) {
	s, err := createFileStorageWithMocks(b.cfg, b.opts, b.deps)
	if err != nil {
		b.t.Fatalf("Failed to create test storage: %v", err)
	}
	s.firstLogIndex.Store(uint64(b.first))
	s.lastLogIndex.Store(uint64(b.last))
	return s, b.deps
}
