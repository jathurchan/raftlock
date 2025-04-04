package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"

	"github.com/jathurchan/raftlock/types"
)

type mockSerializer struct {
	unmarshalFunc func([]byte) (types.LogEntry, error)
	calledWith    []byte
}

func (m *mockSerializer) UnmarshalLogEntry(data []byte) (types.LogEntry, error) {
	m.calledWith = data
	if m.unmarshalFunc != nil {
		return m.unmarshalFunc(data)
	}
	return types.LogEntry{}, errors.New("mockSerializer: unmarshalFunc not set")
}

// All unused methods stubbed
func (m *mockSerializer) MarshalLogEntry(types.LogEntry) ([]byte, error)     { return nil, nil }
func (m *mockSerializer) MarshalMetadata(logMetadata) ([]byte, error)        { return nil, nil }
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

type mockFile struct {
	*bytes.Reader
}

func (r *mockFile) ReadFull(buf []byte) (int, error) { return io.ReadFull(r.Reader, buf) }
func (r *mockFile) Close() error                     { return nil }
func (r *mockFile) ReadAll() ([]byte, error)         { return io.ReadAll(r.Reader) }

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

func createTestData(entryData []byte, prefixSize int) []byte {
	prefix := make([]byte, prefixSize)
	binary.BigEndian.PutUint32(prefix, uint32(len(entryData)))
	return append(prefix, entryData...)
}
