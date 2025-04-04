package storage

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

const (
	testMaxSize    = 1024
	testPrefixSize = 4
)

var (
	testEntry = types.LogEntry{
		Index:   10,
		Term:    1,
		Command: []byte("test-command"),
	}
	serializedTestEntry = []byte(`{"Index":10,"Term":1,"Command":"dGVzdC1jb21tYW5k"}`)
)

func TestReadNext(t *testing.T) {
	log := &logger.NoOpLogger{}

	tests := []struct {
		name          string
		maxSize       int
		fileReader    file
		serializer    *mockSerializer
		wantEntry     types.LogEntry
		wantBytesRead int64
		wantError     error
	}{
		{
			name:    "Success",
			maxSize: testMaxSize,
			fileReader: &mockFile{
				Reader: bytes.NewReader(createTestData(serializedTestEntry, testPrefixSize)),
			},
			serializer: &mockSerializer{
				unmarshalFunc: func(data []byte) (types.LogEntry, error) {
					if !bytes.Equal(data, serializedTestEntry) {
						return types.LogEntry{}, errors.New("unexpected serialized data")
					}
					return testEntry, nil
				},
			},
			wantEntry:     testEntry,
			wantBytesRead: int64(testPrefixSize + len(serializedTestEntry)),
			wantError:     nil,
		},
		{
			name:    "EOF on Prefix",
			maxSize: testMaxSize,
			fileReader: &mockFile{
				Reader: bytes.NewReader([]byte{}),
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: 0,
			wantError:     io.EOF,
		},
		{
			name:    "Partial Prefix",
			maxSize: testMaxSize,
			fileReader: &mockFile{
				Reader: bytes.NewReader([]byte{0x00, 0x01}),
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: 2,
			wantError:     io.EOF,
		},
		{
			name:    "Error Reading Prefix",
			maxSize: testMaxSize,
			fileReader: &failingReader{
				reader:      bytes.NewReader([]byte{}),
				err:         errors.New("read error"),
				bytesToRead: 0,
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: 0,
			wantError:     errors.New("read error"),
		},
		{
			name:    "Zero Length Entry",
			maxSize: testMaxSize,
			fileReader: &mockFile{
				Reader: bytes.NewReader(make([]byte, testPrefixSize)),
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: int64(testPrefixSize),
			wantError:     ErrCorruptedLog,
		},
		{
			name:    "Entry Too Large",
			maxSize: 10,
			fileReader: &mockFile{
				Reader: bytes.NewReader(createTestData(make([]byte, 11), testPrefixSize)),
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: int64(testPrefixSize),
			wantError:     ErrCorruptedLog,
		},
		{
			name:    "Incomplete Entry Body",
			maxSize: testMaxSize,
			fileReader: &mockFile{
				Reader: bytes.NewReader(append(binary.BigEndian.AppendUint32(nil, 10), []byte("short")...)),
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: int64(testPrefixSize + 5),
			wantError:     io.ErrUnexpectedEOF,
		},
		{
			name:    "Error Reading Body",
			maxSize: testMaxSize,
			fileReader: &failingReader{
				reader:      bytes.NewReader(createTestData(serializedTestEntry, testPrefixSize)),
				err:         errors.New("body read error"),
				bytesToRead: testPrefixSize,
			},
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: int64(testPrefixSize),
			wantError:     errors.New("body read error"),
		},
		{
			name:    "Deserialization Error",
			maxSize: testMaxSize,
			fileReader: &mockFile{
				Reader: bytes.NewReader(createTestData(serializedTestEntry, testPrefixSize)),
			},
			serializer: &mockSerializer{
				unmarshalFunc: func(data []byte) (types.LogEntry, error) {
					return types.LogEntry{}, errors.New("deserialization error")
				},
			},
			wantEntry:     types.LogEntry{},
			wantBytesRead: int64(testPrefixSize + len(serializedTestEntry)),
			wantError:     ErrCorruptedLog,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reader := newLogEntryReader(tt.maxSize, testPrefixSize, tt.serializer, log)
			entry, bytesRead, err := reader.ReadNext(tt.fileReader)

			if !reflect.DeepEqual(entry, tt.wantEntry) {
				t.Errorf("ReadNext() entry = %v, want %v", entry, tt.wantEntry)
			}
			if bytesRead != tt.wantBytesRead {
				t.Errorf("ReadNext() bytesRead = %v, want %v", bytesRead, tt.wantBytesRead)
			}
			if tt.wantError == nil {
				if err != nil {
					t.Errorf("ReadNext() error = %v, want nil", err)
				}
			} else {
				if err == nil {
					t.Errorf("ReadNext() error = nil, want error %v", tt.wantError)
				} else if tt.wantError == ErrCorruptedLog {
					if !errors.Is(err, ErrCorruptedLog) {
						t.Errorf("ReadNext() error = %v, want error that satisfies errors.Is(%v)", err, ErrCorruptedLog)
					}
				} else if err.Error() != tt.wantError.Error() {
					t.Errorf("ReadNext() error = %v, want error %v", err, tt.wantError)
				}
			}

			if tt.name == "Success" || tt.name == "Deserialization Error" {
				if !bytes.Equal(tt.serializer.calledWith, serializedTestEntry) {
					t.Errorf("UnmarshalLogEntry called with %v, want %v", tt.serializer.calledWith, serializedTestEntry)
				}
			}
		})
	}
}
