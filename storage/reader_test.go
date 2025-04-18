package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"reflect"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
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

// Helper function to create test entry data
func createSerializedEntry(index types.Index, term types.Term, command string) []byte {
	return []byte(fmt.Sprintf(`{"Index":%d,"Term":%d,"Command":"%s"}`, index, term, command))
}

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
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
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
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
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

// TestReadAtOffset tests the ReadAtOffset method
func TestReadAtOffset(t *testing.T) {
	log := &logger.NoOpLogger{}

	tests := []struct {
		name          string
		maxSize       int
		offset        int64
		expectedIndex types.Index
		fileContents  []byte
		seekErr       error
		serializer    *mockSerializer
		wantEntry     types.LogEntry
		wantBytesRead int64
		wantError     error
		wantErrorType error
	}{
		{
			name:          "Success",
			maxSize:       testMaxSize,
			offset:        0,
			expectedIndex: 10,
			fileContents:  createTestData(serializedTestEntry, testPrefixSize),
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					return testEntry, nil
				},
			},
			wantEntry:     testEntry,
			wantBytesRead: int64(testPrefixSize + len(serializedTestEntry)),
			wantError:     nil,
		},
		{
			name:          "SeekError",
			maxSize:       testMaxSize,
			offset:        5,
			expectedIndex: 10,
			fileContents:  createTestData(serializedTestEntry, testPrefixSize),
			seekErr:       errors.New("seek error"),
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: 0,
			wantErrorType: ErrStorageIO,
		},
		{
			name:          "ReadError",
			maxSize:       testMaxSize,
			offset:        0,
			expectedIndex: 10,
			fileContents:  []byte{}, // Empty file will cause EOF
			serializer:    &mockSerializer{},
			wantEntry:     types.LogEntry{},
			wantBytesRead: 0,
			wantError:     io.EOF,
		},
		{
			name:          "IndexMismatch",
			maxSize:       testMaxSize,
			offset:        0,
			expectedIndex: 20, // Different from actual index (10)
			fileContents:  createTestData(serializedTestEntry, testPrefixSize),
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					return testEntry, nil
				},
			},
			wantEntry:     types.LogEntry{},
			wantBytesRead: int64(testPrefixSize + len(serializedTestEntry)),
			wantErrorType: ErrCorruptedLog,
		},
		{
			name:          "Zero ExpectedIndex",
			maxSize:       testMaxSize,
			offset:        0,
			expectedIndex: 0, // Should not check index
			fileContents:  createTestData(serializedTestEntry, testPrefixSize),
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					return testEntry, nil
				},
			},
			wantEntry:     testEntry,
			wantBytesRead: int64(testPrefixSize + len(serializedTestEntry)),
			wantError:     nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFile := &mockFile{
				Reader: bytes.NewReader(tt.fileContents),
			}

			// Override Seek if needed to simulate errors
			if tt.seekErr != nil {
				originalSeek := mockFile.Seek
				mockFile.SeekFunc = func(offset int64, whence int) (int64, error) {
					return 0, tt.seekErr
				}
				defer func() { mockFile.SeekFunc = originalSeek }()
			}

			reader := newLogEntryReader(tt.maxSize, testPrefixSize, tt.serializer, log)
			entry, bytesRead, err := reader.ReadAtOffset(mockFile, tt.offset, tt.expectedIndex)

			if !reflect.DeepEqual(entry, tt.wantEntry) {
				t.Errorf("ReadAtOffset() entry = %v, want %v", entry, tt.wantEntry)
			}

			if bytesRead != tt.wantBytesRead {
				t.Errorf("ReadAtOffset() bytesRead = %v, want %v", bytesRead, tt.wantBytesRead)
			}

			if tt.wantError == nil && tt.wantErrorType == nil {
				if err != nil {
					t.Errorf("ReadAtOffset() error = %v, want nil", err)
				}
			} else if tt.wantErrorType != nil {
				if !errors.Is(err, tt.wantErrorType) {
					t.Errorf("ReadAtOffset() error = %v, want error that satisfies errors.Is(%v)", err, tt.wantErrorType)
				}
			} else if err == nil || err.Error() != tt.wantError.Error() {
				t.Errorf("ReadAtOffset() error = %v, want %v", err, tt.wantError)
			}
		})
	}
}

// TestScanRange tests the ScanRange method
func TestScanRange(t *testing.T) {
	log := &logger.NoOpLogger{}

	// Create test entries
	entry1 := types.LogEntry{Index: 5, Term: 1, Command: []byte("command1")}
	entry2 := types.LogEntry{Index: 6, Term: 1, Command: []byte("command2")}
	entry3 := types.LogEntry{Index: 7, Term: 1, Command: []byte("command3")}
	entry4 := types.LogEntry{Index: 8, Term: 2, Command: []byte("command4")}

	serializedEntry1 := createSerializedEntry(entry1.Index, entry1.Term, "command1")
	serializedEntry2 := createSerializedEntry(entry2.Index, entry2.Term, "command2")
	serializedEntry3 := createSerializedEntry(entry3.Index, entry3.Term, "command3")
	serializedEntry4 := createSerializedEntry(entry4.Index, entry4.Term, "command4")

	// Create file contents with all entries
	fileContents := append(createTestData(serializedEntry1, testPrefixSize),
		append(createTestData(serializedEntry2, testPrefixSize),
			append(createTestData(serializedEntry3, testPrefixSize),
				createTestData(serializedEntry4, testPrefixSize)...)...)...)

	tests := []struct {
		name          string
		maxSize       int
		fileContents  []byte
		start         types.Index
		end           types.Index
		ctxTimeout    time.Duration // 0 means no timeout
		serializer    *mockSerializer
		wantEntries   []types.LogEntry
		wantError     error
		wantErrorType error
	}{
		{
			name:         "Full Range",
			maxSize:      testMaxSize,
			fileContents: fileContents,
			start:        5,
			end:          9,
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					switch {
					case bytes.Equal(data, serializedEntry1):
						return entry1, nil
					case bytes.Equal(data, serializedEntry2):
						return entry2, nil
					case bytes.Equal(data, serializedEntry3):
						return entry3, nil
					case bytes.Equal(data, serializedEntry4):
						return entry4, nil
					default:
						return types.LogEntry{}, errors.New("unknown entry")
					}
				},
			},
			wantEntries: []types.LogEntry{entry1, entry2, entry3, entry4},
			wantError:   nil,
		},
		{
			name:         "Partial Range",
			maxSize:      testMaxSize,
			fileContents: fileContents,
			start:        6,
			end:          8,
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					switch {
					case bytes.Equal(data, serializedEntry1):
						return entry1, nil
					case bytes.Equal(data, serializedEntry2):
						return entry2, nil
					case bytes.Equal(data, serializedEntry3):
						return entry3, nil
					case bytes.Equal(data, serializedEntry4):
						return entry4, nil
					default:
						return types.LogEntry{}, errors.New("unknown entry")
					}
				},
			},
			wantEntries: []types.LogEntry{entry2, entry3},
			wantError:   nil,
		},
		{
			name:         "Empty Range",
			maxSize:      testMaxSize,
			fileContents: fileContents,
			start:        9,
			end:          10,
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					// Return valid entries but with indices outside the requested range
					if bytes.Equal(data, serializedEntry1) {
						return entry1, nil // Index 5
					} else if bytes.Equal(data, serializedEntry2) {
						return entry2, nil // Index 6
					} else if bytes.Equal(data, serializedEntry3) {
						return entry3, nil // Index 7
					} else if bytes.Equal(data, serializedEntry4) {
						return entry4, nil // Index 8
					}
					return types.LogEntry{}, errors.New("unknown entry")
				},
			},
			wantEntries: []types.LogEntry{},
			wantError:   nil,
		},
		{
			name:         "Context Canceled",
			maxSize:      testMaxSize,
			fileContents: fileContents,
			start:        5,
			end:          9,
			ctxTimeout:   1 * time.Nanosecond, // Extremely short timeout
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					time.Sleep(5 * time.Millisecond) // Ensure context expires
					return types.LogEntry{}, nil
				},
			},
			wantEntries: nil,
			wantError:   context.DeadlineExceeded,
		},
		{
			name:         "Read Error",
			maxSize:      testMaxSize,
			fileContents: createTestData([]byte("invalid"), testPrefixSize),
			start:        5,
			end:          9,
			serializer: &mockSerializer{
				unmarshalLogEntryFunc: func(data []byte) (types.LogEntry, error) {
					return types.LogEntry{}, errors.New("deserialization error")
				},
			},
			wantEntries:   nil,
			wantErrorType: ErrCorruptedLog,
		},
		{
			name:         "Start greater than or equal to end",
			maxSize:      testMaxSize,
			fileContents: []byte{}, // Shouldn't be read
			start:        5,
			end:          5,
			serializer:   &mockSerializer{},
			wantEntries:  []types.LogEntry{},
			wantError:    nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mockFile := &mockFile{
				Reader: bytes.NewReader(tt.fileContents),
			}

			reader := newLogEntryReader(tt.maxSize, testPrefixSize, tt.serializer, log)

			var ctx context.Context
			var cancel context.CancelFunc
			if tt.ctxTimeout > 0 {
				ctx, cancel = context.WithTimeout(context.Background(), tt.ctxTimeout)
				defer cancel()
			} else {
				ctx = context.Background()
			}

			entries, err := reader.ScanRange(ctx, mockFile, tt.start, tt.end)

			if tt.wantError == nil && tt.wantErrorType == nil {
				if err != nil {
					t.Errorf("ScanRange() error = %v, want nil", err)
				}
			} else if tt.wantErrorType != nil {
				if !errors.Is(err, tt.wantErrorType) {
					t.Errorf("ScanRange() error = %v, want error that satisfies errors.Is(%v)", err, tt.wantErrorType)
				}
			} else if err == nil || !errors.Is(err, tt.wantError) {
				t.Errorf("ScanRange() error = %v, want %v", err, tt.wantError)
			}

			if err == nil {
				if !reflect.DeepEqual(entries, tt.wantEntries) {
					t.Errorf("ScanRange() entries = %v, want %v", entries, tt.wantEntries)
				}
			}
		})
	}
}

// TestLogEntryReaderIntegration tests that all methods of the logEntryReader work together
func TestLogEntryReaderIntegration(t *testing.T) {
	// Create a series of test entries
	entries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
		{Index: 2, Term: 1, Command: []byte("command2")},
		{Index: 3, Term: 2, Command: []byte("command3")},
		{Index: 4, Term: 2, Command: []byte("command4")},
		{Index: 5, Term: 3, Command: []byte("command5")},
	}

	// Serialize and combine all entries
	serializer := newJsonSerializer()
	var fileContents []byte
	var offsets []int64
	var currentOffset int64

	offsets = append(offsets, currentOffset)
	for _, entry := range entries {
		data, err := serializer.MarshalLogEntry(entry)
		testutil.AssertNoError(t, err)

		prefix := make([]byte, testPrefixSize)
		binary.BigEndian.PutUint32(prefix, uint32(len(data)))

		fileContents = append(fileContents, prefix...)
		fileContents = append(fileContents, data...)

		currentOffset += int64(testPrefixSize + len(data))
		if len(offsets) < len(entries) {
			offsets = append(offsets, currentOffset)
		}
	}

	log := &logger.NoOpLogger{}
	reader := newLogEntryReader(testMaxSize, testPrefixSize, serializer, log)

	t.Run("Sequential Reading", func(t *testing.T) {
		file := &mockFile{
			Reader: bytes.NewReader(fileContents),
		}

		for i, expectedEntry := range entries {
			entry, bytesRead, err := reader.ReadNext(file)
			testutil.AssertNoError(t, err)
			testutil.AssertEqual(t, expectedEntry, entry)
			if i < len(entries)-1 {
				testutil.AssertEqual(t, offsets[i+1]-offsets[i], bytesRead)
			}
		}

		// Should return EOF after all entries
		_, _, err := reader.ReadNext(file)
		testutil.AssertEqual(t, io.EOF, err)
	})

	t.Run("Random Access", func(t *testing.T) {
		for i, expectedEntry := range entries {
			file := &mockFile{
				Reader: bytes.NewReader(fileContents),
			}

			entry, _, err := reader.ReadAtOffset(file, offsets[i], expectedEntry.Index)
			testutil.AssertNoError(t, err)
			testutil.AssertEqual(t, expectedEntry, entry)
		}
	})

	t.Run("Range Scanning", func(t *testing.T) {
		file := &mockFile{
			Reader: bytes.NewReader(fileContents),
		}

		// Test various ranges
		ranges := []struct {
			start, end types.Index
			expected   []types.LogEntry
		}{
			{1, 6, entries},            // All entries
			{2, 4, entries[1:3]},       // Middle subset
			{1, 2, entries[0:1]},       // Just first
			{5, 6, entries[4:5]},       // Just last
			{6, 7, []types.LogEntry{}}, // Empty range
		}

		for _, r := range ranges {
			file.Seek(0, io.SeekStart) // Reset file position
			result, err := reader.ScanRange(context.Background(), file, r.start, r.end)
			testutil.AssertNoError(t, err)
			testutil.AssertEqual(t, r.expected, result)
		}
	})

	t.Run("Context Cancellation", func(t *testing.T) {
		file := &mockFile{
			Reader: bytes.NewReader(fileContents),
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		_, err := reader.ScanRange(ctx, file, 1, 6)
		testutil.AssertEqual(t, context.Canceled, err)
	})
}
