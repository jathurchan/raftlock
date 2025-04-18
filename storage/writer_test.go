package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// TestWriteEntriesToFile tests the WriteEntriesToFile method of defaultLogWriter
func TestWriteEntriesToFile(t *testing.T) {
	log := &logger.NoOpLogger{}
	serializer := newJsonSerializer()

	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
		{Index: 2, Term: 1, Command: []byte("command2")},
		{Index: 3, Term: 2, Command: []byte("command3")},
	}

	t.Run("Success", func(t *testing.T) {
		writer := newLogWriter(serializer, log)
		buffer := &bytes.Buffer{}
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return buffer.Write(p)
			},
		}

		startOffset := int64(10)
		offsets, finalOffset, err := writer.WriteEntriesToFile(context.Background(), mockFile, startOffset, testEntries)

		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, 3, len(offsets))
		testutil.AssertEqual(t, types.Index(1), offsets[0].Index)
		testutil.AssertEqual(t, startOffset, offsets[0].Offset)
		testutil.AssertTrue(t, finalOffset > startOffset)
		testutil.AssertTrue(t, buffer.Len() > 0)
	})

	t.Run("WriteFails", func(t *testing.T) {
		writer := newLogWriter(serializer, log)
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return 0, errors.New("write error")
			},
		}

		_, _, err := writer.WriteEntriesToFile(context.Background(), mockFile, 0, testEntries)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("SerializationFails", func(t *testing.T) {
		mockSerializer := &mockSerializer{
			marshalLogEntryFunc: func(entry types.LogEntry) ([]byte, error) {
				return nil, errors.New("serialization error")
			},
		}

		writer := newLogWriter(mockSerializer, log)
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
		}

		_, _, err := writer.WriteEntriesToFile(context.Background(), mockFile, 0, testEntries)

		testutil.AssertError(t, err)
		testutil.AssertContains(t, err.Error(), "failed writing log entry")
	})

	t.Run("ContextCancelled", func(t *testing.T) {
		writer := newLogWriter(serializer, log)
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				// Simulate slow write
				time.Sleep(time.Millisecond)
				return len(p), nil
			},
		}

		largeEntries := make([]types.LogEntry, 100)
		for i := range 100 {
			largeEntries[i] = types.LogEntry{
				Index:   types.Index(i + 1),
				Term:    1,
				Command: []byte("command"),
			}
		}

		ctx, cancel := context.WithCancel(context.Background())

		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		_, _, err := writer.WriteEntriesToFile(ctx, mockFile, 0, largeEntries)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)
	})
}

// TestLogAppender tests the defaultLogAppender implementation
func TestLogAppender(t *testing.T) {
	log := &logger.NoOpLogger{}
	serializer := newJsonSerializer()
	logFilePath := "/test/log.dat"

	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
		{Index: 2, Term: 1, Command: []byte("command2")},
		{Index: 3, Term: 2, Command: []byte("command3")},
	}

	validationTests := []struct {
		name        string
		entries     []types.LogEntry
		currentLast types.Index
		expectedErr error
	}{
		{
			name:        "EmptyEntries",
			entries:     []types.LogEntry{},
			currentLast: 0,
			expectedErr: ErrEmptyEntries,
		},
		{
			name: "OutOfOrderEntries",
			entries: []types.LogEntry{
				{Index: 2, Term: 1, Command: []byte("command2")},
				{Index: 1, Term: 1, Command: []byte("command1")},
			},
			currentLast: 0,
			expectedErr: ErrOutOfOrderEntries,
		},
		{
			name: "NonContiguousEntries",
			entries: []types.LogEntry{
				{Index: 3, Term: 1, Command: []byte("command3")},
				{Index: 4, Term: 1, Command: []byte("command4")},
			},
			currentLast: 1,
			expectedErr: ErrNonContiguousEntries,
		},
		{
			name: "NonContiguousWithinEntries",
			entries: []types.LogEntry{
				{Index: 2, Term: 1, Command: []byte("command2")},
				{Index: 4, Term: 1, Command: []byte("command4")},
			},
			currentLast: 1,
			expectedErr: ErrOutOfOrderEntries,
		},
		{
			name: "FirstEntryNotOne",
			entries: []types.LogEntry{
				{Index: 2, Term: 1, Command: []byte("command2")},
			},
			currentLast: 0,
			expectedErr: ErrNonContiguousEntries,
		},
	}

	for _, tc := range validationTests {
		t.Run(tc.name, func(t *testing.T) {
			fs := newMockFileSystem()
			writer := newLogWriter(serializer, log)
			appender := newLogAppender(logFilePath, fs, writer, log, true)

			_, err := appender.Append(context.Background(), tc.entries, tc.currentLast)
			testutil.AssertError(t, err)
			testutil.AssertErrorIs(t, err, tc.expectedErr)
		})
	}

	t.Run("AppendSuccess", func(t *testing.T) {
		fs := newMockFileSystem()
		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, true)

		result, err := appender.Append(context.Background(), testEntries, 0)

		testutil.AssertNoError(t, err)

		testutil.AssertEqual(t, types.Index(1), result.FirstIndex)
		testutil.AssertEqual(t, types.Index(3), result.LastIndex)
		testutil.AssertEqual(t, 3, len(result.Offsets))
	})

	t.Run("AppendWithExistingEntries", func(t *testing.T) {
		fs := newMockFileSystem()
		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, true)

		_, err := appender.Append(context.Background(), testEntries, 0)
		testutil.AssertNoError(t, err)

		moreTEntries := []types.LogEntry{
			{Index: 4, Term: 2, Command: []byte("command4")},
			{Index: 5, Term: 3, Command: []byte("command5")},
		}

		result, err := appender.Append(context.Background(), moreTEntries, 3)

		testutil.AssertNoError(t, err)

		testutil.AssertEqual(t, types.Index(4), result.FirstIndex)
		testutil.AssertEqual(t, types.Index(5), result.LastIndex)
	})

	t.Run("FileOpenError", func(t *testing.T) {
		fs := newMockFileSystem()
		fs.AppendFileFunc = func(name string) (file, error) {
			return nil, errors.New("open error")
		}

		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, true)

		_, err := appender.Append(context.Background(), testEntries, 0)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("SeekError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			SeekFunc: func(offset int64, whence int) (int64, error) {
				return 0, errors.New("seek error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, true)

		_, err := appender.Append(context.Background(), testEntries, 0)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("WriteError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return 0, errors.New("write error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, true)

		_, err := appender.Append(context.Background(), testEntries, 0)

		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)

		testutil.AssertEqual(t, logFilePath, fs.truncatedPath)
		testutil.AssertEqual(t, int64(0), fs.truncatedSize)
	})

	t.Run("SyncError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			SyncFunc: func() error {
				return errors.New("sync error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, true) // sync enabled

		_, err := appender.Append(context.Background(), testEntries, 0)

		testutil.AssertError(t, err)
	})

	t.Run("SyncDisabled", func(t *testing.T) {
		fs := newMockFileSystem()
		syncCalled := false
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			SyncFunc: func() error {
				syncCalled = true
				return nil
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		writer := newLogWriter(serializer, log)
		appender := newLogAppender(logFilePath, fs, writer, log, false) // sync disabled

		_, err := appender.Append(context.Background(), testEntries, 0)

		testutil.AssertNoError(t, err)

		testutil.AssertFalse(t, syncCalled)
	})
}

// TestLogRewriter tests the defaultLogRewriter implementation
func TestLogRewriter(t *testing.T) {
	log := &logger.NoOpLogger{}
	serializer := newJsonSerializer()
	logPath := "/test/log.dat"

	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
		{Index: 2, Term: 1, Command: []byte("command2")},
		{Index: 3, Term: 2, Command: []byte("command3")},
	}

	t.Run("RewriteSuccess", func(t *testing.T) {
		fs := newMockFileSystem()
		rewriter := newLogRewriter(logPath, fs, serializer, log)

		offsets, err := rewriter.Rewrite(context.Background(), testEntries)

		testutil.AssertNoError(t, err)

		testutil.AssertEqual(t, 3, len(offsets))
		testutil.AssertEqual(t, types.Index(1), offsets[0].Index)

		_, exists := fs.files[logPath]
		testutil.AssertTrue(t, exists)
		_, tmpExists := fs.files[logPath+".tmp"]
		testutil.AssertFalse(t, tmpExists)
	})

	t.Run("EmptyEntries", func(t *testing.T) {
		fs := newMockFileSystem()
		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with empty entries (should still work)
		offsets, err := rewriter.Rewrite(context.Background(), []types.LogEntry{})

		// Verify no error
		testutil.AssertNoError(t, err)

		// Verify empty offsets
		testutil.AssertEqual(t, 0, len(offsets))
	})

	t.Run("TempFileCreateError", func(t *testing.T) {
		fs := newMockFileSystem()
		fs.writeFileErr = errors.New("write file error")

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with temp file creation error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("TempFileOpenError", func(t *testing.T) {
		fs := newMockFileSystem()
		fs.AppendFileFunc = func(name string) (file, error) {
			if name == logPath+".tmp" {
				return nil, errors.New("open error")
			}
			return &mockFile{Reader: bytes.NewReader(nil)}, nil
		}

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with temp file open error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("SeekError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			SeekFunc: func(offset int64, whence int) (int64, error) {
				return 0, errors.New("seek error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with seek error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)

		// Verify temp file was cleaned up
		_, exists := fs.files[logPath+".tmp"]
		testutil.AssertFalse(t, exists)
	})

	t.Run("WriteError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return 0, errors.New("write error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with write error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)

		// Verify temp file was cleaned up
		_, exists := fs.files[logPath+".tmp"]
		testutil.AssertFalse(t, exists)
	})

	t.Run("SyncError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			SyncFunc: func() error {
				return errors.New("sync error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with sync error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)

		// Verify temp file was cleaned up
		_, exists := fs.files[logPath+".tmp"]
		testutil.AssertFalse(t, exists)
	})

	t.Run("CloseError", func(t *testing.T) {
		fs := newMockFileSystem()
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			CloseFunc: func() error {
				return errors.New("close error")
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with close error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)
	})

	t.Run("RenameError", func(t *testing.T) {
		fs := newMockFileSystem()
		fs.renameErr = errors.New("rename error")

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Test rewriting with rename error
		_, err := rewriter.Rewrite(context.Background(), testEntries)

		// Verify error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, ErrStorageIO)

		// Verify temp file was cleaned up
		_, exists := fs.files[logPath+".tmp"]
		testutil.AssertFalse(t, exists)
	})

	t.Run("ContextCanceled", func(t *testing.T) {
		fs := newMockFileSystem()

		// Create a mock file with slow write to allow context cancellation
		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				time.Sleep(time.Millisecond)
				return len(p), nil
			},
		}

		fs.AppendFileFunc = func(name string) (file, error) {
			return mockFile, nil
		}

		rewriter := newLogRewriter(logPath, fs, serializer, log)

		// Create many entries to ensure several writes
		manyEntries := make([]types.LogEntry, 100)
		for i := 0; i < 100; i++ {
			manyEntries[i] = types.LogEntry{
				Index:   types.Index(i + 1),
				Term:    1,
				Command: []byte("command"),
			}
		}

		// Create a context that will be cancelled
		ctx, cancel := context.WithCancel(context.Background())

		// Start a goroutine that cancels after a short delay
		go func() {
			time.Sleep(5 * time.Millisecond)
			cancel()
		}()

		// Test rewriting with cancelled context
		_, err := rewriter.Rewrite(ctx, manyEntries)

		// Verify context cancellation error
		testutil.AssertError(t, err)
		testutil.AssertErrorIs(t, err, context.Canceled)

		// Verify temp file was cleaned up
		_, exists := fs.files[logPath+".tmp"]
		testutil.AssertFalse(t, exists)
	})
}

func TestLogWriterWithJSONSerializer(t *testing.T) {
	log := &logger.NoOpLogger{}
	serializer := newJsonSerializer()
	writer := newLogWriter(serializer, log)

	// Create test entries
	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
	}

	t.Run("RoundTrip", func(t *testing.T) {
		// Create a buffer to capture writes
		buffer := &bytes.Buffer{}

		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return buffer.Write(p)
			},
		}

		// Write entry
		_, finalOffset, err := writer.WriteEntriesToFile(context.Background(), mockFile, 0, testEntries)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, int64(buffer.Len()), finalOffset)

		// Read back the data
		data := buffer.Bytes()

		// Verify data format (4-byte length prefix followed by JSON data)
		testutil.AssertEqual(t, lengthPrefixSize, 4)
		testutil.AssertTrue(t, len(data) > 4)

		// Extract length prefix
		prefix := data[:4]
		entryLen := int(binary.BigEndian.Uint32(prefix))

		// Verify length matches actual data length
		testutil.AssertEqual(t, entryLen, len(data)-4)

		// Unmarshal the JSON data
		entryData := data[4:]
		entry, err := serializer.UnmarshalLogEntry(entryData)
		testutil.AssertNoError(t, err)

		// Verify entry matches original
		testutil.AssertEqual(t, testEntries[0], entry)
	})
}

func TestLogWriterWithBinarySerializer(t *testing.T) {
	log := &logger.NoOpLogger{}
	serializer := newBinarySerializer()
	writer := newLogWriter(serializer, log)

	// Create test entries
	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
	}

	t.Run("RoundTrip", func(t *testing.T) {
		// Create a buffer to capture writes
		buffer := &bytes.Buffer{}

		mockFile := &mockFile{
			Reader: bytes.NewReader(nil),
			WriteFunc: func(p []byte) (int, error) {
				return buffer.Write(p)
			},
		}

		// Write entry
		_, finalOffset, err := writer.WriteEntriesToFile(context.Background(), mockFile, 0, testEntries)
		testutil.AssertNoError(t, err)
		testutil.AssertEqual(t, int64(buffer.Len()), finalOffset)

		// Read back the data
		data := buffer.Bytes()

		// Verify data format (4-byte length prefix followed by binary data)
		testutil.AssertEqual(t, lengthPrefixSize, 4)
		testutil.AssertTrue(t, len(data) > 4)

		// Extract length prefix
		prefix := data[:4]
		entryLen := int(binary.BigEndian.Uint32(prefix))

		// Verify length matches actual data length
		testutil.AssertEqual(t, entryLen, len(data)-4)

		// Unmarshal the binary data
		entryData := data[4:]
		entry, err := serializer.UnmarshalLogEntry(entryData)
		testutil.AssertNoError(t, err)

		// Verify entry matches original
		testutil.AssertEqual(t, testEntries[0], entry)
	})
}

// TestLogAppenderCloseErrorDuringRollback tests the code path where file.Close() fails during rollback
func TestLogAppenderCloseErrorDuringRollback(t *testing.T) {
	errorwCalled := false
	warnwCalled := false
	mockLog := &logger.NoOpLogger{
		ErrorwFunc: func(msg string, keysAndValues ...any) {
			if msg == "failed to close file during rollback" {
				errorwCalled = true
			}
		},
		WarnwFunc: func(msg string, keysAndValues ...any) {
			warnwCalled = true
		},
	}

	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
	}

	fs := newMockFileSystem()
	mockFile := &mockFile{
		Reader: bytes.NewReader(nil),
		WriteFunc: func(p []byte) (int, error) {
			return 0, errors.New("write error")
		},
		CloseFunc: func() error {
			return errors.New("close error")
		},
	}
	fs.AppendFileFunc = func(name string) (file, error) {
		return mockFile, nil
	}

	serializer := newJsonSerializer()
	writer := newLogWriter(serializer, mockLog)
	appender := newLogAppender("/test/log.dat", fs, writer, mockLog, true)

	_, err := appender.Append(context.Background(), testEntries, 0)

	testutil.AssertError(t, err)
	testutil.AssertErrorIs(t, err, ErrStorageIO)

	testutil.AssertTrue(t, errorwCalled, "Expected ErrorwFunc to be called for close error")
	testutil.AssertTrue(t, warnwCalled, "Expected WarnwFunc to be called")
}

// TestLogRewriterRemoveErrorDuringCleanup tests the code path where fs.Remove() fails during cleanup
func TestLogRewriterRemoveErrorDuringCleanup(t *testing.T) {
	removeErrorCalled := false
	mockLog := &logger.NoOpLogger{
		ErrorwFunc: func(msg string, keysAndValues ...any) {
			if msg == "failed to remove temp file during cleanup" {
				removeErrorCalled = true
			}
		},
	}

	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
	}

	fs := newMockFileSystem()
	mockFile := &mockFile{
		Reader: bytes.NewReader(nil),
		WriteFunc: func(p []byte) (int, error) {
			return 0, errors.New("write error")
		},
	}
	fs.AppendFileFunc = func(name string) (file, error) {
		return mockFile, nil
	}
	// Set remove to fail (but not with NotExist error)
	fs.RemoveFunc = func(name string) error {
		return errors.New("remove error")
	}

	serializer := newJsonSerializer()
	rewriter := newLogRewriter("/test/log.dat", fs, serializer, mockLog)

	_, err := rewriter.Rewrite(context.Background(), testEntries)

	testutil.AssertError(t, err)
	testutil.AssertErrorIs(t, err, ErrStorageIO)

	testutil.AssertTrue(t, removeErrorCalled, "Expected ErrorwFunc to be called for remove error")
}

// TestLogAppenderRollbackPerformed tests that the "rollback performed" message is logged
func TestLogAppenderRollbackPerformed(t *testing.T) {
	rollbackPerformed := false
	mockLog := &logger.NoOpLogger{
		WarnwFunc: func(msg string, keysAndValues ...any) {
			if msg == "rollback performed" {
				rollbackPerformed = true
			}
		},
	}

	testEntries := []types.LogEntry{
		{Index: 1, Term: 1, Command: []byte("command1")},
	}

	fs := newMockFileSystem()
	mockFile := &mockFile{
		Reader: bytes.NewReader(nil),
		WriteFunc: func(p []byte) (int, error) {
			return 0, errors.New("write error")
		},
	}
	fs.AppendFileFunc = func(name string) (file, error) {
		return mockFile, nil
	}
	// Make sure truncate doesn't return an error, to ensure the "rollback performed" message is logged
	fs.TruncateFunc = func(name string, size int64) error {
		return nil
	}

	serializer := newJsonSerializer()
	writer := newLogWriter(serializer, mockLog)
	appender := newLogAppender("/test/log.dat", fs, writer, mockLog, true)

	_, err := appender.Append(context.Background(), testEntries, 0)

	testutil.AssertError(t, err)
	testutil.AssertErrorIs(t, err, ErrStorageIO)

	testutil.AssertTrue(t, rollbackPerformed, "Expected 'rollback performed' message to be logged")
}
