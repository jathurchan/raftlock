package storage

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jathurchan/raftlock/types"
)

// LogEntryReader provides methods for reading and deserializing log entries
type LogEntryReader interface {
	ReadNextEntry(file File) (types.LogEntry, int64, error)
	DeserializeEntry(data []byte) (types.LogEntry, error)
}

// DefaultLogEntryReader implements LogEntryReader
type DefaultLogEntryReader struct {
	options      *FileStorageOptions
	maxEntrySize int
	prefixSize   int
}

// NewDefaultLogEntryReader creates a new DefaultLogEntryReader
func NewDefaultLogEntryReader(options *FileStorageOptions, maxEntrySize, prefixSize int) *DefaultLogEntryReader {
	return &DefaultLogEntryReader{
		options:      options,
		maxEntrySize: maxEntrySize,
		prefixSize:   prefixSize,
	}
}

// ReadNextEntry reads the next log entry starting from the current file offset.
// Returns the deserialized entry, total bytes read, or an error.
// Returns io.EOF if no complete entry is available.
func (r *DefaultLogEntryReader) ReadNextEntry(file File) (types.LogEntry, int64, error) {
	var bytesRead int64
	lenBuf := make([]byte, r.prefixSize)

	// Read length prefix
	n, err := io.ReadFull(file, lenBuf)
	bytesRead += int64(n)
	if err != nil {
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			return types.LogEntry{}, bytesRead, io.EOF
		}
		return types.LogEntry{}, bytesRead, fmt.Errorf("error reading entry length: %w", err)
	}

	length := binary.BigEndian.Uint32(lenBuf)
	if length == 0 || length > uint32(r.maxEntrySize) {
		return types.LogEntry{}, bytesRead, fmt.Errorf("%w: invalid entry length %d (max %d)",
			ErrCorruptedLog, length, r.maxEntrySize)
	}

	// Read data
	data := make([]byte, length)
	m, err := io.ReadFull(file, data)
	bytesRead += int64(m)
	if err != nil {
		return types.LogEntry{}, bytesRead, fmt.Errorf("error reading entry data: %w", err)
	}

	// Deserialize
	entry, err := r.DeserializeEntry(data)
	if err != nil {
		return types.LogEntry{}, bytesRead, fmt.Errorf("%w: failed to deserialize entry: %v",
			ErrCorruptedLog, err)
	}

	return entry, bytesRead, nil
}

// DeserializeEntry deserializes a log entry from raw bytes based on storage options.
func (r *DefaultLogEntryReader) DeserializeEntry(data []byte) (types.LogEntry, error) {
	if r.options.Features.EnableBinaryFormat {
		return deserializeLogEntry(data)
	}
	var entry types.LogEntry
	if err := json.Unmarshal(data, &entry); err != nil {
		return types.LogEntry{}, err
	}
	return entry, nil
}
