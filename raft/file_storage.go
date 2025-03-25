package raft

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// Read (R), Write(W), Execute(E) for owner & RE for others.
	OwnRWXOthRX os.FileMode = 0755
	// Read (R), Write(W) for owner & R for others.
	OwnRWOthR os.FileMode = 0644
)

// FileStorage implements the Storage interface using files on disk.
// It provides persistence for Raft state and log entries.
type FileStorage struct {
	dir      string       // Directory where all storage files are kept
	stateMu  sync.RWMutex // Mutex for state operations
	logMu    sync.RWMutex // Mutex for log operations
	metadata *logMetadata // Cached metadata about the log
}

// logMetadata stores information about the log structure.
type logMetadata struct {
	FirstIndex uint64 // Index of the first entry in the log.
	LastIndex  uint64 // Index of the last entry in the log.
}

// cleanupTempFiles removes any temporary files from interrupted operations
func (fs *FileStorage) cleanupTempFiles() error {
	tempPattern := filepath.Join(fs.dir, "*.tmp")
	matches, err := filepath.Glob(tempPattern)
	if err != nil {
		return err
	}

	for _, file := range matches {
		if err := os.Remove(file); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	return nil
}

// NewFileStorage creates a new FileStorage instance in the specified directory.
// It initializes the metadata and creates the directory if it doesn't exist.
// Returns a Storage interface and an error if directory creation fails or if metadata loading fails.
func NewFileStorage(config *StorageConfig) (Storage, error) {
	if err := os.MkdirAll(config.Dir, OwnRWXOthRX); err != nil {
		return nil, fmt.Errorf("failed to create directory: %w", err)
	}

	fs := &FileStorage{
		dir: config.Dir,
		metadata: &logMetadata{
			FirstIndex: 0,
			LastIndex:  0,
		},
	}

	// Clean up any temporary files from previous interrupted operations
	if err := fs.cleanupTempFiles(); err != nil {
		return nil, fmt.Errorf("failed to clean up temporary files: %w", err)
	}

	if err := fs.loadMetadata(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return fs, nil
}

// stateFile returns the full path to the state.json file that stores Raft state.
// This file contains the current term and votedFor information.
func (fs *FileStorage) stateFile() string {
	return filepath.Join(fs.dir, "state.json")
}

// metadataFile returns the full path to the metadata.json file that stores log metadata.
// This file contains the first and last log indices.
func (fs *FileStorage) metadataFile() string {
	return filepath.Join(fs.dir, "metadata.json")
}

// logFile returns the full path to the log.dat file that stores the actual log entries.
// Entries are stored in binary format with 4-byte length prefixes, serialized using Protocol Buffers.
func (fs *FileStorage) logFile() string {
	return filepath.Join(fs.dir, "log.dat")
}

// loadMetadata reads and deserializes the log metadata from disk.
// It initializes empty metadata if the file doesn't exist.
// Returns an error if the file exists but can't be read or parsed.
func (fs *FileStorage) loadMetadata() error {
	data, err := os.ReadFile(fs.metadataFile())
	if err != nil {
		if os.IsNotExist(err) {
			fs.metadata = &logMetadata{FirstIndex: 0, LastIndex: 0}
			return nil
		}
		return err
	}

	return json.Unmarshal(data, fs.metadata)
}

// SaveState persists the Raft node's current term and votedFor information to disk.
// Uses atomic file operations to ensure durability even during crashes.
// Returns an error if context is canceled, JSON marshaling fails, or if file operations fail.
func (fs *FileStorage) SaveState(ctx context.Context, state RaftState) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fs.stateMu.Lock()
	defer fs.stateMu.Unlock()
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	tmpFile := fs.stateFile() + ".tmp"
	if err := os.WriteFile(tmpFile, data, OwnRWOthR); err != nil {
		return err
	}
	return os.Rename(tmpFile, fs.stateFile())
}

// LoadState retrieves the persisted Raft state from disk.
// Returns the default initial state (term 0, votedFor -1) if the state file doesn't exist.
// Returns an error if context is canceled, the file exists but is unreadable, or contains corrupted data.
func (fs *FileStorage) LoadState(ctx context.Context) (RaftState, error) {
	if err := ctx.Err(); err != nil {
		return RaftState{}, err
	}
	fs.stateMu.RLock()
	defer fs.stateMu.RUnlock()
	var state RaftState
	data, err := os.ReadFile(fs.stateFile())
	if err != nil {
		if os.IsNotExist(err) {
			return RaftState{CurrentTerm: 0, VotedFor: int64(-1)}, nil
		}
		return state, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, ErrCorruptedData
	}
	return state, nil
}

// saveMetadata writes the current log metadata to disk atomically.
// Uses a temporary file and rename operation to ensure durability.
// Returns an error if JSON marshaling or file operations fail.
func (fs *FileStorage) saveMetadata() error {
	data, err := json.Marshal(fs.metadata)
	if err != nil {
		return err
	}

	tmpFile := fs.metadataFile() + ".tmp"
	if err := os.WriteFile(tmpFile, data, OwnRWOthR); err != nil {
		return err
	}

	return os.Rename(tmpFile, fs.metadataFile())
}

// AppendEntries adds one or more log entries to the Raft log.
// Updates metadata and persists the entries and metadata to disk.
// Returns an error if context is canceled, entries are non-contiguous, the file can't be opened,
// entries can't be written, or metadata can't be saved.
func (fs *FileStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	// First, check that entries within the batch are properly ordered
	for i := 1; i < len(entries); i++ {
		expected := entries[i-1].Index + 1
		if entries[i].Index != expected {
			return fmt.Errorf("out of order log entry: expected index %d, got %d",
				expected, entries[i].Index)
		}
	}

	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	if !fs.isFirstEntry() {
		// Check that entries are contiguous with the existing log
		if entries[0].Index != fs.metadata.LastIndex+1 {
			return fmt.Errorf("non-contiguous log entry: expected index %d, got %d",
				fs.metadata.LastIndex+1, entries[0].Index)
		}
	} else if entries[0].Index != 1 {
		return fmt.Errorf("first log entry must have index 1, got %d", entries[0].Index)
	}

	file, endPos, err := fs.openLogFile()
	if err != nil {
		return err
	}
	defer file.Close()

	if fs.isFirstEntry() {
		fs.metadata.FirstIndex = entries[0].Index
	}
	fs.metadata.LastIndex = entries[len(entries)-1].Index

	if err := fs.writeEntries(file, entries, endPos); err != nil {
		return err
	}
	return fs.saveMetadata()
}

// openLogFile opens the log file in append mode and seeks to the end position.
// Returns the file handle, the current end position for rollback, and any error encountered.
func (fs *FileStorage) openLogFile() (*os.File, int64, error) {
	file, err := os.OpenFile(fs.logFile(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, OwnRWOthR)
	if err != nil {
		return nil, 0, err
	}

	endPos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		file.Close()
		return nil, 0, err
	}

	return file, endPos, nil
}

// writeEntries encodes and writes a batch of log entries to the file.
// Truncates the file to its original position if any write fails.
// Returns an error if any entry fails to encode or write.
func (fs *FileStorage) writeEntries(file *os.File, entries []*pb.LogEntry, rollbackPos int64) error {
	for _, entry := range entries {
		record, err := fs.encodeEntry(entry)
		if err != nil {
			return ErrCorruptedData
		}
		if _, err := file.Write(record); err != nil {
			_ = file.Truncate(rollbackPos) // rollback on failure
			return err
		}
	}
	return nil
}

// encodeEntry serializes a log entry using Protocol Buffers and prefixes it with its length.
// Returns the encoded bytes or an error if serialization fails.
func (fs *FileStorage) encodeEntry(entry *pb.LogEntry) ([]byte, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}

	lenPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lenPrefix, uint32(len(data)))
	return append(lenPrefix, data...), nil
}

// isFirstEntry checks if there are no log entries stored yet.
// Returns true if both FirstIndex and LastIndex are 0, false otherwise.
func (fs *FileStorage) isFirstEntry() bool {
	return fs.metadata.FirstIndex == 0 && fs.metadata.LastIndex == 0
}

// GetEntries retrieves log entries in the range [low, high).
// Adjusts the requested range to available entries and returns an empty slice if range is invalid.
// Returns entries in the adjusted range or an error if context is canceled,
// low > high (ErrIndexOutOfRange), or the log file cannot be read.
func (fs *FileStorage) GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if low > high {
		return nil, ErrIndexOutOfRange
	}
	fs.logMu.RLock()
	defer fs.logMu.RUnlock()
	first := fs.metadata.FirstIndex
	last := fs.metadata.LastIndex
	if first == 0 || high <= first || low > last {
		return []*pb.LogEntry{}, nil
	}
	low = max(low, first)
	high = min(high, last+1)
	if low >= high {
		return []*pb.LogEntry{}, nil
	}
	file, err := os.Open(fs.logFile())
	if err != nil {
		if os.IsNotExist(err) {
			return []*pb.LogEntry{}, nil
		}
		return nil, err
	}
	defer file.Close()

	return fs.readEntriesInRange(ctx, file, low, high)
}

// readEntriesInRange reads log entries within the specified index range from the provided file.
// Returns the requested entries or an error if context is canceled, the file can't be read completely,
// or an entry can't be parsed (ErrCorruptedData).
func (fs *FileStorage) readEntriesInRange(ctx context.Context, file *os.File, low, high uint64) ([]*pb.LogEntry, error) {
	var entries []*pb.LogEntry
	lenBuf := make([]byte, 4)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Read 4-byte length prefix
		if _, err := io.ReadFull(file, lenBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(lenBuf)
		if length == 0 {
			continue
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, ErrCorruptedData
		}
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return nil, ErrCorruptedData
		}
		// Skip until we reach `low`, stop after reaching `high`
		if entry.Index >= high {
			break
		}
		if entry.Index >= low {
			entries = append(entries, entry)
		}
	}
	return entries, nil
}

// GetEntry retrieves a single log entry at the specified index.
// Returns the entry or ErrNotFound if the index is outside the known range,
// or ErrCorruptedData if the entry exists but can't be read correctly.
// Returns an error if context is canceled or the log file can't be opened.
func (fs *FileStorage) GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error) {
	fs.logMu.RLock()
	defer fs.logMu.RUnlock()
	first := fs.metadata.FirstIndex
	last := fs.metadata.LastIndex
	if index < first || index > last {
		return nil, ErrNotFound
	}
	file, err := os.Open(fs.logFile())
	if err != nil {
		if os.IsNotExist(err) {
			return nil, ErrCorruptedData
		}
		return nil, err
	}
	defer file.Close()
	return fs.readSingleEntry(ctx, file, index)
}

// readSingleEntry scans the file and returns the entry with the exact index.
// Returns the matching entry, ErrCorruptedData if an entry cannot be read correctly,
// or ErrNotFound if no entry with the specified index exists in the file.
// Returns an error if context is canceled.
func (fs *FileStorage) readSingleEntry(ctx context.Context, file *os.File, index uint64) (*pb.LogEntry, error) {
	lenBuf := make([]byte, 4)
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		// Read 4-byte length prefix
		if _, err := io.ReadFull(file, lenBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}
		length := binary.BigEndian.Uint32(lenBuf)
		if length == 0 {
			continue
		}
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, ErrCorruptedData
		}
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return nil, ErrCorruptedData
		}
		if entry.Index == index {
			return entry, nil
		}
		if entry.Index > index {
			break // We've passed the desired index
		}
	}
	return nil, ErrNotFound
}

// LastIndex returns the index of the last log entry.
// Returns 0 if the log is empty.
func (fs *FileStorage) LastIndex() uint64 {
	fs.logMu.RLock()
	defer fs.logMu.RUnlock()
	return fs.metadata.LastIndex
}

// FirstIndex returns the index of the first log entry.
// Returns 0 if the log is empty.
func (fs *FileStorage) FirstIndex() uint64 {
	fs.logMu.RLock()
	defer fs.logMu.RUnlock()
	return fs.metadata.FirstIndex
}

// TruncateSuffix removes all log entries with indices greater than or equal to the given index.
// Does nothing if the index is greater than the last index.
// Returns an error if context is canceled, if file operations fail,
// or if metadata can't be saved.
func (fs *FileStorage) TruncateSuffix(ctx context.Context, index uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fs.logMu.Lock()
	defer fs.logMu.Unlock()
	if index > fs.metadata.LastIndex {
		return nil // No need to truncate
	}
	if index <= fs.metadata.FirstIndex {
		return fs.clearAllLogEntries()
	}
	offset, err := fs.findTruncationOffset(index)
	if err != nil {
		return err
	}
	if err := fs.truncateLogFile(offset); err != nil {
		return err
	}
	fs.metadata.LastIndex = index - 1
	return fs.saveMetadata()
}

// clearAllLogEntries removes all log entries by truncating the file to zero size
// and resetting the metadata indices to zero.
// Returns an error if the file truncation fails or metadata can't be saved.
func (fs *FileStorage) clearAllLogEntries() error {
	if err := os.Truncate(fs.logFile(), 0); err != nil {
		return fmt.Errorf("failed to truncate log file: %w", err)
	}

	fs.metadata.FirstIndex = 0
	fs.metadata.LastIndex = 0
	return fs.saveMetadata()
}

// findTruncationOffset scans the log file to find the byte offset for truncation.
// Returns the byte offset position where entries with index >= the given index start,
// or an error if the file can't be read, entries can't be parsed, or context is canceled.
func (fs *FileStorage) findTruncationOffset(index uint64) (int64, error) {
	file, err := os.Open(fs.logFile())
	if err != nil {
		return 0, fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()

	// Current position in file
	var position int64 = 0
	// Track the last entry's starting position
	var lastEntryPosition int64 = 0
	// Flag to track if we've seen any entries
	seenEntries := false

	lenBuf := make([]byte, 4)
	for {
		// Remember position before reading length prefix
		currentPos := position

		n, err := io.ReadFull(file, lenBuf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return 0, fmt.Errorf("failed to read length prefix: %w", err)
		}
		if n < 4 {
			return 0, fmt.Errorf("incomplete length prefix, read only %d bytes", n)
		}

		// Move position past length prefix
		position += 4

		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return 0, fmt.Errorf("failed to read entry data: %w", err)
		}

		// Move position past data
		position += int64(length)

		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return 0, fmt.Errorf("failed to unmarshal log entry: %w", err)
		}

		// We've seen at least one entry
		seenEntries = true

		// Always update lastEntryPosition to the start of the current entry
		lastEntryPosition = currentPos

		if entry.Index >= index {
			// If this entry's index is >= target index, return the position
			// where this entry starts
			return currentPos, nil
		}
	}

	// If we're looking for an index beyond the last entry,
	// return the position of the last entry if we've seen entries
	if seenEntries && index > fs.metadata.LastIndex {
		return lastEntryPosition, nil
	}

	// Otherwise, return the current position (end of file)
	return position, nil
}

// truncateLogFile truncates the log file at the specified byte offset.
// Returns an error if the truncation operation fails.
func (fs *FileStorage) truncateLogFile(offset int64) error {
	if err := os.Truncate(fs.logFile(), offset); err != nil {
		return fmt.Errorf("failed to truncate log file at offset %d: %w", offset, err)
	}
	return nil
}

// TruncatePrefix removes all log entries with indices less than the given index.
// Preserves the last entry if no entries would remain after truncation.
// Returns an error if context is canceled, if the log is empty (ErrEmptyLog),
// if file operations fail, or if metadata can't be saved.
func (fs *FileStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	fs.logMu.Lock()
	defer fs.logMu.Unlock()
	if index <= fs.metadata.FirstIndex {
		return nil // No need to truncate
	}
	if _, err := os.Stat(fs.logFile()); os.IsNotExist(err) {
		return ErrEmptyLog
	}
	entries, err := fs.readEntriesForPrefixTruncation(index)
	if err != nil {
		return err
	}
	if len(entries) == 0 {
		return ErrEmptyLog
	}

	if err := fs.writeEntriesToNewFile(entries); err != nil {
		return err
	}

	fs.metadata.FirstIndex = entries[0].Index
	return fs.saveMetadata()
}

// readEntriesForPrefixTruncation reads all entries with indices >= the given index.
// If no matching entries exist, returns the last entry in the log to preserve state.
// Returns the filtered entries or an error if file operations fail or entries can't be parsed.
func (fs *FileStorage) readEntriesForPrefixTruncation(index uint64) ([]*pb.LogEntry, error) {
	file, err := os.Open(fs.logFile())
	if err != nil {
		return nil, fmt.Errorf("failed to open log file: %w", err)
	}
	defer file.Close()
	var entries []*pb.LogEntry
	var lastEntry *pb.LogEntry
	lenBuf := make([]byte, 4)
	for {
		n, err := io.ReadFull(file, lenBuf)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("failed to read length prefix: %w", err)
		}
		if n < 4 {
			return nil, fmt.Errorf("incomplete length prefix, read only %d bytes", n)
		}
		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, fmt.Errorf("failed to read entry data: %w", err)
		}
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return nil, fmt.Errorf("failed to unmarshal log entry: %w", err)
		}
		// Keep track of the last entry we've seen
		lastEntry = entry
		if entry.Index >= index {
			entries = append(entries, entry)
		}
	}
	// If no entries were found with index >= target index but we have entries,
	// preserve the last entry to maintain the log's state
	if len(entries) == 0 && lastEntry != nil {
		entries = append(entries, lastEntry)
	}
	return entries, nil
}

// writeEntriesToNewFile writes entries to a temporary file and replaces the original log file.
// Uses atomic file operations to ensure durability.
// Returns an error if any file operation fails or entries can't be encoded.
func (fs *FileStorage) writeEntriesToNewFile(entries []*pb.LogEntry) error {
	tmpFile := fs.logFile() + ".tmp"
	tmp, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, OwnRWOthR)
	if err != nil {
		return fmt.Errorf("failed to create temporary file: %w", err)
	}
	defer tmp.Close()
	for _, entry := range entries {
		record, err := fs.encodeEntry(entry)
		if err != nil {
			return fmt.Errorf("failed to encode entry: %w", err)
		}

		if _, err := tmp.Write(record); err != nil {
			return fmt.Errorf("failed to write entry to temporary file: %w", err)
		}
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("failed to sync temporary file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("failed to close temporary file: %w", err)
	}
	if err := os.Rename(tmpFile, fs.logFile()); err != nil {
		return fmt.Errorf("failed to replace log file: %w", err)
	}
	return nil
}

// Close releases resources associated with the storage.
// This is a no-op for FileStorage but implements the Storage interface.
// Always returns nil.
func (fs *FileStorage) Close() error {
	return nil
}
