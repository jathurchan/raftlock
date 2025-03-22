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

// Creates a new FileStorage instance.
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

	if err := fs.loadMetadata(); err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	return fs, nil
}

// Returns path to state.json that stores Raft state (current term and votedFor).
func (fs *FileStorage) stateFile() string {
	return filepath.Join(fs.dir, "state.json")
}

// Returns path to metadata.json that stores log metadata (first and last log indices).
func (fs *FileStorage) metadataFile() string {
	return filepath.Join(fs.dir, "metadata.json")
}

// Returns path to log.dat that stores the actual log entries.
// Log entries are stored in a binary format with length prefixes.
// Each entry is serialized using Protocol Buffers and prefixed with a 4-byte length.
func (fs *FileStorage) logFile() string {
	return filepath.Join(fs.dir, "log.dat")
}

// Loads metadata about the log structure
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

// Persists the Raft node's current term and votedFor information to disk.
// Ensures thread safety via locking and crash resilience by writing to a temporary file
// and atomically renaming it to the final state file path.
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

// Retrieves the persisted Raft state from disk, including the current term
// and the candidate the node voted for in that term. If the state file does not exist,
// it returns the default initial state. Returns an error if the file is unreadable
// or contains corrupted data.
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
			return RaftState{CurrentTerm: 0, VotedFor: -1}, nil
		}
		return state, err
	}
	if err := json.Unmarshal(data, &state); err != nil {
		return state, ErrCorruptedData
	}
	return state, nil
}

// Helper method to write the current log metadata to disk atomically.
// It first marshals the metadata to JSON, writes it to a temporary file,
// and then renames it to the actual metadata file name.
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

// Appends one or more log entries to the Raft log.
// It handles locking, updates metadata, writes entries, and persists metadata.
func (fs *FileStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}
	fs.logMu.Lock()
	defer fs.logMu.Unlock()
	if !fs.isFirstEntry() {
		if err := CheckEntriesContiguity(entries, fs.metadata.LastIndex); err != nil {
			return err
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

// Helper method to open the log file in append mode and seeks to the end.
// Returns the file handle and the position to rollback to in case of error.
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

// Helper method to encode and write a batch of log entries to the file.
// If any entry fails to write, the file is truncated to its original position.
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

// Helper method to serialize a log entry and prefix it with its length.
func (fs *FileStorage) encodeEntry(entry *pb.LogEntry) ([]byte, error) {
	data, err := proto.Marshal(entry)
	if err != nil {
		return nil, err
	}

	lenPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(lenPrefix, uint32(len(data)))
	return append(lenPrefix, data...), nil
}

// Helper method to return true if there are no log entries stored yet.
func (fs *FileStorage) isFirstEntry() bool {
	return fs.metadata.FirstIndex == 0 && fs.metadata.LastIndex == 0
}

// Returns log entries in the range [low, high).
// Log indices start at 1. 'low' is inclusive, 'high' is exclusive.
// The range is automatically adjusted to the available entries.
// Returns an empty slice if adjusted range is invalid.
// Returns ErrIndexOutOfRange if low > high.
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
	if first == 0 || low > last || high <= first {
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

// Helper method to read log entries within the [low, high) range from the provided file.
// Assumes entries are length-prefixed and encoded using protobuf.
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

// Retrieves a single log entry at the given index.
// Log indices start at 1.
// Returns ErrNotFound if the index is outside the known range.
// Returns ErrCorruptedData if the entry cannot be read (e.g., file missing, corrupted, or truncated).
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

// Helper method to scan the file and returns the entry with the exact index.
// Returns ErrCorruptedData if an entry cannot be read or parsed correctly.
// Returns ErrNotFound if the entry does not exist in the file.
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

// Returns the index of the last log entry.
func (fs *FileStorage) LastIndex() uint64 {
	fs.logMu.RLock()
	defer fs.logMu.RUnlock()
	return fs.metadata.LastIndex
}

// FirstIndex returns the index of the first log entry.
func (fs *FileStorage) FirstIndex() uint64 {
	fs.logMu.RLock()
	defer fs.logMu.RUnlock()
	return fs.metadata.FirstIndex
}

// Removes all log entries with indices greater than or equal to the given index.
// Used when conflicting entries are found during log replication.
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
		// Remove all log entries
		fs.metadata.FirstIndex = 0
		fs.metadata.LastIndex = 0
		if err := os.Truncate(fs.logFile(), 0); err != nil {
			return err
		}
		return fs.saveMetadata()
	}

	// Truncate the file up to the index
	file, err := os.Open(fs.logFile())
	if err != nil {
		return err
	}
	defer file.Close()

	var offset int64
	lenBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(file, lenBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return err
		}
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return err
		}
		if entry.Index >= index {
			break
		}
		offset += int64(4 + length)
	}

	if err := os.Truncate(fs.logFile(), offset); err != nil {
		return err
	}

	fs.metadata.LastIndex = index - 1
	return fs.saveMetadata()
}

// TruncatePrefix removes all log entries with indices less than the given index.
// If the given index is greater than all existing indices, it preserves only the
// last entry to maintain the log's state. If the index is less than or equal to
// the first entry's index, no truncation occurs.
func (fs *FileStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	if index <= fs.metadata.FirstIndex {
		return nil // No need to truncate
	}

	file, err := os.Open(fs.logFile())
	if err != nil {
		return err
	}
	defer file.Close()

	var newEntries []*pb.LogEntry
	lenBuf := make([]byte, 4)
	for {
		if _, err := io.ReadFull(file, lenBuf); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)
		if _, err := io.ReadFull(file, data); err != nil {
			return err
		}
		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return err
		}
		if entry.Index >= index {
			newEntries = append(newEntries, entry)
		}
	}

	if len(newEntries) == 0 {
		// Remove all log entries
		fs.metadata.FirstIndex = 0
		fs.metadata.LastIndex = 0
		if err := os.Truncate(fs.logFile(), 0); err != nil {
			return err
		}
		return fs.saveMetadata()
	}

	// Write the remaining entries to a new file and replace the old one
	tmpFile := fs.logFile() + ".tmp"
	tmp, err := os.OpenFile(tmpFile, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, OwnRWOthR)
	if err != nil {
		return err
	}
	defer tmp.Close()

	for _, entry := range newEntries {
		record, err := fs.encodeEntry(entry)
		if err != nil {
			return err
		}
		if _, err := tmp.Write(record); err != nil {
			return err
		}
	}

	if err := os.Rename(tmpFile, fs.logFile()); err != nil {
		return err
	}

	fs.metadata.FirstIndex = newEntries[0].Index
	return fs.saveMetadata()
}

// Releases resources (no-op for FileStorage).
func (fs *FileStorage) Close() error {
	return nil
}
