package raft

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/protobuf/proto"
)

const (
	// Permissions for directories (read/write/execute for owner, read/execute for others)
	DirectoryPermissions os.FileMode = 0755
	// Permissions for Raft data files (read/write for owner, read for others)
	RaftFilePermissions os.FileMode = 0644
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
	if err := os.MkdirAll(config.Dir, DirectoryPermissions); err != nil {
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

// Persists the Raft node's current term and votedFor information.
// Ensures thread safety and crash resilience by writing to a temporary file
// first, then renaming it to the target file path.
func (fs *FileStorage) SaveState(ctx context.Context, state RaftState) error {
	fs.stateMu.Lock()
	defer fs.stateMu.Unlock()

	if err := ctx.Err(); err != nil {
		return err
	}

	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	tmpFile := fs.stateFile() + ".tmp"
	if err := os.WriteFile(tmpFile, data, RaftFilePermissions); err != nil {
		return err
	}

	return os.Rename(tmpFile, fs.stateFile())
}

// Retrieves the Raft node's persisted term and votedFor information.
func (fs *FileStorage) LoadState(ctx context.Context) (RaftState, error) {
	fs.stateMu.RLock()
	defer fs.stateMu.RUnlock()

	if err := ctx.Err(); err != nil {
		return RaftState{}, err
	}

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

// Persists metadata about the log structure.
func (fs *FileStorage) saveMetadata() error {
	data, err := json.Marshal(fs.metadata)
	if err != nil {
		return err
	}

	tmpFile := fs.metadataFile() + ".tmp"
	if err := os.WriteFile(tmpFile, data, RaftFilePermissions); err != nil {
		return err
	}

	return os.Rename(tmpFile, fs.metadataFile())
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

// Appends one or more log entries to the Raft log.
func (fs *FileStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	if err := ctx.Err(); err != nil {
		return err
	}

	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	file, err := os.OpenFile(fs.logFile(), os.O_WRONLY|os.O_CREATE|os.O_APPEND, RaftFilePermissions)
	if err != nil {
		return err
	}
	defer file.Close()

	// Record current file position for rollback in case of failure
	pos, err := file.Seek(0, io.SeekEnd)
	if err != nil {
		return err
	}

	// Initialize metadata if this is the first entry
	if fs.metadata.FirstIndex == 0 && fs.metadata.LastIndex == 0 {
		fs.metadata.FirstIndex = entries[0].Index
	}
	fs.metadata.LastIndex = entries[len(entries)-1].Index

	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return ErrCorruptedData
		}

		// Create buffer with 4-byte length prefix + data
		lenPrefix := make([]byte, 4)
		binary.BigEndian.PutUint32(lenPrefix, uint32(len(data)))
		record := append(lenPrefix, data...)

		if _, err := file.Write(record); err != nil {
			_ = file.Truncate(pos) // rollback
			return err
		}
	}

	return fs.saveMetadata()
}

// Retrieves a slice of log entries within the specified index range [low, high).
// The 'low' index is inclusive, and the 'high' index is exclusive.
// Adjusts range to available entries. Returns empty slice if no entries in range.
// Returns ErrIndexOutOfRange if low > high.
// Assumes log entries are stored in sequential order by index.
func (fs *FileStorage) GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error) {
	if low >= high {
		return nil, ErrIndexOutOfRange
	}

	fs.logMu.RLock()
	defer fs.logMu.RUnlock()

	if fs.metadata.FirstIndex == 0 || low < fs.metadata.FirstIndex || high > fs.metadata.LastIndex+1 {
		return []*pb.LogEntry{}, nil
	}

	file, err := os.Open(fs.logFile())
	if err != nil {
		if os.IsNotExist(err) {
			// Log file doesn't exist yet; no entries available.
			return []*pb.LogEntry{}, nil
		}
		return nil, err
	}
	defer file.Close()

	entries := make([]*pb.LogEntry, 0, high-low)
	lenBuf := make([]byte, 4)

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Read the length prefix (4-bytes, big-endian).
		if _, err := io.ReadFull(file, lenBuf); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		length := binary.BigEndian.Uint32(lenBuf)
		data := make([]byte, length)

		// Read the actual entry data.
		if _, err := io.ReadFull(file, data); err != nil {
			return nil, ErrCorruptedData
		}

		entry := &pb.LogEntry{}
		if err := proto.Unmarshal(data, entry); err != nil {
			return nil, ErrCorruptedData
		}

		if entry.Index >= low && entry.Index < high {
			entries = append(entries, entry)
		} else if entry.Index >= high {
			// entries are sequential; no need to read further.
			break
		}
	}

	return entries, nil
}

// Retrieves a single log entry at the given index.
func (fs *FileStorage) GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entryList, err := fs.GetEntries(ctx, index, index+1)
	if err != nil {
		return nil, err
	}
	if len(entryList) == 0 {
		return nil, ErrNotFound
	}
	return entryList[0], nil
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

// Helper method to rewrite the Raft log file to only include entries in the range [low, high).
// It's used by both TruncatePrefix and TruncateSuffix to discard unwanted log entries.
func (fs *FileStorage) truncateLog(ctx context.Context, low, high uint64) error {
	if low >= high {
		// Nothing to write
		return nil
	}

	entries, err := fs.GetEntries(ctx, low, high)
	if err != nil {
		return fmt.Errorf("failed to get entries for truncate: %w", err)
	}

	tmpFile := fs.logFile() + ".tmp"
	file, err := os.Create(tmpFile)
	if err != nil {
		return fmt.Errorf("failed to create temp log file: %w", err)
	}
	defer file.Close()

	bufWriter := bufio.NewWriter(file)
	for _, entry := range entries {
		data, err := proto.Marshal(entry)
		if err != nil {
			return fmt.Errorf("failed to marshal entry: %w", err)
		}

		var lenBuf [4]byte
		binary.BigEndian.PutUint32(lenBuf[:], uint32(len(data)))

		if _, err := bufWriter.Write(lenBuf[:]); err != nil {
			return fmt.Errorf("failed to write entry length: %w", err)
		}
		if _, err := bufWriter.Write(data); err != nil {
			return fmt.Errorf("failed to write entry data: %w", err)
		}
	}

	if err := bufWriter.Flush(); err != nil {
		return fmt.Errorf("failed to flush buffer: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	if len(entries) > 0 {
		fs.metadata.FirstIndex = entries[0].Index
		fs.metadata.LastIndex = entries[len(entries)-1].Index
	} else {
		fs.metadata.FirstIndex = 0
		fs.metadata.LastIndex = 0
	}

	if err := fs.saveMetadata(); err != nil {
		return fmt.Errorf("failed to save metadata: %w", err)
	}

	// Atomically replace the old file with the new one.
	if err := os.Rename(tmpFile, fs.logFile()); err != nil {
		return fmt.Errorf("failed to rename temp log file: %w", err)
	}

	return nil
}

// Removes all log entries with indices greater than or equal to the given index.
// Used when conflicting entries are found during log replication.
func (fs *FileStorage) TruncateSuffix(ctx context.Context, index uint64) error {
	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	if index > fs.metadata.LastIndex {
		return nil
	}

	return fs.truncateLog(ctx, fs.metadata.FirstIndex, index)
}

// TruncatePrefix removes all log entries with indices less than the given index.
func (fs *FileStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	fs.logMu.Lock()
	defer fs.logMu.Unlock()

	if index < fs.metadata.FirstIndex {
		return nil
	}

	return fs.truncateLog(ctx, index+1, fs.metadata.LastIndex+1)
}

// Releases resources (no-op for MemoryStorage).
func (fs *FileStorage) Close() error {
	return nil
}
