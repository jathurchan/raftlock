package raft

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	pb "github.com/jathurchan/raftlock/proto"
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

// Persists metadata about the log structure.
func (fs *FileStorage) saveMetadata() error {
	return ErrNotImplemented
}

// Appends one or more log entries to the Raft log.
func (fs *FileStorage) AppendEntries(ctx context.Context, entries []*pb.LogEntry) error {
	return ErrNotImplemented
}

// Retrieves a slice of log entries within the specified index range [low, high).
// The 'low' index is inclusive, and the 'high' index is exclusive.
// Adjusts range to available entries. Returns empty slice if no entries in range.
// Returns ErrIndexOutOfRange if low > high.
// Assumes log entries are stored in sequential order by index.
func (fs *FileStorage) GetEntries(ctx context.Context, low, high uint64) ([]*pb.LogEntry, error) {
	return nil, ErrNotImplemented
}

// Retrieves a single log entry at the given index.
func (fs *FileStorage) GetEntry(ctx context.Context, index uint64) (*pb.LogEntry, error) {
	return nil, ErrNotImplemented
}

// Returns the index of the last log entry.
func (fs *FileStorage) LastIndex() uint64 {
	return 0
}

// FirstIndex returns the index of the first log entry.
func (fs *FileStorage) FirstIndex() uint64 {
	return 0
}

// Removes all log entries with indices greater than or equal to the given index.
// Used when conflicting entries are found during log replication.
func (fs *FileStorage) TruncateSuffix(ctx context.Context, index uint64) error {
	return ErrNotImplemented
}

// TruncatePrefix removes all log entries with indices less than the given index.
func (fs *FileStorage) TruncatePrefix(ctx context.Context, index uint64) error {
	return ErrNotImplemented
}

// Releases resources (no-op for FileStorage).
func (fs *FileStorage) Close() error {
	return nil
}
