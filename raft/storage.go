package raft

import (
	"errors"

	pb "github.com/jathurchan/raftlock/proto"
)

var (
	ErrNotFound        = errors.New("raft: entry not found")
	ErrCorruptedData   = errors.New("raft: corrupted data")
	ErrIndexOutOfRange = errors.New("raft: index out of range")
)

// RaftState represents the persistent state of a Raft node.
type RaftState struct {
	CurrentTerm uint64 // The current term of the Raft node.
	VotedFor    int    // Candidate ID that received vote in current term (-1 if none)
}

// Storage defines the interface for persisting Raft node state and log entries.
type Storage interface {
	// Persists the Raft node's current term and votedFor information.
	// Crucial for ensuring consistency and recovery.
	// The operation must be atomic: either the entire state is saved, or nothing.
	// Returns an error if the save operation fails.
	SaveState(state RaftState) error

	// Retrieves the Raft node's persisted term and votedFor information.
	// Essential for Raft to resume operation from the last known state upon startup or recovery.
	// Returns the RaftState and an ErrCorruptedData error if loading fails.
	LoadState() (RaftState, error)

	// Appends one or more log entries to the Raft log.
	// Core functionality for Raft's log replication.
	// The operation must be atomic: either all entries are appended, or none are.
	// Returns an error if the append operation fails.
	AppendEntries(entries []*pb.LogEntry) error

	// Retrieves a slice of log entries within the specified index range [low, high).
	// The 'low' index is inclusive, and the 'high' index is exclusive.
	// Used for log replication, snapshotting, and leader election.
	// Returns the slice of log entries and an error, or ErrIndexOutOfRange if low > high.
	// Returns an empty slice if no entries match the range.
	GetEntries(low, high uint64) ([]*pb.LogEntry, error)

	// Retrieves a single log entry at the given index.
	// Useful for retrieving specific log entries.
	// Returns the log entry and an error.
	// Returns ErrNotFound if the entry does not exist at the specified index.
	GetEntry(index uint64) (*pb.LogEntry, error)

	// Returns the index of the last log entry in the Raft log.
	// Crucial for log replication and other operations.
	// Returns 0 if the log is empty.
	// Returns an error if the operation fails.
	LastIndex() (uint64, error)

	// Returns the index of the first log entry in the Raft log.
	// Essential for log compaction and snapshotting.
	// Returns 0 if the log is empty.
	// Returns an error if the operation fails.
	FirstIndex() (uint64, error)

	// Removes all log entries with indices greater than or equal to the given index.
	// Necessary for log consistency when a follower's log diverges from the leader's.
	// Returns an error if the operation fails.
	TruncateSuffix(index uint64) error

	// Removes all log entries with indices less than or equal to the given index.
	// Essential for log compaction to discard old log entries.
	// Returns an error if the operation fails.
	TruncatePrefix(index uint64) error
}
