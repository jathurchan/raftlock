package types

// NodeID uniquely identifies a Raft node within a cluster.
// It should be globally unique and remain stable across restarts.
type NodeID string

// Term represents a Raft term, which is a monotonically increasing number
// used to determine leadership and maintain log consistency across nodes.
type Term uint64

// Index represents a position in the Raft log.
// Log indices start at 1 and increase with each appended entry.
type Index uint64

type LogEntry struct {
	Term    Term   // Term when entry was received by leader
	Index   Index  // Index in the log
	Command []byte // Command to be applied to the state machine
}

// IndexOffsetPair maps a log entry index to its file offset.
type IndexOffsetPair struct {
	Index  Index
	Offset int64
}

// PersistentState represents the Raft persistent state
type PersistentState struct {
	// The latest term the current node has seen.
	// Starts at 0 and is incremented monotonically.
	CurrentTerm Term `json:"current_term"`
	// The Candidate the current node voted for in the current term.
	// Empty if the node hasn't voted.
	VotedFor NodeID `json:"voted_for"`
}

// SnapshotMetadata contains information about a snapshot
type SnapshotMetadata struct {
	LastIncludedIndex Index `json:"last_included_index"`
	LastIncludedTerm  Term  `json:"last_included_term"`
}

type RequestVoteRequest struct {
	Term         Term   // Candidate's term
	CandidateID  NodeID // Candidate requesting vote (Empty if none)
	LastLogIndex Index  // Index of candidate's last log entry
	LastLogTerm  Term   // Term of candidate's last log entry
}

type RequestVoteResponse struct {
	Term        Term // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

type AppendEntriesRequest struct {
	Term         Term       // Leader's term
	LeaderID     NodeID     // So follower can redirect clients
	PrevLogIndex Index      // Index of log entry immediately preceding new ones
	PrevLogTerm  Term       // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit Index      // Leader's commitIndex
}

type AppendEntriesResponse struct {
	Term    Term // Current term, for leader to update itself
	Success bool // True if follower contained matching entry
}

type InstallSnapshotRequest struct {
	Term              Term   // Leader's term
	LeaderID          NodeID // So follower can redirect clients (Empty if none)
	LastIncludedIndex Index  // Snapshot replaces entries up to this index
	LastIncludedTerm  Term   // Term of lastIncludedIndex
	Offset            uint64 // Offset where chunk is placed
	Data              []byte // Snapshot chunk data
	Done              bool   // True if this is the last chunk
}

type InstallSnapshotResponse struct {
	Term Term // Current term, for leader to update itself
}

// ApplyMsg is sent by Raft to the application to apply a committed command or install a snapshot.
type ApplyMsg struct {
	// For committed log entries
	Command   interface{} // The command to apply
	Index     Index       // Log index of the command
	Term      Term        // Term when the command was committed
	IsCommand bool        // True if this message carries a log entry

	// For snapshot installation
	Snapshot      []byte // Snapshot data
	SnapshotIndex Index  // Last included log index in the snapshot
	SnapshotTerm  Term   // Term of the last included index
	IsSnapshot    bool   // True if this message carries a snapshot

	// Optional: signal when the message has been applied
	Done chan struct{} // Non-nil if Raft expects a notification when done
}
