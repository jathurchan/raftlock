package types

import "time"

// NodeID uniquely identifies a Raft node within a cluster.
// It should be globally unique and remain stable across restarts.
type NodeID string

// Term represents a Raft term, which is a monotonically increasing number
// used to determine leadership and maintain log consistency across nodes.
type Term uint64

// Index represents a position in the Raft log.
// Log indices start at 1 and increase with each appended entry.
type Index uint64

// NodeRole represents the possible roles of a Raft node.
type NodeRole int

const (
	// RoleFollower is the default role of a Raft node at startup.
	// - Followers respond to requests from other nodes but do not initiate actions themselves.
	// - Receiving valid heartbeats (AppendEntries RPCs) from a leader resets their election timeout.
	// - Lack of heartbeats causes a follower to transition to Candidate and start an election.
	RoleFollower NodeRole = iota

	// RoleCandidate is the role a node assumes after an election timeout.
	// - Candidates initiate new elections by incrementing their term and sending RequestVote RPCs.
	// - A Candidate becomes Leader if it wins a majority of votes.
	// - It reverts to Follower upon discovering a current leader or higher term.
	// - If an election is inconclusive, it starts a new election cycle.
	RoleCandidate

	// RoleLeader is the role a node assumes after winning an election.
	// - Leaders send periodic heartbeats (AppendEntries RPCs) to maintain authority.
	// - They handle client requests and replicate log entries to followers.
	// - If a leader observes a higher term, it steps down to Follower.
	RoleLeader
)

// LogEntry represents a single entry in the Raft log.
type LogEntry struct {
	Term    Term   // Term when the entry was created
	Index   Index  // Position of the entry in the log
	Command []byte // Command to apply to the replicated state machine
}

// RaftStatus represents the current state of a Raft node for monitoring and debugging.
type RaftStatus struct {
	ID       NodeID   // This node's identifier
	Role     NodeRole // Current role (Leader, Follower, Candidate)
	Term     Term     // Current term
	LeaderID NodeID   // Current leader's ID (if known, empty if unknown)

	LastLogIndex Index // Highest log entry index in the node's log
	LastLogTerm  Term  // Term of the highest log entry
	CommitIndex  Index // Highest index known to be committed
	LastApplied  Index // Highest index applied to state machine

	SnapshotIndex Index // Index of the last snapshot
	SnapshotTerm  Term  // Term of the last snapshot

	Replication map[NodeID]PeerState
}

// PeerState tracks the replication state for a single peer.
type PeerState struct {
	NextIndex  Index     // Next index to send to this peer
	MatchIndex Index     // Highest log entry known to be replicated on server
	IsActive   bool      // Whether the peer is responding
	LastActive time.Time // When the peer was last known to be active
}

// ApplyMsg is sent on the ApplyChannel once an entry is committed and applied.
// It can represent either a command applied or a snapshot installed.
type ApplyMsg struct {
	CommandValid bool   // True if this message contains a valid command
	Command      []byte // The command applied
	CommandIndex Index  // The log index of the applied command
	CommandTerm  Term   // The term of the log entry containing the command

	SnapshotValid bool   // True if this message contains a snapshot
	Snapshot      []byte // The snapshot data
	SnapshotIndex Index  // The log index included in the snapshot
	SnapshotTerm  Term   // The term of the last log entry included in the snapshot
}

// IndexOffsetPair maps a Raft log index to its file offset.
// Used for efficient log access and compaction.
type IndexOffsetPair struct {
	Index  Index // Log index
	Offset int64 // File offset
}

// PersistentState defines the state that must be saved to stable storage
// before responding to RPCs.
type PersistentState struct {
	// The latest term the current node has seen.
	// Starts at 0 and is incremented monotonically.
	CurrentTerm Term `json:"current_term"`
	// The Candidate the current node voted for in the current term.
	// Empty if the node hasn't voted.
	VotedFor NodeID `json:"voted_for"`
}

// SnapshotMetadata holds information about the most recent snapshot.
type SnapshotMetadata struct {
	LastIncludedIndex Index `json:"last_included_index"`
	LastIncludedTerm  Term  `json:"last_included_term"`
}

// RequestVoteArgs encapsulates the arguments for the RequestVote RPC.
type RequestVoteArgs struct {
	Term         Term   // Candidate's term
	CandidateID  NodeID // Candidate requesting vote (Empty if none)
	LastLogIndex Index  // Index of candidate's last log entry
	LastLogTerm  Term   // Term of candidate's last log entry
	IsPreVote    bool   // Flag for pre-vote phase
}

// RequestVoteReply encapsulates the reply for the RequestVote RPC.
type RequestVoteReply struct {
	Term        Term // Current term, for candidate to update itself
	VoteGranted bool // True means candidate received vote
}

// AppendEntriesArgs encapsulates the arguments for the AppendEntries RPC.
// Can also function as a heartbeat if Entries is empty.
type AppendEntriesArgs struct {
	Term         Term       // Leader's term
	LeaderID     NodeID     // So follower can redirect clients
	PrevLogIndex Index      // Index of log entry immediately preceding new ones
	PrevLogTerm  Term       // Term of prevLogIndex entry
	Entries      []LogEntry // Log entries to store (empty for heartbeat)
	LeaderCommit Index      // Leader's commitIndex
}

// AppendEntriesReply encapsulates the reply for the AppendEntries RPC.
type AppendEntriesReply struct {
	Term          Term  // Current term, for leader to update itself
	Success       bool  // True if follower contained matching entry
	ConflictIndex Index // Recommended nextIndex for leader if Success=false (optimization)
	ConflictTerm  Term  // Term of the conflicting entry if Success=false (optimization)
	MatchIndex    Index // Follower's last matching index (optimization for faster updates)
}

// InstallSnapshotArgs encapsulates the arguments for the InstallSnapshot RPC.
type InstallSnapshotArgs struct {
	Term              Term   // Leader's term
	LeaderID          NodeID // So follower can redirect clients (Empty if none)
	LastIncludedIndex Index  // Snapshot replaces entries up to this index
	LastIncludedTerm  Term   // Term of lastIncludedIndex
	Data              []byte // Snapshot chunk data
}

// InstallSnapshotReply encapsulates the reply for the InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term Term // Current term, for leader to update itself
}

// PeerConnectionStatus describes the current health of a peer connection.
type PeerConnectionStatus struct {
	Connected   bool      // True if connection likely exists.
	LastError   error     // Last significant error; nil if none.
	LastActive  time.Time // Last successful communication timestamp.
	PendingRPCs int       // Number of in-flight RPCs.
}
