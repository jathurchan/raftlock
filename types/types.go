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

// NodeRole represents the possible roles of a Raft node.
type NodeRole int

const (
	// RoleFollower is the default role of a Raft node when it starts up.
	// - Followers do not initiate actions on their own and only respond to requests from other nodes.
	// - If a RoleFollower receives a heartbeat (`AppendEntries` RPC) from a valid Leader, it resets its election timeout.
	// - Else, it transitions to Candidate and starts a new election.
	RoleFollower NodeRole = iota

	// RoleCandidate is the role a node enters when it times out without hearing from a Leader.
	// - A RoleCandidate starts a new election by incrementing its term and sending `RequestVote` RPCs to other servers.
	// - If it receives votes from the majority of servers, it transitions to Leader.
	// - If it discovers an existing Leader (via an `AppendEntries` RPC) or detects a higher term in another node's message,
	//   it transitions back to Follower.
	// - If it times out without a majority vote, it starts a new election cycle.
	RoleCandidate

	// RoleLeader is the role a node enters after winning an election.
	// - A RoleLeader sends periodic heartbeats (`AppendEntries` RPCs) to maintain authority over Followers.
	// - It handles client requests and replicates log entries across the cluster.
	// - If it detects a higher term in another server's message, it steps down and transitions back to Follower.
	RoleLeader
)

type LogEntry struct {
	Term    Term
	Index   Index
	Command []byte // Command to be applied to the state machine
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

// IndexOffsetPair maps a log entry index to its file offset.
type IndexOffsetPair struct {
	Index  Index
	Offset int64
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
	Offset            uint64 // Offset where chunk is placed
	Data              []byte // Snapshot chunk data
	Done              bool   // True if this is the last chunk
}

// InstallSnapshotReply encapsulates the reply for the InstallSnapshot RPC.
type InstallSnapshotReply struct {
	Term Term // Current term, for leader to update itself
}
