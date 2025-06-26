package raft

import "errors"

var (
	// ErrNotLeader is returned when a follower or candidate receives a request
	// that only the current leader is allowed to handle.
	ErrNotLeader = errors.New("raft: node is not the leader")

	// ErrTermMismatch is returned when a message is rejected due to a stale or incorrect term.
	ErrTermMismatch = errors.New("raft: term mismatch")

	// ErrLogConflict is returned when there is a log inconsistency between nodes,
	// typically during AppendEntries replication.
	ErrLogConflict = errors.New("raft: log conflict")

	// ErrShuttingDown is returned when the node is in the process of shutting down
	// and cannot process requests.
	ErrShuttingDown = errors.New("raft: raft node is shutting down")

	// ErrTimeout is returned when an operation fails to complete within the expected time window.
	ErrTimeout = errors.New("raft: operation timed out")

	// ErrCompacted is returned when requested log entries have been compacted
	// and are no longer available.
	ErrCompacted = errors.New("raft: required log entries are compacted")

	// ErrLeadershipLost is returned when a node loses leadership during a critical operation,
	// such as command application or replication.
	ErrLeadershipLost = errors.New("raft: leadership lost during operation")

	// ErrApplyChanFull is returned when the apply channel (used to deliver committed entries)
	// is full, indicating backpressure or a slow state machine.
	ErrApplyChanFull = errors.New("raft: apply channel is full")

	// ErrNotFound is returned when a requested item (log entry, snapshot, etc.) cannot be found.
	ErrNotFound = errors.New("raft: item not found")

	// ErrSnapshotInProgress is returned when a snapshot operation is already in progress,
	// and a conflicting operation is attempted.
	ErrSnapshotInProgress = errors.New("raft: snapshot already in progress")

	// ErrQuorumUnreachable is returned when the node cannot contact a quorum of peers
	// to complete a consensus operation.
	ErrQuorumUnreachable = errors.New("raft: quorum unreachable")

	// ErrUnavailable is returned when the Raft node is not in a state to serve the request,
	// such as during initialization or recovery.
	ErrUnavailable = errors.New("raft: node is unavailable")

	// ErrStateMismatch is returned when an internal consistency check fails,
	// such as mismatched state machine expectations or log assumptions.
	ErrStateMismatch = errors.New("raft: unexpected state mismatch")

	// ErrPeerNotFound indicates the specified peer ID is not known to the network layer
	// or is not part of the current configuration.
	ErrPeerNotFound = errors.New("raft: peer not found")

	// ErrConfigValidation is returned when the provided Raft configuration fails
	// validation checks (e.g., invalid timeouts, missing required fields).
	ErrConfigValidation = errors.New("raft: config validation error")

	// ErrMissingDependencies is returned when essential components are missing
	// from the configuration provided during Raft node initialization.
	ErrMissingDependencies = errors.New("raft: config missing required dependencies")
)

// election.go

// Error definitions
var (
	ErrElectionInProgress = errors.New("raft: election already in progress")
	ErrInvalidState       = errors.New("raft: invalid state for operation")
	ErrConcurrencyLimit   = errors.New("raft: concurrency limit exceeded")
)
