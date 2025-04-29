package raft

import "github.com/jathurchan/raftlock/types"

// Metrics defines the interface for recording Raft operational metrics.
// Implementations of this interface should be thread-safe.
//
// Labels must be provided in key/value pairs: "key1", "value1", "key2", "value2", etc.
// Metric names should be appropriately namespaced (e.g., "raft_commit_index").
type Metrics interface {
	// IncCounter increments a named counter metric by 1.
	IncCounter(name string, labels ...string)

	// SetGauge sets the value of a named gauge metric.
	SetGauge(name string, value float64, labels ...string)

	// ObserveHistogram records a value in a named histogram metric.
	ObserveHistogram(name string, value float64, labels ...string)

	// ObserveCommitIndex records the latest committed log index.
	// Represented as a gauge.
	// Called after new entries are committed (usually on the leader).
	ObserveCommitIndex(index types.Index)

	// ObserveAppliedIndex records the latest log index applied to the state machine.
	// Represented as a gauge.
	// Called after applying entries to the user state machine.
	ObserveAppliedIndex(index types.Index)

	// ObserveLeaderChange records a leadership change event.
	// Implemented as a counter, labeled by the new leader.
	// newLeader specifies the ID of the new leader.
	// term specifies the Raft term during which the change occurred.
	ObserveLeaderChange(newLeader types.NodeID, term types.Term)

	// ObserveLeaderNotificationDropped records that a leader notification was dropped.
	// Implemented as a counter.
	ObserveLeaderNotificationDropped()

	// ObserveRoleChange records a transition in the node's Raft role.
	// Implemented as a counter labeled by the 'from_role' and 'to_role'.
	// term specifies the Raft term during which the transition occurred.
	ObserveRoleChange(newRole types.NodeRole, oldRole types.NodeRole, term types.Term)

	// ObserveElectionStart records the start of a new election attempt.
	// Implemented as a counter labeled by the reason.
	// term specifies the Raft term for which the election is being held.
	// reason specifies why the election was triggered.
	ObserveElectionStart(term types.Term, reason ElectionReason)

	// ObserveProposal records the outcome of a proposal attempt submitted to the node.
	// Implemented as a counter labeled by the outcome (success/reason).
	// success indicates whether the proposal was successfully committed.
	// reason specifies the outcome category (success or failure type).
	ObserveProposal(success bool, reason ProposalResult)

	// ObserveReadIndex records the outcome of a ReadIndex request processed by the node.
	// Implemented as a counter labeled by success status and path.
	// success indicates whether the ReadIndex operation succeeded.
	// path should be a low-cardinality identifier for the request origin or type.
	ObserveReadIndex(success bool, path string)

	// ObserveSnapshot records a snapshot-related operation.
	// Implemented as a counter labeled by the action and potentially other context.
	// action specifies the type of snapshot operation (e.g., create, apply, send, receive).
	// Additional labels may provide contextual information (e.g., "peer_id" for send/receive).
	ObserveSnapshot(action SnapshotAction, labels ...string)

	// ObservePeerReplication records the outcome of a log replication attempt to a specific peer.
	// Implemented as a counter labeled by peer ID and outcome (success/reason).
	// peerID identifies the target peer.
	// success indicates whether replication succeeded for a specific AppendEntries request/batch.
	// reason specifies the outcome category.
	ObservePeerReplication(peerID types.NodeID, success bool, reason ReplicationResult)

	// ObserveHeartbeat records the result and latency of a heartbeat RPC to a peer.
	// Implemented as both a counter (for success/failure) and a histogram (for latency).
	// peerID identifies the target peer.
	// success indicates whether the heartbeat RPC succeeded.
	// latencyMs is the round-trip time in milliseconds (only relevant if successful, typically).
	ObserveHeartbeat(peerID types.NodeID, success bool, latencyMs float64)
}

// ElectionReason specifies why a new election was triggered.
type ElectionReason string

const (
	// ElectionReasonTimeout indicates the election was triggered by a heartbeat timeout.
	ElectionReasonTimeout ElectionReason = "timeout"

	// ElectionReasonLeaderTransfer indicates the election was triggered by an explicit leadership transfer request.
	ElectionReasonLeaderTransfer ElectionReason = "leader_transfer"

	// ElectionReasonPreVote indicates the election attempt started with a pre-vote phase.
	ElectionReasonPreVote ElectionReason = "pre_vote_initiated"

	// ElectionReasonUnknown indicates the cause of the election is unknown or unspecified.
	ElectionReasonUnknown ElectionReason = "unknown"
)

// ProposalResult specifies the outcome of a proposal submission.
type ProposalResult string

const (
	// ProposalResultSuccess indicates the proposal was successfully committed.
	ProposalResultSuccess ProposalResult = "success"

	// ProposalResultTimeout indicates the proposal timed out before being committed.
	ProposalResultTimeout ProposalResult = "timeout"

	// ProposalResultNotLeader indicates the proposal failed because the node was not the leader.
	ProposalResultNotLeader ProposalResult = "not_leader"

	// ProposalResultDropped indicates the proposal was dropped without processing.
	ProposalResultDropped ProposalResult = "dropped"

	// ProposalResultForwarded indicates the proposal was forwarded to the current leader.
	ProposalResultForwarded ProposalResult = "forwarded"

	// ProposalResultStaleTerm indicates the proposal was rejected due to stale term.
	ProposalResultStaleTerm ProposalResult = "stale_term"

	// ProposalResultOther indicates the proposal failed for an unspecified reason.
	ProposalResultOther ProposalResult = "other"
)

// SnapshotAction specifies the type of snapshot operation being recorded.
type SnapshotAction string

const (
	// SnapshotCreate indicates a snapshot was created.
	SnapshotCreate SnapshotAction = "create"

	// SnapshotCreateFailure indicates that an attempt to create a snapshot failed
	SnapshotCreateFailure SnapshotAction = "create_failure"

	// SnapshotApplySuccess indicates a snapshot was successfully applied
	SnapshotApplySuccess SnapshotAction = "apply_success"

	// SnapshotApplyFailure indicates a snapshot application failed
	SnapshotApplyFailure SnapshotAction = "apply_failure"

	// SnapshotSendSuccess indicates a snapshot was successfully sent to a peer.
	SnapshotSendSuccess SnapshotAction = "send_success"

	// SnapshotSendFailure indicates the snapshot sending failed.
	SnapshotSendFailure SnapshotAction = "send_failure"

	// SnapshotReceiveSuccess indicates the snapshot was successfully received from a peer.
	SnapshotReceiveSuccess SnapshotAction = "receive_success"

	// SnapshotReceiveFailure indicates the snapshot receiving failed.
	SnapshotReceiveFailure SnapshotAction = "receive_failure"
)

// ReplicationResult specifies the outcome of a log replication attempt to a peer.
type ReplicationResult string

const (
	// ReplicationResultSuccess indicates replication succeeded without error.
	ReplicationResultSuccess ReplicationResult = "success"

	// ReplicationResultLagging indicates replication is delayed because the follower is lagging behind.
	ReplicationResultLagging ReplicationResult = "lagging"

	// ReplicationResultStaleTerm indicates replication is rejected due to stale term.
	ReplicationResultStaleTerm ReplicationResult = "stale_term"

	// ReplicationResultTimeout indicates replication failed due to a timeout.
	ReplicationResultTimeout ReplicationResult = "timeout"

	// ReplicationResultSnapshotRequired indicates replication cannot proceed without sending a snapshot.
	ReplicationResultSnapshotRequired ReplicationResult = "snapshot_required"

	// ReplicationResultFailed indicates replication failed due to an unspecified error.
	ReplicationResultFailed ReplicationResult = "failed"
)

// No-op metrics implementation
type noOpMetrics struct{}

func (m *noOpMetrics) IncCounter(name string, labels ...string)                      {}
func (m *noOpMetrics) SetGauge(name string, value float64, labels ...string)         {}
func (m *noOpMetrics) ObserveHistogram(name string, value float64, labels ...string) {}
func (m *noOpMetrics) ObserveCommitIndex(index types.Index)                          {}
func (m *noOpMetrics) ObserveAppliedIndex(index types.Index)                         {}
func (m *noOpMetrics) ObserveLeaderChange(newLeader types.NodeID, term types.Term)   {}
func (m *noOpMetrics) ObserveLeaderNotificationDropped()                             {}
func (m *noOpMetrics) ObserveRoleChange(newRole types.NodeRole, oldRole types.NodeRole, term types.Term) {
}
func (m *noOpMetrics) ObserveElectionStart(term types.Term, reason ElectionReason) {}
func (m *noOpMetrics) ObserveProposal(success bool, reason ProposalResult)         {}
func (m *noOpMetrics) ObserveReadIndex(success bool, path string)                  {}
func (m *noOpMetrics) ObserveSnapshot(action SnapshotAction, labels ...string)     {}
func (m *noOpMetrics) ObservePeerReplication(peerID types.NodeID, success bool, reason ReplicationResult) {
}
func (m *noOpMetrics) ObserveHeartbeat(peerID types.NodeID, success bool, latencyMs float64) {}
