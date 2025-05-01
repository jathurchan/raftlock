package raft

import (
	"time"

	"github.com/jathurchan/raftlock/types"
)

// Metrics defines an interface for recording Raft metrics.
// Implementations must be safe for concurrent use.
//
// Labels must be provided in key/value pairs: "key1", "value1", "key2", "value2", etc.
// Metric names should be appropriately namespaced (e.g., "raft_commit_index").
type Metrics interface {
	// IncCounter increments the specified counter metric by 1.
	IncCounter(name string, labels ...string)

	// AddCounter adds the given value to the specified counter metric.
	AddCounter(name string, value float64, labels ...string)

	// SetGauge sets the specified gauge metric to the given value.
	SetGauge(name string, value float64, labels ...string)

	// ObserveHistogram records the given value in the specified histogram metric.
	ObserveHistogram(name string, value float64, labels ...string)

	// ObserveCommitIndex sets the latest committed log index.
	// Gauge: raft_commit_index
	ObserveCommitIndex(index types.Index)

	// ObserveAppliedIndex sets the latest log index applied to the state machine.
	// Gauge: raft_applied_index
	ObserveAppliedIndex(index types.Index)

	// ObserveTerm sets the current Raft term.
	// Gauge: raft_term
	ObserveTerm(term types.Term)

	// ObserveLeaderChange records a leader change.
	// Counter: raft_leader_changes_total (labeled by new leader)
	ObserveLeaderChange(newLeader types.NodeID, term types.Term)

	// ObserveLeaderNotificationDropped records that a leader notification was dropped.
	// Counter: raft_leader_notifications_dropped_total
	ObserveLeaderNotificationDropped()

	// ObserveRoleChange records a Raft role transition.
	// Counter: raft_role_changes_total (labeled by from_role, to_role)
	ObserveRoleChange(newRole types.NodeRole, oldRole types.NodeRole, term types.Term)

	// ObserveElectionStart records the start of a new election.
	// Counter: raft_elections_started_total (labeled by reason)
	ObserveElectionStart(term types.Term, reason ElectionReason)

	// ObserveVoteGranted records that a vote was granted.
	// Counter: raft_votes_granted_total (labeled by term)
	ObserveVoteGranted(term types.Term)

	// ObserveLogState sets log boundary gauges.
	// Gauges: raft_log_first_index, raft_log_last_index, raft_log_last_term
	ObserveLogState(firstIndex, lastIndex types.Index, lastTerm types.Term)

	// ObserveLogAppend records the result of a log append operation.
	// Counters: raft_log_appends_total, raft_log_entries_appended_total (labeled by success)
	// Histogram: raft_log_append_latency_seconds
	ObserveLogAppend(entryCount int, latency time.Duration, success bool)

	// ObserveLogRead records the result of a log read.
	// Counter: raft_log_reads_total (labeled by type and success)
	// Histogram: raft_log_read_latency_seconds (labeled by type)
	ObserveLogRead(readType LogReadType, latency time.Duration, success bool)

	// ObserveLogTruncate records the result of a log truncation.
	// Counters: raft_log_truncates_total, raft_log_entries_truncated_total (labeled by type and success)
	// Histogram: raft_log_truncate_latency_seconds (labeled by type)
	ObserveLogTruncate(truncateType LogTruncateType, entriesRemoved int, latency time.Duration, success bool)

	// ObserveLogConsistencyError records a log consistency issue.
	// Counter: raft_log_consistency_errors_total
	ObserveLogConsistencyError()

	// ObserveElectionElapsed sets the number of ticks since the last election reset.
	// Gauge: raft_election_elapsed_ticks (labeled by node_id and term)
	ObserveElectionElapsed(nodeID types.NodeID, term types.Term, ticks int)

	// ObserveProposal records the result of a proposal.
	// Counter: raft_proposals_total (labeled by result)
	ObserveProposal(success bool, reason ProposalResult)

	// ObserveReadIndex records the result of a ReadIndex request.
	// Counter: raft_read_index_requests_total (labeled by success and path)
	ObserveReadIndex(success bool, path string)

	// ObserveSnapshot records a snapshot operation.
	// Counter: raft_snapshot_operations_total (labeled by action and status)
	ObserveSnapshot(action SnapshotAction, status SnapshotStatus, labels ...string)

	// ObservePeerReplication records the result of a replication to a peer.
	// Counter: raft_peer_replications_total (labeled by peer_id and result)
	ObservePeerReplication(peerID types.NodeID, success bool, reason ReplicationResult)

	// ObserveHeartbeat records the result and latency of a heartbeat to a peer.
	// Counter: raft_peer_heartbeats_total (labeled by peer_id and success)
	// Histogram: raft_peer_heartbeat_latency_seconds (labeled by peer_id)
	ObserveHeartbeat(peerID types.NodeID, success bool, latency time.Duration)
}

// ElectionReason specifies why an election was triggered.
type ElectionReason int

const (
	// ElectionReasonTimeout indicates an election started due to election timeout
	ElectionReasonTimeout ElectionReason = iota
	// ElectionReasonTransfer indicates an election started due to leadership transfer
	ElectionReasonTransfer
	// ElectionReasonRestart indicates an election started after node restart
	ElectionReasonRestart
	// ElectionReasonPreVote indicates a pre-vote phase was initiated
	ElectionReasonPreVote
)

// ProposalResult specifies the outcome of a proposal submission.
type ProposalResult string

const (
	ProposalResultSuccess      ProposalResult = "success"
	ProposalResultTimeout      ProposalResult = "timeout"
	ProposalResultNotLeader    ProposalResult = "not_leader"
	ProposalResultDropped      ProposalResult = "dropped"
	ProposalResultForwarded    ProposalResult = "forwarded"
	ProposalResultStaleTerm    ProposalResult = "stale_term"
	ProposalResultQueueFull    ProposalResult = "queue_full"
	ProposalResultShuttingDown ProposalResult = "shutting_down"
	ProposalResultOther        ProposalResult = "other"
)

// SnapshotAction specifies the type of snapshot operation being recorded.
type SnapshotAction string

const (
	SnapshotActionCreate  SnapshotAction = "create"
	SnapshotActionApply   SnapshotAction = "apply"
	SnapshotActionSend    SnapshotAction = "send"
	SnapshotActionReceive SnapshotAction = "receive"
)

// SnapshotStatus specifies the success or failure of a snapshot operation.
type SnapshotStatus string

const (
	SnapshotStatusSuccess SnapshotStatus = "success"
	SnapshotStatusFailure SnapshotStatus = "failure"
)

// ReplicationResult specifies the outcome of a log replication attempt to a peer.
type ReplicationResult string

const (
	ReplicationResultSuccess          ReplicationResult = "success"
	ReplicationResultLogMismatch      ReplicationResult = "log_mismatch"
	ReplicationResultStaleTerm        ReplicationResult = "stale_term"
	ReplicationResultTimeout          ReplicationResult = "timeout"
	ReplicationResultSnapshotRequired ReplicationResult = "snapshot_required"
	ReplicationResultFailed           ReplicationResult = "failed"
)

// LogReadType specifies the type of log read operation.
type LogReadType string

const (
	LogReadTypeTerm    LogReadType = "term"
	LogReadTypeEntries LogReadType = "entries"
)

// LogTruncateType specifies the type of log truncation operation.
type LogTruncateType string

const (
	LogTruncateTypePrefix LogTruncateType = "prefix"
	LogTruncateTypeSuffix LogTruncateType = "suffix"
)

// No-op metrics implementation
type noOpMetrics struct{}

// NewNoOpMetrics creates a metrics implementation that does nothing.
func NewNoOpMetrics() Metrics {
	return &noOpMetrics{}
}

func (m *noOpMetrics) IncCounter(name string, labels ...string)                      {}
func (m *noOpMetrics) AddCounter(name string, value float64, labels ...string)       {}
func (m *noOpMetrics) SetGauge(name string, value float64, labels ...string)         {}
func (m *noOpMetrics) ObserveHistogram(name string, value float64, labels ...string) {}
func (m *noOpMetrics) ObserveCommitIndex(index types.Index)                          {}
func (m *noOpMetrics) ObserveAppliedIndex(index types.Index)                         {}
func (m *noOpMetrics) ObserveTerm(term types.Term)                                   {}
func (m *noOpMetrics) ObserveLeaderChange(newLeader types.NodeID, term types.Term)   {}
func (m *noOpMetrics) ObserveLeaderNotificationDropped()                             {}
func (m *noOpMetrics) ObserveRoleChange(newRole types.NodeRole, oldRole types.NodeRole, term types.Term) {
}
func (m *noOpMetrics) ObserveElectionStart(term types.Term, reason ElectionReason) {}
func (m *noOpMetrics) ObserveVoteGranted(term types.Term)                          {}
func (m *noOpMetrics) ObserveLogState(firstIndex, lastIndex types.Index, lastTerm types.Term) {
}
func (m *noOpMetrics) ObserveLogAppend(entryCount int, latency time.Duration, success bool) {
}
func (m *noOpMetrics) ObserveLogRead(readType LogReadType, latency time.Duration, success bool) {
}
func (m *noOpMetrics) ObserveLogTruncate(truncateType LogTruncateType, entriesRemoved int, latency time.Duration, success bool) {
}
func (m *noOpMetrics) ObserveLogConsistencyError()                                            {}
func (m *noOpMetrics) ObserveElectionElapsed(nodeID types.NodeID, term types.Term, ticks int) {}
func (m *noOpMetrics) ObserveProposal(success bool, reason ProposalResult)                    {}
func (m *noOpMetrics) ObserveReadIndex(success bool, path string)                             {}
func (m *noOpMetrics) ObserveSnapshot(action SnapshotAction, status SnapshotStatus, labels ...string) {
}
func (m *noOpMetrics) ObservePeerReplication(peerID types.NodeID, success bool, reason ReplicationResult) {
}
func (m *noOpMetrics) ObserveHeartbeat(peerID types.NodeID, success bool, latency time.Duration) {}
