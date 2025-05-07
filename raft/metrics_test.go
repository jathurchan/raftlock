package raft

import (
	"testing"
	"time"

	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

func TestRaftMetrics_NewNoOpMetrics(t *testing.T) {
	metrics := NewNoOpMetrics()
	testutil.AssertNotNil(t, metrics, "NewNoOpMetrics() should not return nil")

	_, ok := metrics.(*noOpMetrics)
	testutil.AssertTrue(t, ok, "Metrics should be of type *noOpMetrics")
}

func TestRaftMetrics_NoOpMetricsDoesNotPanic(t *testing.T) {
	metrics := NewNoOpMetrics()

	metrics.IncCounter("test_counter", "label", "value")
	metrics.AddCounter("test_counter", 42.0, "label", "value")
	metrics.SetGauge("test_gauge", 123.45, "label", "value")
	metrics.ObserveHistogram("test_histogram", 67.89, "label", "value")

	metrics.ObserveCommitIndex(types.Index(100))
	metrics.ObserveAppliedIndex(types.Index(90))

	metrics.ObserveTerm(types.Term(5))
	metrics.ObserveRoleChange(types.RoleLeader, types.RoleCandidate, types.Term(5))

	metrics.ObserveLeaderChange(types.NodeID("node2"), types.Term(5))
	metrics.ObserveLeaderNotificationDropped()
	metrics.ObserveLeadershipLost(types.Term(5), "timeout")

	metrics.ObserveApplyNotificationDropped()
	metrics.ObserveApplyLoopStopped("shutdown")

	metrics.ObserveElectionStart(types.Term(6), ElectionReasonTimeout)
	metrics.ObserveVoteGranted(types.Term(6))
	metrics.ObserveElectionElapsed(types.NodeID("node1"), types.Term(5), 10)

	metrics.ObserveLogState(types.Index(1), types.Index(100), types.Term(5))
	metrics.ObserveLogAppend(10, 50*time.Millisecond, true)
	metrics.ObserveLogRead(LogReadTypeTerm, 20*time.Millisecond, true)
	metrics.ObserveLogTruncate(LogTruncateTypePrefix, 10, 30*time.Millisecond, true)
	metrics.ObserveLogConsistencyError()

	metrics.ObserveProposal(true, ProposalResultSuccess)
	metrics.ObserveReadIndex(true, "lease")

	metrics.ObserveSnapshot(SnapshotActionCapture, SnapshotStatusSuccess, "size", "1024")
	metrics.ObserveSnapshotRecovery(SnapshotStatusSuccess, SnapshotReasonShutdown)

	metrics.ObservePeerReplication(types.NodeID("node2"), true, ReplicationResultSuccess)
	metrics.ObserveHeartbeat(types.NodeID("node2"), true, 15*time.Millisecond)
	metrics.ObserveHeartbeatSent()

	metrics.ObserveAppendEntriesHeartbeat()
	metrics.ObserveAppendEntriesReplication()
	metrics.ObserveAppendEntriesRejected("log_mismatch")
	metrics.ObserveEntriesReceived(5)
	metrics.ObserveCommandBytesReceived(1024)

	metrics.ObserveTick(types.RoleFollower)
	metrics.ObserveComponentStopTimeout("log_manager")
}
