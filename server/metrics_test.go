package server

import (
	"testing"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/types"
)

func TestNoOpServerMetrics(t *testing.T) {
	metrics := NewNoOpServerMetrics()

	metrics.IncrGRPCRequest("TestMethod", true)
	metrics.IncrGRPCRequest("TestMethod", false)

	metrics.IncrLeaderRedirect("TestMethod")

	metrics.IncrRetry("TestMethod")

	metrics.IncrRaftProposal(types.OperationAcquire, true)
	metrics.IncrRaftProposal(types.OperationRelease, false)

	metrics.IncrValidationError("TestMethod", "test_error_type")

	metrics.IncrClientError("TestMethod", pb.ErrorCode_INVALID_ARGUMENT)

	metrics.IncrServerError("TestMethod", "internal_test_error")

	metrics.IncrQueueOverflow("test_queue")

	metrics.IncrLockExpiration()

	metrics.ObserveRequestLatency("TestMethod", 100*time.Millisecond)

	metrics.ObserveRaftProposalLatency(types.OperationRenew, 50*time.Millisecond)

	metrics.ObserveQueueLength("test_queue", 10)

	metrics.ObserveRequestSize("TestMethod", 1024)

	metrics.ObserveResponseSize("TestMethod", 512)

	metrics.IncrConcurrentRequests("TestMethod", 1)
	metrics.IncrConcurrentRequests("TestMethod", -1)

	metrics.IncrHealthCheck(true)
	metrics.IncrHealthCheck(false)

	metrics.SetServerState(true, true)
	metrics.SetServerState(false, false)

	metrics.SetActiveConnections(5)

	metrics.SetRaftTerm(types.Term(1))

	metrics.SetRaftCommitIndex(types.Index(100))

	metrics.Reset()
}
