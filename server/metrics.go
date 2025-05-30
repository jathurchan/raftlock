package server

import (
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/types"
)

// ServerMetrics defines observability hooks for RaftLock server operations.
// Metrics cover gRPC lifecycle, Raft proposals, validation, error types, and system health.
// All methods must be safe for concurrent use.
type ServerMetrics interface {
	// IncrGRPCRequest increments the count for an RPC method invocation.
	// 'method' should match a RaftLock RPC (e.g., "Acquire", "Release").
	// 'success' should reflect overall success from the client’s perspective.
	IncrGRPCRequest(method string, success bool)

	// IncrLeaderRedirect increments the count of requests redirected to the current leader.
	// Typically applies to write operations when the node is not the Raft leader.
	IncrLeaderRedirect(method string)

	// IncrRetry increments a counter when a client retries a failed or redirected request.
	IncrRetry(method string)

	// IncrRaftProposal increments proposal submission counters.
	// 'operation' should reflect the type of lock operation (Acquire, Release, Renew).
	// 'success' indicates whether the proposal was committed by Raft.
	IncrRaftProposal(operation types.LockOperation, success bool)

	// IncrValidationError increments validation failure counters.
	// 'method' is the RPC where validation failed.
	// 'errorType' is a string like "invalid_ttl", "missing_field", or "bad_lock_id".
	IncrValidationError(method string, errorType string)

	// IncrClientError increments counts for errors caused by invalid client input or actions.
	// This typically maps to RaftLock ErrorCodes in the 100–199 or 300–399 range.
	IncrClientError(method string, errorCode pb.ErrorCode)

	// IncrServerError increments counts for internal server-side errors.
	// 'errorType' might include values like "internal_error", "raft_unavailable", "timeout".
	IncrServerError(method string, errorType string)

	// IncrQueueOverflow increments a counter when a queue exceeds its configured capacity.
	IncrQueueOverflow(queueType string)

	// IncrLockExpiration increments a counter when a lock expires due to TTL.
	IncrLockExpiration()

	// ObserveRequestLatency records end-to-end latency for a gRPC method call.
	// This includes validation, Raft proposal (if applicable), and response formatting.
	ObserveRequestLatency(method string, latency time.Duration)

	// ObserveRaftProposalLatency records the latency from submitting a Raft proposal
	// to it being either committed or rejected.
	ObserveRaftProposalLatency(operation types.LockOperation, latency time.Duration)

	// ObserveQueueLength tracks the current size of internal queues (e.g., gRPC, Raft).
	// Typical queue types: "grpc_requests", "raft_proposals", "wait_queue".
	ObserveQueueLength(queueType string, length int)

	// ObserveRequestSize records the raw size of incoming gRPC requests in bytes.
	ObserveRequestSize(method string, sizeBytes int)

	// ObserveResponseSize records the raw size of outgoing gRPC responses in bytes.
	ObserveResponseSize(method string, sizeBytes int)

	// IncrConcurrentRequests adjusts the count of concurrently active requests.
	// Use delta +1 at request start, -1 when completed.
	IncrConcurrentRequests(method string, delta int)

	// IncrHealthCheck increments the count of health check invocations.
	// 'healthy' reflects the result of the check.
	IncrHealthCheck(healthy bool)

	// SetServerState sets leadership and health state gauges.
	// 'isLeader' reflects current Raft role; 'isHealthy' reflects readiness for requests.
	SetServerState(isLeader bool, isHealthy bool)

	// SetActiveConnections sets the number of live gRPC connections to this server.
	SetActiveConnections(count int)

	// SetRaftTerm sets the current Raft term for this node, useful for tracking elections.
	SetRaftTerm(term types.Term)

	// SetRaftCommitIndex sets the commit index last acknowledged by this Raft node.
	SetRaftCommitIndex(index types.Index)

	// Reset clears all metric counters and resets gauges.
	// Useful primarily in unit or integration tests.
	Reset()
}

// NoOpServerMetrics provides a no-operation implementation of ServerMetrics.
// All methods are empty and safe for concurrent use.
type NoOpServerMetrics struct{}

// NewNoOpServerMetrics creates a new no-operation metrics implementation.
func NewNoOpServerMetrics() ServerMetrics {
	return &NoOpServerMetrics{}
}

func (n *NoOpServerMetrics) IncrGRPCRequest(method string, success bool)                  {}
func (n *NoOpServerMetrics) IncrLeaderRedirect(method string)                             {}
func (n *NoOpServerMetrics) IncrRetry(method string)                                      {}
func (n *NoOpServerMetrics) IncrRaftProposal(operation types.LockOperation, success bool) {}
func (n *NoOpServerMetrics) IncrValidationError(method string, errorType string)          {}
func (n *NoOpServerMetrics) IncrClientError(method string, errorCode pb.ErrorCode)        {}
func (n *NoOpServerMetrics) IncrServerError(method string, errorType string)              {}
func (n *NoOpServerMetrics) IncrQueueOverflow(queueType string)                           {}
func (n *NoOpServerMetrics) IncrLockExpiration()                                          {}
func (n *NoOpServerMetrics) ObserveRequestLatency(method string, latency time.Duration)   {}
func (n *NoOpServerMetrics) ObserveRaftProposalLatency(operation types.LockOperation, latency time.Duration) {
}
func (n *NoOpServerMetrics) ObserveQueueLength(queueType string, length int)  {}
func (n *NoOpServerMetrics) ObserveRequestSize(method string, sizeBytes int)  {}
func (n *NoOpServerMetrics) ObserveResponseSize(method string, sizeBytes int) {}
func (n *NoOpServerMetrics) IncrConcurrentRequests(method string, delta int)  {}
func (n *NoOpServerMetrics) IncrHealthCheck(healthy bool)                     {}
func (n *NoOpServerMetrics) SetServerState(isLeader bool, isHealthy bool)     {}
func (n *NoOpServerMetrics) SetActiveConnections(count int)                   {}
func (n *NoOpServerMetrics) SetRaftTerm(term types.Term)                      {}
func (n *NoOpServerMetrics) SetRaftCommitIndex(index types.Index)             {}
func (n *NoOpServerMetrics) Reset()                                           {}
