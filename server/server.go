package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/lock"
	"github.com/jathurchan/raftlock/logger"
	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/storage"
	"google.golang.org/grpc"
)

// raftLockServer implements the RaftLockServer interface.
// It integrates gRPC handling, Raft consensus, and a lock manager
// to provide a strongly consistent, distributed lock service.
type raftLockServer struct {
	pb.UnimplementedRaftLockServer

	// Configuration used to set up and run the server.
	config ServerConfig

	raftNode    raft.Raft        // Raft consensus instance.
	lockManager lock.LockManager // Manages application-level lock state.
	storage     storage.Storage  // Persists Raft logs and snapshots.

	grpcServer *grpc.Server // gRPC server.
	started    atomic.Bool  // True if the server has started.
	stopped    atomic.Bool  // True if the server is stopped.
	startTime  time.Time    // Time when the server was created or started.

	mu         sync.RWMutex  // Guards mutable server state not managed by Raft.
	shutdownCh chan struct{} // Signals internal goroutines to exit.

	logger  logger.Logger
	metrics ServerMetrics

	lastHealthy atomic.Bool // Tracks most recent health status.
}

// NewRaftLockServer creates a new RaftLock server instance using the given config.
// The server is not started automatically—call Start() to initialize components
// and begin serving requests.
func NewRaftLockServer(config ServerConfig) (RaftLockServer, error) {
	if err := config.Validate(); err != nil {
		return nil, fmt.Errorf("invalid server configuration: %w", err)
	}

	actualLogger := config.Logger
	if actualLogger == nil {
		actualLogger = logger.NewNoOpLogger()
	}
	actualMetrics := config.Metrics
	if actualMetrics == nil {
		actualMetrics = NewNoOpServerMetrics()
	}

	server := &raftLockServer{
		config:     config,
		shutdownCh: make(chan struct{}),
		logger:     actualLogger.WithComponent("server"),
		metrics:    actualMetrics,
		startTime:  time.Now(),
	}
	server.lastHealthy.Store(false) // Not healthy until Start() completes.

	return server, nil
}

// Start implements RaftLockServer.Start
func (s *raftLockServer) Start(ctx context.Context) error {
	panic("not implemented")
}

// Stop implements RaftLockServer.Stop
func (s *raftLockServer) Stop(ctx context.Context) error {
	panic("not implemented")
}

// IsLeader implements RaftLockServer.IsLeader
func (s *raftLockServer) IsLeader() bool {
	panic("not implemented")
}

// GetLeaderAddress implements RaftLockServer.GetLeaderAddress
func (s *raftLockServer) GetLeaderAddress() string {
	panic("not implemented")
}

// GetNodeID implements RaftLockServer.GetNodeID
func (s *raftLockServer) GetNodeID() string {
	panic("not implemented")
}

// Metrics implements RaftLockServer.Metrics
func (s *raftLockServer) Metrics() ServerMetrics {
	panic("not implemented")
}

// Acquire implements pb.RaftLockServer.Acquire
func (s *raftLockServer) Acquire(ctx context.Context, req *pb.AcquireRequest) (*pb.AcquireResponse, error) {
	panic("not implemented")
}

// Release implements pb.RaftLockServer.Release
func (s *raftLockServer) Release(ctx context.Context, req *pb.ReleaseRequest) (*pb.ReleaseResponse, error) {
	panic("not implemented")
}

// Renew implements pb.RaftLockServer.Renew
func (s *raftLockServer) Renew(ctx context.Context, req *pb.RenewRequest) (*pb.RenewResponse, error) {
	panic("not implemented")
}

// GetLockInfo implements pb.RaftLockServer.GetLockInfo
func (s *raftLockServer) GetLockInfo(ctx context.Context, req *pb.GetLockInfoRequest) (*pb.GetLockInfoResponse, error) {
	panic("not implemented")
}

// GetLocks implements pb.RaftLockServer.GetLocks
func (s *raftLockServer) GetLocks(ctx context.Context, req *pb.GetLocksRequest) (*pb.GetLocksResponse, error) {
	panic("not implemented")
}

// EnqueueWaiter implements pb.RaftLockServer.EnqueueWaiter
func (s *raftLockServer) EnqueueWaiter(ctx context.Context, req *pb.EnqueueWaiterRequest) (*pb.EnqueueWaiterResponse, error) {
	panic("not implemented")
}

// CancelWait implements pb.RaftLockServer.CancelWait
func (s *raftLockServer) CancelWait(ctx context.Context, req *pb.CancelWaitRequest) (*pb.CancelWaitResponse, error) {
	panic("not implemented")
}

// GetBackoffAdvice implements pb.RaftLockServer.GetBackoffAdvice
func (s *raftLockServer) GetBackoffAdvice(ctx context.Context, req *pb.BackoffAdviceRequest) (*pb.BackoffAdviceResponse, error) {
	panic("not implemented")
}

// GetStatus implements pb.RaftLockServer.GetStatus
func (s *raftLockServer) GetStatus(ctx context.Context, req *pb.GetStatusRequest) (*pb.GetStatusResponse, error) {
	panic("not implemented")
}
