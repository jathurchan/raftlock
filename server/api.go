package server

import (
	"context"

	pb "github.com/jathurchan/raftlock/proto"
)

// RaftLockServer defines the interface for a Raft-backed distributed lock service.
// It provides strong consistency guarantees by leveraging the Raft consensus algorithm.
//
// The server handles client requests, validates them, redirects to the leader if needed,
// and interfaces with the Raft-based lock manager.
type RaftLockServer interface {
	pb.RaftLockServer

	// Start initializes and runs the gRPC server and all underlying components,
	// including the Raft node and background workers.
	//
	// Returns an error if initialization fails (e.g., port conflict, Raft failure).
	Start(ctx context.Context) error

	// Stop gracefully shuts down the server and all components.
	// The provided context can set a deadline for shutdown.
	Stop(ctx context.Context) error

	// IsLeader reports whether this node is currently the Raft leader.
	// Only the leader can handle write operations.
	IsLeader() bool

	// LeaderAddress returns the address of the current Raft leader.
	// Returns an empty string if the leader is unknown.
	GetLeaderAddress() string

	// NodeID returns the unique Raft node ID of this server.
	GetNodeID() string

	// Metrics returns current metrics for observability and monitoring.
	Metrics() ServerMetrics
}
