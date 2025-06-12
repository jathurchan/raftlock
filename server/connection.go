package server

import (
	"sync"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
)

// ConnectionInfo holds metadata about a gRPC client connection.
type ConnectionInfo struct {
	RemoteAddr   string    // Client's remote address
	ConnectedAt  time.Time // Time the connection was established
	LastActive   time.Time // Last time a request was received
	RequestCount int64     // Total number of requests from this connection
}

// ConnectionManager manages gRPC client connections and their lifecycle.
type ConnectionManager interface {
	// Registers a new connection
	OnConnect(remoteAddr string)

	// Removes an existing connection
	OnDisconnect(remoteAddr string)

	// Updates activity for a connection
	OnRequest(remoteAddr string)

	// Returns the number of active connections
	GetActiveConnections() int

	// Returns a snapshot of all connections
	GetAllConnectionInfo() map[string]ConnectionInfo
}

// connectionManager is the default implementation of ConnectionManager.
type connectionManager struct {
	mu sync.RWMutex

	// Active connections keyed by remote address
	connections map[string]*ConnectionInfo

	metrics ServerMetrics
	logger  logger.Logger
	clock   raft.Clock
}

// NewConnectionManager returns a new ConnectionManager.
// Uses the provided metrics, logger, and clock. Falls back to raft.StandardClock if none is given.
func NewConnectionManager(
	metrics ServerMetrics,
	logger logger.Logger,
	clock raft.Clock,
) ConnectionManager {
	if clock == nil {
		clock = raft.NewStandardClock()
		logger.Warnw(
			"ConnectionManager initialized with default raft.StandardClock (no clock provided)",
		)
	}
	return &connectionManager{
		connections: make(map[string]*ConnectionInfo),
		metrics:     metrics,
		logger:      logger.WithComponent("connection-manager"),
		clock:       clock,
	}
}

// OnConnect registers a new client connection.
func (cm *connectionManager) OnConnect(remoteAddr string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.connections[remoteAddr]; exists {
		cm.logger.Warnw("Connection already exists", "remote_addr", remoteAddr)
		return
	}

	now := cm.clock.Now()
	cm.connections[remoteAddr] = &ConnectionInfo{
		RemoteAddr:   remoteAddr,
		ConnectedAt:  now,
		LastActive:   now,
		RequestCount: 0,
	}
	total := len(cm.connections)
	if cm.metrics != nil {
		cm.metrics.SetActiveConnections(total)
	}
	cm.logger.Debugw("New client connection", "remote_addr", remoteAddr, "total_connections", total)
}

// OnDisconnect unregisters a client connection.
func (cm *connectionManager) OnDisconnect(remoteAddr string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	if _, exists := cm.connections[remoteAddr]; exists {
		delete(cm.connections, remoteAddr)
		if cm.metrics != nil {
			cm.metrics.SetActiveConnections(len(cm.connections))
		}
		cm.logger.Debugw(
			"Client connection closed",
			"remote_addr",
			remoteAddr,
			"total_connections",
			len(cm.connections),
		)
	}
}

// OnRequest updates the last activity and request count for a connection.
// Logs if the request is from an unknown connection.
func (cm *connectionManager) OnRequest(remoteAddr string) {
	now := cm.clock.Now()

	cm.mu.Lock()
	defer cm.mu.Unlock()

	conn, exists := cm.connections[remoteAddr]
	if !exists {
		cm.logger.Debugw("Received request for unknown connection", "remote_addr", remoteAddr)
		return
	}

	conn.LastActive = now
	conn.RequestCount++
}

// GetActiveConnections returns the current number of active connections.
func (cm *connectionManager) GetActiveConnections() int {
	cm.mu.RLock()
	defer cm.mu.RUnlock()
	return len(cm.connections)
}

// GetAllConnectionInfo returns a copy of all current connection info.
func (cm *connectionManager) GetAllConnectionInfo() map[string]ConnectionInfo {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	infos := make(map[string]ConnectionInfo, len(cm.connections))
	for addr, connInfoPtr := range cm.connections {
		infos[addr] = *connInfoPtr
	}
	return infos
}
