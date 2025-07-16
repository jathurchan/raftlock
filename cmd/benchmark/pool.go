package main

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// clientPool manages a pool of gRPC connections to RaftLock servers,
// supporting health checks, connection reuse, and basic load balancing.
type clientPool struct {
	sync.RWMutex
	addrs        []string            // Server addresses
	pools        [][]*grpcConnection // Connection pools per server
	poolSize     int                 // Number of connections per server
	healthyNodes int                 // Count of currently healthy nodes
	next         uint32              // Index for round-robin selection

	healthStatus        map[string]bool // Server health status by address
	lastHealthCheck     time.Time       // Timestamp of last health check
	healthCheckInterval time.Duration   // Interval between health checks
}

// grpcConnection wraps a gRPC connection with metadata used for load balancing and health tracking.
type grpcConnection struct {
	conn     *grpc.ClientConn  // Raw gRPC connection
	client   pb.RaftLockClient // gRPC client for RaftLock service
	addr     string            // Server address
	healthy  atomic.Bool       // Health flag
	lastUsed atomic.Value      // Timestamp of last use (time.Time)
}

// newClientPool initializes a new client pool with the given addresses and pool size.
func newClientPool(addrs []string, poolSize int) *clientPool {
	if poolSize <= 0 {
		poolSize = 1
	}
	return &clientPool{
		addrs:               addrs,
		pools:               make([][]*grpcConnection, len(addrs)),
		poolSize:            poolSize,
		healthStatus:        make(map[string]bool),
		healthCheckInterval: 30 * time.Second, // Default health check interval
	}
}

// connect establishes a pool of connections to each server.
func (p *clientPool) connect() error {
	p.Lock()
	defer p.Unlock()

	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                10 * time.Second,
			Timeout:             3 * time.Second,
			PermitWithoutStream: true,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(4*1024*1024),
			grpc.MaxCallSendMsgSize(4*1024*1024),
		),
	}

	for i, addr := range p.addrs {
		p.pools[i] = make([]*grpcConnection, p.poolSize)

		for j := range p.poolSize {
			conn, err := grpc.NewClient(addr, opts...)
			if err != nil {
				p.closePartialConnections(i, j)
				return NewConnectionError(addr, "dial", err)
			}

			grpcConn := &grpcConnection{
				conn:   conn,
				client: pb.NewRaftLockClient(conn),
				addr:   addr,
			}
			grpcConn.healthy.Store(true)
			grpcConn.lastUsed.Store(time.Now())
			p.pools[i][j] = grpcConn
		}
		p.healthStatus[addr] = true
	}
	return nil
}

// closePartialConnections cleans up during failed initialization
func (p *clientPool) closePartialConnections(serverIndex, connectionIndex int) {
	for i := 0; i <= serverIndex; i++ {
		maxJ := p.poolSize
		if i == serverIndex {
			maxJ = connectionIndex
		}

		for j := 0; j < maxJ; j++ {
			if p.pools[i][j] != nil && p.pools[i][j].conn != nil {
				_ = p.pools[i][j].conn.Close()
			}
		}
	}
}

// validateClusterHealth checks the health of each server and updates internal health tracking.
// Returns an error if the cluster does not have quorum.
func (p *clientPool) validateClusterHealth(ctx context.Context) error {
	var healthyNodes int32
	var wg sync.WaitGroup

	p.RLock()
	poolsCopy := make([][]*grpcConnection, len(p.pools))
	for i := range p.pools {
		poolsCopy[i] = p.pools[i]
	}
	p.RUnlock()

	for i, pool := range poolsCopy {
		wg.Add(1)
		go func(serverIndex int, connections []*grpcConnection) {
			defer wg.Done()

			addr := p.addrs[serverIndex]

			if len(connections) > 0 && connections[0] != nil {
				healthCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()

				if _, err := connections[0].client.Health(healthCtx, &pb.HealthRequest{}); err == nil {
					atomic.AddInt32(&healthyNodes, 1)

					p.Lock()
					p.healthStatus[addr] = true
					p.Unlock()

					// Mark all connections as healthy
					for _, conn := range connections {
						if conn != nil {
							conn.healthy.Store(true)
						}
					}
				} else {
					p.Lock()
					p.healthStatus[addr] = false
					p.Unlock()

					// Mark all connections as unhealthy
					for _, conn := range connections {
						if conn != nil {
							conn.healthy.Store(false)
						}
					}
				}
			}
		}(i, pool)
	}

	wg.Wait()

	p.Lock()
	p.healthyNodes = int(healthyNodes)
	p.lastHealthCheck = time.Now()
	p.Unlock()

	requiredNodes := (len(p.addrs) / 2) + 1
	if p.healthyNodes < requiredNodes {
		return ErrInsufficientNodes
	}

	return nil
}

// getHealthyConnection selects a healthy client using round-robin.
// Falls back to any available connection if none are marked healthy.
func (p *clientPool) getHealthyConnection() pb.RaftLockClient {
	p.RLock()
	defer p.RUnlock()

	attempts := 0
	totalConnections := len(p.pools) * p.poolSize

	for attempts < totalConnections {
		next := atomic.AddUint32(&p.next, 1)
		serverIndex := int(next) % len(p.pools)
		connIndex := int(next/uint32(len(p.pools))) % p.poolSize

		if serverIndex < len(p.pools) && connIndex < len(p.pools[serverIndex]) {
			conn := p.pools[serverIndex][connIndex]
			if conn != nil && conn.healthy.Load() {
				conn.lastUsed.Store(time.Now())
				return conn.client
			}
		}

		attempts++
	}

	// Fallback: return any available connection
	for _, pool := range p.pools {
		for _, conn := range pool {
			if conn != nil {
				conn.lastUsed.Store(time.Now())
				return conn.client
			}
		}
	}

	return nil
}

// get returns a client for a specific server index.
func (p *clientPool) get(serverIndex int) pb.RaftLockClient {
	p.RLock()
	defer p.RUnlock()

	if serverIndex >= 0 && serverIndex < len(p.pools) && len(p.pools[serverIndex]) > 0 {
		connIndex := int(atomic.AddUint32(&p.next, 1)) % len(p.pools[serverIndex])
		conn := p.pools[serverIndex][connIndex]
		if conn != nil {
			conn.lastUsed.Store(time.Now())
			return conn.client
		}
	}

	return nil
}

// getRoundRobin returns a client using round-robin selection among healthy nodes.
func (p *clientPool) getRoundRobin() pb.RaftLockClient {
	return p.getHealthyConnection()
}

// findLeader queries nodes to identify the current Raft leader.
// Returns the leader client and its address.
func (p *clientPool) findLeader() (pb.RaftLockClient, string) {
	p.RLock()
	defer p.RUnlock()

	// Use a longer timeout for leader discovery to account for Raft stabilization
	// This timeout should ideally be configurable or derived from benchmark's HealthCheckTimeout
	// For now, hardcode to 10 seconds, matching default HealthCheckTimeout
	leaderDiscoveryTimeout := 10 * time.Second

	for i, pool := range p.pools {
		if len(pool) == 0 {
			continue
		}

		conn := pool[0]
		if conn == nil || !conn.healthy.Load() {
			continue
		}

		// Use the longer timeout for the GetStatus call
		ctx, cancel := context.WithTimeout(context.Background(), leaderDiscoveryTimeout) // UPDATED CONTEXT TIMEOUT
		resp, err := conn.client.GetStatus(ctx, &pb.GetStatusRequest{})
		cancel()

		if err == nil && resp != nil && resp.RaftStatus != nil && resp.RaftStatus.Role == "Leader" {
			conn.lastUsed.Store(time.Now())
			return conn.client, p.addrs[i]
		}
	}

	return nil, ""
}

// findFollower finds a healthy follower node, excluding the current leader.
func (p *clientPool) findFollower() (pb.RaftLockClient, string) {
	_, leaderAddr := p.findLeader()

	p.RLock()
	defer p.RUnlock()

	for i, addr := range p.addrs {
		if addr != leaderAddr && len(p.pools[i]) > 0 {
			conn := p.pools[i][0]
			if conn != nil && conn.healthy.Load() {
				conn.lastUsed.Store(time.Now())
				return conn.client, addr
			}
		}
	}

	return p.getHealthyConnection(), ""
}

// close gracefully shuts down all connections in the pool.
func (p *clientPool) close() {
	p.Lock()
	defer p.Unlock()

	for _, pool := range p.pools {
		for _, conn := range pool {
			if conn != nil && conn.conn != nil {
				_ = conn.conn.Close()
			}
		}
	}
}
