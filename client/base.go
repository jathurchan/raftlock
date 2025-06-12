package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// connector defines an interface for establishing gRPC connections.
// Useful for injecting mocks in tests.
type connector interface {
	// GetConnection returns a cached gRPC connection or creates a new one.
	GetConnection(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error)
}

// grpcConnector implements the default connector.
type grpcConnector struct{}

// GetConnection establishes a new gRPC connection to the given endpoint.
func (c *grpcConnector) GetConnection(endpoint string, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	return grpc.NewClient(endpoint, opts...)
}

// baseClient defines the internal RaftLock client interface.
// It manages retries, leader tracking, and metrics.
type baseClient interface {
	// executeWithRetry runs an operation with retry, backoff, and leader redirection.
	executeWithRetry(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error

	// getCurrentLeader returns the current known leader address.
	getCurrentLeader() string

	// setCurrentLeader sets the current leader address.
	setCurrentLeader(leader string)

	// isConnected reports whether the client has active connections.
	isConnected() bool

	// close releases all resources and shuts down the client.
	close() error

	// setRetryPolicy sets the retry policy for operations.
	setRetryPolicy(policy RetryPolicy)

	// setRand sets the random source used for jitter in backoff calculations.
	setRand(r raft.Rand)

	// getMetrics returns the metrics recorder used by the client.
	getMetrics() Metrics

	// setConnector replaces the connector (mainly for testing).
	setConnector(connector connector)
}

// baseClientImpl provides the default implementation of baseClient.
type baseClientImpl struct {
	config    Config
	endpoints []string

	mu            sync.RWMutex
	conns         map[string]*grpc.ClientConn
	currentLeader string

	metrics   Metrics
	closed    atomic.Bool
	clock     raft.Clock
	rand      raft.Rand
	connector connector

	tryEndpointFunc func(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error
}

// newBaseClient creates a new base client with the given configuration.
func newBaseClient(config Config) (baseClient, error) {
	if len(config.Endpoints) == 0 {
		return nil, errors.New("at least one endpoint must be provided")
	}
	c := &baseClientImpl{
		config:    config,
		endpoints: config.Endpoints,
		conns:     make(map[string]*grpc.ClientConn),
		clock:     raft.NewStandardClock(),
		rand:      raft.NewStandardRand(),
		connector: &grpcConnector{},
	}
	if config.EnableMetrics {
		c.metrics = newMetrics()
	} else {
		c.metrics = &noOpMetrics{}
	}
	return c, nil
}

// setRetryPolicy updates the client's retry policy in a thread-safe manner.
func (c *baseClientImpl) setRetryPolicy(policy RetryPolicy) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.config.RetryPolicy = policy
}

// setRand replaces the random source, typically for testing.
func (c *baseClientImpl) setRand(r raft.Rand) {
	c.rand = r
}

// getMetrics returns the client’s metrics collector.
func (c *baseClientImpl) getMetrics() Metrics {
	return c.metrics
}

// buildDialOptions returns gRPC dial options based on the current configuration.
func (c *baseClientImpl) buildDialOptions() []grpc.DialOption {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                c.config.KeepAlive.Time,
			Timeout:             c.config.KeepAlive.Timeout,
			PermitWithoutStream: c.config.KeepAlive.PermitWithoutStream,
		}),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(c.config.MaxMessageSize),
			grpc.MaxCallSendMsgSize(c.config.MaxMessageSize),
		),
	}
}

// getConnection returns a cached connection or establishes a new one.
func (c *baseClientImpl) getConnection(endpoint string) (*grpc.ClientConn, error) {
	if c.closed.Load() {
		return nil, ErrClientClosed
	}

	c.mu.RLock()
	if conn, ok := c.conns[endpoint]; ok {
		c.mu.RUnlock()
		return conn, nil
	}
	c.mu.RUnlock()

	dialOpts := c.buildDialOptions()

	c.mu.Lock()
	defer c.mu.Unlock()
	if conn, ok := c.conns[endpoint]; ok {
		return conn, nil
	}

	conn, err := c.connector.GetConnection(endpoint, dialOpts...)
	if err != nil {
		return nil, fmt.Errorf("failed to dial %s: %w", endpoint, err)
	}
	c.conns[endpoint] = conn
	return conn, nil
}

// executeWithRetry runs an operation with retry logic, including backoff and leader redirection.
func (c *baseClientImpl) executeWithRetry(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
	if c.closed.Load() {
		return ErrClientClosed
	}
	start := c.clock.Now()
	defer c.metrics.ObserveLatency(operation, c.clock.Since(start))

	c.mu.RLock()
	maxRetries := c.config.RetryPolicy.MaxRetries
	c.mu.RUnlock()

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := c.tryOperation(ctx, operation, fn)
		if err == nil {
			c.metrics.IncrSuccess(operation)
			return nil
		}
		lastErr = err

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if c.handleLeaderRedirect(err) {
			c.metrics.IncrLeaderRedirect()
			attempt-- // Redirects are not counted as retries.
			continue
		}

		if !c.isRetryable(err) || attempt == maxRetries {
			break
		}

		c.metrics.IncrRetry(operation)
		backoff := c.calculateBackoff(attempt + 1)

		select {
		case <-c.clock.After(backoff):
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	c.metrics.IncrFailure(operation)
	return fmt.Errorf("operation %q failed after %d attempts: %w", operation, maxRetries, lastErr)
}

// tryOperation attempts the operation on the current leader or iterates over all endpoints.
func (c *baseClientImpl) tryOperation(ctx context.Context, operation string, fn func(context.Context, pb.RaftLockClient) error) error {
	leader := c.getCurrentLeader()
	if leader != "" {
		err := c.tryEndpoint(ctx, leader, fn)
		detail := extractErrorDetail(err)
		if err == nil || (detail != nil && detail.Code != pb.ErrorCode_NOT_LEADER) {
			return err
		}
		c.setCurrentLeader("")
	}

	var lastErr error
	for _, endpoint := range c.endpoints {
		err := c.tryEndpoint(ctx, endpoint, fn)
		if err == nil {
			c.setCurrentLeader(endpoint)
			return nil
		}
		lastErr = err
	}

	if lastErr != nil {
		return lastErr
	}
	return fmt.Errorf("no available servers for operation %s", operation)
}

// tryEndpoint invokes the operation on the specified endpoint.
func (c *baseClientImpl) tryEndpoint(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
	if c.tryEndpointFunc != nil {
		return c.tryEndpointFunc(ctx, endpoint, fn)
	}

	conn, err := c.getConnection(endpoint)
	if err != nil {
		return err
	}
	client := pb.NewRaftLockClient(conn)

	c.mu.RLock()
	timeout := c.config.RequestTimeout
	c.mu.RUnlock()

	reqCtx, cancel := ctx, context.CancelFunc(func() {})
	if timeout > 0 {
		reqCtx, cancel = context.WithTimeout(ctx, timeout)
	}
	defer cancel()

	return fn(reqCtx, client)
}

// calculateBackoff computes exponential backoff with optional jitter.
func (c *baseClientImpl) calculateBackoff(attempt int) time.Duration {
	c.mu.RLock()
	policy := c.config.RetryPolicy
	c.mu.RUnlock()

	backoff := float64(policy.InitialBackoff)
	for i := 1; i < attempt; i++ {
		backoff *= policy.BackoffMultiplier
	}
	if backoff > float64(policy.MaxBackoff) {
		backoff = float64(policy.MaxBackoff)
	}

	if policy.JitterFactor > 0 {
		jitter := (c.rand.Float64()*2 - 1) * policy.JitterFactor * backoff
		backoff += jitter
	}

	if backoff < 0 {
		return 0
	}
	return time.Duration(backoff)
}

// isRetryable returns true if the error is considered retryable by config or gRPC code.
func (c *baseClientImpl) isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		}
	}
	c.mu.RLock()
	retryable := c.config.RetryPolicy.RetryableErrors
	c.mu.RUnlock()

	if detail := extractErrorDetail(err); detail != nil {
		return slices.Contains(retryable, detail.Code)
	}
	return false
}

// handleLeaderRedirect checks for a NOT_LEADER error and updates the leader address.
func (c *baseClientImpl) handleLeaderRedirect(err error) bool {
	detail := extractErrorDetail(err)
	if detail == nil || detail.Code != pb.ErrorCode_NOT_LEADER {
		return false
	}
	if addr, ok := detail.Details["leader_address"]; ok && addr != "" {
		c.setCurrentLeader(addr)
		return true
	}
	return false
}

// getCurrentLeader returns the most recently known leader address.
func (c *baseClientImpl) getCurrentLeader() string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.currentLeader
}

// setCurrentLeader updates the cached leader address.
func (c *baseClientImpl) setCurrentLeader(leader string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.currentLeader != leader {
		c.currentLeader = leader
	}
}

// isConnected reports whether there are any active connections.
func (c *baseClientImpl) isConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return len(c.conns) > 0
}

// close shuts down all gRPC connections and marks the client as closed.
func (c *baseClientImpl) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return ErrClientClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	var errs []error
	for ep, conn := range c.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection to %s: %w", ep, err))
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)

	if len(errs) > 0 {
		return fmt.Errorf("errors while closing connections: %v", errs)
	}
	return nil
}

// extractErrorDetail extracts a RaftLock error detail from a gRPC status error.
func extractErrorDetail(err error) *pb.ErrorDetail {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, d := range st.Details() {
		if ed, ok := d.(*pb.ErrorDetail); ok {
			return ed
		}
	}
	return nil
}

// setConnector replaces the connector (mainly for testing).
func (c *baseClientImpl) setConnector(conn connector) {
	c.connector = conn
}
