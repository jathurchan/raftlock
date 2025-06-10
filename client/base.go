package client

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"slices"

	pb "github.com/jathurchan/raftlock/proto"
	"github.com/jathurchan/raftlock/raft"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/status"
)

// baseClient defines the core client interface used internally by higher-level clients.
// It enables mocking of client logic in tests.
type baseClient interface {
	// executeWithRetry performs an operation with retries and leader redirection.
	executeWithRetry(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error

	// getCurrentLeader returns the current leader address.
	getCurrentLeader() string

	// setCurrentLeader updates the leader address.
	setCurrentLeader(leader string)

	// isConnected reports whether there are any active connections.
	isConnected() bool

	// close shuts down all connections and marks the client as closed.
	close() error

	// setRetryPolicy replaces the current retry policy.
	setRetryPolicy(policy RetryPolicy)

	// setRand sets a custom random source, used primarily for testing.
	setRand(r raft.Rand)

	// getMetrics returns the active metrics collector.
	getMetrics() Metrics
}

// baseClientImpl implements baseClient and provides core functionality shared
// by all RaftLock clients, including connection management, retry logic, and metrics.
type baseClientImpl struct {
	config    Config
	endpoints []string // Initial list of server endpoints.

	mu            sync.RWMutex                // Protects conns and currentLeader
	conns         map[string]*grpc.ClientConn // Active gRPC connections by endpoint
	currentLeader string                      // Address of the last known leader

	metrics Metrics
	closed  atomic.Bool // Tracks whether the client has been closed
	rand    raft.Rand   // Source of randomness for backoff jitter (mockable for tests)
}

// newBaseClient creates and initializes a defaultClient using the provided config.
func newBaseClient(config Config) (baseClient, error) {
	if len(config.Endpoints) == 0 {
		return nil, errors.New("at least one endpoint must be provided")
	}

	client := &baseClientImpl{
		config:    config,
		endpoints: config.Endpoints,
		conns:     make(map[string]*grpc.ClientConn),
		rand:      raft.NewStandardRand(),
	}

	if config.EnableMetrics {
		client.metrics = newMetrics()
	} else {
		client.metrics = &noOpMetrics{}
	}

	return client, nil
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

// getConnection returns a gRPC connection to the specified endpoint,
// creating it if one doesn’t already exist.
func (c *baseClientImpl) getConnection(endpoint string) (*grpc.ClientConn, error) {
	if c.closed.Load() {
		return nil, ErrClientClosed
	}

	c.mu.RLock()
	if conn, exists := c.conns[endpoint]; exists {
		c.mu.RUnlock()
		return conn, nil
	}
	c.mu.RUnlock()

	c.mu.Lock()
	defer c.mu.Unlock()

	if conn, exists := c.conns[endpoint]; exists {
		return conn, nil
	}

	conn, err := grpc.NewClient(endpoint, c.buildDialOptions()...)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize client connection to %s: %w", endpoint, err)
	}

	c.conns[endpoint] = conn
	return conn, nil
}

// buildDialOptions constructs gRPC dial options from the client's config.
func (c *baseClientImpl) buildDialOptions() []grpc.DialOption {
	opts := []grpc.DialOption{
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

	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	return opts
}

// executeWithRetry runs a gRPC operation with retry and optional leader redirection.
func (c *baseClientImpl) executeWithRetry(ctx context.Context, operation string, fn func(ctx context.Context, client pb.RaftLockClient) error) error {
	start := time.Now()
	defer func() {
		c.metrics.ObserveLatency(operation, time.Since(start))
	}()

	var lastErr error
	for attempt := 0; attempt <= c.config.RetryPolicy.MaxRetries; attempt++ {
		if c.closed.Load() {
			return ErrClientClosed
		}

		if attempt > 0 {
			c.metrics.IncrRetry(operation)
			backoff := c.calculateBackoff(attempt)
			select {
			case <-time.After(backoff):
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		err := c.tryOperation(ctx, operation, fn)
		if err == nil {
			c.metrics.IncrSuccess(operation)
			return nil
		}
		lastErr = err

		if !c.isRetryable(err) {
			break
		}

		if c.handleLeaderRedirect(err) {
			c.metrics.IncrLeaderRedirect()
			continue
		}
	}

	c.metrics.IncrFailure(operation)
	return lastErr
}

// tryOperation attempts the operation on the known leader first,
// then falls back to all other endpoints.
func (c *baseClientImpl) tryOperation(ctx context.Context, operation string, fn func(context.Context, pb.RaftLockClient) error) error {
	if leader := c.getCurrentLeader(); leader != "" {
		if err := c.tryEndpoint(ctx, leader, fn); err == nil {
			return nil
		}
	}

	for _, endpoint := range c.endpoints {
		if endpoint == c.getCurrentLeader() {
			continue
		}
		if err := c.tryEndpoint(ctx, endpoint, fn); err == nil {
			c.setCurrentLeader(endpoint)
			return nil
		}
	}
	return fmt.Errorf("no available servers for operation %s", operation)
}

// tryEndpoint runs the operation against a single endpoint.
func (c *baseClientImpl) tryEndpoint(ctx context.Context, endpoint string, fn func(context.Context, pb.RaftLockClient) error) error {
	conn, err := c.getConnection(endpoint)
	if err != nil {
		return err
	}

	client := pb.NewRaftLockClient(conn)
	reqCtx := ctx
	if c.config.RequestTimeout > 0 {
		var cancel context.CancelFunc
		reqCtx, cancel = context.WithTimeout(ctx, c.config.RequestTimeout)
		defer cancel()
	}
	return fn(reqCtx, client)
}

// calculateBackoff computes a backoff duration for the given retry attempt, including jitter.
func (c *baseClientImpl) calculateBackoff(attempt int) time.Duration {
	policy := c.config.RetryPolicy
	backoff := policy.InitialBackoff
	for i := 1; i < attempt; i++ {
		backoff = time.Duration(float64(backoff) * policy.BackoffMultiplier)
	}
	if backoff > policy.MaxBackoff {
		backoff = policy.MaxBackoff
	}
	if policy.JitterFactor > 0 {
		jitter := time.Duration(policy.JitterFactor * float64(backoff) * (c.rand.Float64()*2 - 1))
		backoff += jitter
	}
	if backoff < 0 {
		return 0
	}
	return backoff
}

// isRetryable returns true if the given error is considered transient and retryable.
func (c *baseClientImpl) isRetryable(err error) bool {
	st, ok := status.FromError(err)
	if ok {
		switch st.Code() {
		case codes.Unavailable, codes.DeadlineExceeded, codes.ResourceExhausted:
			return true
		}
	}
	if errDetail := extractErrorDetail(err); errDetail != nil {
		if slices.Contains(c.config.RetryPolicy.RetryableErrors, errDetail.Code) {
			return true
		}
	}
	return false
}

// handleLeaderRedirect updates the current leader address based on error metadata.
func (c *baseClientImpl) handleLeaderRedirect(err error) bool {
	errDetail := extractErrorDetail(err)
	if errDetail == nil || errDetail.Code != pb.ErrorCode_NOT_LEADER {
		return false
	}
	if leaderAddr, ok := errDetail.Details["leader_address"]; ok && leaderAddr != "" {
		c.setCurrentLeader(leaderAddr)
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

// close closes all connections and marks the client as closed.
func (c *baseClientImpl) close() error {
	if !c.closed.CompareAndSwap(false, true) {
		return nil
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	var errs []error
	for endpoint, conn := range c.conns {
		if err := conn.Close(); err != nil {
			errs = append(errs, fmt.Errorf("failed to close connection to %s: %w", endpoint, err))
		}
	}
	c.conns = make(map[string]*grpc.ClientConn)
	if len(errs) > 0 {
		return fmt.Errorf("errors closing connections: %v", errs)
	}
	return nil
}

// extractErrorDetail extracts a custom ErrorDetail message from a gRPC error, if present.
func extractErrorDetail(err error) *pb.ErrorDetail {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, detail := range st.Details() {
		if errDetail, ok := detail.(*pb.ErrorDetail); ok {
			return errDetail
		}
	}
	return nil
}
