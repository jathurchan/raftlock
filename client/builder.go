package client

import (
	"errors"
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

// RaftLockClientBuilder provides a fluent API for constructing RaftLock clients.
// It supports configuring and creating RaftLockClient, AdminClient, and AdvancedClient instances.
//
// Example:
//
//	client, err := client.NewRaftLockClientBuilder([]string{"localhost:8080"}).
//	    WithTimeouts(2*time.Second, 5*time.Second).
//	    Build()
type RaftLockClientBuilder struct {
	config      Config
	hasEndpoint bool
}

// NewRaftLockClientBuilder returns a new Builder initialized with the given endpoints.
// At least one endpoint is required to build a client.
func NewRaftLockClientBuilder(endpoints []string) *RaftLockClientBuilder {
	b := &RaftLockClientBuilder{
		config: DefaultClientConfig(),
	}
	if len(endpoints) > 0 {
		b.config.Endpoints = endpoints
		b.hasEndpoint = true
	}
	return b
}

// WithEndpoints sets the server endpoints.
// This is required and overrides any previously set endpoints.
func (b *RaftLockClientBuilder) WithEndpoints(endpoints []string) *RaftLockClientBuilder {
	b.config.Endpoints = endpoints
	b.hasEndpoint = len(endpoints) > 0
	return b
}

// WithTimeouts sets the dial and request timeouts.
func (b *RaftLockClientBuilder) WithTimeouts(dialTimeout, requestTimeout time.Duration) *RaftLockClientBuilder {
	if dialTimeout > 0 {
		b.config.DialTimeout = dialTimeout
	}
	if requestTimeout > 0 {
		b.config.RequestTimeout = requestTimeout
	}
	return b
}

// WithKeepAlive sets gRPC keepalive parameters.
func (b *RaftLockClientBuilder) WithKeepAlive(time, timeout time.Duration, permitWithoutStream bool) *RaftLockClientBuilder {
	b.config.KeepAlive = KeepAliveConfig{
		Time:                time,
		Timeout:             timeout,
		PermitWithoutStream: permitWithoutStream,
	}
	return b
}

// WithRetryPolicy sets a custom retry policy.
func (b *RaftLockClientBuilder) WithRetryPolicy(policy RetryPolicy) *RaftLockClientBuilder {
	b.config.RetryPolicy = policy
	return b
}

// WithRetryOptions updates the default retry policy parameters.
func (b *RaftLockClientBuilder) WithRetryOptions(maxRetries int, initialBackoff, maxBackoff time.Duration, multiplier float64) *RaftLockClientBuilder {
	if maxRetries >= 0 {
		b.config.RetryPolicy.MaxRetries = maxRetries
	}
	if initialBackoff > 0 {
		b.config.RetryPolicy.InitialBackoff = initialBackoff
	}
	if maxBackoff > 0 {
		b.config.RetryPolicy.MaxBackoff = maxBackoff
	}
	if multiplier > 0 {
		b.config.RetryPolicy.BackoffMultiplier = multiplier
	}
	return b
}

// WithRetryableErrors sets the error codes that should trigger retries.
// An empty slice disables retryable error codes.
func (b *RaftLockClientBuilder) WithRetryableErrors(codes ...pb.ErrorCode) *RaftLockClientBuilder {
	if len(codes) > 0 {
		b.config.RetryPolicy.RetryableErrors = codes
	} else {
		b.config.RetryPolicy.RetryableErrors = []pb.ErrorCode{}
	}
	return b
}

// WithMetrics enables or disables metrics collection.
func (b *RaftLockClientBuilder) WithMetrics(enabled bool) *RaftLockClientBuilder {
	b.config.EnableMetrics = enabled
	return b
}

// WithMaxMessageSize sets the max gRPC message size (bytes).
func (b *RaftLockClientBuilder) WithMaxMessageSize(size int) *RaftLockClientBuilder {
	if size > 0 {
		b.config.MaxMessageSize = size
	}
	return b
}

// validate checks if the builder has valid configuration.
func (b *RaftLockClientBuilder) validate() error {
	if !b.hasEndpoint || len(b.config.Endpoints) == 0 {
		return errors.New("builder: at least one endpoint must be set")
	}
	return nil
}

// Build returns a configured RaftLockClient.
func (b *RaftLockClientBuilder) Build() (RaftLockClient, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return NewRaftLockClient(b.config)
}

// BuildAdmin returns a configured AdminClient.
func (b *RaftLockClientBuilder) BuildAdmin() (AdminClient, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return NewAdminClient(b.config)
}

// BuildAdvanced returns a configured AdvancedClient.
func (b *RaftLockClientBuilder) BuildAdvanced() (AdvancedClient, error) {
	if err := b.validate(); err != nil {
		return nil, err
	}
	return NewAdvancedClient(b.config)
}
