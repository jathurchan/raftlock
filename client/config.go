package client

import (
	"time"

	pb "github.com/jathurchan/raftlock/proto"
)

// Config holds configuration options for RaftLock clients.
type Config struct {
	// Endpoints is a list of RaftLock server addresses that the client will
	// attempt to connect to. At least one endpoint is required.
	Endpoints []string

	// DialTimeout is the maximum time the client will wait to establish a
	// connection to a server endpoint. Defaults to 5 seconds.
	DialTimeout time.Duration

	// RequestTimeout is the default timeout for individual gRPC requests.
	// This can be overridden by a context with a shorter deadline. Defaults to 30 seconds.
	RequestTimeout time.Duration

	// KeepAlive settings control gRPC's keepalive mechanism, which helps
	// detect dead connections and keep active ones alive through proxies.
	KeepAlive KeepAliveConfig

	// RetryPolicy defines the behavior for retrying failed operations,
	// including backoff strategy and which errors are considered retryable.
	RetryPolicy RetryPolicy

	// EnableMetrics toggles the collection of client-side performance metrics.
	// Defaults to true.
	EnableMetrics bool

	// MaxMessageSize specifies the maximum size of a gRPC message (in bytes)
	// that the client can send or receive. Defaults to 16MB.
	MaxMessageSize int
}

// KeepAliveConfig defines gRPC keepalive settings for the client.
type KeepAliveConfig struct {
	// Time is the interval at which the client sends keepalive pings to the server
	// when no other messages are being sent.
	Time time.Duration

	// Timeout is the duration the client waits for a keepalive ack from the server
	// before considering the connection to be dead.
	Timeout time.Duration

	// PermitWithoutStream allows keepalive pings to be sent even when there are
	// no active streams. This is useful for maintaining connections.
	PermitWithoutStream bool
}

// DefaultClientConfig returns a ClientConfig with sensible default values.
func DefaultClientConfig() Config {
	return Config{
		DialTimeout:    5 * time.Second,
		RequestTimeout: 30 * time.Second,
		KeepAlive: KeepAliveConfig{
			Time:                30 * time.Second,
			Timeout:             5 * time.Second,
			PermitWithoutStream: true,
		},
		RetryPolicy:    DefaultRetryPolicy(),
		EnableMetrics:  true,
		MaxMessageSize: 16 * 1024 * 1024, // 16MB
	}
}

// DefaultRetryPolicy returns a default retry policy that handles common
// transient and leader-related errors.
func DefaultRetryPolicy() RetryPolicy {
	return RetryPolicy{
		MaxRetries:        3,
		InitialBackoff:    100 * time.Millisecond,
		MaxBackoff:        5 * time.Second,
		BackoffMultiplier: 2.0,
		JitterFactor:      0.1,
		RetryableErrors: []pb.ErrorCode{
			pb.ErrorCode_NO_LEADER,
			pb.ErrorCode_NOT_LEADER,
			pb.ErrorCode_UNAVAILABLE,
			pb.ErrorCode_TIMEOUT,
		},
	}
}
