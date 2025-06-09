package client

import "time"

// ClientMetrics exposes client-side metrics for observability and monitoring.
type ClientMetrics interface {
	// GetRequestCount returns the total number of requests for the given operation type.
	GetRequestCount(operation string) uint64

	// GetSuccessRate returns the success rate (0.0 to 1.0) for the given operation type.
	GetSuccessRate(operation string) float64

	// GetAverageLatency returns the average latency for the given operation type.
	GetAverageLatency(operation string) time.Duration

	// GetRetryCount returns the total number of retries for the given operation type.
	GetRetryCount(operation string) uint64

	// GetLeaderRedirectCount returns the total number of leader redirects encountered.
	GetLeaderRedirectCount() uint64

	// GetConnectionCount returns the current number of active connections.
	GetConnectionCount() int

	// Reset clears all collected metrics.
	Reset()
}
