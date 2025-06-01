package server

import "time"

const (
	// --- Default server configuration values ---

	// DefaultListenAddress is the default address for the server's client-facing gRPC endpoint.
	DefaultListenAddress = "0.0.0.0:8080"

	// DefaultRequestTimeout is the default timeout for processing individual client requests.
	DefaultRequestTimeout = 30 * time.Second

	// DefaultShutdownTimeout is the default timeout for graceful server shutdown.
	DefaultShutdownTimeout = 10 * time.Second

	// DefaultMaxRequestSize is the default maximum size for incoming client gRPC requests (4MB).
	DefaultMaxRequestSize = 4 * 1024 * 1024

	// DefaultMaxResponseSize is the default maximum size for outgoing client gRPC responses (4MB).
	DefaultMaxResponseSize = 4 * 1024 * 1024

	// DefaultMaxConcurrentRequests is the default maximum number of concurrent client requests the server will process.
	DefaultMaxConcurrentRequests = 1000

	// --- Rate limiting defaults ---

	// DefaultRateLimit is the default number of requests per second per client.
	DefaultRateLimit = 100

	// DefaultRateLimitBurst is the default burst size for rate limiting.
	DefaultRateLimitBurst = 200

	// DefaultRateLimitWindow is the default time window for rate limiting calculations.
	DefaultRateLimitWindow = time.Second

	// --- Health check defaults ---

	// DefaultHealthCheckInterval is the default interval for internal server health checks.
	DefaultHealthCheckInterval = 30 * time.Second

	// DefaultHealthCheckTimeout is the default timeout for individual internal health checks.
	DefaultHealthCheckTimeout = 5 * time.Second

	// --- Leader redirection defaults ---

	// DefaultRedirectTimeout is the default timeout when redirecting a client request to the Raft leader.
	DefaultRedirectTimeout = 5 * time.Second

	// --- Background task intervals ---

	// DefaultRaftTickInterval is the default interval at which the server application should call Raft.Tick().
	// This should generally align with raft.NominalTickInterval.
	DefaultRaftTickInterval = 100 * time.Millisecond

	// DefaultLockManagerTickInterval is the default interval for LockManager's periodic tasks (e.g., expirations).
	DefaultLockManagerTickInterval = 1 * time.Second

	// DefaultMetricsReportInterval is the default interval for periodic metrics reporting or flushing.
	DefaultMetricsReportInterval = 10 * time.Second

	// --- Client-Facing gRPC Server Configuration ---
	// These defaults apply to the gRPC server that handles client lock requests.
	// They might differ from gRPC settings used for Raft peer-to-peer communication.

	// DefaultGRPCMaxRecvMsgSize is the default maximum size for incoming gRPC messages from clients (16MB).
	DefaultGRPCMaxRecvMsgSize = 16 * 1024 * 1024

	// DefaultGRPCMaxSendMsgSize is the default maximum size for outgoing gRPC messages to clients (16MB).
	DefaultGRPCMaxSendMsgSize = 16 * 1024 * 1024

	// DefaultGRPCKeepaliveTime is the default interval for the server to send keepalive pings to idle client connections.
	// This helps detect dead connections and keep active ones alive through intermediaries.
	DefaultGRPCKeepaliveTime = 30 * time.Second

	// DefaultGRPCKeepaliveTimeout is the default timeout for the server to wait for a keepalive acknowledgment from clients.
	// If a client doesn't respond within this time after a ping, the connection may be considered dead.
	DefaultGRPCKeepaliveTimeout = 5 * time.Second

	// --- Validation limits for client-provided data ---

	// MaxLockIDLength is the maximum allowed length for lock IDs.
	MaxLockIDLength = 256

	// MaxClientIDLength is the maximum allowed length for client IDs.
	MaxClientIDLength = 256

	// MaxMetadataEntries is the maximum number of metadata entries allowed per lock.
	MaxMetadataEntries = 32

	// MaxMetadataKeyLength is the maximum length for metadata keys.
	MaxMetadataKeyLength = 64

	// MaxMetadataValueLength is the maximum length for metadata values.
	MaxMetadataValueLength = 256

	// --- Time bounds for lock operations ---

	// MinLockTTL is the minimum allowed TTL for locks.
	MinLockTTL = 1 * time.Second

	// MaxLockTTL is the maximum allowed TTL for locks.
	MaxLockTTL = 24 * time.Hour

	// MinWaitTimeout is the minimum allowed wait timeout for queue operations.
	MinWaitTimeout = 1 * time.Second

	// MaxWaitTimeout is the maximum allowed wait timeout for queue operations.
	MaxWaitTimeout = 10 * time.Minute

	// --- Priority bounds for lock wait queues ---

	// MinPriority is the minimum allowed priority value for wait queue operations.
	MinPriority = -1000

	// MaxPriority is the maximum allowed priority value for wait queue operations.
	MaxPriority = 1000

	// DefaultPriority is the default priority for wait queue operations if not specified.
	DefaultPriority = 0

	// --- Pagination limits ---

	// DefaultPageLimit is the default number of items returned in paginated responses.
	DefaultPageLimit = 100

	// MaxPageLimit is the maximum number of items that can be requested in a single page.
	MaxPageLimit = 1000

	// --- Request ID constraints ---

	// MaxRequestIDLength is the maximum allowed length for request IDs (used for idempotency).
	MaxRequestIDLength = 128

	// --- Metrics collection defaults ---

	// DefaultMetricsRetentionPeriod is how long to keep historical metrics data if applicable.
	DefaultMetricsRetentionPeriod = 1 * time.Hour
	// DefaultMetricsSampleRate is the rate at which detailed metrics are sampled if applicable.
	DefaultMetricsSampleRate = 0.1 // 10% sampling

	// --- Error message templates for validation ---

	// ErrMsgInvalidLockID is the error message template for invalid lock IDs.
	ErrMsgInvalidLockID = "lock_id must be a non-empty string with length <= %d characters"
	// ErrMsgInvalidClientID is the error message template for invalid client IDs.
	ErrMsgInvalidClientID = "client_id must be a non-empty string with length <= %d characters"
	// ErrMsgInvalidTTL is the error message template for invalid TTL values.
	ErrMsgInvalidTTL = "ttl must be between %v and %v"
	// ErrMsgInvalidTimeout is the error message template for invalid timeout values.
	ErrMsgInvalidTimeout = "timeout must be between %v and %v"
	// ErrMsgInvalidPriority is the error message template for invalid priority values.
	ErrMsgInvalidPriority = "priority must be between %d and %d"
	// ErrMsgTooManyMetadata is the error message for too many metadata entries.
	ErrMsgTooManyMetadata = "metadata cannot have more than %d entries"
	// ErrMsgMetadataKeyTooLong is the error message for metadata keys that are too long.
	ErrMsgMetadataKeyTooLong = "metadata key length cannot exceed %d characters"
	// ErrMsgMetadataValueTooLong is the error message for metadata values that are too long.
	ErrMsgMetadataValueTooLong = "metadata value length cannot exceed %d characters"

	// --- Proposal tracker defaults ---

	// DefaultProposalMaxPendingAge is the default max duration a proposal can remain pending before expiration.
	DefaultProposalMaxPendingAge = 5 * time.Minute

	// DefaultProposalCleanupInterval is the default interval for proposal tracker cleanup tasks.
	DefaultProposalCleanupInterval = 30 * time.Second
)

// ServerOperationalState defines the possible operational states of the server.
type ServerOperationalState string

const (
	// ServerStateStarting indicates the server is in the process of starting up.
	ServerStateStarting ServerOperationalState = "starting"
	// ServerStateRunning indicates the server is running and accepting requests.
	ServerStateRunning ServerOperationalState = "running"
	// ServerStateStopping indicates the server is in the process of shutting down.
	ServerStateStopping ServerOperationalState = "stopping"
	// ServerStateStopped indicates the server has been stopped.
	ServerStateStopped ServerOperationalState = "stopped"
)

// gRPC method names for metrics collection and logging
const (
	MethodAcquire          = "Acquire"
	MethodRelease          = "Release"
	MethodRenew            = "Renew"
	MethodGetLockInfo      = "GetLockInfo"
	MethodGetLocks         = "GetLocks"
	MethodEnqueueWaiter    = "EnqueueWaiter"
	MethodCancelWait       = "CancelWait"
	MethodGetBackoffAdvice = "GetBackoffAdvice"
	MethodGetStatus        = "GetStatus"
	MethodHealth           = "Health"
)

// Queue types for metrics and logging
const (
	QueueTypeGRPCRequests  = "grpc_requests"
	QueueTypeRaftProposals = "raft_proposals"
	QueueTypeLockWaiters   = "lock_waiters"
)

// Error types for metrics and logging (used with ServerMetrics.IncrValidationError/IncrServerError)
const (
	ErrorTypeMissingField    = "missing_field"
	ErrorTypeInvalidFormat   = "invalid_format"
	ErrorTypeOutOfRange      = "out_of_range"
	ErrorTypeTooLong         = "too_long"
	ErrorTypeInternalError   = "internal_error"
	ErrorTypeRaftUnavailable = "raft_unavailable"
	ErrorTypeTimeout         = "timeout"
	ErrorTypeRateLimit       = "rate_limit_exceeded" // More specific for when limit is hit
)
