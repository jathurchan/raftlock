# RaftLock Protocol Buffers

This directory contains the `raftlock.proto` file, which defines the Protocol Buffer messages and gRPC service for the RaftLock distributed lock service.

## Overview

RaftLock provides a distributed lock service with strong consistency guarantees, achieved via the Raft consensus algorithm. The gRPC API defined in `raftlock.proto` enables clients to acquire, release, manage, and monitor distributed locks across a cluster of RaftLock nodes.

The primary Protocol Buffer file is:

* `proto/raftlock.proto`: Defines the main `RaftLock` service, its RPC methods, and all associated request/response messages.

## API Features

The `RaftLock` gRPC service provides the following main capabilities:

* **Core Lock Operations**:
  * `Acquire`: Request a distributed lock with configurable TTL and wait behavior. Includes a `request_id` for idempotent retries.
  * `Release`: Release a previously acquired lock using fencing tokens.
  * `Renew`: Extend the TTL of an existing lock.
* **Lock Information & Querying**:
  * `GetLockInfo`: Query the current state of a specific lock, including an optional `ErrorDetail` if the lock is not found.
  * `GetLocks`: List locks with server-side filtering and pagination.
* **Wait Queue Management**:
  * `EnqueueWaiter`: Join a wait queue for contested locks with priority and timeout support.
  * `CancelWait`: Remove a client from a lock's wait queue.
* **Client Guidance**:
  * `GetBackoffAdvice`: Provides clients with adaptive backoff parameters (initial backoff, max backoff, multiplier, jitter) to intelligently handle retries during contention.
* **Cluster & Server Status**:
  * `GetStatus`: Monitor the Raft cluster state, lock manager statistics, and overall server health.
  * `Health`: A standard health check endpoint, often used for monitoring.

## Key Concepts & Data Structures

The API employs several key concepts for robust distributed locking:

### Fencing Tokens

Every lock acquisition returns a `version` field (an `int64`). This version acts as a monotonically increasing fencing token. Clients must provide this token when releasing or renewing a lock to prevent issues like split-brain or acting on stale lock information.

```protobuf
message Lock {
  string lock_id = 1;
  string owner_id = 2;
  int64 version = 3; // Fencing token
  google.protobuf.Timestamp acquired_at = 4;
  google.protobuf.Timestamp expires_at = 5;
  map<string, string> metadata = 6;
}
```

### Timestamps and Durations

The API uses standard Google Protocol Buffer well-known types for time:

* `google.protobuf.Timestamp`: For absolute points in time (e.g., expires\_at).
* `google.protobuf.Duration`: For time durations (e.g., ttl, wait\_timeout).

### Adaptive Backoff

To help clients manage contention gracefully, the service can provide backoff advice.

```protobuf
message BackoffAdvice {
  google.protobuf.Duration initial_backoff = 1;
  google.protobuf.Duration max_backoff = 2;
  double multiplier = 3;
  double jitter_factor = 4;
}
```

### Structured Error Handling

Operations return detailed error information using the `ErrorDetail` message, which includes a machine-readable `ErrorCode` and a descriptive message.

```protobuf
message ErrorDetail {
  ErrorCode code = 1;
  string message = 2;
  map<string, string> details = 3;
}

enum ErrorCode {
  ERROR_CODE_UNSPECIFIED = 0;
  OK = 1;

  // Lock-specific errors
  LOCK_HELD = 101;
  LOCK_NOT_HELD = 102;
  NOT_LOCK_OWNER = 103;
  VERSION_MISMATCH = 104;
  LOCK_NOT_FOUND = 105;
  INVALID_TTL = 106;
  WAIT_QUEUE_FULL = 107;
  NOT_WAITING = 108;

  // Raft/Cluster related errors
  NOT_LEADER = 201;
  NO_LEADER = 202;
  REPLICATION_FAILURE = 203;

  // General server/request errors
  TIMEOUT = 301;
  RATE_LIMITED = 302;
  UNAVAILABLE = 303;
  INTERNAL_ERROR = 304;
  INVALID_ARGUMENT = 305;
  PERMISSION_DENIED = 306;
}
```

## Code Generation

The Go gRPC code can be generated from the `proto/raftlock.proto` file.

**Prerequisites**:

* `protoc` (Protocol Buffer compiler)
* `protoc-gen-go` (Go plugin for protoc)
* `protoc-gen-go-grpc` (Go gRPC plugin for protoc)

Ensure these are installed and in your system's `PATH`. You can install the Go plugins via:

```bash
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
```

**Command (from the repository root)**:

```bash
protoc --proto_path=. \
  --go_out=. --go_opt=paths=source_relative \
  --go-grpc_out=. --go-grpc_opt=paths=source_relative \
  proto/raftlock.proto
```

This command will generate the following files in the `proto/` directory:

* `raftlock.pb.go`: Contains the Go struct definitions for all messages.
* `raftlock_grpc.pb.go`: Contains the Go interface definitions and client/server stubs for the `RaftLock` gRPC service.

## Versioning

The RaftLock gRPC API aims to follow semantic versioning principles.

* **MAJOR** version incremented for backward-incompatible API changes.
* **MINOR** version incremented for new, backward-compatible functionality.
* **PATCH** version incremented for backward-compatible bug fixes.

Current API Version: `v1.0.0`
