# **`client` package**

The `client` package provides the necessary tools for Go applications to communicate with a RaftLock cluster. It offers a robust and configurable client for managing distributed locks, with features like automatic leader discovery, request retries, and metrics collection.

## Core Features

* **Multiple Client Interfaces** The package offers three distinct client interfaces to cater to different use cases:
  * `RaftLockClient`: A high-level client for standard lock operations like acquiring, releasing, and renewing locks.
  * `AdminClient`: A specialized client for administrative and monitoring tasks, such as checking cluster status and health.
  * `AdvancedClient`: A low-level client for more granular control over lock queuing and other advanced features.
* **Fluent Client Configuration** A `RaftLockClientBuilder` provides a fluent API for constructing and configuring clients, allowing for easy setup of endpoints, timeouts, and other parameters.
* **Automatic Leader Redirection** The client can automatically detect and redirect requests to the current Raft leader, simplifying client-side logic.
* **Configurable Retry Policies** The client supports configurable retry policies with exponential backoff and jitter for handling transient errors and network issues. You can customize the number of retries, backoff durations, and which errors are considered retryable.
* **Stateful Lock Handling** The `LockHandle` interface offers a stateful wrapper for managing the lifecycle of a single lock, simplifying acquire, renew, and release operations.
* **Automated Lock Renewal** The `AutoRenewer` automatically renews a held lock in the background, ensuring it doesn't expire while in use.
* **Comprehensive Metrics** The client includes a `Metrics` interface for collecting detailed operational metrics, which can be integrated with monitoring systems. A no-op implementation is available if metrics are not needed.

## Client Types

The package exposes three client interfaces to meet different needs.

### RaftLockClient

The `RaftLockClient` is the primary client for application use, providing core distributed locking functionality:

* `Acquire`: Attempts to acquire a lock, with an option to wait in a queue if the lock is held.
* `Release`: Releases a previously acquired lock.
* `Renew`: Extends the TTL of a held lock.
* `GetLockInfo`: Retrieves metadata and state for a specific lock.
* `GetLocks`: Returns a paginated list of locks matching a filter.

### AdminClient

The `AdminClient` is designed for administrative and monitoring purposes:

* `GetStatus`: Retrieves the current status of a RaftLock cluster node.
* `Health`: Checks the health of the RaftLock service.
* `GetBackoffAdvice`: Provides adaptive backoff parameters to guide client retry behavior.

### AdvancedClient

The `AdvancedClient` offers low-level control for specialized use cases:

* `EnqueueWaiter`: Explicitly adds the client to a lock's wait queue.
* `CancelWait`: Removes the client from a lock's wait queue.
* `GetLeaderAddress`: Returns the address of the current Raft leader.
* `IsConnected`: Reports whether the client has an active connection.
* `SetRetryPolicy`: Sets the client's retry behavior.

## Getting Started

To get started with the RaftLock client, you first need to construct a client instance using the `RaftLockClientBuilder`.

### Building a Client

The `RaftLockClientBuilder` provides a fluent API for creating and configuring clients. You can set server endpoints, timeouts, keep-alive settings, and more.

```go
package main

import (
 "context"
 "fmt"
 "log"
 "time"

 "github.com/jathurchan/raftlock/client"
)

func main() {
 // A slice of server addresses for the client to connect to.
 endpoints := []string{"localhost:8080", "localhost:8081"}

 // Use the builder to construct a new RaftLockClient.
 raftClient, err := client.NewRaftLockClientBuilder(endpoints).
  WithTimeouts(5*time.Second, 10*time.Second). // Set dial and request timeouts.
  WithMetrics(true).                           // Enable metrics collection.
  Build()
 if err != nil {
  log.Fatalf("Failed to build RaftLock client: %v", err)
 }
 defer raftClient.Close()

 fmt.Println("RaftLock client created successfully!")
}
```

#### Acquiring and Releasing a Lock

You can use a `LockHandle` for a more convenient way to manage a lock's lifecycle.

```go
package main

import (
 "context"
 "fmt"
 "log"
 "time"

 "github.com/jathurchan/raftlock/client"
)

func main() {
 endpoints := []string{"localhost:8080"}
 raftClient, err := client.NewRaftLockClientBuilder(endpoints).Build()
 if err != nil {
  log.Fatalf("Failed to create client: %v", err)
 }
 defer raftClient.Close()

 lockID := "my-resource-lock"
 clientID := "my-application-instance-1"

 // Create a new handle for the lock.
 lockHandle, err := client.NewLockHandle(raftClient, lockID, clientID)
 if err != nil {
  log.Fatalf("Failed to create lock handle: %v", err)
 }
 defer lockHandle.Close(context.Background())

 // Acquire the lock with a 30-second TTL.
 ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
 defer cancel()

 if err := lockHandle.Acquire(ctx, 30*time.Second, true); err != nil {
  log.Fatalf("Failed to acquire lock: %v", err)
 }

 fmt.Printf("Lock '%s' acquired successfully!\n", lockID)

 // ... perform operations while holding the lock ...
 time.Sleep(5 * time.Second)

 // Release the lock.
 if err := lockHandle.Release(context.Background()); err != nil {
  log.Fatalf("Failed to release lock: %v", err)
 }

 fmt.Printf("Lock '%s' released successfully!\n", lockID)
}

```

## Configuration

The client's behavior can be customized through the `Config` struct, which is typically configured using the `RaftLockClientBuilder`.

### Core Configuration Options

* **Endpoints**: A list of RaftLock server addresses.
* **DialTimeout**: The maximum time to wait for a connection to be established.
* **RequestTimeout**: The default timeout for individual gRPC requests.
* **KeepAlive**: gRPC keep-alive settings to detect dead connections.
* **EnableMetrics**: A boolean to enable or disable client-side metrics collection.
* **MaxMessageSize**: The maximum size of a gRPC message the client can send or receive.

### Retry Policy

The `RetryPolicy` struct allows you to define how the client should handle failed operations.

* **MaxRetries**: The maximum number of retry attempts.
* **InitialBackoff**: The initial delay before the first retry.
* **MaxBackoff**: The maximum delay between retries.
* **BackoffMultiplier**: The multiplier for increasing the backoff duration after each attempt.
* **JitterFactor**: A factor to randomize backoff durations to avoid thundering herd scenarios.
* **RetryableErrors**: A list of error codes that should trigger a retry.

## Error Handling

The `client` package defines a set of standard errors that can be returned by its functions. This allows you to handle specific error conditions in your application.

Some of the common errors include:

* **ErrLockHeld**: The lock is already held by another client.
* **ErrNotLockOwner**: The client is not the owner of the lock.
* **ErrVersionMismatch**: The provided lock version is incorrect.
* **ErrLockNotFound**: The specified lock does not exist.
* **ErrTimeout**: The operation timed out.
* **ErrLeaderUnavailable**: No leader is available in the cluster.
* **ErrClientClosed**: The client has been closed and cannot be used.

You can use standard Go error handling techniques, such as `errors.Is`, to check for these specific errors.
