# RaftLock Storage Package (`storage/`)

## Table of Contents

1. [Overview](#1-overview)
2. [Key Features](#2-key-features)
3. [Import](#3-import)
4. [Quick Start](#4-quick-start)
5. [Configuration](#5-configuration)
6. [Public API](#6-public-api)
7. [Internal Design Notes](#7-internal-design-notes)
8. [Testing](#8-testing)
9. [Related Packages](#9-related-packages)
10. [Contributing](#10-contributing)
11. [License](#11-license)

---

## 1. Overview

The **`storage`** package is RaftLock's durable *persistence layer*. It provides a thread-safe, filesystem-based implementation for storing Raft's critical data:

* **Persistent state** – current term & voted-for candidate
* **Log entries** – the ordered sequence of commands agreed upon by the cluster
* **Snapshots** – compact checkpoints that accelerate recovery and enable log compaction

All operations use ***atomic file operations*** and ***fine‑grained locking*** to guarantee consistency, even under crashes or high concurrency. The package serves as the bridge between Raft's in-memory state and durable storage, ensuring critical consensus data survives node restarts and failures.

## 2. Key Features

The `storage` package is designed for **resilient persistence**, **high concurrency**, and **operational insight**, making it ideal for consensus-based systems like Raft.

### Durability & Atomicity

* **Crash-Safe Writes**: All critical data (logs, state, snapshots) are written via a *temp + rename* strategy to guarantee atomicity and prevent partial writes.
* **Self-Healing Startup**: On launch, the system scans for incomplete operations and cleans them up, restoring a consistent and usable state.

### High Concurrency & Thread Safety

* **Isolated Locks**: Uses independent `sync.RWMutex` instances for logs, state, and snapshots, enabling safe parallel access.
* **Deadlock Mitigation**: Optional timeouts on locks help detect and avoid deadlocks under contention.

### Performance Optimizations

* **Compact Binary Format**: Offers a fast, space-efficient binary encoding for log entries (optional; default-enabled for production).
* **Fast Log Lookup**: Maintains an optional in-memory index (`index → offset`) for O(log n) entry access.
* **Efficient Snapshot I/O**: Supports chunked read/write for large snapshots to minimize memory spikes.
* **Asynchronous Log Compaction**: Log truncation after snapshots runs in a background goroutine, avoiding main-path blocking.

### Observability & Metrics

* **Built-in Instrumentation**: Tracks operation counts, error rates, and latencies (P50, P95, P99).
* **Human-Readable Summaries**: Metrics can be printed or scraped, aiding in diagnostics and performance tuning.

### Flexible Configuration

* **Feature Flags**: Enable or disable specific features like atomic writes, binary format, index maps, metrics, etc.
* **Tunable Parameters**: Adjust fsync behavior, lock timeouts, chunk sizes, and truncation thresholds.

## 3. Import

**Prerequisites:**

* Go 1.21+
* Filesystem with atomic `rename()` support (most modern filesystems)

**Import Path:**

```go
import "github.com/jathurchan/raftlock/storage"
```

This package is primarily consumed by the `raft` package.

## 4. Quick Start

### Basic Usage

```go
package main

import (
    "context"
    "log"
    "os"

    "github.com/jathurchan/raftlock/logger"
    "github.com/jathurchan/raftlock/storage"
    "github.com/jathurchan/raftlock/types"
)

func main() {
    // Setup storage directory
    dir, _ := os.MkdirTemp("", "raftlock_storage_")
    defer os.RemoveAll(dir)

    // Create storage with default options
    store, err := storage.NewFileStorage(
        storage.StorageConfig{Dir: dir}, 
        logger.NewNoOpLogger(),
    )
    if err != nil {
        log.Fatal(err)
    }
    defer store.Close()

    ctx := context.Background()

    // --- Persistent State Operations ---
    state := types.PersistentState{
        CurrentTerm: 5,
        VotedFor:    "node-1",
    }
    if err := store.SaveState(ctx, state); err != nil {
        log.Fatal("save state:", err)
    }

    loadedState, err := store.LoadState(ctx)
    if err != nil {
        log.Fatal("load state:", err)
    }
    log.Printf("Loaded state: term=%d, votedFor=%s", 
        loadedState.CurrentTerm, loadedState.VotedFor)

    // --- Log Operations ---
    entries := []types.LogEntry{
        {Index: 1, Term: 1, Command: []byte("SET key1 value1")},
        {Index: 2, Term: 1, Command: []byte("SET key2 value2")},
    }
    if err := store.AppendLogEntries(ctx, entries); err != nil {
        log.Fatal("append entries:", err)
    }

    readEntries, err := store.GetLogEntries(ctx, 1, 3) // [start, end)
    if err != nil {
        log.Fatal("read entries:", err)
    }
    log.Printf("Read %d entries", len(readEntries))

    // --- Snapshot Operations ---
    snapshot := types.SnapshotMetadata{
        LastIncludedIndex: 100,
        LastIncludedTerm:  5,
    }
    snapshotData := []byte("compacted application state")
    if err := store.SaveSnapshot(ctx, snapshot, snapshotData); err != nil {
        log.Fatal("save snapshot:", err)
    }

    meta, data, err := store.LoadSnapshot(ctx)
    if err != nil {
        log.Fatal("load snapshot:", err)
    }
    log.Printf("Loaded snapshot: lastIndex=%d, size=%d bytes", 
        meta.LastIncludedIndex, len(data))
}
```

### Additional Configuration

```go
// High-performance production setup
opts := storage.FileStorageOptions{
    Features: storage.StorageFeatureFlags{
        EnableBinaryFormat:    true,  // Faster serialization
        EnableIndexMap:        true,  // O(log n) lookups
        EnableMetrics:         true,  // Performance monitoring
        EnableAtomicWrites:    true,  // Crash safety
        EnableLockTimeout:     true,  // Deadlock prevention
        EnableChunkedIO:       true,  // Memory efficiency
        EnableAsyncTruncation: true,  // Non-blocking compaction
    },
    AutoTruncateOnSnapshot: true,           // Automatic log cleanup
    SyncOnAppend:          true,            // Durability guarantee
    RetainedLogSize:       1000,            // Keep 1000 entries minimum
    ChunkSize:             2 * 1024 * 1024, // 2MB chunks
    LockTimeout:           10,              // 10-second timeout
    TruncationTimeout:     60 * time.Second, // 1-minute truncation limit
}

store, err := storage.NewFileStorageWithOptions(cfg, opts, logger)
```

### Monitoring and Metrics

```go
// Enable metrics and monitor performance
if store.GetMetrics() != nil {
    metrics := store.GetMetrics()
    
    log.Printf("Operations: append=%d, read=%d, snapshot=%d",
        metrics["append_ops"], 
        metrics["read_ops"], 
        metrics["snapshot_save_ops"])
        
    log.Printf("Latencies: append_p95=%dμs, read_avg=%dμs",
        metrics["p95_append_latency_us"],
        metrics["avg_read_latency_us"])
        
    log.Printf("Storage: log=%d bytes, error_rate=%d‰",
        metrics["log_size"],
        metrics["error_rate_ppt"])
}

// Human-readable summary
fmt.Println(store.GetMetricsSummary())

// Reset metrics for fresh measurement period
store.ResetMetrics()
```

## 5. Configuration

### StorageConfig (Required)

```go
type StorageConfig struct {
    Dir string // Root directory for all storage files (required)
}
```

### FileStorageOptions (Fine-tuning)

```go
type FileStorageOptions struct {
    Features               StorageFeatureFlags // Feature toggles
    AutoTruncateOnSnapshot bool                // Auto-compact after snapshots
    SyncOnAppend           bool                // Force fsync on each append
    RetainedLogSize        uint64              // Min entries after compaction
    ChunkSize              int                 // Buffer size for large I/O
    LockTimeout            int                 // Lock acquisition timeout (seconds)
    TruncationTimeout      time.Duration       // Max time for truncation ops
}
```

### Feature Flags (StorageFeatureFlags)

```go
type StorageFeatureFlags struct {
    EnableBinaryFormat    bool // Use binary encoding (recommended)
    EnableIndexMap        bool // In-memory index for fast lookups
    EnableAsyncTruncation bool // Non-blocking log compaction
    EnableChunkedIO       bool // Memory-efficient large file handling
    EnableAtomicWrites    bool // Atomic file operations (recommended)
    EnableLockTimeout     bool // Deadlock prevention
    EnableMetrics         bool // Performance tracking
}
```

## 6. Public API

The **`Storage`** interface provides a clean abstraction for Raft persistence:

### Core Operations

```go
type Storage interface {
    // Persistent State (term, vote)
    SaveState(ctx context.Context, state types.PersistentState) error
    LoadState(ctx context.Context) (types.PersistentState, error)

    // Log Management
    AppendLogEntries(ctx context.Context, entries []types.LogEntry) error
    GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error)
    GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error)
    
    // Log Compaction
    TruncateLogPrefix(ctx context.Context, index types.Index) error  // Remove entries < index
    TruncateLogSuffix(ctx context.Context, index types.Index) error  // Remove entries >= index

    // Snapshots
    SaveSnapshot(ctx context.Context, meta types.SnapshotMetadata, data []byte) error
    LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error)

    // Metadata
    FirstLogIndex() types.Index // No I/O
    LastLogIndex() types.Index  // No I/O

    // Observability
    GetMetrics() map[string]uint64
    GetMetricsSummary() string
    ResetMetrics()

    // Lifecycle
    Close() error
}
```

### Key Guarantees

* **Atomicity**: All operations are atomic; no partial states
* **Consistency**: Log indices are strictly ordered and contiguous  
* **Durability**: Data survives crashes when `SyncOnAppend` is enabled
* **Thread Safety**: All methods are safe for concurrent use

## 7. Internal Design Notes

### File Layout & Atomicity

```plaintext
storage_dir/
├── state.json              # Raft persistent state (term, vote)
├── log.dat                 # Binary log entries with length prefixes
├── metadata.json           # Log bounds (first/last indices)
├── snapshot_meta.json      # Snapshot metadata
├── snapshot.dat            # Snapshot data
├── *.tmp                   # Temporary files for atomic writes
└── *.marker                # Recovery markers for crash detection
```

**Atomic Operations:** All critical writes use *write-to-temp-then-rename* to prevent corruption. Recovery procedures detect and clean up incomplete operations.

### Performance Optimization

**Binary Log Format:**

```plaintext
[4-byte length][index][term][command_length][command_data]
```

**Index Mapping:** Optional in-memory `[]IndexOffsetPair` enables O(log n) random access vs O(n) scanning.

**Chunked I/O:** Large snapshots are streamed in configurable chunks to respect memory constraints.

### Error Handling & Recovery

* **Graceful degradation**: Corruption is detected and truncated to last valid entry
* **Rollback on failure**: Partial writes are cleaned up automatically  
* **Recovery markers**: Track incomplete operations across process restarts
* **Consistency verification**: Startup checks ensure in-memory state matches disk

## 8. Testing

The package implements **comprehensive unit tests** with extensive mocking. All filesystem operations are abstracted through interfaces, enabling fast, deterministic testing without real I/O.

### Running Tests

```bash
# Full test suite with race detection and coverage
go test ./... -race -cover

# Benchmarks (when available)
go test ./... -bench=. -benchmem
```

**Coverage categories:**

* ✅ **Unit tests** – Individual method isolation
* ✅ **Concurrency tests** – Thread safety verification  
* ✅ **State consistency tests** – In-memory ↔ disk synchronization
* ✅ **Recovery tests** – Crash simulation and cleanup
* ⏳ **Integration tests** – Real filesystem (planned)
* ⏳ **Benchmarks** – Performance baselines (planned)

> **Contributing tests?** See [Contributing](#10-contributing) – benchmarks and integration tests are welcome!

## 9. Related Packages

* **[`types`](../types/)** – Core data structures (`LogEntry`, `PersistentState`, `SnapshotMetadata`)
* **[`logger`](../logger/)** – Pluggable structured logging interface
* **[`raft`](../raft/)** – The Raft consensus algorithm that consumes this storage layer
* **[`testutil`](../testutil/)** – Testing utilities and assertions used extensively in tests

## 10. Contributing

Contributions are welcome!

### How to Contribute

1. **Open an issue first** for major changes or new features
2. **Comprehensive testing**: `go test ./... -race` must pass; add tests for new functionality
3. **Documentation**: Update README and godocs for API changes
4. **Focused commits**: Keep changes atomic and well-described

### Development Setup

```bash
git clone https://github.com/jathurchan/raftlock.git
cd raftlock/storage
go mod tidy
go test ./... -race -cover
```

### Areas for Contribution

* **Benchmarks**: Performance baselines for critical operations
* **Integration tests**: Real filesystem testing scenarios  
* **Optimizations**: Alternative serialization formats, compression
* **Observability**: Additional metrics, tracing integration
* **Documentation**: More examples, best practices guide

## 11. License

MIT License – see [`LICENSE`](../LICENSE) in the repository root.
