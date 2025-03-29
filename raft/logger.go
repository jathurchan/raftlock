package raft

// Logger defines an interface for structured, context-aware logging used in a Raft implementation.
//
// All logging methods support structured output by accepting a message and a variadic list of key-value pairs.
// Keys must be strings and must alternate with values in the form: key1, val1, key2, val2, ...
//
// Context enrichment methods (e.g., With, WithNodeID) return a new logger with additional persistent metadata
// that is automatically included in all subsequent log entries.
//
// Usage:
//
//	// Create a base logger (using any implementation)
//	baseLogger := NewStdLogger()
//
//	// Add persistent context: node ID
//	nodeLogger := baseLogger.WithNodeID(1)
//
//	// Log a message with additional ad-hoc context
//	nodeLogger.Infow("Server started", "address", "10.0.0.1:8080", "peers", 3)
//
//	// Enrich logger with Raft term
//	termLogger := nodeLogger.WithTerm(5)
//
//	// Component-specific logger (e.g., for RPC logic)
//	rpcLogger := termLogger.WithComponent("rpc")
//	rpcLogger.Debugw("Sending request", "to-node", 2, "entries", 3)
//
//	// Temporary context for a single log entry
//	rpcLogger.With("latency", "150ms").Warnw("Slow response")
//
//	// Error logging with contextual information
//	if err != nil {
//		rpcLogger.Errorw("Request failed", "error", err)
//	}
type Logger interface {
	// Debugw logs a debug-level message with optional structured context.
	Debugw(msg string, keysAndValues ...any)

	// Infow logs an info-level message with optional structured context.
	Infow(msg string, keysAndValues ...any)

	// Warnw logs a warning-level message with optional structured context.
	Warnw(msg string, keysAndValues ...any)

	// Errorw logs an error-level message with optional structured context.
	Errorw(msg string, keysAndValues ...any)

	// Fatalw logs a fatal-level message with optional structured context and then terminates the application.
	Fatalw(msg string, keysAndValues ...any)

	// Context enrichment methods return a new logger instance with additional persistent context.

	// With adds arbitrary key-value pairs to the logger's context.
	With(keysAndValues ...any) Logger

	// WithNodeID adds a node identifier to the logger's context.
	// Typically used to distinguish logs from different Raft nodes.
	WithNodeID(id int) Logger

	// WithTerm adds the current Raft term to the logger's context.
	WithTerm(term uint64) Logger

	// WithComponent adds a component label (e.g., "election", "rpc")
	// used to categorize log output.
	WithComponent(name string) Logger
}
