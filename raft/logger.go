package raft

// Logger defines an interface for structured, context-aware logging used in a Raft implementation.
//
// All logging methods support structured output by accepting a message and a variadic list of key-value pairs.
// Keys must be strings and must alternate with values in the form: key1, val1, key2, val2, ...
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

	// WithNodeID adds a node identifier to the logger's context (typically used to distinguish Raft nodes).
	WithNodeID(id int) Logger

	// WithTerm adds the current Raft term to the logger's context.
	WithTerm(term uint64) Logger

	// WithComponent adds a component label (e.g., "election", "rpc") to categorize log output.
	WithComponent(name string) Logger
}
