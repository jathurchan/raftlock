package logger

import (
	"fmt"
	"log"
	"os"
	"strings"
)

// StdLogger implements the Logger interface using Go's standard log package.
// It supports structured logging by formatting key-value pairs and includes
// persistent context such as node ID, term, and component name.
type StdLogger struct {
	context map[string]any
}

// NewStdLogger creates a new StdLogger instance that logs to standard output.
func NewStdLogger() Logger {
	return &StdLogger{
		context: make(map[string]any),
	}
}

// log is a helper method that formats and outputs a structured log message
// at the given log level. It includes persistent context and message-specific key-value pairs.
// If the level is "fatal", it logs the message and terminates the application.
func (l *StdLogger) log(level string, msg string, kvs ...any) {
	var b strings.Builder
	b.WriteString(fmt.Sprintf("[%s] %s", strings.ToUpper(level), msg))

	// Add persistent context
	for k, v := range l.context {
		b.WriteString(fmt.Sprintf(" %s=%v", k, v))
	}

	// Add message-specific key-value pairs
	for i := 0; i < len(kvs); i += 2 {
		// Check if we have both key and value
		if i+1 >= len(kvs) {
			break // Odd number of arguments, stop processing
		}

		key, ok := kvs[i].(string)
		if !ok {
			continue // skip malformed key
		}
		val := kvs[i+1]
		b.WriteString(fmt.Sprintf(" %s=%v", key, val))
	}

	log.Println(b.String())

	if level == "fatal" {
		os.Exit(1)
	}
}

// Logging methods implement the Logger interface at different log levels.

func (l *StdLogger) Debugw(msg string, kvs ...any) { l.log("debug", msg, kvs...) }

func (l *StdLogger) Infow(msg string, kvs ...any) { l.log("info", msg, kvs...) }

func (l *StdLogger) Warnw(msg string, kvs ...any) { l.log("warn", msg, kvs...) }

func (l *StdLogger) Errorw(msg string, kvs ...any) { l.log("error", msg, kvs...) }

func (l *StdLogger) Fatalw(msg string, kvs ...any) { l.log("fatal", msg, kvs...) }

// cloneWithContext creates a new StdLogger with the given context merged
// into the existing logger's context. Used to build loggers with additional metadata.
func (l *StdLogger) cloneWithContext(extra map[string]any) *StdLogger {
	newCtx := make(map[string]any, len(l.context)+len(extra))

	// Copy existing context
	for k, v := range l.context {
		newCtx[k] = v
	}

	// Add new context
	for k, v := range extra {
		newCtx[k] = v
	}

	return &StdLogger{context: newCtx}
}

// With adds arbitrary key-value pairs to the logger's context.
// Malformed pairs (non-string keys or unmatched values) are ignored.
func (l *StdLogger) With(kvs ...any) Logger {
	ctx := make(map[string]any)
	for i := 0; i < len(kvs); i += 2 {
		// Check if we have both key and value
		if i+1 >= len(kvs) {
			break // Odd number of arguments, stop processing
		}

		key, ok := kvs[i].(string)
		if !ok {
			continue // skip invalid key
		}
		ctx[key] = kvs[i+1]
	}
	return l.cloneWithContext(ctx)
}

// WithNodeID returns a new logger with a node identifier added to the context.
func (l *StdLogger) WithNodeID(id int) Logger {
	return l.cloneWithContext(map[string]any{"node": id})
}

// WithTerm returns a new logger with the Raft term added to the context.
func (l *StdLogger) WithTerm(term uint64) Logger {
	return l.cloneWithContext(map[string]any{"term": term})
}

// WithComponent returns a new logger with a component label (e.g., "election", "rpc")
// added to the context to categorize log output.
func (l *StdLogger) WithComponent(name string) Logger {
	return l.cloneWithContext(map[string]any{"component": name})
}
