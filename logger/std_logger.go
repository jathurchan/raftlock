package logger

import (
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/jathurchan/raftlock/types"
)

// LogLevel represents the severity of a log message.
type LogLevel int

const (
	LevelDebug LogLevel = iota
	LevelInfo
	LevelWarn
	LevelError
	LevelFatal
)

// parseLogLevel maps a string to a LogLevel. Defaults to LevelInfo on unknown input.
func parseLogLevel(levelStr string) LogLevel {
	switch strings.ToLower(levelStr) {
	case "debug":
		return LevelDebug
	case "info":
		return LevelInfo
	case "warn", "warning":
		return LevelWarn
	case "error":
		return LevelError
	case "fatal":
		return LevelFatal
	default:
		return LevelInfo
	}
}

// StdLogger logs messages using Go's standard library log package.
type StdLogger struct {
	context  map[string]any
	minLevel LogLevel
}

// NewStdLogger returns a new StdLogger with a minimum log level filter.
func NewStdLogger(minLevelStr string) Logger {
	return &StdLogger{
		context:  make(map[string]any),
		minLevel: parseLogLevel(minLevelStr),
	}
}

// log outputs a structured log entry if the level meets the threshold.
func (l *StdLogger) log(level LogLevel, levelStr string, msg string, kvs ...any) {
	if level < l.minLevel {
		return
	}

	var b strings.Builder
	b.WriteString(fmt.Sprintf("[%s] %s", strings.ToUpper(levelStr), msg))

	// Add persistent context
	for k, v := range l.context {
		b.WriteString(fmt.Sprintf(" %s=%v", k, v))
	}

	// Add message-specific key-value pairs
	for i := 0; i < len(kvs); i += 2 {
		if i+1 >= len(kvs) {
			break
		}
		key, ok := kvs[i].(string)
		if !ok {
			continue
		}
		val := kvs[i+1]
		b.WriteString(fmt.Sprintf(" %s=%v", key, val))
	}

	log.Println(b.String())

	if level == LevelFatal {
		os.Exit(1)
	}
}

func (l *StdLogger) Debugw(msg string, kvs ...any) { l.log(LevelDebug, "debug", msg, kvs...) }
func (l *StdLogger) Infow(msg string, kvs ...any)  { l.log(LevelInfo, "info", msg, kvs...) }
func (l *StdLogger) Warnw(msg string, kvs ...any)  { l.log(LevelWarn, "warn", msg, kvs...) }
func (l *StdLogger) Errorw(msg string, kvs ...any) { l.log(LevelError, "error", msg, kvs...) }
func (l *StdLogger) Fatalw(msg string, kvs ...any) { l.log(LevelFatal, "fatal", msg, kvs...) }

// cloneWithContext returns a copy of the logger with merged context.
func (l *StdLogger) cloneWithContext(extra map[string]any) *StdLogger {
	newCtx := make(map[string]any, len(l.context)+len(extra))
	for k, v := range l.context {
		newCtx[k] = v
	}
	for k, v := range extra {
		newCtx[k] = v
	}
	return &StdLogger{context: newCtx, minLevel: l.minLevel}
}

// With adds key-value pairs to the logger’s context.
func (l *StdLogger) With(kvs ...any) Logger {
	ctx := make(map[string]any)
	for i := 0; i < len(kvs); i += 2 {
		if i+1 >= len(kvs) {
			break
		}
		key, ok := kvs[i].(string)
		if !ok {
			continue
		}
		ctx[key] = kvs[i+1]
	}
	return l.cloneWithContext(ctx)
}

// WithNodeID returns a logger with a node ID added to the context.
func (l *StdLogger) WithNodeID(id types.NodeID) Logger {
	return l.cloneWithContext(map[string]any{"node": id})
}

// WithTerm returns a logger with the Raft term added to the context.
func (l *StdLogger) WithTerm(term types.Term) Logger {
	return l.cloneWithContext(map[string]any{"term": term})
}

// WithComponent returns a logger with a component name added to the context.
func (l *StdLogger) WithComponent(name string) Logger {
	return l.cloneWithContext(map[string]any{"component": name})
}
