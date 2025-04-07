package logger

// NoOpLogger is a Logger implementation that silently discards all log messages.
// It is useful for testing, benchmarking, or disabling logging entirely.
// Each method can be optionally overridden for testing purposes.
type NoOpLogger struct {
	DebugwFunc func(string, ...any)
	InfowFunc  func(string, ...any)
	WarnwFunc  func(string, ...any)
	ErrorwFunc func(string, ...any)
	FatalwFunc func(string, ...any)
}

// Debugw implements Logger.Debugw; it optionally calls DebugwFunc or discards the message.
func (l *NoOpLogger) Debugw(msg string, keysAndValues ...any) {
	if l.DebugwFunc != nil {
		l.DebugwFunc(msg, keysAndValues...)
	}
}

// Infow implements Logger.Infow; it optionally calls InfowFunc or discards the message.
func (l *NoOpLogger) Infow(msg string, keysAndValues ...any) {
	if l.InfowFunc != nil {
		l.InfowFunc(msg, keysAndValues...)
	}
}

// Warnw implements Logger.Warnw; it optionally calls WarnwFunc or discards the message.
func (l *NoOpLogger) Warnw(msg string, keysAndValues ...any) {
	if l.WarnwFunc != nil {
		l.WarnwFunc(msg, keysAndValues...)
	}
}

// Errorw implements Logger.Errorw; it optionally calls ErrorwFunc or discards the message.
func (l *NoOpLogger) Errorw(msg string, keysAndValues ...any) {
	if l.ErrorwFunc != nil {
		l.ErrorwFunc(msg, keysAndValues...)
	}
}

// Fatalw implements Logger.Fatalw; it optionally calls FatalwFunc or discards the message.
func (l *NoOpLogger) Fatalw(msg string, keysAndValues ...any) {
	if l.FatalwFunc != nil {
		l.FatalwFunc(msg, keysAndValues...)
	}
}

// With returns the same NoOpLogger; context is not stored.
func (l *NoOpLogger) With(keysAndValues ...any) Logger { return l }

// WithNodeID returns the same NoOpLogger; context is not stored.
func (l *NoOpLogger) WithNodeID(id int) Logger { return l }

// WithTerm returns the same NoOpLogger; context is not stored.
func (l *NoOpLogger) WithTerm(term uint64) Logger { return l }

// WithComponent returns the same NoOpLogger; context is not stored.
func (l *NoOpLogger) WithComponent(name string) Logger { return l }

// NewNoOpLogger returns a Logger that discards all log messages.
// Can be type-asserted to *NoOpLogger for injecting test behavior.
func NewNoOpLogger() Logger {
	return &NoOpLogger{}
}
