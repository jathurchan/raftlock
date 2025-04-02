package logger

// NoOpLogger is a Logger implementation that silently discards all log messages.
// It is useful for testing, benchmarking, or disabling logging entirely.
type NoOpLogger struct{}

// Debugw implements Logger.Debugw; it discards the debug message and key-value pairs.
func (l *NoOpLogger) Debugw(msg string, keysAndValues ...any) {}

// Infow implements Logger.Infow; it discards the info message and key-value pairs.
func (l *NoOpLogger) Infow(msg string, keysAndValues ...any) {}

// Warnw implements Logger.Warnw; it discards the warning message and key-value pairs.
func (l *NoOpLogger) Warnw(msg string, keysAndValues ...any) {}

// Errorw implements Logger.Errorw; it discards the error message and key-value pairs.
func (l *NoOpLogger) Errorw(msg string, keysAndValues ...any) {}

// Fatalw implements Logger.Fatalw; it discards the fatal message and key-value pairs.
// Unlike typical fatal loggers, this does not terminate the application.
func (l *NoOpLogger) Fatalw(msg string, keysAndValues ...any) {}

// With implements Logger.With; it returns the same NoOpLogger without storing the context.
func (l *NoOpLogger) With(keysAndValues ...any) Logger { return l }

// WithNodeID implements Logger.WithNodeID; it returns the same NoOpLogger without storing the context.
func (l *NoOpLogger) WithNodeID(id int) Logger { return l }

// WithTerm implements Logger.WithTerm; it returns the same NoOpLogger without storing the context.
func (l *NoOpLogger) WithTerm(term uint64) Logger { return l }

// WithComponent implements Logger.WithComponent; it returns the same NoOpLogger without storing the context.
func (l *NoOpLogger) WithComponent(name string) Logger { return l }

// NewNoOpLogger returns a Logger that discards all log messages.
func NewNoOpLogger() Logger {
	return &NoOpLogger{}
}
