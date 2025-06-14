package logger

import (
	"bytes"
	"log"
	"strings"
	"testing"

	"github.com/jathurchan/raftlock/types"
)

// captureLogOutput captures log output for testing
func captureLogOutput(fn func()) string {
	var buf bytes.Buffer

	// Save original logger settings
	originalOutput := log.Writer()
	originalFlags := log.Flags()

	// Set up capture
	log.SetOutput(&buf)
	log.SetFlags(0) // Remove timestamp for consistent testing

	// Restore original settings after test
	defer func() {
		log.SetOutput(originalOutput)
		log.SetFlags(originalFlags)
	}()

	fn()
	return buf.String()
}

func TestParseLogLevel(t *testing.T) {
	tests := []struct {
		input    string
		expected LogLevel
	}{
		{"debug", LevelDebug},
		{"DEBUG", LevelDebug},
		{"Debug", LevelDebug},
		{"info", LevelInfo},
		{"INFO", LevelInfo},
		{"warn", LevelWarn},
		{"WARN", LevelWarn},
		{"warning", LevelWarn},
		{"WARNING", LevelWarn},
		{"error", LevelError},
		{"ERROR", LevelError},
		{"fatal", LevelFatal},
		{"FATAL", LevelFatal},
		{"unknown", LevelInfo},
		{"", LevelInfo},
		{"invalid", LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := parseLogLevel(tt.input)
			if result != tt.expected {
				t.Errorf("parseLogLevel(%q) = %v, want %v", tt.input, result, tt.expected)
			}
		})
	}
}

func TestNewStdLogger(t *testing.T) {
	tests := []struct {
		minLevel string
		expected LogLevel
	}{
		{"debug", LevelDebug},
		{"info", LevelInfo},
		{"warn", LevelWarn},
		{"error", LevelError},
		{"fatal", LevelFatal},
		{"invalid", LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.minLevel, func(t *testing.T) {
			logger := NewStdLogger(tt.minLevel).(*StdLogger)
			if logger.minLevel != tt.expected {
				t.Errorf(
					"NewStdLogger(%q).minLevel = %v, want %v",
					tt.minLevel,
					logger.minLevel,
					tt.expected,
				)
			}
			if logger.context == nil {
				t.Error("NewStdLogger should initialize context map")
			}
			if len(logger.context) != 0 {
				t.Error("NewStdLogger should initialize empty context map")
			}
		})
	}
}

func TestStdLogger_LogLevels(t *testing.T) {
	tests := []struct {
		name      string
		minLevel  string
		logFunc   func(Logger)
		expected  string
		shouldLog bool
	}{
		{
			name:      "debug message with debug level",
			minLevel:  "debug",
			logFunc:   func(l Logger) { l.Debugw("test debug message") },
			expected:  "[DEBUG] test debug message",
			shouldLog: true,
		},
		{
			name:      "debug message with info level",
			minLevel:  "info",
			logFunc:   func(l Logger) { l.Debugw("test debug message") },
			expected:  "",
			shouldLog: false,
		},
		{
			name:      "info message with info level",
			minLevel:  "info",
			logFunc:   func(l Logger) { l.Infow("test info message") },
			expected:  "[INFO] test info message",
			shouldLog: true,
		},
		{
			name:      "info message with warn level",
			minLevel:  "warn",
			logFunc:   func(l Logger) { l.Infow("test info message") },
			expected:  "",
			shouldLog: false,
		},
		{
			name:      "warn message with warn level",
			minLevel:  "warn",
			logFunc:   func(l Logger) { l.Warnw("test warn message") },
			expected:  "[WARN] test warn message",
			shouldLog: true,
		},
		{
			name:      "warn message with error level",
			minLevel:  "error",
			logFunc:   func(l Logger) { l.Warnw("test warn message") },
			expected:  "",
			shouldLog: false,
		},
		{
			name:      "error message with error level",
			minLevel:  "error",
			logFunc:   func(l Logger) { l.Errorw("test error message") },
			expected:  "[ERROR] test error message",
			shouldLog: true,
		},
		{
			name:      "error message with fatal level",
			minLevel:  "fatal",
			logFunc:   func(l Logger) { l.Errorw("test error message") },
			expected:  "",
			shouldLog: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := NewStdLogger(tt.minLevel)

			output := captureLogOutput(func() {
				tt.logFunc(logger)
			})

			if tt.shouldLog {
				if !strings.Contains(output, tt.expected) {
					t.Errorf("Expected log output to contain %q, got %q", tt.expected, output)
				}
			} else {
				if output != "" {
					t.Errorf("Expected no log output, got %q", output)
				}
			}
		})
	}
}

func TestStdLogger_LogWithKeyValues(t *testing.T) {
	logger := NewStdLogger("debug")

	output := captureLogOutput(func() {
		logger.Infow("test message", "key1", "value1", "key2", 42, "key3", true)
	})

	expected := "[INFO] test message key1=value1 key2=42 key3=true"
	if !strings.Contains(output, expected) {
		t.Errorf("Expected log output to contain %q, got %q", expected, output)
	}
}

func TestStdLogger_LogWithOddNumberOfKeyValues(t *testing.T) {
	logger := NewStdLogger("debug")

	output := captureLogOutput(func() {
		logger.Infow("test message", "key1", "value1", "key2") // Odd number
	})

	expected := "[INFO] test message key1=value1"
	if !strings.Contains(output, expected) {
		t.Errorf("Expected log output to contain %q, got %q", expected, output)
	}

	if strings.Contains(output, "key2=") {
		t.Errorf("Expected log output to not contain unpaired key, got %q", output)
	}
}

func TestStdLogger_LogWithNonStringKeys(t *testing.T) {
	logger := NewStdLogger("debug")

	output := captureLogOutput(func() {
		logger.Infow(
			"test message",
			"validKey",
			"validValue",
			123,
			"invalidKey",
			"anotherValid",
			"anotherValue",
		)
	})

	if !strings.Contains(output, "validKey=validValue") {
		t.Errorf("Expected log output to contain valid key-value pair")
	}
	if !strings.Contains(output, "anotherValid=anotherValue") {
		t.Errorf("Expected log output to contain another valid key-value pair")
	}
	if strings.Contains(output, "123=") || strings.Contains(output, "invalidKey") {
		t.Errorf("Expected log output to skip non-string key, got %q", output)
	}
}

func TestStdLogger_With(t *testing.T) {
	logger := NewStdLogger("debug")

	newLogger := logger.With("persistent", "value", "another", 123)

	output := captureLogOutput(func() {
		newLogger.Infow("test message", "temp", "tempValue")
	})

	if !strings.Contains(output, "persistent=value") {
		t.Errorf("Expected persistent context in output")
	}
	if !strings.Contains(output, "another=123") {
		t.Errorf("Expected another persistent context in output")
	}
	if !strings.Contains(output, "temp=tempValue") {
		t.Errorf("Expected temporary context in output")
	}
}

func TestStdLogger_WithNodeID(t *testing.T) {
	logger := NewStdLogger("debug")
	nodeLogger := logger.WithNodeID(types.NodeID("node-123"))

	output := captureLogOutput(func() {
		nodeLogger.Infow("test message")
	})

	expected := "node=node-123"
	if !strings.Contains(output, expected) {
		t.Errorf("Expected log output to contain %q, got %q", expected, output)
	}
}

func TestStdLogger_WithTerm(t *testing.T) {
	logger := NewStdLogger("debug")
	termLogger := logger.WithTerm(types.Term(42))

	output := captureLogOutput(func() {
		termLogger.Infow("test message")
	})

	expected := "term=42"
	if !strings.Contains(output, expected) {
		t.Errorf("Expected log output to contain %q, got %q", expected, output)
	}
}

func TestStdLogger_WithComponent(t *testing.T) {
	logger := NewStdLogger("debug")
	componentLogger := logger.WithComponent("election")

	output := captureLogOutput(func() {
		componentLogger.Infow("test message")
	})

	expected := "component=election"
	if !strings.Contains(output, expected) {
		t.Errorf("Expected log output to contain %q, got %q", expected, output)
	}
}

func TestStdLogger_ChainedContext(t *testing.T) {
	logger := NewStdLogger("debug")

	chainedLogger := logger.
		WithNodeID(types.NodeID("node-1")).
		WithTerm(types.Term(5)).
		WithComponent("rpc").
		With("session", "abc123")

	output := captureLogOutput(func() {
		chainedLogger.Infow("complex message", "temp", "value")
	})

	expectedContext := []string{
		"node=node-1",
		"term=5",
		"component=rpc",
		"session=abc123",
		"temp=value",
	}

	for _, expected := range expectedContext {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected log output to contain %q, got %q", expected, output)
		}
	}
}

func TestStdLogger_ContextIsolation(t *testing.T) {
	baseLogger := NewStdLogger("debug")
	logger1 := baseLogger.WithNodeID(types.NodeID("node-1"))
	logger2 := baseLogger.WithNodeID(types.NodeID("node-2"))

	output1 := captureLogOutput(func() {
		logger1.Infow("message from logger1")
	})

	output2 := captureLogOutput(func() {
		logger2.Infow("message from logger2")
	})

	if !strings.Contains(output1, "node=node-1") {
		t.Errorf("Logger1 should contain node-1 context")
	}
	if strings.Contains(output1, "node=node-2") {
		t.Errorf("Logger1 should not contain node-2 context")
	}

	if !strings.Contains(output2, "node=node-2") {
		t.Errorf("Logger2 should contain node-2 context")
	}
	if strings.Contains(output2, "node=node-1") {
		t.Errorf("Logger2 should not contain node-1 context")
	}
}

func TestStdLogger_CloneWithContext(t *testing.T) {
	original := &StdLogger{
		context:  map[string]any{"existing": "value"},
		minLevel: LevelWarn,
	}

	extra := map[string]any{"new": "data", "existing": "overwritten"}
	cloned := original.cloneWithContext(extra)

	if cloned.context["existing"] != "overwritten" {
		t.Errorf("Expected overwritten value, got %v", cloned.context["existing"])
	}
	if cloned.context["new"] != "data" {
		t.Errorf("Expected new value, got %v", cloned.context["new"])
	}

	if cloned.minLevel != LevelWarn {
		t.Errorf("Expected minLevel to be preserved, got %v", cloned.minLevel)
	}

	if original.context["new"] != nil {
		t.Errorf("Original context should not be modified")
	}
	if original.context["existing"] != "value" {
		t.Errorf("Original context should not be modified")
	}
}

func TestStdLogger_EmptyContext(t *testing.T) {
	logger := NewStdLogger("debug")

	output := captureLogOutput(func() {
		logger.Infow("simple message")
	})

	expected := "[INFO] simple message"
	lines := strings.Split(strings.TrimSpace(output), "\n")
	if len(lines) != 1 {
		t.Errorf("Expected single line output, got %d lines", len(lines))
	}

	if !strings.HasSuffix(strings.TrimSpace(lines[0]), expected) {
		t.Errorf("Expected output to end with %q, got %q", expected, lines[0])
	}
}

func TestStdLogger_FatalSkipped(t *testing.T) {
	t.Skip("Fatalw testing skipped as it calls os.Exit(1)")
}

func TestStdLogger_WithOddKeyValues(t *testing.T) {
	logger := NewStdLogger("debug")

	newLogger := logger.With("key1", "value1", "key2") // Missing value for key2

	output := captureLogOutput(func() {
		newLogger.Infow("test message")
	})

	if !strings.Contains(output, "key1=value1") {
		t.Errorf("Expected complete key-value pair in context")
	}

	if strings.Contains(output, "key2=") {
		t.Errorf("Expected unpaired key to be skipped")
	}
}

func TestStdLogger_WithNonStringKeysInWith(t *testing.T) {
	logger := NewStdLogger("debug")

	newLogger := logger.With(
		"validKey",
		"validValue",
		123,
		"shouldBeSkipped",
		"anotherKey",
		"anotherValue",
	)

	output := captureLogOutput(func() {
		newLogger.Infow("test message")
	})

	if !strings.Contains(output, "validKey=validValue") {
		t.Errorf("Expected valid key-value pair in context")
	}
	if !strings.Contains(output, "anotherKey=anotherValue") {
		t.Errorf("Expected another valid key-value pair in context")
	}

	if strings.Contains(output, "123=") || strings.Contains(output, "shouldBeSkipped") {
		t.Errorf("Expected non-string key to be skipped")
	}
}
