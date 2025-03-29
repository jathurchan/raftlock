package raft

import (
	"bytes"
	"log"
	"os"
	"os/exec"
	"strings"
	"testing"
)

func TestNoOpLogger(t *testing.T) {
	logger := NewNoOpLogger()

	// Test that all logging methods can be called without panicking
	logger.Debugw("debug message", "key", "value")
	logger.Infow("info message", "key", "value")
	logger.Warnw("warn message", "key", "value")
	logger.Errorw("error message", "key", "value")

	// NoOpLogger.Fatalw should not terminate the process
	logger.Fatalw("fatal message", "key", "value")

	// Test context enrichment methods
	enriched := logger.With("key", "value")
	enriched.Infow("enriched message")

	nodeLogger := logger.WithNodeID(1)
	nodeLogger.Infow("node message")

	termLogger := logger.WithTerm(5)
	termLogger.Infow("term message")

	compLogger := logger.WithComponent("test")
	compLogger.Infow("component message")

	// Test chaining of context enrichment methods
	chainedLogger := logger.WithNodeID(1).WithTerm(5).WithComponent("test").With("key", "value")
	chainedLogger.Infow("chained message")
}

func TestStdLogger_LogMethods(t *testing.T) {
	// Capture standard log output
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr) // Restore default output

	logger := NewStdLogger()

	// Test each log level
	logger.Debugw("debug message", "key", "value")
	if !strings.Contains(buf.String(), "[DEBUG] debug message key=value") {
		t.Errorf("Debug log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	logger.Infow("info message", "key", "value")
	if !strings.Contains(buf.String(), "[INFO] info message key=value") {
		t.Errorf("Info log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	logger.Warnw("warn message", "key", "value")
	if !strings.Contains(buf.String(), "[WARN] warn message key=value") {
		t.Errorf("Warn log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	logger.Errorw("error message", "key", "value")
	if !strings.Contains(buf.String(), "[ERROR] error message key=value") {
		t.Errorf("Error log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	// Test with multiple key-value pairs
	logger.Infow("multiple pairs", "key1", "value1", "key2", 42, "key3", true)
	output := buf.String()
	buf.Reset()

	for _, expected := range []string{"[INFO] multiple pairs", "key1=value1", "key2=42", "key3=true"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected log to contain %q, got: %q", expected, output)
		}
	}
}

func TestStdLogger_Fatalw(t *testing.T) {
	if os.Getenv("TEST_FATALW") == "1" {
		logger := NewStdLogger()
		logger.Fatalw("fatal message", "key", "value")
		return
	}

	// Run the test in a subprocess to test Fatalw without terminating the current process
	cmd := exec.Command(os.Args[0], "-test.run=TestStdLogger_Fatalw")
	cmd.Env = append(os.Environ(), "TEST_FATALW=1")
	err := cmd.Run()

	// Verify that the process exited with a non-zero status
	if e, ok := err.(*exec.ExitError); !ok || e.Success() {
		t.Errorf("Fatalw() did not cause process termination")
	}
}

func TestStdLogger_WithChainedAdds(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger().With("k1", "v1").With("k2", "v2")
	logger.Infow("combined context")

	output := buf.String()
	for _, expected := range []string{"[INFO] combined context", "k1=v1", "k2=v2"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected combined context to contain %q, got: %q", expected, output)
		}
	}
}

func TestStdLogger_MalformedInputSafety(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger()
	logger.With("keyOnly").Infow("malformed")

	output := buf.String()
	if !strings.Contains(output, "[INFO] malformed") {
		t.Errorf("Expected malformed message, got: %q", output)
	}
	if strings.Contains(output, "keyOnly") {
		t.Errorf("Malformed key-only pair should not appear in output: %q", output)
	}
}

func TestStdLogger_FatalwIncludesMessage(t *testing.T) {
	if os.Getenv("TEST_FATALW_MESSAGE") == "1" {
		logger := NewStdLogger()
		logger.Fatalw("fatal test message", "key", "value")
		return
	}

	cmd := exec.Command(os.Args[0], "-test.run=TestStdLogger_FatalwIncludesMessage")
	cmd.Env = append(os.Environ(), "TEST_FATALW_MESSAGE=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	if e, ok := err.(*exec.ExitError); !ok || e.Success() {
		t.Errorf("Fatalw() did not cause process termination")
	}

	output := stderr.String()
	for _, expected := range []string{"[FATAL] fatal test message", "key=value"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected fatal log to contain %q, got: %q", expected, output)
		}
	}
}

func TestStdLogger_With(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger()

	// Test With method
	enriched := logger.With("context", "value")
	enriched.Infow("message with context")
	if !strings.Contains(buf.String(), "[INFO] message with context context=value") {
		t.Errorf("Log doesn't contain expected context: %s", buf.String())
	}
	buf.Reset()

	// Test With method with multiple pairs
	multiEnriched := logger.With("key1", "value1", "key2", 42)
	multiEnriched.Infow("message with multiple context")
	output := buf.String()
	buf.Reset()
	for _, expected := range []string{"[INFO] message with multiple context", "key1=value1", "key2=42"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected log to contain %q, got: %q", expected, output)
		}
	}

	// Test With method with malformed pairs (odd number)
	malformedEnriched := logger.With("key1", "value1", "key2")
	malformedEnriched.Infow("message with malformed context")
	output = buf.String()
	buf.Reset()
	if !strings.Contains(output, "key1=value1") {
		t.Errorf("Expected log to contain key1=value1, got: %q", output)
	}
	if strings.Contains(output, "key2=") {
		t.Errorf("Expected log to not contain key2, got: %q", output)
	}

	// Test With method with non-string keys
	nonStringEnriched := logger.With(123, "value1", "key2", "value2")
	nonStringEnriched.Infow("message with non-string key")
	output = buf.String()
	buf.Reset()
	if !strings.Contains(output, "key2=value2") {
		t.Errorf("Expected log to contain key2=value2, got: %q", output)
	}
	if strings.Contains(output, "123=value1") {
		t.Errorf("Expected log to not contain 123=value1, got: %q", output)
	}
}

func TestStdLogger_ContextEnrichment(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger()

	// Test WithNodeID
	nodeLogger := logger.WithNodeID(1)
	nodeLogger.Infow("node message")
	if !strings.Contains(buf.String(), "[INFO] node message") || !strings.Contains(buf.String(), "node=1") {
		t.Errorf("Node log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	// Test WithTerm
	termLogger := logger.WithTerm(5)
	termLogger.Infow("term message")
	if !strings.Contains(buf.String(), "[INFO] term message") || !strings.Contains(buf.String(), "term=5") {
		t.Errorf("Term log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	// Test WithComponent
	compLogger := logger.WithComponent("rpc")
	compLogger.Infow("component message")
	if !strings.Contains(buf.String(), "[INFO] component message") || !strings.Contains(buf.String(), "component=rpc") {
		t.Errorf("Component log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	// Test chaining context methods
	chainedLogger := logger.WithNodeID(1).WithTerm(5).WithComponent("rpc")
	chainedLogger.Infow("chained message")
	output := buf.String()
	buf.Reset()
	// Check that all expected fields are present, regardless of order
	for _, expected := range []string{"[INFO] chained message", "node=1", "term=5", "component=rpc"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected chained log to contain %q, got: %q", expected, output)
		}
	}

	// Test adding ad-hoc context to enriched logger
	chainedLogger.Infow("ad-hoc context", "key", "value")
	output = buf.String()
	buf.Reset()
	for _, expected := range []string{"[INFO] ad-hoc context", "node=1", "term=5", "component=rpc", "key=value"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected ad-hoc log to contain %q, got: %q", expected, output)
		}
	}

	// Test using With with enriched logger
	withLogger := chainedLogger.With("extra", "context")
	withLogger.Infow("with and enriched")
	output = buf.String()
	buf.Reset()
	for _, expected := range []string{"[INFO] with and enriched", "node=1", "term=5", "component=rpc", "extra=context"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected with+enriched log to contain %q, got: %q", expected, output)
		}
	}
}

func TestStdLogger_ContextOverride(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger()

	// Create logger with initial context
	nodeLogger := logger.WithNodeID(1)

	// Override context with new values
	newNodeLogger := nodeLogger.WithNodeID(2)

	// Check that original logger retains its context
	nodeLogger.Infow("original node")
	if !strings.Contains(buf.String(), "[INFO] original node") || !strings.Contains(buf.String(), "node=1") {
		t.Errorf("Original node log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	// Check that new logger has the updated context
	newNodeLogger.Infow("new node")
	if !strings.Contains(buf.String(), "[INFO] new node") || !strings.Contains(buf.String(), "node=2") {
		t.Errorf("New node log doesn't contain expected content: %s", buf.String())
	}
	buf.Reset()

	// Test overriding multiple contexts
	multiLogger := logger.WithNodeID(1).WithTerm(5)
	updatedLogger := multiLogger.WithNodeID(2).WithTerm(6)

	multiLogger.Infow("original multi")
	output := buf.String()
	buf.Reset()
	for _, expected := range []string{"[INFO] original multi", "node=1", "term=5"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected original multi log to contain %q, got: %q", expected, output)
		}
	}

	updatedLogger.Infow("updated multi")
	output = buf.String()
	buf.Reset()
	for _, expected := range []string{"[INFO] updated multi", "node=2", "term=6"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected updated multi log to contain %q, got: %q", expected, output)
		}
	}
}

func TestStdLogger_TemporaryContext(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger().WithNodeID(1)

	// Add temporary context for a single log entry
	logger.With("latency", "150ms").Warnw("Slow response")
	output := buf.String()
	buf.Reset()
	for _, expected := range []string{"[WARN] Slow response", "node=1", "latency=150ms"} {
		if !strings.Contains(output, expected) {
			t.Errorf("Expected log to contain %q, got: %q", expected, output)
		}
	}

	// Verify the original logger's context is unchanged
	logger.Infow("Normal message")
	output = buf.String()
	buf.Reset()
	if !strings.Contains(output, "[INFO] Normal message") || !strings.Contains(output, "node=1") {
		t.Errorf("Normal message log doesn't contain expected content: %s", output)
	}
	if strings.Contains(output, "latency") {
		t.Errorf("Normal message contains unexpected latency field: %s", output)
	}
}

// Add test for odd-length key-value pairs to verify our fix
func TestStdLogger_OddLengthKeyValuePairs(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger()

	// Test with odd number of arguments
	logger.Infow("odd kv pairs", "key1", "value1", "key2")
	output := buf.String()
	buf.Reset()

	if !strings.Contains(output, "[INFO] odd kv pairs") {
		t.Errorf("Expected log to contain message, got: %q", output)
	}

	if !strings.Contains(output, "key1=value1") {
		t.Errorf("Expected log to contain the complete key-value pair, got: %q", output)
	}

	if strings.Contains(output, "key2") {
		t.Errorf("Expected log to not contain the incomplete key, got: %q", output)
	}
}

// Test for non-string keys in direct logging calls
func TestStdLogger_NonStringKeysInLogging(t *testing.T) {
	var buf bytes.Buffer
	log.SetOutput(&buf)
	defer log.SetOutput(os.Stderr)

	logger := NewStdLogger()

	// Test with non-string keys in direct logging call
	logger.Infow("non-string keys", 123, "value1", "key2", "value2")
	output := buf.String()
	buf.Reset()

	if !strings.Contains(output, "[INFO] non-string keys") {
		t.Errorf("Expected log to contain message, got: %q", output)
	}

	if strings.Contains(output, "123=value1") {
		t.Errorf("Expected log to not contain the non-string key pair, got: %q", output)
	}

	if !strings.Contains(output, "key2=value2") {
		t.Errorf("Expected log to contain the valid key-value pair, got: %q", output)
	}
}
