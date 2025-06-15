package main

import (
	"context"
	"fmt"
	"maps"
	"sync"
	"time"
)

// BenchmarkContext wraps context with benchmark-specific functionality
type BenchmarkContext struct {
	context.Context
	suite     *BenchmarkSuite
	phase     string
	startTime time.Time
	mu        sync.RWMutex
	data      map[string]any
	metrics   map[string]int64
}

// NewBenchmarkContext creates a new benchmark context with phase tracking
func NewBenchmarkContext(
	ctx context.Context,
	suite *BenchmarkSuite,
	phase string,
) *BenchmarkContext {
	return &BenchmarkContext{
		Context:   ctx,
		suite:     suite,
		phase:     phase,
		startTime: time.Now(),
		data:      make(map[string]any),
		metrics:   make(map[string]int64),
	}
}

// Phase returns the current benchmark phase
func (bc *BenchmarkContext) Phase() string {
	return bc.phase
}

// Elapsed returns time elapsed since context creation
func (bc *BenchmarkContext) Elapsed() time.Duration {
	return time.Since(bc.startTime)
}

// Set stores a value in the context
func (bc *BenchmarkContext) Set(key string, value any) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.data[key] = value
}

// Get retrieves a value from the context
func (bc *BenchmarkContext) Get(key string) (any, bool) {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	value, exists := bc.data[key]
	return value, exists
}

// IncrementMetric atomically increments a metric counter
func (bc *BenchmarkContext) IncrementMetric(metric string) {
	bc.mu.Lock()
	defer bc.mu.Unlock()
	bc.metrics[metric]++
}

// GetMetric retrieves a metric value
func (bc *BenchmarkContext) GetMetric(metric string) int64 {
	bc.mu.RLock()
	defer bc.mu.RUnlock()
	return bc.metrics[metric]
}

// GetAllMetrics returns a copy of all metrics
func (bc *BenchmarkContext) GetAllMetrics() map[string]int64 {
	bc.mu.RLock()
	defer bc.mu.RUnlock()

	result := make(map[string]int64, len(bc.metrics))
	for k, v := range bc.metrics {
		result[k] = v
	}
	return result
}

// WithTimeout creates a child context with timeout
func (bc *BenchmarkContext) WithTimeout(
	timeout time.Duration,
) (*BenchmarkContext, context.CancelFunc) {
	ctx, cancel := context.WithTimeout(bc.Context, timeout)
	childCtx := &BenchmarkContext{
		Context:   ctx,
		suite:     bc.suite,
		phase:     bc.phase,
		startTime: bc.startTime,
		data:      make(map[string]any),
		metrics:   make(map[string]int64),
	}

	// Copy parent data and metrics
	bc.mu.RLock()
	maps.Copy(childCtx.data, bc.data)
	for k, v := range bc.metrics {
		childCtx.metrics[k] = v
	}
	bc.mu.RUnlock()

	return childCtx, cancel
}

// LogProgress logs progress with context information
func (bc *BenchmarkContext) LogProgress(message string, args ...any) {
	if bc.suite.config.Verbose {
		elapsed := bc.Elapsed()
		prefixedMsg := fmt.Sprintf(
			"[%s] [%v] %s",
			bc.phase,
			elapsed.Round(time.Millisecond),
			message,
		)
		bc.suite.logger.Infow(prefixedMsg, args...)
	}
}

// LogInfo logs informational messages
func (bc *BenchmarkContext) LogInfo(message string, args ...any) {
	prefixedMsg := fmt.Sprintf("[%s] %s", bc.phase, message)
	bc.suite.logger.Infow(prefixedMsg, args...)
}

// LogWarn logs warning messages
func (bc *BenchmarkContext) LogWarn(message string, args ...any) {
	prefixedMsg := fmt.Sprintf("[%s] %s", bc.phase, message)
	bc.suite.logger.Warnw(prefixedMsg, args...)
}

// LogError logs error messages
func (bc *BenchmarkContext) LogError(message string, args ...any) {
	prefixedMsg := fmt.Sprintf("[%s] %s", bc.phase, message)
	bc.suite.logger.Errorw(prefixedMsg, args...)
}

// CheckCancellation returns an error if context is canceled
func (bc *BenchmarkContext) CheckCancellation() error {
	select {
	case <-bc.Done():
		return ErrBenchmarkCanceled
	default:
		return nil
	}
}

// RecordOperation records an operation for metrics tracking
func (bc *BenchmarkContext) RecordOperation(
	operation string,
	success bool,
	duration time.Duration,
) {
	bc.IncrementMetric("total_operations")
	if success {
		bc.IncrementMetric("successful_operations")
	} else {
		bc.IncrementMetric("failed_operations")
	}

	// Record operation-specific metrics
	bc.IncrementMetric(fmt.Sprintf("%s_operations", operation))
	if success {
		bc.IncrementMetric(fmt.Sprintf("%s_successful", operation))
	} else {
		bc.IncrementMetric(fmt.Sprintf("%s_failed", operation))
	}

	// Record duration in context data for later analysis
	durations, exists := bc.Get(fmt.Sprintf("%s_durations", operation))
	if !exists {
		durations = []time.Duration{}
	}
	durationList := durations.([]time.Duration)
	durationList = append(durationList, duration)
	bc.Set(fmt.Sprintf("%s_durations", operation), durationList)
}

// GetOperationStats returns operation statistics
func (bc *BenchmarkContext) GetOperationStats(operation string) (total, successful, failed int64) {
	total = bc.GetMetric(fmt.Sprintf("%s_operations", operation))
	successful = bc.GetMetric(fmt.Sprintf("%s_successful", operation))
	failed = bc.GetMetric(fmt.Sprintf("%s_failed", operation))
	return
}

// GetPhaseStats returns overall phase statistics
func (bc *BenchmarkContext) GetPhaseStats() (total, successful, failed int64, duration time.Duration) {
	total = bc.GetMetric("total_operations")
	successful = bc.GetMetric("successful_operations")
	failed = bc.GetMetric("failed_operations")
	duration = bc.Elapsed()
	return
}
