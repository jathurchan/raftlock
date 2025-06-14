package server

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/types"
)

type testProposalBuilder struct {
	id        types.ProposalID
	index     types.Index
	term      types.Term
	operation types.LockOperation
	clientID  types.ClientID
	lockID    types.LockID
	ctx       context.Context
	resultCh  chan types.ProposalResult
}

func newTestProposal(id string) *testProposalBuilder {
	return &testProposalBuilder{
		id:        types.ProposalID(id),
		index:     1,
		term:      1,
		operation: types.OperationAcquire,
		clientID:  types.ClientID("test-client"),
		lockID:    types.LockID("test-lock"),
		ctx:       context.Background(),
		resultCh:  make(chan types.ProposalResult, 1),
	}
}

func (b *testProposalBuilder) withIndex(index types.Index) *testProposalBuilder {
	b.index = index
	return b
}

func (b *testProposalBuilder) withTerm(term types.Term) *testProposalBuilder {
	b.term = term
	return b
}

func (b *testProposalBuilder) withOperation(op types.LockOperation) *testProposalBuilder {
	b.operation = op
	return b
}

func (b *testProposalBuilder) withContext(ctx context.Context) *testProposalBuilder {
	b.ctx = ctx
	return b
}

func (b *testProposalBuilder) withBufferedResult(size int) *testProposalBuilder {
	b.resultCh = make(chan types.ProposalResult, size)
	return b
}

func (b *testProposalBuilder) build() *types.PendingProposal {
	return &types.PendingProposal{
		ID:        b.id,
		Index:     b.index,
		Term:      b.term,
		Operation: b.operation,
		StartTime: time.Now(),
		ResultCh:  b.resultCh,
		Context:   b.ctx,
		Command:   fmt.Appendf(nil, "cmd-%s", b.id),
		ClientID:  b.clientID,
		LockID:    b.lockID,
	}
}

func createApplyMsg(
	index types.Index,
	term types.Term,
	data interface{},
	err error,
) types.ApplyMsg {
	return types.ApplyMsg{
		CommandValid:       true,
		Command:            []byte(fmt.Sprintf("cmd-%d-%d", term, index)),
		CommandIndex:       index,
		CommandTerm:        term,
		CommandResultData:  data,
		CommandResultError: err,
	}
}

func waitForResult(
	t *testing.T,
	ch <-chan types.ProposalResult,
	timeout time.Duration,
	expectResult bool,
) (*types.ProposalResult, bool) {
	t.Helper()

	select {
	case result := <-ch:
		if !expectResult {
			t.Errorf("Received unexpected result: %+v", result)
			return nil, false
		}
		return &result, true
	case <-time.After(timeout):
		if expectResult {
			t.Errorf("Expected result but timed out after %v", timeout)
			return nil, false
		}
		return nil, false
	}
}

func assertContains(t *testing.T, s, substr string) {
	t.Helper()
	if !strings.Contains(s, substr) {
		t.Errorf("Expected string %q to contain %q", s, substr)
	}
}

func assertNotContains(t *testing.T, s, substr string) {
	t.Helper()
	if strings.Contains(s, substr) {
		t.Errorf("Expected string %q to not contain %q", s, substr)
	}
}

func TestProposalTracker_Creation(t *testing.T) {
	tests := []struct {
		name       string
		options    []ProposalTrackerOption
		validateFn func(t *testing.T, pt ProposalTracker)
	}{
		{
			name:    "default configuration",
			options: nil,
			validateFn: func(t *testing.T, pt ProposalTracker) {
				if count := pt.GetPendingCount(); count != 0 {
					t.Errorf("Expected 0 pending proposals, got %d", count)
				}

				stats := pt.GetStats()
				if stats.TotalProposals != 0 {
					t.Errorf("Expected 0 total proposals, got %d", stats.TotalProposals)
				}
			},
		},
		{
			name: "with custom options",
			options: []ProposalTrackerOption{
				WithMaxPendingAge(5 * time.Minute),
				WithCleanupInterval(30 * time.Second),
				WithClock(newMockClock()),
			},
			validateFn: func(t *testing.T, pt ProposalTracker) {
				if count := pt.GetPendingCount(); count != 0 {
					t.Errorf("Expected 0 pending proposals, got %d", count)
				}
			},
		},
		{
			name: "disabled cleanup",
			options: []ProposalTrackerOption{
				WithCleanupInterval(0), // Disable cleanup
			},
			validateFn: func(t *testing.T, pt ProposalTracker) {
				// Should not crash and work normally
				if count := pt.GetPendingCount(); count != 0 {
					t.Errorf("Expected 0 pending proposals, got %d", count)
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := logger.NewNoOpLogger()
			pt := NewProposalTracker(logger, tt.options...)
			defer func() {
				if err := pt.Close(); err != nil {
					t.Errorf("Error closing tracker: %v", err)
				}
			}()

			tt.validateFn(t, pt)
		})
	}
}

func TestProposalTracker_TrackValidation(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)
	defer pt.Close()

	tests := []struct {
		name          string
		proposal      *types.PendingProposal
		expectError   bool
		errorContains string
	}{
		{
			name:          "nil proposal",
			proposal:      nil,
			expectError:   true,
			errorContains: "proposal cannot be nil",
		},
		{
			name:          "empty ID",
			proposal:      newTestProposal("").build(),
			expectError:   true,
			errorContains: "proposal ID cannot be empty",
		},
		{
			name: "nil result channel",
			proposal: func() *types.PendingProposal {
				p := newTestProposal("test-nil-ch").build()
				p.ResultCh = nil
				return p
			}(),
			expectError:   true,
			errorContains: "proposal ResultCh cannot be nil",
		},
		{
			name:        "valid proposal",
			proposal:    newTestProposal("valid-1").build(),
			expectError: false,
		},
		{
			name:          "duplicate ID",
			proposal:      newTestProposal("valid-1").withIndex(2).build(), // Same ID as above
			expectError:   true,
			errorContains: "already being tracked",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := pt.Track(tt.proposal)

			if tt.expectError {
				if err == nil {
					t.Fatal("Expected error but got none")
				}
				if tt.errorContains != "" {
					assertContains(t, err.Error(), tt.errorContains)
				}
			} else {
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
		})
	}
}

func TestProposalTracker_SuccessfulFlow(t *testing.T) {
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	pt := NewProposalTracker(logger, WithClock(clock))
	defer pt.Close()

	proposal := newTestProposal("5-10").withIndex(10).withTerm(5).build()
	err := pt.Track(proposal)
	if err != nil {
		t.Fatalf("Failed to track proposal: %v", err)
	}

	if count := pt.GetPendingCount(); count != 1 {
		t.Errorf("Expected 1 pending proposal, got %d", count)
	}

	// Advance time to simulate processing
	clock.Advance(50 * time.Millisecond)

	applyMsg := createApplyMsg(10, 5, "success-data", nil)
	pt.HandleAppliedCommand(applyMsg)

	result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
	if !ok {
		t.Fatal("Failed to get result")
	}

	if !result.Success {
		t.Errorf("Expected successful result, got error: %v", result.Error)
	}
	if result.Data != "success-data" {
		t.Errorf("Expected data 'success-data', got %v", result.Data)
	}
	if result.Duration == 0 {
		t.Errorf("Expected non-zero duration")
	}

	if count := pt.GetPendingCount(); count != 0 {
		t.Errorf("Expected 0 pending proposals after completion, got %d", count)
	}

	stats := pt.GetStats()
	if stats.TotalProposals != 1 {
		t.Errorf("Expected 1 total proposal, got %d", stats.TotalProposals)
	}
	if stats.SuccessfulProposals != 1 {
		t.Errorf("Expected 1 successful proposal, got %d", stats.SuccessfulProposals)
	}
}

func TestProposalTracker_FailedFlow(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)
	defer pt.Close()

	proposal := newTestProposal("5-10").withIndex(10).withTerm(5).build()
	err := pt.Track(proposal)
	if err != nil {
		t.Fatalf("Failed to track proposal: %v", err)
	}

	applyError := errors.New("state machine error")
	applyMsg := createApplyMsg(10, 5, nil, applyError)
	pt.HandleAppliedCommand(applyMsg)

	result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
	if !ok {
		t.Fatal("Failed to get result")
	}

	if result.Success {
		t.Errorf("Expected failed result")
	}
	if result.Error != applyError {
		t.Errorf("Expected error %v, got %v", applyError, result.Error)
	}

	stats := pt.GetStats()
	if stats.FailedProposals != 1 {
		t.Errorf("Expected 1 failed proposal, got %d", stats.FailedProposals)
	}
}

func TestProposalTracker_ClientCancellation(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)
	defer pt.Close()

	tests := []struct {
		name           string
		setupProposal  func() *types.PendingProposal
		cancelID       types.ProposalID
		cancelReason   error
		expectCanceled bool
	}{
		{
			name: "cancel existing proposal",
			setupProposal: func() *types.PendingProposal {
				return newTestProposal("cancel-existing").build()
			},
			cancelID:       "cancel-existing",
			cancelReason:   context.Canceled,
			expectCanceled: true,
		},
		{
			name: "cancel non-existent proposal",
			setupProposal: func() *types.PendingProposal {
				return newTestProposal("cancel-existing-2").build()
			},
			cancelID:       "non-existent",
			cancelReason:   context.Canceled,
			expectCanceled: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proposal := tt.setupProposal()
			err := pt.Track(proposal)
			if err != nil {
				t.Fatalf("Failed to track proposal: %v", err)
			}

			canceled := pt.ClientCancel(tt.cancelID, tt.cancelReason)
			if canceled != tt.expectCanceled {
				t.Errorf("Expected canceled=%v, got %v", tt.expectCanceled, canceled)
			}

			if tt.expectCanceled && tt.cancelID == proposal.ID {
				result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
				if !ok {
					t.Fatal("Failed to get cancellation result")
				}

				if result.Success {
					t.Errorf("Expected canceled proposal to fail")
				}
				if result.Error != tt.cancelReason {
					t.Errorf("Expected error %v, got %v", tt.cancelReason, result.Error)
				}
			}
		})
	}
}

func TestProposalTracker_SnapshotInvalidation(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)
	defer pt.Close()

	// Snapshot will be at index 12, so proposals with index <= 12 should be invalidated
	proposals := []*types.PendingProposal{
		newTestProposal(
			"1-5",
		).withIndex(5).
			withTerm(1).
			build(), // Should be invalidated (5 <= 12)
		newTestProposal(
			"1-10",
		).withIndex(10).
			withTerm(1).
			build(), // Should be invalidated (10 <= 12)
		newTestProposal("1-15").withIndex(15).withTerm(1).build(), // Should remain (15 > 12)
		newTestProposal("1-20").withIndex(20).withTerm(1).build(), // Should remain (20 > 12)
	}

	for _, proposal := range proposals {
		err := pt.Track(proposal)
		if err != nil {
			t.Fatalf("Failed to track proposal %s: %v", proposal.ID, err)
		}
	}

	if count := pt.GetPendingCount(); count != 4 {
		t.Fatalf("Expected 4 pending proposals before snapshot, got %d", count)
	}

	// Apply snapshot at index 12 - this should invalidate proposals with index <= 12
	pt.HandleSnapshotApplied(12, 3)

	// First two proposals should be invalidated (indices 5 and 10 are <= 12)
	for i := range 2 {
		result, ok := waitForResult(t, proposals[i].ResultCh, 100*time.Millisecond, true)
		if !ok {
			t.Fatalf("Failed to get invalidation result for proposal %d (ID: %s, Index: %d)",
				i, proposals[i].ID, proposals[i].Index)
		}

		if result.Success {
			t.Errorf("Expected proposal %d (ID: %s, Index: %d) to be invalidated",
				i, proposals[i].ID, proposals[i].Index)
		}
		assertContains(t, result.Error.Error(), "invalidated by snapshot")
	}

	// Third and fourth proposals should still be pending (indices 15 and 20 are > 12)
	for i := 2; i < 4; i++ {
		_, ok := waitForResult(t, proposals[i].ResultCh, 10*time.Millisecond, false)
		if ok {
			t.Errorf("Proposal %d (ID: %s, Index: %d) should still be pending",
				i, proposals[i].ID, proposals[i].Index)
		}
	}

	if count := pt.GetPendingCount(); count != 2 {
		t.Errorf("Expected 2 pending proposals after snapshot, got %d", count)
	}
}

func TestProposalTracker_ContextCancellation(t *testing.T) {
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	pt := NewProposalTracker(logger,
		WithClock(clock),
		WithMaxPendingAge(time.Hour),
		WithCleanupInterval(0),
	)
	defer pt.Close()

	ctx, cancel := context.WithCancel(context.Background())
	proposal := newTestProposal("1-10").withContext(ctx).withIndex(10).withTerm(1).build()

	err := pt.Track(proposal)
	if err != nil {
		t.Fatalf("Failed to track proposal: %v", err)
	}

	cancel()

	select {
	case <-ctx.Done():
	case <-time.After(1 * time.Second):
		t.Fatal("Context was not cancelled within timeout")
	}

	applyMsg := createApplyMsg(10, 1, "data", nil)
	pt.HandleAppliedCommand(applyMsg)

	result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
	if !ok {
		t.Fatal("Failed to get result for cancelled context from HandleAppliedCommand")
	}

	if result.Success {
		t.Errorf("Expected failure for cancelled context")
	}
	if !errors.Is(result.Error, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got %v", result.Error)
	}

	if count := pt.GetPendingCount(); count != 0 {
		t.Errorf("Expected 0 pending proposals after cancellation, got %d", count)
	}

	cleaned := pt.Cleanup()
	if cleaned != 0 {
		t.Errorf("Expected cleanup to find 0 proposals (already processed), got %d", cleaned)
	}
}

func TestProposalTracker_Cleanup(t *testing.T) {
	logger := logger.NewNoOpLogger()
	clock := newMockClock()

	pt := NewProposalTracker(logger,
		WithClock(clock),
		WithMaxPendingAge(100*time.Millisecond),
		WithCleanupInterval(0), // Disable automatic cleanup
	)
	defer pt.Close()

	proposal1 := newTestProposal("cleanup-old").build()
	err := pt.Track(proposal1)
	if err != nil {
		t.Fatalf("Failed to track proposal1: %v", err)
	}

	clock.Advance(50 * time.Millisecond)

	proposal2 := newTestProposal("cleanup-new").build()
	err = pt.Track(proposal2)
	if err != nil {
		t.Fatalf("Failed to track proposal2: %v", err)
	}

	clock.Advance(60 * time.Millisecond) // Total: 110ms for first, 60ms for second

	cleaned := pt.Cleanup()
	if cleaned != 1 {
		t.Errorf("Expected to clean 1 proposal, got %d", cleaned)
	}

	result, ok := waitForResult(t, proposal1.ResultCh, 100*time.Millisecond, true)
	if !ok {
		t.Fatal("Failed to get cleanup result for expired proposal")
	}
	if result.Success {
		t.Errorf("Expected expired proposal to fail")
	}

	_, ok = waitForResult(t, proposal2.ResultCh, 10*time.Millisecond, false)
	if ok {
		t.Errorf("Second proposal should still be pending")
	}

	if count := pt.GetPendingCount(); count != 1 {
		t.Errorf("Expected 1 pending proposal after cleanup, got %d", count)
	}
}

func TestProposalTracker_AutomaticCleanup(t *testing.T) {
	logger := logger.NewNoOpLogger()
	clock := newMockClock()

	pt := NewProposalTracker(logger,
		WithClock(clock),
		WithMaxPendingAge(50*time.Millisecond),
		WithCleanupInterval(20*time.Millisecond),
	)
	defer pt.Close()

	ctx, cancel := context.WithCancel(context.Background())
	proposal := newTestProposal("auto-cleanup").withContext(ctx).build()

	err := pt.Track(proposal)
	if err != nil {
		t.Fatalf("Failed to track proposal: %v", err)
	}

	cancel()

	clock.Advance(25 * time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	clock.Advance(50 * time.Millisecond)

	time.Sleep(10 * time.Millisecond)

	result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
	if !ok {
		t.Fatal("Failed to get automatic cleanup result")
	}

	if result.Success {
		t.Errorf("Expected cleanup to fail the proposal")
	}
	if !errors.Is(result.Error, context.Canceled) {
		t.Errorf("Expected context.Canceled error, got %v", result.Error)
	}
}

func TestProposalTracker_ConcurrentOperations(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)
	defer pt.Close()

	const (
		numWorkers         = 10
		proposalsPerWorker = 20
		totalProposals     = numWorkers * proposalsPerWorker
	)

	var wg sync.WaitGroup
	errors := make(chan error, totalProposals)
	proposals := make([]*types.PendingProposal, totalProposals)

	for i := range numWorkers {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for j := range proposalsPerWorker {
				idx := workerID*proposalsPerWorker + j
				id := fmt.Sprintf("concurrent-%d-%d", workerID, j)
				proposal := newTestProposal(id).withIndex(types.Index(idx + 1)).build()
				proposals[idx] = proposal

				if err := pt.Track(proposal); err != nil {
					errors <- fmt.Errorf("worker %d: %w", workerID, err)
				}
			}
		}(i)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(5 * time.Millisecond)

		for i := range 10 {
			applyMsg := createApplyMsg(types.Index(i+1), 1, fmt.Sprintf("data-%d", i), nil)
			pt.HandleAppliedCommand(applyMsg)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		time.Sleep(10 * time.Millisecond)

		for i := range 5 {
			id := fmt.Sprintf("concurrent-%d-5", i)
			pt.ClientCancel(types.ProposalID(id), context.Canceled)
			time.Sleep(1 * time.Millisecond)
		}
	}()

	wg.Wait()
	close(errors)

	for err := range errors {
		t.Errorf("Concurrent operation error: %v", err)
	}

	stats := pt.GetStats()
	if stats.TotalProposals != totalProposals {
		t.Errorf("Expected %d total proposals, got %d", totalProposals, stats.TotalProposals)
	}

	processed := stats.SuccessfulProposals + stats.FailedProposals + stats.CancelledProposals
	if processed == 0 {
		t.Errorf("Expected some proposals to be processed")
	}

	expectedPending := totalProposals - processed
	if stats.PendingProposals != expectedPending {
		t.Errorf("Expected %d pending proposals, got %d", expectedPending, stats.PendingProposals)
	}
}

func TestProposalTracker_GracefulShutdown(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)

	proposals := []*types.PendingProposal{
		newTestProposal("shutdown-1").build(),
		newTestProposal("shutdown-2").build(),
		newTestProposal("shutdown-3").build(),
	}

	for _, proposal := range proposals {
		err := pt.Track(proposal)
		if err != nil {
			t.Fatalf("Failed to track proposal %s: %v", proposal.ID, err)
		}
	}

	err := pt.Close()
	if err != nil {
		t.Errorf("Unexpected error during close: %v", err)
	}

	for i, proposal := range proposals {
		result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
		if !ok {
			t.Fatalf("Failed to get shutdown result for proposal %d", i)
		}

		if result.Success {
			t.Errorf("Expected proposal %d to fail on shutdown", i)
		}
		assertContains(t, result.Error.Error(), "tracker shutdown")
	}

	newProposal := newTestProposal("post-shutdown").build()
	err = pt.Track(newProposal)
	if err == nil {
		t.Errorf("Expected error when tracking proposal after shutdown")
	}
	assertContains(t, err.Error(), "tracker is closed")

	err = pt.Close()
	if err != nil {
		t.Errorf("Multiple close should not error: %v", err)
	}
}

func TestProposalTracker_StatisticsAccuracy(t *testing.T) {
	logger := logger.NewNoOpLogger()
	clock := newMockClock()
	pt := NewProposalTracker(logger, WithClock(clock))
	defer pt.Close()

	successProposal := newTestProposal("1-1").withIndex(1).withTerm(1).build()
	failProposal := newTestProposal("1-2").withIndex(2).withTerm(1).build()
	cancelProposal := newTestProposal("1-3").withIndex(3).withTerm(1).build()

	for _, p := range []*types.PendingProposal{successProposal, failProposal, cancelProposal} {
		err := pt.Track(p)
		if err != nil {
			t.Fatalf("Failed to track proposal %s: %v", p.ID, err)
		}
		clock.Advance(10 * time.Millisecond) // Different start times
	}

	clock.Advance(50 * time.Millisecond)
	pt.HandleAppliedCommand(createApplyMsg(1, 1, "success", nil))

	clock.Advance(30 * time.Millisecond)
	pt.HandleAppliedCommand(createApplyMsg(2, 1, nil, errors.New("state machine error")))

	pt.ClientCancel("1-3", context.Canceled)

	stats := pt.GetStats()

	expectedStats := map[string]int64{
		"total":      3,
		"successful": 1,
		"failed":     1,
		"cancelled":  1,
		"pending":    0,
	}

	actual := map[string]int64{
		"total":      stats.TotalProposals,
		"successful": stats.SuccessfulProposals,
		"failed":     stats.FailedProposals,
		"cancelled":  stats.CancelledProposals,
		"pending":    stats.PendingProposals,
	}

	for key, expected := range expectedStats {
		if actual[key] != expected {
			t.Errorf("Expected %s=%d, got %d", key, expected, actual[key])
		}
	}

	if stats.AverageLatency == 0 {
		t.Errorf("Expected non-zero average latency")
	}
	if stats.MaxLatency == 0 {
		t.Errorf("Expected non-zero max latency")
	}

	results := make([]*types.ProposalResult, 3)
	proposals := []*types.PendingProposal{successProposal, failProposal, cancelProposal}

	for i, proposal := range proposals {
		result, ok := waitForResult(t, proposal.ResultCh, 100*time.Millisecond, true)
		if !ok {
			t.Fatalf("Failed to get result for proposal %d", i)
		}
		results[i] = result
	}

	if !results[0].Success {
		t.Errorf("Expected first proposal to succeed")
	}
	if results[1].Success {
		t.Errorf("Expected second proposal to fail")
	}
	if results[2].Success {
		t.Errorf("Expected third proposal to be cancelled")
	}
}

func TestProposalTracker_GetPendingProposal(t *testing.T) {
	logger := logger.NewNoOpLogger()
	pt := NewProposalTracker(logger)
	defer pt.Close()

	original := newTestProposal("7-42").withIndex(42).withTerm(7).build()
	err := pt.Track(original)
	if err != nil {
		t.Fatalf("Failed to track proposal: %v", err)
	}

	tests := []struct {
		name       string
		proposalID types.ProposalID
		expectFind bool
	}{
		{
			name:       "existing proposal",
			proposalID: "7-42",
			expectFind: true,
		},
		{
			name:       "non-existent proposal",
			proposalID: "non-existent",
			expectFind: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			proposal, found := pt.GetPendingProposal(tt.proposalID)

			if found != tt.expectFind {
				t.Errorf("Expected found=%v, got %v", tt.expectFind, found)
			}

			if tt.expectFind {
				// Verify it's a copy with correct data
				if proposal.ID != original.ID {
					t.Errorf("Expected ID=%s, got %s", original.ID, proposal.ID)
				}
				if proposal.Index != original.Index {
					t.Errorf("Expected Index=%d, got %d", original.Index, proposal.Index)
				}
				if proposal.Term != original.Term {
					t.Errorf("Expected Term=%d, got %d", original.Term, proposal.Term)
				}
				if proposal.Operation != original.Operation {
					t.Errorf(
						"Expected Operation=%s, got %s",
						original.Operation,
						proposal.Operation,
					)
				}
			}
		})
	}

	pt.HandleAppliedCommand(createApplyMsg(42, 7, "data", nil))

	_, found := pt.GetPendingProposal("7-42")
	if found {
		t.Errorf("Expected proposal to not be found after completion")
	}
}

func TestProposalTracker_ErrorHandling(t *testing.T) {
	t.Run("panic recovery in send result", func(t *testing.T) {
		logger := logger.NewNoOpLogger()
		pt := NewProposalTracker(logger)
		defer pt.Close()

		proposal := newTestProposal("panic-test").build()
		close(proposal.ResultCh)

		err := pt.Track(proposal)
		if err != nil {
			t.Fatalf("Failed to track proposal: %v", err)
		}

		defer func() {
			if r := recover(); r != nil {
				t.Errorf("Unexpected panic: %v", r)
			}
		}()

		pt.HandleAppliedCommand(createApplyMsg(1, 1, "data", nil))
		time.Sleep(10 * time.Millisecond) // Allow async processing
	})

	t.Run("operations after close", func(t *testing.T) {
		logger := logger.NewNoOpLogger()
		pt := NewProposalTracker(logger)
		pt.Close()

		proposal := newTestProposal("after-close").build()
		err := pt.Track(proposal)
		if err == nil {
			t.Errorf("Expected error tracking proposal after close")
		}

		pt.HandleAppliedCommand(createApplyMsg(1, 1, "data", nil))
		pt.HandleSnapshotApplied(10, 1)
		canceled := pt.ClientCancel("non-existent", context.Canceled)
		if canceled {
			t.Errorf("Should not be able to cancel after close")
		}

		count := pt.GetPendingCount()
		if count != 0 {
			t.Errorf("Expected 0 pending after close, got %d", count)
		}

		cleaned := pt.Cleanup()
		if cleaned != 0 {
			t.Errorf("Expected 0 cleaned after close, got %d", cleaned)
		}
	})
}

func TestProposalTracker_SendResultFallback(t *testing.T) {
	setupTest := func(name string, bufferSize int, ctx context.Context) (ProposalTracker, *types.PendingProposal) {
		logger := logger.NewNoOpLogger()
		pt := NewProposalTracker(logger)

		builder := newTestProposal(fmt.Sprintf("%d-%d", 1, 1)). // term-index format
									withIndex(1).
									withTerm(1).
									withBufferedResult(bufferSize)
		if ctx != nil {
			builder = builder.withContext(ctx)
		}
		proposal := builder.build()

		err := pt.Track(proposal)
		if err != nil {
			t.Fatalf("Track failed: %v", err)
		}
		return pt, proposal
	}

	t.Run("buffered channel success", func(t *testing.T) {
		pt, proposal := setupTest("buffered", 1, nil)
		defer pt.Close()

		pt.HandleAppliedCommand(createApplyMsg(1, 1, "success", nil))

		select {
		case result := <-proposal.ResultCh:
			if !result.Success || result.Data != "success" {
				t.Errorf(
					"Expected success with data 'success', got success=%v, data=%v",
					result.Success,
					result.Data,
				)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Expected immediate result from buffered channel")
		}
	})

	t.Run("unbuffered channel with receiver", func(t *testing.T) {
		pt, proposal := setupTest("unbuffered", 0, nil)
		defer pt.Close()

		done := make(chan types.ProposalResult, 1)
		go func() {
			done <- <-proposal.ResultCh
		}()

		pt.HandleAppliedCommand(createApplyMsg(1, 1, "blocking", nil))

		select {
		case result := <-done:
			if !result.Success || result.Data != "blocking" {
				t.Errorf(
					"Expected success with data 'blocking', got success=%v, data=%v",
					result.Success,
					result.Data,
				)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Blocking send should have succeeded")
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel() // Cancel immediately

		pt, proposal := setupTest("cancelled", 0, ctx)
		defer pt.Close()

		pt.HandleAppliedCommand(createApplyMsg(1, 1, "ignored", nil))

		select {
		case result := <-proposal.ResultCh:
			t.Errorf("Should not receive result for cancelled context: %+v", result)
		case <-time.After(50 * time.Millisecond):
			// Expected - no result due to cancelled context
		}
	})

	t.Run("panic recovery on closed channel", func(t *testing.T) {
		pt, proposal := setupTest("panic", 1, nil)
		defer pt.Close()

		close(proposal.ResultCh) // This will cause panic when sending

		// Should not crash due to panic recovery
		pt.HandleAppliedCommand(createApplyMsg(1, 1, "panic", nil))

		// Small delay to ensure sendResult completes
		time.Sleep(10 * time.Millisecond)
		// If we reach here without crashing, panic recovery worked
	})

	t.Run("full buffer fallback", func(t *testing.T) {
		pt, proposal := setupTest("full", 1, nil)
		defer pt.Close()

		// Fill the buffer
		proposal.ResultCh <- types.ProposalResult{Success: true, Data: "filler"}

		done := make(chan []types.ProposalResult, 1)
		go func() {
			var results []types.ProposalResult
			results = append(results, <-proposal.ResultCh) // filler
			results = append(results, <-proposal.ResultCh) // actual
			done <- results
		}()

		pt.HandleAppliedCommand(createApplyMsg(1, 1, "after-full", nil))

		select {
		case results := <-done:
			if len(results) != 2 {
				t.Fatalf("Expected 2 results, got %d", len(results))
			}
			if results[0].Data != "filler" {
				t.Errorf("Expected first result 'filler', got %v", results[0].Data)
			}
			if results[1].Data != "after-full" {
				t.Errorf("Expected second result 'after-full', got %v", results[1].Data)
			}
		case <-time.After(100 * time.Millisecond):
			t.Fatal("Should have received both results")
		}
	})
}
