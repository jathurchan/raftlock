package server

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/raft"
	"github.com/jathurchan/raftlock/types"
)

// ProposalTracker defines the interface for managing Raft proposals,
// tracking their lifecycle, and resolving outcomes as they are applied or invalidated.
type ProposalTracker interface {
	// Track begins monitoring a new proposal.
	// The proposal.ID must be unique.
	// The proposal.ResultCh is used to send back the outcome and
	// should be a buffered channel to prevent blocking the tracker.
	Track(proposal *types.PendingProposal) error

	// HandleAppliedCommand is called when a Raft log entry has been successfully
	// applied to the state machine. It resolves the associated pending proposal.
	// 'commandResultData' and 'commandError' represent the result of the state machine's application.
	HandleAppliedCommand(applyMsg types.ApplyMsg, commandResultData interface{}, commandError error)

	// HandleSnapshotApplied is invoked when a snapshot is applied to the state machine.
	// This invalidates any pending proposals that fall within the snapshot's covered range.
	HandleSnapshotApplied(snapshotIndex types.Index, snapshotTerm types.Term)

	// ClientCancel is called when a client's context is canceled before the proposal
	// is committed and applied. It cleans up the corresponding pending proposal and
	// signals any waiting goroutines.
	// Returns true if the proposal was found and canceled; false otherwise.
	ClientCancel(proposalID types.ProposalID, reason error) bool

	// GetPendingCount returns the total number of proposals currently pending
	// Raft commitment and application.
	GetPendingCount() int64

	// GetStats returns aggregated metrics and statistics related to proposal tracking.
	GetStats() types.ProposalStats

	// GetPendingProposal retrieves a *copy* of the specified pending proposal, if it exists.
	// Useful for observation and debugging; does not alter internal tracker state.
	GetPendingProposal(proposalID types.ProposalID) (types.PendingProposal, bool)

	// Cleanup performs manual removal of expired or stale pending proposals.
	// Returns the number of proposals removed.
	Cleanup() int

	// Close gracefully shuts down the tracker, terminating background processes
	// and failing any unresolved proposals.
	// Returns an error if the shutdown process encounters issues.
	Close() error
}

// proposalStatus represents the lifecycle state of a proposal within the tracker.
type proposalStatus int

const (
	// proposalStatusPending indicates the proposal is awaiting commitment and application by Raft.
	proposalStatusPending proposalStatus = iota

	// proposalStatusAppliedSuccess indicates the proposal was successfully applied to the state machine.
	proposalStatusAppliedSuccess

	// proposalStatusAppliedFailure indicates the proposal was applied, but the state machine returned an error.
	proposalStatusAppliedFailure

	// proposalStatusClientCancelled means the proposal was cancelled by the client before being committed.
	proposalStatusClientCancelled

	// proposalStatusSnapshotInvalidated means the proposal was invalidated due to a snapshot
	// that included or superseded its log entry.
	proposalStatusSnapshotInvalidated

	// proposalStatusExpired means the proposal was removed due to expiration or timeout,
	// typically during cleanup.
	proposalStatusExpired

	// proposalStatusTrackerShutdown indicates the proposal failed due to the tracker shutting down
	// before it could be applied or resolved.
	proposalStatusTrackerShutdown
)

func (ps proposalStatus) String() string {
	switch ps {
	case proposalStatusPending:
		return "pending"
	case proposalStatusAppliedSuccess:
		return "applied_success"
	case proposalStatusAppliedFailure:
		return "applied_failure"
	case proposalStatusClientCancelled:
		return "client_cancelled"
	case proposalStatusSnapshotInvalidated:
		return "snapshot_invalidated"
	case proposalStatusExpired:
		return "expired_by_tracker"
	case proposalStatusTrackerShutdown:
		return "failed_tracker_shutdown"
	default:
		return "unknown"
	}
}

// proposalEntry wraps a PendingProposal with internal metadata used for lifecycle tracking.
type proposalEntry struct {
	*types.PendingProposal

	status      proposalStatus // Current internal status of the proposal
	submittedAt time.Time      // Timestamp when Track() was called
	lastAccess  time.Time      // Last access time
	finalizedAt time.Time      // Timestamp when the proposal transitioned to a terminal state
}

// proposalTracker is the default implementation of the ProposalTracker interface.
// It manages active Raft proposals, handles their resolution, tracks statistics,
// and performs periodic cleanup of expired entries.
type proposalTracker struct {
	mu sync.RWMutex

	proposals map[types.ProposalID]*proposalEntry // Active proposals

	maxPendingAge   time.Duration // Max duration a proposal may remain pending before expiring
	cleanupInterval time.Duration // Interval between periodic cleanup cycles

	stopCleanup chan struct{}  // Signal channel to stop the cleanup goroutine
	cleanupWg   sync.WaitGroup // WaitGroup to manage cleanup goroutine lifecycle

	stats         types.ProposalStats
	totalDuration time.Duration // Cumulative duration of finalized proposals (for avg latency)
	closed        atomic.Bool   // Indicates whether the tracker has been closed

	logger        logger.Logger
	clock         raft.Clock
	tickerFactory func(d time.Duration) raft.Ticker
}

// ProposalTrackerOption defines a functional option for configuring a proposalTracker instance.
type ProposalTrackerOption func(*proposalTracker)

// WithMaxPendingAge configures the maximum allowed age for a proposal before it is considered expired
// and eligible for removal during cleanup. A non-positive value disables automatic expiration.
func WithMaxPendingAge(age time.Duration) ProposalTrackerOption {
	return func(pt *proposalTracker) {
		if age > 0 {
			pt.maxPendingAge = age
		}
	}
}

// WithCleanupInterval sets the frequency at which the background cleanup process runs.
// A non-positive interval disables the periodic cleanup goroutine.
func WithCleanupInterval(interval time.Duration) ProposalTrackerOption {
	return func(pt *proposalTracker) {
		if interval > 0 {
			pt.cleanupInterval = interval
		}
	}
}

// WithClock injects a custom clock implementation used for time-based operations such as expiration.
func WithClock(clock raft.Clock) ProposalTrackerOption {
	return func(pt *proposalTracker) {
		pt.clock = clock
	}
}

// NewProposalTracker creates a new enhanced proposal tracker.
func NewProposalTracker(logger logger.Logger, opts ...ProposalTrackerOption) ProposalTracker {
	pt := &proposalTracker{
		proposals:       make(map[types.ProposalID]*proposalEntry),
		maxPendingAge:   DefaultProposalMaxPendingAge,
		cleanupInterval: DefaultProposalCleanupInterval,
		stopCleanup:     make(chan struct{}),
		logger:          logger.WithComponent("proposaltracker"),
		clock:           raft.NewStandardClock(),
	}

	pt.tickerFactory = pt.clock.NewTicker

	for _, opt := range opts {
		opt(pt)
	}

	pt.startPeriodicCleanup()
	pt.logger.Infow("Proposal tracker initialized", "maxPendingAge", pt.maxPendingAge, "cleanupInterval", pt.cleanupInterval)
	return pt
}

// startPeriodicCleanup launches a background goroutine that periodically runs the Cleanup method.
func (pt *proposalTracker) startPeriodicCleanup() {
	if pt.cleanupInterval <= 0 {
		pt.logger.Infow("Periodic cleanup disabled (interval <= 0)")
		return
	}

	pt.cleanupWg.Add(1)
	go func() {
		defer pt.cleanupWg.Done()

		ticker := pt.tickerFactory(pt.cleanupInterval)
		defer ticker.Stop()

		pt.logger.Infow("Periodic proposal cleanup routine started", "interval", pt.cleanupInterval)

		for {
			select {
			case <-ticker.Chan():
				if pt.closed.Load() {
					pt.logger.Infow("Cleanup loop exiting due to tracker shutdown")
					return
				}
				pt.Cleanup()
			case <-pt.stopCleanup:
				pt.logger.Infow("Periodic proposal cleanup routine stopped.")
				return
			}
		}
	}()
}

// Track registers a new proposal with the tracker.
func (pt *proposalTracker) Track(proposal *types.PendingProposal) error {
	if proposal == nil {
		return fmt.Errorf("proposal cannot be nil")
	}
	if proposal.ID == "" {
		return fmt.Errorf("proposal ID cannot be empty")
	}
	if proposal.ResultCh == nil {
		// Clients are expected to provide a non-nil (preferably buffered) ResultCh
		return fmt.Errorf("proposal ResultCh cannot be nil")
	}

	pt.mu.Lock()
	defer pt.mu.Unlock()

	if pt.closed.Load() {
		return fmt.Errorf("proposal tracker is closed")
	}

	if _, exists := pt.proposals[proposal.ID]; exists {
		pt.logger.Warnw("Duplicate proposal ID detected",
			"id", proposal.ID, "op", proposal.Operation)
		return fmt.Errorf("proposal ID %q is already being tracked", proposal.ID)
	}

	now := pt.clock.Now()
	pt.proposals[proposal.ID] = &proposalEntry{
		PendingProposal: proposal,
		status:          proposalStatusPending,
		submittedAt:     now,
		lastAccess:      now,
	}
	pt.stats.TotalProposals++
	pt.stats.PendingProposals++

	pt.logger.Debugw("New proposal tracked",
		"id", proposal.ID,
		"op", proposal.Operation,
		"index", proposal.Index,
		"term", proposal.Term,
		"clientID", proposal.ClientID,
		"lockID", proposal.LockID)

	return nil
}

// HandleAppliedCommand is called when a command has been applied to the state machine.
func (pt *proposalTracker) HandleAppliedCommand(applyMsg types.ApplyMsg, commandResultData interface{}, commandError error) {
	proposalID := types.ProposalID(fmt.Sprintf("%d-%d", applyMsg.CommandTerm, applyMsg.CommandIndex))
	now := pt.clock.Now()

	entry, duration, exists := pt.finalizeProposalMetadata(proposalID, commandError, now)
	if !exists {
		pt.logger.Debugw("No pending proposal found for applied command",
			"id", proposalID,
			"cmd_idx", applyMsg.CommandIndex,
			"cmd_term", applyMsg.CommandTerm)
		return
	}

	if entry.Context != nil && entry.Context.Err() != nil {
		pt.handleCancelledProposal(entry, commandError, now, duration)
		return
	}

	pt.deliverAppliedResult(entry, commandResultData, commandError, now, duration)
}

// finalizeProposalMetadata looks up and removes a proposal from the tracker by ID,
// marks it as successfully or unsuccessfully applied based on the commandError,
// and updates internal metrics (including latency stats).
func (pt *proposalTracker) finalizeProposalMetadata(
	proposalID types.ProposalID,
	commandError error,
	now time.Time,
) (*proposalEntry, time.Duration, bool) {
	pt.mu.Lock()
	defer pt.mu.Unlock()

	entry, exists := pt.proposals[proposalID]
	if !exists {
		return nil, 0, false
	}

	delete(pt.proposals, proposalID)
	pt.stats.PendingProposals--

	entry.finalizedAt = now
	duration := now.Sub(entry.submittedAt)

	if commandError == nil {
		pt.stats.SuccessfulProposals++
		entry.status = proposalStatusAppliedSuccess
	} else {
		pt.stats.FailedProposals++
		entry.status = proposalStatusAppliedFailure
	}
	pt.updateLatencyStatsLocked(duration)

	return entry, duration, true
}

// handleCancelledProposal sends a failure result for a proposal whose client context
// was already cancelled or expired before the command was applied.
func (pt *proposalTracker) handleCancelledProposal(
	entry *proposalEntry,
	commandError error,
	now time.Time,
	duration time.Duration,
) {
	ctxErr := entry.Context.Err()

	pt.logger.Infow("Client context already done before apply result could be delivered",
		"id", entry.ID,
		"op", entry.Operation,
		"error", ctxErr)

	finalErr := ctxErr
	if finalErr == nil {
		finalErr = commandError
	}

	pt.sendResult(entry, types.ProposalResult{
		Success:   false,
		Error:     finalErr,
		AppliedAt: now,
		Duration:  duration,
	})
}

func (pt *proposalTracker) deliverAppliedResult(
	entry *proposalEntry,
	data interface{},
	err error,
	now time.Time,
	duration time.Duration,
) {
	result := types.ProposalResult{
		Success:   err == nil,
		Error:     err,
		Data:      data,
		AppliedAt: now,
		Duration:  duration,
	}

	pt.logger.Debugw("Sending result for applied proposal",
		"id", entry.ID,
		"op", entry.Operation,
		"success", result.Success,
		"error", result.Error,
		"duration", result.Duration)

	pt.sendResult(entry, result)
}

// HandleSnapshotApplied is invoked when a snapshot has been applied to the state machine.
// It invalidates any pending proposals whose log index is at or below the snapshot index,
// as those proposals are no longer guaranteed to be applied by Raft.
func (pt *proposalTracker) HandleSnapshotApplied(snapshotIndex types.Index, snapshotTerm types.Term) {
	now := pt.clock.Now()

	pt.mu.Lock()
	invalidated := pt.invalidateProposalsFromSnapshotUnlocked(snapshotIndex, now)
	pt.mu.Unlock()

	pt.sendSnapshotInvalidationResults(invalidated, snapshotIndex, snapshotTerm, now)
}

// invalidateProposalsFromSnapshotUnlocked removes and marks as invalid any proposal
// whose index is covered by the given snapshot. Must be called with pt.mu held.
func (pt *proposalTracker) invalidateProposalsFromSnapshotUnlocked(snapshotIndex types.Index, now time.Time) []*proposalEntry {
	var invalidated []*proposalEntry

	for id, entry := range pt.proposals {
		if entry.Index <= snapshotIndex {
			pt.logger.Warnw("Invalidating pending proposal due to snapshot",
				"proposalID", id,
				"op", entry.Operation,
				"proposalIndex", entry.Index,
				"proposalTerm", entry.Term)

			delete(pt.proposals, id)
			entry.status = proposalStatusSnapshotInvalidated
			entry.finalizedAt = now
			invalidated = append(invalidated, entry)

			pt.stats.PendingProposals--
			pt.stats.FailedProposals++
			pt.updateLatencyStatsLocked(now.Sub(entry.submittedAt))
		}
	}

	return invalidated
}

// sendSnapshotInvalidationResults sends failure results to clients for all proposals invalidated
// by the snapshot, indicating they can no longer be applied.
func (pt *proposalTracker) sendSnapshotInvalidationResults(
	proposals []*proposalEntry,
	snapshotIndex types.Index,
	snapshotTerm types.Term,
	now time.Time,
) {
	for _, entry := range proposals {
		result := types.ProposalResult{
			Success: false,
			Error: fmt.Errorf(
				"proposal at index %d (term %d) invalidated by snapshot at index %d (term %d)",
				entry.Index, entry.Term, snapshotIndex, snapshotTerm,
			),
			AppliedAt: now,
			Duration:  now.Sub(entry.submittedAt),
		}
		pt.sendResult(entry, result)
	}
}

// ClientCancel is called when the client's context is cancelled before the proposal is applied.
// If the proposal is still pending, it is removed from tracking and marked as cancelled.
// A failure result is sent to the client with the provided cancellation reason.
//
// Returns true if the proposal was actively pending and was cancelled; false otherwise.
func (pt *proposalTracker) ClientCancel(proposalID types.ProposalID, reason error) bool {
	var entry *proposalEntry
	var wasCancelled bool
	now := pt.clock.Now()

	pt.mu.Lock()
	defer pt.mu.Unlock()

	entry, exists := pt.proposals[proposalID]
	if !exists {
		return false
	}

	if entry.status != proposalStatusPending {
		return false
	}

	delete(pt.proposals, proposalID)
	entry.status = proposalStatusClientCancelled
	entry.finalizedAt = now
	pt.stats.PendingProposals--
	pt.stats.CancelledProposals++
	pt.updateLatencyStatsLocked(now.Sub(entry.submittedAt))
	wasCancelled = true

	pt.logger.Infow("Client cancelled pending proposal",
		"id", proposalID,
		"op", entry.Operation,
		"reason", reason)

	go pt.sendResult(entry, types.ProposalResult{
		Success:   false,
		Error:     reason,
		AppliedAt: now,
		Duration:  now.Sub(entry.submittedAt),
	})

	return wasCancelled
}

// GetPendingCount returns the number of proposals currently being tracked
// that have not yet been applied, cancelled, or expired.
func (pt *proposalTracker) GetPendingCount() int64 {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	return pt.stats.PendingProposals
}

// GetStats returns a snapshot of aggregated proposal tracking metrics.
// The returned ProposalStats is a copy and safe for concurrent use by the caller.
//
// AverageLatency is calculated only if at least one finalized proposal exists.
func (pt *proposalTracker) GetStats() types.ProposalStats {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	stats := pt.stats // return a copy

	totalFinalized := stats.SuccessfulProposals +
		stats.FailedProposals +
		stats.CancelledProposals +
		stats.ExpiredProposals

	if totalFinalized > 0 {
		stats.AverageLatency = time.Duration(
			pt.totalDuration.Nanoseconds() / totalFinalized,
		)
	} else {
		stats.AverageLatency = 0
	}

	return stats
}

// GetPendingProposal returns a copy of the pending proposal with the given ID, if it exists.
// If the proposal is not found or is no longer pending, it returns false.
func (pt *proposalTracker) GetPendingProposal(proposalID types.ProposalID) (types.PendingProposal, bool) {
	pt.mu.RLock()
	defer pt.mu.RUnlock()

	entry, exists := pt.proposals[proposalID]
	if !exists || entry.status != proposalStatusPending {
		return types.PendingProposal{}, false
	}

	// Return a shallow copy to prevent external mutation
	return *entry.PendingProposal, true
}

// Cleanup scans and removes expired or cancelled proposals from the tracker.
// It also notifies any listeners of the failure results.
// Returns the number of proposals removed during this cleanup cycle.
func (pt *proposalTracker) Cleanup() int {
	pt.mu.Lock()
	if pt.closed.Load() {
		pt.mu.Unlock()
		return 0
	}

	now := pt.clock.Now()
	expired := pt.collectExpiredProposals(now)

	pt.mu.Unlock() // Unlock before sending results to avoid holding lock during callbacks
	pt.finalizeExpiredProposals(expired, now)

	if len(expired) > 0 {
		pt.logger.Infow("Cleaned up expired or cancelled proposals", "count", len(expired))
	}

	return len(expired)
}

// collectExpiredProposals identifies and removes expired proposals from the tracker.
func (pt *proposalTracker) collectExpiredProposals(now time.Time) []*proposalEntry {
	var expired []*proposalEntry

	for id, entry := range pt.proposals {
		if reason, shouldExpire := pt.shouldExpireProposal(entry, now); shouldExpire {
			pt.logger.Warnw("Proposal expired during cleanup",
				"id", entry.ID,
				"op", entry.Operation,
				"age", now.Sub(entry.submittedAt),
				"reason", reason)

			delete(pt.proposals, id)
			entry.status = proposalStatusExpired
			entry.finalizedAt = now
			expired = append(expired, entry)

			pt.stats.PendingProposals--
			pt.stats.ExpiredProposals++
			pt.updateLatencyStatsLocked(now.Sub(entry.submittedAt))
		}
	}
	return expired
}

// shouldExpireProposal determines whether a given proposal should be considered expired.
func (pt *proposalTracker) shouldExpireProposal(entry *proposalEntry, now time.Time) (error, bool) {
	if entry.Context != nil && entry.Context.Err() != nil {
		return entry.Context.Err(), true
	}
	if pt.maxPendingAge > 0 && now.Sub(entry.submittedAt) > pt.maxPendingAge {
		return fmt.Errorf("proposal exceeded max pending age of %v", pt.maxPendingAge), true
	}
	return nil, false
}

// finalizeExpiredProposals sends failure results for proposals that were expired during cleanup.
func (pt *proposalTracker) finalizeExpiredProposals(entries []*proposalEntry, now time.Time) {
	for _, entry := range entries {
		err := entry.Context.Err()
		if err == nil {
			err = fmt.Errorf("proposal expired after %v (max pending age %v)",
				entry.finalizedAt.Sub(entry.submittedAt), pt.maxPendingAge)
		}
		result := types.ProposalResult{
			Success:   false,
			Error:     err,
			AppliedAt: entry.finalizedAt,
			Duration:  entry.finalizedAt.Sub(entry.submittedAt),
		}
		pt.sendResult(entry, result)
	}
}

// updateLatencyStatsLocked updates internal latency metrics using the given proposal duration.
func (pt *proposalTracker) updateLatencyStatsLocked(duration time.Duration) {
	pt.totalDuration += duration
	if duration > pt.stats.MaxLatency {
		pt.stats.MaxLatency = duration
	}
}

// sendResult attempts to deliver the final result of a proposal to the client via its ResultCh.
// It uses a non-blocking send to prevent stalling the Raft apply loop or cleanup routines.
func (pt *proposalTracker) sendResult(entry *proposalEntry, result types.ProposalResult) {
	defer func() {
		if r := recover(); r != nil {
			pt.logger.Warnw("Panic recovered while sending proposal result (likely channel closed)",
				"id", entry.ID, "op", entry.Operation, "panic", r)
		}
	}()

	select {
	case entry.ResultCh <- result: // Attempts to send the result
		pt.logger.Debugw("Successfully sent proposal result",
			"id", entry.ID, "op", entry.Operation, "success", result.Success)
	case <-entry.Context.Done(): // Handles client context cancellation
		pt.logger.Infow("Client context done while attempting to send result",
			"id", entry.ID, "op", entry.Operation, "contextError", entry.Context.Err())
	default: // This case is hit if ResultCh is unbuffered and receiver isn't ready,
		// or if ResultCh is buffered but full.
		pt.logger.Warnw("Could not send result to proposal channel (full or no listener)",
			"id", entry.ID, "op", entry.Operation)
	}
}

// Close gracefully shuts down the proposal tracker.
// It stops the periodic cleanup routine, finalizes all unresolved proposals with failure,
// updates internal metrics, and ensures all resources are released.
// Returns nil on success, or silently ignores if already closed.
func (pt *proposalTracker) Close() error {
	if !pt.closed.CompareAndSwap(false, true) {
		pt.logger.Infow("Proposal tracker already closed or closing.")
		return nil
	}

	pt.logger.Infow("Closing proposal tracker...")

	close(pt.stopCleanup)
	pt.cleanupWg.Wait()

	pt.mu.Lock()
	now := pt.clock.Now()
	pending := make([]*proposalEntry, 0, len(pt.proposals))

	for _, entry := range pt.proposals {
		pending = append(pending, entry)
	}
	pt.proposals = make(map[types.ProposalID]*proposalEntry)

	pt.stats.PendingProposals = 0
	pt.stats.FailedProposals += int64(len(pending))
	pt.mu.Unlock()

	go pt.failProposalsDueToShutdown(pending, now)

	pt.logger.Infow("Proposal tracker closed", "failedPendingOnClose", len(pending))
	return nil
}

// failProposalsDueToShutdown marks all pending proposals as failed due to tracker shutdown
// and sends a failure result to each client. Called asynchronously during Close.
func (pt *proposalTracker) failProposalsDueToShutdown(proposals []*proposalEntry, shutdownTime time.Time) {
	for _, entry := range proposals {
		duration := shutdownTime.Sub(entry.submittedAt)

		pt.mu.Lock()
		pt.updateLatencyStatsLocked(duration)
		pt.mu.Unlock()

		result := types.ProposalResult{
			Success:   false,
			Error:     fmt.Errorf("proposal tracker shutdown; proposal %q (%s) invalidated", entry.ID, entry.Operation),
			AppliedAt: shutdownTime,
			Duration:  duration,
		}
		pt.sendResult(entry, result)
	}
}
