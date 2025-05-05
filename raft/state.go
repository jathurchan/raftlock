package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// StateManager defines the interface for managing Raft node state.
// It handles term changes, role transitions, vote tracking, and log index updates.
type StateManager interface {
	// Initialize loads the persisted state (term, votedFor) from storage.
	// If no state exists or it's corrupted, it initializes default state.
	Initialize(ctx context.Context) error

	// GetState returns the current term, role, and leader ID.
	// Safe for concurrent use.
	GetState() (types.Term, types.NodeRole, types.NodeID)

	// GetStateUnsafe returns the current term, role, and leader ID without synchronization.
	// Intended for internal use where the caller can guarantee exclusive access or immutability.
	// Not safe for concurrent use.
	GetStateUnsafe() (types.Term, types.NodeRole, types.NodeID)

	// GetLastKnownLeader returns the most recently known leader ID.
	// Useful for client request forwarding even when current leader is temporarily unknown.
	// Safe for concurrent use.
	GetLastKnownLeader() types.NodeID

	// BecomeCandidate transitions the node to candidate state.
	// Increments the term, votes for self, and persists the state.
	// Returns true if the transition was successful (including persistence).
	BecomeCandidate(ctx context.Context, reason ElectionReason) bool

	// BecomeLeader transitions the node to leader state after winning an election.
	// Persists the current state (which should reflect the election term and self-vote).
	// Returns true if the transition was successful.
	BecomeLeader(ctx context.Context) bool

	// CheckTermAndStepDown compares the given RPC term with the local term.
	// If the RPC term is higher, it updates the local term, resets votedFor,
	// becomes a follower, persists the state, and returns true.
	// If the terms are equal but the node is not a follower or the RPC provides a
	// different leader hint, it becomes a follower (updating leader hint) and returns true.
	// Otherwise, returns false. Returns the node's term before any potential update.
	BecomeFollower(ctx context.Context, term types.Term, leaderID types.NodeID)

	// CheckTermAndStepDown compares the given term with the local term.
	// If the given term is higher, steps down and updates state.
	// Returns whether a step-down occurred and the previous term.
	CheckTermAndStepDown(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (steppedDown bool, previousTerm types.Term)

	// GrantVote attempts to grant a vote to the given candidate for the specified term,
	// checking term validity and if the node has already voted in this term.
	// Persists the vote if granted. Returns true if the vote was granted and persisted.
	GrantVote(ctx context.Context, candidateID types.NodeID, term types.Term) bool

	// UpdateCommitIndex sets the commit index if the new index is higher.
	// Returns true if the index was updated.
	// Safe for concurrent use.
	UpdateCommitIndex(newCommitIndex types.Index) bool

	// UpdateCommitIndex sets the commit index if the new index is higher.
	// Returns true if the index was updated.
	// The caller must ensure thread safety.
	UpdateCommitIndexUnsafe(newCommitIndex types.Index) bool

	// UpdateLastApplied sets the last applied index if the new index is higher.
	// Returns true if the index was updated. Safe for concurrent use.
	UpdateLastApplied(newLastApplied types.Index) bool

	// GetCommitIndex returns the current commit index.
	// Safe for concurrent use.
	GetCommitIndex() types.Index

	// GetCommitIndexUnsafe returns the current commit index without synchronization.
	// The caller must ensure thread safety.
	GetCommitIndexUnsafe() types.Index

	// GetLastApplied returns the index of the last applied log entry. Safe for concurrent use.
	GetLastApplied() types.Index

	// Stop signals the state manager to stop processing and releases resources.
	Stop()
}

// stateManager implements the StateManager interface.
// It manages the persistent Raft state (current term, votedFor),
// handles role transitions, and coordinates related metadata updates
// like commit/applied indices.
// It ensures that state changes are persisted before they are acted upon.
type stateManager struct {
	mu         *sync.RWMutex // Raft's mutex protecting state fields
	isShutdown *atomic.Bool  // Shared flag indicating Raft shutdown

	id             types.NodeID        // ID of the local Raft node.
	leaderChangeCh chan<- types.NodeID // Channel to notify leader changes

	logger  logger.Logger
	metrics Metrics
	storage storage.Storage

	currentTerm types.Term   // Latest term server has seen
	votedFor    types.NodeID // CandidateID that received vote in current term

	role              types.NodeRole // Current role (Follower, Candidate, or Leader)
	leaderID          types.NodeID   // Node ID of the current known leader, "" if unknown.
	lastKnownLeaderID types.NodeID   // Stores the ID of the last known leader, persists even if leader becomes unknown.

	commitIndex types.Index // Index of highest log entry known to be committed
	lastApplied types.Index // Index of highest log entry applied (applier)
}

// NewStateManager creates and initializes a new state manager.
func NewStateManager(mu *sync.RWMutex, shutdownFlag *atomic.Bool, deps Dependencies,
	nodeID types.NodeID, leaderChangeCh chan<- types.NodeID) StateManager {
	if mu == nil || shutdownFlag == nil || deps.Storage == nil || deps.Metrics == nil || deps.Logger == nil || nodeID == "" || leaderChangeCh == nil {
		panic("raft: invalid arguments provided to NewStateManager")
	}
	return &stateManager{
		mu:             mu,
		isShutdown:     shutdownFlag,
		storage:        deps.Storage,
		metrics:        deps.Metrics,
		logger:         deps.Logger.WithComponent("state"),
		id:             nodeID,
		leaderChangeCh: leaderChangeCh,
		role:           types.RoleFollower,
		commitIndex:    0,
		lastApplied:    0,
	}
}

// Initialize loads the persisted state (term, votedFor) from storage during Raft startup.
// If the persisted state is corrupted, it resets to a default follower state and persists that.
func (sm *stateManager) Initialize(ctx context.Context) error {
	sm.logger.Infow("Initializing state manager...")

	state, err := sm.storage.LoadState(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrCorruptedState) {
			sm.logger.Warnw("Persistent state corrupted or missing, initializing default state", "error", err)
			return sm.initializeDefaultState(ctx)
		}

		sm.logger.Errorw("Failed to load persistent state", "error", err)
		return fmt.Errorf("failed to load persistent state: %w", err)
	}

	sm.mu.Lock()
	sm.applyPersistentStateLocked(state.CurrentTerm, state.VotedFor)
	sm.role = types.RoleFollower
	sm.leaderID = unknownNodeID
	currentTerm := sm.currentTerm
	votedFor := sm.votedFor
	sm.mu.Unlock()

	sm.logger.Infow("Loaded persistent state from storage",
		"term", currentTerm,
		"voted_for", votedFor)

	sm.metrics.SetGauge("raft_term", float64(state.CurrentTerm))
	return nil
}

// initializeDefaultState sets the in-memory state to term 0, no vote, follower role,
// and persists this initial state. Called when loading fails or state is corrupted.
func (sm *stateManager) initializeDefaultState(ctx context.Context) error {
	initialTerm := types.Term(0)
	initialVotedFor := unknownNodeID

	sm.mu.Lock()
	sm.applyPersistentStateLocked(initialTerm, initialVotedFor)
	sm.role = types.RoleFollower
	sm.leaderID = unknownNodeID
	sm.mu.Unlock()

	if err := sm.persistState(ctx, initialTerm, initialVotedFor); err != nil {
		sm.logger.Errorw("Failed to persist initial default state", "error", err)
		return fmt.Errorf("failed to persist initial default state: %w", err)
	}

	sm.logger.Infow("Initialized and persisted default state",
		"term", initialTerm, "voted_for", initialVotedFor)

	sm.metrics.SetGauge("raft_term", float64(initialTerm))
	return nil
}

// GetState returns the current term, role, and leader ID using a read lock.
// This method is safe for concurrent use.
func (sm *stateManager) GetState() (types.Term, types.NodeRole, types.NodeID) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.GetStateUnsafe()
}

// GetStateUnsafe returns the current term, role, and leader ID without acquiring a lock.
// This method is not safe for concurrent use and should only be called when external synchronization is guaranteed.
func (sm *stateManager) GetStateUnsafe() (types.Term, types.NodeRole, types.NodeID) {
	return sm.currentTerm, sm.role, sm.leaderID
}

// GetLastKnownLeader returns the most recently known leader ID using a read lock.
// This might differ from `leaderID` if leadership is currently unknown but was known previously.
func (sm *stateManager) GetLastKnownLeader() types.NodeID {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastKnownLeaderID
}

// BecomeCandidate attempts to transition the node from follower to candidate state.
// It increments the term, votes for itself, and persists the new state *before*
// signalling success. Returns true only if the transition and persistence succeed.
func (sm *stateManager) BecomeCandidate(ctx context.Context, reason ElectionReason) bool {
	if sm.isShutdown.Load() {
		sm.logger.Warnw("Attempted BecomeCandidate on shutdown node")
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.role.CanTransitionTo(types.RoleCandidate) {
		sm.logger.Warnw("Cannot become candidate: invalid role transition",
			"current_role", sm.role.String(),
			"current_term", sm.currentTerm)
		return false
	}

	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor
	previousRole := sm.role
	previousLeader := sm.leaderID

	sm.startElectionLocked()
	newTerm := sm.currentTerm

	if err := sm.persistState(ctx, newTerm, sm.id); err != nil {
		sm.logger.Errorw("Failed to persist state when becoming candidate, rolling back",
			"new_term", newTerm,
			"error", err)

		sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
		sm.setRoleAndLeaderLocked(previousRole, previousLeader, previousTerm)

		return false
	}

	sm.metrics.SetGauge("raft_term", float64(newTerm))
	sm.metrics.ObserveElectionStart(newTerm, reason)

	sm.logger.Infow("Transitioned to Candidate", "new_term", newTerm)
	return true
}

// BecomeLeader transitions the node to leader state, typically after winning an election.
// Assumes the node is currently a Candidate in the correct term.
func (sm *stateManager) BecomeLeader(ctx context.Context) bool {
	if sm.isShutdown.Load() {
		sm.logger.Warnw("Attempted BecomeLeader on shutdown node")
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if !sm.role.CanTransitionTo(types.RoleLeader) {
		sm.logger.Warnw("Cannot become leader: invalid role transition",
			"current_role", sm.role.String(),
			"current_term", sm.currentTerm)
		return false
	}

	currentTerm := sm.currentTerm
	leaderID := sm.id // Leader is self

	sm.setRoleAndLeaderLocked(types.RoleLeader, leaderID, currentTerm)

	sm.logger.Infow("Transitioned to Leader", "term", currentTerm, "leader", leaderID)
	return true
}

// BecomeFollower transitions the node into Follower state for the specified term and leader.
// If the provided term is greater than the current term, it updates the term and resets votedFor,
// persisting the new state.
func (sm *stateManager) BecomeFollower(ctx context.Context, term types.Term, leaderID types.NodeID) {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Attempted BecomeFollower on shutdown node")
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if term < sm.currentTerm {
		sm.logger.Errorw("BecomeFollower called with lower term than current",
			"current_term", sm.currentTerm,
			"received_term", term,
			"current_role", sm.role.String())
		return
	}

	if sm.role == types.RoleFollower && sm.currentTerm == term && sm.leaderID == leaderID {
		return
	}

	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor

	termChanged := false
	if term > sm.currentTerm {
		sm.logger.Infow("Becoming follower due to higher term",
			"current_term", previousTerm, "new_term", term, "new_leader", leaderID)
		sm.currentTerm = term
		sm.votedFor = unknownNodeID
		termChanged = true

		if err := sm.persistState(ctx, sm.currentTerm, sm.votedFor); err != nil {
			sm.logger.Errorw("Failed to persist state change in BecomeFollower, rolling back term/vote",
				"new_term", sm.currentTerm, "error", err)
			sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
			sm.setRoleAndLeaderLocked(types.RoleFollower, leaderID, previousTerm)
			return
		}
		sm.metrics.SetGauge("raft_term", float64(sm.currentTerm))
	}

	sm.setRoleAndLeaderLocked(types.RoleFollower, leaderID, sm.currentTerm)

	if !termChanged {
		sm.logger.Infow("Becoming follower in same term",
			"term", sm.currentTerm, "new_leader", leaderID, "previous_role", sm.role.String())
	}
}

// CheckTermAndStepDown compares an RPC term with the local term and potentially steps down.
// Returns true if the node stepped down (became Follower), false otherwise.
// Also returns the node's term before any potential change.
func (sm *stateManager) CheckTermAndStepDown(ctx context.Context, rpcTerm types.Term, rpcLeader types.NodeID) (steppedDown bool, previousTerm types.Term) {
	if sm.isShutdown.Load() {
		sm.mu.RLock()
		prevTerm := sm.currentTerm
		sm.mu.RUnlock()
		sm.logger.Debugw("CheckTermAndStepDown called on shutdown node")
		return false, prevTerm
	}

	sm.mu.Lock()

	previousTerm = sm.currentTerm
	currentRole := sm.role
	currentLeader := sm.leaderID

	if rpcTerm > sm.currentTerm {
		sm.logger.Infow("Higher term received via RPC, stepping down",
			"current_term", sm.currentTerm,
			"rpc_term", rpcTerm,
			"rpc_leader_hint", rpcLeader,
			"current_role", currentRole.String(),
		)

		if err := sm.stepDownToHigherTermLocked(ctx, rpcTerm, rpcLeader); err != nil {
			sm.logger.Errorw("Persistence failed during step down", "error", err)
		}
		sm.mu.Unlock()
		return true, previousTerm
	}

	if rpcTerm == sm.currentTerm {
		if currentRole != types.RoleFollower || (rpcLeader != unknownNodeID && rpcLeader != currentLeader) {
			if currentRole != types.RoleFollower {
				sm.logger.Infow("Same term RPC received; stepping down from non-follower role",
					"term", sm.currentTerm,
					"current_role", currentRole.String(),
					"rpc_leader_hint", rpcLeader,
				)
			} else {
				sm.logger.Infow("Same term RPC with new leader hint; updating follower's known leader",
					"term", sm.currentTerm,
					"old_leader", currentLeader,
					"new_leader_hint", rpcLeader,
				)
			}
			sm.setRoleAndLeaderLocked(types.RoleFollower, rpcLeader, sm.currentTerm)
			sm.mu.Unlock()
			return true, previousTerm
		}
	}

	sm.mu.Unlock()
	return false, previousTerm
}

// stepDownToHigherTermLocked updates term, resets vote, becomes follower, and persists state.
// Must be called with sm.mu.Lock() held. Returns error from persistence.
func (sm *stateManager) stepDownToHigherTermLocked(ctx context.Context, newTerm types.Term, leaderHint types.NodeID) error {
	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor

	sm.applyPersistentStateLocked(newTerm, unknownNodeID)

	if err := sm.persistState(ctx, newTerm, unknownNodeID); err != nil {
		sm.logger.Errorw("Failed to persist state after stepping down to higher term",
			"new_term", newTerm,
			"error", err,
		)

		sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
		sm.setRoleAndLeaderLocked(types.RoleFollower, leaderHint, previousTerm)

		return err
	}

	sm.setRoleAndLeaderLocked(types.RoleFollower, leaderHint, newTerm)
	sm.metrics.SetGauge("raft_term", float64(newTerm))
	return nil
}

// GrantVote attempts to grant a vote for the given candidate in the specified term.
// Checks term validity and ensures only one vote is cast per term. Persists the vote if granted.
func (sm *stateManager) GrantVote(ctx context.Context, candidateID types.NodeID, term types.Term) bool {
	if sm.isShutdown.Load() {
		sm.logger.Infow("Vote rejected: node is shut down",
			"requested_term", term,
			"candidate_id", candidateID)
		return false
	}

	sm.mu.Lock()

	if term < sm.currentTerm {
		sm.logger.Debugw("Vote rejected: candidate term lower than current term",
			"current_term", sm.currentTerm,
			"candidate_term", term,
			"candidate_id", candidateID)
		sm.mu.Unlock()
		return false
	}

	canVote := sm.votedFor == unknownNodeID || sm.votedFor == candidateID
	if !canVote {
		sm.logger.Infow("Vote rejected: already voted for another candidate in this term",
			"current_term", sm.currentTerm,
			"voted_for", sm.votedFor,
			"candidate_id", candidateID)
		sm.mu.Unlock()
		return false
	}

	previousVotedFor := sm.votedFor
	currentTerm := sm.currentTerm

	sm.votedFor = candidateID

	sm.mu.Unlock()

	if err := sm.persistState(ctx, currentTerm, candidateID); err != nil {
		sm.logger.Errorw("Failed to persist vote, rolling back in-memory vote",
			"term", currentTerm,
			"candidate_id", candidateID,
			"error", err)
		sm.mu.Lock()

		if sm.currentTerm == currentTerm && sm.votedFor == candidateID {
			sm.votedFor = previousVotedFor
		}
		sm.mu.Unlock()
		return false
	}

	sm.logger.Infow("Vote granted",
		"term", currentTerm,
		"candidate_id", candidateID)
	sm.metrics.ObserveVoteGranted(currentTerm)
	return true
}

// UpdateCommitIndex updates the commit index if the new index is higher.
// Safe for concurrent use.
func (sm *stateManager) UpdateCommitIndex(newCommitIndex types.Index) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.UpdateCommitIndexUnsafe(newCommitIndex)
}

// UpdateCommitIndex updates the commit index if the new index is higher.
// Assumes holds sm.mu.Lock()
func (sm *stateManager) UpdateCommitIndexUnsafe(newCommitIndex types.Index) bool {
	if newCommitIndex <= sm.commitIndex {
		return false
	}

	prev := sm.commitIndex
	sm.commitIndex = newCommitIndex

	sm.logger.Debugw("Commit index updated",
		"from", prev,
		"to", newCommitIndex,
		"term", sm.currentTerm,
		"role", sm.role.String())

	sm.metrics.ObserveCommitIndex(newCommitIndex)
	return true
}

// UpdateLastApplied updates the last applied index if the new index is higher.
// Safe for concurrent use.
func (sm *stateManager) UpdateLastApplied(newLastApplied types.Index) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if newLastApplied > sm.commitIndex {
		sm.logger.Errorw("Attempted to set lastApplied beyond commitIndex",
			"last_applied", newLastApplied,
			"commit_index", sm.commitIndex,
			"term", sm.currentTerm,
			"role", sm.role.String())
		return false
	}

	if newLastApplied <= sm.lastApplied {
		return false
	}

	prev := sm.lastApplied
	sm.lastApplied = newLastApplied

	sm.logger.Debugw("Last applied index updated",
		"from", prev,
		"to", newLastApplied,
		"term", sm.currentTerm,
		"role", sm.role.String())

	sm.metrics.ObserveAppliedIndex(newLastApplied)
	return true
}

// GetCommitIndex returns the current commit index using a read lock.
func (sm *stateManager) GetCommitIndex() types.Index {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.commitIndex
}

// GetCommitIndexUnsafe returns the current commit index without acquiring a lock.
// Assumes the caller holds sm.mu.RLock()
func (sm *stateManager) GetCommitIndexUnsafe() types.Index {
	return sm.commitIndex
}

// GetLastApplied returns the index of the last applied log entry using a read lock.
func (sm *stateManager) GetLastApplied() types.Index {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastApplied
}

// Stop sets the shutdown flag atomically.
func (sm *stateManager) Stop() {
	if sm.isShutdown.CompareAndSwap(false, true) {
		sm.logger.Infow("StateManager received stop signal")
	} else {
		sm.logger.Debugw("StateManager stop signal received multiple times")
	}
}

// applyPersistentStateLocked updates only the persistent fields (term, votedFor) in memory.
// Must be called with sm.mu.Lock() held.
func (sm *stateManager) applyPersistentStateLocked(term types.Term, votedFor types.NodeID) {
	sm.currentTerm = term
	sm.votedFor = votedFor
}

// startElectionLocked increments the term and votes for self.
// Must be called with sm.mu.Lock() held.
func (sm *stateManager) startElectionLocked() {
	newTerm := sm.currentTerm + 1
	sm.applyPersistentStateLocked(newTerm, sm.id)
	sm.setRoleAndLeaderLocked(types.RoleCandidate, unknownNodeID, newTerm)
}

// setRoleAndLeaderLocked updates the role and leaderID, logging the change and notifying listeners.
// Must be called with sm.mu.Lock() held.
func (sm *stateManager) setRoleAndLeaderLocked(newRole types.NodeRole, newLeader types.NodeID, term types.Term) {
	oldRole := sm.role
	oldLeader := sm.leaderID

	if oldRole == newRole && oldLeader == newLeader {
		return
	}

	sm.role = newRole
	sm.leaderID = newLeader

	if newLeader != unknownNodeID {
		sm.lastKnownLeaderID = newLeader
	}

	sm.logger.Infow("Role changed",
		"new_role", newRole.String(),
		"term", term,
		"leader", newLeader,
		"previous_role", oldRole.String(),
		"previous_leader", oldLeader,
	)

	sm.metrics.ObserveRoleChange(newRole, oldRole, term)

	if oldLeader != newLeader {
		sm.notifyLeaderChangeNonBlocking(newLeader, term)
	}
}

// persistState saves the given term and votedFor to stable storage.
func (sm *stateManager) persistState(ctx context.Context, term types.Term, votedFor types.NodeID) error {
	state := types.PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	sm.logger.Debugw("Persisting state", "term", term, "voted_for", votedFor)
	err := sm.storage.SaveState(ctx, state)
	if err != nil {
		sm.logger.Errorw("Failed to persist state",
			"term", state.CurrentTerm,
			"votedFor", state.VotedFor,
			"error", err)

		return fmt.Errorf("failed to save state (term: %d, votedFor: '%s'): %w", state.CurrentTerm, state.VotedFor, err)
	}
	sm.logger.Debugw("State persisted successfully", "term", term, "voted_for", votedFor)
	return nil
}

// notifyLeaderChangeNonBlocking sends leader updates to the leader change channel without blocking.
// Logs a warning if the channel is full (indicates the receiver might be stuck).
func (sm *stateManager) notifyLeaderChangeNonBlocking(newLeader types.NodeID, term types.Term) {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Skipped notifying leader change due to shutdown",
			"new_leader", newLeader,
			"term", term)
		return
	}

	sm.metrics.ObserveLeaderChange(newLeader, term)

	select {
	case sm.leaderChangeCh <- newLeader:
		sm.logger.Infow("Notified leader change",
			"new_leader", newLeader,
			"term", term)
	default:
		sm.logger.Warnw("Leader change notification channel full, notification dropped",
			"new_leader", newLeader,
			"term", term)

		sm.metrics.ObserveLeaderNotificationDropped()
	}
}
