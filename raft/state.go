package raft

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/types"
)

// StateSnapshot provides atomic access to all state at a point in time
type StateSnapshot struct {
	Term            types.Term     // Current term
	Role            types.NodeRole // Current role
	LeaderID        types.NodeID   // Current leader
	CommitIndex     types.Index    // Highest committed log index
	LastApplied     types.Index    // Highest applied log index
	VotedFor        types.NodeID   // Who we voted for in current term
	LastKnownLeader types.NodeID   // Last known leader (persists when leader unknown)
}

// StateManager manages the state of a Raft node, including persistent fields
// (currentTerm, votedFor), volatile state (role, leader), and log indices
// (commitIndex, lastApplied). All persistent changes are atomically stored before
// taking effect; failures trigger rollback to ensure consistency.
//
// Thread Safety:
// - Methods without the "Unsafe" suffix are safe for concurrent use.
// - "Unsafe" methods require external synchronization but offer better performance.
type StateManager interface {
	// Initialize loads persisted state or defaults (term=0, votedFor=unknown, role=Follower).
	// Must be called before any other method.
	// - Loads from storage with timeout/retries
	// - Validates and initializes in-memory structures and metrics
	Initialize(ctx context.Context) error

	// Stop performs a graceful shutdown:
	// - Prevents new operations
	// - Waits for in-flight operations
	// - Flushes state to disk and releases resources
	// Safe to call multiple times.
	Stop()

	// GetState returns the current term, role, and leader ID atomically.
	// Thread-safe.
	GetState() (types.Term, types.NodeRole, types.NodeID)

	// GetStateUnsafe returns term, role, and leader without synchronization.
	// Caller must hold a read or write lock.
	GetStateUnsafe() (types.Term, types.NodeRole, types.NodeID)

	// GetLastKnownLeader returns the last observed leader ID.
	// Useful even when no current leader is known.
	// Thread-safe.
	GetLastKnownLeader() types.NodeID

	// BecomeCandidate increments term, votes for self, clears leader,
	// transitions to Candidate, and persists changes atomically.
	// Returns true on success, false if preconditions or persistence fail.
	BecomeCandidate(ctx context.Context, reason ElectionReason) bool

	// BecomeCandidateForTerm transitions to Candidate for a specific term,
	// used for pre-vote flows. Persists term and vote.
	BecomeCandidateForTerm(ctx context.Context, term types.Term) bool

	// BecomeLeaderUnsafe transitions from Candidate to Leader.
	// Updates leader ID, metrics, and triggers notifications.
	// Does not persist state (term already persisted by candidacy).
	BecomeLeaderUnsafe(ctx context.Context) bool

	// BecomeFollower transitions to Follower with given term and leader.
	// If term is higher, updates and persists new term; otherwise, just updates state.
	// Handles rollback on failure.
	BecomeFollower(ctx context.Context, term types.Term, leaderID types.NodeID)

	// CheckTermAndStepDown compares the given term with local state.
	// If higher, steps down to Follower, persists new term, and returns true.
	// Otherwise, returns false.
	CheckTermAndStepDown(
		ctx context.Context,
		rpcTerm types.Term,
		rpcLeader types.NodeID,
	) (steppedDown bool, previousTerm types.Term)

	// GrantVote attempts to grant a vote to the candidate.
	// Applies Raft voting rules and persists changes if vote is granted.
	// Returns true on success and persistence; false otherwise.
	GrantVote(ctx context.Context, candidateID types.NodeID, term types.Term) bool

	// UpdateCommitIndex sets a new commit index if it's higher than the current.
	// Thread-safe.
	UpdateCommitIndex(newCommitIndex types.Index) bool

	// UpdateCommitIndexUnsafe is an unsynchronized version of UpdateCommitIndex.
	// Caller must hold a write lock.
	UpdateCommitIndexUnsafe(newCommitIndex types.Index) bool

	// UpdateLastApplied sets a new last applied index if:
	// - it's greater than the current
	// - and ≤ commitIndex
	// Logs an error if out of range. Thread-safe.
	UpdateLastApplied(newLastApplied types.Index) bool

	// GetCommitIndex returns the current commit index.
	// Thread-safe.
	GetCommitIndex() types.Index

	// GetCommitIndexUnsafe returns the commit index without locking.
	// Caller must hold a lock.
	GetCommitIndexUnsafe() types.Index

	// GetLastApplied returns the last applied index.
	// Thread-safe.
	GetLastApplied() types.Index

	// GetLastAppliedUnsafe returns the last applied index without locking.
	// Caller must hold a lock.
	GetLastAppliedUnsafe() types.Index

	// GetLeaderInfo returns the current leader and last known leader IDs,
	// along with a boolean indicating if a current leader is known.
	// Thread-safe.
	GetLeaderInfo() (currentLeader, lastKnownLeader types.NodeID, hasLeader bool)

	// GetLeaderInfoUnsafe returns current and last known leader IDs without locking.
	// Caller must hold a read or write lock.
	GetLeaderInfoUnsafe() (currentLeader, lastKnownLeader types.NodeID, hasLeader bool)

	// GetVotedFor returns who this node voted for in the current term.
	// Safe for concurrent use.
	GetVotedFor() types.NodeID

	// GetVotedForUnsafe returns who this node voted for without locking.
	// Caller must hold a lock.
	GetVotedForUnsafe() types.NodeID

	// GetTimeSinceLastRoleChange returns the duration since the last role transition.
	// Useful for election timeout handling and monitoring.
	GetTimeSinceLastRoleChange() time.Duration
}

// stateManager implements the StateManager interface.
// It manages the persistent Raft state (current term, votedFor),
// handles role transitions, and coordinates related metadata updates
// like commit/applied indices.
// It ensures that state changes are persisted before they are acted upon.
type stateManager struct {
	mu         *sync.RWMutex // Shared Raft mutex
	isShutdown *atomic.Bool  // Shared shutdown flag

	id             types.NodeID      // Local node ID
	leaderChangeCh chan types.NodeID // Channel for leader change notifications

	logger  logger.Logger
	metrics Metrics
	storage storage.Storage
	clock   Clock

	currentTerm types.Term   // Latest term server has seen (persisted)
	votedFor    types.NodeID // CandidateID that received vote in current term (persisted)

	role              types.NodeRole // Current role (Follower, Candidate, Leader)
	leaderID          types.NodeID   // Current known leader
	lastKnownLeaderID types.NodeID   // Last known leader (persists even when leader unknown)

	commitIndex types.Index // Index of highest log entry known to be committed
	lastApplied types.Index // Index of highest log entry applied to state machine

	lastRoleChange  time.Time     // Timestamp of last role change
	roleChangeCount atomic.Uint64 // Count of role changes for debugging
	termChangeCount atomic.Uint64 // Count of term changes for debugging

	persistenceInProgress atomic.Bool   // Flag to prevent concurrent persistence operations
	lastPersistAttempt    time.Time     // Timestamp of last persistence attempt
	persistFailureCount   atomic.Uint64 // Count of persistence failures
}

// StateManagerDeps contains all dependencies required for state manager
type StateManagerDeps struct {
	ID             types.NodeID
	Mu             *sync.RWMutex
	IsShutdown     *atomic.Bool
	LeaderChangeCh chan types.NodeID
	Logger         logger.Logger
	Metrics        Metrics
	Clock          Clock
	Storage        storage.Storage
}

// NewStateManager creates a new state manager with improved error handling
func NewStateManager(deps StateManagerDeps) (StateManager, error) {
	if err := validateStateManagerDeps(deps); err != nil {
		return nil, fmt.Errorf("invalid state manager dependencies: %w", err)
	}

	sm := &stateManager{
		mu:             deps.Mu,
		isShutdown:     deps.IsShutdown,
		id:             deps.ID,
		leaderChangeCh: deps.LeaderChangeCh,
		logger:         deps.Logger.WithComponent("state"),
		metrics:        deps.Metrics,
		storage:        deps.Storage,
		clock:          deps.Clock,

		// Initialize to unknown/default values
		currentTerm:       0,
		votedFor:          unknownNodeID,
		role:              types.RoleFollower,
		leaderID:          unknownNodeID,
		lastKnownLeaderID: unknownNodeID,
		commitIndex:       0,
		lastApplied:       0,
	}

	sm.lastRoleChange = sm.clock.Now()

	sm.logger.Infow("State manager created",
		"nodeID", sm.id,
		"initialRole", sm.role.String())

	return sm, nil
}

// validateStateManagerDeps ensures all required dependencies are provided
func validateStateManagerDeps(deps StateManagerDeps) error {
	if deps.ID == unknownNodeID {
		return errors.New("node ID must be provided")
	}
	if deps.Mu == nil {
		return errors.New("mutex must not be nil")
	}
	if deps.IsShutdown == nil {
		return errors.New("shutdown flag must not be nil")
	}
	if deps.LeaderChangeCh == nil {
		return errors.New("leader change channel must not be nil")
	}
	if deps.Logger == nil {
		return errors.New("logger must not be nil")
	}
	if deps.Metrics == nil {
		return errors.New("metrics must not be nil")
	}
	if deps.Storage == nil {
		return errors.New("storage must not be nil")
	}
	if deps.Clock == nil {
		return errors.New("clock must not be nil")
	}
	return nil
}

// Initialize loads persisted state and sets up the state manager
func (sm *stateManager) Initialize(ctx context.Context) error {
	if sm.isShutdown.Load() {
		return errors.New("cannot initialize: node is shutting down")
	}

	sm.logger.Infow("Initializing state manager", "nodeID", sm.id)

	// Load persistent state with timeout
	loadCtx, cancel := context.WithTimeout(ctx, stateManagerOpTimeout)
	defer cancel()

	if err := sm.loadPersistedState(loadCtx); err != nil {
		sm.logger.Errorw("Failed to load persisted state, initializing defaults",
			"error", err, "nodeID", sm.id)

		// Try to initialize with defaults
		if initErr := sm.initializeDefaultState(loadCtx); initErr != nil {
			return fmt.Errorf("failed to initialize default state: %w", initErr)
		}
	}

	// Log initial state
	snapshot := sm.GetSnapshot()
	sm.logger.Infow("State manager initialized successfully",
		"nodeID", sm.id,
		"term", snapshot.Term,
		"role", snapshot.Role.String(),
		"leader", snapshot.LeaderID,
		"commitIndex", snapshot.CommitIndex,
		"lastApplied", snapshot.LastApplied,
		"votedFor", snapshot.VotedFor)

	return nil
}

// loadPersistedState loads the persistent state from storage
func (sm *stateManager) loadPersistedState(ctx context.Context) error {
	state, err := sm.storage.LoadState(ctx)
	if err != nil {
		return fmt.Errorf("failed to load persistent state: %w", err)
	}

	sm.mu.Lock()
	sm.applyPersistentStateLocked(state.CurrentTerm, state.VotedFor)
	sm.mu.Unlock()

	sm.logger.Infow("Loaded persisted state",
		"term", state.CurrentTerm,
		"votedFor", state.VotedFor,
		"nodeID", sm.id)

	sm.metrics.ObserveTerm(state.CurrentTerm)
	return nil
}

// initializeDefaultState sets up default state when persistence fails
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
		"term", initialTerm,
		"votedFor", initialVotedFor,
		"nodeID", sm.id)

	sm.metrics.ObserveTerm(initialTerm)
	return nil
}

// GetState returns the current state with read lock protection
func (sm *stateManager) GetState() (types.Term, types.NodeRole, types.NodeID) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.GetStateUnsafe()
}

// GetStateUnsafe returns current state without locking (caller must hold lock)
func (sm *stateManager) GetStateUnsafe() (types.Term, types.NodeRole, types.NodeID) {
	return sm.currentTerm, sm.role, sm.leaderID
}

// GetSnapshot returns a consistent snapshot of all state
func (sm *stateManager) GetSnapshot() StateSnapshot {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.GetSnapshotUnsafe()
}

// GetSnapshotUnsafe returns state snapshot without locking (caller must hold lock)
func (sm *stateManager) GetSnapshotUnsafe() StateSnapshot {
	return StateSnapshot{
		Term:            sm.currentTerm,
		Role:            sm.role,
		LeaderID:        sm.leaderID,
		CommitIndex:     sm.commitIndex,
		LastApplied:     sm.lastApplied,
		VotedFor:        sm.votedFor,
		LastKnownLeader: sm.lastKnownLeaderID,
	}
}

// GetLastKnownLeader returns the most recently known leader
func (sm *stateManager) GetLastKnownLeader() types.NodeID {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastKnownLeaderID
}

// BecomeCandidate transitions to candidate state with improved error handling
func (sm *stateManager) BecomeCandidate(ctx context.Context, reason ElectionReason) bool {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Cannot become candidate: node shutting down",
			"reason", reason, "nodeID", sm.id)
		return false
	}

	// Use timeout context to prevent hanging
	transitionCtx, cancel := context.WithTimeout(ctx, stateTransitionTimeout)
	defer cancel()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentTerm, currentRole, _ := sm.GetStateUnsafe()

	// Only followers can become candidates
	if currentRole != types.RoleFollower {
		sm.logger.Debugw("Cannot become candidate: not a follower",
			"currentRole", currentRole.String(),
			"reason", reason,
			"nodeID", sm.id)
		return false
	}

	newTerm := currentTerm + 1

	sm.logger.Infow("Attempting to become candidate",
		"currentTerm", currentTerm,
		"newTerm", newTerm,
		"reason", reason,
		"nodeID", sm.id)

	// Start election process
	sm.startElectionLocked()

	// Persist the new state
	if err := sm.persistState(transitionCtx, newTerm, sm.id); err != nil {
		sm.logger.Errorw("Failed to persist candidate state, rolling back",
			"term", newTerm,
			"error", err,
			"nodeID", sm.id)

		// Rollback to previous state
		sm.applyPersistentStateLocked(currentTerm, unknownNodeID)
		sm.setRoleAndLeaderLocked(types.RoleFollower, unknownNodeID, currentTerm)
		return false
	}

	sm.logger.Infow("Successfully became candidate",
		"term", newTerm,
		"reason", reason,
		"nodeID", sm.id)

	sm.roleChangeCount.Add(1)
	sm.termChangeCount.Add(1)
	sm.metrics.ObserveElectionStart(newTerm, reason)
	sm.metrics.ObserveRoleChange(types.RoleCandidate, currentRole, newTerm)
	sm.metrics.ObserveTerm(newTerm)

	return true
}

// BecomeLeaderUnsafe transitions to leader state with validation
func (sm *stateManager) BecomeLeaderUnsafe(ctx context.Context) bool {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Cannot become leader: node shutting down", "nodeID", sm.id)
		return false
	}

	currentTerm, currentRole, _ := sm.GetStateUnsafe()

	// Only candidates can become leaders
	if currentRole != types.RoleCandidate {
		sm.logger.Warnw("Cannot become leader: not a candidate",
			"currentRole", currentRole.String(),
			"term", currentTerm,
			"nodeID", sm.id)
		return false
	}

	sm.logger.Infow("Becoming leader",
		"term", currentTerm,
		"nodeID", sm.id)

	// Transition to leader role
	sm.setRoleAndLeaderLocked(types.RoleLeader, sm.id, currentTerm)

	sm.logger.Infow("Successfully became leader",
		"term", currentTerm,
		"nodeID", sm.id)

	sm.roleChangeCount.Add(1)
	sm.metrics.ObserveRoleChange(types.RoleLeader, currentRole, currentTerm)

	// Notify about leader change
	sm.notifyLeaderChange(sm.id)

	return true
}

// BecomeCandidateForTerm transitions to candidate state for a specific term
func (sm *stateManager) BecomeCandidateForTerm(ctx context.Context, term types.Term) bool {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Cannot become candidate: node shutting down",
			"targetTerm", term, "nodeID", sm.id)
		return false
	}

	transitionCtx, cancel := context.WithTimeout(ctx, stateTransitionTimeout)
	defer cancel()

	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentTerm, currentRole, _ := sm.GetStateUnsafe()

	if currentRole != types.RoleFollower {
		sm.logger.Debugw("Cannot become candidate: not a follower",
			"currentRole", currentRole.String(),
			"targetTerm", term,
			"nodeID", sm.id)
		return false
	}

	if term <= currentTerm {
		sm.logger.Debugw("Cannot become candidate: target term not higher than current",
			"currentTerm", currentTerm,
			"targetTerm", term,
			"nodeID", sm.id)
		return false
	}

	sm.logger.Infow("Attempting to become candidate for specific term",
		"currentTerm", currentTerm,
		"targetTerm", term,
		"nodeID", sm.id)

	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor
	previousRole := sm.role
	previousLeader := sm.leaderID

	sm.applyPersistentStateLocked(term, sm.id)
	sm.setRoleAndLeaderLocked(types.RoleCandidate, unknownNodeID, term)

	if err := sm.persistState(transitionCtx, term, sm.id); err != nil {
		sm.logger.Errorw("Failed to persist candidate state for term, rolling back",
			"targetTerm", term,
			"error", err,
			"nodeID", sm.id)

		sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
		sm.setRoleAndLeaderLocked(previousRole, previousLeader, previousTerm)
		return false
	}

	sm.logger.Infow("Successfully became candidate for specific term",
		"term", term,
		"previousTerm", currentTerm,
		"nodeID", sm.id)

	sm.roleChangeCount.Add(1)
	sm.termChangeCount.Add(1)
	sm.metrics.ObserveTerm(term)
	sm.metrics.ObserveRoleChange(types.RoleCandidate, previousRole, term)

	return true
}

// BecomeFollower transitions to follower state
func (sm *stateManager) BecomeFollower(
	ctx context.Context,
	term types.Term,
	leaderID types.NodeID,
) {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Cannot become follower: node shutting down",
			"term", term, "leader", leaderID, "nodeID", sm.id)
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentTerm, currentRole, currentLeader := sm.GetStateUnsafe()

	sm.logger.Infow("Becoming follower",
		"currentTerm", currentTerm,
		"newTerm", term,
		"currentRole", currentRole.String(),
		"currentLeader", currentLeader,
		"newLeader", leaderID,
		"nodeID", sm.id)

	// Update term if necessary
	if term > currentTerm {
		sm.applyPersistentStateLocked(term, unknownNodeID)
		sm.termChangeCount.Add(1)
		sm.metrics.ObserveTerm(term)

		// Persist the new term
		persistCtx, cancel := context.WithTimeout(ctx, persistOperationTimeout)
		defer cancel()

		if err := sm.persistState(persistCtx, term, unknownNodeID); err != nil {
			sm.logger.Errorw("Failed to persist follower state",
				"term", term, "error", err, "nodeID", sm.id)
			// Continue despite persistence error - state is still updated in memory
		}
	}

	// Update role and leader
	previousRole := sm.role
	sm.setRoleAndLeaderLocked(types.RoleFollower, leaderID, term)

	if previousRole != types.RoleFollower {
		sm.roleChangeCount.Add(1)
		sm.metrics.ObserveRoleChange(types.RoleFollower, currentRole, sm.currentTerm)
	}

	sm.logger.Infow("Successfully became follower",
		"term", term,
		"leader", leaderID,
		"previousRole", previousRole.String(),
		"nodeID", sm.id)

	// Notify about leader change if it actually changed
	if currentLeader != leaderID {
		sm.notifyLeaderChange(leaderID)
	}
}

// CheckTermAndStepDown checks if we should step down due to a higher term
func (sm *stateManager) CheckTermAndStepDown(
	ctx context.Context,
	rpcTerm types.Term,
	rpcLeader types.NodeID,
) (steppedDown bool, previousTerm types.Term) {
	if sm.isShutdown.Load() {
		sm.mu.RLock()
		currentTerm := sm.currentTerm
		sm.mu.RUnlock()
		return false, currentTerm
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentTerm, currentRole, _ := sm.GetStateUnsafe()

	if rpcTerm <= currentTerm {
		return false, currentTerm
	}

	sm.logger.Infow("Stepping down due to higher term",
		"currentTerm", currentTerm,
		"rpcTerm", rpcTerm,
		"currentRole", currentRole.String(),
		"rpcLeader", rpcLeader,
		"nodeID", sm.id)

	// Update to higher term and reset vote
	sm.applyPersistentStateLocked(rpcTerm, unknownNodeID)
	sm.setRoleAndLeaderLocked(types.RoleFollower, rpcLeader, rpcTerm)

	// Persist the new state
	persistCtx, cancel := context.WithTimeout(ctx, persistOperationTimeout)
	defer cancel()

	if err := sm.persistState(persistCtx, rpcTerm, unknownNodeID); err != nil {
		sm.logger.Errorw("Failed to persist step-down state",
			"term", rpcTerm, "error", err, "nodeID", sm.id)
		// Continue despite persistence error
	}

	sm.termChangeCount.Add(1)
	if currentRole != types.RoleFollower {
		sm.roleChangeCount.Add(1)
		sm.metrics.ObserveRoleChange(types.RoleFollower, currentRole, sm.currentTerm)
	}
	sm.metrics.ObserveTerm(rpcTerm)

	// Notify about leader change
	sm.notifyLeaderChange(rpcLeader)

	return true, currentTerm
}

// GrantVote attempts to grant a vote with improved validation and persistence
func (sm *stateManager) GrantVote(
	ctx context.Context,
	candidateID types.NodeID,
	term types.Term,
) bool {
	if sm.isShutdown.Load() {
		sm.logger.Infow("Vote rejected: node is shut down",
			"requestedTerm", term,
			"candidateID", candidateID,
			"nodeID", sm.id)
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentTerm := sm.currentTerm

	// Reject votes for lower terms
	if term < currentTerm {
		sm.logger.Debugw("Vote rejected: candidate term lower than current term",
			"currentTerm", currentTerm,
			"candidateTerm", term,
			"candidateID", candidateID,
			"nodeID", sm.id)
		return false
	}

	// Store previous state for rollback
	previousVotedFor := sm.votedFor
	previousTerm := sm.currentTerm

	// Update to higher term if necessary and reset vote
	if term > currentTerm {
		sm.logger.Debugw("Updating to higher term, resetting vote",
			"currentTerm", currentTerm,
			"newTerm", term,
			"previousVote", sm.votedFor,
			"nodeID", sm.id)

		sm.currentTerm = term
		sm.votedFor = unknownNodeID // Reset vote for new term
		sm.termChangeCount.Add(1)
		sm.metrics.ObserveTerm(term)
	}

	// Now check if we can vote in the current (possibly updated) term
	canVote := sm.votedFor == unknownNodeID || sm.votedFor == candidateID
	if !canVote {
		sm.logger.Infow("Vote rejected: already voted for another candidate",
			"currentTerm", sm.currentTerm,
			"votedFor", sm.votedFor,
			"candidateID", candidateID,
			"nodeID", sm.id)

		// If we updated the term but can't vote, we still need to persist the term update
		if term > previousTerm {
			persistCtx, cancel := context.WithTimeout(ctx, persistOperationTimeout)
			defer cancel()

			if err := sm.persistStateWithRetry(persistCtx, sm.currentTerm, sm.votedFor); err != nil {
				sm.logger.Errorw("Failed to persist term update after vote rejection",
					"term", sm.currentTerm,
					"error", err,
					"nodeID", sm.id)
				// Rollback in-memory state
				sm.currentTerm = previousTerm
				sm.votedFor = previousVotedFor
			}
		}
		return false
	}

	sm.logger.Debugw("Attempting to grant vote",
		"term", sm.currentTerm,
		"candidateID", candidateID,
		"currentVotedFor", sm.votedFor,
		"nodeID", sm.id)

	// Update vote in memory
	sm.votedFor = candidateID

	// Persist the vote with retry logic
	persistCtx, cancel := context.WithTimeout(ctx, persistOperationTimeout)
	defer cancel()

	if err := sm.persistStateWithRetry(persistCtx, sm.currentTerm, candidateID); err != nil {
		sm.logger.Errorw("Failed to persist vote, rolling back",
			"term", sm.currentTerm,
			"candidateID", candidateID,
			"error", err,
			"nodeID", sm.id)

		// Rollback in-memory state
		sm.votedFor = previousVotedFor
		sm.currentTerm = previousTerm

		return false
	}

	sm.logger.Infow("Vote granted and persisted",
		"term", sm.currentTerm,
		"candidateID", candidateID,
		"nodeID", sm.id)

	sm.metrics.ObserveVoteGranted(sm.currentTerm)
	return true
}

// UpdateCommitIndex updates the commit index if higher
func (sm *stateManager) UpdateCommitIndex(newCommitIndex types.Index) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	return sm.UpdateCommitIndexUnsafe(newCommitIndex)
}

// UpdateCommitIndexUnsafe updates commit index without locking
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
		"role", sm.role.String(),
		"nodeID", sm.id)

	sm.metrics.ObserveCommitIndex(newCommitIndex)
	return true
}

// UpdateLastApplied updates the last applied index with validation
func (sm *stateManager) UpdateLastApplied(newLastApplied types.Index) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Validate that lastApplied doesn't exceed commitIndex
	if newLastApplied > sm.commitIndex {
		sm.logger.Errorw("Attempted to set lastApplied beyond commitIndex",
			"lastApplied", newLastApplied,
			"commitIndex", sm.commitIndex,
			"term", sm.currentTerm,
			"role", sm.role.String(),
			"nodeID", sm.id)
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
		"role", sm.role.String(),
		"nodeID", sm.id)

	sm.metrics.ObserveAppliedIndex(newLastApplied)
	return true
}

// GetCommitIndex returns the current commit index safely
func (sm *stateManager) GetCommitIndex() types.Index {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.commitIndex
}

// GetCommitIndexUnsafe returns commit index without locking
func (sm *stateManager) GetCommitIndexUnsafe() types.Index {
	return sm.commitIndex
}

// GetLastApplied returns the last applied index safely
func (sm *stateManager) GetLastApplied() types.Index {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.lastApplied
}

// GetLastAppliedUnsafe returns last applied index without locking
func (sm *stateManager) GetLastAppliedUnsafe() types.Index {
	return sm.lastApplied
}

// Utility methods for role checking

// IsLeader returns true if the node is currently the leader
func (sm *stateManager) IsLeader() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.role == types.RoleLeader
}

// IsCandidate returns true if the node is currently a candidate
func (sm *stateManager) IsCandidate() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.role == types.RoleCandidate
}

// IsFollower returns true if the node is currently a follower
func (sm *stateManager) IsFollower() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.role == types.RoleFollower
}

// Stop performs cleanup when shutting down
func (sm *stateManager) Stop() {
	if sm.isShutdown.CompareAndSwap(false, true) {
		sm.logger.Infow("StateManager received stop signal",
			"nodeID", sm.id,
			"roleChangeCount", sm.roleChangeCount.Load(),
			"termChangeCount", sm.termChangeCount.Load(),
			"persistFailureCount", sm.persistFailureCount.Load())
	} else {
		sm.logger.Debugw("StateManager stop signal received multiple times", "nodeID", sm.id)
	}
}

// startElectionLocked starts an election by incrementing term and voting for self
// Must be called with sm.mu.Lock() held
func (sm *stateManager) startElectionLocked() {
	newTerm := sm.currentTerm + 1
	sm.applyPersistentStateLocked(newTerm, sm.id)
	sm.setRoleAndLeaderLocked(types.RoleCandidate, unknownNodeID, newTerm)

	sm.logger.Debugw("Started election",
		"newTerm", newTerm,
		"nodeID", sm.id)
}

// applyPersistentStateLocked updates persistent state fields in memory
// Must be called with sm.mu.Lock() held
func (sm *stateManager) applyPersistentStateLocked(term types.Term, votedFor types.NodeID) {
	sm.currentTerm = term
	sm.votedFor = votedFor
}

// setRoleAndLeaderLocked updates role and leader information
// Must be called with sm.mu.Lock() held
func (sm *stateManager) setRoleAndLeaderLocked(
	role types.NodeRole,
	leaderID types.NodeID,
	term types.Term,
) {
	previousRole := sm.role
	previousLeader := sm.leaderID

	sm.role = role
	sm.leaderID = leaderID
	sm.lastRoleChange = sm.clock.Now()

	// Update last known leader if we have a valid leader
	if leaderID != unknownNodeID {
		sm.lastKnownLeaderID = leaderID
	}

	sm.logger.Debugw("Role and leader updated",
		"previousRole", previousRole.String(),
		"newRole", role.String(),
		"previousLeader", previousLeader,
		"newLeader", leaderID,
		"term", term,
		"nodeID", sm.id)
}

// persistState persists the current term and voted for to storage
func (sm *stateManager) persistState(
	ctx context.Context,
	term types.Term,
	votedFor types.NodeID,
) error {
	// Prevent concurrent persistence operations
	if !sm.persistenceInProgress.CompareAndSwap(false, true) {
		sm.logger.Debugw("Persistence operation already in progress, skipping",
			"term", term, "votedFor", votedFor, "nodeID", sm.id)
		return errors.New("persistence operation already in progress")
	}
	defer sm.persistenceInProgress.Store(false)

	sm.lastPersistAttempt = sm.clock.Now()

	sm.logger.Debugw("Persisting state",
		"term", term,
		"votedFor", votedFor,
		"nodeID", sm.id)

	state := types.PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	if err := sm.storage.SaveState(ctx, state); err != nil {
		sm.persistFailureCount.Add(1)
		return fmt.Errorf("failed to persist state: %w", err)
	}

	sm.logger.Debugw("State persisted successfully",
		"term", term,
		"votedFor", votedFor,
		"nodeID", sm.id)

	return nil
}

// persistStateWithRetry persists state with retry logic
func (sm *stateManager) persistStateWithRetry(
	ctx context.Context,
	term types.Term,
	votedFor types.NodeID,
) error {
	var lastErr error
	delay := basePersistDelay

	for attempt := 1; attempt <= maxPersistRetries; attempt++ {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("context cancelled during persistence retry: %w", err)
		}

		lastErr = sm.persistState(ctx, term, votedFor)
		if lastErr == nil {
			if attempt > 1 {
				sm.logger.Infow("Persistence succeeded after retry",
					"attempt", attempt,
					"term", term,
					"votedFor", votedFor,
					"nodeID", sm.id)
			}
			return nil
		}

		sm.logger.Warnw("Persistence attempt failed",
			"attempt", attempt,
			"maxRetries", maxPersistRetries,
			"term", term,
			"votedFor", votedFor,
			"error", lastErr,
			"nodeID", sm.id)

		// Don't delay after the last attempt
		if attempt < maxPersistRetries {
			select {
			case <-ctx.Done():
				return fmt.Errorf("context cancelled during retry delay: %w", ctx.Err())
			case <-sm.clock.After(delay):
				// Exponential backoff with cap
				delay = delay * 2
				if delay > maxPersistDelay {
					delay = maxPersistDelay
				}
			}
		}
	}

	sm.logger.Errorw("All persistence attempts failed",
		"maxRetries", maxPersistRetries,
		"term", term,
		"votedFor", votedFor,
		"finalError", lastErr,
		"nodeID", sm.id)

	return fmt.Errorf("persistence failed after %d attempts: %w", maxPersistRetries, lastErr)
}

// notifyLeaderChange sends leader change notification safely
func (sm *stateManager) notifyLeaderChange(newLeaderID types.NodeID) {
	if sm.leaderChangeCh == nil {
		sm.logger.Debugw("No leader change channel configured", "nodeID", sm.id)
		return
	}

	select {
	case sm.leaderChangeCh <- newLeaderID:
		sm.logger.Debugw("Leader change notification sent",
			"newLeader", newLeaderID,
			"nodeID", sm.id)
	default:
		sm.logger.Warnw("Leader change channel full, dropping notification",
			"newLeader", newLeaderID,
			"nodeID", sm.id)
	}
}

// Debug and monitoring methods

// GetStateInfo returns detailed state information for debugging
func (sm *stateManager) GetStateInfo() map[string]interface{} {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return map[string]interface{}{
		"nodeID":                sm.id,
		"currentTerm":           sm.currentTerm,
		"votedFor":              sm.votedFor,
		"role":                  sm.role.String(),
		"leaderID":              sm.leaderID,
		"lastKnownLeaderID":     sm.lastKnownLeaderID,
		"commitIndex":           sm.commitIndex,
		"lastApplied":           sm.lastApplied,
		"lastRoleChange":        sm.lastRoleChange,
		"roleChangeCount":       sm.roleChangeCount.Load(),
		"termChangeCount":       sm.termChangeCount.Load(),
		"persistFailureCount":   sm.persistFailureCount.Load(),
		"lastPersistAttempt":    sm.lastPersistAttempt,
		"isShutdown":            sm.isShutdown.Load(),
		"persistenceInProgress": sm.persistenceInProgress.Load(),
	}
}

// GetRoleChangeCount returns the number of role changes for monitoring
func (sm *stateManager) GetRoleChangeCount() uint64 {
	return sm.roleChangeCount.Load()
}

// GetTermChangeCount returns the number of term changes for monitoring
func (sm *stateManager) GetTermChangeCount() uint64 {
	return sm.termChangeCount.Load()
}

// GetPersistFailureCount returns the number of persistence failures
func (sm *stateManager) GetPersistFailureCount() uint64 {
	return sm.persistFailureCount.Load()
}

// Advanced state management methods

// CompareAndUpdateTerm atomically updates term if the new term is higher
func (sm *stateManager) CompareAndUpdateTerm(
	ctx context.Context,
	newTerm types.Term,
) (updated bool, currentTerm types.Term) {
	if sm.isShutdown.Load() {
		sm.mu.RLock()
		currentTerm := sm.currentTerm
		sm.mu.RUnlock()
		return false, currentTerm
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	currentTerm = sm.currentTerm
	if newTerm <= currentTerm {
		return false, currentTerm
	}

	sm.logger.Debugw("Updating term",
		"from", currentTerm,
		"to", newTerm,
		"nodeID", sm.id)

	// Update term and reset vote
	sm.applyPersistentStateLocked(newTerm, unknownNodeID)
	sm.termChangeCount.Add(1)
	sm.metrics.ObserveTerm(newTerm)

	// Persist the new term
	persistCtx, cancel := context.WithTimeout(ctx, persistOperationTimeout)
	defer cancel()

	if err := sm.persistState(persistCtx, newTerm, unknownNodeID); err != nil {
		sm.logger.Errorw("Failed to persist new term",
			"term", newTerm, "error", err, "nodeID", sm.id)
		// Rollback
		sm.applyPersistentStateLocked(currentTerm, sm.votedFor)
		return false, currentTerm
	}

	return true, newTerm
}

// GetVotedFor returns who this node voted for in the current term
func (sm *stateManager) GetVotedFor() types.NodeID {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.votedFor
}

// GetVotedForUnsafe returns who this node voted for without locking
func (sm *stateManager) GetVotedForUnsafe() types.NodeID {
	return sm.votedFor
}

// HasVoted returns true if this node has voted in the current term
func (sm *stateManager) HasVoted() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.votedFor != unknownNodeID
}

// HasVotedUnsafe returns true if this node has voted without locking
func (sm *stateManager) HasVotedUnsafe() bool {
	return sm.votedFor != unknownNodeID
}

// CanVoteFor returns true if this node can vote for the given candidate
func (sm *stateManager) CanVoteFor(candidateID types.NodeID) bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.votedFor == unknownNodeID || sm.votedFor == candidateID
}

// CanVoteForUnsafe returns true if this node can vote for the given candidate without locking
func (sm *stateManager) CanVoteForUnsafe(candidateID types.NodeID) bool {
	return sm.votedFor == unknownNodeID || sm.votedFor == candidateID
}

// GetTimeSinceLastRoleChange returns time elapsed since last role change
func (sm *stateManager) GetTimeSinceLastRoleChange() time.Duration {
	sm.mu.RLock()
	defer sm.mu.RUnlock()
	return sm.clock.Since(sm.lastRoleChange)
}

// IsInTransition returns true if the node is in a transitional state
func (sm *stateManager) IsInTransition() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	// Consider it in transition if it's a candidate or recent role change
	return sm.role == types.RoleCandidate ||
		sm.clock.Since(sm.lastRoleChange) < 100*time.Millisecond ||
		sm.persistenceInProgress.Load()
}

// ValidateState performs internal consistency checks
func (sm *stateManager) ValidateState() []string {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	var issues []string

	// Check that lastApplied <= commitIndex
	if sm.lastApplied > sm.commitIndex {
		issues = append(issues, fmt.Sprintf("lastApplied (%d) > commitIndex (%d)",
			sm.lastApplied, sm.commitIndex))
	}

	// Check that leaders have themselves as leader
	if sm.role == types.RoleLeader && sm.leaderID != sm.id {
		issues = append(issues, fmt.Sprintf("leader role but leaderID is %s, not %s",
			sm.leaderID, sm.id))
	}

	// Check that candidates have no leader
	if sm.role == types.RoleCandidate && sm.leaderID != unknownNodeID {
		issues = append(issues, fmt.Sprintf("candidate role but has leader %s", sm.leaderID))
	}

	// Check that we voted for ourselves if we're a candidate
	if sm.role == types.RoleCandidate && sm.votedFor != sm.id {
		issues = append(
			issues,
			fmt.Sprintf("candidate role but voted for %s, not self", sm.votedFor),
		)
	}

	return issues
}

// StateTransition represents a state transition for logging/debugging
type StateTransition struct {
	Timestamp  time.Time
	FromRole   types.NodeRole
	ToRole     types.NodeRole
	FromTerm   types.Term
	ToTerm     types.Term
	FromLeader types.NodeID
	ToLeader   types.NodeID
	Reason     string
}

// Additional helper methods for specific Raft operations

// PrepareForElection prepares the state for starting an election
// Returns the new term and whether preparation was successful
func (sm *stateManager) PrepareForElection(ctx context.Context) (types.Term, bool) {
	if sm.isShutdown.Load() {
		return 0, false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Only followers can start elections
	if sm.role != types.RoleFollower {
		sm.logger.Debugw("Cannot prepare for election: not a follower",
			"currentRole", sm.role.String(),
			"nodeID", sm.id)
		return sm.currentTerm, false
	}

	newTerm := sm.currentTerm + 1

	sm.logger.Debugw("Preparing for election",
		"currentTerm", sm.currentTerm,
		"newTerm", newTerm,
		"nodeID", sm.id)

	return newTerm, true
}

// ResetElectionState resets election-related state (typically after stepping down)
func (sm *stateManager) ResetElectionState(ctx context.Context) {
	if sm.isShutdown.Load() {
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.role != types.RoleFollower {
		sm.logger.Debugw("Resetting election state",
			"previousRole", sm.role.String(),
			"term", sm.currentTerm,
			"nodeID", sm.id)

		sm.setRoleAndLeaderLocked(types.RoleFollower, unknownNodeID, sm.currentTerm)
		sm.roleChangeCount.Add(1)
		sm.metrics.ObserveRoleChange(types.RoleFollower, sm.role, sm.currentTerm)
	}
}

// GetLeaderInfo returns current leader information
func (sm *stateManager) GetLeaderInfo() (currentLeader, lastKnownLeader types.NodeID, hasLeader bool) {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	return sm.leaderID, sm.lastKnownLeaderID, sm.leaderID != unknownNodeID
}

// GetLeaderInfoUnsafe returns leader information without locking
func (sm *stateManager) GetLeaderInfoUnsafe() (currentLeader, lastKnownLeader types.NodeID, hasLeader bool) {
	return sm.leaderID, sm.lastKnownLeaderID, sm.leaderID != unknownNodeID
}
