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

	BecomeCandidateForTerm(ctx context.Context, term types.Term) bool

	IsInLeadershipGracePeriod() bool
	SetLeaderGracePeriod(gracePeriod time.Duration)
	SetLeaderStabilityMinimum(stabilityMin time.Duration)

	// BecomeLeader transitions the node to leader state after winning an election.
	// Persists the current state (which should reflect the election term and self-vote).
	// Returns true if the transition was successful.
	BecomeLeader(ctx context.Context) bool

	// BecomeFollower transitions the node to follower state for the given term and leader ID.
	// If the term is greater than the current term, updates the local term and resets votedFor.
	// In both cases (higher or same term), updates role and known leader if they differ.
	// Persists state only when the term is updated. No-op if already a follower in the same term and leader.
	// This method does not return a value; failure to persist on term change results in rollback.
	BecomeFollower(ctx context.Context, term types.Term, leaderID types.NodeID)

	// CheckTermAndStepDown compares the given term with the local term.
	// If the given term is higher, steps down and updates state.
	// Returns whether a step-down occurred and the previous term.
	CheckTermAndStepDown(
		ctx context.Context,
		rpcTerm types.Term,
		rpcLeader types.NodeID,
	) (steppedDown bool, previousTerm types.Term)

	// GrantVote attempts to grant a vote to the given candidate for the specified term,
	// checking term validity and if the node has already voted in this term.
	// Persists the vote if granted. Returns true if the vote was granted and persisted.
	GrantVote(ctx context.Context, candidateID types.NodeID, term types.Term) bool

	// UpdateCommitIndex sets the commit index if the new index is higher.
	// Acquires a lock for thread safety. Returns true if the index was updated.
	UpdateCommitIndex(newCommitIndex types.Index) bool

	// UpdateCommitIndexUnsafe sets the commit index if the new index is higher.
	// The caller must ensure thread safety. Returns true if the index was updated.
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

	// GetLastAppliedUnsafe rreturns the index of the last applied log entry without synchronization.
	// The caller must ensure thread safety.
	GetLastAppliedUnsafe() types.Index

	// Stop signals the state manager to stop processing and releases resources.
	Stop()
}

// stateManager implements the StateManager interface.
// It manages the persistent Raft state (current term, votedFor),
// handles role transitions, and coordinates related metadata updates
// like commit/applied indices.
// It ensures that state changes are persisted before they are acted upon.
type stateManager struct {
	id types.NodeID // ID of the local Raft node.

	isShutdown *atomic.Bool // Shared flag indicating Raft shutdown

	mu          *sync.RWMutex  // Raft's mutex protecting state fields
	currentTerm types.Term     // Latest term server has seen
	votedFor    types.NodeID   // CandidateID that received vote in current term
	role        types.NodeRole // Current role (Follower, Candidate, or Leader)
	leaderID    types.NodeID   // Node ID of the current known leader, "" if unknown.
	commitIndex types.Index    // Index of highest log entry known to be committed
	lastApplied types.Index    // Index of highest log entry applied (applier)

	leaderChangeCh chan<- types.NodeID // Channel to notify leader changes

	logger  logger.Logger
	metrics Metrics
	storage storage.Storage
	clock   Clock

	becameLeaderAt     time.Time     // When this node became leader
	lastKnownLeaderID  types.NodeID  // Stores the ID of the last known leader, persists even if leader becomes unknown.
	leaderGracePeriod  time.Duration // Grace period for new leaders
	leaderStabilityMin time.Duration // Minimum time before considering leadership challenges

}

// NewStateManager creates and initializes a new state manager.
func NewStateManager(mu *sync.RWMutex, shutdownFlag *atomic.Bool, deps Dependencies,
	nodeID types.NodeID, leaderChangeCh chan<- types.NodeID,
) StateManager {
	if mu == nil || shutdownFlag == nil || deps.Storage == nil || deps.Metrics == nil ||
		deps.Logger == nil ||
		nodeID == "" ||
		leaderChangeCh == nil {
		panic("raft: invalid arguments provided to NewStateManager")
	}
	return &stateManager{
		mu:                 mu,
		isShutdown:         shutdownFlag,
		storage:            deps.Storage,
		clock:              deps.Clock,
		metrics:            deps.Metrics,
		logger:             deps.Logger.WithComponent("state"),
		id:                 nodeID,
		leaderChangeCh:     leaderChangeCh,
		role:               types.RoleFollower,
		commitIndex:        0,
		lastApplied:        0,
		leaderGracePeriod:  500 * time.Millisecond, // Grace period for new leaders
		leaderStabilityMin: 200 * time.Millisecond, // Minimum leadership duration before challenges
	}
}

// Initialize loads the persisted state (term, votedFor) from storage during Raft startup.
// If the persisted state is corrupted, it resets to a default follower state and persists that.
func (sm *stateManager) Initialize(ctx context.Context) error {
	sm.logger.Infow("Initializing state manager...")

	state, err := sm.storage.LoadState(ctx)
	if err != nil {
		if errors.Is(err, storage.ErrCorruptedState) {
			sm.logger.Warnw(
				"Persistent state corrupted or missing, initializing default state",
				"error",
				err,
			)
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
	sm.metrics.ObserveTerm(state.CurrentTerm)
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

	sm.metrics.ObserveTerm(initialTerm)
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

	sm.metrics.ObserveTerm(newTerm)
	sm.metrics.ObserveElectionStart(newTerm, reason)

	sm.logger.Infow("Transitioned to Candidate", "new_term", newTerm)
	return true
}

func (sm *stateManager) BecomeCandidateForTerm(ctx context.Context, term types.Term) bool {
	if sm.isShutdown.Load() {
		sm.logger.Warnw("Attempted BecomeCandidateForTerm on shutdown node")
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Validate the term transition
	if term <= sm.currentTerm {
		sm.logger.Warnw("Cannot become candidate: term not higher than current",
			"requestedTerm", term, "currentTerm", sm.currentTerm)
		return false
	}

	// Validate role transition
	if !sm.role.CanTransitionTo(types.RoleCandidate) {
		sm.logger.Warnw("Cannot become candidate: invalid role transition",
			"currentRole", sm.role.String(), "currentTerm", sm.currentTerm)
		return false
	}

	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor
	previousRole := sm.role

	// Update state atomically
	sm.currentTerm = term
	sm.votedFor = sm.id // Vote for self

	// Persist the new state
	if err := sm.persistState(ctx, sm.currentTerm, sm.votedFor); err != nil {
		sm.logger.Errorw("Failed to persist candidate state, rolling back",
			"newTerm", sm.currentTerm, "error", err)

		// Rollback on persistence failure
		sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
		sm.setRoleAndLeaderLocked(previousRole, unknownNodeID, previousTerm)
		return false
	}

	// Update role and metrics
	sm.setRoleAndLeaderLocked(types.RoleCandidate, unknownNodeID, sm.currentTerm)
	sm.metrics.ObserveTerm(sm.currentTerm)

	sm.logger.Infow("Became candidate",
		"term", sm.currentTerm,
		"previousTerm", previousTerm,
		"previousRole", previousRole.String())

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

	// Validate current state
	if sm.role != types.RoleCandidate {
		sm.logger.Warnw("Cannot become leader: not a candidate",
			"currentRole", sm.role.String(), "currentTerm", sm.currentTerm)
		return false
	}

	// Ensure we voted for ourselves
	if sm.votedFor != sm.id {
		sm.logger.Warnw("Cannot become leader: did not vote for self",
			"votedFor", sm.votedFor, "selfID", sm.id)
		return false
	}

	currentTerm := sm.currentTerm
	sm.becameLeaderAt = sm.clock.Now()

	// Transition to leader
	sm.setRoleAndLeaderLocked(types.RoleLeader, sm.id, currentTerm)

	sm.logger.Infow("Transitioned to Leader",
		"term", currentTerm,
		"leader", sm.id,
		"became_leader_at", sm.becameLeaderAt.Format(time.RFC3339Nano))

	return true
}

// BecomeFollower transitions the node into Follower state for the specified term and leader.
// If the provided term is greater than the current term, it updates the term and resets votedFor,
// persisting the new state.
func (sm *stateManager) BecomeFollower(
	ctx context.Context,
	term types.Term,
	leaderID types.NodeID,
) {
	if sm.isShutdown.Load() {
		sm.logger.Debugw("Attempted BecomeFollower on shutdown node")
		return
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// Reject lower terms
	if term < sm.currentTerm {
		sm.logger.Warnw("BecomeFollower called with lower term than current",
			"current_term", sm.currentTerm,
			"received_term", term,
			"current_role", sm.role.String())
		return
	}

	// Check if this is a no-op
	if sm.role == types.RoleFollower && sm.currentTerm == term && sm.leaderID == leaderID {
		return
	}

	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor
	previousRole := sm.role

	// Reset leadership tracking if stepping down from leader
	if previousRole == types.RoleLeader {
		sm.becameLeaderAt = time.Time{}
		sm.logger.Infow("Leadership tracking reset - stepping down from leader",
			"previous_term", previousTerm,
			"new_term", term,
			"new_leader", leaderID)
	}

	// Handle term change
	termChanged := false
	if term > sm.currentTerm {
		sm.logger.Infow("Becoming follower due to higher term",
			"current_term", previousTerm, "new_term", term, "new_leader", leaderID)

		sm.currentTerm = term
		sm.votedFor = unknownNodeID
		termChanged = true

		// Persist the state change
		if err := sm.persistState(ctx, sm.currentTerm, sm.votedFor); err != nil {
			sm.logger.Errorw(
				"Failed to persist state change in BecomeFollower, rolling back",
				"new_term", sm.currentTerm,
				"error", err)

			// Rollback on failure
			sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
			sm.setRoleAndLeaderLocked(types.RoleFollower, leaderID, previousTerm)
			return
		}
		sm.metrics.ObserveTerm(sm.currentTerm)
	}

	// Update role and leader
	sm.setRoleAndLeaderLocked(types.RoleFollower, leaderID, sm.currentTerm)

	if !termChanged {
		sm.logger.Infow("Becoming follower in same term",
			"term", sm.currentTerm, "new_leader", leaderID,
			"previous_role", previousRole.String())
	}
}

// ENHANCED: Additional method to get leadership duration (useful for debugging)
func (sm *stateManager) GetLeadershipDuration() time.Duration {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.role != types.RoleLeader || sm.becameLeaderAt.IsZero() {
		return 0
	}

	return sm.clock.Since(sm.becameLeaderAt)
}

func (sm *stateManager) IsInLeadershipGracePeriod() bool {
	sm.mu.RLock()
	defer sm.mu.RUnlock()

	if sm.role != types.RoleLeader || sm.becameLeaderAt.IsZero() {
		return false
	}

	return sm.clock.Since(sm.becameLeaderAt) < sm.leaderGracePeriod
}

func (sm *stateManager) SetLeaderGracePeriod(gracePeriod time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.leaderGracePeriod = gracePeriod
	sm.logger.Infow("Leadership grace period updated",
		"new_grace_period", gracePeriod)
}

func (sm *stateManager) SetLeaderStabilityMinimum(stabilityMin time.Duration) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	sm.leaderStabilityMin = stabilityMin
	sm.logger.Infow("Leadership stability minimum updated",
		"new_stability_min", stabilityMin)
}

// CheckTermAndStepDown compares an RPC term with the local term and potentially steps down.
// Returns true if the node stepped down (became Follower), false otherwise.
// Also returns the node's term before any potential change.
func (sm *stateManager) CheckTermAndStepDown(
	ctx context.Context,
	rpcTerm types.Term,
	rpcLeader types.NodeID,
) (steppedDown bool, previousTerm types.Term) {
	if sm.isShutdown.Load() {
		sm.mu.RLock()
		prevTerm := sm.currentTerm
		sm.mu.RUnlock()
		sm.logger.Debugw("CheckTermAndStepDown called on shutdown node")
		return false, prevTerm
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	previousTerm = sm.currentTerm
	currentRole := sm.role

	// ONLY handle higher term RPCs - this prevents leadership thrashing
	if rpcTerm > sm.currentTerm {
		sm.logger.Errorw(">>> STEPPING DOWN",
			"node", sm.id,
			"oldTerm", sm.currentTerm,
			"newTerm", rpcTerm,
			"reason", "HIGHER_TERM_RPC")

		sm.logger.Infow("Higher term received via RPC, stepping down",
			"current_term", sm.currentTerm,
			"rpc_term", rpcTerm,
			"rpc_leader_hint", rpcLeader,
			"current_role", currentRole.String())

		if err := sm.stepDownToHigherTermLocked(ctx, rpcTerm, rpcLeader); err != nil {
			sm.logger.Errorw("Persistence failed during step down", "error", err)
		}
		return true, previousTerm
	}

	// For same-term RPCs, only update leader hint if we're a follower and don't know the leader
	if rpcTerm == sm.currentTerm && currentRole == types.RoleFollower {
		if rpcLeader != unknownNodeID && (sm.leaderID == unknownNodeID || sm.leaderID != rpcLeader) {
			sm.logger.Debugw("Updating follower's known leader from RPC hint",
				"term", sm.currentTerm,
				"old_leader", sm.leaderID,
				"new_leader_hint", rpcLeader)
			sm.setRoleAndLeaderLocked(types.RoleFollower, rpcLeader, sm.currentTerm)
		}
	}

	return false, previousTerm
}

func (sm *stateManager) isValidLeadershipChallenge(challengerID types.NodeID, leadershipDuration time.Duration) bool {
	// 1. Leadership must be established for minimum duration
	if leadershipDuration < sm.leaderStabilityMin {
		return false
	}

	// 2. Challenger cannot be ourselves
	if challengerID == sm.id {
		sm.logger.Warnw("Received leadership challenge from self - this should not happen",
			"challenger", challengerID,
			"self", sm.id)
		return false
	}

	// 3. Must be past grace period
	if leadershipDuration < sm.leaderGracePeriod {
		return false
	}

	return true
}

// stepDownToHigherTermLocked updates term, resets vote, becomes follower, and persists state.
// Must be called with sm.mu.Lock() held. Returns error from persistence.
func (sm *stateManager) stepDownToHigherTermLocked(
	ctx context.Context,
	newTerm types.Term,
	newLeader types.NodeID,
) error {
	previousTerm := sm.currentTerm
	previousVotedFor := sm.votedFor

	sm.currentTerm = newTerm
	sm.votedFor = unknownNodeID

	// Persist the new state
	if err := sm.persistState(ctx, sm.currentTerm, sm.votedFor); err != nil {
		// Rollback on persistence failure
		sm.applyPersistentStateLocked(previousTerm, previousVotedFor)
		return fmt.Errorf("failed to persist higher term step down: %w", err)
	}

	// Update role and metrics
	sm.setRoleAndLeaderLocked(types.RoleFollower, newLeader, sm.currentTerm)
	sm.metrics.ObserveTerm(sm.currentTerm)

	return nil
}

// GrantVote attempts to grant a vote for the given candidate in the specified term.
// Checks term validity and ensures only one vote is cast per term. Persists the vote if granted.
func (sm *stateManager) GrantVote(
	ctx context.Context,
	candidateID types.NodeID,
	term types.Term,
) bool {
	if sm.isShutdown.Load() {
		sm.logger.Infow("Vote rejected: node is shut down",
			"requested_term", term,
			"candidate_id", candidateID)
		return false
	}

	sm.mu.Lock()
	defer sm.mu.Unlock()

	// If an RPC has a higher term, the recipient MUST update its term and step down.
	if term > sm.currentTerm {
		sm.logger.Infow("Stepping down to follower due to higher term in vote request",
			"current_term", sm.currentTerm, "new_term", term)
		sm.currentTerm = term
		sm.votedFor = "" // Reset vote for the new term
		sm.role = types.RoleFollower
		sm.leaderID = "" // Leader is unknown in new term
	}

	if term < sm.currentTerm {
		sm.logger.Debugw("Vote rejected: candidate term lower than current term",
			"current_term", sm.currentTerm,
			"candidate_term", term,
			"candidate_id", candidateID)
		return false
	}

	// Check if we can grant the vote in the (possibly new) current term.
	canVote := sm.votedFor == "" || sm.votedFor == candidateID
	if !canVote {
		sm.logger.Infow("Vote rejected: already voted for another candidate in this term",
			"current_term", sm.currentTerm,
			"voted_for", sm.votedFor,
			"candidate_id", candidateID)
		return false
	}

	// We can vote. Persist the state BEFORE returning true.
	// The lock is held throughout this operation.
	if err := sm.persistState(ctx, sm.currentTerm, candidateID); err != nil {
		sm.logger.Errorw("Failed to persist vote, vote not granted",
			"term", sm.currentTerm,
			"candidate_id", candidateID,
			"error", err)
		// Do not change state if persistence fails. The `defer` will unlock.
		return false
	}

	// Only update in-memory state after successful persistence.
	sm.votedFor = candidateID
	sm.logger.Infow("Vote granted",
		"term", sm.currentTerm,
		"candidate_id", candidateID)
	sm.metrics.ObserveVoteGranted(sm.currentTerm)
	return true
}

// UpdateCommitIndex sets the commit index if the new index is higher.
// Acquires a lock for thread safety. Returns true if the index was updated.
func (sm *stateManager) UpdateCommitIndex(newCommitIndex types.Index) bool {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	return sm.UpdateCommitIndexUnsafe(newCommitIndex)
}

// UpdateCommitIndexUnsafe sets the commit index if the new index is higher.
// Does not acquire a lock; caller must hold sm.mu.Lock(). Returns true if the index was updated.
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

// GetLastAppliedUnsafe returns the index of the last applied log entry without acquiring a lock.
// Assumes the caller holds sm.mu.RLock()
func (sm *stateManager) GetLastAppliedUnsafe() types.Index {
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

// startElectionLocked increments the term and votes for self.
// Must be called with sm.mu.Lock() held.
func (sm *stateManager) startElectionLocked() {
	newTerm := sm.currentTerm + 1
	sm.applyPersistentStateLocked(newTerm, sm.id)
	sm.setRoleAndLeaderLocked(types.RoleCandidate, unknownNodeID, newTerm)
}

// applyPersistentStateLocked updates only the persistent fields (term, votedFor) in memory.
// Must be called with sm.mu.Lock() held.
func (sm *stateManager) applyPersistentStateLocked(term types.Term, votedFor types.NodeID) {
	sm.currentTerm = term
	sm.votedFor = votedFor
}

// setRoleAndLeaderLocked updates the role and leaderID, logging the change and notifying listeners.
// Must be called with sm.mu.Lock() held.
func (sm *stateManager) setRoleAndLeaderLocked(
	role types.NodeRole,
	leaderID types.NodeID,
	term types.Term,
) {
	previousRole := sm.role
	previousLeader := sm.leaderID

	sm.role = role
	sm.leaderID = leaderID

	// Update last known leader for client forwarding
	if leaderID != unknownNodeID {
		sm.lastKnownLeaderID = leaderID
	}

	// Log the role change
	sm.logger.Infow("Role changed",
		"new_role", role.String(),
		"term", term,
		"leader", leaderID,
		"previous_role", previousRole.String(),
		"previous_leader", previousLeader)

	// Notify about leader changes
	if leaderID != previousLeader {
		sm.logger.Infow("Notified leader change",
			"new_leader", leaderID,
			"term", term)

		// Non-blocking notification
		select {
		case sm.leaderChangeCh <- leaderID:
		default:
			sm.logger.Warnw("Leader change notification channel full, notification dropped",
				"new_leader", leaderID, "term", term)
		}
	}

	// Update metrics
	sm.metrics.ObserveRoleChange(role, previousRole, term)
	if role == types.RoleLeader {
		sm.metrics.ObserveLeaderChange(leaderID, term)
	}
}

// persistState saves the given term and votedFor to stable storage.
func (sm *stateManager) persistState(
	ctx context.Context,
	term types.Term,
	votedFor types.NodeID,
) error {
	persistCtx, cancel := context.WithTimeout(ctx, 1*time.Second)
	defer cancel()

	state := types.PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
	}

	return sm.storage.SaveState(persistCtx, state)
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
