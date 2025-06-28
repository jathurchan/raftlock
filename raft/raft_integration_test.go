package raft

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// TestRaftIntegration is the main entry point for Raft integration tests.
func TestRaftIntegration(t *testing.T) {
	// Test suite for a 3-node cluster with basic functionality.
	t.Run("Basic-3-Nodes", func(t *testing.T) {
		cluster := NewTestCluster(t, 3, defaultTestConfig())
		defer cluster.Stop()

		t.Run("LeaderElection", func(t *testing.T) { testBasicElection(t, cluster) })
		t.Run("CommandReplication", func(t *testing.T) { testCommandReplication(t, cluster) })
	})

	// Test suite for a 5-node cluster focusing on fault tolerance.
	t.Run("FaultTolerance-5-Nodes", func(t *testing.T) {
		cluster := NewTestCluster(t, 5, defaultTestConfig())
		defer cluster.Stop()

		t.Run("LeaderFailureRecovery", func(t *testing.T) { testLeaderFailureRecovery(t, cluster) })
	})

	// Test suite for a 3-node cluster focusing on snapshotting and log compaction.
	t.Run("Snapshots-3-Nodes", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Options.SnapshotThreshold = 10
		cfg.Options.LogCompactionMinEntries = 5
		cluster := NewTestCluster(t, 3, cfg)
		defer cluster.Stop()

		t.Run("BasicSnapshotting", func(t *testing.T) { testBasicSnapshotting(t, cluster) })
	})
}

// TestCluster provides a controlled environment for testing a Raft cluster.
type TestCluster struct {
	t      *testing.T
	nodes  map[types.NodeID]*TestNode
	config Config
	clock  *mockClock
	mu     sync.RWMutex
}

// TestNode represents a single node in the test cluster.
type TestNode struct {
	id         types.NodeID
	raft       Raft
	applier    *TestApplier
	networkMgr *gRPCNetworkManager
	isShutdown *atomic.Bool
	cancel     context.CancelFunc
	cancelMu   sync.Mutex
}

// testBasicElection verifies that a leader is elected in a stable cluster.
func testBasicElection(t *testing.T, cluster *TestCluster) {
	t.Helper()

	// Advance the mock clock to trigger an election.
	cluster.tick(cluster.config.Options.ElectionTickCount * 2)
	time.Sleep(100 * time.Millisecond) // Allow time for network communication.

	// The issue: After pre-vote succeeds, there's an election delay (up to 1.238s based on logs)
	// We need to continuously advance the mock clock until a leader is elected
	// Since NominalTickInterval = 100ms, we need ~13 ticks for 1.3s delay

	leaderElected := false
	maxAttempts := 50 // Prevent infinite loop

	for i := 0; i < maxAttempts && !leaderElected; i++ {
		// Advance the clock significantly to handle election delays
		cluster.tick(15)                  // 15 * 100ms = 1.5s advance
		time.Sleep(50 * time.Millisecond) // Allow for processing

		// Check if a leader has been elected
		cluster.mu.RLock()
		for _, node := range cluster.nodes {
			if !node.isShutdown.Load() {
				_, isLeader := node.raft.GetState()
				if isLeader {
					leaderElected = true
					break
				}
			}
		}
		cluster.mu.RUnlock()

		if leaderElected {
			break
		}

		// Log progress every few attempts
		if i%5 == 0 {
			t.Logf("Attempt %d: Still waiting for leader election...", i+1)
		}
	}

	// Now use WaitForLeader with a shorter timeout since we've been trying
	leader := cluster.WaitForLeader(t, 5*time.Second)
	testutil.AssertTrue(t, leader != "", "A leader should be elected")

	leaders := cluster.countLeaders()
	testutil.AssertEqual(t, 1, leaders, "There should be exactly one leader")
}

// testCommandReplication verifies that a command is proposed and replicated to all nodes.
func testCommandReplication(t *testing.T, cluster *TestCluster) {
	t.Helper()

	// Trigger an election and wait for a leader.
	cluster.tick(cluster.config.Options.ElectionTickCount * 2)
	time.Sleep(100 * time.Millisecond)
	leaderID := cluster.WaitForLeader(t, 15*time.Second)
	leader := cluster.GetNode(leaderID)

	// Propose a command to the leader.
	cmd := []byte("test-command")
	index, term, isLeader, err := leader.raft.Propose(context.Background(), cmd)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, isLeader, "Propose should succeed on the leader")
	testutil.AssertEqual(t, types.Index(1), index)
	testutil.AssertEqual(t, types.Term(1), term)

	// Advance the clock to allow for replication.
	cluster.tick(cluster.config.Options.HeartbeatTickCount * 3)
	time.Sleep(50 * time.Millisecond)

	// Wait for the command to be applied on all nodes.
	cluster.WaitForApplied(t, 1, 10*time.Second)
	cluster.CheckStateConsistency(t, 1)
}

// testLeaderFailureRecovery verifies that the cluster can elect a new leader after the current leader fails.
func testLeaderFailureRecovery(t *testing.T, cluster *TestCluster) {
	t.Helper()

	// Elect a leader and replicate a command.
	cluster.tick(cluster.config.Options.ElectionTickCount * 3)
	time.Sleep(200 * time.Millisecond)
	oldLeaderID := cluster.WaitForLeader(t, 20*time.Second)
	t.Logf("Initial leader elected: %s", oldLeaderID)
	_, _, _, err := cluster.GetNode(oldLeaderID).raft.Propose(context.Background(), []byte("cmd-before-failure"))
	testutil.AssertNoError(t, err)
	cluster.tick(cluster.config.Options.HeartbeatTickCount * 5)
	time.Sleep(100 * time.Millisecond)
	cluster.WaitForApplied(t, 1, 15*time.Second)

	// Stop the leader.
	cluster.StopNode(oldLeaderID)
	t.Logf("Stopped old leader: %s", oldLeaderID)
	time.Sleep(200 * time.Millisecond)

	// Trigger a new election.
	cluster.tick(cluster.config.Options.ElectionTickCount * 10)
	time.Sleep(200 * time.Millisecond)

	// Wait for a new leader to be elected.
	newLeaderID := cluster.WaitForLeader(t, 45*time.Second)
	testutil.AssertTrue(t, newLeaderID != oldLeaderID, "A new leader should be elected")
	testutil.AssertTrue(t, newLeaderID != "", "A new leader should be elected")
	t.Logf("New leader elected: %s", newLeaderID)

	// Propose a command to the new leader.
	_, _, _, err = cluster.GetNode(newLeaderID).raft.Propose(context.Background(), []byte("cmd-after-failure"))
	testutil.AssertNoError(t, err)
	cluster.tick(cluster.config.Options.HeartbeatTickCount * 5)
	time.Sleep(100 * time.Millisecond)

	// Verify that all active nodes have applied both commands.
	cluster.WaitForAppliedSubset(t, 2, 20*time.Second, cluster.GetActiveNodeIDs())
	cluster.CheckStateConsistency(t, 2)
}

// testBasicSnapshotting verifies that a snapshot is created when the log size exceeds the threshold.
func testBasicSnapshotting(t *testing.T, cluster *TestCluster) {
	t.Helper()
	cluster.tick(cluster.config.Options.ElectionTickCount * 3)
	time.Sleep(100 * time.Millisecond)
	leaderID := cluster.WaitForLeader(t, 15*time.Second)

	// Propose enough commands to trigger a snapshot.
	threshold := cluster.config.Options.SnapshotThreshold
	for i := 0; i < threshold+1; i++ {
		_, _, _, err := cluster.GetNode(leaderID).raft.Propose(context.Background(), []byte(fmt.Sprintf("cmd-%d", i+1)))
		testutil.AssertNoError(t, err)
		cluster.tick(2)
	}

	cluster.tick(cluster.config.Options.HeartbeatTickCount * 5)
	time.Sleep(100 * time.Millisecond)

	// Wait for all commands to be applied and a snapshot to be created.
	cluster.WaitForApplied(t, threshold+1, 15*time.Second)
	cluster.tick(cluster.config.Options.ElectionTickCount)
	leader := cluster.GetNode(leaderID)
	status := leader.raft.Status()
	testutil.AssertTrue(t, status.SnapshotIndex > 0, "A snapshot should have been created")
	t.Logf("Snapshot created at index %d", status.SnapshotIndex)

	cluster.CheckStateConsistency(t, threshold+1)
}

// NewTestCluster creates a new test cluster with the specified size and configuration.
func NewTestCluster(t *testing.T, size int, cfg Config) *TestCluster {
	t.Helper()
	cluster := &TestCluster{
		t:      t,
		nodes:  make(map[types.NodeID]*TestNode),
		config: cfg,
		clock:  newMockClock(),
	}

	peers := make(map[types.NodeID]PeerConfig)
	listeners := make(map[types.NodeID]net.Listener)
	for i := 1; i <= size; i++ {
		id := types.NodeID(fmt.Sprintf("node%d", i))
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		testutil.AssertNoError(t, err)
		peers[id] = PeerConfig{ID: id, Address: listener.Addr().String()}
		listeners[id] = listener
	}
	t.Cleanup(func() {
		for _, l := range listeners {
			l.Close()
		}
	})

	for id, peerCfg := range peers {
		nodeCfg := cfg
		nodeCfg.ID = id
		nodeCfg.Peers = peers

		applier := NewTestApplier()
		storage := NewMemoryStorage()
		isShutdown := &atomic.Bool{}

		raftNode, err := NewRaftBuilder().
			WithConfig(nodeCfg).
			WithStorage(storage).
			WithApplier(applier).
			WithLogger(logger.NewStdLogger("debug")).
			WithMetrics(NewNoOpMetrics()).
			WithClock(cluster.clock).
			WithRand(NewStandardRand()).
			Build()
		testutil.AssertNoError(t, err)

		networkMgr, err := NewGRPCNetworkManager(
			id,
			peerCfg.Address,
			peers,
			raftNode, // Corrected: raftNode implements RPCHandler
			isShutdown,
			logger.NewStdLogger("debug"),
			NewNoOpMetrics(),
			cluster.clock,
			DefaultGRPCNetworkManagerOptions(),
		)
		testutil.AssertNoError(t, err)
		networkMgr.listener = listeners[id]
		err = networkMgr.Start()
		testutil.AssertNoError(t, err)
		raftNode.SetNetworkManager(networkMgr)

		_, cancel := context.WithCancel(context.Background())
		go func(node Raft, nodeID types.NodeID) {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Raft node %s panicked: %v", nodeID, r)
				}
			}()
			err := node.Start()
			if err != nil && !isShutdown.Load() {
				t.Logf("Raft node %s stopped with error: %v", nodeID, err)
			}
		}(raftNode, id)

		cluster.nodes[id] = &TestNode{
			id:         id,
			raft:       raftNode,
			applier:    applier,
			networkMgr: networkMgr,
			isShutdown: isShutdown,
			cancel:     cancel,
		}
	}

	time.Sleep(500 * time.Millisecond)
	t.Logf("All %d Raft nodes in the cluster have started their main loops.", size)
	t.Logf("All %d network managers have started.", size)
	return cluster
}

// tick advances the mock clock for all nodes in the cluster.
func (c *TestCluster) tick(numTicks int) {
	for i := 0; i < numTicks; i++ {
		c.clock.advanceTime(NominalTickInterval)

		var activeNodes []*TestNode
		c.mu.RLock()
		for _, node := range c.nodes {
			if !node.isShutdown.Load() {
				activeNodes = append(activeNodes, node)
			}
		}
		c.mu.RUnlock()

		for _, node := range activeNodes {
			node.raft.Tick(context.Background())
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Stop stops all nodes in the cluster.
func (c *TestCluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, node := range c.nodes {
		node.cancelMu.Lock()
		if node.cancel != nil {
			node.cancel()
			node.cancel = nil
		}
		node.cancelMu.Unlock()
		node.isShutdown.Store(true)

		if node.networkMgr != nil {
			err := node.networkMgr.Stop()
			if err != nil {
				c.t.Logf("Error stopping network manager for %s: %v", node.id, err)
			}
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		err := node.raft.Stop(ctx)
		if err != nil {
			c.t.Logf("Error stopping node %s: %v", node.id, err)
		}
	}
}

// StopNode stops a specific node in the cluster.
func (c *TestCluster) StopNode(id types.NodeID) {
	c.mu.Lock()
	node, exists := c.nodes[id]
	c.mu.Unlock()

	if !exists {
		c.t.Logf("Node %s not found for stopping", id)
		return
	}

	node.isShutdown.Store(true)
	node.cancelMu.Lock()
	if node.cancel != nil {
		node.cancel()
		node.cancel = nil
	}
	node.cancelMu.Unlock()

	if node.networkMgr != nil {
		err := node.networkMgr.Stop()
		if err != nil {
			c.t.Logf("Error stopping network manager for %s: %v", node.id, err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := node.raft.Stop(ctx)
	if err != nil {
		c.t.Logf("Error stopping node %s: %v", node.id, err)
	}
}

// GetNode returns the TestNode with the specified ID.
func (c *TestCluster) GetNode(id types.NodeID) *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, ok := c.nodes[id]
	testutil.AssertTrue(c.t, ok, "Node %s not found", id)
	return node
}

// GetActiveNodeIDs returns the IDs of all active (not shut down) nodes in the cluster.
func (c *TestCluster) GetActiveNodeIDs() []types.NodeID {
	var ids []types.NodeID
	c.mu.RLock()
	defer c.mu.RUnlock()
	for id, node := range c.nodes {
		if !node.isShutdown.Load() {
			ids = append(ids, id)
		}
	}
	return ids
}

// WaitForLeader waits for a leader to be elected and returns its ID.
func (c *TestCluster) WaitForLeader(t *testing.T, timeout time.Duration) types.NodeID {
	t.Helper()
	deadline := time.Now().Add(timeout)
	checkInterval := 50 * time.Millisecond // Reduced from 100ms
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for {
		if time.Now().After(deadline) {
			c.mu.RLock()
			for id, node := range c.nodes {
				if !node.isShutdown.Load() {
					term, isLeader := node.raft.GetState()
					status := node.raft.Status()
					t.Logf("Node %s: term=%d, isLeader=%t, role=%s, leaderID=%s", id, term, isLeader, status.Role, status.LeaderID)
				}
			}
			c.mu.RUnlock()
			t.Fatalf("Timeout waiting for leader after %v", timeout)
			return ""
		}

		select {
		case <-ticker.C:
			// More aggressive clock advancement to handle election delays
			// Each election delay can be up to 1.3s, so advance by 1.5s worth of ticks
			ticksFor1_5Seconds := int(1500 / int(NominalTickInterval.Milliseconds())) // ~15 ticks
			c.tick(ticksFor1_5Seconds)

			c.mu.RLock()
			for _, node := range c.nodes {
				if !node.isShutdown.Load() {
					_, isLeader := node.raft.GetState()
					if isLeader {
						nodeID := node.id
						c.mu.RUnlock()
						return nodeID
					}
				}
			}
			c.mu.RUnlock()
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// countLeaders returns the number of leaders in the cluster.
func (c *TestCluster) countLeaders() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	count := 0
	for _, node := range c.nodes {
		if !node.isShutdown.Load() {
			_, isLeader := node.raft.GetState()
			if isLeader {
				count++
			}
		}
	}
	return count
}

// WaitForApplied waits for the specified number of entries to be applied on all active nodes.
func (c *TestCluster) WaitForApplied(t *testing.T, expectedCount int, timeout time.Duration) {
	t.Helper()
	c.WaitForAppliedSubset(t, expectedCount, timeout, c.GetActiveNodeIDs())
}

// WaitForAppliedSubset waits for the specified number of entries to be applied on a subset of nodes.
func (c *TestCluster) WaitForAppliedSubset(t *testing.T, expectedCount int, timeout time.Duration, nodeIDs []types.NodeID) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		if time.Now().After(deadline) {
			for _, id := range nodeIDs {
				node := c.GetNode(id)
				if !node.isShutdown.Load() {
					count := node.applier.GetAppliedCount()
					t.Logf("Node %s has applied %d entries (expected %d)", id, count, expectedCount)
				}
			}
			t.Fatalf("Timeout waiting for %d entries to be applied across specified nodes", expectedCount)
		}

		select {
		case <-ticker.C:
			c.tick(2)
			allApplied := true
			for _, id := range nodeIDs {
				node := c.GetNode(id)
				if !node.isShutdown.Load() && node.applier.GetAppliedCount() < expectedCount {
					allApplied = false
					break
				}
			}
			if allApplied {
				return
			}
		default:
			time.Sleep(10 * time.Millisecond)
		}
	}
}

// CheckStateConsistency verifies that all active nodes have the same applied entries.
func (c *TestCluster) CheckStateConsistency(t *testing.T, expectedCount int) {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()

	var referenceEntries []types.ApplyMsg
	for id, node := range c.nodes {
		if !node.isShutdown.Load() {
			referenceEntries = node.applier.GetAppliedEntries()
			if len(referenceEntries) != expectedCount {
				t.Fatalf("Node %s has %d applied entries, expected %d", id, len(referenceEntries), expectedCount)
			}
			break
		}
	}

	for id, node := range c.nodes {
		if !node.isShutdown.Load() {
			entries := node.applier.GetAppliedEntries()
			testutil.AssertEqual(t, len(referenceEntries), len(entries), "Applied entry count mismatch for node %s", id)
			for i, entry := range entries {
				refEntry := referenceEntries[i]
				testutil.AssertEqual(t, refEntry.CommandIndex, entry.CommandIndex, "Index mismatch at %d for node %s", i, id)
				testutil.AssertEqual(t, refEntry.CommandTerm, entry.CommandTerm, "Term mismatch at %d for node %s", i, id)
			}
		}
	}
}

// WaitForCondition waits for a condition to be true, with a timeout.
func (c *TestCluster) WaitForCondition(timeout, interval time.Duration, condition func() bool, description string) {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
		c.tick(1)
	}
	c.t.Fatalf("Condition not met within %v: %s", timeout, description)
}

// TestApplier is a simple in-memory implementation of the Applier interface for testing.
type TestApplier struct {
	mu             sync.RWMutex
	appliedEntries []types.ApplyMsg
}

// NewTestApplier creates a new TestApplier.
func NewTestApplier() *TestApplier {
	return &TestApplier{appliedEntries: make([]types.ApplyMsg, 0)}
}

// Apply applies a command to the in-memory store.
func (a *TestApplier) Apply(ctx context.Context, index types.Index, command []byte) (any, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	msg := types.ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: index,
		CommandTerm:  0,
	}
	a.appliedEntries = append(a.appliedEntries, msg)
	return nil, nil
}

// Snapshot creates a snapshot of the in-memory store.
func (a *TestApplier) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.appliedEntries) == 0 {
		return 0, nil, nil
	}
	return a.appliedEntries[len(a.appliedEntries)-1].CommandIndex, []byte("snapshot"), nil
}

// RestoreSnapshot restores the in-memory store from a snapshot.
func (a *TestApplier) RestoreSnapshot(ctx context.Context, lastIncludedIndex types.Index, lastIncludedTerm types.Term, data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appliedEntries = make([]types.ApplyMsg, 0)
	return nil
}

// GetAppliedEntries returns a copy of the applied entries.
func (a *TestApplier) GetAppliedEntries() []types.ApplyMsg {
	a.mu.RLock()
	defer a.mu.RUnlock()
	entries := make([]types.ApplyMsg, len(a.appliedEntries))
	copy(entries, a.appliedEntries)
	return entries
}

// GetAppliedCount returns the number of applied entries.
func (a *TestApplier) GetAppliedCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.appliedEntries)
}

// defaultTestConfig returns a default configuration for testing.
func defaultTestConfig() Config {
	return Config{
		Options: Options{
			ElectionTickCount:           30,
			HeartbeatTickCount:          3,
			ElectionRandomizationFactor: 2.0,
			MaxLogEntriesPerRequest:     100,
			SnapshotThreshold:           1000,
			LogCompactionMinEntries:     500,
		},
		FeatureFlags: FeatureFlags{
			EnableReadIndex:   true,
			EnableLeaderLease: true,
			PreVoteEnabled:    false,
		}.WithExplicitFlags(),
	}
}

// MemoryStorage is an in-memory implementation of the Storage interface for testing.
type MemoryStorage struct {
	mu           sync.RWMutex
	state        types.PersistentState
	log          []types.LogEntry
	snapshotMeta types.SnapshotMetadata
	snapshotData []byte
}

// NewMemoryStorage creates a new MemoryStorage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		log: make([]types.LogEntry, 0),
	}
}

// SaveState saves the persistent state to memory.
func (s *MemoryStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}

// LoadState loads the persistent state from memory.
func (s *MemoryStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

// AppendLogEntries appends log entries to the in-memory log.
func (s *MemoryStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return nil
}

// GetLogEntries retrieves log entries from the in-memory log.
func (s *MemoryStorage) GetLogEntries(ctx context.Context, start, end types.Index) ([]types.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if start > end {
		return nil, storage.ErrInvalidLogRange
	}
	firstIndex := s.firstIndexLocked()
	if start < firstIndex {
		return nil, storage.ErrInvalidLogRange
	}
	if start > s.lastIndexLocked()+1 {
		return []types.LogEntry{}, nil
	}
	startOffset := int(start - firstIndex)
	endOffset := int(end - firstIndex)
	if endOffset > len(s.log) {
		endOffset = len(s.log)
	}
	if startOffset >= endOffset {
		return []types.LogEntry{}, nil
	}
	return s.log[startOffset:endOffset], nil
}

// GetLogEntry retrieves a single log entry.
func (s *MemoryStorage) GetLogEntry(ctx context.Context, index types.Index) (types.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index == 0 {
		return types.LogEntry{}, storage.ErrEntryNotFound
	}
	firstIndex := s.firstIndexLocked()
	if index < firstIndex || index > s.lastIndexLocked() {
		return types.LogEntry{}, storage.ErrEntryNotFound
	}
	offset := int(index - firstIndex)
	return s.log[offset], nil
}

// TruncateLogPrefix truncates the prefix of the in-memory log.
func (s *MemoryStorage) TruncateLogPrefix(ctx context.Context, upToIndex types.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	firstIndex := s.firstIndexLocked()
	if upToIndex < firstIndex {
		return nil
	}
	if upToIndex > s.lastIndexLocked() {
		s.log = make([]types.LogEntry, 0)
		return nil
	}
	offset := int(upToIndex - firstIndex + 1)
	s.log = s.log[offset:]
	return nil
}

// TruncateLogSuffix truncates the suffix of the in-memory log.
func (s *MemoryStorage) TruncateLogSuffix(ctx context.Context, fromIndex types.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	firstIndex := s.firstIndexLocked()
	if fromIndex <= firstIndex {
		s.log = make([]types.LogEntry, 0)
		return nil
	}
	if fromIndex > s.lastIndexLocked()+1 {
		return nil
	}
	newLength := int(fromIndex - firstIndex)
	if newLength < 0 {
		newLength = 0
	}
	s.log = s.log[:newLength]
	return nil
}

// FirstLogIndex returns the first index of the log.
func (s *MemoryStorage) FirstLogIndex() types.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked()
}

// LastLogIndex returns the last index of the log.
func (s *MemoryStorage) LastLogIndex() types.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

// Close closes the storage.
func (s *MemoryStorage) Close() error {
	return nil
}

// ResetMetrics resets the storage metrics.
func (s *MemoryStorage) ResetMetrics() {}

// GetMetrics returns the storage metrics.
func (s *MemoryStorage) GetMetrics() map[string]uint64 {
	return nil
}

// GetMetricsSummary returns a summary of the storage metrics.
func (s *MemoryStorage) GetMetricsSummary() string {
	return "metrics not implemented for MemoryStorage"
}

// SaveSnapshot saves a snapshot to memory.
func (s *MemoryStorage) SaveSnapshot(ctx context.Context, metadata types.SnapshotMetadata, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotMeta = metadata
	s.snapshotData = make([]byte, len(data))
	copy(s.snapshotData, data)
	return nil
}

// LoadSnapshot loads a snapshot from memory.
func (s *MemoryStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshotData == nil {
		return types.SnapshotMetadata{}, nil, storage.ErrNoSnapshot
	}
	data := make([]byte, len(s.snapshotData))
	copy(data, s.snapshotData)
	return s.snapshotMeta, data, nil
}

// firstIndexLocked returns the first index of the log.
func (s *MemoryStorage) firstIndexLocked() types.Index {
	if len(s.log) == 0 {
		return s.snapshotMeta.LastIncludedIndex + 1
	}
	return s.log[0].Index
}

// lastIndexLocked returns the last index of the log.
func (s *MemoryStorage) lastIndexLocked() types.Index {
	if len(s.log) == 0 {
		return s.snapshotMeta.LastIncludedIndex
	}
	return s.log[len(s.log)-1].Index
}

// TestNetworkManager is a mock implementation of the NetworkManager interface for testing.
type TestNetworkManager struct {
	mu       sync.RWMutex
	id       types.NodeID
	peers    map[types.NodeID]string
	handlers map[types.NodeID]RPCHandler
	cluster  *TestCluster
}

// NewTestNetworkManager creates a new TestNetworkManager.
func NewTestNetworkManager(id types.NodeID, cluster *TestCluster) *TestNetworkManager {
	return &TestNetworkManager{
		id:       id,
		peers:    make(map[types.NodeID]string),
		handlers: make(map[types.NodeID]RPCHandler),
		cluster:  cluster,
	}
}

// Start starts the test network manager.
func (tnm *TestNetworkManager) Start() error { return nil }

// Stop stops the test network manager.
func (tnm *TestNetworkManager) Stop() error { return nil }

// SendRequestVote sends a RequestVote RPC to a target peer.
func (tnm *TestNetworkManager) SendRequestVote(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	tnm.mu.RLock()
	handler, exists := tnm.handlers[target]
	tnm.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no handler for target %s", target)
	}
	return handler.RequestVote(ctx, args)
}

// SendAppendEntries sends an AppendEntries RPC to a target peer.
func (tnm *TestNetworkManager) SendAppendEntries(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	tnm.mu.RLock()
	handler, exists := tnm.handlers[target]
	tnm.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no handler for target %s", target)
	}
	return handler.AppendEntries(ctx, args)
}

// SendInstallSnapshot sends an InstallSnapshot RPC to a target peer.
func (tnm *TestNetworkManager) SendInstallSnapshot(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	tnm.mu.RLock()
	handler, exists := tnm.handlers[target]
	tnm.mu.RUnlock()
	if !exists {
		return nil, fmt.Errorf("no handler for target %s", target)
	}
	return handler.InstallSnapshot(ctx, args)
}

// PeerStatus returns the status of a peer.
func (tnm *TestNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	tnm.mu.RLock()
	defer tnm.mu.RUnlock()
	_, connected := tnm.handlers[peer]
	return types.PeerConnectionStatus{Connected: connected, LastActive: time.Now(), PendingRPCs: 0}, nil
}

// LocalAddr returns the local address of the test network manager.
func (tnm *TestNetworkManager) LocalAddr() string { return fmt.Sprintf("test://%s", tnm.id) }

// ResetConnection resets the connection to a peer.
func (tnm *TestNetworkManager) ResetConnection(ctx context.Context, peerID types.NodeID) error {
	return nil
}

// RegisterHandler registers an RPC handler for a peer.
func (tnm *TestNetworkManager) RegisterHandler(id types.NodeID, handler RPCHandler) {
	tnm.mu.Lock()
	defer tnm.mu.Unlock()
	tnm.handlers[id] = handler
}
