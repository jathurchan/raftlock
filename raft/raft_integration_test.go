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

// This test suite sets up a multi-node Raft cluster and runs various integration
// scenarios against it.
func TestRaftIntegration(t *testing.T) {
	t.Run("Basic-3-Nodes", func(t *testing.T) {
		cluster := NewTestCluster(t, 3, defaultTestConfig())
		defer cluster.Stop()

		t.Run("LeaderElection", func(t *testing.T) { testBasicElection(t, cluster) })
		t.Run("CommandReplication", func(t *testing.T) { testCommandReplication(t, cluster) })
	})

	t.Run("FaultTolerance-5-Nodes", func(t *testing.T) {
		cluster := NewTestCluster(t, 5, defaultTestConfig())
		defer cluster.Stop()

		t.Run("LeaderFailureRecovery", func(t *testing.T) { testLeaderFailureRecovery(t, cluster) })
	})

	t.Run("Snapshots-3-Nodes", func(t *testing.T) {
		cfg := defaultTestConfig()
		cfg.Options.SnapshotThreshold = 10
		cfg.Options.LogCompactionMinEntries = 5
		cluster := NewTestCluster(t, 3, cfg)
		defer cluster.Stop()

		t.Run("BasicSnapshotting", func(t *testing.T) { testBasicSnapshotting(t, cluster) })
	})
}

type TestCluster struct {
	t      *testing.T
	nodes  map[types.NodeID]*TestNode
	config Config
	clock  *mockClock
	mu     sync.RWMutex
}

type TestNode struct {
	id         types.NodeID
	raft       Raft
	applier    *TestApplier
	networkMgr *gRPCNetworkManager
	isShutdown *atomic.Bool
	cancel     context.CancelFunc
}

func testBasicElection(t *testing.T, cluster *TestCluster) {
	t.Helper()
	cluster.tick(cluster.config.Options.ElectionTickCount + 5)
	leader := cluster.WaitForLeader(t, 10*time.Second)
	testutil.AssertTrue(t, leader != "", "A leader should be elected")

	leaders := cluster.countLeaders()
	testutil.AssertEqual(t, 1, leaders, "There should be exactly one leader")
}

func testCommandReplication(t *testing.T, cluster *TestCluster) {
	t.Helper()
	cluster.tick(cluster.config.Options.ElectionTickCount + 5)
	leaderID := cluster.WaitForLeader(t, 10*time.Second)
	leader := cluster.GetNode(leaderID)

	cmd := []byte("test-command")
	index, term, isLeader, err := leader.raft.Propose(context.Background(), cmd)
	testutil.AssertNoError(t, err)
	testutil.AssertTrue(t, isLeader, "Propose should succeed on leader")
	testutil.AssertEqual(t, types.Index(1), index)
	testutil.AssertEqual(t, types.Term(1), term)

	cluster.tick(cluster.config.Options.HeartbeatTickCount * 2)
	cluster.WaitForApplied(t, 1, 5*time.Second)
	cluster.CheckStateConsistency(t, 1)
}

func testLeaderFailureRecovery(t *testing.T, cluster *TestCluster) {
	t.Helper()
	cluster.tick(cluster.config.Options.ElectionTickCount * 2)
	oldLeaderID := cluster.WaitForLeader(t, 10*time.Second)

	_, _, _, err := cluster.GetNode(
		oldLeaderID,
	).raft.Propose(
		context.Background(),
		[]byte("cmd-before-failure"),
	)
	testutil.AssertNoError(t, err)
	cluster.tick(cluster.config.Options.HeartbeatTickCount * 2)
	cluster.WaitForApplied(t, 1, 5*time.Second)

	cluster.StopNode(oldLeaderID)
	t.Logf("Stopped old leader: %s", oldLeaderID)

	cluster.tick(cluster.config.Options.ElectionTickCount*2 + 5)

	newLeaderID := cluster.WaitForLeader(t, 15*time.Second)
	testutil.AssertTrue(t, newLeaderID != oldLeaderID, "A new leader should be elected")
	t.Logf("New leader elected: %s", newLeaderID)

	_, _, _, err = cluster.GetNode(
		newLeaderID,
	).raft.Propose(
		context.Background(),
		[]byte("cmd-after-failure"),
	)
	testutil.AssertNoError(t, err)
	cluster.tick(cluster.config.Options.HeartbeatTickCount * 2)

	cluster.WaitForAppliedSubset(t, 2, 10*time.Second, cluster.GetActiveNodeIDs())
	cluster.CheckStateConsistency(t, 2)
}

func testBasicSnapshotting(t *testing.T, cluster *TestCluster) {
	t.Helper()
	cluster.tick(cluster.config.Options.ElectionTickCount * 2)
	leaderID := cluster.WaitForLeader(t, 10*time.Second)

	threshold := cluster.config.Options.SnapshotThreshold
	for i := 0; i < threshold+1; i++ {
		_, _, _, err := cluster.GetNode(
			leaderID,
		).raft.Propose(
			context.Background(),
			[]byte(fmt.Sprintf("cmd-%d", i+1)),
		)
		testutil.AssertNoError(t, err)
	}

	cluster.tick(cluster.config.Options.HeartbeatTickCount * 2)
	cluster.WaitForApplied(t, threshold+1, 10*time.Second)
	cluster.tick(cluster.config.Options.ElectionTickCount)

	leader := cluster.GetNode(leaderID)
	status := leader.raft.Status()
	testutil.AssertTrue(t, status.SnapshotIndex > 0, "Snapshot should have been created")
	t.Logf("Snapshot created at index %d", status.SnapshotIndex)

	cluster.CheckStateConsistency(t, threshold+1)
}

func NewTestCluster(t *testing.T, size int, cfg Config) *TestCluster {
	t.Helper()
	cluster := &TestCluster{
		t:      t,
		nodes:  make(map[types.NodeID]*TestNode),
		config: cfg,
		clock:  newMockClock(),
	}

	// Step 1: Pre-allocate ports and create peer configurations
	peers := make(map[types.NodeID]PeerConfig)
	for i := 1; i <= size; i++ {
		id := types.NodeID(fmt.Sprintf("node%d", i))
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		testutil.AssertNoError(t, err, "Failed to listen on a port for %s", id)
		peers[id] = PeerConfig{ID: id, Address: listener.Addr().String()}
		listener.Close()
	}

	// Step 2: Create all Raft node instances first
	for id := range peers {
		nodeCfg := cfg
		nodeCfg.ID = id
		nodeCfg.Peers = peers
		cluster.createRaftNodeOnly(id, nodeCfg)
	}

	// Step 3: Create and wire up network managers for each node
	for id := range peers {
		node := cluster.nodes[id]
		localAddr := peers[id].Address
		cluster.createAndWireNetworkManager(node, localAddr, peers)
	}

	// Step 4: Start all nodes now that they are fully wired
	for _, node := range cluster.nodes {
		cluster.startNode(node)
	}

	return cluster
}

func (c *TestCluster) createRaftNodeOnly(id types.NodeID, cfg Config) {
	applier := NewTestApplier()
	storage := NewMemoryStorage()

	raftNode, err := NewRaftBuilder().
		WithConfig(cfg).
		WithStorage(storage).
		WithApplier(applier).
		WithLogger(logger.NewStdLogger("info")).
		WithMetrics(NewNoOpMetrics()).
		WithClock(c.clock).
		WithRand(NewStandardRand()).
		Build()
	testutil.AssertNoError(c.t, err)

	c.nodes[id] = &TestNode{
		id:      id,
		raft:    raftNode,
		applier: applier,
	}
}

func (c *TestCluster) createAndWireNetworkManager(
	node *TestNode,
	localAddr string,
	peers map[types.NodeID]PeerConfig,
) {
	isShutdown := &atomic.Bool{}
	rpcHandler := node.raft

	networkMgr, err := NewGRPCNetworkManager(
		node.id,
		localAddr,
		peers,
		rpcHandler,
		isShutdown,
		logger.NewNoOpLogger(),
		NewNoOpMetrics(),
		c.clock,
		DefaultGRPCNetworkManagerOptions(),
	)
	testutil.AssertNoError(c.t, err, "Failed to create gRPC network manager for %s", node.id)

	node.networkMgr = networkMgr
	node.isShutdown = isShutdown
	node.raft.SetNetworkManager(networkMgr)
}

func (c *TestCluster) startNode(node *TestNode) {
	t := c.t
	t.Helper()
	err := node.raft.Start()
	testutil.AssertNoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	node.cancel = cancel

	go func() {
		applyCh := node.raft.ApplyChannel()
		for {
			select {
			case <-ctx.Done():
				return
			case msg, ok := <-applyCh:
				if !ok {
					return
				}
				if msg.CommandValid {
					node.applier.RecordAppliedMessage(msg)
				}
			}
		}
	}()
}

func (c *TestCluster) tick(numTicks int) {
	for i := 0; i < numTicks; i++ {
		c.clock.advanceTime(NominalTickInterval)
		for _, node := range c.nodes {
			if !node.isShutdown.Load() {
				node.raft.Tick(context.Background())
			}
		}
	}
}

func (c *TestCluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, node := range c.nodes {
		if node.cancel != nil {
			node.cancel()
		}
		node.isShutdown.Store(true)
		err := node.raft.Stop(context.Background())
		if err != nil {
			c.t.Logf("Error stopping node %s: %v", node.id, err)
		}
	}
}

func (c *TestCluster) StopNode(id types.NodeID) {
	node := c.GetNode(id)
	node.isShutdown.Store(true)
	if node.cancel != nil {
		node.cancel()
	}
	err := node.raft.Stop(context.Background())
	if err != nil {
		c.t.Logf("Error stopping node %s: %v", node.id, err)
	}
}

func (c *TestCluster) GetNode(id types.NodeID) *TestNode {
	c.mu.RLock()
	defer c.mu.RUnlock()
	node, ok := c.nodes[id]
	testutil.AssertTrue(c.t, ok, "Node %s not found", id)
	return node
}

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

func (c *TestCluster) WaitForLeader(t *testing.T, timeout time.Duration) types.NodeID {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for leader after %v", timeout)
			return ""
		default:
			for _, node := range c.nodes {
				if !node.isShutdown.Load() {
					_, isLeader := node.raft.GetState()
					if isLeader {
						return node.id
					}
				}
			}
			time.Sleep(100 * time.Millisecond) // prevent busy-looping
		}
	}
}

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

func (c *TestCluster) WaitForApplied(t *testing.T, expectedCount int, timeout time.Duration) {
	t.Helper()
	c.WaitForAppliedSubset(t, expectedCount, timeout, c.GetActiveNodeIDs())
}

func (c *TestCluster) WaitForAppliedSubset(
	t *testing.T,
	expectedCount int,
	timeout time.Duration,
	nodeIDs []types.NodeID,
) {
	t.Helper()
	deadline := time.After(timeout)
	for {
		select {
		case <-deadline:
			t.Fatalf("Timeout waiting for %d entries to be applied across all nodes", expectedCount)
		default:
			allApplied := true
			for _, id := range nodeIDs {
				node := c.GetNode(id)
				if node.applier.GetAppliedCount() < expectedCount {
					allApplied = false
					break
				}
			}
			if allApplied {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func (c *TestCluster) CheckStateConsistency(t *testing.T, expectedCount int) {
	t.Helper()
	c.mu.RLock()
	defer c.mu.RUnlock()

	var referenceEntries []types.ApplyMsg

	for id, node := range c.nodes {
		if !node.isShutdown.Load() {
			referenceEntries = node.applier.GetAppliedEntries()
			if len(referenceEntries) != expectedCount {
				t.Fatalf(
					"Node %s has %d applied entries, expected %d",
					id,
					len(referenceEntries),
					expectedCount,
				)
			}
			break
		}
	}

	for id, node := range c.nodes {
		if !node.isShutdown.Load() {
			entries := node.applier.GetAppliedEntries()
			testutil.AssertEqual(
				t,
				len(referenceEntries),
				len(entries),
				"Applied entry count mismatch for node %s",
				id,
			)
			for i, entry := range entries {
				refEntry := referenceEntries[i]
				testutil.AssertEqual(
					t,
					refEntry.CommandIndex,
					entry.CommandIndex,
					"Index mismatch at %d for node %s",
					i,
					id,
				)
				testutil.AssertEqual(
					t,
					refEntry.CommandTerm,
					entry.CommandTerm,
					"Term mismatch at %d for node %s",
					i,
					id,
				)
			}
		}
	}
}

func (c *TestCluster) WaitForCondition(timeout, interval time.Duration,
	condition func() bool, description string) {
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(interval)
	}

	c.t.Fatalf("Condition not met within %v: %s", timeout, description)
}

type TestApplier struct {
	mu             sync.RWMutex
	appliedEntries []types.ApplyMsg
}

func NewTestApplier() *TestApplier {
	return &TestApplier{appliedEntries: make([]types.ApplyMsg, 0)}
}
func (a *TestApplier) Apply(ctx context.Context, index types.Index, command []byte) (any, error) {
	return nil, nil
}
func (a *TestApplier) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.appliedEntries) == 0 {
		return 0, nil, nil
	}
	return a.appliedEntries[len(a.appliedEntries)-1].CommandIndex, []byte("snapshot"), nil
}

func (a *TestApplier) RestoreSnapshot(
	ctx context.Context,
	lastIncludedIndex types.Index,
	lastIncludedTerm types.Term,
	data []byte,
) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appliedEntries = []types.ApplyMsg{
		{CommandValid: true, CommandIndex: lastIncludedIndex, CommandTerm: lastIncludedTerm},
	}
	return nil
}
func (a *TestApplier) GetAppliedCount() int {
	a.mu.RLock()
	defer a.mu.RUnlock()
	return len(a.appliedEntries)
}
func (a *TestApplier) GetAppliedEntries() []types.ApplyMsg {
	a.mu.RLock()
	defer a.mu.RUnlock()
	result := make([]types.ApplyMsg, len(a.appliedEntries))
	copy(result, a.appliedEntries)
	return result
}
func (a *TestApplier) RecordAppliedMessage(msg types.ApplyMsg) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appliedEntries = append(a.appliedEntries, msg)
}

type NetworkConditions struct {
	mu         sync.RWMutex
	partitions map[types.NodeID]map[types.NodeID]bool
	delays     map[types.NodeID]time.Duration
	dropRates  map[types.NodeID]float64
}

func NewNetworkConditions() *NetworkConditions {
	return &NetworkConditions{
		partitions: make(map[types.NodeID]map[types.NodeID]bool),
		delays:     make(map[types.NodeID]time.Duration),
		dropRates:  make(map[types.NodeID]float64),
	}
}

func (nc *NetworkConditions) CanCommunicate(from, to types.NodeID) bool {
	nc.mu.RLock()
	defer nc.mu.RUnlock()

	if partition, exists := nc.partitions[from]; exists {
		return !partition[to]
	}
	return true
}

func (nc *NetworkConditions) CreatePartition(minority, majority []types.NodeID) {
	nc.mu.Lock()
	defer nc.mu.Unlock()

	// Clear existing partitions
	nc.partitions = make(map[types.NodeID]map[types.NodeID]bool)

	// Create partition between minority and majority
	for _, minNode := range minority {
		if nc.partitions[minNode] == nil {
			nc.partitions[minNode] = make(map[types.NodeID]bool)
		}
		for _, majNode := range majority {
			nc.partitions[minNode][majNode] = true
		}
	}

	for _, majNode := range majority {
		if nc.partitions[majNode] == nil {
			nc.partitions[majNode] = make(map[types.NodeID]bool)
		}
		for _, minNode := range minority {
			nc.partitions[majNode][minNode] = true
		}
	}
}

func (nc *NetworkConditions) HealPartition() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.partitions = make(map[types.NodeID]map[types.NodeID]bool)
}

type TestNetworkManager struct {
	id         types.NodeID
	conditions *NetworkConditions
	handlers   map[types.NodeID]RPCHandler
	mu         sync.RWMutex
}

func NewTestNetworkManager(id types.NodeID, conditions *NetworkConditions) *TestNetworkManager {
	return &TestNetworkManager{
		id:         id,
		conditions: conditions,
		handlers:   make(map[types.NodeID]RPCHandler),
	}
}

func (tnm *TestNetworkManager) Start() error {
	return nil
}

func (tnm *TestNetworkManager) Stop() error {
	return nil
}

func (tnm *TestNetworkManager) SendRequestVote(ctx context.Context, target types.NodeID,
	args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {

	if !tnm.conditions.CanCommunicate(tnm.id, target) {
		return nil, fmt.Errorf("network partition: cannot reach %s", target)
	}

	tnm.mu.RLock()
	handler, exists := tnm.handlers[target]
	tnm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no handler for target %s", target)
	}

	// Simulate network delay
	time.Sleep(time.Millisecond)

	return handler.RequestVote(ctx, args)
}

func (tnm *TestNetworkManager) SendAppendEntries(ctx context.Context, target types.NodeID,
	args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {

	if !tnm.conditions.CanCommunicate(tnm.id, target) {
		return nil, fmt.Errorf("network partition: cannot reach %s", target)
	}

	tnm.mu.RLock()
	handler, exists := tnm.handlers[target]
	tnm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no handler for target %s", target)
	}

	// Simulate network delay
	time.Sleep(time.Millisecond)

	return handler.AppendEntries(ctx, args)
}

func (tnm *TestNetworkManager) SendInstallSnapshot(ctx context.Context, target types.NodeID,
	args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {

	if !tnm.conditions.CanCommunicate(tnm.id, target) {
		return nil, fmt.Errorf("network partition: cannot reach %s", target)
	}

	tnm.mu.RLock()
	handler, exists := tnm.handlers[target]
	tnm.mu.RUnlock()

	if !exists {
		return nil, fmt.Errorf("no handler for target %s", target)
	}

	// Simulate network delay for snapshot transfer
	time.Sleep(5 * time.Millisecond)

	return handler.InstallSnapshot(ctx, args)
}

func (tnm *TestNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	connected := tnm.conditions.CanCommunicate(tnm.id, peer)
	return types.PeerConnectionStatus{
		Connected:   connected,
		LastActive:  time.Now(),
		PendingRPCs: 0,
	}, nil
}

func (tnm *TestNetworkManager) LocalAddr() string {
	return fmt.Sprintf("test://%s", tnm.id)
}

func (tnm *TestNetworkManager) RegisterHandler(id types.NodeID, handler RPCHandler) {
	tnm.mu.Lock()
	defer tnm.mu.Unlock()
	tnm.handlers[id] = handler
}

func defaultTestConfig() Config {
	return Config{
		Options: Options{
			ElectionTickCount:           10,
			HeartbeatTickCount:          2,
			ElectionRandomizationFactor: 0.2,
			MaxLogEntriesPerRequest:     100,
			SnapshotThreshold:           1000,
			LogCompactionMinEntries:     500,
		},
		FeatureFlags: FeatureFlags{
			EnableReadIndex:   true,
			EnableLeaderLease: true,
			PreVoteEnabled:    true,
		}.WithExplicitFlags(),
	}
}

func (nm *gRPCNetworkManager) RegisterRPCHandler(id types.NodeID, handler RPCHandler) {
	nm.rpcHandler = handler
}

type MemoryStorage struct {
	mu           sync.RWMutex
	state        types.PersistentState
	log          []types.LogEntry
	snapshotMeta types.SnapshotMetadata
	snapshotData []byte
}

func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		log: make([]types.LogEntry, 0),
	}
}

func (s *MemoryStorage) SaveState(ctx context.Context, state types.PersistentState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.state = state
	return nil
}

func (s *MemoryStorage) LoadState(ctx context.Context) (types.PersistentState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state, nil
}

func (s *MemoryStorage) AppendLogEntries(ctx context.Context, entries []types.LogEntry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.log = append(s.log, entries...)
	return nil
}

func (s *MemoryStorage) GetLogEntries(
	ctx context.Context,
	start, end types.Index,
) ([]types.LogEntry, error) {
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

func (s *MemoryStorage) GetLogEntry(
	ctx context.Context,
	index types.Index,
) (types.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if index < s.firstIndexLocked() || index > s.lastIndexLocked() {
		return types.LogEntry{}, storage.ErrEntryNotFound
	}
	return s.log[index-s.firstIndexLocked()], nil
}

func (s *MemoryStorage) TruncateLogSuffix(ctx context.Context, index types.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.firstIndexLocked() {
		s.log = []types.LogEntry{}
		return nil
	}
	if index > s.lastIndexLocked()+1 {
		return nil
	}
	s.log = s.log[:index-s.firstIndexLocked()]
	return nil
}

func (s *MemoryStorage) TruncateLogPrefix(ctx context.Context, index types.Index) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if index <= s.firstIndexLocked() {
		return nil
	}
	if index > s.lastIndexLocked()+1 {
		s.log = []types.LogEntry{}
		return nil
	}
	s.log = s.log[index-s.firstIndexLocked():]
	return nil
}

func (s *MemoryStorage) SaveSnapshot(
	ctx context.Context,
	metadata types.SnapshotMetadata,
	data []byte,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.snapshotMeta = metadata
	s.snapshotData = data
	return nil
}

func (s *MemoryStorage) LoadSnapshot(ctx context.Context) (types.SnapshotMetadata, []byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.snapshotMeta.LastIncludedIndex == 0 {
		return types.SnapshotMetadata{}, nil, storage.ErrCorruptedSnapshot
	}
	return s.snapshotMeta, s.snapshotData, nil
}

func (s *MemoryStorage) LastLogIndex() types.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndexLocked()
}

func (s *MemoryStorage) FirstLogIndex() types.Index {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.firstIndexLocked()
}

func (s *MemoryStorage) firstIndexLocked() types.Index {
	if len(s.log) == 0 {
		return s.snapshotMeta.LastIncludedIndex + 1
	}
	return s.log[0].Index
}

func (s *MemoryStorage) lastIndexLocked() types.Index {
	if len(s.log) == 0 {
		return s.snapshotMeta.LastIncludedIndex
	}
	return s.log[len(s.log)-1].Index
}

func (s *MemoryStorage) Close() error                  { return nil }
func (s *MemoryStorage) ResetMetrics()                 {}
func (s *MemoryStorage) GetMetrics() map[string]uint64 { return nil }
func (s *MemoryStorage) GetMetricsSummary() string     { return "" }
