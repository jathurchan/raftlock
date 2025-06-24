package raft

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jathurchan/raftlock/logger"
	"github.com/jathurchan/raftlock/storage"
	"github.com/jathurchan/raftlock/testutil"
	"github.com/jathurchan/raftlock/types"
)

// --- Test Suite Runner ---

func TestRaftIntegration_gRPC_FullSuite(t *testing.T) {
	suite3 := NewTestSuite("3-Node Cluster", 3)
	suite3.AddTest("Basic Leader Election", testBasicLeaderElection)
	suite3.AddTest("Command Replication", testCommandReplication)
	suite3.AddTest("Concurrent Proposals", testConcurrentProposals)
	suite3.Run(t)

	suite5 := NewTestSuite("5-Node Cluster", 5)
	suite5.AddTest("Leader Failure and Recovery", testLeaderFailure)
	suite5.AddTest("Network Partition and Heal", testNetworkPartition)
	suite5.AddTest("Follower Restart and Catch-up", testFollowerRestart)
	suite5.Run(t)

	snapshotConfig := optimalTestConfigForgRPC()
	snapshotConfig.Options.SnapshotThreshold = 10
	snapshotSuite := NewTestSuiteWithConfig("Snapshot and Compaction Test", 3, snapshotConfig)
	snapshotSuite.AddTest("Snapshot and Log Compaction", testSnapshotAndLogCompaction)
	snapshotSuite.Run(t)
}

// --- Test Case Implementations (Unchanged) ---
func testBasicLeaderElection(t *testing.T, cluster *gRPCTestCluster) {
	leaderID := cluster.CheckLeader(15 * time.Second)
	testutil.AssertNotEqual(t, "", leaderID, "Expected a leader to be elected")
	t.Logf("Leader elected: %s", leaderID)
}

func testCommandReplication(t *testing.T, cluster *gRPCTestCluster) {
	leaderID := cluster.CheckLeader(15 * time.Second)
	leaderNode := cluster.GetNode(leaderID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	t.Log("Proposing a test command...")
	command := []byte("test-command-1")
	index, term, isLeader, err := leaderNode.raft.Propose(ctx, command)
	testutil.AssertNoError(t, err, "Propose failed")
	testutil.AssertTrue(t, isLeader, "Propose returned isLeader=false")
	t.Logf("Command proposed at index %d, term %d", index, term)

	// ADD THIS DEBUGGING: Wait a bit and check why WaitForApplied might hang
	time.Sleep(2 * time.Second)
	cluster.debugWhyWaitingForApplied(1)

	cluster.WaitForApplied(1, 10*time.Second)
	cluster.CheckConsistency(t)
}

func testLeaderFailure(t *testing.T, cluster *gRPCTestCluster) {
	originalLeaderID := cluster.CheckLeader(15 * time.Second)
	t.Logf("Initial leader elected: %s", originalLeaderID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, _, _, err := cluster.GetNode(originalLeaderID).raft.Propose(ctx, []byte("pre-failure-cmd"))
	cancel()
	testutil.AssertNoError(t, err)
	cluster.WaitForApplied(1, 10*time.Second)
	t.Log("Initial command replicated successfully.")

	t.Logf("Stopping leader node %s...", originalLeaderID)
	cluster.StopNode(originalLeaderID)
	t.Logf("Node %s stopped.", originalLeaderID)

	t.Log("Waiting for a new leader to be elected...")
	newLeaderID := cluster.CheckLeader(15 * time.Second)
	testutil.AssertNotEqual(t, originalLeaderID, newLeaderID, "A new leader should have been elected")
	t.Logf("New leader elected: %s", newLeaderID)

	t.Log("Proposing command to new leader...")
	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	_, _, _, err = cluster.GetNode(newLeaderID).raft.Propose(ctx2, []byte("post-failure-cmd"))
	cancel2()
	testutil.AssertNoError(t, err, "New leader failed to propose command")

	cluster.WaitForApplied(2, 10*time.Second)
	cluster.CheckConsistency(t)
	t.Log("New leader successfully replicated a command. Leader failure test passed.")
}

func testNetworkPartition(t *testing.T, cluster *gRPCTestCluster) {
	originalLeaderID := cluster.CheckLeader(15 * time.Second)
	t.Logf("Initial leader elected: %s", originalLeaderID)

	allNodes := cluster.GetNodeIDs()
	minorityPartition := []types.NodeID{allNodes[0], allNodes[1]}
	majorityPartition := []types.NodeID{allNodes[2], allNodes[3], allNodes[4]}

	t.Logf("Creating partition: %v | %v", minorityPartition, majorityPartition)
	cluster.CreatePartition(minorityPartition, majorityPartition)

	time.Sleep(time.Duration(cluster.GetConfig().Options.ElectionTickCount) * NominalTickInterval * 2)
	t.Log("Waiting for new leader election in majority partition...")
	newLeaderID := cluster.CheckLeaderInPartition(majorityPartition, 15*time.Second)
	t.Logf("New leader in majority partition: %s", newLeaderID)

	hasLeader := cluster.HasLeaderInPartition(minorityPartition)
	testutil.AssertFalse(t, hasLeader, "Minority partition should not have a leader")
	t.Log("Verified no leader in minority partition.")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	_, _, _, err := cluster.GetNode(newLeaderID).raft.Propose(ctx, []byte("partition-cmd"))
	cancel()
	testutil.AssertNoError(t, err)

	t.Log("Healing network partition...")
	cluster.HealPartition()
	time.Sleep(time.Duration(cluster.GetConfig().Options.HeartbeatTickCount) * NominalTickInterval * 5)

	finalLeaderID := cluster.CheckLeader(15 * time.Second)
	t.Logf("Cluster converged on leader %s after partition heal.", finalLeaderID)

	cluster.WaitForApplied(1, 15*time.Second)
	cluster.CheckConsistency(t)
	t.Log("Network partition test passed.")
}

func testConcurrentProposals(t *testing.T, cluster *gRPCTestCluster) {
	leaderID := cluster.CheckLeader(15 * time.Second)
	leaderNode := cluster.GetNode(leaderID)
	t.Logf("Leader for concurrent test: %s", leaderID)

	numClients := 10
	commandsPerClient := 5
	totalCommands := int64(numClients * commandsPerClient)
	var successfulProposals int64

	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			for j := 0; j < commandsPerClient; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				command := []byte(fmt.Sprintf("client-%d-cmd-%d", clientID, j))
				_, _, _, err := leaderNode.raft.Propose(ctx, command)
				if err == nil {
					atomic.AddInt64(&successfulProposals, 1)
				}
				cancel()
			}
		}(i)
	}

	wg.Wait()
	t.Logf("Finished proposing commands. Successful proposals: %d/%d", successfulProposals, totalCommands)

	cluster.WaitForApplied(int(successfulProposals), 20*time.Second)
	cluster.CheckConsistency(t)
	t.Log("Concurrent proposals test passed.")
}

func testSnapshotAndLogCompaction(t *testing.T, cluster *gRPCTestCluster) {
	leaderID := cluster.CheckLeader(15 * time.Second)
	leaderNode := cluster.GetNode(leaderID)
	cfg := cluster.GetConfig()
	numCommands := int(cfg.Options.SnapshotThreshold) + 5

	t.Logf("Proposing %d commands to trigger snapshot (threshold is %d)", numCommands, cfg.Options.SnapshotThreshold)
	for i := 0; i < numCommands; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		command := []byte(fmt.Sprintf("snapshot-cmd-%d", i))
		_, _, _, err := leaderNode.raft.Propose(ctx, command)
		cancel()
		testutil.AssertNoError(t, err, "Failed to propose command %d", i)
	}

	cluster.WaitForApplied(numCommands, 20*time.Second)
	t.Log("All commands applied. Waiting for snapshot and compaction to occur...")
	time.Sleep(2 * time.Second)

	status := leaderNode.raft.Status()
	testutil.AssertTrue(t, status.SnapshotIndex > 0, "Expected snapshot to have been taken, but snapshot index is 0")
	t.Logf("Log compaction verified on leader. First available log index is now %d.", status.SnapshotIndex+1)

	t.Log("Proposing a post-snapshot command...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, _, _, err := leaderNode.raft.Propose(ctx, []byte("post-snapshot-cmd"))
	cancel()
	testutil.AssertNoError(t, err)

	cluster.WaitForApplied(numCommands+1, 10*time.Second)
	cluster.CheckConsistency(t)
	t.Log("Snapshot and log compaction test passed.")
}

func testFollowerRestart(t *testing.T, cluster *gRPCTestCluster) {
	leaderID := cluster.CheckLeader(15 * time.Second)
	t.Logf("Initial leader elected: %s", leaderID)

	var followerID types.NodeID
	for _, id := range cluster.GetNodeIDs() {
		if id != leaderID {
			followerID = id
			break
		}
	}
	followerNode := cluster.GetNode(followerID)
	followerStorage := followerNode.storage

	t.Log("Proposing initial 5 commands...")
	for i := 0; i < 5; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, _, err := cluster.GetNode(leaderID).raft.Propose(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		cancel()
		testutil.AssertNoError(t, err)
	}
	cluster.WaitForApplied(5, 10*time.Second)
	t.Log("Initial commands applied by all nodes.")

	t.Logf("Stopping follower node %s...", followerID)
	cluster.StopNode(followerID)

	t.Log("Proposing 5 more commands while follower is offline...")
	for i := 5; i < 10; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		_, _, _, err := cluster.GetNode(leaderID).raft.Propose(ctx, []byte(fmt.Sprintf("cmd-%d", i)))
		cancel()
		testutil.AssertNoError(t, err)
	}
	cluster.WaitForApplied(10, 10*time.Second)
	t.Log("Commands applied by remaining nodes.")

	t.Logf("Restarting follower node %s...", followerID)
	restartedNode := cluster.RestartNode(followerNode.id, followerNode.port, followerStorage)

	time.Sleep(time.Duration(cluster.GetConfig().Options.ElectionTickCount) * NominalTickInterval)

	cluster.WaitForApplied(10, 15*time.Second)
	cluster.CheckConsistency(t)

	restartedNodeApplier := restartedNode.applier
	testutil.AssertEqual(t, 10, restartedNodeApplier.GetAppliedCount(), "Restarted node did not apply all commands")
	t.Log("Follower restart and catch-up test passed.")
}

// --- Test Framework and Helpers ---

// TestStorage provides a simple in-memory implementation of the Storage interface for testing.
type TestStorage struct {
	mu            sync.RWMutex
	state         types.PersistentState
	log           []types.LogEntry
	firstLogIndex types.Index
	snapshotMeta  types.SnapshotMetadata
	snapshotData  []byte
}

func NewTestStorage() *TestStorage {
	return &TestStorage{
		log:           make([]types.LogEntry, 0),
		firstLogIndex: 1,
	}
}

// lastLogIndexUnsafe returns the last log index without locking. Assumes caller holds the lock.
func (ts *TestStorage) lastLogIndexUnsafe() types.Index {
	if len(ts.log) == 0 {
		return ts.firstLogIndex - 1
	}
	return ts.log[len(ts.log)-1].Index
}

// truncateLogPrefixUnsafe truncates the log prefix without locking. Assumes caller holds the lock.
func (ts *TestStorage) truncateLogPrefixUnsafe(_ context.Context, newFirstIndex types.Index) error {
	if newFirstIndex <= ts.firstLogIndex {
		return nil
	}
	var newLog []types.LogEntry
	for _, entry := range ts.log {
		if entry.Index >= newFirstIndex {
			newLog = append(newLog, entry)
		}
	}
	ts.log = newLog
	ts.firstLogIndex = newFirstIndex
	return nil
}

func (ts *TestStorage) SaveState(_ context.Context, state types.PersistentState) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.state = state
	return nil
}

func (ts *TestStorage) LoadState(_ context.Context) (types.PersistentState, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.state, nil
}

func (ts *TestStorage) AppendLogEntries(_ context.Context, entries []types.LogEntry) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	if len(entries) > 0 {
		if ts.lastLogIndexUnsafe()+1 != entries[0].Index {
			return storage.ErrNonContiguousEntries
		}
	}
	ts.log = append(ts.log, entries...)
	return nil
}

func (ts *TestStorage) GetLogEntry(_ context.Context, index types.Index) (types.LogEntry, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if index < ts.firstLogIndex {
		return types.LogEntry{}, storage.ErrCorruptedLog
	}
	for _, entry := range ts.log {
		if entry.Index == index {
			return entry, nil
		}
	}
	return types.LogEntry{}, storage.ErrEntryNotFound
}

func (ts *TestStorage) GetLogEntries(_ context.Context, start, end types.Index) ([]types.LogEntry, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if start < ts.firstLogIndex {
		return nil, storage.ErrCorruptedLog
	}
	var result []types.LogEntry
	for _, entry := range ts.log {
		if entry.Index >= start && entry.Index < end {
			result = append(result, entry)
		}
	}
	return result, nil
}

func (ts *TestStorage) TruncateLogPrefix(ctx context.Context, newFirstIndex types.Index) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	return ts.truncateLogPrefixUnsafe(ctx, newFirstIndex)
}

func (ts *TestStorage) TruncateLogSuffix(_ context.Context, index types.Index) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	var newLog []types.LogEntry
	for _, entry := range ts.log {
		if entry.Index < index {
			newLog = append(newLog, entry)
		}
	}
	ts.log = newLog
	return nil
}

func (ts *TestStorage) FirstLogIndex() types.Index {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.firstLogIndex
}

func (ts *TestStorage) LastLogIndex() types.Index {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	return ts.lastLogIndexUnsafe()
}

func (ts *TestStorage) SaveSnapshot(ctx context.Context, meta types.SnapshotMetadata, data []byte) error {
	ts.mu.Lock()
	defer ts.mu.Unlock()
	ts.snapshotMeta = meta
	ts.snapshotData = data
	return ts.truncateLogPrefixUnsafe(ctx, meta.LastIncludedIndex+1)
}

func (ts *TestStorage) LoadSnapshot(_ context.Context) (types.SnapshotMetadata, []byte, error) {
	ts.mu.RLock()
	defer ts.mu.RUnlock()
	if ts.snapshotData == nil {
		return types.SnapshotMetadata{}, nil, storage.ErrNoSnapshot
	}
	return ts.snapshotMeta, ts.snapshotData, nil
}
func (ts *TestStorage) Close() error                  { return nil }
func (ts *TestStorage) GetMetrics() map[string]uint64 { return nil }
func (ts *TestStorage) GetMetricsSummary() string     { return "" }
func (ts *TestStorage) ResetMetrics()                 {}

// TestApplier provides a simple in-memory implementation of the Applier interface.
type TestApplier struct {
	mu             sync.RWMutex
	appliedEntries []types.ApplyMsg
}

func NewTestApplier(t *testing.T) *TestApplier {
	return &TestApplier{
		appliedEntries: make([]types.ApplyMsg, 0),
	}
}
func (a *TestApplier) Apply(ctx context.Context, index types.Index, command []byte) (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	msg := types.ApplyMsg{
		CommandValid: true,
		Command:      command,
		CommandIndex: index,
	}
	a.appliedEntries = append(a.appliedEntries, msg)
	return "OK", nil
}
func (a *TestApplier) Snapshot(ctx context.Context) (types.Index, []byte, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()
	if len(a.appliedEntries) == 0 {
		return 0, nil, nil
	}
	lastEntry := a.appliedEntries[len(a.appliedEntries)-1]
	return lastEntry.CommandIndex, []byte("snapshot-data"), nil
}
func (a *TestApplier) RestoreSnapshot(ctx context.Context, lastIndex types.Index, lastTerm types.Term, data []byte) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.appliedEntries = []types.ApplyMsg{{
		SnapshotValid: true,
		SnapshotIndex: lastIndex,
		SnapshotTerm:  lastTerm,
	}}
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

// gRPCTestCluster and related methods.
type gRPCTestCluster struct {
	nodes       map[types.NodeID]*gRPCTestNode
	t           *testing.T
	stopOnce    sync.Once
	stopped     atomic.Bool
	mu          sync.RWMutex
	basePort    int
	networkCond *NetworkConditions
	config      Config
}

type rpcCall struct {
	rpcType  string
	args     interface{}
	respChan chan rpcResponse
}

type rpcResponse struct {
	reply interface{}
	err   error
}

type gRPCTestNode struct {
	id         types.NodeID
	raft       Raft
	storage    *TestStorage
	applier    *TestApplier
	networkMgr NetworkManager
	address    string
	port       int
	t          *testing.T
	cancelTick context.CancelFunc
	rpcChan    chan rpcCall
}

func NewgRPCTestCluster(t *testing.T, nodeCount int, customConfig ...Config) *gRPCTestCluster {
	cluster := &gRPCTestCluster{
		nodes:       make(map[types.NodeID]*gRPCTestNode),
		t:           t,
		basePort:    getFreePortRange(nodeCount),
		networkCond: NewNetworkConditions(),
	}

	if len(customConfig) > 0 {
		cluster.config = customConfig[0]
	} else {
		cluster.config = optimalTestConfigForgRPC()
	}

	peers := make(map[types.NodeID]PeerConfig, nodeCount)
	for i := 1; i <= nodeCount; i++ {
		nodeID := types.NodeID(fmt.Sprintf("node%d", i))
		port := cluster.basePort + i - 1
		address := fmt.Sprintf("localhost:%d", port)
		peers[nodeID] = PeerConfig{ID: nodeID, Address: address}
	}
	cluster.config.Peers = peers

	for i := 1; i <= nodeCount; i++ {
		nodeID := types.NodeID(fmt.Sprintf("node%d", i))
		port := cluster.basePort + i - 1
		nodeCfg := cluster.config
		nodeCfg.ID = nodeID
		cluster.createNode(nodeCfg, port, NewTestStorage())
	}

	return cluster
}

func (cluster *gRPCTestCluster) createNode(config Config, port int, storage *TestStorage) *gRPCTestNode {
	applier := NewTestApplier(cluster.t)
	logger := logger.NewStdLogger("info")

	raftNode, err := NewRaftBuilder().
		WithConfig(config).
		WithStorage(storage).
		WithApplier(applier).
		WithLogger(logger).
		Build()
	testutil.RequireNoError(cluster.t, err, "Failed to build Raft node %s", config.ID)

	networkMgr := NewTestNetworkManager(config.ID, cluster)
	raftNode.SetNetworkManager(networkMgr)

	node := &gRPCTestNode{
		id:         config.ID,
		raft:       raftNode,
		storage:    storage,
		applier:    applier,
		networkMgr: networkMgr,
		address:    fmt.Sprintf("localhost:%d", port),
		port:       port,
		t:          cluster.t,
		rpcChan:    make(chan rpcCall, 100),
	}

	cluster.mu.Lock()
	cluster.nodes[config.ID] = node
	cluster.mu.Unlock()
	return node
}

func (cluster *gRPCTestCluster) Start() {
	cluster.mu.RLock()
	nodesToStart := make([]*gRPCTestNode, 0, len(cluster.nodes))
	for _, node := range cluster.nodes {
		nodesToStart = append(nodesToStart, node)
	}
	cluster.mu.RUnlock()

	cluster.t.Logf("Starting cluster with %d nodes", len(nodesToStart))
	for _, node := range nodesToStart {
		cluster.StartNode(node)
	}
	time.Sleep(1 * time.Second)
	cluster.t.Log("All nodes started successfully")
}

func (cluster *gRPCTestCluster) StartNode(node *gRPCTestNode) {
	// Ensure clean state
	if node.rpcChan != nil {
		close(node.rpcChan)
	}
	node.rpcChan = make(chan rpcCall, 100)

	// Create a sync channel to ensure RPC loop is ready
	rpcLoopReady := make(chan struct{})

	// Start the RPC processing loop with proper error handling
	go func() {
		defer func() {
			if r := recover(); r != nil {
				cluster.t.Logf("RPC processing loop panicked for node %s: %v", node.id, r)
			}
			cluster.t.Logf("RPC processing loop stopped for node %s", node.id)
		}()

		// Signal that the RPC loop is ready
		close(rpcLoopReady)

		for call := range node.rpcChan {
			// Handle each RPC in a separate goroutine to prevent blocking
			go cluster.handleRPC(node, call)
		}
	}()

	// Wait for RPC loop to be ready
	select {
	case <-rpcLoopReady:
		// RPC loop is ready
	case <-time.After(1 * time.Second):
		cluster.t.Fatalf("RPC loop failed to start for node %s", node.id)
	}

	// Start the raft node
	err := node.raft.Start()
	testutil.RequireNoError(cluster.t, err, "Failed to start raft node %s", node.id)
	cluster.drainApplyChannel(node)

	// Start the ticker
	ctx, cancel := context.WithCancel(context.Background())
	node.cancelTick = cancel
	go cluster.backgroundTickerForNode(ctx, node)

	// Give the node time to fully initialize
	time.Sleep(100 * time.Millisecond)

	cluster.t.Logf("Node %s started and ready", node.id)
}

func (cluster *gRPCTestCluster) handleRPC(node *gRPCTestNode, call rpcCall) {
	defer func() {
		if r := recover(); r != nil {
			cluster.t.Logf("RPC handler panicked for %s on node %s: %v", call.rpcType, node.id, r)
			// Send error response to prevent caller from hanging
			select {
			case call.respChan <- rpcResponse{reply: nil, err: fmt.Errorf("RPC handler panicked: %v", r)}:
			case <-time.After(100 * time.Millisecond):
				cluster.t.Logf("Failed to send panic response for %s", call.rpcType)
			}
		}
	}()

	// Handle test RPCs specially
	if call.rpcType == "test" {
		select {
		case call.respChan <- rpcResponse{reply: "test-ok", err: nil}:
		case <-time.After(100 * time.Millisecond):
		}
		return
	}

	var reply any
	var err error

	handlerCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	switch args := call.args.(type) {
	case *types.RequestVoteArgs:
		reply, err = node.raft.RequestVote(handlerCtx, args)
	case *types.AppendEntriesArgs:
		reply, err = node.raft.AppendEntries(handlerCtx, args)
	case *types.InstallSnapshotArgs:
		reply, err = node.raft.InstallSnapshot(handlerCtx, args)
	default:
		err = fmt.Errorf("unknown RPC type: %T", args)
	}

	// Send response back with timeout protection
	select {
	case call.respChan <- rpcResponse{reply: reply, err: err}:
		// Response sent successfully
	case <-time.After(2 * time.Second):
		cluster.t.Logf("WARNING: Failed to send RPC response for %s after timeout", call.rpcType)
	case <-handlerCtx.Done():
		cluster.t.Logf("WARNING: RPC handler context cancelled for %s", call.rpcType)
	}
}

func (cluster *gRPCTestCluster) RestartNode(id types.NodeID, port int, storage *TestStorage) *gRPCTestNode {
	cluster.t.Logf("Restarting node %s using its existing storage.", id)
	cfg := cluster.GetConfig()
	cfg.ID = id
	node := cluster.createNode(cfg, port, storage)
	cluster.StartNode(node)
	return node
}

func (cluster *gRPCTestCluster) backgroundTickerForNode(ctx context.Context, node *gRPCTestNode) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			node.raft.Tick(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (cluster *gRPCTestCluster) drainApplyChannel(node *gRPCTestNode) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
			}
		}()
		for range node.raft.ApplyChannel() {
		}
	}()
}

func (cluster *gRPCTestCluster) Stop() {
	cluster.stopOnce.Do(func() {
		cluster.stopped.Store(true)
		cluster.t.Log("Stopping cluster...")
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var wg sync.WaitGroup
		cluster.mu.RLock()
		nodes := make([]*gRPCTestNode, 0, len(cluster.nodes))
		for _, node := range cluster.nodes {
			nodes = append(nodes, node)
		}
		cluster.mu.RUnlock()

		for _, node := range nodes {
			wg.Add(1)
			go func(n *gRPCTestNode) {
				defer wg.Done()
				n.cancelTick()
				close(n.rpcChan)
				if err := n.raft.Stop(ctx); err != nil {
					cluster.t.Logf("Error stopping raft node %s: %v", n.id, err)
				}
			}(node)
		}
		wg.Wait()
		cluster.t.Log("Cluster stopped successfully")
	})
}

func (cluster *gRPCTestCluster) CheckLeader(timeout time.Duration) types.NodeID {
	return cluster.CheckLeaderInPartition(cluster.GetNodeIDs(), timeout)
}

func (cluster *gRPCTestCluster) CheckLeaderInPartition(partition []types.NodeID, timeout time.Duration) types.NodeID {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leaders := cluster.getLeadersInPartition(partition)
		if len(leaders) == 1 {
			cluster.t.Logf("SUCCESS: Single leader found in partition: %s", leaders[0])
			return leaders[0]
		}
		if len(leaders) > 1 {
			cluster.t.Fatalf("FATAL: Multiple leaders detected in partition: %v", leaders)
		}
		time.Sleep(100 * time.Millisecond)
	}
	cluster.t.Fatalf("Timed out waiting for a leader to be elected in partition")
	return ""
}

func (cluster *gRPCTestCluster) HasLeaderInPartition(partition []types.NodeID) bool {
	return len(cluster.getLeadersInPartition(partition)) > 0
}

func (cluster *gRPCTestCluster) getLeadersInPartition(partition []types.NodeID) []types.NodeID {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	leaders := []types.NodeID{}
	for _, id := range partition {
		if node, exists := cluster.nodes[id]; exists {
			if _, isLeader := node.raft.GetState(); isLeader {
				leaders = append(leaders, id)
			}
		}
	}
	return leaders
}

func (cluster *gRPCTestCluster) GetNode(nodeID types.NodeID) *gRPCTestNode {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	node, exists := cluster.nodes[nodeID]
	testutil.AssertTrue(cluster.t, exists, "node %s not found", nodeID)
	return node
}

func (cluster *gRPCTestCluster) StopNode(nodeID types.NodeID) {
	cluster.mu.Lock()
	node, exists := cluster.nodes[nodeID]
	if exists {
		delete(cluster.nodes, nodeID)
	}
	cluster.mu.Unlock()

	if !exists {
		cluster.t.Logf("WARNING: Node %s not found when trying to stop", nodeID)
		return
	}

	cluster.t.Logf("Stopping node %s...", nodeID)

	// 1. Stop ticker first
	if node.cancelTick != nil {
		node.cancelTick()
		node.cancelTick = nil
	}

	// 2. Stop the raft node with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if err := node.raft.Stop(ctx); err != nil {
		cluster.t.Logf("Error stopping raft node %s: %v", nodeID, err)
	}

	// 3. Close RPC channel and wait a bit for cleanup
	if node.rpcChan != nil {
		close(node.rpcChan)
		node.rpcChan = nil
	}

	// 4. Give time for goroutines to finish
	time.Sleep(50 * time.Millisecond)

	cluster.t.Logf("Node %s stopped", nodeID)
}

func (cluster *gRPCTestCluster) GetNodeIDs() []types.NodeID {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	ids := make([]types.NodeID, 0, len(cluster.nodes))
	for id := range cluster.nodes {
		ids = append(ids, id)
	}
	return ids
}

func (cluster *gRPCTestCluster) GetConfig() Config {
	return cluster.config
}

func (cluster *gRPCTestCluster) WaitForApplied(expectedCount int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	lastLogTime := time.Now()

	for {
		if time.Now().After(deadline) {
			cluster.t.Logf("TIMEOUT: WaitForApplied timed out after %v", timeout)
			cluster.logAppliedStatus(expectedCount)
			cluster.logDetailedNodeStatus()
			cluster.t.Fatalf("Timed out waiting for all nodes to apply %d commands", expectedCount)
		}

		allApplied := true
		cluster.mu.RLock()
		for _, node := range cluster.nodes {
			if node.applier.GetAppliedCount() < expectedCount {
				allApplied = false
				break
			}
		}
		cluster.mu.RUnlock()

		if allApplied {
			cluster.t.Logf("All nodes successfully applied %d commands", expectedCount)
			return
		}

		// Log status periodically
		if time.Since(lastLogTime) > 1*time.Second {
			cluster.logAppliedStatus(expectedCount)
			lastLogTime = time.Now()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (cluster *gRPCTestCluster) logDetailedNodeStatus() {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()

	cluster.t.Log("=== DETAILED NODE STATUS ===")
	for id, node := range cluster.nodes {
		status := node.raft.Status()
		appliedCount := node.applier.GetAppliedCount()

		// Test RPC responsiveness
		responsiveNodes := []string{}
		for otherID := range cluster.nodes {
			if otherID == id {
				continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			_, err := node.networkMgr.(*TestNetworkManager).sendAsync(
				ctx, otherID, "test", &types.RequestVoteArgs{Term: 0, CandidateID: id})
			cancel()

			if err == nil {
				responsiveNodes = append(responsiveNodes, string(otherID))
			}
		}

		cluster.t.Logf("Node %s: role=%v, term=%d, commit=%d, applied=%d, canReach=[%s]",
			id, status.Role, status.Term, status.CommitIndex, appliedCount,
			strings.Join(responsiveNodes, ","))
	}
	cluster.t.Log("=== END DETAILED STATUS ===")
}

func (cluster *gRPCTestCluster) logAppliedStatus(expectedCount int) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	var statuses []string
	for id, node := range cluster.nodes {
		count := node.applier.GetAppliedCount()
		status := node.raft.Status()
		statuses = append(statuses, fmt.Sprintf("Node %s: applied %d/%d, role=%v, term=%d, cmtIdx=%d",
			id, count, expectedCount, status.Role, status.Term, status.CommitIndex))
	}
	cluster.t.Logf("Apply Status:\n%s", strings.Join(statuses, "\n"))
}

func (cluster *gRPCTestCluster) CheckConsistency(t *testing.T) {
	t.Helper()
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()
	var firstNodeApplied []types.ApplyMsg
	var firstNodeID types.NodeID
	for id, node := range cluster.nodes {
		if firstNodeID == "" {
			firstNodeID = id
			firstNodeApplied = node.applier.GetAppliedEntries()
		} else {
			currentNodeApplied := node.applier.GetAppliedEntries()
			testutil.AssertLen(t, currentNodeApplied, len(firstNodeApplied),
				"Applied log length mismatch between %s and %s", firstNodeID, id)
			for i := range firstNodeApplied {
				testutil.AssertTrue(t, bytes.Equal(firstNodeApplied[i].Command, currentNodeApplied[i].Command),
					"Applied log entry mismatch at index %d between %s and %s. Got %s, want %s",
					firstNodeApplied[i].CommandIndex, firstNodeID, id, string(currentNodeApplied[i].Command), string(firstNodeApplied[i].Command))
			}
		}
	}
}

func (cluster *gRPCTestCluster) CreatePartition(partition1, partition2 []types.NodeID) {
	cluster.networkCond.CreatePartition(partition1, partition2)
	cluster.t.Logf("Created network partition between %v and %v", partition1, partition2)
}

func (cluster *gRPCTestCluster) HealPartition() {
	cluster.networkCond.HealPartition()
	cluster.t.Log("Healed all network partitions")
}

func getFreePortRange(count int) int {
	basePort := 9000
	for {
		listeners := make([]net.Listener, 0, count)
		allFree := true
		for i := 0; i < count; i++ {
			addr := fmt.Sprintf("localhost:%d", basePort+i)
			l, err := net.Listen("tcp", addr)
			if err != nil {
				allFree = false
				break
			}
			listeners = append(listeners, l)
		}

		for _, l := range listeners {
			l.Close()
		}

		if allFree {
			return basePort
		}
		basePort += count
		if basePort > 65000 {
			panic("Could not find free port range")
		}
	}
}

func optimalTestConfigForgRPC() Config {
	return Config{
		Options: Options{
			ElectionTickCount:           30,
			HeartbeatTickCount:          3,
			MaxLogEntriesPerRequest:     100,
			SnapshotThreshold:           1000,
			ElectionRandomizationFactor: 0.5,
		},
		FeatureFlags: FeatureFlags{
			EnableReadIndex:   true,
			EnableLeaderLease: true,
			PreVoteEnabled:    true,
		},
	}
}

type NetworkConditions struct {
	mu         sync.RWMutex
	partitions map[types.NodeID]map[types.NodeID]bool
}

func NewNetworkConditions() *NetworkConditions {
	return &NetworkConditions{
		partitions: make(map[types.NodeID]map[types.NodeID]bool),
	}
}
func (nc *NetworkConditions) CreatePartition(partition1, partition2 []types.NodeID) {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	for _, n1 := range partition1 {
		for _, n2 := range partition2 {
			if nc.partitions[n1] == nil {
				nc.partitions[n1] = make(map[types.NodeID]bool)
			}
			nc.partitions[n1][n2] = true
			if nc.partitions[n2] == nil {
				nc.partitions[n2] = make(map[types.NodeID]bool)
			}
			nc.partitions[n2][n1] = true
		}
	}
}
func (nc *NetworkConditions) HealPartition() {
	nc.mu.Lock()
	defer nc.mu.Unlock()
	nc.partitions = make(map[types.NodeID]map[types.NodeID]bool)
}
func (nc *NetworkConditions) IsPartitioned(from, to types.NodeID) bool {
	nc.mu.RLock()
	defer nc.mu.RUnlock()
	return nc.partitions[from] != nil && nc.partitions[from][to]
}

// TestNetworkManager simulates an asynchronous network for integration tests.
type TestNetworkManager struct {
	id      types.NodeID
	cluster *gRPCTestCluster
	cond    *NetworkConditions
}

func NewTestNetworkManager(id types.NodeID, cluster *gRPCTestCluster) *TestNetworkManager {
	return &TestNetworkManager{id: id, cluster: cluster, cond: cluster.networkCond}
}
func (m *TestNetworkManager) Start() error { return nil }
func (m *TestNetworkManager) Stop() error  { return nil }

func (m *TestNetworkManager) sendAsync(ctx context.Context, target types.NodeID, rpcType string, args interface{}) (interface{}, error) {
	if m.cond.IsPartitioned(m.id, target) {
		return nil, fmt.Errorf("network partition")
	}

	// Get target node with proper locking
	m.cluster.mu.RLock()
	targetNode, exists := m.cluster.nodes[target]
	m.cluster.mu.RUnlock()

	if !exists || targetNode == nil {
		return nil, fmt.Errorf("peer %s not found", target)
	}

	respChan := make(chan rpcResponse, 1)
	call := rpcCall{
		rpcType:  rpcType,
		args:     args,
		respChan: respChan,
	}

	// CRITICAL: Check if this is a test RPC and handle it specially
	if rpcType == "test" {
		// For test RPCs, just check if the channel is open
		select {
		case targetNode.rpcChan <- call:
			// Successfully sent test RPC, drain the response
			select {
			case <-respChan:
				return nil, nil
			case <-time.After(50 * time.Millisecond):
				return nil, fmt.Errorf("test RPC response timeout")
			}
		case <-time.After(50 * time.Millisecond):
			return nil, fmt.Errorf("test RPC channel send timeout")
		}
	}

	// For real RPCs, use the improved timeout mechanism
	select {
	case targetNode.rpcChan <- call:
		// Successfully queued the RPC, now wait for response
		select {
		case resp := <-respChan:
			return resp.reply, resp.err
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(2 * time.Second): // Shorter timeout for faster failure detection
			return nil, fmt.Errorf("RPC response timeout for %s to %s", rpcType, target)
		}
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(50 * time.Millisecond): // Very short timeout for channel send
		// This is the key fix - fail fast if we can't send to the channel
		return nil, fmt.Errorf("failed to send %s RPC to %s: target node not responding", rpcType, target)
	}
}

func (m *TestNetworkManager) SendRequestVote(ctx context.Context, target types.NodeID, args *types.RequestVoteArgs) (*types.RequestVoteReply, error) {
	reply, err := m.sendAsync(ctx, target, "RequestVote", args)
	if err != nil {
		return nil, err
	}
	return reply.(*types.RequestVoteReply), nil
}
func (m *TestNetworkManager) SendAppendEntries(ctx context.Context, target types.NodeID, args *types.AppendEntriesArgs) (*types.AppendEntriesReply, error) {
	reply, err := m.sendAsync(ctx, target, "AppendEntries", args)
	if err != nil {
		return nil, err
	}
	return reply.(*types.AppendEntriesReply), nil
}
func (m *TestNetworkManager) SendInstallSnapshot(ctx context.Context, target types.NodeID, args *types.InstallSnapshotArgs) (*types.InstallSnapshotReply, error) {
	reply, err := m.sendAsync(ctx, target, "InstallSnapshot", args)
	if err != nil {
		return nil, err
	}
	return reply.(*types.InstallSnapshotReply), nil
}
func (m *TestNetworkManager) PeerStatus(peer types.NodeID) (types.PeerConnectionStatus, error) {
	return types.PeerConnectionStatus{Connected: !m.cond.IsPartitioned(m.id, peer)}, nil
}
func (m *TestNetworkManager) LocalAddr() string { return string(m.id) }

type TestSuite struct {
	name        string
	tests       []TestCase
	clusterSize int
	config      Config
}

type TestCase struct {
	name string
	fn   func(t *testing.T, cluster *gRPCTestCluster)
}

func NewTestSuite(name string, clusterSize int) *TestSuite {
	return &TestSuite{
		name:        name,
		clusterSize: clusterSize,
		tests:       make([]TestCase, 0),
		config:      optimalTestConfigForgRPC(),
	}
}

func NewTestSuiteWithConfig(name string, clusterSize int, config Config) *TestSuite {
	return &TestSuite{
		name:        name,
		clusterSize: clusterSize,
		tests:       make([]TestCase, 0),
		config:      config,
	}
}

func (ts *TestSuite) AddTest(name string, fn func(t *testing.T, cluster *gRPCTestCluster)) {
	ts.tests = append(ts.tests, TestCase{name: name, fn: fn})
}

func (ts *TestSuite) Run(t *testing.T) {
	t.Run(ts.name, func(t *testing.T) {
		for i, testCase := range ts.tests {
			t.Run(testCase.name, func(t *testing.T) {
				// CRITICAL: Create a completely fresh cluster for each test
				cluster := NewgRPCTestCluster(t, ts.clusterSize, ts.config)
				cluster.Start()

				// Wait for cluster to fully stabilize before running test
				time.Sleep(500 * time.Millisecond)

				// Verify all nodes are responsive before starting the test
				if !cluster.verifyAllNodesResponsive() {
					t.Fatal("Not all nodes are responsive before test start")
				}

				testCase.fn(t, cluster)

				// Clean shutdown
				cluster.Stop()

				// CRITICAL: Wait between tests to ensure complete cleanup
				if i < len(ts.tests)-1 {
					time.Sleep(300 * time.Millisecond)
				}
			})
		}
	})
}

func (cluster *gRPCTestCluster) verifyAllNodesResponsive() bool {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()

	cluster.t.Log("Verifying all nodes are responsive...")

	for fromID, fromNode := range cluster.nodes {
		for toID := range cluster.nodes {
			if fromID == toID {
				continue
			}

			// Test if we can send a dummy RPC
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			_, err := fromNode.networkMgr.(*TestNetworkManager).sendAsync(
				ctx, toID, "test", &types.RequestVoteArgs{Term: 0, CandidateID: fromID})
			cancel()

			if err != nil {
				cluster.t.Logf("Node %s cannot reach node %s: %v", fromID, toID, err)
				return false
			}
		}
	}

	cluster.t.Log("All nodes are responsive")
	return true
}

func (cluster *gRPCTestCluster) WaitForAppliedWithDetailedLogging(expectedCount int, timeout time.Duration) {
	deadline := time.Now().Add(timeout)
	lastLogTime := time.Now()
	consecutiveFailures := 0

	for {
		if time.Now().After(deadline) {
			cluster.t.Logf("TIMEOUT: WaitForApplied timed out after %v", timeout)
			cluster.logAppliedStatus(expectedCount)
			cluster.logDetailedNodeStatus()
			cluster.debugWhyWaitingForApplied(expectedCount)
			cluster.t.Fatalf("Timed out waiting for all nodes to apply %d commands", expectedCount)
		}

		allApplied := true
		notAppliedNodes := []string{}

		cluster.mu.RLock()
		for id, node := range cluster.nodes {
			appliedCount := node.applier.GetAppliedCount()
			if appliedCount < expectedCount {
				allApplied = false
				notAppliedNodes = append(notAppliedNodes, fmt.Sprintf("%s(%d/%d)", id, appliedCount, expectedCount))
			}
		}
		cluster.mu.RUnlock()

		if allApplied {
			cluster.t.Logf("All nodes successfully applied %d commands", expectedCount)
			return
		}

		// Log status more frequently if we're consistently failing
		consecutiveFailures++
		if time.Since(lastLogTime) > 1*time.Second || consecutiveFailures%10 == 0 {
			cluster.t.Logf("Still waiting for nodes to apply entries: %v", notAppliedNodes)
			if consecutiveFailures >= 20 { // After 2 seconds of failures
				cluster.debugWhyWaitingForApplied(expectedCount)
			}
			lastLogTime = time.Now()
		}

		time.Sleep(100 * time.Millisecond)
	}
}

func (cluster *gRPCTestCluster) debugWhyWaitingForApplied(expectedCount int) {
	cluster.mu.RLock()
	defer cluster.mu.RUnlock()

	cluster.t.Log("=== DEBUG: Why are we still waiting for applied entries? ===")

	for id, node := range cluster.nodes {
		status := node.raft.Status()
		appliedCount := node.applier.GetAppliedCount()

		cluster.t.Logf("Node %s: role=%v, term=%d, commitIndex=%d, applied=%d/%d",
			id, status.Role, status.Term, status.CommitIndex, appliedCount, expectedCount)

		if status.Role == types.RoleLeader {
			cluster.t.Logf("  LEADER %s: Checking replication to followers...", id)

			// Test connectivity to each peer
			for otherID := range cluster.nodes {
				if otherID == id {
					continue
				}

				// Try a test RPC to see if the peer is responsive
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				_, err := node.networkMgr.(*TestNetworkManager).sendAsync(
					ctx, otherID, "test", &types.RequestVoteArgs{Term: 0, CandidateID: id})
				cancel()

				if err != nil {
					cluster.t.Logf("  PROBLEM: Leader %s cannot reach follower %s: %v", id, otherID, err)
				} else {
					cluster.t.Logf("  OK: Leader %s can reach follower %s", id, otherID)
				}
			}
		}
	}

	cluster.t.Log("=== END DEBUG ===")
}
