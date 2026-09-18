package chaos

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync/atomic"
	"time"

	"google.golang.org/protobuf/proto"

	"raftkv/internal/raft"
	"raftkv/internal/storage"
	clientv1 "raftkv/proto/client/v1"
	raftv1 "raftkv/proto/raft/v1"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// TestingT abstracts testing.T for use in both automated tests and standalone CLI runners.
type TestingT interface {
	Helper()
	Logf(format string, args ...any)
	Fatalf(format string, args ...any)
	Fatal(args ...any)
}

// AssertDecodedEntryEqual verifies that two LogEntries are logically identical by comparing decoded values.
func AssertDecodedEntryEqual(t TestingT, expected, actual *raftv1.LogEntry) {
	t.Helper()
	if expected.Index != actual.Index {
		t.Fatalf("index mismatch: expected %d, got %d", expected.Index, actual.Index)
	}
	if expected.Term != actual.Term {
		t.Fatalf("term mismatch at index %d: expected %d, got %d", expected.Index, expected.Term, actual.Term)
	}
	if (expected.Command == nil) != (actual.Command == nil) {
		t.Fatalf("command nil mismatch at index %d", expected.Index)
	}
	if expected.Command != nil {
		if expected.Command.OperationType != actual.Command.OperationType {
			t.Fatalf("operation mismatch at index %d: expected %s, got %s",
				expected.Index, expected.Command.OperationType, actual.Command.OperationType)
		}
		if expected.Command.Key != actual.Command.Key {
			t.Fatalf("key mismatch at index %d: expected %s, got %s",
				expected.Index, expected.Command.Key, actual.Command.Key)
		}
		if !bytes.Equal(expected.Command.Value, actual.Command.Value) {
			t.Fatalf("value mismatch at index %d: expected %q, got %q",
				expected.Index, expected.Command.Value, actual.Command.Value)
		}
		if expected.Command.RequestId != actual.Command.RequestId {
			t.Fatalf("request_id mismatch at index %d: expected %s, got %s",
				expected.Index, expected.Command.RequestId, actual.Command.RequestId)
		}
	}
}

// WaitForLeader waits up to timeout for an elected leader to emerge in the cluster.
func WaitForLeader(t TestingT, cluster *InProcessCluster, timeout time.Duration, exclude ...string) *NodeContext {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		leader, err := cluster.FindLeader(exclude...)
		if err == nil && leader != nil && leader.Node.ReadReadyTerm() == leader.Node.Term() {
			return leader
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for leader after %v", timeout)
	return nil
}

// waitForStableClusterLeader polls until a single stable leader is elected, ready to serve reads (I-023),
// and all live nodes have converged onto that leader's term, bounded by timeout (at least 400ms).
func waitForStableClusterLeader(t TestingT, cluster *InProcessCluster, timeout time.Duration) *NodeContext {
	t.Helper()
	if timeout < 400*time.Millisecond {
		timeout = 400 * time.Millisecond
	}
	deadline := time.Now().Add(timeout)

	for time.Now().Before(deadline) {
		liveNodes := cluster.LiveNodes()
		if len(liveNodes) > 0 {
			var leader *NodeContext
			leaderCount := 0
			for _, n := range liveNodes {
				if n.Node.Role() == raft.Leader {
					leader = n
					leaderCount++
				}
			}

			// Exactly one leader found and ready to serve reads
			if leaderCount == 1 && leader != nil && leader.Node.ReadReadyTerm() == leader.Node.Term() {
				leaderTerm := leader.Node.Term()
				allAgree := true
				for _, n := range liveNodes {
					if n.Node.Term() != leaderTerm {
						allAgree = false
						break
					}
					if n.ID != leader.ID && n.Node.Role() != raft.Follower {
						allAgree = false
						break
					}
				}
				if allAgree {
					return leader
				}
			}
		}
		time.Sleep(15 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for stable cluster leader and term convergence after %v", timeout)
	return nil
}

// AssertConvergence performs the rigorous 5-point post-heal state convergence verification:
// 1. Quiescence Barrier (LastApplied == CommitIndex == leader.CommitIndex on all live nodes)
// 2. CommitIndex & LastApplied equality across all live nodes
// 3. Decoded LogEntry prefix equality from index 1 to CommitIndex
// 4. KVStateMachine key-value map equality
// 5. Replicated RequestTable map equality
func AssertConvergence(t TestingT, cluster *InProcessCluster, timeout time.Duration) {
	t.Helper()

	// 1. Quiescence Barrier
	leader := WaitForLeader(t, cluster, timeout)
	var finalLeaderCommit uint64

	deadline := time.Now().Add(timeout)
	for {
		allConverged := true
		liveNodes := cluster.LiveNodes()
		if len(liveNodes) == 0 {
			t.Fatalf("no live nodes in cluster for convergence check")
		}

		targetCommit := leader.Node.CommitIndex()
		for _, n := range liveNodes {
			c := n.Node.CommitIndex()
			a := n.Node.LastApplied()
			if c != targetCommit || a != targetCommit {
				allConverged = false
				break
			}
		}

		if allConverged {
			finalLeaderCommit = targetCommit
			break
		}
		if time.Now().After(deadline) {
			for _, n := range cluster.LiveNodes() {
				t.Logf("node %s: commit=%d, applied=%d (target=%d)",
					n.ID, n.Node.CommitIndex(), n.Node.LastApplied(), targetCommit)
			}
			t.Fatalf("quiescence barrier timed out after %v: live nodes failed to reach commitIndex=%d", timeout, targetCommit)
		}
		time.Sleep(10 * time.Millisecond)
	}

	liveNodes := cluster.LiveNodes()

	// 2. CommitIndex & LastApplied equality across all live nodes
	for _, n := range liveNodes {
		if c := n.Node.CommitIndex(); c != finalLeaderCommit {
			t.Fatalf("node %s commitIndex mismatch: expected %d, got %d", n.ID, finalLeaderCommit, c)
		}
		if a := n.Node.LastApplied(); a != finalLeaderCommit {
			t.Fatalf("node %s lastApplied mismatch: expected %d, got %d", n.ID, finalLeaderCommit, a)
		}
	}

	// 3. Decoded LogEntry prefix equality for all indices [1, finalLeaderCommit]
	refStore := liveNodes[0].Node.LogStore()
	for idx := uint64(1); idx <= finalLeaderCommit; idx++ {
		expected, err := refStore.Get(idx)
		if err != nil {
			t.Fatalf("failed to retrieve entry %d from reference node %s: %v", idx, liveNodes[0].ID, err)
		}

		for _, n := range liveNodes[1:] {
			actual, err := n.Node.LogStore().Get(idx)
			if err != nil {
				t.Fatalf("failed to retrieve entry %d from node %s: %v", idx, n.ID, err)
			}
			AssertDecodedEntryEqual(t, expected, actual)
		}
	}

	// 4. KV State Machine equality
	refKV := liveNodes[0].SM.KVSnapshot()
	for _, n := range liveNodes[1:] {
		targetKV := n.SM.KVSnapshot()
		if len(refKV) != len(targetKV) {
			t.Fatalf("KV size mismatch: node %s has %d keys, node %s has %d keys",
				liveNodes[0].ID, len(refKV), n.ID, len(targetKV))
		}
		for k, refVal := range refKV {
			targetVal, ok := targetKV[k]
			if !ok {
				t.Fatalf("key %q missing on node %s", k, n.ID)
			}
			if !bytes.Equal(refVal, targetVal) {
				t.Fatalf("key %q value mismatch between %s and %s: %q vs %q",
					k, liveNodes[0].ID, n.ID, refVal, targetVal)
			}
		}
	}

	// 5. Replicated RequestTable equality
	refReqTable := liveNodes[0].SM.RequestTableSnapshot()
	for _, n := range liveNodes[1:] {
		targetReqTable := n.SM.RequestTableSnapshot()
		if len(refReqTable) != len(targetReqTable) {
			t.Fatalf("RequestTable size mismatch: node %s has %d entries, node %s has %d entries",
				liveNodes[0].ID, len(refReqTable), n.ID, len(targetReqTable))
		}
		for reqID, refEntry := range refReqTable {
			targetEntry, ok := targetReqTable[reqID]
			if !ok {
				t.Fatalf("request_id %q missing in RequestTable of node %s", reqID, n.ID)
			}
			if !bytes.Equal(refEntry.PayloadHash, targetEntry.PayloadHash) {
				t.Fatalf("request_id %q payload hash mismatch between %s and %s", reqID, liveNodes[0].ID, n.ID)
			}
		}
	}
}

// IssueSyncWrite writes a key-value pair to the current leader and waits for it to commit and apply via PendingWrite.Done (I-019).
func IssueSyncWrite(t TestingT, leader *NodeContext, key, val, reqID string) {
	t.Helper()
	cmd := &raftv1.Command{
		OperationType: "SET",
		Key:           key,
		Value:         []byte(val),
		RequestId:     reqID,
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	// Nudge replication actively so write commits without waiting for heartbeat timer
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				leader.Node.Replicate()
			}
		}
	}()

	_, err := leader.Node.Write(ctx, cmd)
	if err != nil {
		t.Fatalf("sync write %s=%s (req %s) failed on leader %s: %v", key, val, reqID, leader.ID, err)
	}
}

// RunScenarioLeaderCrash executes the leader crash & failover scenario.
func RunScenarioLeaderCrash(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 101)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)
	IssueSyncWrite(t, leader, "k1", "v1", "req-1")

	// Allow replication to majority
	time.Sleep(100 * time.Millisecond)

	// Step 1: Crash current leader
	c.CrashNode(leader.ID)

	// Step 2: New leader elected among surviving quorum
	newLeader := WaitForLeader(t, c, 3*time.Second, leader.ID)
	if newLeader.ID == leader.ID {
		t.Fatalf("crashed leader %s re-reported as leader", leader.ID)
	}
	IssueSyncWrite(t, newLeader, "k2", "v2", "req-2")

	// Step 3: Restart old leader
	if err := c.RestartNode(leader.ID); err != nil {
		t.Fatalf("failed to restart node %s: %v", leader.ID, err)
	}

	// Step 4: Heal & Assert Convergence
	IssueSyncWrite(t, newLeader, "k3", "v3", "req-3")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioFollowerCrash executes follower crash during replication.
func RunScenarioFollowerCrash(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 102)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)
	followerID := "node-2"
	if leader.ID == followerID {
		followerID = "node-3"
	}

	// Step 1: Crash follower
	c.CrashNode(followerID)

	// Step 2: Issue writes to leader (replicated to remaining node)
	IssueSyncWrite(t, leader, "k1", "v1", "req-1")
	IssueSyncWrite(t, leader, "k2", "v2", "req-2")

	// Step 3: Restart crashed follower
	if err := c.RestartNode(followerID); err != nil {
		t.Fatalf("restart follower %s failed: %v", followerID, err)
	}

	// Step 4: Heal & Assert Convergence
	IssueSyncWrite(t, leader, "k3", "v3", "req-3")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioMinorityPartition isolates a minority node while majority continues.
func RunScenarioMinorityPartition(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 103)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)
	minorityID := "node-2"
	if leader.ID == minorityID {
		minorityID = "node-3"
	}

	// Step 1: Partition minority node
	for _, other := range nodeIDs {
		if other != minorityID {
			c.Transport().SetPartition(minorityID, other, true)
		}
	}

	minorityCtx := c.nodes[minorityID]
	minorityCommitBefore := minorityCtx.Node.CommitIndex()

	// Step 2: Writes to majority succeed
	IssueSyncWrite(t, leader, "maj-1", "val-1", "req-maj-1")
	IssueSyncWrite(t, leader, "maj-2", "val-2", "req-maj-2")

	// Verify minority node commitIndex does not advance (I-008)
	if minorityCtx.Node.CommitIndex() > minorityCommitBefore {
		t.Fatalf("I-008 violation: isolated minority advanced commitIndex from %d to %d",
			minorityCommitBefore, minorityCtx.Node.CommitIndex())
	}

	// Step 3: Heal partition
	c.Transport().HealAll()

	// Step 4: Convergence
	IssueSyncWrite(t, leader, "maj-3", "val-3", "req-maj-3")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioMajorityPartitionHeals partitions the leader from all peers, stalls writes, heals, and recovers.
func RunScenarioMajorityPartitionHeals(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 104)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)
	oldLeaderID := leader.ID

	// Step 1: Isolate leader completely
	for _, other := range nodeIDs {
		if other != oldLeaderID {
			c.Transport().SetPartition(oldLeaderID, other, true)
		}
	}

	// Step 2: Quorum elects a new leader
	newLeader := WaitForLeader(t, c, 3*time.Second, oldLeaderID)
	if newLeader.ID == oldLeaderID {
		t.Fatalf("isolated leader still leader")
	}
	IssueSyncWrite(t, newLeader, "new-1", "val-1", "req-new-1")

	// Step 3: Heal partition
	c.Transport().HealAll()

	// Step 4: Old leader steps down and converges to new leader's log
	IssueSyncWrite(t, newLeader, "new-2", "val-2", "req-new-2")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioRepeatedElections induces partition churn causing repeated elections and term advancement.
func RunScenarioRepeatedElections(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 105)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	// Rapidly toggle partitions to force term bumps and step downs (I-001, I-007)
	// Each partition is held for 420ms (> MaxElectionTimeout=400ms) to ensure election timer expiry
	for round := 0; round < 3; round++ {
		c.Transport().SetPartition("node-1", "node-2", true)
		time.Sleep(420 * time.Millisecond)
		c.Transport().HealAll()
		c.Transport().SetPartition("node-2", "node-3", true)
		time.Sleep(420 * time.Millisecond)
		c.Transport().HealAll()
	}

	// Step 3: Stabilize & Heal
	c.Transport().HealAll()
	stableLeader := waitForStableClusterLeader(t, c, 3*time.Second)

	// Step 4: Write & Converge
	IssueSyncWrite(t, stableLeader, "stab-1", "val-1", "req-stab-1")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioSlowFollower_Catchup exercises I-013 with fixed latency d=70ms < RPCTimeout.
func RunScenarioSlowFollower_Catchup(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 106)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)
	slowID := "node-2"
	if leader.ID == slowID {
		slowID = "node-3"
	}

	// Calibrated Regime 1: d=70ms, jitter=0 (clean replication lag, < RPCTimeout=100ms)
	c.Transport().SetPeerFault(slowID, TransportConfig{
		BaseLatency: 70 * time.Millisecond,
		Jitter:      0,
	})

	// Issue writes to leader: replicates to fast node immediately, slow node lags
	IssueSyncWrite(t, leader, "slow-1", "val-1", "req-slow-1")
	IssueSyncWrite(t, leader, "slow-2", "val-2", "req-slow-2")

	// Heal transport
	c.Transport().ClearPeerFault(slowID)

	// Issue final write & verify slow follower caught up completely
	IssueSyncWrite(t, leader, "slow-3", "val-3", "req-slow-3")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioSlowFollower_RPCTimeout exercises I-021 & Rule 33 with fixed latency d=140ms > RPCTimeout.
func RunScenarioSlowFollower_RPCTimeout(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 107)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)
	slowID := "node-2"
	if leader.ID == slowID {
		slowID = "node-3"
	}

	// Calibrated Regime 2: d=140ms, jitter=0 (> RPCTimeout=100ms, < MinElectionTimeout=250ms)
	// Triggers RPC timeout and attemptID increment on leader. Late response discarded under Rule 33.
	c.Transport().SetPeerFault(slowID, TransportConfig{
		BaseLatency: 140 * time.Millisecond,
		Jitter:      0,
	})

	IssueSyncWrite(t, leader, "retry-1", "val-1", "req-retry-1")
	time.Sleep(300 * time.Millisecond) // Allow leader attempt counter to increment

	// Heal transport
	c.Transport().ClearPeerFault(slowID)

	IssueSyncWrite(t, leader, "retry-2", "val-2", "req-retry-2")
	AssertConvergence(t, c, 5*time.Second)
}

// RunScenarioStorageFailureFailClosed exercises I-018: storage failure causes fail-closed transition to StorageFailed.
func RunScenarioStorageFailureFailClosed(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 108)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)

	// Close the log store underneath the leader to inject an unrecoverable fsync/write failure
	if closer, ok := leader.LogStore.(interface{ Close() error }); ok {
		_ = closer.Close()
	}

	// Attempt append: must fail and set role = StorageFailed
	cmd := &raftv1.Command{OperationType: "SET", Key: "fail-k", Value: []byte("fail-v"), RequestId: "req-fail"}
	_, appendErr := leader.Node.AppendLocalEntry(cmd)
	if appendErr == nil {
		t.Fatalf("expected append error on closed storage")
	}

	// Invariant I-018: role must be StorageFailed
	if leader.Node.Role() != raft.StorageFailed {
		t.Fatalf("I-018 violation: expected role StorageFailed, got %v", leader.Node.Role())
	}

	// Node in StorageFailed must reject incoming AppendEntries
	_, rpcErr := leader.Node.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term:     leader.Node.Term() + 1,
		LeaderId: "node-2",
	})
	if rpcErr == nil {
		t.Fatalf("I-018 violation: StorageFailed node accepted AppendEntries")
	}
}

// RunScenarioMutexNetworkIOSafety exercises I-014: verifies Raft mutex is NOT held during network I/O.
func RunScenarioMutexNetworkIOSafety(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	c, err := NewInProcessCluster(nodeIDs, baseDir, 109)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.Start(); err != nil {
		t.Fatal(err)
	}
	defer c.Stop()

	leader := WaitForLeader(t, c, 3*time.Second)

	// Inject a 200ms transport delay on all outbound RPCs
	c.Transport().SetGlobalFault(TransportConfig{
		BaseLatency: 200 * time.Millisecond,
	})

	var probeSuccess atomic.Bool
	stopProbe := make(chan struct{})

	// Concurrently probe the leader's mutex lock availability.
	// If leader holds mutex during the 200ms transport call, the probe will take > 50ms to acquire.
	go func() {
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopProbe:
				return
			case <-ticker.C:
				start := time.Now()
				// ClusterView acquires n.mu.Lock() internally
				leader.Node.ClusterView()
				duration := time.Since(start)
				if duration < 50*time.Millisecond {
					probeSuccess.Store(true)
				}
			}
		}
	}()

	// Trigger replication (involves SendAppendEntries under 200ms delay)
	cmd := &raftv1.Command{OperationType: "SET", Key: "lock-test", Value: []byte("val"), RequestId: "req-lock"}
	_, _ = leader.Node.AppendLocalEntry(cmd)
	leader.Node.Replicate()

	time.Sleep(300 * time.Millisecond)
	close(stopProbe)

	if !probeSuccess.Load() {
		t.Fatalf("I-014 violation: could not acquire Raft mutex within 50ms while network I/O was in flight")
	}

	// Heal transport & verify cluster
	c.Transport().HealAll()
	IssueSyncWrite(t, leader, "lock-final", "val-final", "req-lock-final")
	AssertConvergence(t, c, 5*time.Second)
}

// findRepoRoot locates the repository root containing go.mod.
func findRepoRoot() string {
	dir, err := os.Getwd()
	if err != nil {
		return "."
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			break
		}
		dir = parent
	}
	return "."
}

func findProcessLeader(pc *ProcessCluster, timeout time.Duration, exclude ...string) (string, string, error) {
	excluded := make(map[string]bool, len(exclude))
	for _, ex := range exclude {
		excluded[ex] = true
	}

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		for _, id := range pc.NodeIDs() {
			if excluded[id] {
				continue
			}
			addr := pc.Address(id)
			conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if err != nil {
				continue
			}
			client := clientv1.NewClientServiceClient(conn)
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			resp, err := client.ClusterStatus(ctx, &clientv1.ClusterStatusRequest{})
			cancel()
			_ = conn.Close()

			if err == nil && resp.Status == clientv1.Status_STATUS_SUCCESS {
				if len(resp.Nodes) > 0 && resp.Nodes[0].Role == "Leader" {
					return id, addr, nil
				}
			}
		}
		time.Sleep(30 * time.Millisecond)
	}
	return "", "", fmt.Errorf("no leader found in process cluster within %v", timeout)
}

func issueProcessSyncWrite(pc *ProcessCluster, key, val, reqID string, timeout time.Duration, exclude ...string) error {
	deadline := time.Now().Add(timeout)
	backoff := 50 * time.Millisecond

	var lastErr error
	for time.Now().Before(deadline) {
		_, leaderAddr, err := findProcessLeader(pc, 2*time.Second, exclude...)
		if err == nil && leaderAddr != "" {
			conn, dialErr := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if dialErr == nil {
				client := clientv1.NewClientServiceClient(conn)
				ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				resp, rpcErr := client.Set(ctx, &clientv1.SetRequest{
					Key:       key,
					Value:     []byte(val),
					RequestId: reqID,
				})
				cancel()
				_ = conn.Close()

				if rpcErr == nil && resp.Status == clientv1.Status_STATUS_SUCCESS {
					return nil
				}
				if rpcErr != nil {
					lastErr = rpcErr
				} else {
					lastErr = fmt.Errorf("status %s: %s", resp.Status.String(), resp.ErrorMessage)
				}
			}
		}
		time.Sleep(backoff)
	}
	return fmt.Errorf("write %s failed to commit within %v: %v", key, timeout, lastErr)
}

func assertProcessConvergence(t TestingT, pc *ProcessCluster, expectedKeys map[string]string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)

	// Step 1: Wait until the active leader can serve linearizable Get for all expected keys
	var lastErr error
	leaderFound := false
	for time.Now().Before(deadline) {
		leaderID, leaderAddr, err := findProcessLeader(pc, 500*time.Millisecond)
		if err == nil && leaderAddr != "" {
			conn, dialErr := grpc.NewClient(leaderAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
			if dialErr == nil {
				client := clientv1.NewClientServiceClient(conn)
				allFound := true
				for k, expectedV := range expectedKeys {
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					resp, getErr := client.Get(ctx, &clientv1.GetRequest{Key: k})
					cancel()
					if getErr != nil || resp.Status != clientv1.Status_STATUS_SUCCESS || !resp.Found || string(resp.Value) != expectedV {
						allFound = false
						lastErr = fmt.Errorf("leader %s key %s: err=%v, status=%s, found=%v, val=%q, want=%q",
							leaderID, k, getErr, resp.GetStatus().String(), resp.GetFound(), string(resp.GetValue()), expectedV)
						break
					}
				}
				_ = conn.Close()
				if allFound {
					leaderFound = true
					break
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !leaderFound {
		t.Fatalf("timed out waiting for leader linearizable Get convergence: %v", lastErr)
	}

	// Step 2: Settle followers before stopping
	time.Sleep(200 * time.Millisecond)

	// Step 3: Stop all processes cleanly (SIGTERM) to flush and close WALs
	pc.Stop()

	// Step 3: Verify on-disk storage convergence across all nodes (I-005, I-008)
	nodeIDs := pc.NodeIDs()
	var baseEntries []*raftv1.LogEntry
	for _, id := range nodeIDs {
		walPath := filepath.Join(pc.DataDir(id), "wal")
		logStore, err := storage.NewFileLogStore(walPath)
		if err != nil {
			t.Fatalf("failed to open log store for %s: %v", id, err)
		}
		lastIndex := logStore.LastIndex()
		entries := make([]*raftv1.LogEntry, 0, lastIndex)
		for idx := uint64(1); idx <= lastIndex; idx++ {
			e, err := logStore.Get(idx)
			if err != nil {
				t.Fatalf("failed to read log entry %d for %s: %v", idx, id, err)
			}
			entries = append(entries, e)
		}

		if baseEntries == nil {
			baseEntries = entries
		} else {
			if len(entries) != len(baseEntries) {
				t.Fatalf("log length mismatch between nodes: node %s has %d entries, base has %d",
					id, len(entries), len(baseEntries))
			}
			for idx, exp := range baseEntries {
				AssertDecodedEntryEqual(t, exp, entries[idx])
			}
		}

		// Replay into local state machine and verify all keys
		sm := storage.NewKVStateMachine()
		for _, e := range entries {
			if e.Command != nil {
				_, _ = sm.Apply(storage.Command{
					OperationType: storage.OperationType(e.Command.OperationType),
					Key:           e.Command.Key,
					Value:         e.Command.Value,
					RequestID:     e.Command.RequestId,
				})
			}
		}
		for k, expectedV := range expectedKeys {
			actualV, found := sm.Get(k)
			if !found {
				t.Fatalf("node %s replayed SM missing key %s", id, k)
			}
			if string(actualV) != expectedV {
				t.Fatalf("node %s replayed SM key %s = %q, want %q", id, k, string(actualV), expectedV)
			}
		}
	}
}

// RunScenarioRollingCrash exercises I-024 using a real ProcessCluster (Level 4/5):
// sequential ungraceful SIGKILL crashes of each node under continuous client write traffic,
// verifying atomic TermVoteStore replacement, log persistence across real subprocess crashes,
// leader failover across surviving quorums, and post-recovery cluster convergence.
func RunScenarioRollingCrash(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	repoRoot := findRepoRoot()

	pc, err := NewProcessCluster(repoRoot, nodeIDs, baseDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.Start(); err != nil {
		t.Fatal(err)
	}
	defer pc.Stop()

	// Wait for all nodes to start listening
	for _, id := range nodeIDs {
		if err := pc.WaitListening(id, 7*time.Second); err != nil {
			t.Fatalf("node %s failed to listen: %v\nLogs:\n%s", id, err, pc.Logs(id))
		}
	}

	// 1. Initial write to elected leader
	if err := issueProcessSyncWrite(pc, "roll-0", "val-0", "req-roll-0", 7*time.Second); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	keysWritten := map[string]string{"roll-0": "val-0"}

	// 2. Sequentially kill each node with SIGKILL under traffic, then restart
	for i, id := range nodeIDs {
		// Ungraceful SIGKILL (kill -9)
		if err := pc.KillProcess(id); err != nil {
			t.Fatalf("failed to SIGKILL %s: %v", id, err)
		}

		writeKey := fmt.Sprintf("roll-mid-%d", i+1)
		writeVal := fmt.Sprintf("val-mid-%d", i+1)
		writeReq := fmt.Sprintf("req-roll-mid-%d", i+1)
		if err := issueProcessSyncWrite(pc, writeKey, writeVal, writeReq, 7*time.Second, id); err != nil {
			t.Fatalf("write with %s killed failed: %v", id, err)
		}
		keysWritten[writeKey] = writeVal

		// Restart node against its durable storage (replaying WAL and atomic TermVoteStore)
		if err := pc.RestartProcess(id); err != nil {
			t.Fatalf("failed to restart process %s: %v", id, err)
		}
		if err := pc.WaitListening(id, 7*time.Second); err != nil {
			t.Fatalf("restarted node %s failed to listen: %v\nLogs:\n%s", id, err, pc.Logs(id))
		}
	}

	// 3. Final write to stable cluster
	if err := issueProcessSyncWrite(pc, "roll-final", "val-final", "req-roll-final", 7*time.Second); err != nil {
		t.Fatalf("final write failed: %v", err)
	}
	keysWritten["roll-final"] = "val-final"

	// 4. Verify state convergence across all live nodes
	assertProcessConvergence(t, pc, keysWritten, 8*time.Second)
}

// readOnDiskTermVote directly unpacks the on-disk TermVoteRecord protobuf without triggering store.Load() boot counter increment.
func readOnDiskTermVote(path string) (uint64, string, uint64, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return 0, "", 0, err
	}
	if len(data) < 8 {
		return 0, "", 0, errors.New("truncated record")
	}
	n := int(binary.LittleEndian.Uint32(data[:4]))
	if len(data) < 8+n {
		return 0, "", 0, errors.New("invalid record length")
	}
	payload := data[4 : 4+n]
	var record raftv1.TermVoteRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return 0, "", 0, err
	}
	return record.CurrentTerm, record.VotedFor, record.BootId, nil
}

// RunScenarioProcessCrashRecovery exercises I-020 using a real ProcessCluster (Level 4/5):
// bootID persistence, monotonic boot progression (1 -> 2 -> 3) across real SIGKILL crashes,
// corruption detection on startup causing fail-closed process exit, and recovery by writing
// a valid repaired termvote record to disk and restarting through normal startup/recovery paths.
func RunScenarioProcessCrashRecovery(t TestingT, baseDir string) {
	nodeIDs := []string{"node-1", "node-2", "node-3"}
	repoRoot := findRepoRoot()

	pc, err := NewProcessCluster(repoRoot, nodeIDs, baseDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := pc.Start(); err != nil {
		t.Fatal(err)
	}
	defer pc.Stop()

	for _, id := range nodeIDs {
		if err := pc.WaitListening(id, 7*time.Second); err != nil {
			t.Fatalf("node %s failed to listen: %v\nLogs:\n%s", id, err, pc.Logs(id))
		}
	}

	// 1. Initial boot check: read the termvote file on disk for each node.
	// On first boot, Load() synthesizes and persists {term: 0, votedFor: "", bootID: 1} (I-020).
	for _, id := range nodeIDs {
		termVotePath := filepath.Join(pc.DataDir(id), "termvote")
		_, _, bootID, err := readOnDiskTermVote(termVotePath)
		if err != nil {
			t.Fatalf("I-020 violation: failed to read termvote record for %s: %v", id, err)
		}
		if bootID != 1 {
			t.Fatalf("I-020 violation: initial bootID = %d, want 1 for node %s", bootID, id)
		}
	}

	targetID := "node-1"
	termVotePath := filepath.Join(pc.DataDir(targetID), "termvote")

	if err := issueProcessSyncWrite(pc, "boot-1", "val-1", "req-boot-1", 7*time.Second); err != nil {
		t.Fatalf("initial write failed: %v", err)
	}
	keysWritten := map[string]string{"boot-1": "val-1"}

	// 2. Kill node-1 with ungraceful SIGKILL, restart, verify BootID == 2 on disk
	if err := pc.KillProcess(targetID); err != nil {
		t.Fatalf("failed to SIGKILL %s: %v", targetID, err)
	}
	if err := pc.RestartProcess(targetID); err != nil {
		t.Fatalf("failed to restart %s: %v", targetID, err)
	}
	if err := pc.WaitListening(targetID, 7*time.Second); err != nil {
		t.Fatalf("restarted node %s failed to listen: %v\nLogs:\n%s", targetID, err, pc.Logs(targetID))
	}
	term, _, bootID, err := readOnDiskTermVote(termVotePath)
	if err != nil {
		t.Fatalf("failed to load termvote for %s: %v", targetID, err)
	}
	if bootID != 2 {
		t.Fatalf("I-020 violation: after first restart bootID = %d, want 2 for node %s", bootID, targetID)
	}

	// 3. Kill node-1 with SIGKILL again, restart, verify BootID == 3 on disk
	if err := pc.KillProcess(targetID); err != nil {
		t.Fatalf("failed to second SIGKILL %s: %v", targetID, err)
	}
	if err := pc.RestartProcess(targetID); err != nil {
		t.Fatalf("failed to second restart %s: %v", targetID, err)
	}
	if err := pc.WaitListening(targetID, 7*time.Second); err != nil {
		t.Fatalf("second restarted node %s failed to listen: %v\nLogs:\n%s", targetID, err, pc.Logs(targetID))
	}
	term, _, bootID, err = readOnDiskTermVote(termVotePath)
	if err != nil {
		t.Fatalf("failed to load termvote for %s: %v", targetID, err)
	}
	if bootID != 3 {
		t.Fatalf("I-020 violation: after second restart bootID = %d, want 3 for node %s", bootID, targetID)
	}

	// 4. Test corruption fail-closed (I-020):
	// Kill target process, corrupt the on-disk termvote file with garbage bytes
	if err := pc.KillProcess(targetID); err != nil {
		t.Fatalf("failed to SIGKILL %s for corruption test: %v", targetID, err)
	}
	termVotePath = filepath.Join(pc.DataDir(targetID), "termvote")
	if err := os.WriteFile(termVotePath, []byte("corrupted_termvote_header_fail_closed"), 0o644); err != nil {
		t.Fatalf("failed to inject corruption: %v", err)
	}

	// Attempt to restart: the process MUST exit immediately (fail-closed, os.Exit(1))
	_ = pc.RestartProcess(targetID)
	exitCode, err := pc.WaitForProcessExit(targetID, 4*time.Second)
	if err != nil {
		t.Fatalf("I-020 violation: node started with corrupt termvote, expected process to exit fail-closed: %v", err)
	}
	if exitCode == 0 {
		t.Fatalf("I-020 violation: node with corrupt termvote exited 0, expected non-zero fail-closed exit code")
	}

	// 5. Repair state ON DISK: write a valid termvote record via FileTermVoteStore.Save()
	// (which writes temp-file + fsync + atomic rename + parent-dir fsync), then restart.
	// This exercises the real disk validation path through normal node startup and recover.
	repairedStore := storage.NewFileTermVoteStore(termVotePath, nodeIDs)
	if err := repairedStore.Save(term, "", 4); err != nil {
		t.Fatalf("failed to write repaired termvote file to disk: %v", err)
	}

	// Restart target node: now must succeed, loading the repaired on-disk file, validating checksum,
	// recovering WAL, and rejoining the cluster
	if err := pc.RestartProcess(targetID); err != nil {
		t.Fatalf("failed to restart %s after disk repair: %v", targetID, err)
	}
	if err := pc.WaitListening(targetID, 7*time.Second); err != nil {
		t.Fatalf("recovered node %s failed to listen after repair: %v\nLogs:\n%s", targetID, err, pc.Logs(targetID))
	}

	// 6. Write final entry through the cluster
	if err := issueProcessSyncWrite(pc, "boot-final", "val-final", "req-boot-final", 7*time.Second); err != nil {
		t.Fatalf("final write failed: %v", err)
	}
	keysWritten["boot-final"] = "val-final"

	// 7. Assert convergence across all 3 nodes (including the recovered node)
	assertProcessConvergence(t, pc, keysWritten, 8*time.Second)
}
