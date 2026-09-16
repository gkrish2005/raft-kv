package cluster_test

import (
	"bytes"
	"context"
	"sync"
	"testing"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

type memoryStore struct {
	mu   sync.Mutex
	term uint64
	vote string
	boot uint64
}

func (s *memoryStore) Save(term uint64, vote string, boot uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term, s.vote, s.boot = term, vote, boot
	return nil
}

func (s *memoryStore) Load() (uint64, string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.vote, s.boot, nil
}

// In-memory fault-injectable transport connecting nodes in a test cluster.
type faultTransport struct {
	mu      sync.Mutex
	nodes   map[string]*raft.Node
	dropped map[string]map[string]bool // from -> to -> dropped
}

func newFaultTransport() *faultTransport {
	return &faultTransport{
		nodes:   make(map[string]*raft.Node),
		dropped: make(map[string]map[string]bool),
	}
}

func (t *faultTransport) register(id string, n *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = n
}

func (t *faultTransport) setPartition(nodeA, nodeB string, drop bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.dropped[nodeA] == nil {
		t.dropped[nodeA] = make(map[string]bool)
	}
	if t.dropped[nodeB] == nil {
		t.dropped[nodeB] = make(map[string]bool)
	}
	t.dropped[nodeA][nodeB] = drop
	t.dropped[nodeB][nodeA] = drop
}

func (t *faultTransport) SendRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	t.mu.Lock()
	target, ok := t.nodes[peer]
	drop := t.dropped[req.CandidateId] != nil && t.dropped[req.CandidateId][peer]
	t.mu.Unlock()

	if !ok || drop {
		return nil, context.DeadlineExceeded
	}
	return target.RequestVote(ctx, req)
}

func (t *faultTransport) SendAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	t.mu.Lock()
	target, ok := t.nodes[peer]
	drop := t.dropped[req.LeaderId] != nil && t.dropped[req.LeaderId][peer]
	t.mu.Unlock()

	if !ok || drop {
		return nil, context.DeadlineExceeded
	}
	return target.AppendEntries(ctx, req)
}

// assertDecodedEntryEqual verifies that two LogEntries are logically identical by comparing decoded values.
func assertDecodedEntryEqual(t *testing.T, expected, actual *raftv1.LogEntry) {
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

// TestReplicationConvergenceWithFaultInjection follows the required scenario shape:
// 1. Inject fault (partition follower-3)
// 2. Issue writes to the leader during the fault
// 3. Heal the fault
// 4. Assert eventual convergence across all nodes' raw logs using decoded LogEntry equality
func TestReplicationConvergenceWithFaultInjection(t *testing.T) {
	transport := newFaultTransport()
	clock := raft.NewFakeClock(time.Unix(0, 0))

	stores := make(map[string]*storage.InMemoryLogStore)
	nodes := make(map[string]*raft.Node)
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	electionTimeouts := map[string]time.Duration{
		"node-1": 150 * time.Millisecond,
		"node-2": 300 * time.Millisecond,
		"node-3": 450 * time.Millisecond,
	}

	for _, id := range nodeIDs {
		var peers []string
		for _, other := range nodeIDs {
			if other != id {
				peers = append(peers, other)
			}
		}
		logStore := storage.NewInMemoryLogStore()
		stores[id] = logStore
		timeout := electionTimeouts[id]
		node, err := raft.NewNode(raft.Config{
			ID:              id,
			Peers:           peers,
			Clock:           clock,
			Transport:       transport,
			Store:           &memoryStore{boot: 1},
			LogStore:        logStore,
			ElectionTimeout: func() time.Duration { return timeout },
			RPCTimeout:      50 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		nodes[id] = node
		transport.register(id, node)
	}

	// Start all nodes
	for _, n := range nodes {
		if err := n.Start(); err != nil {
			t.Fatal(err)
		}
		defer n.Stop()
	}

	// Advance clock until node-1's election timer expires and it wins election
	leader := nodes["node-1"]
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && leader.Role() != raft.Leader {
		clock.Advance(50 * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}

	if leader.Role() != raft.Leader {
		t.Fatalf("expected node-1 to be elected leader, got role=%s", leader.Role())
	}

	// STEP 1: Inject fault (isolate node-3)
	transport.setPartition("node-1", "node-3", true)
	transport.setPartition("node-2", "node-3", true)

	// STEP 2: Issue writes to the leader during the fault
	cmd1 := &raftv1.Command{OperationType: "SET", Key: "alpha", Value: []byte("val-1"), RequestId: "req-1"}
	cmd2 := &raftv1.Command{OperationType: "SET", Key: "beta", Value: []byte("val-2"), RequestId: "req-2"}
	cmd3 := &raftv1.Command{OperationType: "SET", Key: "gamma", Value: []byte("val-3"), RequestId: "req-3"}

	e1, err := leader.AppendLocalEntry(cmd1)
	if err != nil {
		t.Fatal(err)
	}
	e2, err := leader.AppendLocalEntry(cmd2)
	if err != nil {
		t.Fatal(err)
	}
	e3, err := leader.AppendLocalEntry(cmd3)
	if err != nil {
		t.Fatal(err)
	}

	// Replicate under fault: node-2 should receive the entries; node-3 will not
	leader.Replicate()

	// Verify node-2 has received all entries:
	// In Phase 3 (I-023), leader's current-term NOOP occupies index 1, so 3 client writes occupy indices 2-4 (lastIndex=4).
	waitLastIndex(t, stores["node-2"], 4, 2*time.Second)

	// Verify node-3 is still at index 1 (received only the pre-partition election NOOP, I-023; none of the 3 partitioned client writes)
	if stores["node-3"].LastIndex() != 1 {
		t.Fatalf("expected node-3 lastIndex=1 while partitioned, got %d", stores["node-3"].LastIndex())
	}

	// STEP 3: HEAL the fault
	transport.setPartition("node-1", "node-3", false)
	transport.setPartition("node-2", "node-3", false)

	// Replicate to catch up node-3
	leader.Replicate()

	// STEP 4: Assert eventual convergence of raw logs across all nodes
	for _, id := range nodeIDs {
		store := stores[id]
		// In Phase 3 (I-023), leader's current-term NOOP occupies index 1, so 3 client writes occupy indices 2-4 (lastIndex=4).
		waitLastIndex(t, store, 4, 2*time.Second)

		// Verify NOOP entry at index 1
		noopEntry, err := store.Get(1)
		if err != nil {
			t.Fatalf("node %s failed to get NOOP entry at index 1: %v", id, err)
		}
		if noopEntry.Command == nil || noopEntry.Command.OperationType != "NOOP" {
			t.Fatalf("expected index 1 to be NOOP (I-023), got %+v", noopEntry)
		}

		expectedEntries := []*raftv1.LogEntry{e1, e2, e3}
		for i, expected := range expectedEntries {
			idx := uint64(i + 2) // client entries start at index 2 (after NOOP at index 1, I-023)
			actual, err := store.Get(idx)
			if err != nil {
				t.Fatalf("node %s failed to get entry %d: %v", id, idx, err)
			}
			assertDecodedEntryEqual(t, expected, actual)
		}
	}
}

func waitLastIndex(t *testing.T, store storage.LogStore, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if store.LastIndex() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	if store.LastIndex() != want {
		t.Fatalf("timed out waiting for store lastIndex=%d, got %d", want, store.LastIndex())
	}
}
