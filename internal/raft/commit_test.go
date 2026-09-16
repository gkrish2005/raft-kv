package raft

import (
	"context"
	"testing"
	"time"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

// Test 1: Replicated-but-not-committed (older-term entries):
// Ensures majority replication alone != commitment (I-006).
func TestCommitIndexAdvanceRequiresCurrentTerm(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	// Populate entries: entry 1 (term 1), entry 2 (term 2), entry 3 (term 3)
	logStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "req-1"),
		makeTestEntry(2, 2, "req-2"),
		makeTestEntry(3, 3, "req-3"),
	})

	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1", "p2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  logStore,
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 3
	n.state.matchIndex = make(map[string]uint64)
	// All 3 nodes (leader + p1 + p2) have replicated entry 2 (term 2)
	n.state.matchIndex["leader"] = 3
	n.state.matchIndex["p1"] = 2
	n.state.matchIndex["p2"] = 2
	n.state.commitIndex = 0

	// Attempt to advance commitIndex: entry 2 has majority (3/3), but entry.Term (2) != currentTerm (3)
	n.tryAdvanceCommitIndexLocked()
	if n.state.commitIndex != 0 {
		t.Fatalf("expected commitIndex to remain 0 for older-term entry, got %d", n.state.commitIndex)
	}

	// Now replicate entry 3 (term 3, currentTerm) to majority (leader + p1)
	n.state.matchIndex["p1"] = 3
	n.tryAdvanceCommitIndexLocked()
	if n.state.commitIndex != 3 {
		t.Fatalf("expected commitIndex to advance to 3 once current-term entry has majority, got %d", n.state.commitIndex)
	}
	n.mu.Unlock()
}

// Test 2: Committed-but-not-yet-applied:
// Transient gap commitIndex > lastApplied is valid and caught up by applier (I-009).
func TestCommitIndexAdvanceTransientGapWithLastApplied(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	logStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "req-1"),
		makeTestEntry(2, 1, "req-2"),
	})

	sm := storage.NewKVStateMachine()
	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        []string{"p1", "p2"},
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{boot: 1},
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	n.mu.Lock()
	n.state.role = Follower
	n.state.currentTerm = 1
	n.state.lastApplied = 0
	// Advance commitIndex to 2, creating a transient gap
	n.state.commitIndex = 2
	if n.state.commitIndex <= n.state.lastApplied {
		t.Fatalf("expected transient gap commitIndex > lastApplied")
	}
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	// Applier should catch up lastApplied to commitIndex
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n.LastApplied() == 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	if n.LastApplied() != 2 {
		t.Fatalf("expected lastApplied to catch up to 2, got %d", n.LastApplied())
	}

	// Verify entries were applied to the state machine
	val, found := sm.Get("k")
	if !found || string(val) != "v" {
		t.Fatalf("expected state machine to have key 'k'='v', got val=%s found=%v", string(val), found)
	}
}

// Test 3: Figure-8 scenario (I-006 canonical current-term commit restriction).
func TestFigure8ScenarioCurrentTermRestriction(t *testing.T) {
	// Cluster of 5 nodes: S1..S5. Quorum = 3.
	// S1 is leader in term 4.
	// S1 has log: index 1 (term 1), index 2 (term 2), index 3 (term 4).
	logStore := storage.NewInMemoryLogStore()
	logStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "req-1"),
		makeTestEntry(2, 2, "req-2"),
		makeTestEntry(3, 4, "req-3"),
	})

	s1, err := NewNode(Config{
		ID:        "s1",
		Peers:     []string{"s2", "s3", "s4", "s5"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  logStore,
	})
	if err != nil {
		t.Fatal(err)
	}

	s1.mu.Lock()
	s1.state.role = Leader
	s1.state.currentTerm = 4
	s1.state.commitIndex = 1
	s1.state.matchIndex = make(map[string]uint64)

	// In Figure 8(c): S1 replicates entry 2 (term 2) to S3.
	// Now S1, S2, S3 all have entry 2 (3 nodes = majority of 5).
	s1.state.matchIndex["s1"] = 3
	s1.state.matchIndex["s2"] = 2
	s1.state.matchIndex["s3"] = 2
	s1.state.matchIndex["s4"] = 0
	s1.state.matchIndex["s5"] = 0

	// S1 must NOT commit index 2 because its term (2) != currentTerm (4)
	s1.tryAdvanceCommitIndexLocked()
	if s1.state.commitIndex != 1 {
		t.Fatalf("Figure 8 violation: commitIndex advanced to %d for older-term entry without current-term commit", s1.state.commitIndex)
	}

	// In Figure 8(e): S1 replicates its current-term entry (index 3, term 4) to majority (S1, S2, S3)
	s1.state.matchIndex["s2"] = 3
	s1.state.matchIndex["s3"] = 3
	s1.tryAdvanceCommitIndexLocked()
	if s1.state.commitIndex != 3 {
		t.Fatalf("expected commitIndex to advance to 3 (committing index 2 indirectly), got %d", s1.state.commitIndex)
	}
	s1.mu.Unlock()
}

// Test 6: Application-error-still-applies:
// lastApplied advances past error-producing entries; commitIndex unaffected; pw.Done receives CommandResult{Err: err}.
func TestApplicationErrorStillAppliesAndAdvancesLastApplied(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	sm := storage.NewKVStateMachine()
	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        []string{"p1", "p2"},
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{boot: 1},
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Append an entry with an unknown/invalid operation type that causes an apply error
	invalidCmd := &raftv1.Command{
		OperationType: "INVALID_OP_TYPE",
		Key:           "bad-key",
		Value:         []byte("val"),
		RequestId:     "req-invalid-1",
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()

	entry, err := n.AppendLocalEntry(invalidCmd)
	if err != nil {
		t.Fatalf("AppendLocalEntry failed: %v", err)
	}

	n.mu.Lock()
	pw := n.pendingWrites[entry.Index]
	if pw == nil {
		t.Fatalf("expected PendingWrite to be registered")
	}
	// Replicate and advance commitIndex past this entry
	n.state.matchIndex["p1"] = entry.Index
	n.state.commitIndex = entry.Index
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	// Wait for PendingWrite resolution
	select {
	case res := <-pw.Done:
		if res.Err == nil {
			t.Fatalf("expected application error on pw.Done, got nil")
		}
		if res.Err == ErrSuperseded || res.Err == ErrLeadershipLost || res.Err == ErrShutdown {
			t.Fatalf("expected application error from state machine, got protocol error: %v", res.Err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for PendingWrite Done")
	}

	// Invariant I-005 / docs/client-semantics.md:
	// lastApplied must still advance past the error-producing entry!
	if n.LastApplied() != entry.Index {
		t.Fatalf("expected lastApplied=%d after error-producing entry, got %d", entry.Index, n.LastApplied())
	}
	if n.CommitIndex() != entry.Index {
		t.Fatalf("expected commitIndex=%d to be unaffected, got %d", entry.Index, n.CommitIndex())
	}
}

// Test 7: Write-waiter identity test — supersede path (I-019).
func TestPendingWriteIdentitySuperseded(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	sm := storage.NewKVStateMachine()
	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        []string{"p1", "p2"},
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{boot: 1},
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Register a PendingWrite at index 5 with RequestID "req-original"
	pw := &PendingWrite{
		RequestID: "req-original",
		Index:     5,
		Term:      1,
		Done:      make(chan CommandResult, 1),
	}
	n.mu.Lock()
	n.pendingWrites[5] = pw

	// Now simulate applier resolving index 5, but the committed entry has a DIFFERENT RequestID
	differentEntry := &raftv1.LogEntry{
		Index: 5,
		Term:  2,
		Command: &raftv1.Command{
			OperationType: "SET",
			Key:           "k",
			Value:         []byte("v"),
			RequestId:     "req-replacement",
		},
	}

	n.resolvePendingWriteLocked(5, differentEntry, storage.CommandResult{}, nil)
	n.mu.Unlock()

	select {
	case res := <-pw.Done:
		if res.Err != ErrSuperseded {
			t.Fatalf("expected ErrSuperseded, got %v", res.Err)
		}
	default:
		t.Fatalf("expected pw.Done to have resolved with ErrSuperseded")
	}
}

// Test 7b: Write-waiter leadership-loss cleanup — stepDown path (I-019).
func TestPendingWriteLeadershipLossCleanup(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1", "p2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  logStore,
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 2

	pw1 := &PendingWrite{RequestID: "r1", Index: 2, Term: 2, Done: make(chan CommandResult, 1)}
	pw2 := &PendingWrite{RequestID: "r2", Index: 3, Term: 2, Done: make(chan CommandResult, 1)}
	n.pendingWrites[2] = pw1
	n.pendingWrites[3] = pw2

	// Step down to term 3
	n.stepDownLocked(3)
	n.mu.Unlock()

	// Assert both waiters resolved with ErrLeadershipLost
	select {
	case res := <-pw1.Done:
		if res.Err != ErrLeadershipLost {
			t.Fatalf("expected ErrLeadershipLost on pw1, got %v", res.Err)
		}
	default:
		t.Fatalf("expected pw1 to be resolved")
	}

	select {
	case res := <-pw2.Done:
		if res.Err != ErrLeadershipLost {
			t.Fatalf("expected ErrLeadershipLost on pw2, got %v", res.Err)
		}
	default:
		t.Fatalf("expected pw2 to be resolved")
	}

	n.mu.Lock()
	if len(n.pendingWrites) != 0 {
		t.Fatalf("expected pendingWrites to be empty after stepDown, got len=%d", len(n.pendingWrites))
	}
	n.mu.Unlock()
}

// Test 8: PendingWrite cleanup on client cancellation:
// Context cancellation removes waiter from map; subsequent apply is a no-op.
func TestPendingWriteContextCancellationCleanup(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	sm := storage.NewKVStateMachine()
	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        []string{"p1", "p2"},
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{boot: 1},
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	cmd := &raftv1.Command{
		OperationType: "SET",
		Key:           "cancel-k",
		Value:         []byte("val"),
		RequestId:     "req-cancel-1",
	}

	// Write will time out because no quorum replication occurs
	_, err = n.Write(ctx, cmd)
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded, got %v", err)
	}

	// Verify pendingWrites has removed index
	n.mu.Lock()
	if len(n.pendingWrites) != 0 {
		t.Fatalf("expected pendingWrites to be cleaned up after cancellation, got %d", len(n.pendingWrites))
	}

	// Now simulate subsequent apply at index 2 (NOOP was at index 1)
	entry, _ := logStore.Get(2)
	n.resolvePendingWriteLocked(2, entry, storage.CommandResult{}, nil)
	n.mu.Unlock()
	// No panic, no deadlock — verified.
}

// Test 9: Done-channel non-blocking test (under -race):
// Capacity-1 makes applier non-blocking even with no reader.
func TestPendingWriteDoneChannelNonBlocking(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	sm := storage.NewKVStateMachine()
	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        []string{"p1", "p2"},
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{boot: 1},
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	pw := &PendingWrite{
		RequestID: "abandoned-req",
		Index:     10,
		Term:      1,
		Done:      make(chan CommandResult, 1),
	}
	n.mu.Lock()
	n.pendingWrites[10] = pw

	entry := &raftv1.LogEntry{
		Index: 10,
		Term:  1,
		Command: &raftv1.Command{
			OperationType: "SET",
			Key:           "k",
			Value:         []byte("v"),
			RequestId:     "abandoned-req",
		},
	}

	doneChan := make(chan struct{})
	// Outer test holds n.mu; goroutine calls resolvePendingWriteLocked without re-locking, mirroring applierLoop's lock-hold pattern.
	go func() {
		n.resolvePendingWriteLocked(10, entry, storage.CommandResult{}, nil)
		close(doneChan)
	}()

	select {
	case <-doneChan:
		// Send completed immediately without blocking
	case <-time.After(500 * time.Millisecond):
		t.Fatalf("resolvePendingWriteLocked blocked on Done channel with no reader")
	}
	n.mu.Unlock()
}

// Test 16: Follower commitIndex clamped to lastNewEntryIndex, not local log length (§5.3 / I-008).
func TestFollowerCommitIndexClampedToLastNewEntryIndex(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
	// Follower has local log entries up to index 5
	followerStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "r1"),
		makeTestEntry(2, 1, "r2"),
		makeTestEntry(3, 1, "r3"),
		makeTestEntry(4, 1, "r4"),
		makeTestEntry(5, 1, "r5"),
	})

	follower, err := NewNode(Config{
		ID:        "follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  followerStore,
	})
	if err != nil {
		t.Fatal(err)
	}

	follower.mu.Lock()
	follower.state.currentTerm = 1
	follower.state.commitIndex = 0
	follower.mu.Unlock()

	// Leader sends an empty heartbeat with PrevLogIndex=2, LeaderCommit=10
	req := &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []*raftv1.LogEntry{}, // empty entries
		LeaderCommit: 10,
	}

	resp, err := follower.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatalf("expected AppendEntries to succeed, got %v", resp)
	}

	// lastNewEntryIndex = req.PrevLogIndex + len(req.Entries) = 2 + 0 = 2.
	// commitIndex must be min(10, 2) = 2, NOT min(10, 5) = 5!
	if follower.CommitIndex() != 2 {
		t.Fatalf("commitIndex clamp bug: expected commitIndex=2, got %d", follower.CommitIndex())
	}
}

// Test 18: Idempotent Node.Stop() safety.
func TestIdempotentNodeStop(t *testing.T) {
	n, err := NewNode(Config{
		ID:        "node",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  storage.NewInMemoryLogStore(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	// First Stop
	n.Stop()

	// Second Stop (must be a safe no-op)
	n.Stop()

	// Third Stop (must be a safe no-op)
	n.Stop()
}

// Test 19: Late AppendEntries response after Node.Stop() does not panic.
func TestLateAppendEntriesResponseAfterStopDoesNotPanic(t *testing.T) {
	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  storage.NewInMemoryLogStore(),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 1
	n.state.matchIndex = make(map[string]uint64)
	n.state.nextIndex = make(map[string]uint64)
	n.replicationAttempt["p1"] = 1
	n.mu.Unlock()

	// Stop node
	n.Stop()

	// Late response arrives after Stop
	req := &raftv1.AppendEntriesRequest{Term: 1, LeaderId: "leader", PrevLogIndex: 0}
	resp := &raftv1.AppendEntriesResponse{Term: 1, Success: true}

	n.mu.Lock()
	// Must not panic or double-close notify channels
	n.handleAppendEntriesResponseLocked("p1", req, resp, 1)
	n.mu.Unlock()
}

// Test 20: AppendLocalEntry returns ErrNodeStopped after Node.Stop().
func TestAppendLocalEntryReturnsErrNodeStoppedAfterStop(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  logStore,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()

	n.Stop()

	cmd := &raftv1.Command{OperationType: "SET", Key: "k", Value: []byte("v"), RequestId: "r1"}
	entry, err := n.AppendLocalEntry(cmd)
	if err != ErrNodeStopped {
		t.Fatalf("expected ErrNodeStopped, got err=%v, entry=%v", err, entry)
	}

	if logStore.LastIndex() != 1 { // index 1 was NOOP on becoming leader before stop
		if logStore.LastIndex() > 1 {
			t.Fatalf("expected no new entry appended after stop, got lastIndex=%d", logStore.LastIndex())
		}
	}
}
