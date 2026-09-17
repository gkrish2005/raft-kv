package raft

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

type mockTransport struct {
	mu                    sync.Mutex
	sendAppendEntriesFunc func(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error)
	sendRequestVoteFunc   func(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error)
}

func (m *mockTransport) SendAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	m.mu.Lock()
	fn := m.sendAppendEntriesFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, peer, req)
	}
	return &raftv1.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

func (m *mockTransport) SendRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	m.mu.Lock()
	fn := m.sendRequestVoteFunc
	m.mu.Unlock()
	if fn != nil {
		return fn(ctx, peer, req)
	}
	return &raftv1.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

// Test 4: Read-barrier blocks until lastApplied catches up.
func TestReadBarrierBlocksUntilLastAppliedCatchesUp(t *testing.T) {
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()

	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        nil, // single node cluster (self-quorum)
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    &mockTransport{},
		Store:        &memoryStore{term: 1, boot: 1},
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
	// Commit and apply NOOP (index 1) so readReadyTerm becomes 1
	n.state.commitIndex = 1
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	for n.ReadReadyTerm() != 1 {
		time.Sleep(5 * time.Millisecond)
	}

	// Hold sm.Lock() to freeze applier from advancing lastApplied past 1
	sm.Lock()

	entry2, err := n.AppendLocalEntry(&raftv1.Command{
		OperationType: "SET",
		Key:           "k",
		Value:         []byte("v-barrier"),
		RequestId:     "r2",
	})
	if err != nil {
		sm.Unlock()
		t.Fatal(err)
	}

	// Advance commitIndex to 2 (commitIndex=2 > lastApplied=1)
	n.mu.Lock()
	n.state.commitIndex = entry2.Index
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	readDone := make(chan struct{})
	var readVal []byte
	var readFound bool
	var readErr error

	go func() {
		readVal, readFound, readErr = n.LinearizableGet(context.Background(), "k")
		close(readDone)
	}()

	// Assert read is blocked in barrier wait
	select {
	case <-readDone:
		sm.Unlock()
		t.Fatalf("expected LinearizableGet to block while lastApplied < commitIndex")
	case <-time.After(50 * time.Millisecond):
		// Expected to remain blocked
	}

	// Release sm.Lock(), allowing applierLoop to apply entry 2 and advance lastApplied to 2
	sm.Unlock()

	select {
	case <-readDone:
		if readErr != nil {
			t.Fatalf("LinearizableGet failed: %v", readErr)
		}
		if !readFound || string(readVal) != "v-barrier" {
			t.Fatalf("expected 'v-barrier', got val=%s found=%v", string(readVal), readFound)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for LinearizableGet to complete")
	}
}

// Test 5: Read term-revalidation test:
// Higher-term response during read barrier forces step-down; pending GET fails with ErrLeaderChanged.
func TestReadTermRevalidationFailsOnLeadershipLoss(t *testing.T) {
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()

	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        nil,
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    &mockTransport{},
		Store:        &memoryStore{term: 1, boot: 1},
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
	// Commit and apply NOOP (index 1) so readReadyTerm becomes 1
	n.state.commitIndex = 1
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	for n.ReadReadyTerm() != 1 {
		time.Sleep(5 * time.Millisecond)
	}

	// Hold sm.Lock() to freeze applier
	sm.Lock()

	entry2, err := n.AppendLocalEntry(&raftv1.Command{
		OperationType: "SET",
		Key:           "k",
		Value:         []byte("v"),
		RequestId:     "r2",
	})
	if err != nil {
		sm.Unlock()
		t.Fatal(err)
	}

	// Advance commitIndex to 2
	n.mu.Lock()
	n.state.commitIndex = entry2.Index
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	readDone := make(chan error, 1)
	go func() {
		_, _, err := n.LinearizableGet(context.Background(), "k")
		readDone <- err
	}()

	// Wait briefly to ensure it is parked in barrier wait
	time.Sleep(30 * time.Millisecond)

	// Step down leader to term 2 while read is parked in barrier wait
	n.mu.Lock()
	n.stepDownLocked(2)
	n.mu.Unlock()

	sm.Unlock() // unfreeze applier

	select {
	case err := <-readDone:
		if err != ErrLeaderChanged {
			t.Fatalf("expected ErrLeaderChanged, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for LinearizableGet to fail on step-down")
	}
}

// Test 10: New-leader no-op read — Test A (I-023).
// Old leader commits a write and crashes; a new leader is elected;
// issue GET immediately after election, before any client write in the new term —
// assert it observes the old leader's committed write.
func TestNewLeaderNoOpReadTestA(t *testing.T) {
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()

	// Old leader committed write SET key="x" val="prev-val" in term 1
	logStore.Append([]*raftv1.LogEntry{
		{
			Index: 1,
			Term:  1,
			Command: &raftv1.Command{
				OperationType: "SET",
				Key:           "x",
				Value:         []byte("prev-val"),
				RequestId:     "req-old-1",
			},
		},
	})

	n, err := NewNode(Config{
		ID:           "node-2",
		Peers:        nil,
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    &mockTransport{},
		Store:        &memoryStore{term: 2, boot: 1},
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

	// Node 2 becomes leader in term 2
	n.mu.Lock()
	n.becomeLeaderLocked()
	// New leader automatically appended NOOP at index 2
	noopIdx := n.leaderNoOpIndex
	if noopIdx != 2 {
		t.Fatalf("expected leaderNoOpIndex=2, got %d", noopIdx)
	}

	// Commit the NOOP (and transitively index 1)
	n.state.commitIndex = 2
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	// Issue GET immediately (before any client write in the new term)
	val, found, err := n.LinearizableGet(context.Background(), "x")
	if err != nil {
		t.Fatalf("LinearizableGet failed: %v", err)
	}
	if !found || string(val) != "prev-val" {
		t.Fatalf("expected to observe old leader's committed write 'prev-val', got found=%v val=%s", found, string(val))
	}
}

// Test 11: New-leader read-blocked-before-no-op — Test B (I-023).
// A new leader is elected; before its no-op has committed/applied, issue a GET —
// assert it does NOT complete (blocks on readReadyTerm gate) until the no-op commits and applies.
func TestNewLeaderReadBlockedBeforeNoOpTestB(t *testing.T) {
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()

	n, err := NewNode(Config{
		ID:           "node-new",
		Peers:        nil,
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    &mockTransport{},
		Store:        &memoryStore{term: 1, boot: 1},
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
	// NOOP is at index 1, but commitIndex and lastApplied are 0
	// Crucially: readReadyTerm is 0!
	n.state.commitIndex = 0
	n.state.lastApplied = 0
	n.mu.Unlock()

	readDone := make(chan struct{})
	var readErr error

	go func() {
		_, _, readErr = n.LinearizableGet(context.Background(), "any")
		close(readDone)
	}()

	// Assert read is blocked on readReadyTerm gate (even though commitIndex=0 and lastApplied=0)
	select {
	case <-readDone:
		t.Fatalf("Test B failed: GET completed before NOOP committed/applied! readReadyTerm gate was bypassed")
	case <-time.After(50 * time.Millisecond):
		// Correctly blocked!
	}

	// Now commit and apply the NOOP at index 1
	n.mu.Lock()
	n.state.commitIndex = 1
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	// Wait for GET to complete now that no-op has applied and readReadyTerm is set
	select {
	case <-readDone:
		if readErr != nil {
			t.Fatalf("LinearizableGet failed after NOOP commit: %v", readErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for GET to complete after NOOP commit")
	}

	if n.ReadReadyTerm() != n.Term() {
		t.Fatalf("expected readReadyTerm == %d, got %d", n.Term(), n.ReadReadyTerm())
	}
}

// Test 12: Exactly-one-no-op per election (I-023).
func TestExactlyOneNoOpPerElection(t *testing.T) {
	logStore := storage.NewInMemoryLogStore()
	n, err := NewNode(Config{
		ID:        "leader",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: &mockTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  logStore,
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	firstNoOpIdx := n.leaderNoOpIndex
	if firstNoOpIdx != 1 {
		t.Fatalf("expected first no-op at index 1, got %d", firstNoOpIdx)
	}

	// Double-invocation within the same term must not double-append NOOP
	n.becomeLeaderLocked()
	n.mu.Unlock()

	if logStore.LastIndex() != 1 {
		t.Fatalf("expected exactly 1 NOOP entry in log, got %d", logStore.LastIndex())
	}
	e, _ := logStore.Get(1)
	if e.Command.OperationType != "NOOP" {
		t.Fatalf("expected NOOP command, got %s", e.Command.OperationType)
	}
}

// Test 13: Read cancellation under partition.
func TestReadCancellationUnderPartition(t *testing.T) {
	// 3-node cluster. Transport fails/delays so quorum confirmation cannot complete.
	transport := &mockTransport{
		sendAppendEntriesFunc: func(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1", "p2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: transport,
		Store:     &memoryStore{boot: 1},
		LogStore:  storage.NewInMemoryLogStore(),
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	_, _, err = n.LinearizableGet(ctx, "k")
	if err != context.DeadlineExceeded {
		t.Fatalf("expected context.DeadlineExceeded under partition, got %v", err)
	}
}

// Test 14: Read unblocks promptly on Node.Stop().
func TestReadUnblocksPromptlyOnNodeStop(t *testing.T) {
	transport := &mockTransport{
		sendAppendEntriesFunc: func(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
			// Hang indefinitely
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1", "p2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: transport,
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
	n.becomeLeaderLocked()
	n.mu.Unlock()

	readDone := make(chan error, 1)
	go func() {
		_, _, err := n.LinearizableGet(context.Background(), "k")
		readDone <- err
	}()

	time.Sleep(30 * time.Millisecond)

	// Stop node
	n.Stop()

	select {
	case err := <-readDone:
		if err != ErrNodeStopped {
			t.Fatalf("expected ErrNodeStopped on Stop(), got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for LinearizableGet to unblock on Node.Stop()")
	}
}

// Test 15: Read step 2 unblocks promptly on leadership loss.
func TestReadStep2UnblocksPromptlyOnLeadershipLoss(t *testing.T) {
	transport := &mockTransport{
		sendAppendEntriesFunc: func(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1", "p2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: transport,
		Store:     &memoryStore{boot: 1},
		LogStore:  storage.NewInMemoryLogStore(),
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()

	readDone := make(chan error, 1)
	go func() {
		err := n.ConfirmLeadershipQuorum(context.Background())
		readDone <- err
	}()

	time.Sleep(30 * time.Millisecond)

	// Leader steps down
	n.mu.Lock()
	n.stepDownLocked(n.state.currentTerm + 1)
	n.mu.Unlock()

	select {
	case err := <-readDone:
		if err != ErrLeaderChanged {
			t.Fatalf("expected ErrLeaderChanged on stepDown, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("timed out waiting for confirmLeadershipQuorum to unblock on stepDown")
	}
}

// Test 17: Concurrent read confirmation and heartbeat ticker never create duplicate in-flight RPCs.
func TestConcurrentReadConfirmationAndHeartbeatTickerNoDuplicateRPC(t *testing.T) {
	var inFlightCount int32
	var maxInFlight int32

	transport := &mockTransport{
		sendAppendEntriesFunc: func(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
			cur := atomic.AddInt32(&inFlightCount, 1)
			defer atomic.AddInt32(&inFlightCount, -1)

			for {
				oldMax := atomic.LoadInt32(&maxInFlight)
				if cur <= oldMax || atomic.CompareAndSwapInt32(&maxInFlight, oldMax, cur) {
					break
				}
			}

			// Simulate slight network delay
			time.Sleep(2 * time.Millisecond)
			return &raftv1.AppendEntriesResponse{Term: req.Term, Success: true}, nil
		},
	}

	n, err := NewNode(Config{
		ID:         "leader",
		Peers:      []string{"p1"},
		Clock:      NewFakeClock(time.Unix(0, 0)),
		Transport:  transport,
		Store:      &memoryStore{boot: 1},
		LogStore:   storage.NewInMemoryLogStore(),
		RPCTimeout: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.mu.Unlock()

	var wg sync.WaitGroup
	// Concurrently invoke confirmLeadershipQuorum and simulate heartbeat ticker triggers
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			_ = n.ConfirmLeadershipQuorum(ctx)
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.mu.Lock()
			if !n.peerInFlight["p1"] {
				go n.replicateToPeer("p1")
			}
			n.mu.Unlock()
		}()
	}

	wg.Wait()

	maxObserved := atomic.LoadInt32(&maxInFlight)
	if maxObserved > 1 {
		t.Fatalf("at-most-one-in-flight violation: max concurrent AppendEntries RPCs to p1 was %d, expected <= 1", maxObserved)
	}
}

// Test 21: Stale AppendEntries response does not update confirmedAttempt (I-021 / I-016).
func TestStaleAppendEntriesResponseDoesNotUpdateConfirmedAttempt(t *testing.T) {
	n, err := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"p1"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: &mockTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  storage.NewInMemoryLogStore(),
	})
	if err != nil {
		t.Fatal(err)
	}

	n.mu.Lock()
	n.becomeLeaderLocked()
	n.state.currentTerm = 2
	n.replicationAttempt["p1"] = 5
	n.confirmedAttempt["p1"] = 0

	req := &raftv1.AppendEntriesRequest{Term: 2, LeaderId: "leader", PrevLogIndex: 0}
	resp := &raftv1.AppendEntriesResponse{Term: 2, Success: true}

	// 1. Stale attempt (attempt 4 != replicationAttempt 5)
	n.handleAppendEntriesResponseLocked("p1", req, resp, 4)
	if n.confirmedAttempt["p1"] != 0 {
		t.Fatalf("stale attempt updated confirmedAttempt: expected 0, got %d", n.confirmedAttempt["p1"])
	}

	// 2. Stale term (resp.Term 1 < currentTerm 2)
	staleTermResp := &raftv1.AppendEntriesResponse{Term: 1, Success: true}
	n.handleAppendEntriesResponseLocked("p1", req, staleTermResp, 5)
	if n.confirmedAttempt["p1"] != 0 {
		t.Fatalf("stale term updated confirmedAttempt: expected 0, got %d", n.confirmedAttempt["p1"])
	}

	// 3. Valid attempt and current term: confirmedAttempt must update to 5
	n.handleAppendEntriesResponseLocked("p1", req, resp, 5)
	if n.confirmedAttempt["p1"] != 5 {
		t.Fatalf("valid attempt failed to update confirmedAttempt: expected 5, got %d", n.confirmedAttempt["p1"])
	}
	n.mu.Unlock()
}

// Test 17: TestLinearizableGet_PreCancelledContextReturnsError proves that LinearizableGet
// immediately returns ctx.Err() when invoked with an expired or pre-cancelled context,
// even on single-node clusters where quorum confirmation and barrier waits are instantaneous (I-016).
func TestLinearizableGet_PreCancelledContextReturnsError(t *testing.T) {
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()

	n, err := NewNode(Config{
		ID:           "leader",
		Peers:        nil, // single node
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    &mockTransport{},
		Store:        &memoryStore{term: 1, boot: 1},
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
	n.state.commitIndex = 1
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	for n.ReadReadyTerm() != 1 || n.LastApplied() < 1 {
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	val, found, err := n.LinearizableGet(ctx, "k")
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled on pre-cancelled context, got err=%v, val=%v, found=%v", err, val, found)
	}
}
