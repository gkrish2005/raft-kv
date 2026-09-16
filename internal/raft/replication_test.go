package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

func makeTestEntry(idx uint64, term uint64, reqID string) *raftv1.LogEntry {
	return &raftv1.LogEntry{
		Index: idx,
		Term:  term,
		Command: &raftv1.Command{
			OperationType: "SET",
			Key:           "k",
			Value:         []byte("v"),
			RequestId:     reqID,
		},
	}
}

// Scenario 1: Follower log empty, leader sends entry 1 -> success
func TestAppendEntriesFollowerEmpty(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
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
	follower.state.currentTerm = 1

	req := &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raftv1.LogEntry{makeTestEntry(1, 1, "req-1")},
	}

	resp, err := follower.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got %+v", resp)
	}
	if followerStore.LastIndex() != 1 {
		t.Fatalf("expected follower lastIndex=1, got %d", followerStore.LastIndex())
	}
	e, _ := followerStore.Get(1)
	if e.Index != 1 || e.Term != 1 || e.Command.RequestId != "req-1" {
		t.Fatalf("unexpected entry in follower log: %+v", e)
	}
}

// Scenario 2: Follower missing entries (prevLogIndex > follower.lastLogIndex)
// Returns ConflictIndex = lastLogIndex + 1, ConflictTerm = 0
func TestAppendEntriesFollowerMissingEntriesFastBacktrack(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
	_ = followerStore.Append([]*raftv1.LogEntry{makeTestEntry(1, 1, "req-1")})

	follower, _ := NewNode(Config{
		ID:        "follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  followerStore,
	})
	follower.state.currentTerm = 1

	req := &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 5, // follower only has 1
		PrevLogTerm:  1,
		Entries:      []*raftv1.LogEntry{makeTestEntry(6, 1, "req-6")},
	}

	resp, err := follower.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatal("expected success=false for missing prevLogIndex")
	}
	if resp.ConflictIndex != 2 || resp.ConflictTerm != 0 {
		t.Fatalf("expected ConflictIndex=2, ConflictTerm=0; got ConflictIndex=%d, ConflictTerm=%d",
			resp.ConflictIndex, resp.ConflictTerm)
	}
}

// Scenario 3: Follower has conflicting term at prevLogIndex
// Returns ConflictTerm = follower.log[prevLogIndex].Term and ConflictIndex = first index of that term
func TestAppendEntriesFollowerTermMismatchFastBacktrack(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
	// Follower has entries: index 1 (term 1), index 2 (term 1), index 3 (term 2), index 4 (term 2)
	_ = followerStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "req-1"),
		makeTestEntry(2, 1, "req-2"),
		makeTestEntry(3, 2, "req-3"),
		makeTestEntry(4, 2, "req-4"),
	})

	follower, _ := NewNode(Config{
		ID:        "follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  followerStore,
	})
	follower.state.currentTerm = 3

	// Leader sends AppendEntries with prevLogIndex=4, prevLogTerm=3 (follower has term 2 at index 4)
	req := &raftv1.AppendEntriesRequest{
		Term:         3,
		LeaderId:     "leader",
		PrevLogIndex: 4,
		PrevLogTerm:  3,
		Entries:      []*raftv1.LogEntry{makeTestEntry(5, 3, "req-5")},
	}

	resp, err := follower.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Success {
		t.Fatal("expected success=false for term mismatch")
	}
	// ConflictTerm should be 2, ConflictIndex should be 3 (first index with term 2)
	if resp.ConflictTerm != 2 || resp.ConflictIndex != 3 {
		t.Fatalf("expected ConflictTerm=2, ConflictIndex=3; got ConflictTerm=%d, ConflictIndex=%d",
			resp.ConflictTerm, resp.ConflictIndex)
	}
}

// Scenario 4: Fast-backtrack leader resolution when leader HAS ConflictTerm
func TestFastBacktrackLeaderHasConflictTerm(t *testing.T) {
	leaderStore := storage.NewInMemoryLogStore()
	// Leader log: [1: term 1, 2: term 1, 3: term 2, 4: term 2, 5: term 3]
	_ = leaderStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "1"),
		makeTestEntry(2, 1, "2"),
		makeTestEntry(3, 2, "3"),
		makeTestEntry(4, 2, "4"),
		makeTestEntry(5, 3, "5"),
	})

	leader, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  leaderStore,
	})
	leader.state.role = Leader
	leader.state.currentTerm = 3
	leader.state.nextIndex = map[string]uint64{"follower": 6}
	leader.state.matchIndex = map[string]uint64{"follower": 0, "leader": 5}
	leader.replicationAttempt = map[string]uint64{"follower": 1}

	// Follower responded with ConflictTerm=2, ConflictIndex=3
	resp := &raftv1.AppendEntriesResponse{
		Term:          3,
		Success:       false,
		ConflictIndex: 3,
		ConflictTerm:  2,
	}

	leader.HandleAppendEntriesResponse("follower", &raftv1.AppendEntriesRequest{
		Term:         3,
		PrevLogIndex: 5,
		PrevLogTerm:  3,
	}, resp, 1)

	// Leader has term 2 up to index 4 -> nextIndex should be set to 4 + 1 = 5
	if leader.NextIndex("follower") != 5 {
		t.Fatalf("expected nextIndex=5, got %d", leader.NextIndex("follower"))
	}
}

// Scenario 5: Fast-backtrack leader resolution when leader LACKS ConflictTerm
func TestFastBacktrackLeaderLacksConflictTerm(t *testing.T) {
	leaderStore := storage.NewInMemoryLogStore()
	// Leader log has only term 3 entries: [1: term 1, 2: term 3, 3: term 3]
	_ = leaderStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "1"),
		makeTestEntry(2, 3, "2"),
		makeTestEntry(3, 3, "3"),
	})

	leader, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  leaderStore,
	})
	leader.state.role = Leader
	leader.state.currentTerm = 3
	leader.state.nextIndex = map[string]uint64{"follower": 4}
	leader.state.matchIndex = map[string]uint64{"follower": 0, "leader": 3}
	leader.replicationAttempt = map[string]uint64{"follower": 1}

	// Follower responded with ConflictTerm=2, ConflictIndex=2 (leader has no term 2)
	resp := &raftv1.AppendEntriesResponse{
		Term:          3,
		Success:       false,
		ConflictIndex: 2,
		ConflictTerm:  2,
	}

	leader.HandleAppendEntriesResponse("follower", &raftv1.AppendEntriesRequest{
		Term:         3,
		PrevLogIndex: 3,
		PrevLogTerm:  3,
	}, resp, 1)

	// Leader lacks term 2 -> nextIndex should fall back to ConflictIndex = 2
	if leader.NextIndex("follower") != 2 {
		t.Fatalf("expected nextIndex=2, got %d", leader.NextIndex("follower"))
	}
}

// Scenario 6: Follower truncates conflicting uncommitted suffix and appends new entries
func TestAppendEntriesTruncatesConflictingSuffix(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
	// Follower has [1: term 1, 2: term 1, 3: term 1] (uncommitted suffix at 3)
	_ = followerStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "1"),
		makeTestEntry(2, 1, "2"),
		makeTestEntry(3, 1, "uncommitted-old"),
	})

	follower, _ := NewNode(Config{
		ID:        "follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  followerStore,
	})
	follower.state.currentTerm = 2

	// Leader sends prevLogIndex=2, prevLogTerm=1, entries=[3: term 2, 4: term 2]
	req := &raftv1.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries: []*raftv1.LogEntry{
			makeTestEntry(3, 2, "committed-new-3"),
			makeTestEntry(4, 2, "committed-new-4"),
		},
	}

	resp, err := follower.AppendEntries(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true, got %+v", resp)
	}

	if followerStore.LastIndex() != 4 {
		t.Fatalf("expected follower lastIndex=4, got %d", followerStore.LastIndex())
	}

	e3, _ := followerStore.Get(3)
	if e3.Term != 2 || e3.Command.RequestId != "committed-new-3" {
		t.Fatalf("entry 3 not replaced correctly: %+v", e3)
	}
	e4, _ := followerStore.Get(4)
	if e4.Term != 2 || e4.Command.RequestId != "committed-new-4" {
		t.Fatalf("entry 4 not appended correctly: %+v", e4)
	}
}

// Invariant I-002 & I-022: Leader local append updates matchIndex[self] == lastLogIndex
func TestLeaderLocalAppendUpdatesMatchIndexSelf(t *testing.T) {
	leaderStore := storage.NewInMemoryLogStore()
	leader, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower-1", "follower-2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  leaderStore,
	})
	leader.mu.Lock()
	leader.becomeLeaderLocked()
	leader.mu.Unlock()

	if leader.MatchIndex("leader") != 1 {
		t.Fatalf("expected initial matchIndex[self]=1 (NOOP entry), got %d", leader.MatchIndex("leader"))
	}

	entry1, err := leader.AppendLocalEntry(&raftv1.Command{OperationType: "SET", Key: "a", Value: []byte("1"), RequestId: "r1"})
	if err != nil {
		t.Fatal(err)
	}
	if entry1.Index != 2 || leader.MatchIndex("leader") != 2 || leaderStore.LastIndex() != 2 {
		t.Fatalf("mismatch after first append: entry=%+v matchIndex=%d lastIndex=%d",
			entry1, leader.MatchIndex("leader"), leaderStore.LastIndex())
	}

	entry2, err := leader.AppendLocalEntry(&raftv1.Command{OperationType: "SET", Key: "b", Value: []byte("2"), RequestId: "r2"})
	if err != nil {
		t.Fatal(err)
	}
	if entry2.Index != 3 || leader.MatchIndex("leader") != 3 || leaderStore.LastIndex() != 3 {
		t.Fatalf("mismatch after second append: entry=%+v matchIndex=%d lastIndex=%d",
			entry2, leader.MatchIndex("leader"), leaderStore.LastIndex())
	}
}

// Invariant I-013: A follower's replication only counts toward matchIndex after that follower's own WAL fsync completes
func TestFollowerFsyncDelayedMatchIndexDoesNotAdvanceUntilComplete(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
	fsyncBlocked := make(chan struct{})
	fsyncDone := make(chan struct{})

	// Hook to simulate a delayed fsync in follower storage
	followerStore.SetAppendHook(func(entries []*raftv1.LogEntry) error {
		<-fsyncBlocked
		close(fsyncDone)
		return nil
	})

	follower, _ := NewNode(Config{
		ID:        "follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  followerStore,
	})
	follower.state.currentTerm = 1

	leaderStore := storage.NewInMemoryLogStore()
	_ = leaderStore.Append([]*raftv1.LogEntry{makeTestEntry(1, 1, "req-1")})

	leader, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  leaderStore,
	})
	leader.mu.Lock()
	leader.state.currentTerm = 1
	leader.becomeLeaderLocked()
	leader.replicationAttempt["follower"] = 1
	leader.mu.Unlock()

	if leader.MatchIndex("follower") != 0 {
		t.Fatalf("expected matchIndex[follower]=0 initially")
	}

	req := &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      []*raftv1.LogEntry{makeTestEntry(1, 1, "req-1")},
	}

	var resp *raftv1.AppendEntriesResponse
	var followerErr error
	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		resp, followerErr = follower.AppendEntries(context.Background(), req)
	}()

	// While fsync is blocked, assert matchIndex has NOT advanced
	time.Sleep(10 * time.Millisecond)
	if leader.MatchIndex("follower") != 0 {
		t.Fatalf("matchIndex advanced before follower fsync completed!")
	}

	// Unblock follower fsync
	close(fsyncBlocked)
	wg.Wait()

	if followerErr != nil || !resp.Success {
		t.Fatalf("follower failed: %v resp: %+v", followerErr, resp)
	}

	// Now deliver response to leader
	leader.HandleAppendEntriesResponse("follower", req, resp, 1)

	if leader.MatchIndex("follower") != 1 {
		t.Fatalf("expected matchIndex[follower]=1 after response, got %d", leader.MatchIndex("follower"))
	}
}

// Finding #2 test: Empty entries slice (heartbeat/probe) handles matching & mismatched prevLogIndex correctly without log mutation
func TestAppendEntriesEmptyEntriesHeartbeat(t *testing.T) {
	followerStore := storage.NewInMemoryLogStore()
	_ = followerStore.Append([]*raftv1.LogEntry{
		makeTestEntry(1, 1, "req-1"),
		makeTestEntry(2, 1, "req-2"),
	})

	follower, _ := NewNode(Config{
		ID:        "follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  followerStore,
	})
	follower.state.currentTerm = 2

	// 1. Heartbeat matching follower's log tip (prevLogIndex=2, prevLogTerm=1)
	reqValid := &raftv1.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		PrevLogIndex: 2,
		PrevLogTerm:  1,
		Entries:      []*raftv1.LogEntry{}, // empty slice
	}

	resp, err := follower.AppendEntries(context.Background(), reqValid)
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if !resp.Success {
		t.Fatalf("expected success=true for matching heartbeat, got %+v", resp)
	}
	if followerStore.LastIndex() != 2 {
		t.Fatalf("expected follower log unchanged at lastIndex=2, got %d", followerStore.LastIndex())
	}

	// 2. Heartbeat on empty follower log (prevLogIndex=0, prevLogTerm=0)
	emptyStore := storage.NewInMemoryLogStore()
	emptyFollower, _ := NewNode(Config{
		ID:        "empty-follower",
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     &memoryStore{boot: 1},
		LogStore:  emptyStore,
	})
	emptyFollower.state.currentTerm = 1

	reqEmpty := &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "leader",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
	}
	respEmpty, err := emptyFollower.AppendEntries(context.Background(), reqEmpty)
	if err != nil {
		t.Fatalf("AppendEntries on empty log failed: %v", err)
	}
	if !respEmpty.Success || emptyStore.LastIndex() != 0 {
		t.Fatalf("expected success=true, lastIndex=0; got resp=%+v lastIndex=%d", respEmpty, emptyStore.LastIndex())
	}

	// 3. Heartbeat with mismatching prevLogIndex (prevLogIndex=5)
	reqMismatch := &raftv1.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "leader",
		PrevLogIndex: 5,
		PrevLogTerm:  2,
		Entries:      []*raftv1.LogEntry{},
	}
	respMismatch, err := follower.AppendEntries(context.Background(), reqMismatch)
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}
	if respMismatch.Success {
		t.Fatal("expected success=false for mismatching heartbeat prevLogIndex")
	}
	if respMismatch.ConflictIndex != 3 || respMismatch.ConflictTerm != 0 {
		t.Fatalf("expected ConflictIndex=3, ConflictTerm=0; got ConflictIndex=%d, ConflictTerm=%d",
			respMismatch.ConflictIndex, respMismatch.ConflictTerm)
	}
}

type interceptingTransport struct {
	mu          sync.Mutex
	onSend      func(req *raftv1.AppendEntriesRequest)
	holdCh      map[uint64]chan struct{}
	appendCalls int
}

func (t *interceptingTransport) SendRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return &raftv1.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}

func (t *interceptingTransport) SendAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	t.mu.Lock()
	t.appendCalls++
	onSend := t.onSend
	var hold chan struct{}
	if t.holdCh != nil {
		hold = t.holdCh[req.Term]
	}
	t.mu.Unlock()

	if onSend != nil {
		onSend(req)
	}

	if hold != nil {
		select {
		case <-hold:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	return &raftv1.AppendEntriesResponse{Term: req.Term, Success: true}, nil
}

// Finding #4 test: Stale RPC returning from a prior term/attempt does NOT clear peerInFlight for a new active attempt
func TestStaleRPCDoesNotClearPeerInFlightInNewTerm(t *testing.T) {
	holdRPC1 := make(chan struct{})
	holdRPC2 := make(chan struct{})
	rpc1Entered := make(chan struct{}, 1)
	rpc2Entered := make(chan struct{}, 1)

	tr := &interceptingTransport{
		holdCh: map[uint64]chan struct{}{
			2: holdRPC1,
			4: holdRPC2,
		},
		onSend: func(req *raftv1.AppendEntriesRequest) {
			if req.Term == 2 {
				select {
				case rpc1Entered <- struct{}{}:
				default:
				}
			} else if req.Term == 4 {
				select {
				case rpc2Entered <- struct{}{}:
				default:
				}
			}
		},
	}

	leaderStore := storage.NewInMemoryLogStore()
	leader, _ := NewNode(Config{
		ID:         "leader",
		Peers:      []string{"peer-B"},
		Clock:      NewFakeClock(time.Unix(0, 0)),
		Transport:  tr,
		Store:      &memoryStore{boot: 1},
		LogStore:   leaderStore,
		RPCTimeout: 5 * time.Second,
	})

	// 1. Leader in Term 2
	leader.mu.Lock()
	leader.state.currentTerm = 2
	leader.becomeLeaderLocked()
	leader.mu.Unlock()

	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		leader.replicateToPeer("peer-B")
	}()

	// Deterministically wait until RPC 1 has started and is blocked in Transport
	select {
	case <-rpc1Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RPC 1 to enter Transport")
	}

	leader.mu.Lock()
	if !leader.peerInFlight["peer-B"] {
		leader.mu.Unlock()
		t.Fatal("expected peerInFlight[peer-B]=true for Term 2 RPC 1")
	}
	leader.mu.Unlock()

	// 2. Node steps down on higher term 3, then becomes Leader in Term 4
	leader.mu.Lock()
	leader.stepDownLocked(3)
	leader.state.currentTerm = 4
	leader.becomeLeaderLocked()
	leader.mu.Unlock()

	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		leader.replicateToPeer("peer-B")
	}()

	// Deterministically wait until RPC 2 has started and is blocked in Transport
	select {
	case <-rpc2Entered:
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for RPC 2 to enter Transport")
	}

	leader.mu.Lock()
	if !leader.peerInFlight["peer-B"] {
		leader.mu.Unlock()
		t.Fatal("expected peerInFlight[peer-B]=true for Term 4 RPC 2")
	}
	leader.mu.Unlock()

	tr.mu.Lock()
	callsBefore := tr.appendCalls
	tr.mu.Unlock()

	// 3. Unblock RPC 1 (from Term 2) and deterministically wait for its goroutine to complete
	close(holdRPC1)
	wg1.Wait()

	// 4. Assert peerInFlight["peer-B"] is STILL true because RPC 2 is still in-flight!
	leader.mu.Lock()
	inFlight := leader.peerInFlight["peer-B"]
	leader.mu.Unlock()
	if !inFlight {
		t.Fatal("stale RPC 1 completion erroneously cleared peerInFlight for active Term 4 attempt!")
	}

	// 5. Trigger replicateToPeer again; it must NOT dispatch a second concurrent RPC while RPC 2 is active
	leader.replicateToPeer("peer-B")

	tr.mu.Lock()
	callsAfter := tr.appendCalls
	tr.mu.Unlock()

	if callsAfter != callsBefore {
		t.Fatalf("concurrent RPC was dispatched! callsBefore=%d, callsAfter=%d", callsBefore, callsAfter)
	}

	// 6. Unblock RPC 2 and deterministically wait for its goroutine to complete
	close(holdRPC2)
	wg2.Wait()

	leader.mu.Lock()
	inFlightFinal := leader.peerInFlight["peer-B"]
	leader.mu.Unlock()

	if inFlightFinal {
		t.Fatal("expected peerInFlight[peer-B]=false after active RPC 2 completed")
	}
}

