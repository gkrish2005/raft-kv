package raft

import (
	"testing"
	"time"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

// Invariant I-021: Lower term response is ignored completely
func TestStaleResponseLowerTermIgnored(t *testing.T) {
	store := &memoryStore{boot: 1}
	logStore := storage.NewInMemoryLogStore()
	n, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     store,
		LogStore:  logStore,
	})
	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 5
	n.state.nextIndex = map[string]uint64{"follower": 3}
	n.state.matchIndex = map[string]uint64{"follower": 2, "leader": 3}
	n.replicationAttempt = map[string]uint64{"follower": 1}
	n.mu.Unlock()

	// Late response from term 4
	resp := &raftv1.AppendEntriesResponse{
		Term:    4,
		Success: true,
	}
	req := &raftv1.AppendEntriesRequest{
		Term:         4,
		PrevLogIndex: 3,
		Entries:      []*raftv1.LogEntry{{Index: 4, Term: 4}},
	}

	n.HandleAppendEntriesResponse("follower", req, resp, 1)

	if n.Role() != Leader || n.Term() != 5 {
		t.Fatalf("expected role=Leader, term=5; got role=%s, term=%d", n.Role(), n.Term())
	}
	if n.MatchIndex("follower") != 2 || n.NextIndex("follower") != 3 {
		t.Fatalf("indices changed on stale lower-term response: match=%d next=%d",
			n.MatchIndex("follower"), n.NextIndex("follower"))
	}
}

// Invariant I-021 & I-007: Higher term response triggers step-down and does NOT mutate replication indices
func TestStaleResponseHigherTermStepsDownWithoutUpdatingReplicationIndices(t *testing.T) {
	store := &memoryStore{boot: 1}
	logStore := storage.NewInMemoryLogStore()
	n, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     store,
		LogStore:  logStore,
	})
	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 5
	n.state.nextIndex = map[string]uint64{"follower": 3}
	n.state.matchIndex = map[string]uint64{"follower": 2, "leader": 3}
	n.replicationAttempt = map[string]uint64{"follower": 1}
	n.mu.Unlock()

	// Response carries higher term 6
	resp := &raftv1.AppendEntriesResponse{
		Term:    6,
		Success: false,
	}
	req := &raftv1.AppendEntriesRequest{
		Term:         5,
		PrevLogIndex: 2,
		Entries:      []*raftv1.LogEntry{{Index: 3, Term: 5}},
	}

	n.HandleAppendEntriesResponse("follower", req, resp, 1)

	if n.Role() != Follower || n.Term() != 6 {
		t.Fatalf("expected node to step down to Follower with term 6, got role=%s, term=%d", n.Role(), n.Term())
	}
	// Check that replication indices were NOT modified based on this response
	if n.MatchIndex("follower") != 2 || n.NextIndex("follower") != 3 {
		t.Fatalf("replication indices mutated on higher-term response: match=%d next=%d",
			n.MatchIndex("follower"), n.NextIndex("follower"))
	}
	// Verify higher term was durably saved
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.saves) == 0 || store.saves[len(store.saves)-1].term != 6 {
		t.Fatalf("higher term was not durably persisted: %+v", store.saves)
	}
}

// Invariant I-021: Attempt ID mismatch drops late same-term response
func TestStaleResponseAttemptIDMismatchIgnored(t *testing.T) {
	store := &memoryStore{boot: 1}
	logStore := storage.NewInMemoryLogStore()
	n, _ := NewNode(Config{
		ID:        "leader",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     store,
		LogStore:  logStore,
	})
	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 5
	n.state.nextIndex = map[string]uint64{"follower": 2}
	n.state.matchIndex = map[string]uint64{"follower": 0, "leader": 3}
	// Current active attempt is 2 (e.g. attempt 1 timed out locally)
	n.replicationAttempt = map[string]uint64{"follower": 2}
	n.mu.Unlock()

	// Now attempt 1's late response arrives in same term 5
	resp := &raftv1.AppendEntriesResponse{
		Term:    5,
		Success: true,
	}
	req := &raftv1.AppendEntriesRequest{
		Term:         5,
		PrevLogIndex: 0,
		Entries:      []*raftv1.LogEntry{{Index: 1, Term: 5}},
	}

	// Deliver with stale attempt = 1
	n.HandleAppendEntriesResponse("follower", req, resp, 1)

	if n.MatchIndex("follower") != 0 || n.NextIndex("follower") != 2 {
		t.Fatalf("stale attempt response modified replication indices: match=%d next=%d",
			n.MatchIndex("follower"), n.NextIndex("follower"))
	}

	// Now deliver with active attempt = 2 -> should be processed normally
	req2 := &raftv1.AppendEntriesRequest{
		Term:         5,
		PrevLogIndex: 0,
		Entries:      []*raftv1.LogEntry{{Index: 1, Term: 5}, {Index: 2, Term: 5}},
	}
	n.HandleAppendEntriesResponse("follower", req2, resp, 2)

	if n.MatchIndex("follower") != 2 || n.NextIndex("follower") != 3 {
		t.Fatalf("active attempt was not processed: match=%d next=%d",
			n.MatchIndex("follower"), n.NextIndex("follower"))
	}
}

// Invariant I-021: Same-term response after stepping down to Follower is ignored
func TestStaleResponseAfterStepDownToFollowerIgnored(t *testing.T) {
	store := &memoryStore{boot: 1}
	logStore := storage.NewInMemoryLogStore()
	n, _ := NewNode(Config{
		ID:        "node-1",
		Peers:     []string{"follower"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     store,
		LogStore:  logStore,
	})
	n.mu.Lock()
	n.state.role = Follower // Node has already stepped down in the same term
	n.state.currentTerm = 5
	n.state.nextIndex = map[string]uint64{"follower": 2}
	n.state.matchIndex = map[string]uint64{"follower": 0, "node-1": 3}
	n.replicationAttempt = map[string]uint64{"follower": 1}
	n.mu.Unlock()

	resp := &raftv1.AppendEntriesResponse{
		Term:    5,
		Success: true,
	}
	req := &raftv1.AppendEntriesRequest{
		Term:         5,
		PrevLogIndex: 0,
		Entries:      []*raftv1.LogEntry{{Index: 1, Term: 5}},
	}

	n.HandleAppendEntriesResponse("follower", req, resp, 1)

	if n.MatchIndex("follower") != 0 || n.NextIndex("follower") != 2 {
		t.Fatalf("response accepted after step down: match=%d next=%d",
			n.MatchIndex("follower"), n.NextIndex("follower"))
	}
}
