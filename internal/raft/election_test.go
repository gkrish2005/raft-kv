package raft

import (
	"context"
	"sync"
	"testing"
	"time"

	raftv1 "raftkv/proto/raft/v1"
)

type memoryStore struct {
	mu    sync.Mutex
	term  uint64
	vote  string
	boot  uint64
	saves []struct {
		term uint64
		vote string
	}
}

func (s *memoryStore) Save(term uint64, vote string, boot uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.term, s.vote, s.boot = term, vote, boot
	s.saves = append(s.saves, struct {
		term uint64
		vote string
	}{term, vote})
	return nil
}
func (s *memoryStore) Load() (uint64, string, uint64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.term, s.vote, s.boot, nil
}

type noTransport struct{}

func (noTransport) SendRequestVote(context.Context, string, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return nil, context.Canceled
}
func (noTransport) SendAppendEntries(context.Context, string, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return nil, context.Canceled
}

func TestHigherTermRequestVoteResponseStepsDownBeforeFiltering(t *testing.T) {
	store := &memoryStore{boot: 1}
	n, err := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	n.state = nodeState{currentTerm: 5, votedFor: "n1", role: Follower, electionTerm: 0, bootID: 1} // role/filter would reject a same-term response.
	n.HandleRequestVoteResponse(&raftv1.RequestVoteResponse{Term: 6, VoteGranted: true})
	if got := n.Term(); got != 6 {
		t.Fatalf("term=%d, want 6", got)
	}
	if got := n.Role(); got != Follower {
		t.Fatalf("role=%s, want Follower", got)
	}
	store.mu.Lock()
	defer store.mu.Unlock()
	if len(store.saves) != 1 || store.saves[0].term != 6 || store.saves[0].vote != "" {
		t.Fatalf("higher term was not persisted as {6, empty}: %#v", store.saves)
	}
}

func TestHigherTermAllFourTouchpointsPersistBeforeContinuing(t *testing.T) {
	cases := []struct {
		name   string
		invoke func(*Node)
	}{
		{"incoming request vote", func(n *Node) {
			_, _ = n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 6, CandidateId: "other"})
		}},
		{"incoming append entries", func(n *Node) {
			_, _ = n.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 6, LeaderId: "other"})
		}},
		{"request vote response", func(n *Node) { n.HandleRequestVoteResponse(&raftv1.RequestVoteResponse{Term: 6}) }},
		{"append entries response", func(n *Node) { n.HandleAppendEntriesResponse(&raftv1.AppendEntriesResponse{Term: 6}) }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			store := &memoryStore{boot: 1}
			n, _ := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
			n.state = nodeState{currentTerm: 5, votedFor: "n1", role: Candidate, electionTerm: 5, bootID: 1}
			tc.invoke(n)
			if n.Term() != 6 || n.Role() != Follower {
				t.Fatalf("state = term %d role %s", n.Term(), n.Role())
			}
			store.mu.Lock()
			defer store.mu.Unlock()
			if len(store.saves) == 0 || store.saves[len(store.saves)-1].term != 6 {
				t.Fatalf("term transition was not durably saved: %#v", store.saves)
			}
		})
	}
}

type directTransport struct{ nodes map[string]*Node }

func (t directTransport) SendRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return t.nodes[peer].RequestVote(ctx, req)
}
func (t directTransport) SendAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return t.nodes[peer].AppendEntries(ctx, req)
}

func TestDeterministicElectionConvergesRepeatedly(t *testing.T) {
	for _, size := range []int{3, 5} {
		for run := 0; run < 100; run++ {
			nodes := map[string]*Node{}
			clocks := map[string]*FakeClock{}
			transport := directTransport{nodes: nodes}
			for i := 0; i < size; i++ {
				id := string(rune('a' + i))
				clocks[id] = NewFakeClock(time.Unix(0, 0))
				peers := []string{}
				for j := 0; j < size; j++ {
					if i != j {
						peers = append(peers, string(rune('a'+j)))
					}
				}
				n, err := NewNode(Config{ID: id, Peers: peers, Clock: clocks[id], Transport: transport, Store: &memoryStore{boot: 1}})
				if err != nil {
					t.Fatal(err)
				}
				n.state.bootID = 1
				nodes[id] = n
			}
			// This is a cooperative Level-2 simulator: time only advances when
			// the test advances the fake clock, and the due node's exact
			// election-loop transition is then stepped synchronously.  No
			// goroutine scheduling participates in the protocol ordering.
			timer := clocks["a"].NewTimer(MinElectionTimeout)
			clocks["a"].Advance(MinElectionTimeout)
			select {
			case <-timer.C():
				nodes["a"].handleElectionTimeout()
			default:
				t.Fatal("fake election timer did not fire")
			}
			leaders := 0
			for _, n := range nodes {
				if n.Role() == Leader {
					leaders++
				}
			}
			if leaders != 1 {
				t.Fatalf("size %d run %d leaders=%d", size, run, leaders)
			}
		}
	}
}

type orderingTransport struct {
	store  *memoryStore
	called bool
}

func (t *orderingTransport) SendRequestVote(_ context.Context, _ string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	t.called = true
	t.store.mu.Lock()
	defer t.store.mu.Unlock()
	if t.store.term != req.Term || t.store.vote != "n1" {
		return nil, context.Canceled
	}
	return &raftv1.RequestVoteResponse{Term: req.Term, VoteGranted: true}, nil
}
func (t *orderingTransport) SendAppendEntries(context.Context, string, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return &raftv1.AppendEntriesResponse{Term: 1, Success: true}, nil
}

func TestSelfVoteIsPersistedBeforeOutgoingRequestVote(t *testing.T) {
	store := &memoryStore{boot: 1}
	transport := &orderingTransport{store: store}
	n, err := NewNode(Config{ID: "n1", Peers: []string{"n2"}, Clock: NewFakeClock(time.Unix(0, 0)), Transport: transport, Store: store})
	if err != nil {
		t.Fatal(err)
	}
	n.state.bootID = 1
	n.startElection()
	if !transport.called {
		t.Fatal("RequestVote was not sent")
	}
	if n.Role() != Leader {
		t.Fatalf("role=%s, want Leader", n.Role())
	}
}

func TestHandleElectionTimeoutReturnsHeartbeatIntervalAfterWin(t *testing.T) {
	n, err := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: &memoryStore{boot: 1}})
	if err != nil {
		t.Fatal(err)
	}
	n.state.bootID = 1
	next := n.handleElectionTimeout()
	if n.Role() != Leader {
		t.Fatalf("role=%s, want Leader", n.Role())
	}
	if next != HeartbeatInterval {
		t.Fatalf("next timer after winning election = %s, want heartbeat interval %s", next, HeartbeatInterval)
	}
}

func TestHeartbeatStepsDownCandidate(t *testing.T) {
	store := &memoryStore{boot: 1}
	n, _ := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
	n.state = nodeState{currentTerm: 3, votedFor: "n1", role: Candidate, electionTerm: 3, bootID: 1}
	resp, err := n.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{Term: 3, LeaderId: "n2"})
	if err != nil || !resp.Success {
		t.Fatalf("heartbeat response = %#v, %v", resp, err)
	}
	if n.Role() != Follower {
		t.Fatalf("role=%s, want Follower", n.Role())
	}
}

func TestRequestVoteRejectsStaleLogUsingExactFormula(t *testing.T) {
	store := &memoryStore{boot: 1}
	n, _ := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
	n.state = nodeState{currentTerm: 4, role: Follower, bootID: 1, lastLogTerm: 3, lastLogIndex: 9}
	stale, err := n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 4, CandidateId: "n2", LastLogTerm: 3, LastLogIndex: 8})
	if err != nil || stale.VoteGranted {
		t.Fatalf("stale candidate was granted: %#v, %v", stale, err)
	}
	freshTerm, err := n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 4, CandidateId: "n2", LastLogTerm: 4, LastLogIndex: 0})
	if err != nil || !freshTerm.VoteGranted {
		t.Fatalf("newer-term candidate was rejected: %#v, %v", freshTerm, err)
	}
}

type failingSaveStore struct{ memoryStore }

func (s *failingSaveStore) Save(uint64, string, uint64) error { return context.Canceled }

func TestCrashBeforeVoteFsyncDoesNotGrantVote(t *testing.T) {
	store := &failingSaveStore{memoryStore: memoryStore{term: 7, boot: 1}}
	n, _ := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
	n.state = nodeState{currentTerm: 7, role: Follower, bootID: 1}
	if resp, err := n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 8, CandidateId: "n2"}); err == nil || resp != nil {
		t.Fatalf("vote response before failed fsync = %#v, %v; want no response", resp, err)
	}
	if store.term != 7 || store.vote != "" {
		t.Fatalf("failed save changed durable state to (%d,%q)", store.term, store.vote)
	}
}

func TestCrashAfterVoteFsyncBeforeResponsePreservesVote(t *testing.T) {
	// Save models the completed fsync.  Dropping the process immediately after
	// it returns is equivalent to losing the RPC response but not this record.
	store := &memoryStore{term: 7, boot: 1}
	if err := store.Save(8, "n2", 1); err != nil {
		t.Fatal(err)
	}
	restarted, _ := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()
	resp, err := restarted.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 8, CandidateId: "n3"})
	if err != nil || resp.VoteGranted {
		t.Fatalf("restarted node granted a second vote: %#v, %v", resp, err)
	}
}

type crashBeforeRequestVoteTransport struct{}

func (crashBeforeRequestVoteTransport) SendRequestVote(context.Context, string, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	panic("simulated crash after self-vote fsync, before outgoing RequestVote")
}
func (crashBeforeRequestVoteTransport) SendAppendEntries(context.Context, string, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return nil, context.Canceled
}

func TestCrashAfterSelfVoteFsyncBeforeRequestVotePreservesSelfVote(t *testing.T) {
	store := &memoryStore{boot: 1}
	n, _ := NewNode(Config{ID: "n1", Peers: []string{"n2"}, Clock: NewFakeClock(time.Unix(0, 0)), Transport: crashBeforeRequestVoteTransport{}, Store: store})
	n.state.bootID = 1
	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("expected simulated crash")
			}
		}()
		n.startElection()
	}()
	if store.term != 1 || store.vote != "n1" {
		t.Fatalf("self-vote was not durable before outgoing RPC: (%d,%q)", store.term, store.vote)
	}
	restarted, _ := NewNode(Config{ID: "n1", Clock: NewFakeClock(time.Unix(0, 0)), Transport: noTransport{}, Store: store})
	if err := restarted.Start(); err != nil {
		t.Fatal(err)
	}
	defer restarted.Stop()
	resp, err := restarted.RequestVote(context.Background(), &raftv1.RequestVoteRequest{Term: 1, CandidateId: "n2"})
	if err != nil || resp.VoteGranted {
		t.Fatalf("restarted node granted a second same-term vote: %#v, %v", resp, err)
	}
}
