package client

import (
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

type noTransport struct{}

func (noTransport) SendRequestVote(context.Context, string, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return nil, nil
}

func (noTransport) SendAppendEntries(context.Context, string, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return nil, nil
}

func setupLeaderNode(t *testing.T) (*raft.Node, *storage.KVStateMachine) {
	t.Helper()
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()
	clock := raft.NewFakeClock(time.Unix(0, 0))

	n, err := raft.NewNode(raft.Config{
		ID:           "leader",
		Peers:        nil,
		Clock:        clock,
		Transport:    noTransport{},
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

	// Advance clock until elected leader
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && n.Role() != raft.Leader {
		clock.Advance(50 * time.Millisecond)
		time.Sleep(10 * time.Millisecond)
	}
	if n.Role() != raft.Leader {
		t.Fatalf("expected node to become leader, got %v", n.Role())
	}

	// Wait for the leader NOOP to commit and apply so readReadyTerm is set
	for n.ReadReadyTerm() != n.Term() {
		time.Sleep(5 * time.Millisecond)
	}
	return n, sm
}

func TestClient_BasicCRUD(t *testing.T) {
	node, _ := setupLeaderNode(t)
	defer node.Stop()

	// Wait briefly for applier to catch up noop
	time.Sleep(20 * time.Millisecond)

	c := New(node)
	if c.Node() != node {
		t.Fatalf("expected c.Node() to match node")
	}

	ctx := context.Background()

	// 1. SET key "user" = "alice"
	if err := c.Set(ctx, "user", []byte("alice"), "req-client-1"); err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// 2. GET key "user"
	val, found, err := c.Get(ctx, "user")
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if !found || string(val) != "alice" {
		t.Fatalf("expected 'alice', got val=%s found=%v", string(val), found)
	}

	// 3. DELETE key "user"
	if err := c.Delete(ctx, "user", "req-client-2"); err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// 4. GET key "user" should not be found
	val, found, err = c.Get(ctx, "user")
	if err != nil {
		t.Fatalf("Get failed after delete: %v", err)
	}
	if found {
		t.Fatalf("expected key to be deleted, got val=%s", string(val))
	}
}

func TestClient_NonLeaderGetReturnsErrNotLeader(t *testing.T) {
	sm := storage.NewKVStateMachine()
	n, err := raft.NewNode(raft.Config{
		ID:           "follower",
		Peers:        []string{"other"},
		Clock:        raft.NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{term: 1, boot: 1},
		LogStore:     storage.NewInMemoryLogStore(),
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	c := New(n)
	_, _, err = c.Get(context.Background(), "k")
	if err != ErrNotLeader {
		t.Fatalf("expected ErrNotLeader, got %v", err)
	}
}

func TestClient_StoppedNodeReturnsErrNodeStopped(t *testing.T) {
	node, _ := setupLeaderNode(t)
	node.Stop()

	c := New(node)
	err := c.Set(context.Background(), "k", []byte("v"), "req-stop-1")
	if err != ErrNodeStopped {
		t.Fatalf("expected ErrNodeStopped, got %v", err)
	}

	_, _, err = c.Get(context.Background(), "k")
	if err != ErrNodeStopped {
		t.Fatalf("expected ErrNodeStopped on Get, got %v", err)
	}
}
