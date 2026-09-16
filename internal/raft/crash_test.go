package raft

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

// Test 18: Commit-Advanced-Then-Crash-Before-Apply (I-005, I-022)
// Direct disk setup with no Node goroutines pre-crash (avoids Stop()).
// Recovery resets volatile commitIndex to 0; applier waits for leader contact.
func TestCrash_CommitAdvancedThenCrashBeforeApply(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "wal")
	tvPath := filepath.Join(dir, "termvote")
	nodes := []string{"n1", "n2"}

	// --- Pre-crash disk setup: write 3 entries durably to disk ---
	ls, err := storage.NewFileLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	entries := []*raftv1.LogEntry{
		{Index: 1, Term: 1, Command: &raftv1.Command{OperationType: "SET", Key: "k1", Value: []byte("v1"), RequestId: "req-1"}},
		{Index: 2, Term: 1, Command: &raftv1.Command{OperationType: "SET", Key: "k2", Value: []byte("v2"), RequestId: "req-2"}},
		{Index: 3, Term: 1, Command: &raftv1.Command{OperationType: "SET", Key: "k3", Value: []byte("v3"), RequestId: "req-3"}},
	}
	if err := ls.Append(entries); err != nil {
		t.Fatal(err)
	}
	_ = ls.Close()

	tv := storage.NewFileTermVoteStore(tvPath, nodes)
	if _, _, _, err := tv.Load(); err != nil {
		t.Fatal(err)
	}
	if err := tv.Save(1, "", 1); err != nil {
		t.Fatal(err)
	}

	// "Crash": the node crashed before the applier ran for commitIndex=3.
	// commitIndex is volatile and never persisted. No Node was running.

	// --- Recovery: brand-new Node reading the same disk files ---
	lsRecovered, err := storage.NewFileLogStore(logPath)
	if err != nil {
		t.Fatal(err)
	}
	sm := storage.NewKVStateMachine()
	n, err := NewNode(Config{
		ID:           "n1",
		Peers:        []string{"n2"},
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        storage.NewFileTermVoteStore(tvPath, nodes),
		LogStore:     lsRecovered,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Recover(); err != nil {
		t.Fatalf("Recover failed: %v", err)
	}

	// Invariant I-005 check: commitIndex and lastApplied must reset to 0
	if n.CommitIndex() != 0 {
		t.Fatalf("expected commitIndex=0 after recovery, got %d", n.CommitIndex())
	}
	if n.LastApplied() != 0 {
		t.Fatalf("expected lastApplied=0 after recovery, got %d", n.LastApplied())
	}

	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// State machine must NOT have applied entry 1, 2, or 3 yet
	if res, ok := sm.Get("k1"); ok || string(res) != "" {
		t.Fatalf("expected state machine empty before leader commit, got %q", string(res))
	}

	// Rejoin: leader sends AppendEntries with LeaderCommit = 3
	resp, err := n.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "n2",
		PrevLogIndex: 3,
		PrevLogTerm:  1,
		LeaderCommit: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !resp.Success {
		t.Fatalf("expected AppendEntries success, got %+v", resp)
	}

	// Wait for applierLoop to process commitIndex advance
	time.Sleep(50 * time.Millisecond)

	if n.CommitIndex() != 3 {
		t.Fatalf("expected commitIndex=3, got %d", n.CommitIndex())
	}
	if n.LastApplied() != 3 {
		t.Fatalf("expected lastApplied=3, got %d", n.LastApplied())
	}
	if res, ok := sm.Get("k1"); !ok || string(res) != "v1" {
		t.Fatalf("expected state machine to have applied k1=v1, got %q", string(res))
	}
}

// Test 19: Term/Vote Fsync Boundary — Before Fsync (I-012)
// Simulated crash hook AfterTmpWrite causes save to fail; vote not durable on restart.
func TestCrash_TermVoteFsyncBoundary_BeforeFsync(t *testing.T) {
	dir := t.TempDir()
	tvPath := filepath.Join(dir, "termvote")
	nodes := []string{"n1", "n2"}
	tv := storage.NewFileTermVoteStore(tvPath, nodes)
	if _, _, _, err := tv.Load(); err != nil {
		t.Fatal(err)
	}

	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     tv,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Inject failure at AfterTmpWrite (before fsync of tmp file)
	tv.Hooks.AfterTmpWrite = func() error {
		return errors.New("simulated crash before fsync")
	}

	// Incoming RequestVote from candidate "n2" in term 1
	resp, _ := n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{
		Term:         1,
		CandidateId:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if resp != nil && resp.VoteGranted {
		t.Fatalf("vote must not be granted when fsync fails: %+v", resp)
	}

	// On restart, uncommitted vote is NOT durable
	tvRecovered := storage.NewFileTermVoteStore(tvPath, nodes)
	term, vote, _, err := tvRecovered.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 || vote != "" {
		t.Fatalf("expected term 0 and vote \"\" on restart, got (%d, %q)", term, vote)
	}
}

// Test 20: Term/Vote Fsync Boundary — After Fsync Before Response (I-012)
// Full Save completes, but process crashes before sending response.
// On restart, the vote IS preserved.
func TestCrash_TermVoteFsyncBoundary_AfterFsyncBeforeResponse(t *testing.T) {
	dir := t.TempDir()
	tvPath := filepath.Join(dir, "termvote")
	nodes := []string{"n1", "n2"}
	tv := storage.NewFileTermVoteStore(tvPath, nodes)
	if _, _, _, err := tv.Load(); err != nil {
		t.Fatal(err)
	}

	// Save term 1, votedFor "n2" (simulating fsync landed before response)
	if err := tv.Save(1, "n2", 1); err != nil {
		t.Fatal(err)
	}

	// "Crash" occurs before response reaches the network.
	// On restart, vote MUST be durable.
	tvRecovered := storage.NewFileTermVoteStore(tvPath, nodes)
	term, vote, _, err := tvRecovered.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 1 || vote != "n2" {
		t.Fatalf("expected term 1 and vote \"n2\" preserved on restart, got (%d, %q)", term, vote)
	}
}

// Test 21: Candidate Self-Vote Persisted Before Outgoing RPC (I-012)
func TestCrash_CandidateSelfVoteBeforeRPC(t *testing.T) {
	dir := t.TempDir()
	tvPath := filepath.Join(dir, "termvote")
	nodes := []string{"n1", "n2", "n3"}
	tv := storage.NewFileTermVoteStore(tvPath, nodes)
	if _, _, _, err := tv.Load(); err != nil {
		t.Fatal(err)
	}

	// Pre-crash: candidate n1 votes for self in term 2 and persists it
	if err := tv.Save(2, "n1", 1); err != nil {
		t.Fatal(err)
	}

	// Crash before outgoing RequestVote RPCs.
	// Restart: node recovers term 2, votedFor "n1"
	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     []string{"n2", "n3"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     storage.NewFileTermVoteStore(tvPath, nodes),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Recover(); err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	if n.Term() != 2 {
		t.Fatalf("expected term 2, got %d", n.Term())
	}

	// Candidate n2 asks for vote in term 2: MUST be rejected because n1 already voted for self
	resp, err := n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{
		Term:         2,
		CandidateId:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.VoteGranted {
		t.Fatal("node must not grant vote to n2 when it already voted for self in term 2")
	}
}

// Test 22: Corrupt Term/Vote Record Refuses Startup (I-020)
func TestCrash_CorruptTermVoteRefusesStartup(t *testing.T) {
	dir := t.TempDir()
	tvPath := filepath.Join(dir, "termvote")
	if err := os.WriteFile(tvPath, []byte("garbage corrupt bytes"), 0o600); err != nil {
		t.Fatal(err)
	}

	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     storage.NewFileTermVoteStore(tvPath, []string{"n1", "n2"}),
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := n.Recover(); err == nil {
		t.Fatal("expected Node.Recover() to fail on corrupt termvote file")
	}
	if err := n.Start(); err == nil {
		t.Fatal("expected Node.Start() to fail on corrupt termvote file")
	}
}

// faultInjectingLogStore wraps LogStore for fault injection
type faultInjectingLogStore struct {
	storage.LogStore
	failAppend   error
	failTruncate error
}

func (f *faultInjectingLogStore) Append(entries []*raftv1.LogEntry) error {
	if f.failAppend != nil {
		return f.failAppend
	}
	return f.LogStore.Append(entries)
}

func (f *faultInjectingLogStore) TruncateFrom(index uint64) error {
	if f.failTruncate != nil {
		return f.failTruncate
	}
	return f.LogStore.TruncateFrom(index)
}

// Test 23: Storage Failure Fail-Closed on Append (I-018)
func TestCrash_StorageFailureFailClosed_Append(t *testing.T) {
	mockStore := &memoryStore{boot: 1}
	memLog := storage.NewInMemoryLogStore()
	failingLog := &faultInjectingLogStore{LogStore: memLog}

	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     mockStore,
		LogStore:  failingLog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Become leader
	n.mu.Lock()
	n.state.role = Leader
	n.state.currentTerm = 1
	n.mu.Unlock()

	// Inject append failure
	failingLog.failAppend = errors.New("injected disk append error")

	// Attempt local append
	_, _, err = n.appendLocalEntryWithPendingWrite(&raftv1.Command{
		OperationType: "SET",
		Key:           "k1",
		Value:         []byte("v1"),
		RequestId:     "req-1",
	})
	if err == nil {
		t.Fatal("expected error on failing append")
	}

	// Verify fail-closed behavior: role transitions to StorageFailed
	if n.Role() != StorageFailed {
		t.Fatalf("expected role StorageFailed, got %v", n.Role())
	}
}

// Test 24: Storage Failure Fail-Closed on Truncate (I-018)
func TestCrash_StorageFailureFailClosed_Truncate(t *testing.T) {
	mockStore := &memoryStore{boot: 1}
	memLog := storage.NewInMemoryLogStore()
	// Pre-populate follower log with entry 1 in term 1
	_ = memLog.Append([]*raftv1.LogEntry{
		{Index: 1, Term: 1, Command: &raftv1.Command{OperationType: "SET", Key: "k", Value: []byte("v")}},
	})
	failingLog := &faultInjectingLogStore{LogStore: memLog}

	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     mockStore,
		LogStore:  failingLog,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Inject truncate failure
	failingLog.failTruncate = errors.New("injected disk truncate error")

	// Leader sends AppendEntries with conflicting entry 1 in term 2 (triggers TruncateFrom)
	resp, err := n.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term:         2,
		LeaderId:     "n2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries: []*raftv1.LogEntry{
			{Index: 1, Term: 2, Command: &raftv1.Command{OperationType: "SET", Key: "k", Value: []byte("v2")}},
		},
	})
	if err == nil {
		t.Fatalf("expected error on failing truncate, got resp: %+v", resp)
	}

	// Verify node transitioned to StorageFailed
	if n.Role() != StorageFailed {
		t.Fatalf("expected role StorageFailed, got %v", n.Role())
	}
}

// faultInjectingTermVoteStore wraps TermVoteStore for fault injection
type faultInjectingTermVoteStore struct {
	storage.TermVoteStore
	failSave error
}

func (f *faultInjectingTermVoteStore) Save(term uint64, votedFor string, bootID uint64) error {
	if f.failSave != nil {
		return f.failSave
	}
	return f.TermVoteStore.Save(term, votedFor, bootID)
}

// Test 25: Storage Failure Fail-Closed on Save (I-018)
func TestCrash_StorageFailureFailClosed_Save(t *testing.T) {
	memStore := &memoryStore{boot: 1}
	failingStore := &faultInjectingTermVoteStore{TermVoteStore: memStore}

	n, err := NewNode(Config{
		ID:        "n1",
		Peers:     []string{"n2"},
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
		Store:     failingStore,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := n.Start(); err != nil {
		t.Fatal(err)
	}
	defer n.Stop()

	// Inject Save failure
	failingStore.failSave = errors.New("injected disk save error")

	// Incoming RequestVote from candidate "n2"
	resp, err := n.RequestVote(context.Background(), &raftv1.RequestVoteRequest{
		Term:         1,
		CandidateId:  "n2",
		LastLogIndex: 0,
		LastLogTerm:  0,
	})
	if err == nil && resp.VoteGranted {
		t.Fatal("vote must not be granted when Save fails")
	}

	// Verify fail-closed: transitions to StorageFailed
	if n.Role() != StorageFailed {
		t.Fatalf("expected role StorageFailed, got %v", n.Role())
	}
}

// fakeTermVoteStore has no semantic validation — simulates a custom/foreign TermVoteStore
type fakeTermVoteStore struct {
	term   uint64
	vote   string
	bootID uint64
}

func (f *fakeTermVoteStore) Save(t uint64, v string, b uint64) error {
	f.term, f.vote, f.bootID = t, v, b
	return nil
}

func (f *fakeTermVoteStore) Load() (uint64, string, uint64, error) {
	// Deliberately skips votedFor cluster-membership validation
	return f.term, f.vote, f.bootID, nil
}

// Test 26: Independent votedFor Validation in Node.Recover() (Gap 4 defense-in-depth)
func TestRecover_IndependentVotedForValidation(t *testing.T) {
	fake := &fakeTermVoteStore{term: 2, vote: "foreign-node-xyz", bootID: 3}
	n, err := NewNode(Config{
		ID:        "node-1",
		Peers:     []string{"node-2", "node-3"},
		Store:     fake,
		Clock:     NewFakeClock(time.Unix(0, 0)),
		Transport: noTransport{},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = n.Recover()
	if err == nil {
		t.Fatal("expected Node.Recover() to refuse when votedFor is not a configured node")
	}
	if !strings.Contains(err.Error(), "foreign-node-xyz") {
		t.Fatalf("expected error mentioning foreign-node-xyz, got: %v", err)
	}
}
