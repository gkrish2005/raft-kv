package raft

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

// Test 33: TestApplyDedup_DifferentPayload_I017 verifies that:
// 1. A committed command whose RequestID matches an existing applied command but has
//    a different payload hash is rejected with ErrRequestIDReused (I-017).
// 2. The entry is applied through the real Raft applier loop without bypassing consensus.
// 3. I-005 regression proof: lastApplied advances unconditionally to the entry's index
//    even when the application error ErrRequestIDReused is returned (Rule 8 / I-005).
// 4. State machine state is not mutated by the rejected command.
// 5. Dedup-hit reuse of resolvePendingWriteLocked preserves I-019 index/term keying semantics.
func TestApplyDedup_DifferentPayload_I017(t *testing.T) {
	sm := storage.NewKVStateMachine()
	logStore := storage.NewInMemoryLogStore()

	n, err := NewNode(Config{
		ID:           "test-node-1",
		Peers:        nil, // single node: quorum size 1
		Clock:        NewFakeClock(time.Unix(0, 0)),
		Transport:    noTransport{},
		Store:        &memoryStore{term: 1, boot: 1},
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}

	if err := n.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer n.Stop()

	// Establish node as Leader and apply initial NOOP entry at index 1
	n.mu.Lock()
	n.becomeLeaderLocked()
	n.state.commitIndex = 1
	n.notifyLocked(&n.commitNotifyCh)
	n.mu.Unlock()

	// Wait for leader readiness gate (I-023)
	deadline := time.Now().Add(2 * time.Second)
	for n.ReadReadyTerm() != 1 {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for readReadyTerm == 1")
		}
		time.Sleep(5 * time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 1. Write original command (index 2)
	cmd1 := &raftv1.Command{
		OperationType: "SET",
		Key:           "key-i017",
		Value:         []byte("val-original"),
		RequestId:     "req-shared-i017",
	}
	_, err = n.Write(ctx, cmd1)
	if err != nil {
		t.Fatalf("initial write failed: %v", err)
	}

	if n.LastApplied() != 2 {
		t.Fatalf("expected lastApplied == 2 after first write, got %d", n.LastApplied())
	}
	val, found := sm.Get("key-i017")
	if !found || !bytes.Equal(val, []byte("val-original")) {
		t.Fatalf("expected key-i017 = 'val-original', got %s, found=%v", string(val), found)
	}

	// 2. Write duplicate RequestID with DIFFERENT payload (index 3)
	cmd2 := &raftv1.Command{
		OperationType: "SET",
		Key:           "key-i017",
		Value:         []byte("val-different-payload"),
		RequestId:     "req-shared-i017", // same RequestID, different value
	}
	_, err = n.Write(ctx, cmd2)
	if err == nil {
		t.Fatalf("expected error on duplicate RequestID with different payload, got nil")
	}
	if !errors.Is(err, storage.ErrRequestIDReused) {
		t.Fatalf("expected ErrRequestIDReused, got: %v", err)
	}

	// I-005 / Rule 8 regression proof: lastApplied MUST advance to 3 even on application error
	if n.LastApplied() != 3 {
		t.Fatalf("I-005 violation: expected lastApplied == 3 after application error, got %d", n.LastApplied())
	}
	if n.CommitIndex() != 3 {
		t.Fatalf("expected commitIndex == 3, got %d", n.CommitIndex())
	}

	// State machine must NOT have adopted the conflicting payload
	val, found = sm.Get("key-i017")
	if !found || !bytes.Equal(val, []byte("val-original")) {
		t.Fatalf("state machine corrupted by rejected write: expected 'val-original', got %s", string(val))
	}

	// 3. Write duplicate RequestID with SAME payload as original (index 4) -> dedup hit
	cmd3 := &raftv1.Command{
		OperationType: "SET",
		Key:           "key-i017",
		Value:         []byte("val-original"),
		RequestId:     "req-shared-i017", // same RequestID and same payload
	}
	_, err = n.Write(ctx, cmd3)
	if err != nil {
		t.Fatalf("dedup hit write failed: %v", err)
	}

	// I-019 regression proof: dedup hit resolves cleanly via resolvePendingWriteLocked
	if n.LastApplied() != 4 {
		t.Fatalf("expected lastApplied == 4 after dedup hit, got %d", n.LastApplied())
	}
	val, found = sm.Get("key-i017")
	if !found || !bytes.Equal(val, []byte("val-original")) {
		t.Fatalf("expected 'val-original', got %s", string(val))
	}

	// 4. Write duplicate RequestID with different OperationType (DELETE vs SET) (index 5)
	cmd4 := &raftv1.Command{
		OperationType: "DELETE",
		Key:           "key-i017",
		RequestId:     "req-shared-i017",
	}
	_, err = n.Write(ctx, cmd4)
	if err == nil {
		t.Fatalf("expected error on duplicate RequestID with DELETE vs SET, got nil")
	}
	if !errors.Is(err, storage.ErrRequestIDReused) {
		t.Fatalf("expected ErrRequestIDReused, got: %v", err)
	}

	// I-005: lastApplied advances to 5
	if n.LastApplied() != 5 {
		t.Fatalf("expected lastApplied == 5, got %d", n.LastApplied())
	}
	// Key must NOT have been deleted
	val, found = sm.Get("key-i017")
	if !found || !bytes.Equal(val, []byte("val-original")) {
		t.Fatalf("key-i017 was unexpectedly deleted on rejected write")
	}
}
