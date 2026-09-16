package main

import (
	"context"
	"io"
	"log/slog"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/raft"
	"raftkv/internal/storage"
	clientv1 "raftkv/proto/client/v1"
	raftv1 "raftkv/proto/raft/v1"
)

type dummyTransport struct{}

func (dummyTransport) SendRequestVote(context.Context, string, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	return nil, context.Canceled
}

func (dummyTransport) SendAppendEntries(context.Context, string, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	return nil, context.Canceled
}

func TestServerGet_LinearizableGetError_DoesNotFallbackToStateMachine(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	// Pre-populate state machine with a known key-value pair.
	sm := storage.NewKVStateMachine()
	_, err := sm.Apply(storage.Command{
		OperationType: storage.Set,
		Key:           "test-key",
		Value:         []byte("local-stale-value"),
		RequestID:     "req-1",
	})
	if err != nil {
		t.Fatalf("sm.Apply failed: %v", err)
	}

	t.Run("nil raftNode uses state machine directly", func(t *testing.T) {
		srv := newServer("standalone-node", sm, nil, nil, logger)
		resp, err := srv.Get(context.Background(), &clientv1.GetRequest{Key: "test-key"})
		if err != nil {
			t.Fatalf("unexpected RPC error: %v", err)
		}
		if resp.GetStatus() != clientv1.Status_STATUS_SUCCESS {
			t.Fatalf("expected STATUS_SUCCESS for standalone dev node, got %v", resp.GetStatus())
		}
		if !resp.GetFound() || string(resp.GetValue()) != "local-stale-value" {
			t.Fatalf("expected found=true and value='local-stale-value', got found=%v, val=%q", resp.GetFound(), string(resp.GetValue()))
		}
	})

	t.Run("LinearizableGet error returns STATUS_NOT_LEADER without falling back to state machine", func(t *testing.T) {
		dir := t.TempDir()
		allNodes := []string{"node1", "node2"}
		tvStore := storage.NewFileTermVoteStore(filepath.Join(dir, "termvote"), allNodes)
		logStore, err := storage.NewFileLogStore(filepath.Join(dir, "wal"))
		if err != nil {
			t.Fatalf("NewFileLogStore failed: %v", err)
		}

		// Node configured as a follower (peer "node2", clock not advanced, role = Follower).
		raftNode, err := raft.NewNode(raft.Config{
			ID:           "node1",
			Peers:        []string{"node2"},
			Clock:        raft.NewFakeClock(time.Unix(0, 0)),
			Transport:    dummyTransport{},
			Store:        tvStore,
			LogStore:     logStore,
			StateMachine: sm,
		})
		if err != nil {
			t.Fatalf("NewNode failed: %v", err)
		}
		if err := raftNode.Start(); err != nil {
			t.Fatalf("Start failed: %v", err)
		}
		defer raftNode.Stop()

		srv := newServer("node1", sm, raftNode, nil, logger)

		// Request key that exists in s.sm.
		// raftNode is Follower, so after the 1-second leader-wait deadline,
		// LinearizableGet fails with ErrNotLeader.
		// I-016: Get MUST NOT fall back to reading s.sm.Get("test-key").
		resp, err := srv.Get(context.Background(), &clientv1.GetRequest{Key: "test-key"})
		if err != nil {
			t.Fatalf("unexpected RPC error: %v", err)
		}

		if resp.GetStatus() != clientv1.Status_STATUS_NOT_LEADER {
			t.Fatalf("expected STATUS_NOT_LEADER, got status=%v (returned value=%q)", resp.GetStatus(), string(resp.GetValue()))
		}
		if resp.GetErrorMessage() == "" {
			t.Fatalf("expected non-empty ErrorMessage in response, got empty")
		}
		if resp.GetFound() {
			t.Fatalf("expected Found=false, got Found=true")
		}
		if len(resp.GetValue()) > 0 {
			t.Fatalf("expected Value to be empty, got %q (leaked from state machine!)", string(resp.GetValue()))
		}
	})
}

// Test 37: TestErrorMapping_NotLeader verifies that server.Set, Delete, and Get
// return STATUS_NOT_LEADER immediately (no busy-wait) when not leader, and populate
// the leader_hint when a leader is known.
func TestErrorMapping_NotLeader(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sm := storage.NewKVStateMachine()
	dir := t.TempDir()
	allNodes := []string{"node1", "node2"}
	tvStore := storage.NewFileTermVoteStore(filepath.Join(dir, "termvote"), allNodes)
	logStore, err := storage.NewFileLogStore(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("NewFileLogStore failed: %v", err)
	}

	raftNode, err := raft.NewNode(raft.Config{
		ID:           "node1",
		Peers:        []string{"node2"},
		Clock:        raft.NewFakeClock(time.Unix(0, 0)),
		Transport:    dummyTransport{},
		Store:        tvStore,
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if err := raftNode.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer raftNode.Stop()

	srv := newServer("node1", sm, raftNode, nil, logger)

	// 1. Leader unknown: should return STATUS_NOT_LEADER with empty leader hint
	start := time.Now()
	setResp, err := srv.Set(context.Background(), &clientv1.SetRequest{
		Key:       "k1",
		Value:     []byte("v1"),
		RequestId: "req-not-leader-1",
	})
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if setResp.GetStatus() != clientv1.Status_STATUS_NOT_LEADER {
		t.Fatalf("expected STATUS_NOT_LEADER, got %v", setResp.GetStatus())
	}
	if setResp.GetLeaderHint() != "" {
		t.Fatalf("expected empty leader hint when unknown, got %q", setResp.GetLeaderHint())
	}
	if elapsed > 100*time.Millisecond {
		t.Fatalf("Set busy-waited when not leader: took %v", elapsed)
	}

	delResp, err := srv.Delete(context.Background(), &clientv1.DeleteRequest{
		Key:       "k1",
		RequestId: "req-not-leader-2",
	})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if delResp.GetStatus() != clientv1.Status_STATUS_NOT_LEADER {
		t.Fatalf("expected STATUS_NOT_LEADER on Delete, got %v", delResp.GetStatus())
	}

	// 2. Leader known: simulate receiving AppendEntries from node2
	_, err = raftNode.AppendEntries(context.Background(), &raftv1.AppendEntriesRequest{
		Term:         1,
		LeaderId:     "node2",
		PrevLogIndex: 0,
		PrevLogTerm:  0,
	})
	if err != nil {
		t.Fatalf("AppendEntries failed: %v", err)
	}

	setResp2, err := srv.Set(context.Background(), &clientv1.SetRequest{
		Key:       "k1",
		Value:     []byte("v1"),
		RequestId: "req-not-leader-3",
	})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if setResp2.GetStatus() != clientv1.Status_STATUS_NOT_LEADER {
		t.Fatalf("expected STATUS_NOT_LEADER, got %v", setResp2.GetStatus())
	}
	if setResp2.GetLeaderHint() != "node2" {
		t.Fatalf("expected leader hint 'node2', got %q", setResp2.GetLeaderHint())
	}

	getResp, err := srv.Get(context.Background(), &clientv1.GetRequest{Key: "k1"})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if getResp.GetStatus() != clientv1.Status_STATUS_NOT_LEADER {
		t.Fatalf("expected STATUS_NOT_LEADER on Get, got %v", getResp.GetStatus())
	}
	if getResp.GetLeaderHint() != "node2" {
		t.Fatalf("expected leader hint 'node2' on Get, got %q", getResp.GetLeaderHint())
	}
}

// Test 38: TestErrorMapping_Timeout verifies that cancelled context or deadline exceeded
// maps to STATUS_TIMEOUT without returning unconfirmed data (I-016 regression check).
func TestErrorMapping_Timeout(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sm := storage.NewKVStateMachine()
	// Pre-fill local state with a known value
	_, _ = sm.Apply(storage.Command{
		OperationType: storage.Set,
		Key:           "timeout-key",
		Value:         []byte("local-val"),
		RequestID:     "init-req",
	})

	dir := t.TempDir()
	tvStore := storage.NewFileTermVoteStore(filepath.Join(dir, "termvote"), []string{"leader"})
	logStore, err := storage.NewFileLogStore(filepath.Join(dir, "wal"))
	if err != nil {
		t.Fatalf("NewFileLogStore failed: %v", err)
	}

	raftNode, err := raft.NewNode(raft.Config{
		ID:           "leader",
		Peers:        nil, // single-node cluster
		Clock:        raft.RealClock(),
		Transport:    dummyTransport{},
		Store:        tvStore,
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		t.Fatalf("NewNode failed: %v", err)
	}
	if err := raftNode.Start(); err != nil {
		t.Fatalf("Start failed: %v", err)
	}
	defer raftNode.Stop()

	// Wait for leader election (1-node cluster becomes leader in ~150-300ms)
	deadline := time.Now().Add(2 * time.Second)
	for raftNode.Role() != raft.Leader {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for node to become leader")
		}
		time.Sleep(5 * time.Millisecond)
	}

	srv := newServer("leader", sm, raftNode, nil, logger)

	// Pre-cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// 1. Get with cancelled context -> STATUS_TIMEOUT, never returns unconfirmed data (I-016)
	getResp, err := srv.Get(ctx, &clientv1.GetRequest{Key: "timeout-key"})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if getResp.GetStatus() != clientv1.Status_STATUS_TIMEOUT {
		t.Fatalf("expected STATUS_TIMEOUT on Get, got %v", getResp.GetStatus())
	}
	if getResp.GetFound() {
		t.Fatalf("I-016 violation: Get returned Found=true on timeout")
	}
	if len(getResp.GetValue()) > 0 {
		t.Fatalf("I-016 violation: Get returned non-empty value on timeout")
	}

	// 2. Set with cancelled context -> STATUS_TIMEOUT
	setResp, err := srv.Set(ctx, &clientv1.SetRequest{
		Key:       "timeout-key",
		Value:     []byte("new-val"),
		RequestId: "req-timeout-set",
	})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if setResp.GetStatus() != clientv1.Status_STATUS_TIMEOUT {
		t.Fatalf("expected STATUS_TIMEOUT on Set, got %v", setResp.GetStatus())
	}

	// 3. Delete with cancelled context -> STATUS_TIMEOUT
	delResp, err := srv.Delete(ctx, &clientv1.DeleteRequest{
		Key:       "timeout-key",
		RequestId: "req-timeout-del",
	})
	if err != nil {
		t.Fatalf("unexpected RPC error: %v", err)
	}
	if delResp.GetStatus() != clientv1.Status_STATUS_TIMEOUT {
		t.Fatalf("expected STATUS_TIMEOUT on Delete, got %v", delResp.GetStatus())
	}
}

// Test 39: TestErrorMapping_InvalidRequest verifies resource bounds and request format
// enforcement: empty/oversized/non-UTF8 request_id, empty/oversized key, oversized value (Group 3).
func TestErrorMapping_InvalidRequest(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sm := storage.NewKVStateMachine()
	srv := newServer("node1", sm, nil, nil, logger)

	ctx := context.Background()

	// 1. Invalid RequestID
	badReqIDs := []string{
		"",                                 // empty
		string(make([]byte, 65)),           // > 64 bytes
		string([]byte{0xff, 0xfe, 0xfd}),   // invalid UTF-8
	}
	for _, id := range badReqIDs {
		resp, err := srv.Set(ctx, &clientv1.SetRequest{
			Key:       "key",
			Value:     []byte("val"),
			RequestId: id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetStatus() != clientv1.Status_STATUS_INVALID_REQUEST {
			t.Errorf("expected STATUS_INVALID_REQUEST for request_id %q, got %v", id, resp.GetStatus())
		}

		delResp, err := srv.Delete(ctx, &clientv1.DeleteRequest{
			Key:       "key",
			RequestId: id,
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if delResp.GetStatus() != clientv1.Status_STATUS_INVALID_REQUEST {
			t.Errorf("expected STATUS_INVALID_REQUEST on Delete for request_id %q, got %v", id, delResp.GetStatus())
		}
	}

	// 2. Invalid Key (empty or > 4096 bytes)
	badKeys := []string{
		"",
		string(make([]byte, 4097)),
	}
	for _, k := range badKeys {
		resp, err := srv.Set(ctx, &clientv1.SetRequest{
			Key:       k,
			Value:     []byte("val"),
			RequestId: "valid-req-id",
		})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if resp.GetStatus() != clientv1.Status_STATUS_INVALID_REQUEST {
			t.Errorf("expected STATUS_INVALID_REQUEST for key length %d, got %v", len(k), resp.GetStatus())
		}

		getResp, err := srv.Get(ctx, &clientv1.GetRequest{Key: k})
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if getResp.GetStatus() != clientv1.Status_STATUS_INVALID_REQUEST {
			t.Errorf("expected STATUS_INVALID_REQUEST on Get for key length %d, got %v", len(k), getResp.GetStatus())
		}
	}

	// 3. Oversized Value (> 1 MiB)
	oversizedValue := make([]byte, (1<<20)+1)
	resp, err := srv.Set(ctx, &clientv1.SetRequest{
		Key:       "valid-key",
		Value:     oversizedValue,
		RequestId: "valid-req-id",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.GetStatus() != clientv1.Status_STATUS_INVALID_REQUEST {
		t.Fatalf("expected STATUS_INVALID_REQUEST for oversized value, got %v", resp.GetStatus())
	}
}

// Test 40: TestRequestIDReused_ServerMapping verifies that the gRPC server maps
// ErrRequestIDReused to STATUS_REQUEST_ID_REUSED on duplicate RequestID with different payload.
func TestRequestIDReused_ServerMapping(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sm := storage.NewKVStateMachine()
	srv := newServer("node1", sm, nil, nil, logger)

	ctx := context.Background()

	// 1. Initial Set
	resp1, err := srv.Set(ctx, &clientv1.SetRequest{
		Key:       "server-dedup-key",
		Value:     []byte("v1"),
		RequestId: "shared-req-id-server",
	})
	if err != nil {
		t.Fatalf("first Set failed: %v", err)
	}
	if resp1.GetStatus() != clientv1.Status_STATUS_SUCCESS {
		t.Fatalf("expected STATUS_SUCCESS, got %v", resp1.GetStatus())
	}

	// 2. Set with same RequestID but different value -> STATUS_REQUEST_ID_REUSED
	resp2, err := srv.Set(ctx, &clientv1.SetRequest{
		Key:       "server-dedup-key",
		Value:     []byte("v2-different"),
		RequestId: "shared-req-id-server",
	})
	if err != nil {
		t.Fatalf("second Set failed: %v", err)
	}
	if resp2.GetStatus() != clientv1.Status_STATUS_REQUEST_ID_REUSED {
		t.Fatalf("expected STATUS_REQUEST_ID_REUSED, got %v", resp2.GetStatus())
	}

	// 3. Delete with same RequestID -> STATUS_REQUEST_ID_REUSED
	resp3, err := srv.Delete(ctx, &clientv1.DeleteRequest{
		Key:       "server-dedup-key",
		RequestId: "shared-req-id-server",
	})
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}
	if resp3.GetStatus() != clientv1.Status_STATUS_REQUEST_ID_REUSED {
		t.Fatalf("expected STATUS_REQUEST_ID_REUSED on Delete, got %v", resp3.GetStatus())
	}

	// 4. Set with same RequestID and same value -> STATUS_SUCCESS (dedup hit)
	resp4, err := srv.Set(ctx, &clientv1.SetRequest{
		Key:       "server-dedup-key",
		Value:     []byte("v1"),
		RequestId: "shared-req-id-server",
	})
	if err != nil {
		t.Fatalf("retry Set failed: %v", err)
	}
	if resp4.GetStatus() != clientv1.Status_STATUS_SUCCESS {
		t.Fatalf("expected STATUS_SUCCESS on dedup hit, got %v", resp4.GetStatus())
	}
}

