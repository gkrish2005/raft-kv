package storage

import (
	"bytes"
	"fmt"
	"sync"
	"testing"
)

func TestKVStateMachine_BasicCRUD(t *testing.T) {
	sm := NewKVStateMachine()

	// Initial Get on missing key
	val, found := sm.Get("key1")
	if found {
		t.Fatalf("expected found=false for missing key, got value=%s", string(val))
	}

	// Set key1 -> val1
	res, err := sm.Apply(Command{
		OperationType: Set,
		Key:           "key1",
		Value:         []byte("val1"),
		RequestID:     "req-1",
	})
	if err != nil {
		t.Fatalf("Apply Set failed: %v", err)
	}
	_ = res

	// Get key1
	val, found = sm.Get("key1")
	if !found {
		t.Fatalf("expected found=true for key1")
	}
	if !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("expected 'val1', got %s", string(val))
	}

	// Overwrite key1 -> val2
	_, err = sm.Apply(Command{
		OperationType: Set,
		Key:           "key1",
		Value:         []byte("val2"),
		RequestID:     "req-2",
	})
	if err != nil {
		t.Fatalf("Apply Set overwrite failed: %v", err)
	}

	val, found = sm.Get("key1")
	if !found || !bytes.Equal(val, []byte("val2")) {
		t.Fatalf("expected 'val2', got found=%v, val=%s", found, string(val))
	}

	// Delete key1
	_, err = sm.Apply(Command{
		OperationType: Delete,
		Key:           "key1",
		RequestID:     "req-3",
	})
	if err != nil {
		t.Fatalf("Apply Delete failed: %v", err)
	}

	// Verify key1 is deleted
	val, found = sm.Get("key1")
	if found {
		t.Fatalf("expected found=false after delete, got %s", string(val))
	}
}

func TestKVStateMachine_EmptyValueVsMissingKey(t *testing.T) {
	sm := NewKVStateMachine()

	// 1. Missing key
	val, found := sm.Get("nonexistent")
	if found {
		t.Fatalf("expected found=false for nonexistent key")
	}
	if val != nil {
		t.Fatalf("expected nil value for nonexistent key, got %v", val)
	}

	// 2. Explicit empty value
	_, err := sm.Apply(Command{
		OperationType: Set,
		Key:           "empty-val-key",
		Value:         []byte(""),
		RequestID:     "req-empty",
	})
	if err != nil {
		t.Fatalf("Apply Set empty value failed: %v", err)
	}

	val, found = sm.Get("empty-val-key")
	if !found {
		t.Fatalf("expected found=true for empty-val-key")
	}
	if len(val) != 0 {
		t.Fatalf("expected len(val)=0, got %d bytes: %q", len(val), val)
	}
}

func TestKVStateMachine_DeleteMissingKey(t *testing.T) {
	sm := NewKVStateMachine()

	// Deleting a non-existent key must succeed cleanly as a no-op
	_, err := sm.Apply(Command{
		OperationType: Delete,
		Key:           "never-existed",
		RequestID:     "req-del-missing",
	})
	if err != nil {
		t.Fatalf("expected Delete on missing key to succeed, got %v", err)
	}
}

func TestKVStateMachine_Noop(t *testing.T) {
	sm := NewKVStateMachine()

	// Set key1
	_, err := sm.Apply(Command{
		OperationType: Set,
		Key:           "key1",
		Value:         []byte("val1"),
		RequestID:     "req-1",
	})
	if err != nil {
		t.Fatalf("Set failed: %v", err)
	}

	// Apply NOOP
	_, err = sm.Apply(Command{
		OperationType: Noop,
	})
	if err != nil {
		t.Fatalf("Apply Noop failed: %v", err)
	}

	// Verify key1 is unaffected
	val, found := sm.Get("key1")
	if !found || !bytes.Equal(val, []byte("val1")) {
		t.Fatalf("expected 'val1' after NOOP, got %s", string(val))
	}
}

func TestKVStateMachine_UnknownOperation(t *testing.T) {
	sm := NewKVStateMachine()

	_, err := sm.Apply(Command{
		OperationType: OperationType("INVALID_OP"),
		Key:           "key",
	})
	if err == nil {
		t.Fatalf("expected error on unknown operation type, got nil")
	}
}

func TestKVStateMachine_ConcurrentAccess(t *testing.T) {
	sm := NewKVStateMachine()
	numGoroutines := 20
	iterations := 100

	var wg sync.WaitGroup
	wg.Add(numGoroutines * 2)

	// Concurrent writers
	for i := 0; i < numGoroutines; i++ {
		go func(writerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("key-%d", j%5)
				val := fmt.Sprintf("val-%d-%d", writerID, j)
				_, err := sm.Apply(Command{
					OperationType: Set,
					Key:           key,
					Value:         []byte(val),
					RequestID:     fmt.Sprintf("w-%d-%d", writerID, j),
				})
				if err != nil {
					t.Errorf("concurrent Set error: %v", err)
				}
			}
		}(i)
	}

	// Concurrent readers
	for i := 0; i < numGoroutines; i++ {
		go func(readerID int) {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				key := fmt.Sprintf("key-%d", j%5)
				_, _ = sm.Get(key)
			}
		}(i)
	}

	wg.Wait()
}

func TestKVStateMachine_ExternalLocking(t *testing.T) {
	sm := NewKVStateMachine()

	// 1. External write lock across ApplyLocked
	sm.Lock()
	res, err := sm.ApplyLocked(Command{
		OperationType: Set,
		Key:           "k1",
		Value:         []byte("v1"),
		RequestID:     "req-ext-1",
	})
	sm.Unlock()
	if err != nil {
		t.Fatalf("ApplyLocked failed: %v", err)
	}
	_ = res

	// 2. External read lock across GetLocked
	sm.RLock()
	val, found := sm.GetLocked("k1")
	sm.RUnlock()
	if !found || !bytes.Equal(val, []byte("v1")) {
		t.Fatalf("expected 'v1', got found=%v, val=%s", found, string(val))
	}
}

// Test 30: TestCanonicalEncode_Determinism proves that identical semantic commands
// produce byte-identical CanonicalEncode output and identical SHA-256 hashes across
// independent constructions, and that RequestID differences do not affect the hash (I-017).
func TestCanonicalEncode_Determinism(t *testing.T) {
	cmdA := Command{
		OperationType: Set,
		Key:           "test-key-determinism",
		Value:         []byte("test-value-12345"),
		RequestID:     "req-A",
	}
	cmdB := Command{
		OperationType: Set,
		Key:           string([]byte("test-key-determinism")),
		Value:         bytes.Clone([]byte("test-value-12345")),
		RequestID:     "req-B", // different RequestID must not alter canonical encoding
	}

	encA := CanonicalEncode(cmdA)
	encB := CanonicalEncode(cmdB)

	if !bytes.Equal(encA, encB) {
		t.Fatalf("CanonicalEncode non-deterministic: %x != %x", encA, encB)
	}

	hashA := CommandPayloadHash(cmdA)
	hashB := CommandPayloadHash(cmdB)

	if !bytes.Equal(hashA, hashB) {
		t.Fatalf("CommandPayloadHash non-deterministic: %x != %x", hashA, hashB)
	}
}

// Test 31: TestCanonicalEncode_Disambiguation proves that commands with different
// fields produce distinct canonical encodings and distinct SHA-256 hashes even if their
// concatenated raw characters might appear ambiguous without length prefixing (I-017).
func TestCanonicalEncode_Disambiguation(t *testing.T) {
	pairs := []struct {
		name string
		cmd1 Command
		cmd2 Command
	}{
		{
			name: "key/value boundary shift",
			cmd1: Command{OperationType: Set, Key: "ab", Value: []byte("c")},
			cmd2: Command{OperationType: Set, Key: "a", Value: []byte("bc")},
		},
		{
			name: "operation/key boundary shift",
			cmd1: Command{OperationType: "SE", Key: "Tkey", Value: []byte("v")},
			cmd2: Command{OperationType: "SET", Key: "key", Value: []byte("v")},
		},
		{
			name: "SET empty value vs DELETE",
			cmd1: Command{OperationType: Set, Key: "k", Value: []byte("")},
			cmd2: Command{OperationType: Delete, Key: "k", Value: nil},
		},
	}

	for _, p := range pairs {
		t.Run(p.name, func(t *testing.T) {
			enc1 := CanonicalEncode(p.cmd1)
			enc2 := CanonicalEncode(p.cmd2)
			if bytes.Equal(enc1, enc2) {
				t.Fatalf("expected distinct canonical encodings for %s, got identical: %x", p.name, enc1)
			}
			hash1 := CommandPayloadHash(p.cmd1)
			hash2 := CommandPayloadHash(p.cmd2)
			if bytes.Equal(hash1, hash2) {
				t.Fatalf("expected distinct hashes for %s, got identical: %x", p.name, hash1)
			}
		})
	}
}

// Test 32: TestApplyDedup_SamePayload proves that retrying a command with the same
// RequestID and same payload results in a dedup hit, returning the cached result
// without re-executing state machine mutation (I-017).
func TestApplyDedup_SamePayload(t *testing.T) {
	sm := NewKVStateMachine()

	cmd := Command{
		OperationType: Set,
		Key:           "dedup-key",
		Value:         []byte("val-1"),
		RequestID:     "req-dedup-1",
	}

	// 1. First apply: fresh execution
	res1, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("first Apply failed: %v", err)
	}

	val, found := sm.Get("dedup-key")
	if !found || !bytes.Equal(val, []byte("val-1")) {
		t.Fatalf("expected 'val-1', got %s", string(val))
	}

	// Tamper with in-memory KV directly under lock to prove second apply does NOT re-execute
	sm.Lock()
	sm.kv["dedup-key"] = []byte("tampered-val")
	sm.Unlock()

	// 2. Second apply: duplicate RequestID with same payload -> dedup hit
	res2, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("second Apply failed: %v", err)
	}
	if !bytes.Equal(res1.Value, res2.Value) {
		t.Fatalf("expected cached result %v, got %v", res1, res2)
	}

	// Verify KV store was NOT mutated back to "val-1" (dedup hit avoided re-execution)
	val, found = sm.Get("dedup-key")
	if !found || !bytes.Equal(val, []byte("tampered-val")) {
		t.Fatalf("expected 'tampered-val' preserved on dedup hit, got %s", string(val))
	}
}

// Test 34: TestApplyDedup_NeverCommitted proves that if an original write was never
// committed/applied, a retry with the same RequestID is applied fresh (I-017).
func TestApplyDedup_NeverCommitted(t *testing.T) {
	sm := NewKVStateMachine()

	// RequestID "req-uncommitted" was never submitted to sm.Apply.
	// When retry arrives, it must be applied fresh.
	cmd := Command{
		OperationType: Set,
		Key:           "retry-key",
		Value:         []byte("retry-val"),
		RequestID:     "req-uncommitted",
	}

	res, err := sm.Apply(cmd)
	if err != nil {
		t.Fatalf("Apply failed: %v", err)
	}
	_ = res

	val, found := sm.Get("retry-key")
	if !found || !bytes.Equal(val, []byte("retry-val")) {
		t.Fatalf("expected 'retry-val', got found=%v, val=%s", found, string(val))
	}
}
