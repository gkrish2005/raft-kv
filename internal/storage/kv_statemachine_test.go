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
