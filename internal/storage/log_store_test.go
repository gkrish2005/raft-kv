package storage

import (
	"encoding/binary"
	"errors"
	"os"
	"path/filepath"
	"testing"

	raftv1 "raftkv/proto/raft/v1"
)

func makeEntries(startIdx uint64, count int, term uint64) []*raftv1.LogEntry {
	entries := make([]*raftv1.LogEntry, count)
	for i := 0; i < count; i++ {
		entries[i] = &raftv1.LogEntry{
			Index: startIdx + uint64(i),
			Term:  term,
			Command: &raftv1.Command{
				OperationType: "SET",
				Key:           "k",
				Value:         []byte("v"),
				RequestId:     "req-1",
			},
		}
	}
	return entries
}

func TestFileLogStoreAppendAndGet(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	store, err := NewFileLogStore(path)
	if err != nil {
		t.Fatalf("NewFileLogStore failed: %v", err)
	}
	defer store.Close()

	if store.LastIndex() != 0 {
		t.Fatalf("expected lastIndex=0 on empty store, got %d", store.LastIndex())
	}

	// Append empty batch (no-op)
	if err := store.Append(nil); err != nil {
		t.Fatalf("Append(nil) should succeed, got %v", err)
	}

	// Append first batch: entries 1..3
	entries := makeEntries(1, 3, 1)
	if err := store.Append(entries); err != nil {
		t.Fatalf("Append entries 1..3 failed: %v", err)
	}

	if store.LastIndex() != 3 {
		t.Fatalf("expected lastIndex=3, got %d", store.LastIndex())
	}

	// Verify entries can be read
	for i := uint64(1); i <= 3; i++ {
		e, err := store.Get(i)
		if err != nil {
			t.Fatalf("Get(%d) failed: %v", i, err)
		}
		if e.Index != i || e.Term != 1 {
			t.Fatalf("Get(%d) returned unexpected entry: %+v", i, e)
		}
	}

	// Non-existent index
	if _, err := store.Get(4); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound for index 4, got %v", err)
	}
	if _, err := store.Get(0); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound for index 0, got %v", err)
	}
}

func TestFileLogStorePreconditions(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	store, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	// Append starting at index 2 (gap on empty log)
	gapEntries := makeEntries(2, 2, 1)
	if err := store.Append(gapEntries); !errors.Is(err, ErrGapDetected) {
		t.Fatalf("expected ErrGapDetected, got %v", err)
	}

	// Append valid entry 1
	if err := store.Append(makeEntries(1, 1, 1)); err != nil {
		t.Fatal(err)
	}

	// Attempt overwrite without TruncateFrom (appending index 1 again)
	if err := store.Append(makeEntries(1, 1, 2)); !errors.Is(err, ErrOverwriteAttempt) {
		t.Fatalf("expected ErrOverwriteAttempt, got %v", err)
	}

	// Non-contiguous batch: [2, 4]
	badBatch := []*raftv1.LogEntry{
		{Index: 2, Term: 1},
		{Index: 4, Term: 1},
	}
	if err := store.Append(badBatch); !errors.Is(err, ErrGapDetected) {
		t.Fatalf("expected ErrGapDetected for non-contiguous batch, got %v", err)
	}
}

func TestFileLogStoreTruncateFrom(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	store, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()

	if err := store.Append(makeEntries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	if store.LastIndex() != 5 {
		t.Fatalf("expected lastIndex=5, got %d", store.LastIndex())
	}

	// Edge case: TruncateFrom(0) -> error
	if err := store.TruncateFrom(0); !errors.Is(err, ErrInvalidIndex) {
		t.Fatalf("expected ErrInvalidIndex for TruncateFrom(0), got %v", err)
	}

	// Edge case: TruncateFrom(lastIndex+1 = 6) -> no-op
	if err := store.TruncateFrom(6); err != nil {
		t.Fatalf("expected no-op for TruncateFrom(6), got %v", err)
	}
	if store.LastIndex() != 5 {
		t.Fatalf("expected lastIndex=5 after no-op truncate, got %d", store.LastIndex())
	}

	// Edge case: TruncateFrom(> lastIndex+1 = 7) -> error
	if err := store.TruncateFrom(7); !errors.Is(err, ErrTruncateOutOfRange) {
		t.Fatalf("expected ErrTruncateOutOfRange for TruncateFrom(7), got %v", err)
	}

	// Invariant I-011: committed entries cannot be truncated
	store.SetCommitIndex(3)
	if err := store.TruncateFrom(3); !errors.Is(err, ErrCommittedTruncate) {
		t.Fatalf("expected ErrCommittedTruncate for TruncateFrom(3 <= commitIndex), got %v", err)
	}
	if err := store.TruncateFrom(2); !errors.Is(err, ErrCommittedTruncate) {
		t.Fatalf("expected ErrCommittedTruncate for TruncateFrom(2 < commitIndex), got %v", err)
	}

	// Valid TruncateFrom(4): truncates entries 4 and 5
	if err := store.TruncateFrom(4); err != nil {
		t.Fatalf("TruncateFrom(4) failed: %v", err)
	}
	if store.LastIndex() != 3 {
		t.Fatalf("expected lastIndex=3 after truncate, got %d", store.LastIndex())
	}

	// Verify entries 1..3 exist, 4..5 are gone
	for i := uint64(1); i <= 3; i++ {
		if _, err := store.Get(i); err != nil {
			t.Fatalf("Get(%d) should succeed, got %v", i, err)
		}
	}
	if _, err := store.Get(4); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound for index 4, got %v", err)
	}
	if _, err := store.Get(5); !errors.Is(err, ErrEntryNotFound) {
		t.Fatalf("expected ErrEntryNotFound for index 5, got %v", err)
	}

	// Append new suffix starting at 4 in term 2
	newEntries := makeEntries(4, 2, 2)
	if err := store.Append(newEntries); err != nil {
		t.Fatalf("Append after truncate failed: %v", err)
	}
	if store.LastIndex() != 5 {
		t.Fatalf("expected lastIndex=5, got %d", store.LastIndex())
	}

	e4, _ := store.Get(4)
	if e4.Term != 2 {
		t.Fatalf("expected entry 4 to have term 2, got %d", e4.Term)
	}
}

func TestFileLogStoreReplayAndTornTailRecovery(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	// Create and write 5 entries
	store1, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store1.Append(makeEntries(1, 5, 1)); err != nil {
		t.Fatal(err)
	}
	store1.Close()

	// Re-open and verify all 5 entries replayed
	store2, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if store2.LastIndex() != 5 {
		t.Fatalf("expected lastIndex=5 on replay, got %d", store2.LastIndex())
	}
	store2.Close()

	// Simulate torn tail by appending garbage bytes to the file
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// Write partial record length + corrupt bytes
	_, _ = f.Write([]byte{0x05, 0x00, 0x00, 0x00, 0xAA, 0xBB})
	_ = f.Sync()
	_ = f.Close()

	// Reopen: should recover to intact prefix (5 entries) and discard the torn tail
	store3, err := NewFileLogStore(path)
	if err != nil {
		t.Fatalf("NewFileLogStore failed on torn tail: %v", err)
	}
	defer store3.Close()

	if store3.LastIndex() != 5 {
		t.Fatalf("expected lastIndex=5 after torn tail recovery, got %d", store3.LastIndex())
	}

	// Append should now succeed contiguously at index 6
	if err := store3.Append(makeEntries(6, 1, 2)); err != nil {
		t.Fatalf("Append after torn tail recovery failed: %v", err)
	}
	if store3.LastIndex() != 6 {
		t.Fatalf("expected lastIndex=6, got %d", store3.LastIndex())
	}
}

func TestFileLogStoreCorruptChecksumDiscardsSuffix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	store1, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store1.Append(makeEntries(1, 3, 1)); err != nil {
		t.Fatal(err)
	}
	store1.Close()

	// Corrupt the CRC of the 3rd record
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Corrupt the last 4 bytes (CRC of record 3)
	data[len(data)-1] ^= 0xFF
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	// Replay should safely discard record 3 and recover prefix [1, 2]
	store2, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	if store2.LastIndex() != 2 {
		t.Fatalf("expected lastIndex=2 after corrupt checksum recovery, got %d", store2.LastIndex())
	}
}

func TestFileLogStoreOversizedLengthDiscardsSuffix(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.wal")

	store1, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := store1.Append(makeEntries(1, 2, 1)); err != nil {
		t.Fatal(err)
	}
	store1.Close()

	// Append an oversized length prefix (e.g. 0xFFFFFFFF)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	var lenBuf [4]byte
	binary.LittleEndian.PutUint32(lenBuf[:], 0xFFFFFFFF)
	_, _ = f.Write(lenBuf[:])
	_ = f.Close()

	// Replay should discard the oversized record prefix and keep entries 1..2
	store2, err := NewFileLogStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer store2.Close()

	if store2.LastIndex() != 2 {
		t.Fatalf("expected lastIndex=2, got %d", store2.LastIndex())
	}
}

func TestInMemoryLogStoreContract(t *testing.T) {
	store := NewInMemoryLogStore()

	if err := store.Append(makeEntries(1, 4, 1)); err != nil {
		t.Fatal(err)
	}
	if store.LastIndex() != 4 {
		t.Fatalf("expected lastIndex=4, got %d", store.LastIndex())
	}

	// Invariant I-011
	store.SetCommitIndex(2)
	if err := store.TruncateFrom(2); !errors.Is(err, ErrCommittedTruncate) {
		t.Fatalf("expected ErrCommittedTruncate, got %v", err)
	}

	// Valid truncate at 3
	if err := store.TruncateFrom(3); err != nil {
		t.Fatal(err)
	}
	if store.LastIndex() != 2 {
		t.Fatalf("expected lastIndex=2, got %d", store.LastIndex())
	}
}
