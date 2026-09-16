package storage

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

func makeTestWALRecord(term, index uint64, cmdData string) *raftv1.WALRecord {
	return &raftv1.WALRecord{
		Entry: &raftv1.LogEntry{
			Term:  term,
			Index: index,
			Command: &raftv1.Command{
				OperationType: "SET",
				Key:           "key",
				Value:         []byte(cmdData),
				RequestId:     "req-" + cmdData,
			},
		},
	}
}

// Test 1: Empty WAL
func TestWAL_Empty(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL failed: %v", err)
	}
	defer wal.Close()

	records, err := wal.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

// Test 2: One Record
func TestWAL_OneRecord(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL failed: %v", err)
	}

	rec := makeTestWALRecord(1, 1, "val1")
	if err := wal.Append(rec); err != nil {
		t.Fatalf("Append failed: %v", err)
	}
	if err := wal.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("reopen OpenFileWAL failed: %v", err)
	}
	defer wal2.Close()

	records, err := wal2.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0].Entry.Index != 1 || string(records[0].Entry.Command.Value) != "val1" {
		t.Fatalf("record mismatch: %+v", records[0])
	}
}

// Test 3: Many Records
func TestWAL_ManyRecords(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL failed: %v", err)
	}

	const count = 50
	records := make([]*raftv1.WALRecord, count)
	for i := 0; i < count; i++ {
		records[i] = makeTestWALRecord(1, uint64(i+1), string(rune('a'+i%26)))
	}

	offsets, err := wal.AppendBatch(records)
	if err != nil {
		t.Fatalf("AppendBatch failed: %v", err)
	}
	if len(offsets) != count {
		t.Fatalf("expected %d offsets, got %d", count, len(offsets))
	}
	_ = wal.Close()

	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("reopen failed: %v", err)
	}
	defer wal2.Close()

	replayed, err := wal2.ReplayWithOffsets()
	if err != nil {
		t.Fatalf("ReplayWithOffsets failed: %v", err)
	}
	if len(replayed) != count {
		t.Fatalf("expected %d records, got %d", count, len(replayed))
	}
	for i := 0; i < count; i++ {
		if replayed[i].Offset != offsets[i] {
			t.Fatalf("record %d offset mismatch: expected %d, got %d", i, offsets[i], replayed[i].Offset)
		}
		if replayed[i].Record.Entry.Index != uint64(i+1) {
			t.Fatalf("record %d index mismatch: %d", i, replayed[i].Record.Entry.Index)
		}
	}
}

// Test 4: Truncated Final Record (Torn Tail)
func TestWAL_TruncatedFinalRecord_TornTail(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatalf("OpenFileWAL failed: %v", err)
	}

	r1 := makeTestWALRecord(1, 1, "first")
	r2 := makeTestWALRecord(1, 2, "second")
	if err := wal.Append(r1); err != nil {
		t.Fatal(err)
	}
	if err := wal.Append(r2); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Append a partial 5-byte garbage suffix simulating torn write of a 3rd record
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{0x20, 0x00, 0x00, 0x00, 0xAA}); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Replay should recover r1 and r2, truncate away the torn tail, and sync
	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	replayed, err := wal2.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected 2 valid records, got %d", len(replayed))
	}
	if replayed[0].Entry.Index != 1 || replayed[1].Entry.Index != 2 {
		t.Fatalf("unexpected replayed records: %+v", replayed)
	}

	// Verify that reopening again sees clean EOF at the end of r2
	wal3, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal3.Close()
	replayed3, err := wal3.ReplayAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed3) != 2 {
		t.Fatalf("expected file to have been truncated on disk to 2 records, got %d", len(replayed3))
	}
}

// Test 5: Mid-Log Corruption (Non-Final Record) Discards Suffix
func TestWAL_MidLogCorruption_DiscardSuffix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	r1 := makeTestWALRecord(1, 1, "first")
	r2 := makeTestWALRecord(1, 2, "second")
	r3 := makeTestWALRecord(1, 3, "third")
	offsets, err := wal.AppendBatch([]*raftv1.WALRecord{r1, r2, r3})
	if err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Corrupt record 2's payload bytes (while record 3's bytes are physically still present later in the file)
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// offset[1] is start of r2. length is at offset[1], payload is at offset[1]+4.
	// Corrupt payload byte
	if _, err := f.WriteAt([]byte{0xFF, 0xFF}, offsets[1]+5); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Replay must treat mid-log corruption identically to a torn tail: stop scanning
	// at r2 and truncate from r2 onward, discarding r2 and r3 to prevent log gaps.
	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	replayed, err := wal2.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected exactly 1 record before corruption, got %d", len(replayed))
	}
	if replayed[0].Entry.Index != 1 {
		t.Fatalf("expected record 1, got %+v", replayed[0])
	}
}

// Test 6: Corrupt Checksum
func TestWAL_CorruptChecksum(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	r1 := makeTestWALRecord(1, 1, "valid")
	offsets, err := wal.AppendBatch([]*raftv1.WALRecord{r1})
	if err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Corrupt the CRC at the end of record 1
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(path, os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	// The last 4 bytes are CRC32
	if _, err := f.WriteAt([]byte{0x00, 0x00, 0x00, 0x00}, info.Size()-4); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	replayed, err := wal2.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if len(replayed) != 0 {
		t.Fatalf("expected 0 records due to CRC corruption at offset %d, got %d", offsets[0], len(replayed))
	}
}

// Test 7: Corrupt / Oversized Length Prefix
func TestWAL_CorruptOversizedLengthPrefix(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := makeTestWALRecord(1, 1, "good")
	if err := wal.Append(r1); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Append an oversized length prefix (e.g., 0xFFFFFFFF = ~4 GiB, > 1 MiB)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	badLen := make([]byte, 4)
	binary.LittleEndian.PutUint32(badLen, 0xFFFFFFFF)
	if _, err := f.Write(badLen); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	// Should reject before allocation, truncate bad length, and return only r1
	replayed, err := wal2.ReplayAll()
	if err != nil {
		t.Fatalf("ReplayAll failed: %v", err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 valid record, got %d", len(replayed))
	}
	if replayed[0].Entry.Index != 1 {
		t.Fatalf("expected record 1, got %+v", replayed[0])
	}
}

// Test 8: Crash Between Append and Fsync
func TestWAL_CrashBetweenAppendAndFsync(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	r1 := makeTestWALRecord(1, 1, "committed_and_synced")
	if err := wal.Append(r1); err != nil {
		t.Fatal(err)
	}

	// Record the synced size
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	syncedSize := info.Size()

	// Write an unsynced record directly to the file without fsync,
	// then simulate crash by truncating to the last synced size
	r2 := makeTestWALRecord(1, 2, "unflushed_append")
	p2, _ := proto.Marshal(r2)
	frame2 := encodeWALFrame(p2)
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(frame2); err != nil {
		t.Fatal(err)
	}
	_ = f.Close()

	// Simulate crash before fsync: disk rollback to last synced size
	if err := os.Truncate(path, syncedSize); err != nil {
		t.Fatal(err)
	}

	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	replayed, err := wal2.ReplayAll()
	if err != nil {
		t.Fatal(err)
	}
	if len(replayed) != 1 {
		t.Fatalf("expected 1 record after unsynced crash, got %d", len(replayed))
	}
	if string(replayed[0].Entry.Command.Value) != "committed_and_synced" {
		t.Fatalf("expected committed record, got %+v", replayed[0])
	}
}

// Test 9: Crash Immediately After TruncateFrom (General Safety Properties)
func TestWAL_CrashImmediatelyAfterTruncateFrom(t *testing.T) {
	path := filepath.Join(t.TempDir(), "wal")
	wal, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}

	// Write 5 records
	records := make([]*raftv1.WALRecord, 5)
	for i := 0; i < 5; i++ {
		records[i] = makeTestWALRecord(1, uint64(i+1), string(rune('1'+i)))
	}
	offsets, err := wal.AppendBatch(records)
	if err != nil {
		t.Fatal(err)
	}

	// Truncate at record 3 (offset[2])
	if err := wal.TruncateAt(offsets[2]); err != nil {
		t.Fatal(err)
	}
	_ = wal.Close()

	// Verify general safety properties per docs/architecture.md:
	// 1. WAL is structurally valid.
	// 2. Recovered log is a valid prefix (indices 1..2).
	// 3. No committed entry lost.
	wal2, err := OpenFileWAL(path)
	if err != nil {
		t.Fatal(err)
	}
	defer wal2.Close()

	replayed, err := wal2.ReplayAll()
	if err != nil {
		t.Fatalf("WAL not structurally valid after truncate crash: %v", err)
	}
	if len(replayed) != 2 {
		t.Fatalf("expected valid prefix of length 2, got %d", len(replayed))
	}
	for i := 0; i < 2; i++ {
		if replayed[i].Entry.Index != uint64(i+1) {
			t.Fatalf("prefix index mismatch at %d: %d", i, replayed[i].Entry.Index)
		}
	}
}
