package storage

import (
	"fmt"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

// ReconstructLog sequentially reads replayed records from the WAL and reconstructs
// the in-memory log entries map, byte-offset map, and lastIndex.
// Entries must be strictly contiguous 1-based log indices (1, 2, 3...).
// If an entry is nil or has an index gap/mismatch, the WAL is truncated at that
// record's offset, discarding subsequent records to prevent log gaps.
func ReconstructLog(wal WAL) (map[uint64]*raftv1.LogEntry, map[uint64]int64, uint64, error) {
	replayed, err := wal.ReplayWithOffsets()
	if err != nil {
		return nil, nil, 0, fmt.Errorf("replay wal with offsets: %w", err)
	}

	entries := make(map[uint64]*raftv1.LogEntry)
	offsets := make(map[uint64]int64)
	var lastIndex uint64 = 0
	var expectedIndex uint64 = 1

	for _, r := range replayed {
		entry := r.Record.GetEntry()
		if entry == nil || entry.Index != expectedIndex {
			// Gap or invalid index detected: truncate WAL at this record's offset and terminate scan.
			if err := wal.TruncateAt(r.Offset); err != nil {
				return entries, offsets, lastIndex, fmt.Errorf("truncate gap at offset %d during log reconstruction: %w", r.Offset, err)
			}
			break
		}
		entries[entry.Index] = proto.Clone(entry).(*raftv1.LogEntry)
		offsets[entry.Index] = r.Offset
		lastIndex = entry.Index
		expectedIndex++
	}

	return entries, offsets, lastIndex, nil
}
