package storage

import (
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

const (
	MaxWALRecordSize uint32 = 1 << 20 // 1 MiB
)

var (
	ErrEntryNotFound      = errors.New("log entry not found")
	ErrInvalidIndex       = errors.New("invalid log index")
	ErrGapDetected        = errors.New("log append would introduce a gap")
	ErrOverwriteAttempt   = errors.New("log append cannot overwrite existing entries without TruncateFrom")
	ErrCommittedTruncate  = errors.New("fatal invariant violation: cannot truncate committed log entries (I-011)")
	ErrTruncateOutOfRange = errors.New("truncate index out of range")
)

// LogEntry is an alias to protobuf LogEntry.
type LogEntry = raftv1.LogEntry

// LogStore defines the storage contract for Raft log persistence and retrieval.
type LogStore interface {
	Append(entries []*raftv1.LogEntry) error // must be durable (fsync) before returning
	TruncateFrom(index uint64) error         // must be durable (fsync) before returning
	Get(index uint64) (*raftv1.LogEntry, error)
	LastIndex() uint64
}

// FileLogStore is a durable append-only log store backed by FileWAL.
type FileLogStore struct {
	mu          sync.RWMutex
	wal         WAL
	offsets     map[uint64]int64
	entries     map[uint64]*raftv1.LogEntry
	lastIndex   uint64
	commitIndex uint64
}

// NewFileLogStore opens or creates a WAL file at path, reconstructing existing records.
func NewFileLogStore(path string) (*FileLogStore, error) {
	wal, err := OpenFileWAL(path)
	if err != nil {
		return nil, fmt.Errorf("open file wal: %w", err)
	}

	entries, offsets, lastIndex, err := ReconstructLog(wal)
	if err != nil {
		_ = wal.Close()
		return nil, fmt.Errorf("reconstruct log: %w", err)
	}

	return &FileLogStore{
		wal:       wal,
		offsets:   offsets,
		entries:   entries,
		lastIndex: lastIndex,
	}, nil
}

// Recover refreshes in-memory state from the underlying WAL on restart.
func (s *FileLogStore) Recover() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	entries, offsets, lastIndex, err := ReconstructLog(s.wal)
	if err != nil {
		return fmt.Errorf("reconstruct log: %w", err)
	}
	s.entries = entries
	s.offsets = offsets
	s.lastIndex = lastIndex
	return nil
}

// SetCommitIndex updates the internal commitIndex barrier for invariant I-011 enforcement.
func (s *FileLogStore) SetCommitIndex(commit uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = commit
}

// Append appends a contiguous batch of log entries durably.
func (s *FileLogStore) Append(entries []*raftv1.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Precondition validation
	if entries[0].Index != s.lastIndex+1 {
		if entries[0].Index <= s.lastIndex {
			return fmt.Errorf("%w: attempted index %d, lastIndex is %d", ErrOverwriteAttempt, entries[0].Index, s.lastIndex)
		}
		return fmt.Errorf("%w: attempted index %d, expected %d", ErrGapDetected, entries[0].Index, s.lastIndex+1)
	}

	for i := 1; i < len(entries); i++ {
		if entries[i].Index != entries[i-1].Index+1 {
			return fmt.Errorf("%w: batch is not contiguous at index %d", ErrGapDetected, entries[i].Index)
		}
	}

	records := make([]*raftv1.WALRecord, len(entries))
	for i, entry := range entries {
		records[i] = &raftv1.WALRecord{Entry: entry}
	}

	// AppendBatch writes and fsyncs BEFORE mutating in-memory state (I-018)
	offsets, err := s.wal.AppendBatch(records)
	if err != nil {
		return fmt.Errorf("durable write failed: %w", err)
	}

	// In-memory mutation strictly AFTER fsync succeeds (I-018)
	for i, entry := range entries {
		s.offsets[entry.Index] = offsets[i]
		s.entries[entry.Index] = proto.Clone(entry).(*raftv1.LogEntry)
	}
	s.lastIndex = entries[len(entries)-1].Index

	return nil
}

// TruncateFrom truncates the log from index to lastIndex inclusive.
func (s *FileLogStore) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index == 0 {
		return fmt.Errorf("%w: index 0 cannot be truncated", ErrInvalidIndex)
	}
	if index <= s.commitIndex {
		return fmt.Errorf("%w: attempted truncation at index %d <= commitIndex %d", ErrCommittedTruncate, index, s.commitIndex)
	}
	if index == s.lastIndex+1 {
		return nil // No-op
	}
	if index > s.lastIndex+1 {
		return fmt.Errorf("%w: truncate index %d > lastIndex+1 %d", ErrTruncateOutOfRange, index, s.lastIndex+1)
	}

	offset, ok := s.offsets[index]
	if !ok {
		return fmt.Errorf("%w: no offset for index %d", ErrEntryNotFound, index)
	}

	// Truncate on disk and fsync BEFORE mutating memory (I-018)
	if err := s.wal.TruncateAt(offset); err != nil {
		return fmt.Errorf("truncate log file: %w", err)
	}

	// In-memory mutation strictly AFTER fsync succeeds
	for i := index; i <= s.lastIndex; i++ {
		delete(s.offsets, i)
		delete(s.entries, i)
	}
	s.lastIndex = index - 1

	return nil
}

// Get returns the log entry at index.
func (s *FileLogStore) Get(index uint64) (*raftv1.LogEntry, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if index == 0 || index > s.lastIndex {
		return nil, ErrEntryNotFound
	}
	entry, ok := s.entries[index]
	if !ok {
		return nil, ErrEntryNotFound
	}
	return proto.Clone(entry).(*raftv1.LogEntry), nil
}

// LastIndex returns the highest 1-based log index present.
func (s *FileLogStore) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex
}

// Close closes the underlying WAL cleanly.
func (s *FileLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.wal != nil {
		return s.wal.Close()
	}
	return nil
}

var _ LogStore = (*FileLogStore)(nil)
