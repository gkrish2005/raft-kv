package storage

import (
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

// InMemoryLogStore provides an in-memory LogStore implementation for deterministic Level 1/2 tests.
type InMemoryLogStore struct {
	mu          sync.RWMutex
	entries     map[uint64]*raftv1.LogEntry
	lastIndex   uint64
	commitIndex uint64
	// Optional hook for fault injection (e.g. fsync delay/error simulation)
	appendHook   func([]*raftv1.LogEntry) error
	truncateHook func(uint64) error
}

// NewInMemoryLogStore creates a new empty InMemoryLogStore.
func NewInMemoryLogStore() *InMemoryLogStore {
	return &InMemoryLogStore{
		entries: make(map[uint64]*raftv1.LogEntry),
	}
}

// SetCommitIndex updates the internal commitIndex barrier for invariant I-011 enforcement.
func (s *InMemoryLogStore) SetCommitIndex(commit uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.commitIndex = commit
}

// SetAppendHook sets a hook called before in-memory mutation in Append.
func (s *InMemoryLogStore) SetAppendHook(hook func([]*raftv1.LogEntry) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.appendHook = hook
}

// SetTruncateHook sets a hook called before in-memory mutation in TruncateFrom.
func (s *InMemoryLogStore) SetTruncateHook(hook func(uint64) error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.truncateHook = hook
}

// Append appends entries if contiguous and valid.
func (s *InMemoryLogStore) Append(entries []*raftv1.LogEntry) error {
	if len(entries) == 0 {
		return nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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

	if s.appendHook != nil {
		if err := s.appendHook(entries); err != nil {
			return err
		}
	}

	for _, entry := range entries {
		s.entries[entry.Index] = proto.Clone(entry).(*raftv1.LogEntry)
	}
	s.lastIndex = entries[len(entries)-1].Index

	return nil
}

// TruncateFrom truncates uncommitted entries from index to lastIndex.
func (s *InMemoryLogStore) TruncateFrom(index uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if index == 0 {
		return fmt.Errorf("%w: index 0 cannot be truncated", ErrInvalidIndex)
	}
	if index <= s.commitIndex {
		return fmt.Errorf("%w: attempted truncation at index %d <= commitIndex %d", ErrCommittedTruncate, index, s.commitIndex)
	}
	if index == s.lastIndex+1 {
		return nil
	}
	if index > s.lastIndex+1 {
		return fmt.Errorf("%w: truncate index %d > lastIndex+1 %d", ErrTruncateOutOfRange, index, s.lastIndex+1)
	}

	if s.truncateHook != nil {
		if err := s.truncateHook(index); err != nil {
			return err
		}
	}

	for i := index; i <= s.lastIndex; i++ {
		delete(s.entries, i)
	}
	s.lastIndex = index - 1

	return nil
}

// Get returns the entry at index.
func (s *InMemoryLogStore) Get(index uint64) (*raftv1.LogEntry, error) {
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

// LastIndex returns the current highest index.
func (s *InMemoryLogStore) LastIndex() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.lastIndex
}

var _ LogStore = (*InMemoryLogStore)(nil)
