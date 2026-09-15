package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
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

// FileLogStore is a durable append-only WAL backed by a single file with durable truncation.
type FileLogStore struct {
	mu          sync.RWMutex
	path        string
	file        *os.File
	offsets     map[uint64]int64
	entries     map[uint64]*raftv1.LogEntry
	lastIndex   uint64
	commitIndex uint64
}

// NewFileLogStore opens or creates a WAL file at path, replaying existing records to rebuild the offset map.
func NewFileLogStore(path string) (*FileLogStore, error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("create log store directory: %w", err)
	}

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open log file: %w", err)
	}

	store := &FileLogStore{
		path:    path,
		file:    f,
		offsets: make(map[uint64]int64),
		entries: make(map[uint64]*raftv1.LogEntry),
	}

	if err := store.replay(); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("replay log file: %w", err)
	}

	return store, nil
}

func (s *FileLogStore) replay() error {
	if _, err := s.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek log start: %w", err)
	}

	var validOffset int64 = 0
	var expectedIndex uint64 = 1

	for {
		startOffset := validOffset
		var lenBuf [4]byte
		n, err := io.ReadFull(s.file, lenBuf[:])
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Clean or partial EOF at record start
			if n > 0 {
				_ = s.truncateFileAt(startOffset)
			}
			break
		}
		if err != nil {
			return fmt.Errorf("read record length: %w", err)
		}

		recordLen := binary.LittleEndian.Uint32(lenBuf[:])
		if recordLen == 0 || recordLen > MaxWALRecordSize {
			// Corrupt/oversized length prefix: stop and truncate
			_ = s.truncateFileAt(startOffset)
			break
		}

		payload := make([]byte, recordLen)
		if _, err := io.ReadFull(s.file, payload); err != nil {
			// Torn payload: stop and truncate
			_ = s.truncateFileAt(startOffset)
			break
		}

		var crcBuf [4]byte
		if _, err := io.ReadFull(s.file, crcBuf[:]); err != nil {
			// Torn CRC: stop and truncate
			_ = s.truncateFileAt(startOffset)
			break
		}

		expectedCRC := binary.LittleEndian.Uint32(crcBuf[:])
		if crc32.ChecksumIEEE(payload) != expectedCRC {
			// Checksum mismatch: stop and truncate
			_ = s.truncateFileAt(startOffset)
			break
		}

		var record raftv1.WALRecord
		if err := proto.Unmarshal(payload, &record); err != nil {
			_ = s.truncateFileAt(startOffset)
			break
		}
		entry := record.GetEntry()
		if entry == nil || entry.Index != expectedIndex {
			// Invalid entry or gap detected: stop and truncate
			_ = s.truncateFileAt(startOffset)
			break
		}

		s.offsets[entry.Index] = startOffset
		s.entries[entry.Index] = entry
		s.lastIndex = entry.Index
		expectedIndex++
		validOffset = startOffset + 4 + int64(recordLen) + 4
	}

	return nil
}

func (s *FileLogStore) truncateFileAt(offset int64) error {
	if err := s.file.Truncate(offset); err != nil {
		return err
	}
	if err := s.file.Sync(); err != nil {
		return err
	}
	_, err := s.file.Seek(offset, io.SeekStart)
	return err
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

	// Prepare records
	type pendingRecord struct {
		index  uint64
		offset int64
		entry  *raftv1.LogEntry
		bytes  []byte
	}

	fileInfo, err := s.file.Stat()
	if err != nil {
		return fmt.Errorf("stat log file: %w", err)
	}
	currentOffset := fileInfo.Size()

	pending := make([]pendingRecord, len(entries))
	var totalBytes []byte

	for i, entry := range entries {
		record := &raftv1.WALRecord{Entry: entry}
		payload, err := proto.Marshal(record)
		if err != nil {
			return fmt.Errorf("marshal wal record: %w", err)
		}
		if uint32(len(payload)) > MaxWALRecordSize {
			return fmt.Errorf("wal record exceeds maximum size %d", MaxWALRecordSize)
		}

		rec := make([]byte, 4+len(payload)+4)
		binary.LittleEndian.PutUint32(rec[:4], uint32(len(payload)))
		copy(rec[4:], payload)
		binary.LittleEndian.PutUint32(rec[4+len(payload):], crc32.ChecksumIEEE(payload))

		pending[i] = pendingRecord{
			index:  entry.Index,
			offset: currentOffset,
			entry:  proto.Clone(entry).(*raftv1.LogEntry),
			bytes:  rec,
		}
		currentOffset += int64(len(rec))
		totalBytes = append(totalBytes, rec...)
	}

	// Write and fsync BEFORE mutating in-memory state (I-018)
	if _, err := s.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("seek log end: %w", err)
	}

	var written int
	if written, err = s.file.Write(totalBytes); err == nil && written != len(totalBytes) {
		err = io.ErrShortWrite
	}
	if err == nil {
		err = s.file.Sync()
	}
	if err != nil {
		// Durable write failed: fail closed, do not mutate memory
		_ = s.truncateFileAt(fileInfo.Size())
		return fmt.Errorf("durable write failed: %w", err)
	}

	// In-memory state mutation strictly AFTER fsync succeeds (I-018)
	for _, p := range pending {
		s.offsets[p.index] = p.offset
		s.entries[p.index] = p.entry
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
	if err := s.truncateFileAt(offset); err != nil {
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

// Close closes the underlying WAL file cleanly.
func (s *FileLogStore) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.file != nil {
		err := s.file.Close()
		s.file = nil
		return err
	}
	return nil
}

var _ LogStore = (*FileLogStore)(nil)
