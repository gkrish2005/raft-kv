package storage

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

// TermVoteStore durably keeps Raft's persistent term and vote as one record.
type TermVoteStore interface {
	Save(term uint64, votedFor string, bootID uint64) error
	Load() (term uint64, votedFor string, bootID uint64, err error)
}

// SaveHooks defines fault-injection hooks for testing replacement atomicity.
type SaveHooks struct {
	AfterTmpWrite func() error // called after writing tmp, before fsync(tmp)
	AfterTmpFsync func() error // called after fsync(tmp), before rename
	AfterRename   func() error // called after rename, before dir fsync
	AfterDirFsync func() error // called after dir fsync
}

// FileTermVoteStore stores the canonical record at Path.
type FileTermVoteStore struct {
	Path         string
	allowedNodes map[string]bool
	Hooks        SaveHooks
}

// NewFileTermVoteStore constructs a FileTermVoteStore with configured allowed cluster node IDs.
func NewFileTermVoteStore(path string, allowedNodes []string) *FileTermVoteStore {
	m := make(map[string]bool, len(allowedNodes))
	for _, n := range allowedNodes {
		m[n] = true
	}
	return &FileTermVoteStore{
		Path:         path,
		allowedNodes: m,
	}
}

func (s *FileTermVoteStore) Save(term uint64, votedFor string, bootID uint64) error {
	if bootID == 0 {
		return errors.New("boot ID must be non-zero")
	}
	payload, err := proto.Marshal(&raftv1.TermVoteRecord{CurrentTerm: term, VotedFor: votedFor, BootId: bootID})
	if err != nil {
		return fmt.Errorf("marshal term/vote record: %w", err)
	}
	record := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(record[:4], uint32(len(payload)))
	copy(record[4:], payload)
	binary.LittleEndian.PutUint32(record[4+len(payload):], crc32.ChecksumIEEE(payload))
	dir := filepath.Dir(s.Path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create term/vote directory: %w", err)
	}
	tmp := s.Path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("open term/vote temp file: %w", err)
	}
	var written int
	if written, err = f.Write(record); err == nil && written != len(record) {
		err = io.ErrShortWrite
	}
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("write term/vote temp file: %w", err)
	}

	if s.Hooks.AfterTmpWrite != nil {
		if hookErr := s.Hooks.AfterTmpWrite(); hookErr != nil {
			_ = f.Close()
			return hookErr
		}
	}

	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("sync term/vote temp file: %w", err)
	}

	if s.Hooks.AfterTmpFsync != nil {
		if hookErr := s.Hooks.AfterTmpFsync(); hookErr != nil {
			_ = f.Close()
			return hookErr
		}
	}

	closeErr := f.Close()
	if closeErr != nil {
		return fmt.Errorf("close term/vote temp file: %w", closeErr)
	}

	if err := os.Rename(tmp, s.Path); err != nil {
		return fmt.Errorf("rename term/vote record: %w", err)
	}

	if s.Hooks.AfterRename != nil {
		if hookErr := s.Hooks.AfterRename(); hookErr != nil {
			return hookErr
		}
	}

	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open term/vote parent directory: %w", err)
	}
	err = d.Sync()
	closeErr = d.Close()
	if err != nil {
		return fmt.Errorf("sync term/vote parent directory: %w", err)
	}
	if closeErr != nil {
		return fmt.Errorf("close term/vote parent directory: %w", closeErr)
	}

	if s.Hooks.AfterDirFsync != nil {
		if hookErr := s.Hooks.AfterDirFsync(); hookErr != nil {
			return hookErr
		}
	}

	return nil
}

func (s *FileTermVoteStore) Load() (uint64, string, uint64, error) {
	data, err := os.ReadFile(s.Path)
	if errors.Is(err, os.ErrNotExist) {
		if _, tmpErr := os.Stat(s.Path + ".tmp"); tmpErr == nil {
			return 0, "", 0, errors.New("term/vote canonical record missing while temp record exists")
		} else if !errors.Is(tmpErr, os.ErrNotExist) {
			return 0, "", 0, fmt.Errorf("stat term/vote temp record: %w", tmpErr)
		}
		// First boot: synthesize {term: 0, votedFor: "", bootID: 1} and immediately persist
		if err := s.Save(0, "", 1); err != nil {
			return 0, "", 0, err
		}
		return 0, "", 1, nil
	}
	if err != nil {
		return 0, "", 0, fmt.Errorf("read term/vote record: %w", err)
	}
	if len(data) < 8 {
		return 0, "", 0, errors.New("term/vote record truncated")
	}
	n := int(binary.LittleEndian.Uint32(data[:4]))
	if n < 0 || len(data) != 8+n {
		return 0, "", 0, errors.New("term/vote record has invalid length")
	}
	payload := data[4 : 4+n]
	if binary.LittleEndian.Uint32(data[4+n:]) != crc32.ChecksumIEEE(payload) {
		return 0, "", 0, errors.New("term/vote record checksum mismatch")
	}
	var record raftv1.TermVoteRecord
	if err := proto.Unmarshal(payload, &record); err != nil {
		return 0, "", 0, fmt.Errorf("decode term/vote record: %w", err)
	}
	if record.BootId == 0 {
		return 0, "", 0, errors.New("term/vote record has zero boot ID")
	}
	// Semantic validation: votedFor == "" OR votedFor is one of the configured cluster NodeIDs
	if record.VotedFor != "" && len(s.allowedNodes) > 0 && !s.allowedNodes[record.VotedFor] {
		return 0, "", 0, fmt.Errorf("term/vote record votedFor %q not in configured cluster nodes", record.VotedFor)
	}
	if err := s.Save(record.CurrentTerm, record.VotedFor, record.BootId+1); err != nil {
		return 0, "", 0, err
	}
	return record.CurrentTerm, record.VotedFor, record.BootId + 1, nil
}

var _ TermVoteStore = (*FileTermVoteStore)(nil)
