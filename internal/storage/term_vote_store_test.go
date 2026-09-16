package storage

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/protobuf/proto"

	raftv1 "raftkv/proto/raft/v1"
)

// Test 10: Basic Save/Load Round-Trip
func TestTermVoteStore_SaveLoadRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termvote")
	s := NewFileTermVoteStore(path, []string{"node-1", "node-2"})

	// First load initializes first boot
	term, vote, boot, err := s.Load()
	if err != nil {
		t.Fatalf("initial Load failed: %v", err)
	}
	if term != 0 || vote != "" || boot != 1 {
		t.Fatalf("expected (0, \"\", 1), got (%d, %q, %d)", term, vote, boot)
	}

	// Save term 5, vote "node-2", bootID 1
	if err := s.Save(5, "node-2", boot); err != nil {
		t.Fatalf("Save failed: %v", err)
	}

	// Reopen store and verify Load increments bootID and recovers term/vote
	s2 := NewFileTermVoteStore(path, []string{"node-1", "node-2"})
	term2, vote2, boot2, err := s2.Load()
	if err != nil {
		t.Fatalf("second Load failed: %v", err)
	}
	if term2 != 5 || vote2 != "node-2" || boot2 != 2 {
		t.Fatalf("expected (5, \"node-2\", 2), got (%d, %q, %d)", term2, vote2, boot2)
	}
}

// Test 11: First-Boot Immediate Persistence (I-020)
// Must observe bootID=1 on first boot, bootID=2 on genuine second boot, bootID=3 on third boot.
func TestTermVoteStore_FirstBootImmediatePersistence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termvote")
	nodes := []string{"node-1", "node-2"}

	// First boot
	s1 := NewFileTermVoteStore(path, nodes)
	term1, vote1, boot1, err := s1.Load()
	if err != nil {
		t.Fatalf("boot 1 failed: %v", err)
	}
	if term1 != 0 || vote1 != "" || boot1 != 1 {
		t.Fatalf("expected boot 1 = (0, \"\", 1), got (%d, %q, %d)", term1, vote1, boot1)
	}

	// Second boot against same path proves first boot was immediately durable
	s2 := NewFileTermVoteStore(path, nodes)
	term2, vote2, boot2, err := s2.Load()
	if err != nil {
		t.Fatalf("boot 2 failed: %v", err)
	}
	if term2 != 0 || vote2 != "" || boot2 != 2 {
		t.Fatalf("expected boot 2 = (0, \"\", 2), got (%d, %q, %d)", term2, vote2, boot2)
	}

	// Third boot
	s3 := NewFileTermVoteStore(path, nodes)
	term3, vote3, boot3, err := s3.Load()
	if err != nil {
		t.Fatalf("boot 3 failed: %v", err)
	}
	if term3 != 0 || vote3 != "" || boot3 != 3 {
		t.Fatalf("expected boot 3 = (0, \"\", 3), got (%d, %q, %d)", term3, vote3, boot3)
	}
}

// Test 12: Corrupt / Truncated Record Fails Startup (I-020)
func TestTermVoteStore_CorruptTruncatedFailsStartup(t *testing.T) {
	nodes := []string{"node-1"}

	// Case A: Truncated file (< 8 bytes)
	t.Run("TruncatedRecord", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		if err := os.WriteFile(path, []byte{0x01, 0x02, 0x03}, 0o600); err != nil {
			t.Fatal(err)
		}
		s := NewFileTermVoteStore(path, nodes)
		if _, _, _, err := s.Load(); err == nil {
			t.Fatal("expected Load() to fail on truncated file")
		}
	})

	// Case B: Corrupt CRC
	t.Run("ChecksumMismatch", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		if err := s.Save(1, "node-1", 1); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// Flip a bit in the CRC at the end
		data[len(data)-1] ^= 0xFF
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := s.Load(); err == nil {
			t.Fatal("expected Load() to fail on checksum mismatch")
		}
	})

	// Case C: Invalid Length Prefix
	t.Run("InvalidLength", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		data := make([]byte, 16)
		binary.LittleEndian.PutUint32(data[:4], 100) // Claims 100 bytes payload, but total is 16
		if err := os.WriteFile(path, data, 0o600); err != nil {
			t.Fatal(err)
		}
		s := NewFileTermVoteStore(path, nodes)
		if _, _, _, err := s.Load(); err == nil {
			t.Fatal("expected Load() to fail on invalid length")
		}
	})
}

// Test 13: Semantic Validity - Foreign VotedFor Rejected by Load() directly
func TestTermVoteStore_SemanticValidity_ForeignVotedFor(t *testing.T) {
	path := filepath.Join(t.TempDir(), "termvote")
	s := NewFileTermVoteStore(path, []string{"node-1", "node-2"})

	// Durably write a record with votedFor = "foreign-node-xyz"
	payload, err := proto.Marshal(&raftv1.TermVoteRecord{
		CurrentTerm: 2,
		VotedFor:    "foreign-node-xyz",
		BootId:      1,
	})
	if err != nil {
		t.Fatal(err)
	}
	rec := make([]byte, 4+len(payload)+4)
	binary.LittleEndian.PutUint32(rec[:4], uint32(len(payload)))
	copy(rec[4:], payload)
	binary.LittleEndian.PutUint32(rec[4+len(payload):], crc32.ChecksumIEEE(payload))
	if err := os.WriteFile(path, rec, 0o600); err != nil {
		t.Fatal(err)
	}

	// Load() must directly reject the foreign votedFor
	_, _, _, err = s.Load()
	if err == nil {
		t.Fatal("expected Load() to reject foreign votedFor")
	}
	if !strings.Contains(err.Error(), "foreign-node-xyz") {
		t.Fatalf("expected error mentioning foreign-node-xyz, got: %v", err)
	}
}

// Test 14: Semantic Validity (Zero BootID) and Orphan Temp File Variants (I-020, I-024)
func TestTermVoteStore_SemanticValidity_ZeroBootID_And_OrphanTemp(t *testing.T) {
	nodes := []string{"node-1"}

	// Case A: Zero boot ID rejected
	t.Run("ZeroBootID", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		payload, _ := proto.Marshal(&raftv1.TermVoteRecord{
			CurrentTerm: 1,
			VotedFor:    "node-1",
			BootId:      0,
		})
		rec := make([]byte, 4+len(payload)+4)
		binary.LittleEndian.PutUint32(rec[:4], uint32(len(payload)))
		copy(rec[4:], payload)
		binary.LittleEndian.PutUint32(rec[4+len(payload):], crc32.ChecksumIEEE(payload))
		if err := os.WriteFile(path, rec, 0o600); err != nil {
			t.Fatal(err)
		}
		s := NewFileTermVoteStore(path, nodes)
		if _, _, _, err := s.Load(); err == nil {
			t.Fatal("expected Load() to reject zero boot ID")
		}
	})

	// Case B: Canonical file present alongside valid temp file (I-024)
	// Must ignore temp file entirely and use canonical.
	t.Run("OrphanTempAlongsideCanonical", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		if err := s.Save(3, "node-1", 1); err != nil {
			t.Fatal(err)
		}
		// Write garbage to .tmp
		if err := os.WriteFile(path+".tmp", []byte("leftover garbage"), 0o600); err != nil {
			t.Fatal(err)
		}
		term, vote, boot, err := s.Load()
		if err != nil {
			t.Fatalf("Load failed with orphan temp alongside canonical: %v", err)
		}
		if term != 3 || vote != "node-1" || boot != 2 {
			t.Fatalf("expected canonical record (3, \"node-1\", 2), got (%d, %q, %d)", term, vote, boot)
		}
	})

	// Case C: Canonical file missing while temp file is present (I-024)
	// Must fail startup (refuse to start) rather than promote orphan temp file.
	t.Run("OrphanTempWithoutCanonical", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		if err := os.WriteFile(path+".tmp", []byte("orphan"), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, _, _, err := s.Load(); err == nil {
			t.Fatal("expected Load() to fail when canonical missing but temp present")
		}
	})
}

// Test 15: Replacement Atomicity Across 4 Stages (I-024)
// Exercises AfterTmpWrite, AfterTmpFsync, AfterRename, and AfterDirFsync.
func TestTermVoteStore_ReplacementAtomicity(t *testing.T) {
	nodes := []string{"node-1", "node-2"}

	// Stage 1: Crash after tmp write (before tmp fsync)
	t.Run("AfterTmpWrite", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		// Establish initial valid record: term 1, boot 1
		if err := s.Save(1, "node-1", 1); err != nil {
			t.Fatal(err)
		}

		hookFired := false
		s.Hooks.AfterTmpWrite = func() error {
			hookFired = true
			return errors.New("simulated crash after tmp write")
		}

		err := s.Save(2, "node-2", 2)
		if err == nil {
			t.Fatal("expected Save to fail from hook")
		}
		if !hookFired {
			t.Fatal("AfterTmpWrite hook was not invoked")
		}

		// Recovery must see the old canonical record (term 1)
		sRecover := NewFileTermVoteStore(path, nodes)
		term, vote, boot, err := sRecover.Load()
		if err != nil {
			t.Fatalf("Load failed after crash at AfterTmpWrite: %v", err)
		}
		if term != 1 || vote != "node-1" || boot != 2 {
			t.Fatalf("expected old record (1, \"node-1\", 2), got (%d, %q, %d)", term, vote, boot)
		}
	})

	// Stage 2: Crash after tmp fsync (before rename)
	t.Run("AfterTmpFsync", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		if err := s.Save(1, "node-1", 1); err != nil {
			t.Fatal(err)
		}

		hookFired := false
		s.Hooks.AfterTmpFsync = func() error {
			hookFired = true
			return errors.New("simulated crash after tmp fsync")
		}

		err := s.Save(2, "node-2", 2)
		if err == nil {
			t.Fatal("expected Save to fail from hook")
		}
		if !hookFired {
			t.Fatal("AfterTmpFsync hook was not invoked")
		}

		// Recovery sees valid canonical and valid tmp -> must use canonical (term 1)
		sRecover := NewFileTermVoteStore(path, nodes)
		term, vote, boot, err := sRecover.Load()
		if err != nil {
			t.Fatalf("Load failed after crash at AfterTmpFsync: %v", err)
		}
		if term != 1 || vote != "node-1" || boot != 2 {
			t.Fatalf("expected old record (1, \"node-1\", 2), got (%d, %q, %d)", term, vote, boot)
		}
	})

	// Stage 3: Crash after rename (before directory fsync)
	t.Run("AfterRename", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		if err := s.Save(1, "node-1", 1); err != nil {
			t.Fatal(err)
		}

		hookFired := false
		s.Hooks.AfterRename = func() error {
			hookFired = true
			return errors.New("simulated crash after rename")
		}

		err := s.Save(2, "node-2", 2)
		if err == nil {
			t.Fatal("expected Save to fail from hook")
		}
		if !hookFired {
			t.Fatal("AfterRename hook was not invoked")
		}

		// Since rename already completed, recovery reads the new canonical record (term 2)
		sRecover := NewFileTermVoteStore(path, nodes)
		term, vote, boot, err := sRecover.Load()
		if err != nil {
			t.Fatalf("Load failed after crash at AfterRename: %v", err)
		}
		if term != 2 || vote != "node-2" || boot != 3 {
			t.Fatalf("expected new record (2, \"node-2\", 3), got (%d, %q, %d)", term, vote, boot)
		}
	})

	// Stage 4: Crash after directory fsync
	t.Run("AfterDirFsync", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "termvote")
		s := NewFileTermVoteStore(path, nodes)
		if err := s.Save(1, "node-1", 1); err != nil {
			t.Fatal(err)
		}

		hookFired := false
		s.Hooks.AfterDirFsync = func() error {
			hookFired = true
			return errors.New("simulated crash after directory fsync")
		}

		err := s.Save(2, "node-2", 2)
		if err == nil {
			t.Fatal("expected Save to fail from hook")
		}
		if !hookFired {
			t.Fatal("AfterDirFsync hook was not invoked")
		}

		// Recovery reads the new canonical record (term 2)
		sRecover := NewFileTermVoteStore(path, nodes)
		term, vote, boot, err := sRecover.Load()
		if err != nil {
			t.Fatalf("Load failed after crash at AfterDirFsync: %v", err)
		}
		if term != 2 || vote != "node-2" || boot != 3 {
			t.Fatalf("expected new record (2, \"node-2\", 3), got (%d, %q, %d)", term, vote, boot)
		}
	})
}
