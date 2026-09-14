package storage

import (
	"os"
	"testing"
)

func TestFileTermVoteStorePersistsAndIncrementsBootID(t *testing.T) {
	path := t.TempDir() + "/termvote"
	s := NewFileTermVoteStore(path)
	term, vote, boot, err := s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 0 || vote != "" || boot != 1 {
		t.Fatalf("first boot = (%d,%q,%d)", term, vote, boot)
	}
	if err := s.Save(4, "node-a", boot); err != nil {
		t.Fatal(err)
	}
	term, vote, boot, err = s.Load()
	if err != nil {
		t.Fatal(err)
	}
	if term != 4 || vote != "node-a" || boot != 2 {
		t.Fatalf("restart = (%d,%q,%d)", term, vote, boot)
	}
}

func TestFileTermVoteStoreRejectsCorruptionAndOrphanTemp(t *testing.T) {
	path := t.TempDir() + "/termvote"
	s := NewFileTermVoteStore(path)
	if err := os.WriteFile(path, []byte("broken"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.Load(); err == nil {
		t.Fatal("corrupt canonical record was accepted")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path+".tmp", []byte("orphan"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, _, err := s.Load(); err == nil {
		t.Fatal("orphan temp record was accepted")
	}
}
