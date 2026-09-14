package cluster

import (
	"os"
	"path/filepath"
	"testing"
)

func TestClusterConfig_LoadValidJSON(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.json")

	content := `{
		"node_id": "node-1",
		"peers": [
			{"id": "node-2", "address": "localhost:50052"},
			{"id": "node-3", "address": "localhost:50053"}
		]
	}`

	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.NodeID != "node-1" {
		t.Errorf("expected node_id 'node-1', got %s", cfg.NodeID)
	}
	if len(cfg.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(cfg.Peers))
	}
	if cfg.Peers[0].ID != "node-2" || cfg.Peers[0].Address != "localhost:50052" {
		t.Errorf("unexpected peer 0: %+v", cfg.Peers[0])
	}
}

func TestClusterConfig_LoadValidYAML(t *testing.T) {
	tmpDir := t.TempDir()
	cfgPath := filepath.Join(tmpDir, "config.yaml")

	content := `
node_id: "node-1"
peers:
  - id: "node-2"
    address: "localhost:50052"
  - id: "node-3"
    address: "localhost:50053"
`

	if err := os.WriteFile(cfgPath, []byte(content), 0644); err != nil {
		t.Fatalf("failed to write test file: %v", err)
	}

	cfg, err := LoadConfig(cfgPath)
	if err != nil {
		t.Fatalf("LoadConfig failed: %v", err)
	}

	if cfg.NodeID != "node-1" {
		t.Errorf("expected node_id 'node-1', got %s", cfg.NodeID)
	}
	if len(cfg.Peers) != 2 {
		t.Fatalf("expected 2 peers, got %d", len(cfg.Peers))
	}
}

func TestClusterConfig_ValidationErrors(t *testing.T) {
	// Empty node_id
	cfg1 := &ClusterConfig{
		NodeID: "",
		Peers:  []PeerAddr{{ID: "node-2", Address: "localhost:50052"}},
	}
	if err := cfg1.Validate(); err == nil {
		t.Errorf("expected error for empty node_id, got nil")
	}

	// Empty peer address
	cfg2 := &ClusterConfig{
		NodeID: "node-1",
		Peers:  []PeerAddr{{ID: "node-2", Address: ""}},
	}
	if err := cfg2.Validate(); err == nil {
		t.Errorf("expected error for empty peer address, got nil")
	}

	// Duplicate peer ID
	cfg3 := &ClusterConfig{
		NodeID: "node-1",
		Peers: []PeerAddr{
			{ID: "node-2", Address: "localhost:50052"},
			{ID: "node-2", Address: "localhost:50053"},
		},
	}
	if err := cfg3.Validate(); err == nil {
		t.Errorf("expected error for duplicate peer ID, got nil")
	}
}
