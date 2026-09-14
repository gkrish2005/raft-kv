package cluster

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

// PeerAddr defines the network address for a cluster peer.
type PeerAddr struct {
	ID      string `json:"id" yaml:"id"`
	Address string `json:"address" yaml:"address"`
}

// ClusterConfig specifies static cluster membership and this node's identity.
type ClusterConfig struct {
	NodeID string     `json:"node_id" yaml:"node_id"`
	Peers  []PeerAddr `json:"peers" yaml:"peers"`
}

// Validate checks that the cluster configuration is structurally sound.
func (c *ClusterConfig) Validate() error {
	if strings.TrimSpace(c.NodeID) == "" {
		return fmt.Errorf("node_id must not be empty")
	}

	seen := make(map[string]bool)
	for i, peer := range c.Peers {
		if strings.TrimSpace(peer.ID) == "" {
			return fmt.Errorf("peers[%d]: id must not be empty", i)
		}
		if strings.TrimSpace(peer.Address) == "" {
			return fmt.Errorf("peers[%d]: address must not be empty", i)
		}
		if seen[peer.ID] {
			return fmt.Errorf("duplicate peer id: %s", peer.ID)
		}
		seen[peer.ID] = true
	}

	return nil
}

// LoadConfig parses a JSON or YAML configuration file.
func LoadConfig(path string) (*ClusterConfig, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file %s: %w", path, err)
	}

	ext := strings.ToLower(filepath.Ext(path))
	var cfg ClusterConfig

	switch ext {
	case ".json":
		if err := json.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse JSON config: %w", err)
		}
	case ".yaml", ".yml":
		if err := yaml.Unmarshal(data, &cfg); err != nil {
			return nil, fmt.Errorf("failed to parse YAML config: %w", err)
		}
	default:
		// Attempt JSON first, then YAML fallback
		if err := json.Unmarshal(data, &cfg); err != nil {
			if yamlErr := yaml.Unmarshal(data, &cfg); yamlErr != nil {
				return nil, fmt.Errorf("unsupported config format or parse error (json: %v, yaml: %v)", err, yamlErr)
			}
		}
	}

	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid cluster config: %w", err)
	}

	return &cfg, nil
}
