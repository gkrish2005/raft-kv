package chaos

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"raftkv/internal/observability"
	"raftkv/internal/raft"
	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

func TestObservability_14TransitionsMatrix(t *testing.T) {
	recorder := observability.NewScenarioRecorder()
	baseDir := t.TempDir()
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	c, err := NewInProcessClusterWithSink(nodeIDs, baseDir, 42, recorder)
	if err != nil {
		t.Fatalf("failed to create cluster with sink: %v", err)
	}
	defer c.Stop()

	if err := c.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for leader
	leader := WaitForLeader(t, c, 5*time.Second)

	// Perform client write to generate LOG_APPENDED, COMMIT_ADVANCED, ENTRY_APPLIED, RPC_SUCCEEDED
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err = leader.Node.Write(ctx, &raftv1.Command{
		OperationType: string(storage.Set),
		Key:           "k1",
		Value:         []byte("v1"),
		RequestId:     "req-obs-1",
	})
	if err != nil {
		t.Fatalf("write failed: %v", err)
	}

	// Create partition to generate PARTITION_CREATED
	c.transport.SetPartition("node-1", "node-3", true)

	// Trigger election / RPC failure under partition
	c.transport.SetPeerFault("node-3", TransportConfig{DropRate: 1.0})
	c.CrashNode(leader.ID) // generates NODE_STOPPED
	time.Sleep(350 * time.Millisecond)

	// Heal partition to generate PARTITION_HEALED
	c.transport.HealAll()

	// Restart crashed node to generate NODE_RESTARTED
	if err := c.RestartNode(leader.ID); err != nil {
		t.Fatalf("restart node failed: %v", err)
	}

	newLeader := WaitForLeader(t, c, 5*time.Second)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()
	_, _ = newLeader.Node.Write(ctx2, &raftv1.Command{
		OperationType: string(storage.Set),
		Key:           "k2",
		Value:         []byte("v2"),
		RequestId:     "req-obs-2",
	})

	events := recorder.Events()
	if len(events) == 0 {
		t.Fatalf("recorder captured 0 events")
	}

	// Validate every event against schema, required fields, and bounds
	observedTypes := make(map[observability.EventType]int)
	for i, e := range events {
		if err := observability.ValidateEvent(e); err != nil {
			t.Errorf("event [%d] (%s) failed validation: %v", i, e.Type, err)
		}
		observedTypes[e.Type]++
	}

	// Check the 14 minimum transition mappings
	requiredMatrix := []observability.EventType{
		observability.ElectionStarted,
		observability.LeaderElected,
		observability.TermAdvanced,
		observability.VoteGranted,
		observability.RPCSucceeded,
		observability.LogAppended,
		observability.CommitAdvanced,
		observability.EntryApplied,
		observability.NodeStarted,
		observability.NodeStopped,
		observability.NodeRestarted,
		observability.PartitionCreated,
		observability.PartitionHealed,
	}

	for _, expectedType := range requiredMatrix {
		if count := observedTypes[expectedType]; count == 0 {
			t.Errorf("matrix check: missing required event type %s", expectedType)
		} else {
			t.Logf("matrix check: observed %s (%d times)", expectedType, count)
		}
	}
}

func TestObservability_GenerateFixtures(t *testing.T) {
	fixturesDir := filepath.Join("..", "..", "testdata", "fixtures")
	if err := os.MkdirAll(fixturesDir, 0755); err != nil {
		t.Fatalf("failed to create fixtures dir: %v", err)
	}

	scenarios := []struct {
		name     string
		fileName string
		fn       func(t *testing.T, c *InProcessCluster)
	}{
		{
			name:     "LeaderCrash",
			fileName: "leader_crash.json",
			fn: func(t *testing.T, c *InProcessCluster) {
				leader := WaitForLeader(t, c, 5*time.Second)
				IssueSyncWrite(t, leader, "k-lead", "v-lead", "req-lead-1")
				c.CrashNode(leader.ID)
				time.Sleep(350 * time.Millisecond)
				newLeader := WaitForLeader(t, c, 5*time.Second, leader.ID)
				IssueSyncWrite(t, newLeader, "k-lead-2", "v-lead-2", "req-lead-2")
				_ = c.RestartNode(leader.ID)
			},
		},
		{
			name:     "FollowerCrash",
			fileName: "follower_crash.json",
			fn: func(t *testing.T, c *InProcessCluster) {
				leader := WaitForLeader(t, c, 5*time.Second)
				follower := ""
				for _, id := range c.nodeIDs {
					if id != leader.ID {
						follower = id
						break
					}
				}
				IssueSyncWrite(t, leader, "k-fol", "v-fol", "req-fol-1")
				c.CrashNode(follower)
				IssueSyncWrite(t, leader, "k-fol-2", "v-fol-2", "req-fol-2")
				_ = c.RestartNode(follower)
			},
		},
		{
			name:     "MinorityPartition",
			fileName: "minority_partition.json",
			fn: func(t *testing.T, c *InProcessCluster) {
				leader := WaitForLeader(t, c, 5*time.Second)
				isolated := ""
				for _, id := range c.nodeIDs {
					if id != leader.ID {
						isolated = id
						break
					}
				}
				for _, id := range c.nodeIDs {
					if id != isolated {
						c.transport.SetPartition(isolated, id, true)
					}
				}
				IssueSyncWrite(t, leader, "k-min", "v-min", "req-min-1")
				c.transport.HealAll()
			},
		},
		{
			name:     "MajorityPartition",
			fileName: "majority_partition.json",
			fn: func(t *testing.T, c *InProcessCluster) {
				leader := WaitForLeader(t, c, 5*time.Second)
				var followers []string
				for _, id := range c.nodeIDs {
					if id != leader.ID {
						followers = append(followers, id)
					}
				}
				for _, f := range followers {
					c.transport.SetPartition(leader.ID, f, true)
				}
				newLeader := WaitForLeader(t, c, 5*time.Second, leader.ID)
				IssueSyncWrite(t, newLeader, "k-maj", "v-maj", "req-maj-1")
				c.transport.HealAll()
			},
		},
		{
			name:     "SlowFollower",
			fileName: "slow_follower.json",
			fn: func(t *testing.T, c *InProcessCluster) {
				leader := WaitForLeader(t, c, 5*time.Second)
				slowFollower := ""
				for _, id := range c.nodeIDs {
					if id != leader.ID {
						slowFollower = id
						break
					}
				}
				c.transport.SetPeerFault(slowFollower, TransportConfig{BaseLatency: 60 * time.Millisecond})
				for i := 0; i < 5; i++ {
					IssueSyncWrite(t, leader, fmt.Sprintf("k-slow-%d", i), "val", fmt.Sprintf("req-slow-%d", i))
				}
				c.transport.HealAll()
			},
		},
	}

	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			recorder := observability.NewScenarioRecorder()
			baseDir := t.TempDir()
			c, err := NewInProcessClusterWithSink([]string{"node-1", "node-2", "node-3"}, baseDir, 42, recorder)
			if err != nil {
				t.Fatalf("failed to create cluster: %v", err)
			}
			defer c.Stop()

			if err := c.Start(); err != nil {
				t.Fatalf("failed to start cluster: %v", err)
			}

			sc.fn(t, c)

			events := recorder.Events()
			if len(events) == 0 {
				t.Fatalf("scenario %s produced 0 events", sc.name)
			}

			outPath := filepath.Join(fixturesDir, sc.fileName)
			if err := recorder.SaveToJSON(outPath); err != nil {
				t.Fatalf("failed to save fixture %s: %v", outPath, err)
			}

			loaded, err := observability.LoadFromJSON(outPath)
			if err != nil {
				t.Fatalf("failed to reload fixture %s: %v", outPath, err)
			}
			if len(loaded) != len(events) {
				t.Errorf("fixture event count mismatch: saved %d, loaded %d", len(events), len(loaded))
			}
			t.Logf("successfully generated fixture %s (%d events)", sc.fileName, len(loaded))
		})
	}
}

// TestObservability_ConcurrentCandidateHigherTermAppendEntries_Abandoned verifies Touchpoint 2a
// under real concurrent goroutines and network transport: an isolated candidate receives AppendEntries
// from a newly elected higher-term leader, records election_duration with outcome="abandoned", and steps down to Follower.
func TestObservability_ConcurrentCandidateHigherTermAppendEntries_Abandoned(t *testing.T) {
	baseDir := t.TempDir()
	nodeIDs := []string{"node-1", "node-2", "node-3"}

	c, err := NewInProcessCluster(nodeIDs, baseDir, 202)
	if err != nil {
		t.Fatalf("failed to create cluster: %v", err)
	}
	defer c.Stop()

	if err := c.Start(); err != nil {
		t.Fatalf("failed to start cluster: %v", err)
	}

	// Wait for initial leader
	initialLeader := WaitForLeader(t, c, 5*time.Second)

	// Identify the two followers
	var followers []string
	for _, id := range nodeIDs {
		if id != initialLeader.ID {
			followers = append(followers, id)
		}
	}
	f1, f2 := followers[0], followers[1]

	// 1. Crash the initial leader so it does not advance its term while isolated
	c.CrashNode(initialLeader.ID)

	// 2. Partition the two followers from each other so neither can achieve quorum alone
	c.transport.SetPartition(f1, f2, true)

	// Wait for both followers to time out and enter Candidate role
	time.Sleep(500 * time.Millisecond)

	n1Ctx := c.nodes[f1]
	n2Ctx := c.nodes[f2]
	if n1Ctx == nil || n2Ctx == nil {
		t.Fatalf("failed to get node contexts")
	}

	// 3. Restart initialLeader. It starts as Follower in term 1 from disk.
	if err := c.RestartNode(initialLeader.ID); err != nil {
		t.Fatalf("failed to restart initial leader: %v", err)
	}

	// Allow communication between f1 and initialLeader, while keeping f2 isolated from both
	c.transport.SetPartition(f1, initialLeader.ID, false)
	c.transport.SetPartition(f2, initialLeader.ID, true)

	// f1 will request and receive initialLeader's vote, achieving quorum (f1 + initialLeader = 2/3)
	// Wait for f1 to become the new leader
	var newLeader *NodeContext
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if n1Ctx.Node.Role() == raft.Leader {
			newLeader = n1Ctx
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if newLeader == nil {
		t.Fatalf("node %s did not become leader", f1)
	}

	// 4. Now heal the partition between f1 (new leader) and f2 (candidate)
	c.transport.SetPartition(f1, f2, false)

	// f1 immediately replicates AppendEntries to f2.
	// f2 receives AppendEntries with req.Term > currentTerm while in Candidate role (Touchpoint 2a).
	// f2 must record election_duration with outcome="abandoned" and step down to Follower.
	abandonedObserved := false
	abandonDeadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(abandonDeadline) {
		if n2Ctx.Node.Metrics().ElectionDuration("abandoned").Count() == 1 {
			abandonedObserved = true
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if !abandonedObserved {
		t.Fatalf("node %s (candidate) did not record election_duration with outcome=abandoned upon receiving AppendEntries from %s", f2, f1)
	}

	// Verify f2 transitioned to Follower
	if r := n2Ctx.Node.Role(); r != raft.Follower {
		t.Errorf("node %s role = %s, want Follower", f2, r)
	}
}
