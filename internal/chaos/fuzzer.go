package chaos

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	raftv1 "raftkv/proto/raft/v1"
)

// FuzzerConfig specifies execution parameters for the randomized chaos fuzzer.
type FuzzerConfig struct {
	Seed        int64
	Duration    time.Duration
	RequestRate int // requests per second during active cycles
	NodeCount   int
	BaseDir     string
}

// FuzzerStats captures telemetry over a fuzzer execution run.
type FuzzerStats struct {
	TotalWritesAttempted int
	TotalWritesConfirmed int
	CyclesCompleted      int
	FaultsInjected       int
	Seed                 int64
}

// Fuzzer coordinates randomized fault injection and concurrent client traffic.
type Fuzzer struct {
	cfg     FuzzerConfig
	cluster *InProcessCluster
	rng     *rand.Rand
	stats   FuzzerStats
	mu      sync.Mutex

	// Quorum tracking
	crashedNodes map[string]bool
	partitions   map[string]map[string]bool
}

// NewFuzzer creates a new seedable chaos fuzzer.
func NewFuzzer(cfg FuzzerConfig) (*Fuzzer, error) {
	if cfg.NodeCount < 3 {
		cfg.NodeCount = 3
	}
	if cfg.RequestRate <= 0 {
		cfg.RequestRate = 20
	}
	if cfg.Seed == 0 {
		cfg.Seed = time.Now().UnixNano()
	}

	nodeIDs := make([]string, cfg.NodeCount)
	for i := 0; i < cfg.NodeCount; i++ {
		nodeIDs[i] = fmt.Sprintf("node-%d", i+1)
	}

	cluster, err := NewInProcessCluster(nodeIDs, cfg.BaseDir, cfg.Seed)
	if err != nil {
		return nil, err
	}

	return &Fuzzer{
		cfg:          cfg,
		cluster:      cluster,
		rng:          rand.New(rand.NewSource(cfg.Seed)),
		crashedNodes: make(map[string]bool),
		partitions:   make(map[string]map[string]bool),
		stats: FuzzerStats{
			Seed: cfg.Seed,
		},
	}, nil
}

// Run executes the fuzzer for the configured duration with alternating active and quiescent cycles.
func (f *Fuzzer) Run(t TestingT) FuzzerStats {
	if err := f.cluster.Start(); err != nil {
		t.Fatalf("start cluster failed: %v", err)
	}
	defer f.cluster.Stop()

	// Initial election wait
	_ = WaitForLeader(t, f.cluster, 4*time.Second)

	startTime := time.Now()
	deadline := startTime.Add(f.cfg.Duration)

	cycleIndex := 0
	for time.Now().Before(deadline) {
		cycleIndex++
		// Determine remaining duration for this cycle
		remaining := time.Until(deadline)
		cycleDuration := 30 * time.Second
		if remaining < cycleDuration {
			cycleDuration = remaining
		}

		// 1. ACTIVE CHAOS CYCLE with continuous client traffic
		f.runActiveCycle(t, cycleDuration)

		// 2. PERIODIC QUIESCENT INTERVAL
		// Pause traffic, clear faults, restart nodes, and poll quiescence barrier
		f.runPeriodicQuiescence(t)
	}

	// 3. FINAL HEAL & 5-POINT CONVERGENCE CHECK
	f.cluster.Transport().HealAll()
	for id, crashed := range f.crashedNodes {
		if crashed {
			_ = f.cluster.RestartNode(id)
			f.crashedNodes[id] = false
		}
	}

	t.Logf("fuzzer soak completed %d cycles with seed %d. Running final convergence check...",
		cycleIndex, f.cfg.Seed)

	// Issue final barrier write to ensure clean leader commit across cluster
	leader := WaitForLeader(t, f.cluster, 4*time.Second)
	IssueSyncWrite(t, leader, "final-soak-key", "final-val", fmt.Sprintf("req-final-%d", f.cfg.Seed))

	AssertConvergence(t, f.cluster, 10*time.Second)

	f.stats.CyclesCompleted = cycleIndex
	return f.stats
}

func (f *Fuzzer) runActiveCycle(t TestingT, duration time.Duration) {
	cycleEnd := time.Now().Add(duration)
	writeTicker := time.NewTicker(time.Second / time.Duration(f.cfg.RequestRate))
	defer writeTicker.Stop()

	faultTicker := time.NewTicker(2 * time.Second)
	defer faultTicker.Stop()

	reqCounter := 0

	for time.Now().Before(cycleEnd) {
		select {
		case <-writeTicker.C:
			reqCounter++
			f.stats.TotalWritesAttempted++
			key := fmt.Sprintf("fuzz-k-%d", reqCounter%100)
			val := fmt.Sprintf("val-%d", reqCounter)
			reqID := fmt.Sprintf("req-fuzz-%d-%d", f.cfg.Seed, reqCounter)

			// Best-effort write to current leader
			leader, err := f.cluster.FindLeader()
			if err == nil && leader != nil && !leader.Stopped {
				cmd := &raftv1.Command{
					OperationType: "SET",
					Key:           key,
					Value:         []byte(val),
					RequestId:     reqID,
				}
				if _, appendErr := leader.Node.AppendLocalEntry(cmd); appendErr == nil {
					leader.Node.Replicate()
					f.stats.TotalWritesConfirmed++
				}
			}

		case <-faultTicker.C:
			f.injectRandomCalibratedFault(t)
		}
	}
}

// injectRandomCalibratedFault selects a fault from the 5 discrete calibrated regimes
// while strictly enforcing the Quorum-Safety Invariant (§2.2).
func (f *Fuzzer) injectRandomCalibratedFault(_ TestingT) {
	f.mu.Lock()
	defer f.mu.Unlock()

	nodes := f.cluster.nodeIDs
	quorum := len(nodes)/2 + 1

	regime := f.rng.Intn(5) // 0 to 4
	switch regime {
	case 0:
		// Regime 1: CleanSlow (latency 50-75ms, jitter <= 4ms, max <= 79ms < 80ms)
		target := nodes[f.rng.Intn(len(nodes))]
		latency := time.Duration(50+f.rng.Intn(26)) * time.Millisecond
		f.cluster.Transport().SetPeerFault(target, TransportConfig{
			BaseLatency: latency,
			Jitter:      time.Duration(f.rng.Intn(5)) * time.Millisecond,
		})
		f.stats.FaultsInjected++

	case 1:
		// Regime 2: TimeoutSlow (latency 130-160ms, jitter <= 5ms, strictly [125-165ms])
		target := nodes[f.rng.Intn(len(nodes))]
		latency := time.Duration(130+f.rng.Intn(31)) * time.Millisecond
		f.cluster.Transport().SetPeerFault(target, TransportConfig{
			BaseLatency: latency,
			Jitter:      time.Duration(f.rng.Intn(6)) * time.Millisecond,
		})
		f.stats.FaultsInjected++

	case 2:
		// Regime 3: FullPartition (with Quorum-Safety Guard)
		// Can only partition if live unpartitioned nodes remain >= quorum
		target := nodes[f.rng.Intn(len(nodes))]
		if f.crashedNodes[target] {
			return
		}

		// Count currently unpartitioned live nodes
		liveHealthy := 0
		for _, id := range nodes {
			if !f.crashedNodes[id] && !f.isNodePartitionedLocked(id) {
				liveHealthy++
			}
		}

		// Must leave at least quorum nodes healthy
		if liveHealthy <= quorum {
			return // Quorum safety: do not isolate
		}

		// Partition target from all peers for 1.5s
		for _, peer := range nodes {
			if peer != target {
				f.cluster.Transport().SetPartition(target, peer, true)
			}
		}
		f.setPartitionTrackLocked(target, true)
		f.stats.FaultsInjected++

		// Automatically auto-heal after 1.5s
		go func(nodeToHeal string) {
			time.Sleep(1500 * time.Millisecond)
			f.mu.Lock()
			defer f.mu.Unlock()
			for _, peer := range nodes {
				if peer != nodeToHeal {
					f.cluster.Transport().SetPartition(nodeToHeal, peer, false)
				}
			}
			f.setPartitionTrackLocked(nodeToHeal, false)
		}(target)

	case 3:
		// Regime 4: LightDrop (packet drop rate 5% to 15%)
		target := nodes[f.rng.Intn(len(nodes))]
		dropRate := 0.05 + f.rng.Float64()*0.10
		f.cluster.Transport().SetPeerFault(target, TransportConfig{
			DropRate: dropRate,
		})
		f.stats.FaultsInjected++

	case 4:
		// Regime 5: NodeChurn (Simulated In-Process Crash & Restart via Node.Stop/Recover with Quorum-Safety Guard)
		target := nodes[f.rng.Intn(len(nodes))]
		if f.crashedNodes[target] {
			// If already crashed, restart it
			if err := f.cluster.RestartNode(target); err == nil {
				f.crashedNodes[target] = false
			}
			return
		}

		// Check quorum safety: cannot crash if live unpartitioned nodes <= quorum
		liveHealthy := 0
		for _, id := range nodes {
			if !f.crashedNodes[id] && !f.isNodePartitionedLocked(id) {
				liveHealthy++
			}
		}
		if liveHealthy <= quorum {
			return // Quorum safety: do not crash
		}

		f.cluster.CrashNode(target)
		f.crashedNodes[target] = true
		f.stats.FaultsInjected++

		// Automatically restart after 1.0s
		go func(crashedID string) {
			time.Sleep(1000 * time.Millisecond)
			f.mu.Lock()
			defer f.mu.Unlock()
			if f.crashedNodes[crashedID] {
				_ = f.cluster.RestartNode(crashedID)
				f.crashedNodes[crashedID] = false
			}
		}(target)
	}
}

func (f *Fuzzer) isNodePartitionedLocked(id string) bool {
	if f.partitions[id] == nil {
		return false
	}
	for _, blocked := range f.partitions[id] {
		if blocked {
			return true
		}
	}
	return false
}

func (f *Fuzzer) setPartitionTrackLocked(id string, blocked bool) {
	if f.partitions[id] == nil {
		f.partitions[id] = make(map[string]bool)
	}
	for _, peer := range f.cluster.nodeIDs {
		if peer != id {
			f.partitions[id][peer] = blocked
			if f.partitions[peer] == nil {
				f.partitions[peer] = make(map[string]bool)
			}
			f.partitions[peer][id] = blocked
		}
	}
}

// runPeriodicQuiescence pauses writes, heals faults, restarts nodes, and polls the quiescence barrier.
func (f *Fuzzer) runPeriodicQuiescence(t TestingT) {
	f.mu.Lock()
	f.cluster.Transport().HealAll()
	for id, crashed := range f.crashedNodes {
		if crashed {
			_ = f.cluster.RestartNode(id)
			f.crashedNodes[id] = false
		}
	}
	f.partitions = make(map[string]map[string]bool)
	f.mu.Unlock()

	// Dynamic barrier timeout: Timeout_election (1.2s) + FsyncGrace (1.5s)
	// Under healthy conditions, live nodes clear this barrier within tens of milliseconds
	timeout := 4 * time.Second
	leader := WaitForLeader(t, f.cluster, timeout)

	// Issue synchronous barrier write to flush pending in-flight writes from active cycle
	barrierReqID := fmt.Sprintf("req-quiesce-barrier-%d-%d", f.cfg.Seed, time.Now().UnixNano())
	IssueSyncWrite(t, leader, "quiesce-barrier", "val", barrierReqID)

	deadline := time.Now().Add(timeout)
	for {
		targetCommit := leader.Node.CommitIndex()
		allCaughtUp := true

		for _, n := range f.cluster.LiveNodes() {
			if n.Node.CommitIndex() != targetCommit || n.Node.LastApplied() != targetCommit {
				allCaughtUp = false
				break
			}
		}

		if allCaughtUp {
			return
		}

		if time.Now().After(deadline) {
			t.Fatalf("periodic quiescence barrier failed: nodes did not catch up within %v", timeout)
		}
		time.Sleep(15 * time.Millisecond)
	}
}
