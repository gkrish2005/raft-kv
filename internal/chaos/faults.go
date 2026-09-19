package chaos

import (
	"context"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"raftkv/internal/observability"
	"raftkv/internal/raft"
	raftv1 "raftkv/proto/raft/v1"
)

// FaultMode describes the type of transport fault applied to an RPC.
type FaultMode string

const (
	FaultCleanSlow   FaultMode = "CleanSlow"
	FaultTimeoutSlow FaultMode = "TimeoutSlow"
	FaultPartition   FaultMode = "Partition"
	FaultLightDrop   FaultMode = "LightDrop"
)

// TransportConfig holds parameters for fault injection on a connection or globally.
type TransportConfig struct {
	BaseLatency time.Duration
	Jitter      time.Duration
	DropRate    float64 // 0.0 to 1.0
}

// FaultTransport wraps an underlying raft.Transport or in-memory routing map to inject
// partitions, latency, jitter, and drop probability in a thread-safe, dynamically-configurable manner.
type FaultTransport struct {
	mu            sync.RWMutex
	underlying    raft.Transport
	nodes         map[string]*raft.Node
	partitions    map[string]map[string]bool // from -> to -> blocked
	peerConfigs   map[string]TransportConfig // peerID -> config
	globalConfig  TransportConfig
	rng           *rand.Rand
	onSendHook    func(ctx context.Context, from, to, rpcType string)
	delayedCtx    context.Context
	delayedCancel context.CancelFunc
	sink          observability.EventSink
}

func (t *FaultTransport) SetEventSink(sink observability.EventSink) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.sink = sink
}

// NewFaultTransport creates a new FaultTransport.
// If underlying is nil, it can route directly to registered *raft.Node instances.
func NewFaultTransport(underlying raft.Transport, seed int64) *FaultTransport {
	src := rand.NewSource(seed)
	if seed == 0 {
		src = rand.NewSource(time.Now().UnixNano())
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &FaultTransport{
		underlying:    underlying,
		nodes:         make(map[string]*raft.Node),
		partitions:    make(map[string]map[string]bool),
		peerConfigs:   make(map[string]TransportConfig),
		rng:           rand.New(src),
		delayedCtx:    ctx,
		delayedCancel: cancel,
	}
}

func (t *FaultTransport) cancelPendingDelaysLocked() {
	if t.delayedCancel != nil {
		t.delayedCancel()
	}
	t.delayedCtx, t.delayedCancel = context.WithCancel(context.Background())
}

func (t *FaultTransport) getDelayedCtx() context.Context {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.delayedCtx
}

// RegisterNode registers a local in-memory raft.Node by ID for direct dispatch when underlying is nil.
func (t *FaultTransport) RegisterNode(id string, n *raft.Node) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.nodes[id] = n
}

// UnregisterNode removes a node by ID from direct dispatch (e.g. on node crash).
func (t *FaultTransport) UnregisterNode(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.nodes, id)
}

// SetOnSendHook sets an optional callback executed on every RPC dispatch before network delivery.
func (t *FaultTransport) SetOnSendHook(hook func(ctx context.Context, from, to, rpcType string)) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.onSendHook = hook
}

// SetPartition sets bidirectional network partition state between nodeA and nodeB.
func (t *FaultTransport) SetPartition(nodeA, nodeB string, blocked bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partitions[nodeA] == nil {
		t.partitions[nodeA] = make(map[string]bool)
	}
	if t.partitions[nodeB] == nil {
		t.partitions[nodeB] = make(map[string]bool)
	}
	t.partitions[nodeA][nodeB] = blocked
	t.partitions[nodeB][nodeA] = blocked
	if blocked {
		t.cancelPendingDelaysLocked()
	}

	peersList := []string{nodeA, nodeB}
	sort.Strings(peersList)
	peersStr := strings.Join(peersList, ",")

	if t.sink != nil {
		eventType := observability.PartitionCreated
		if !blocked {
			eventType = observability.PartitionHealed
		}
		t.sink.Emit(observability.ClusterEvent{
			SchemaVersion: observability.ClusterEventSchemaVersion,
			EventID:       fmt.Sprintf("chaos/partition/%s-%d", peersStr, time.Now().UnixNano()),
			Timestamp:     time.Now(),
			NodeID:        nodeA,
			PeerID:        nodeB,
			Type:          eventType,
			Fields:        map[string]string{"peers": peersStr},
		})
	}
}

// SetUnidirectionalPartition sets partition from nodeA to nodeB (asymmetric).
func (t *FaultTransport) SetUnidirectionalPartition(from, to string, blocked bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.partitions[from] == nil {
		t.partitions[from] = make(map[string]bool)
	}
	t.partitions[from][to] = blocked
	if blocked {
		t.cancelPendingDelaysLocked()
	}
}

// IsPartitioned checks if communication from 'from' to 'to' is blocked.
func (t *FaultTransport) IsPartitioned(from, to string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.partitions[from] == nil {
		return false
	}
	return t.partitions[from][to]
}

// SetPeerFault sets latency, jitter, and drop rate for traffic targeting targetPeer.
func (t *FaultTransport) SetPeerFault(targetPeer string, cfg TransportConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.peerConfigs[targetPeer] = cfg
}

// ClearPeerFault clears fault configuration for targetPeer.
func (t *FaultTransport) ClearPeerFault(targetPeer string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.peerConfigs, targetPeer)
}

// SetGlobalFault sets default latency, jitter, and drop rate for all traffic.
func (t *FaultTransport) SetGlobalFault(cfg TransportConfig) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.globalConfig = cfg
}

// HealAll resets all partitions, peer faults, and global faults, and cancels pending delayed deliveries.
func (t *FaultTransport) HealAll() {
	t.mu.Lock()
	defer t.mu.Unlock()

	var allPeersMap = make(map[string]bool)
	for a, targets := range t.partitions {
		for b, isPart := range targets {
			if isPart {
				allPeersMap[a] = true
				allPeersMap[b] = true
			}
		}
	}

	t.partitions = make(map[string]map[string]bool)
	t.peerConfigs = make(map[string]TransportConfig)
	t.globalConfig = TransportConfig{}
	t.cancelPendingDelaysLocked()

	if len(allPeersMap) > 0 && t.sink != nil {
		var pList []string
		for p := range allPeersMap {
			pList = append(pList, p)
		}
		sort.Strings(pList)
		peersStr := strings.Join(pList, ",")
		t.sink.Emit(observability.ClusterEvent{
			SchemaVersion: observability.ClusterEventSchemaVersion,
			EventID:       fmt.Sprintf("chaos/heal-all/%d", time.Now().UnixNano()),
			Timestamp:     time.Now(),
			NodeID:        pList[0],
			Type:          observability.PartitionHealed,
			Fields:        map[string]string{"peers": peersStr},
		})
	}
}

func (t *FaultTransport) evaluateFault(from, to string) (blocked bool, delay time.Duration, dropped bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.partitions[from] != nil && t.partitions[from][to] {
		return true, 0, true
	}

	cfg := t.globalConfig
	if pCfg, ok := t.peerConfigs[to]; ok {
		cfg = pCfg
	}

	if cfg.DropRate > 0 && t.rng.Float64() < cfg.DropRate {
		return false, 0, true
	}

	if cfg.BaseLatency > 0 {
		delay = cfg.BaseLatency
		if cfg.Jitter > 0 {
			jitterDelta := time.Duration(t.rng.Int63n(int64(cfg.Jitter)*2) - int64(cfg.Jitter))
			delay += jitterDelta
			if delay < 0 {
				delay = 0
			}
		}
	}

	return false, delay, false
}

func (t *FaultTransport) deliverRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	if t.underlying != nil {
		return t.underlying.SendRequestVote(ctx, peer, req)
	}
	t.mu.RLock()
	target, ok := t.nodes[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, errors.New("peer not found")
	}
	return target.RequestVote(ctx, req)
}

func (t *FaultTransport) deliverAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	if t.underlying != nil {
		return t.underlying.SendAppendEntries(ctx, peer, req)
	}
	t.mu.RLock()
	target, ok := t.nodes[peer]
	t.mu.RUnlock()
	if !ok {
		return nil, errors.New("peer not found")
	}
	return target.AppendEntries(ctx, req)
}

// SendRequestVote delivers RequestVote with injected faults.
func (t *FaultTransport) SendRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	from := req.CandidateId
	t.mu.RLock()
	hook := t.onSendHook
	t.mu.RUnlock()
	if hook != nil {
		hook(ctx, from, peer, "RequestVote")
	}

	blocked, delay, dropped := t.evaluateFault(from, peer)
	// Probabilistic drops (LightDrop) and total partitions (FullPartition) are never delivered
	if blocked || dropped {
		return nil, context.DeadlineExceeded
	}

	// Finite nonzero delay handling (CleanSlow / TimeoutSlow)
	if delay > 0 {
		start := time.Now()
		timer := time.NewTimer(delay)

		select {
		case <-timer.C:
			timer.Stop()
			return t.deliverRequestVote(ctx, peer, req)
		case <-ctx.Done():
			timer.Stop()
			// Sender timed out (e.g. RPCTimeout=100ms < delay=140ms).
			// In a real network, the packet continues on the wire and reaches the receiver.
			// Deliver asynchronously after remaining delay if not cancelled or partitioned.
			remaining := delay - time.Since(start)
			if remaining < 0 {
				remaining = 0
			}
			delayedCtx := t.getDelayedCtx()
			go func(rem time.Duration) {
				remTimer := time.NewTimer(rem)
				defer remTimer.Stop()
				select {
				case <-remTimer.C:
					if !t.IsPartitioned(from, peer) {
						recvCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
						defer cancel()
						_, _ = t.deliverRequestVote(recvCtx, peer, req)
					}
				case <-delayedCtx.Done():
					return // Cancelled by HealAll() or new partition: drop cleanly, no goroutine leak
				}
			}(remaining)
			return nil, ctx.Err()
		}
	}

	return t.deliverRequestVote(ctx, peer, req)
}

// SendAppendEntries delivers AppendEntries with injected faults.
func (t *FaultTransport) SendAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	from := req.LeaderId
	t.mu.RLock()
	hook := t.onSendHook
	t.mu.RUnlock()
	if hook != nil {
		hook(ctx, from, peer, "AppendEntries")
	}

	blocked, delay, dropped := t.evaluateFault(from, peer)
	// Probabilistic drops (LightDrop) and total partitions (FullPartition) are never delivered
	if blocked || dropped {
		return nil, context.DeadlineExceeded
	}

	// Finite nonzero delay handling (CleanSlow / TimeoutSlow)
	if delay > 0 {
		start := time.Now()
		timer := time.NewTimer(delay)

		select {
		case <-timer.C:
			timer.Stop()
			return t.deliverAppendEntries(ctx, peer, req)
		case <-ctx.Done():
			timer.Stop()
			// Sender timed out (e.g. RPCTimeout=100ms < delay=140ms).
			// Packet is in-flight on the wire. Deliver asynchronously to receiver after remaining delay.
			remaining := delay - time.Since(start)
			if remaining < 0 {
				remaining = 0
			}
			delayedCtx := t.getDelayedCtx()
			go func(rem time.Duration) {
				remTimer := time.NewTimer(rem)
				defer remTimer.Stop()
				select {
				case <-remTimer.C:
					if !t.IsPartitioned(from, peer) {
						recvCtx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
						defer cancel()
						_, _ = t.deliverAppendEntries(recvCtx, peer, req)
					}
				case <-delayedCtx.Done():
					return // Cancelled by HealAll() or new partition: drop cleanly, no goroutine leak
				}
			}(remaining)
			return nil, ctx.Err()
		}
	}

	return t.deliverAppendEntries(ctx, peer, req)
}
