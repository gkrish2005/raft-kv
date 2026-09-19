package raft

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	"raftkv/internal/observability"
	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

const (
	HeartbeatInterval  = 50 * time.Millisecond
	MinElectionTimeout = 250 * time.Millisecond
	MaxElectionTimeout = 400 * time.Millisecond
	RPCTimeout         = 100 * time.Millisecond
)

type Transport interface {
	SendRequestVote(context.Context, string, *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error)
	SendAppendEntries(context.Context, string, *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error)
}
type Config struct {
	ID              string
	Peers           []string
	Clock           Clock
	Transport       Transport
	Store           storage.TermVoteStore
	LogStore        storage.LogStore
	StateMachine    *storage.KVStateMachine
	ElectionTimeout func() time.Duration
	RPCTimeout      time.Duration
	EventSink       observability.EventSink
	Metrics         *observability.MetricsRegistry
}
type Node struct {
	raftv1.UnimplementedRaftServiceServer
	mu                 sync.Mutex
	cfg                Config
	state              nodeState
	sm                 *storage.KVStateMachine
	readReadyTerm      uint64
	leaderNoOpIndex    uint64
	leaderNoOpTerm     uint64
	pendingWrites      map[uint64]*PendingWrite
	confirmedAttempt   map[string]uint64
	applierWg          sync.WaitGroup
	stopped            bool
	commitNotifyCh     chan struct{}
	applyNotifyCh      chan struct{}
	readReadyNotifyCh  chan struct{}
	readQuorumNotifyCh chan struct{}
	stop               chan struct{}
	done               chan struct{}
	resetTimer         chan time.Duration
	replicationAttempt map[string]uint64
	peerInFlight       map[string]bool
	started            bool
	recovered          bool
	emitter            *observability.EventEmitter
	metrics            *observability.MetricsRegistry
	electionStartTime  time.Time
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" || cfg.Clock == nil || cfg.Transport == nil || cfg.Store == nil {
		return nil, errors.New("raft node requires id, clock, transport, and term/vote store")
	}
	if cfg.LogStore == nil {
		cfg.LogStore = storage.NewInMemoryLogStore()
	}
	if cfg.StateMachine == nil {
		cfg.StateMachine = storage.NewKVStateMachine()
	}
	if cfg.ElectionTimeout == nil {
		cfg.ElectionTimeout = func() time.Duration {
			return MinElectionTimeout + time.Duration(rand.IntN(int(MaxElectionTimeout-MinElectionTimeout)+1))
		}
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = RPCTimeout
	}
	if cfg.EventSink == nil {
		cfg.EventSink = &observability.NopSink{}
	}
	if cfg.Metrics == nil {
		cfg.Metrics = observability.NewMetricsRegistry()
	}
	return &Node{
		cfg:                cfg,
		state:              nodeState{role: Follower},
		sm:                 cfg.StateMachine,
		metrics:            cfg.Metrics,
		replicationAttempt: make(map[string]uint64),
		peerInFlight:       make(map[string]bool),
		confirmedAttempt:   make(map[string]uint64),
		pendingWrites:      make(map[uint64]*PendingWrite),
		commitNotifyCh:     make(chan struct{}),
		applyNotifyCh:      make(chan struct{}),
		readReadyNotifyCh:  make(chan struct{}),
		readQuorumNotifyCh: make(chan struct{}),
	}, nil
}

func (n *Node) Metrics() *observability.MetricsRegistry {
	return n.metrics
}

func (n *Node) Emitter() *observability.EventEmitter {
	return n.emitter
}
// Recover executes the crash-recovery sequence per docs/architecture.md, I-005, and I-020:
// 1. Read persisted currentTerm, votedFor, bootID from TermVoteStore.
// 2. Perform independent semantic validation on votedFor against ID and Peers (defense-in-depth).
// 3. Replay WAL via LogStore.Recover() to reconstruct log[] and offset map.
// 4. Reset volatile state: Follower, commitIndex = 0, lastApplied = 0.
func (n *Node) Recover() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.stopped {
		return ErrNodeStopped
	}

	term, vote, boot, err := n.cfg.Store.Load()
	if err != nil {
		return fmt.Errorf("term/vote store recovery failed: %w", err)
	}

	// Defense-in-depth: semantic validation against cluster configuration
	if vote != "" && vote != n.cfg.ID {
		validPeer := false
		for _, p := range n.cfg.Peers {
			if p == vote {
				validPeer = true
				break
			}
		}
		if !validPeer {
			return fmt.Errorf("recovery failed: term/vote record has unknown votedFor %q", vote)
		}
	}
	n.state.currentTerm = term
	n.state.votedFor = vote
	n.state.bootID = boot

	// Replay WAL to reconstruct log[] and LogStore offset map
	if rec, ok := n.cfg.LogStore.(interface{ Recover() error }); ok {
		if err := rec.Recover(); err != nil {
			return fmt.Errorf("log recovery failed: %w", err)
		}
	}

	// Volatile state initialization: start as Follower, commitIndex = 0, lastApplied = 0 (I-005)
	n.state.role = Follower
	n.state.commitIndex = 0
	n.state.lastApplied = 0
	// Reset the LogStore's I-011 committed-truncation barrier to match the volatile state reset.
	// Without this, a FileLogStore opened across a recovery boundary could retain a stale
	// committed commitIndex from a previous run and incorrectly block legitimate truncations.
	if cs, ok := n.cfg.LogStore.(interface{ SetCommitIndex(uint64) }); ok {
		cs.SetCommitIndex(0)
	}
	n.recovered = true

	return nil
}

func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return nil
	}
	if !n.recovered {
		n.mu.Unlock()
		if err := n.Recover(); err != nil {
			n.mu.Lock()
			return err
		}
		n.mu.Lock()
	}

	n.stop = make(chan struct{})
	n.done = make(chan struct{})
	n.resetTimer = make(chan time.Duration, 1)
	n.started = true
	n.stopped = false

	// Initialize EventEmitter with durably persisted BootID (I-020).
	// Emission happens strictly after Recover() finishes durable write.
	n.emitter = observability.NewEventEmitter(n.cfg.ID, n.state.bootID, n.cfg.EventSink, n.cfg.Clock.Now)
	if n.state.bootID == 1 {
		n.emitter.Emit(observability.NodeStarted, "", "", n.state.currentTerm, n.lastLogIndexLocked(), nil)
	} else {
		n.emitter.Emit(observability.NodeRestarted, "", "", n.state.currentTerm, n.lastLogIndexLocked(), nil)
	}
	n.metrics.SetNodeUp(true)

	slog.Info("raft node started", "node_id", n.cfg.ID, "role", n.state.role, "term", n.state.currentTerm)
	go n.electionLoop()
	n.applierWg.Add(1)
	go n.applierLoop()
	return nil
}
func (n *Node) Stop() {
	n.mu.Lock()
	if !n.started || n.stopped {
		n.mu.Unlock()
		return
	}
	n.stopped = true
	n.started = false
	close(n.stop)

	for idx, pw := range n.pendingWrites {
		delete(n.pendingWrites, idx)
		pw.Done <- CommandResult{Err: ErrShutdown}
	}

	close(n.commitNotifyCh)
	close(n.applyNotifyCh)
	close(n.readReadyNotifyCh)
	close(n.readQuorumNotifyCh)

	if n.emitter != nil {
		n.emitter.Emit(observability.NodeStopped, "", "", n.state.currentTerm, n.lastLogIndexLocked(), nil)
	}
	n.metrics.SetNodeUp(false)

	done := n.done
	n.mu.Unlock()

	<-done
	n.applierWg.Wait()
}
func (n *Node) Role() Role   { n.mu.Lock(); defer n.mu.Unlock(); return n.state.role }
func (n *Node) Term() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.state.currentTerm }
func (n *Node) BootID() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.state.bootID }

// LeaderHint returns the known leader ID under the Raft mutex.
// If this node is the leader, returns this node's ID. Otherwise returns the last known leader ID (or empty if unknown).
func (n *Node) LeaderHint() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state.role == Leader {
		return n.cfg.ID
	}
	return n.state.leaderID
}

// ClusterView returns a consistent snapshot of (currentTerm, role, leaderHint) under the Raft mutex.
func (n *Node) ClusterView() (term uint64, role Role, leaderHint string) {
	n.mu.Lock()
	defer n.mu.Unlock()
	term = n.state.currentTerm
	role = n.state.role
	if role == Leader {
		leaderHint = n.cfg.ID
	} else {
		leaderHint = n.state.leaderID
	}
	return term, role, leaderHint
}

// notifyLocked signals a notification channel if the node is still running.
// If the node has been stopped (n.stopped == true), the channel was already
// permanently closed by Node.Stop() and must not be double-closed or recreated.
func (n *Node) notifyLocked(ch *chan struct{}) {
	if n.stopped {
		return
	}
	close(*ch)
	*ch = make(chan struct{})
}

// quorumSizeLocked returns the minimum number of nodes required to form a majority quorum.
func (n *Node) quorumSizeLocked() int {
	return (len(n.cfg.Peers)+1)/2 + 1
}

// StateMachine returns the node's state machine.
func (n *Node) StateMachine() *storage.KVStateMachine {
	return n.sm
}

// LogStore returns the node's underlying log store.
func (n *Node) LogStore() storage.LogStore {
	return n.cfg.LogStore
}

// CommitIndex returns the current commitIndex (thread-safe).
func (n *Node) CommitIndex() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.commitIndex
}

// LastApplied returns the current lastApplied (thread-safe).
func (n *Node) LastApplied() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.state.lastApplied
}

// ReadReadyTerm returns the current readReadyTerm (thread-safe).
func (n *Node) ReadReadyTerm() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.readReadyTerm
}
func (n *Node) electionLoop() {
	defer close(n.done)
	timer := n.cfg.Clock.NewTimer(n.cfg.ElectionTimeout())
	for {
		select {
		case <-n.stop:
			timer.Stop()
			return
		case d := <-n.resetTimer:
			timer.Reset(d)
		case <-timer.C():
			// A heartbeat/vote-grant reset can race with expiry. Prefer the
			// reset so a follower that just heard the leader does not start
			// a disrupting election.
			select {
			case d := <-n.resetTimer:
				timer.Reset(d)
			default:
				timer.Reset(n.handleElectionTimeout())
			}
		}
	}
}

// handleElectionTimeout performs one timer expiry and returns the next timer
// duration. Keeping this transition separate lets the deterministic simulator
// drive exactly the same election-loop behavior without a scheduler race.
func (n *Node) handleElectionTimeout() time.Duration {
	if n.Role() == Leader {
		n.sendHeartbeats()
		return HeartbeatInterval
	}
	n.startElection()
	if n.Role() == Leader {
		return HeartbeatInterval
	}
	return n.cfg.ElectionTimeout()
}

func (n *Node) requestElectionTimerReset() {
	n.requestTimerReset(n.cfg.ElectionTimeout())
}

func (n *Node) requestHeartbeatTimerReset() {
	n.requestTimerReset(HeartbeatInterval)
}

func (n *Node) requestTimerReset(d time.Duration) {
	select {
	case n.resetTimer <- d:
		return
	default:
	}
	select {
	case <-n.resetTimer:
	default:
	}
	select {
	case n.resetTimer <- d:
	default:
	}
}

func (n *Node) lastLogIndexLocked() uint64 {
	return n.cfg.LogStore.LastIndex()
}

func (n *Node) lastLogTermLocked() uint64 {
	lastIdx := n.lastLogIndexLocked()
	if lastIdx == 0 {
		return 0
	}
	entry, err := n.cfg.LogStore.Get(lastIdx)
	if err != nil {
		return 0
	}
	return entry.Term
}

// MatchIndex returns the matchIndex for peer (or self if peer == ID).
func (n *Node) MatchIndex(peer string) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state.matchIndex == nil {
		return 0
	}
	return n.state.matchIndex[peer]
}

// NextIndex returns the nextIndex for peer.
func (n *Node) NextIndex(peer string) uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.state.nextIndex == nil {
		return 0
	}
	return n.state.nextIndex[peer]
}

func (n *Node) appendLocalEntryLocked(cmd *raftv1.Command) (*raftv1.LogEntry, error) {
	if n.stopped {
		return nil, ErrNodeStopped
	}
	if n.state.role != Leader {
		return nil, errors.New("cannot append entry: node is not leader")
	}

	newIndex := n.lastLogIndexLocked() + 1
	entry := &raftv1.LogEntry{
		Index:   newIndex,
		Term:    n.state.currentTerm,
		Command: cmd,
	}

	// Disk-before-memory ordering (I-018)
	if err := n.cfg.LogStore.Append([]*raftv1.LogEntry{entry}); err != nil {
		n.state.role = StorageFailed
		return nil, fmt.Errorf("local log append failed: %w", err)
	}

	// matchIndex[self] == lastLogIndex (I-022)
	n.state.matchIndex[n.cfg.ID] = newIndex
	if n.emitter != nil {
		n.emitter.Emit(observability.LogAppended, "", "", n.state.currentTerm, newIndex, nil)
	}
	n.tryAdvanceCommitIndexLocked()
	return entry, nil
}

// appendLocalEntryWithPendingWrite is the single authoritative helper that appends a command
// to the leader's log and registers a PendingWrite if cmd is a client write (RequestId != "").
func (n *Node) appendLocalEntryWithPendingWrite(cmd *raftv1.Command) (*raftv1.LogEntry, *PendingWrite, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.stopped {
		return nil, nil, ErrNodeStopped
	}

	entry, err := n.appendLocalEntryLocked(cmd)
	if err != nil {
		return nil, nil, err
	}

	var pw *PendingWrite
	// Only register PendingWrite if cmd is a client write (has RequestId).
	// NOOP commands have RequestId == "" and must never be inserted into
	// the PendingWrite registry (docs/client-semantics.md).
	if cmd.RequestId != "" {
		pw = &PendingWrite{
			RequestID: cmd.RequestId,
			Index:     entry.Index,
			Term:      n.state.currentTerm,
			Done:      make(chan CommandResult, 1),
		}
		n.pendingWrites[entry.Index] = pw
	}
	return entry, pw, nil
}

// AppendLocalEntry appends a command to the leader's local log durably and updates matchIndex[self].
func (n *Node) AppendLocalEntry(cmd *raftv1.Command) (*raftv1.LogEntry, error) {
	entry, _, err := n.appendLocalEntryWithPendingWrite(cmd)
	return entry, err
}

