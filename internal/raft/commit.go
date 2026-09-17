package raft

import (
	"context"
	"errors"

	"raftkv/internal/storage"
	raftv1 "raftkv/proto/raft/v1"
)

var (
	// ErrSuperseded indicates a different command's RequestID was committed at this log index.
	ErrSuperseded = errors.New("command superseded by another entry")
	// ErrLeadershipLost indicates the node stepped down before this index committed.
	ErrLeadershipLost = errors.New("leadership lost before entry committed")
	// ErrShutdown indicates the node shut down before this entry committed.
	ErrShutdown = errors.New("node is shutting down")
	// ErrNodeStopped indicates the operation failed because the node has been stopped.
	ErrNodeStopped = errors.New("raft node is stopped")
	// ErrNotLeader indicates the node is not in the Leader role.
	ErrNotLeader = errors.New("node is not leader")
	// ErrLeaderChanged indicates leadership or term changed during the read barrier sequence.
	ErrLeaderChanged = errors.New("leader changed or term expired")
)

// CommandResult represents the result delivered to a PendingWrite waiter.
type CommandResult struct {
	Value []byte
	Err   error
}

// PendingWrite tracks a client write waiting for quorum commit and state machine application.
type PendingWrite struct {
	RequestID string
	Index     uint64
	Term      uint64              // leadership epoch
	Done      chan CommandResult  // capacity 1, non-blocking send under Raft mutex
}

// tryAdvanceCommitIndexLocked checks if any uncommitted current-term entry has reached majority.
// Must be called with n.mu held (I-006).
func (n *Node) tryAdvanceCommitIndexLocked() {
	for N := n.lastLogIndexLocked(); N > n.state.commitIndex; N-- {
		entry, err := n.cfg.LogStore.Get(N)
		if err != nil || entry.Term != n.state.currentTerm {
			continue // I-006: only current-term entries directly establish commitment
		}
		count := 1 // self
		for _, peer := range n.cfg.Peers {
			if n.state.matchIndex[peer] >= N {
				count++
			}
		}
		if count >= n.quorumSizeLocked() {
			n.state.commitIndex = N
			// Arm the LogStore's I-011 committed-truncation guard so TruncateFrom
			// rejects any attempt to truncate committed entries (leader path).
			if cs, ok := n.cfg.LogStore.(interface{ SetCommitIndex(uint64) }); ok {
				cs.SetCommitIndex(N)
			}
			n.notifyLocked(&n.commitNotifyCh)
			return
		}
	}
}

// resolvePendingWriteLocked resolves a pending write at idx after state machine application.
// Must be called with n.mu held (I-019).
func (n *Node) resolvePendingWriteLocked(idx uint64, entry *raftv1.LogEntry, result storage.CommandResult, applyErr error) {
	pw, ok := n.pendingWrites[idx]
	if !ok {
		return // follower, NOOP (no RequestID), or waiter already cancelled
	}
	delete(n.pendingWrites, idx)

	if pw.RequestID == entry.Command.RequestId {
		pw.Done <- CommandResult{Value: result.Value, Err: applyErr}
	} else {
		pw.Done <- CommandResult{Err: ErrSuperseded}
	}
}

// ReadBarrier represents the read barrier captured at confirmation time (I-016).
type ReadBarrier struct {
	Term        uint64
	CommitIndex uint64
}

// confirmLeadershipQuorum performs quorum leadership confirmation via the replication lane (I-016).
func (n *Node) confirmLeadershipQuorum(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return ErrNodeStopped
	}
	if n.state.role != Leader {
		n.mu.Unlock()
		return ErrNotLeader
	}
	term := n.state.currentTerm
	quorum := n.quorumSizeLocked()

	// (c) Self-confirmation: leader counts itself immediately as 1 vote toward quorum.
	if 1 >= quorum {
		n.mu.Unlock()
		return nil
	}

	// (a) Interaction with an already-in-flight per-follower RPC:
	targetAttempt := make(map[string]uint64, len(n.cfg.Peers))
	for _, peer := range n.cfg.Peers {
		if n.peerInFlight[peer] {
			targetAttempt[peer] = n.replicationAttempt[peer]
		} else {
			targetAttempt[peer] = n.replicationAttempt[peer] + 1
			go n.replicateToPeer(peer)
		}
	}

	// (d) Wait loop with ctx.Done() / n.stop / notify channel:
	for {
		if n.stopped {
			n.mu.Unlock()
			return ErrNodeStopped
		}
		if n.state.role != Leader || n.state.currentTerm != term {
			n.mu.Unlock()
			return ErrLeaderChanged
		}

		confirmed := 1
		for _, peer := range n.cfg.Peers {
			// (b) Intentional >= comparison: a successful response for attempt >= targetAttempt
			// proves the peer acknowledged our leadership at or after this read request was initiated.
			if n.confirmedAttempt[peer] >= targetAttempt[peer] {
				confirmed++
			}
		}
		if confirmed >= quorum {
			n.mu.Unlock()
			return nil
		}

		ch := n.readQuorumNotifyCh
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-n.stop:
			return ErrNodeStopped
		case <-ch:
		}

		n.mu.Lock()
	}
}

// ConfirmLeadershipQuorum exposes quorum confirmation for testing and client callers.
func (n *Node) ConfirmLeadershipQuorum(ctx context.Context) error {
	return n.confirmLeadershipQuorum(ctx)
}

// LinearizableGet performs a quorum-confirmed linearizable read per docs/client-semantics.md (I-016, I-023).
func (n *Node) LinearizableGet(ctx context.Context, key string) ([]byte, bool, error) {
	if err := ctx.Err(); err != nil {
		return nil, false, err
	}

	// Step 1: Client sends GET to the leader.
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, false, ErrNodeStopped
	}
	if n.state.role != Leader {
		n.mu.Unlock()
		return nil, false, ErrNotLeader
	}
	n.mu.Unlock()

	// Step 2: Quorum leadership confirmation.
	if err := n.confirmLeadershipQuorum(ctx); err != nil {
		return nil, false, err
	}

	// Step 3: readReadyTerm gate (I-023).
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return nil, false, ErrNodeStopped
		}
		if n.state.role != Leader {
			n.mu.Unlock()
			return nil, false, ErrNotLeader
		}
		if n.readReadyTerm != 0 && n.readReadyTerm == n.state.currentTerm {
			n.mu.Unlock()
			break
		}
		ch := n.readReadyNotifyCh
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-n.stop:
			return nil, false, ErrNodeStopped
		case <-ch:
		}
	}

	// Step 4: Capture barrier.
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, false, ErrNodeStopped
	}
	barrier := ReadBarrier{Term: n.state.currentTerm, CommitIndex: n.state.commitIndex}
	n.mu.Unlock()

	// Step 5: Barrier wait (OUTSIDE any lock — I-014).
	for {
		n.mu.Lock()
		if n.stopped {
			n.mu.Unlock()
			return nil, false, ErrNodeStopped
		}
		if n.state.lastApplied >= barrier.CommitIndex {
			n.mu.Unlock()
			break
		}
		if n.state.role != Leader || n.state.currentTerm != barrier.Term {
			n.mu.Unlock()
			return nil, false, ErrLeaderChanged
		}
		ch := n.applyNotifyCh
		n.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-n.stop:
			return nil, false, ErrNodeStopped
		case <-ch:
		}
	}

	// Steps 6+7: Revalidation + KV read (Raft mutex THEN StateMachine RLock — rule 34).
	n.mu.Lock()
	if n.stopped {
		n.mu.Unlock()
		return nil, false, ErrNodeStopped
	}
	n.sm.RLock() // Raft mutex acquired FIRST, then StateMachine — never reversed
	if n.state.role != Leader ||
		n.state.currentTerm != barrier.Term ||
		n.readReadyTerm != barrier.Term {
		n.sm.RUnlock()
		n.mu.Unlock()
		return nil, false, ErrLeaderChanged // fail/retry — NEVER return a value
	}
	// ═══ LINEARIZATION POINT (I-016) ═══
	value, found := n.sm.GetLocked(key) // read while holding RLock
	n.sm.RUnlock()
	n.mu.Unlock()
	return value, found, nil
}

// Get is an alias for LinearizableGet on Node.
func (n *Node) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return n.LinearizableGet(ctx, key)
}

// Write appends a command to the leader's log and waits for it to commit and apply (I-019).
func (n *Node) Write(ctx context.Context, cmd *raftv1.Command) ([]byte, error) {
	entry, pw, err := n.appendLocalEntryWithPendingWrite(cmd)
	if err != nil {
		return nil, err
	}
	if pw == nil {
		return nil, errors.New("pending write not registered")
	}

	select {
	case <-ctx.Done():
		n.mu.Lock()
		delete(n.pendingWrites, entry.Index)
		n.mu.Unlock()
		return nil, ctx.Err()
	case res := <-pw.Done:
		return res.Value, res.Err
	}
}

