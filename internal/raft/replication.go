package raft

import (
	"context"
	"errors"
	"fmt"
	"time"

	"raftkv/internal/observability"
	raftv1 "raftkv/proto/raft/v1"
)

// sendHeartbeats triggers a round of AppendEntries replication/heartbeats to all peers.
func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state.role != Leader {
		n.mu.Unlock()
		return
	}
	peers := append([]string(nil), n.cfg.Peers...)
	n.mu.Unlock()

	for _, peer := range peers {
		go n.replicateToPeer(peer)
	}
}

// Replicate triggers replication to all peers (e.g. after a local append).
func (n *Node) Replicate() {
	n.sendHeartbeats()
}

// replicateToPeer performs at most one in-flight AppendEntries RPC to peer.
func (n *Node) replicateToPeer(peer string) {
	n.mu.Lock()
	if n.state.role != Leader {
		n.mu.Unlock()
		return
	}

	// At most one AppendEntries RPC in flight per follower (docs/architecture.md)
	if n.peerInFlight[peer] {
		n.mu.Unlock()
		return
	}

	nextIdx, ok := n.state.nextIndex[peer]
	if !ok || nextIdx == 0 {
		nextIdx = n.lastLogIndexLocked() + 1
		n.state.nextIndex[peer] = nextIdx
	}

	prevLogIndex := nextIdx - 1
	var prevLogTerm uint64 = 0
	if prevLogIndex > 0 {
		if prevEntry, err := n.cfg.LogStore.Get(prevLogIndex); err == nil {
			prevLogTerm = prevEntry.Term
		}
	}

	lastIdx := n.lastLogIndexLocked()
	var entries []*raftv1.LogEntry
	if lastIdx >= nextIdx {
		entries = make([]*raftv1.LogEntry, 0, lastIdx-nextIdx+1)
		for idx := nextIdx; idx <= lastIdx; idx++ {
			if e, err := n.cfg.LogStore.Get(idx); err == nil {
				entries = append(entries, e)
			}
		}
	}

	n.replicationAttempt[peer]++
	attempt := n.replicationAttempt[peer]
	n.peerInFlight[peer] = true

	req := &raftv1.AppendEntriesRequest{
		Term:         n.state.currentTerm,
		LeaderId:     n.cfg.ID,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: n.state.commitIndex,
	}

	correlationID := fmt.Sprintf("ae-%s-%d-%d", n.cfg.ID, req.Term, attempt)
	req.CorrelationId = correlationID

	// Release Raft mutex before performing network I/O (I-014)
	n.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
	resp, err := n.cfg.Transport.SendAppendEntries(ctx, peer, req)
	cancel()

	// Reacquire n.mu before handling response (I-014: zero network I/O under n.mu)
	n.mu.Lock()
	defer n.mu.Unlock()

	// Only clear in-flight status if this RPC belongs to the currently active
	// replication attempt for this leader term. A stale RPC from a prior term
	// or aborted attempt must never clear in-flight for an active attempt.
	if n.state.role == Leader && n.state.currentTerm == req.Term && n.replicationAttempt[peer] == attempt {
		n.peerInFlight[peer] = false
	}
	if err != nil {
		if n.emitter != nil {
			n.emitter.Emit(observability.RPCFailed, peer, correlationID, n.state.currentTerm, n.lastLogIndexLocked(), map[string]string{
				"rpc_type":    "AppendEntries",
				"peer":        peer,
				"error_class": "network_error",
			})
		}
		n.metrics.IncAppendEntriesFailures(peer)
		return
	}

	if n.emitter != nil {
		n.emitter.Emit(observability.RPCSucceeded, peer, correlationID, n.state.currentTerm, n.lastLogIndexLocked(), map[string]string{
			"rpc_type": "AppendEntries",
			"peer":     peer,
		})
	}

	n.handleAppendEntriesResponseLocked(peer, req, resp, attempt)
}

// handleAppendEntriesResponseLocked processes an AppendEntriesResponse with attempt tracking (I-021).
func (n *Node) handleAppendEntriesResponseLocked(peer string, req *raftv1.AppendEntriesRequest, resp *raftv1.AppendEntriesResponse, attempt uint64) {
	// 1. Mandatory higher-term check (I-007 touchpoint 4)
	if resp.Term > n.state.currentTerm {
		n.stepDownLocked(resp.Term)
		return
	}

	// 2. Ignore lower term
	if resp.Term < n.state.currentTerm {
		return
	}

	// 3. Must still be Leader
	if n.state.role != Leader {
		return
	}

	// 4. Stale attempt check (I-021)
	if attempt != n.replicationAttempt[peer] {
		return
	}

	// 5. Success vs. Conflict
	if resp.Success {
		match := req.PrevLogIndex + uint64(len(req.Entries))
		if match > n.state.matchIndex[peer] {
			n.state.matchIndex[peer] = match
		}
		n.state.nextIndex[peer] = n.state.matchIndex[peer] + 1

		// Phase 3 additions: confirmedAttempt, readQuorum notify, commitIndex advance
		n.confirmedAttempt[peer] = attempt
		n.notifyLocked(&n.readQuorumNotifyCh)
		n.tryAdvanceCommitIndexLocked()
	} else {
		n.handleAppendEntriesConflictLocked(peer, resp)
	}

	lastIdx := n.lastLogIndexLocked()
	matchIdx := n.state.matchIndex[peer]
	if lastIdx >= matchIdx {
		n.metrics.SetReplicationLag(peer, lastIdx-matchIdx)
	}
}

// handleAppendEntriesConflictLocked implements the exact fast-backtrack algorithm from docs/architecture.md.
func (n *Node) handleAppendEntriesConflictLocked(peer string, resp *raftv1.AppendEntriesResponse) {
	if resp.ConflictTerm == 0 {
		n.state.nextIndex[peer] = resp.ConflictIndex
		return
	}

	// Leader has ConflictTerm > 0: search leader log for the last entry with Term == ConflictTerm
	lastIndex := n.lastLogIndexLocked()
	var foundIndex uint64 = 0
	for idx := lastIndex; idx >= 1; idx-- {
		e, err := n.cfg.LogStore.Get(idx)
		if err == nil && e.Term == resp.ConflictTerm {
			foundIndex = idx
			break
		}
		if err == nil && e.Term < resp.ConflictTerm {
			break
		}
	}

	if foundIndex > 0 {
		n.state.nextIndex[peer] = foundIndex + 1
	} else {
		n.state.nextIndex[peer] = resp.ConflictIndex
	}
}

// HandleAppendEntriesResponse provides an explicit entrypoint for tests.
func (n *Node) HandleAppendEntriesResponse(peer string, req *raftv1.AppendEntriesRequest, resp *raftv1.AppendEntriesResponse, attempt uint64) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handleAppendEntriesResponseLocked(peer, req, resp, attempt)
}

// AppendEntries implements the Raft follower AppendEntries RPC receiver.
func (n *Node) AppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	if n.state.role == StorageFailed {
		return nil, errors.New("cannot process AppendEntries: node is in StorageFailed state")
	}

	// 1. Reply false if term < currentTerm
	if req.Term < n.state.currentTerm {
		return &raftv1.AppendEntriesResponse{
			Term:    n.state.currentTerm,
			Success: false,
		}, nil
	}

	// 2. If term > currentTerm, step down
	if req.Term > n.state.currentTerm {
		if !n.stepDownLocked(req.Term) {
			return nil, context.Canceled
		}
	}

	// Reset election timer and ensure follower role
	if n.state.role == Candidate && n.state.electionTerm != 0 {
		if n.metrics != nil {
			n.metrics.ObserveElectionDuration(time.Since(n.electionStartTime).Seconds(), "abandoned")
		}
		n.state.electionTerm = 0
	}
	n.state.role = Follower
	n.state.leaderID = req.LeaderId
	n.requestElectionTimerReset()

	lastIdx := n.lastLogIndexLocked()

	// 3. Log-matching check: reply false if log doesn't contain entry at prevLogIndex with prevLogTerm
	if req.PrevLogIndex > lastIdx {
		return &raftv1.AppendEntriesResponse{
			Term:          n.state.currentTerm,
			Success:       false,
			ConflictIndex: lastIdx + 1,
			ConflictTerm:  0,
		}, nil
	}

	if req.PrevLogIndex > 0 {
		prevEntry, err := n.cfg.LogStore.Get(req.PrevLogIndex)
		if err != nil || prevEntry.Term != req.PrevLogTerm {
			conflictTerm := uint64(0)
			if err == nil {
				conflictTerm = prevEntry.Term
			}

			// Find the first index in follower's log with conflictTerm
			firstConflictIndex := req.PrevLogIndex
			for idx := req.PrevLogIndex; idx >= 1; idx-- {
				e, err := n.cfg.LogStore.Get(idx)
				if err == nil && e.Term == conflictTerm {
					firstConflictIndex = idx
				} else {
					break
				}
			}

			return &raftv1.AppendEntriesResponse{
				Term:          n.state.currentTerm,
				Success:       false,
				ConflictIndex: firstConflictIndex,
				ConflictTerm:  conflictTerm,
			}, nil
		}
	}

	// 4. If an existing entry conflicts with a new one (same index, different terms),
	// truncate the existing entry and all that follow it.
	entriesToAppend := req.Entries
	for i, entry := range req.Entries {
		if entry.Index <= n.lastLogIndexLocked() {
			existing, err := n.cfg.LogStore.Get(entry.Index)
			if err == nil && existing.Term != entry.Term {
				// Conflict: truncate uncommitted suffix
				if err := n.cfg.LogStore.TruncateFrom(entry.Index); err != nil {
					n.state.role = StorageFailed
					return nil, fmt.Errorf("truncate conflicting entries failed: %w", err)
				}
				if n.emitter != nil {
					n.emitter.Emit(observability.LogConflict, req.LeaderId, req.CorrelationId, n.state.currentTerm, entry.Index, map[string]string{
						"peer":  req.LeaderId,
						"index": fmt.Sprintf("%d", entry.Index),
						"term":  fmt.Sprintf("%d", existing.Term),
					})
				}
				entriesToAppend = req.Entries[i:]
				break
			}
		} else {
			entriesToAppend = req.Entries[i:]
			break
		}
		if i == len(req.Entries)-1 {
			entriesToAppend = nil
		}
	}

	// 5. Append any new entries not already in the log (fsync completes before returning success)
	if len(entriesToAppend) > 0 {
		if err := n.cfg.LogStore.Append(entriesToAppend); err != nil {
			n.state.role = StorageFailed
			return nil, fmt.Errorf("append entries failed: %w", err)
		}
		if n.emitter != nil {
			for _, e := range entriesToAppend {
				n.emitter.Emit(observability.LogAppended, req.LeaderId, req.CorrelationId, n.state.currentTerm, e.Index, nil)
			}
		}
	}

	// 6. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry) (I-008)
	// Clamped to lastNewEntryIndex (req.PrevLogIndex + len(req.Entries)), NOT raw local log length.
	lastNewEntryIndex := req.PrevLogIndex + uint64(len(req.Entries))
	if req.LeaderCommit > n.state.commitIndex {
		oldCommit := n.state.commitIndex
		n.state.commitIndex = min(req.LeaderCommit, lastNewEntryIndex)
		// Arm the LogStore's I-011 committed-truncation guard (follower path).
		// This is the operationally critical site: TruncateFrom is called from step 4 above
		// during conflict resolution, and must see an up-to-date commitIndex barrier.
		if cs, ok := n.cfg.LogStore.(interface{ SetCommitIndex(uint64) }); ok {
			cs.SetCommitIndex(n.state.commitIndex)
		}
		if n.emitter != nil && n.state.commitIndex > oldCommit {
			n.emitter.Emit(observability.CommitAdvanced, req.LeaderId, req.CorrelationId, n.state.currentTerm, n.state.commitIndex, map[string]string{
				"old_index": fmt.Sprintf("%d", oldCommit),
				"new_index": fmt.Sprintf("%d", n.state.commitIndex),
			})
		}
		n.notifyLocked(&n.commitNotifyCh)
	}

	return &raftv1.AppendEntriesResponse{
		Term:    n.state.currentTerm,
		Success: true,
	}, nil
}
