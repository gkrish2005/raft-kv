package raft

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"raftkv/internal/observability"
	raftv1 "raftkv/proto/raft/v1"
)

func (n *Node) persistLocked(term uint64, vote string) bool {
	if n.cfg.Store.Save(term, vote, n.state.bootID) != nil {
		n.state.role = StorageFailed
		return false
	}
	n.state.currentTerm = term
	n.state.votedFor = vote
	return true
}
func (n *Node) stepDownLocked(term uint64) bool {
	if term <= n.state.currentTerm {
		return true
	}

	// Capture outgoing term BEFORE persistLocked overwrites currentTerm
	outgoingTerm := n.state.currentTerm

	// Fail all PendingWrites from outgoing leadership epoch with ErrLeadershipLost (I-019)
	for idx, pw := range n.pendingWrites {
		if pw.Term == outgoingTerm {
			delete(n.pendingWrites, idx)
			pw.Done <- CommandResult{Err: ErrLeadershipLost}
		}
	}

	// Wake all parked read waiters so they fail fast on leadership change
	n.notifyLocked(&n.readQuorumNotifyCh)
	n.notifyLocked(&n.readReadyNotifyCh)
	n.notifyLocked(&n.applyNotifyCh)

	outgoingRole := n.state.role
	outgoingElectionTerm := n.state.electionTerm

	if !n.persistLocked(term, "") {
		return false
	}
	n.state.role = Follower
	n.state.leaderID = ""
	n.state.electionTerm = 0
	n.leaderNoOpIndex = 0
	n.leaderNoOpTerm = 0
	n.readReadyTerm = 0

	if outgoingRole == Candidate && outgoingElectionTerm != 0 && n.metrics != nil {
		n.metrics.ObserveElectionDuration(time.Since(n.electionStartTime).Seconds(), "abandoned")
	}

	if n.emitter != nil {
		n.emitter.Emit(observability.TermAdvanced, "", "", term, n.lastLogIndexLocked(), map[string]string{
			"old_term": fmt.Sprintf("%d", outgoingTerm),
			"new_term": fmt.Sprintf("%d", term),
		})
		if outgoingRole == Leader {
			n.emitter.Emit(observability.LeaderSteppedDown, "", "", term, n.lastLogIndexLocked(), map[string]string{
				"reason": "higher_term",
			})
		}
	}
	return true
}
func (n *Node) startElection() {
	n.mu.Lock()
	if n.state.role == StorageFailed || n.stopped {
		n.mu.Unlock()
		return
	}
	term := n.state.currentTerm + 1
	oldTerm := n.state.currentTerm
	if !n.persistLocked(term, n.cfg.ID) {
		n.mu.Unlock()
		return
	}
	n.state.role = Candidate
	n.state.leaderID = ""
	n.state.electionTerm = term
	n.electionStartTime = time.Now()

	if n.emitter != nil {
		n.emitter.Emit(observability.TermAdvanced, "", "", term, n.lastLogIndexLocked(), map[string]string{
			"old_term": fmt.Sprintf("%d", oldTerm),
			"new_term": fmt.Sprintf("%d", term),
		})
		n.emitter.Emit(observability.ElectionStarted, "", "", term, n.lastLogIndexLocked(), nil)
	}

	lastLogIdx := n.lastLogIndexLocked()
	lastLogTerm := n.lastLogTermLocked()
	peers := append([]string(nil), n.cfg.Peers...)
	quorum := n.quorumSizeLocked()
	n.mu.Unlock()

	votes := 1
	if votes >= quorum {
		n.mu.Lock()
		if n.state.role == Candidate && n.state.electionTerm == term {
			n.becomeLeaderLocked()
			n.metrics.ObserveElectionDuration(time.Since(n.electionStartTime).Seconds(), "elected")
		}
		n.mu.Unlock()
		n.sendHeartbeats()
		n.requestHeartbeatTimerReset()
		return
	}
	for _, peer := range peers {
		correlationID := fmt.Sprintf("rv-%s-%d-%d", n.cfg.ID, term, time.Now().UnixNano())
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		resp, err := n.cfg.Transport.SendRequestVote(ctx, peer, &raftv1.RequestVoteRequest{
			Term:          term,
			CandidateId:   n.cfg.ID,
			LastLogIndex:  lastLogIdx,
			LastLogTerm:   lastLogTerm,
			CorrelationId: correlationID,
		})
		cancel()

		// Reacquire n.mu before handling response (I-014: zero network I/O under n.mu)
		n.mu.Lock()
		if n.state.role != Candidate || n.state.electionTerm != term {
			n.mu.Unlock()
			return
		}
		if err != nil {
			if n.emitter != nil {
				n.emitter.Emit(observability.RPCFailed, peer, correlationID, term, lastLogIdx, map[string]string{
					"rpc_type":    "RequestVote",
					"peer":        peer,
					"error_class": "network_error",
				})
			}
			n.mu.Unlock()
			continue
		}

		if n.emitter != nil {
			n.emitter.Emit(observability.RPCSucceeded, peer, correlationID, term, lastLogIdx, map[string]string{
				"rpc_type": "RequestVote",
				"peer":     peer,
			})
		}

		if n.handleRequestVoteResponseLocked(resp) {
			votes++
			if votes >= quorum {
				n.becomeLeaderLocked()
				n.metrics.ObserveElectionDuration(time.Since(n.electionStartTime).Seconds(), "elected")
				n.mu.Unlock()
				n.sendHeartbeats()
				n.requestHeartbeatTimerReset()
				return
			}
		} else if n.state.role != Candidate || n.state.electionTerm != term {
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
	}

	n.mu.Lock()
	if n.state.role == Candidate && n.state.electionTerm == term {
		n.metrics.ObserveElectionDuration(time.Since(n.electionStartTime).Seconds(), "lost")
	}
	n.mu.Unlock()
}

func (n *Node) becomeLeaderLocked() {
	if n.stopped {
		return
	}
	// docs/architecture.md I-023: all three conditions must hold to skip duplicate NOOP append:
	// 1. role == Leader
	// 2. leaderNoOpTerm == currentTerm
	// 3. leaderNoOpIndex != 0
	if n.state.role == Leader && n.leaderNoOpTerm == n.state.currentTerm && n.leaderNoOpIndex != 0 {
		return
	}

	n.state.role = Leader
	n.state.leaderID = ""
	n.readReadyTerm = 0
	n.leaderNoOpIndex = 0
	n.leaderNoOpTerm = 0
	n.confirmedAttempt = make(map[string]uint64)
	n.readQuorumNotifyCh = make(chan struct{})
	n.readReadyNotifyCh = make(chan struct{})

	n.state.nextIndex = make(map[string]uint64)
	n.state.matchIndex = make(map[string]uint64)
	n.replicationAttempt = make(map[string]uint64)
	n.peerInFlight = make(map[string]bool)

	lastIdx := n.lastLogIndexLocked()
	for _, peer := range n.cfg.Peers {
		n.state.nextIndex[peer] = lastIdx + 1
		n.state.matchIndex[peer] = 0
	}
	n.state.matchIndex[n.cfg.ID] = lastIdx

	slog.Info("raft leader elected", "node_id", n.cfg.ID, "term", n.state.currentTerm)

	if n.emitter != nil {
		n.emitter.Emit(observability.LeaderElected, "", "", n.state.currentTerm, lastIdx, map[string]string{
			"leader": n.cfg.ID,
		})
	}
	n.metrics.IncLeaderChanges()

	// Append exactly one NOOP entry for this leader term (I-023).
	// Calls appendLocalEntryLocked directly under n.mu.
	// Does NOT register a PendingWrite (cmd has no RequestID).
	entry, err := n.appendLocalEntryLocked(&raftv1.Command{OperationType: "NOOP"})
	if err != nil {
		return // fail-closed; leaderNoOpIndex stays 0
	}
	n.leaderNoOpIndex = entry.Index
	n.leaderNoOpTerm = n.state.currentTerm
}

// handleRequestVoteResponseLocked handles a higher term before any candidacy filter.
func (n *Node) handleRequestVoteResponseLocked(resp *raftv1.RequestVoteResponse) bool {
	if resp.Term > n.state.currentTerm {
		n.stepDownLocked(resp.Term)
		return false
	}
	return n.state.role == Candidate && n.state.electionTerm == n.state.currentTerm && resp.Term == n.state.currentTerm && resp.VoteGranted
}

func (n *Node) HandleRequestVoteResponse(resp *raftv1.RequestVoteResponse) {
	n.mu.Lock()
	n.handleRequestVoteResponseLocked(resp)
	n.mu.Unlock()
}

func (n *Node) RequestVote(ctx context.Context, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.state.currentTerm {
		return &raftv1.RequestVoteResponse{Term: n.state.currentTerm}, nil
	}
	if req.Term > n.state.currentTerm && !n.stepDownLocked(req.Term) {
		return nil, context.Canceled
	}
	grant := false
	myLastLogIndex := n.lastLogIndexLocked()
	myLastLogTerm := n.lastLogTermLocked()
	upToDate := req.LastLogTerm > myLastLogTerm || (req.LastLogTerm == myLastLogTerm && req.LastLogIndex >= myLastLogIndex)
	if n.state.role != StorageFailed && upToDate && (n.state.votedFor == "" || n.state.votedFor == req.CandidateId) {
		if n.state.votedFor != req.CandidateId && !n.persistLocked(n.state.currentTerm, req.CandidateId) {
			return nil, context.Canceled
		}
		grant = true
	}
	if grant {
		if n.emitter != nil {
			n.emitter.Emit(observability.VoteGranted, req.CandidateId, req.CorrelationId, n.state.currentTerm, myLastLogIndex, map[string]string{
				"candidate": req.CandidateId,
			})
		}
		n.requestElectionTimerReset()
	} else {
		if n.emitter != nil {
			n.emitter.Emit(observability.VoteRejected, req.CandidateId, req.CorrelationId, n.state.currentTerm, myLastLogIndex, map[string]string{
				"candidate": req.CandidateId,
			})
		}
	}
	return &raftv1.RequestVoteResponse{Term: n.state.currentTerm, VoteGranted: grant}, nil
}

