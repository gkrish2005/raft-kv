package raft

import (
	"context"
	"log/slog"

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
	if !n.persistLocked(term, "") {
		return false
	}
	n.state.role = Follower
	n.state.electionTerm = 0
	return true
}
func (n *Node) startElection() {
	n.mu.Lock()
	if n.state.role == StorageFailed {
		n.mu.Unlock()
		return
	}
	term := n.state.currentTerm + 1
	if !n.persistLocked(term, n.cfg.ID) {
		n.mu.Unlock()
		return
	}
	n.state.role = Candidate
	n.state.electionTerm = term
	peers := append([]string(nil), n.cfg.Peers...)
	n.mu.Unlock()
	votes := 1
	quorum := (len(peers)+1)/2 + 1
	if votes >= quorum {
		n.mu.Lock()
		if n.state.role == Candidate && n.state.electionTerm == term {
			n.becomeLeaderLocked()
		}
		n.mu.Unlock()
		n.sendHeartbeats()
		n.requestHeartbeatTimerReset()
		return
	}
	for _, peer := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		resp, err := n.cfg.Transport.SendRequestVote(ctx, peer, &raftv1.RequestVoteRequest{Term: term, CandidateId: n.cfg.ID})
		cancel()
		if err != nil {
			continue
		}
		n.mu.Lock()
		if n.handleRequestVoteResponseLocked(resp) {
			votes++
			if votes >= quorum {
				n.becomeLeaderLocked()
				n.mu.Unlock()
				n.sendHeartbeats()
				n.requestHeartbeatTimerReset()
				return
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) becomeLeaderLocked() {
	n.state.role = Leader
	slog.Info("raft leader elected", "node_id", n.cfg.ID, "term", n.state.currentTerm)
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
func (n *Node) sendHeartbeats() {
	n.mu.Lock()
	if n.state.role != Leader {
		n.mu.Unlock()
		return
	}
	term := n.state.currentTerm
	peers := append([]string(nil), n.cfg.Peers...)
	n.mu.Unlock()
	for _, peer := range peers {
		ctx, cancel := context.WithTimeout(context.Background(), n.cfg.RPCTimeout)
		resp, err := n.cfg.Transport.SendAppendEntries(ctx, peer, &raftv1.AppendEntriesRequest{Term: term, LeaderId: n.cfg.ID})
		cancel()
		if err == nil {
			n.mu.Lock()
			n.handleAppendEntriesResponseLocked(resp)
			n.mu.Unlock()
		}
	}
}

func (n *Node) handleAppendEntriesResponseLocked(resp *raftv1.AppendEntriesResponse) {
	if resp.Term > n.state.currentTerm {
		n.stepDownLocked(resp.Term)
	}
}

func (n *Node) HandleAppendEntriesResponse(resp *raftv1.AppendEntriesResponse) {
	n.mu.Lock()
	n.handleAppendEntriesResponseLocked(resp)
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
	upToDate := req.LastLogTerm > n.state.lastLogTerm || (req.LastLogTerm == n.state.lastLogTerm && req.LastLogIndex >= n.state.lastLogIndex)
	if n.state.role != StorageFailed && upToDate && (n.state.votedFor == "" || n.state.votedFor == req.CandidateId) {
		if n.state.votedFor != req.CandidateId && !n.persistLocked(n.state.currentTerm, req.CandidateId) {
			return nil, context.Canceled
		}
		grant = true
	}
	if grant {
		n.requestElectionTimerReset()
	}
	return &raftv1.RequestVoteResponse{Term: n.state.currentTerm, VoteGranted: grant}, nil
}
func (n *Node) AppendEntries(ctx context.Context, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if req.Term < n.state.currentTerm {
		return &raftv1.AppendEntriesResponse{Term: n.state.currentTerm}, nil
	}
	if req.Term > n.state.currentTerm && !n.stepDownLocked(req.Term) {
		return nil, context.Canceled
	}
	n.state.role = Follower
	n.requestElectionTimerReset()
	return &raftv1.AppendEntriesResponse{Term: n.state.currentTerm, Success: true}, nil
}
