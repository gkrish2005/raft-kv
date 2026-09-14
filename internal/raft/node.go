package raft

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

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
	ElectionTimeout func() time.Duration
	RPCTimeout      time.Duration
}
type Node struct {
	raftv1.UnimplementedRaftServiceServer
	mu         sync.Mutex
	cfg        Config
	state      nodeState
	stop       chan struct{}
	done       chan struct{}
	resetTimer chan time.Duration
	started    bool
}

func NewNode(cfg Config) (*Node, error) {
	if cfg.ID == "" || cfg.Clock == nil || cfg.Transport == nil || cfg.Store == nil {
		return nil, errors.New("raft node requires id, clock, transport, and term/vote store")
	}
	if cfg.ElectionTimeout == nil {
		cfg.ElectionTimeout = func() time.Duration {
			return MinElectionTimeout + time.Duration(rand.IntN(int(MaxElectionTimeout-MinElectionTimeout)+1))
		}
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = RPCTimeout
	}
	return &Node{cfg: cfg, state: nodeState{role: Follower}}, nil
}
func (n *Node) Start() error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.started {
		return nil
	}
	term, vote, boot, err := n.cfg.Store.Load()
	if err != nil {
		return err
	}
	n.state.currentTerm, n.state.votedFor, n.state.bootID = term, vote, boot
	n.stop = make(chan struct{})
	n.done = make(chan struct{})
	n.resetTimer = make(chan time.Duration, 1)
	n.started = true
	slog.Info("raft node started", "node_id", n.cfg.ID, "role", n.state.role, "term", n.state.currentTerm)
	go n.electionLoop()
	return nil
}
func (n *Node) Stop() {
	n.mu.Lock()
	if !n.started {
		n.mu.Unlock()
		return
	}
	close(n.stop)
	done := n.done
	n.started = false
	n.mu.Unlock()
	<-done
}
func (n *Node) Role() Role   { n.mu.Lock(); defer n.mu.Unlock(); return n.state.role }
func (n *Node) Term() uint64 { n.mu.Lock(); defer n.mu.Unlock(); return n.state.currentTerm }
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
