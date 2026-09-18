package chaos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"raftkv/internal/cluster"
	"raftkv/internal/raft"
	"raftkv/internal/storage"
)

// NodeContext represents a running or stopped node in the test cluster.
type NodeContext struct {
	ID        string
	Node      *raft.Node
	Store     storage.TermVoteStore
	LogStore  storage.LogStore
	SM        *storage.KVStateMachine
	DataDir   string
	Stopped   bool
	transport *FaultTransport
}

// InProcessCluster manages an in-memory cluster of Raft nodes with FaultTransport.
type InProcessCluster struct {
	mu        sync.RWMutex
	nodeIDs   []string
	nodes     map[string]*NodeContext
	transport *FaultTransport
	baseDir   string
}

// NewInProcessCluster initializes a cluster of n nodes using FileLogStore and FileTermVoteStore.
func NewInProcessCluster(nodeIDs []string, baseDir string, seed int64) (*InProcessCluster, error) {
	transport := NewFaultTransport(nil, seed)
	nodes := make(map[string]*NodeContext, len(nodeIDs))

	c := &InProcessCluster{
		nodeIDs:   nodeIDs,
		nodes:     nodes,
		transport: transport,
		baseDir:   baseDir,
	}

	for _, id := range nodeIDs {
		dataDir := filepath.Join(baseDir, id)
		if err := os.MkdirAll(dataDir, 0o755); err != nil {
			return nil, fmt.Errorf("create data dir for %s: %w", id, err)
		}

		ctx, err := c.createNodeContext(id, dataDir)
		if err != nil {
			return nil, err
		}
		nodes[id] = ctx
		transport.RegisterNode(id, ctx.Node)
	}

	return c, nil
}

func (c *InProcessCluster) createNodeContext(id, dataDir string) (*NodeContext, error) {
	var peers []string
	for _, peer := range c.nodeIDs {
		if peer != id {
			peers = append(peers, peer)
		}
	}

	allNodes := append([]string{id}, peers...)
	store := storage.NewFileTermVoteStore(filepath.Join(dataDir, "termvote"), allNodes)
	logStore, err := storage.NewFileLogStore(filepath.Join(dataDir, "wal"))
	if err != nil {
		return nil, fmt.Errorf("create file log store for %s: %w", id, err)
	}
	sm := storage.NewKVStateMachine()

	cfg := raft.Config{
		ID:           id,
		Peers:        peers,
		Clock:        raft.RealClock(),
		Transport:    c.transport,
		Store:        store,
		LogStore:     logStore,
		StateMachine: sm,
		ElectionTimeout: func() time.Duration {
			// Deterministic staggered timeouts based on node ID to avoid split votes
			idx := 0
			for i, n := range c.nodeIDs {
				if n == id {
					idx = i
					break
				}
			}
			return raft.MinElectionTimeout + time.Duration(idx*50)*time.Millisecond
		},
		RPCTimeout: raft.RPCTimeout,
	}

	node, err := raft.NewNode(cfg)
	if err != nil {
		return nil, fmt.Errorf("create raft node %s: %w", id, err)
	}

	return &NodeContext{
		ID:        id,
		Node:      node,
		Store:     store,
		LogStore:  logStore,
		SM:        sm,
		DataDir:   dataDir,
		transport: c.transport,
	}, nil
}

// Start starts all nodes in the cluster.
func (c *InProcessCluster) Start() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if err := n.Node.Start(); err != nil {
			return fmt.Errorf("start node %s: %w", n.ID, err)
		}
	}
	return nil
}

// Stop stops all nodes in the cluster.
func (c *InProcessCluster) Stop() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, n := range c.nodes {
		if !n.Stopped {
			c.transport.UnregisterNode(n.ID)
			n.Node.Stop()
			n.Stopped = true
		}
	}
}

// CrashNode stops a node, simulating a crash without clearing durable data.
func (c *InProcessCluster) CrashNode(id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok || n.Stopped {
		return
	}
	c.transport.UnregisterNode(id)
	n.Node.Stop()
	n.Stopped = true
}

// RestartNode restarts a previously crashed node against its existing data directory.
func (c *InProcessCluster) RestartNode(id string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	n, ok := c.nodes[id]
	if !ok {
		return fmt.Errorf("node %s not found", id)
	}
	if !n.Stopped {
		return nil
	}

	newCtx, err := c.createNodeContext(id, n.DataDir)
	if err != nil {
		return err
	}

	// Perform recovery sequence
	if err := newCtx.Node.Recover(); err != nil {
		return fmt.Errorf("recover node %s: %w", id, err)
	}
	if err := newCtx.Node.Start(); err != nil {
		return fmt.Errorf("start recovered node %s: %w", id, err)
	}

	c.nodes[id] = newCtx
	c.transport.RegisterNode(id, newCtx.Node)
	return nil
}

// Nodes returns a copy of all node contexts.
func (c *InProcessCluster) Nodes() []*NodeContext {
	c.mu.RLock()
	defer c.mu.RUnlock()
	list := make([]*NodeContext, 0, len(c.nodeIDs))
	for _, id := range c.nodeIDs {
		list = append(list, c.nodes[id])
	}
	return list
}

// LiveNodes returns all currently non-stopped node contexts.
func (c *InProcessCluster) LiveNodes() []*NodeContext {
	c.mu.RLock()
	defer c.mu.RUnlock()
	var list []*NodeContext
	for _, id := range c.nodeIDs {
		if n := c.nodes[id]; !n.Stopped {
			list = append(list, n)
		}
	}
	return list
}

// Transport returns the cluster's FaultTransport.
func (c *InProcessCluster) Transport() *FaultTransport {
	return c.transport
}

// FindLeader finds the leader node among live nodes (excluding any IDs in exclude).
// If multiple nodes claim Leader role (e.g. across a partition), it returns the one
// with the highest current term.
func (c *InProcessCluster) FindLeader(exclude ...string) (*NodeContext, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	excluded := make(map[string]bool, len(exclude))
	for _, id := range exclude {
		excluded[id] = true
	}

	var bestLeader *NodeContext
	var highestTerm uint64

	for _, id := range c.nodeIDs {
		if excluded[id] {
			continue
		}
		ctx := c.nodes[id]
		if !ctx.Stopped && ctx.Node.Role() == raft.Leader {
			term := ctx.Node.Term()
			if bestLeader == nil || term > highestTerm {
				bestLeader = ctx
				highestTerm = term
			}
		}
	}
	if bestLeader != nil {
		return bestLeader, nil
	}
	return nil, raft.ErrNotLeader
}

// ProcessCluster manages external raftkv-node processes (Level 4/5).
type ProcessCluster struct {
	mu         sync.Mutex
	binary     string
	repoRoot   string
	nodeIDs    []string
	addresses  map[string]string
	dataDirs   map[string]string
	configs    map[string]string
	processes  map[string]*exec.Cmd
	cancels    map[string]context.CancelFunc
	logs       map[string]*bytes.Buffer
}

// NewProcessCluster builds raftkv-node and configures a multi-process cluster.
func NewProcessCluster(repoRoot string, nodeIDs []string, baseDir string) (*ProcessCluster, error) {
	binary := filepath.Join(baseDir, "raftkv-node")
	buildCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cmd := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "./cmd/raftkv-node")
	cmd.Dir = repoRoot
	if out, err := cmd.CombinedOutput(); err != nil {
		return nil, fmt.Errorf("build raftkv-node: %w\n%s", err, out)
	}

	addresses := make(map[string]string, len(nodeIDs))
	dataDirs := make(map[string]string, len(nodeIDs))
	configs := make(map[string]string, len(nodeIDs))

	for _, id := range nodeIDs {
		lis, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			return nil, fmt.Errorf("listen for %s: %w", id, err)
		}
		addresses[id] = lis.Addr().String()
		_ = lis.Close()

		dataDirs[id] = filepath.Join(baseDir, id, "data")
		if err := os.MkdirAll(dataDirs[id], 0o755); err != nil {
			return nil, err
		}
	}

	for _, id := range nodeIDs {
		peers := make([]cluster.PeerAddr, len(nodeIDs))
		for i, pID := range nodeIDs {
			peers[i] = cluster.PeerAddr{ID: pID, Address: addresses[pID]}
		}
		data, err := json.Marshal(cluster.ClusterConfig{NodeID: id, Peers: peers})
		if err != nil {
			return nil, err
		}
		cfgPath := filepath.Join(baseDir, id, "config.json")
		if err := os.WriteFile(cfgPath, data, 0o600); err != nil {
			return nil, err
		}
		configs[id] = cfgPath
	}

	return &ProcessCluster{
		binary:    binary,
		repoRoot:  repoRoot,
		nodeIDs:   nodeIDs,
		addresses: addresses,
		dataDirs:  dataDirs,
		configs:   configs,
		processes: make(map[string]*exec.Cmd),
		cancels:   make(map[string]context.CancelFunc),
		logs:      make(map[string]*bytes.Buffer),
	}, nil
}

// Start launches all child processes.
func (p *ProcessCluster) Start() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, id := range p.nodeIDs {
		if err := p.startProcessLocked(id); err != nil {
			return err
		}
	}
	return nil
}

func (p *ProcessCluster) startProcessLocked(id string) error {
	ctx, cancel := context.WithCancel(context.Background())
	logBuf := new(bytes.Buffer)
	cmd := exec.CommandContext(ctx, p.binary,
		"-id", id,
		"-addr", p.addresses[id],
		"-config", p.configs[id],
		"-data-dir", p.dataDirs[id],
	)
	cmd.Stdout = logBuf
	cmd.Stderr = logBuf

	if err := cmd.Start(); err != nil {
		cancel()
		return fmt.Errorf("start process %s: %w", id, err)
	}

	p.processes[id] = cmd
	p.cancels[id] = cancel
	p.logs[id] = logBuf
	return nil
}

// SignalProcess sends an OS signal to a node process.
// If sig == syscall.SIGQUIT, it captures the backtrace before process exit.
func (p *ProcessCluster) SignalProcess(id string, sig syscall.Signal) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cmd, ok := p.processes[id]
	if !ok || cmd == nil || cmd.Process == nil {
		return fmt.Errorf("process %s not running", id)
	}
	return cmd.Process.Signal(sig)
}

// KillProcess kills a node with SIGKILL.
func (p *ProcessCluster) KillProcess(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	cmd, ok := p.processes[id]
	if !ok || cmd == nil || cmd.Process == nil {
		return nil
	}
	_ = cmd.Process.Signal(syscall.SIGKILL)
	_ = cmd.Wait()
	delete(p.processes, id)
	return nil
}

// RestartProcess restarts a previously killed node process against its data directory.
func (p *ProcessCluster) RestartProcess(id string) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cmd, ok := p.processes[id]; ok && cmd != nil && cmd.Process != nil {
		return nil
	}
	return p.startProcessLocked(id)
}

// Stop terminates all processes cleanly.
func (p *ProcessCluster) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for id, cmd := range p.processes {
		if cmd != nil && cmd.Process != nil {
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
		}
		delete(p.processes, id)
	}
}

// Logs returns the stdout/stderr logs for a given node.
func (p *ProcessCluster) Logs(id string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if buf, ok := p.logs[id]; ok {
		return buf.String()
	}
	return ""
}

// Address returns the listening address of a node.
func (p *ProcessCluster) Address(id string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addresses[id]
}

// DataDir returns the data directory of a node.
func (p *ProcessCluster) DataDir(id string) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.dataDirs[id]
}

// NodeIDs returns the configured node IDs.
func (p *ProcessCluster) NodeIDs() []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	res := make([]string, len(p.nodeIDs))
	copy(res, p.nodeIDs)
	return res
}

// WaitListening polls until the node's TCP port is accepting connections or timeout expires.
func (p *ProcessCluster) WaitListening(id string, timeout time.Duration) error {
	p.mu.Lock()
	addr := p.addresses[id]
	p.mu.Unlock()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(25 * time.Millisecond)
	}
	return fmt.Errorf("node %s at %s did not become ready within %v", id, addr, timeout)
}

// WaitForProcessExit waits for a process to exit on its own (e.g. startup failure).
func (p *ProcessCluster) WaitForProcessExit(id string, timeout time.Duration) (int, error) {
	p.mu.Lock()
	cmd, ok := p.processes[id]
	p.mu.Unlock()
	if !ok || cmd == nil || cmd.Process == nil {
		return 0, fmt.Errorf("process %s not found", id)
	}

	done := make(chan error, 1)
	go func() {
		done <- cmd.Wait()
	}()

	select {
	case err := <-done:
		p.mu.Lock()
		delete(p.processes, id)
		p.mu.Unlock()
		if err != nil {
			if exitErr, ok := err.(*exec.ExitError); ok {
				return exitErr.ExitCode(), nil
			}
			return -1, err
		}
		return 0, nil
	case <-time.After(timeout):
		return -1, fmt.Errorf("process %s did not exit within %v", id, timeout)
	}
}
