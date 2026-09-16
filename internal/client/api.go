package client

import (
	"context"

	"raftkv/internal/raft"
	raftv1 "raftkv/proto/raft/v1"
)

var (
	// ErrNotLeader indicates the target node is not the leader.
	ErrNotLeader = raft.ErrNotLeader
	// ErrLeaderChanged indicates leadership or term changed during operation.
	ErrLeaderChanged = raft.ErrLeaderChanged
	// ErrNodeStopped indicates the node has stopped.
	ErrNodeStopped = raft.ErrNodeStopped
	// ErrSuperseded indicates a write was superseded by a different command.
	ErrSuperseded = raft.ErrSuperseded
	// ErrLeadershipLost indicates the node stepped down before the write committed.
	ErrLeadershipLost = raft.ErrLeadershipLost
	// ErrShutdown indicates the node shut down before write completion.
	ErrShutdown = raft.ErrShutdown
)

// ReadBarrier represents the read barrier captured at confirmation time.
type ReadBarrier = raft.ReadBarrier

// Client provides a client-facing API for issuing linearizable reads and writes
// to a local Raft node.
type Client struct {
	node *raft.Node
}

// New creates a new Client for the given Raft node.
func New(node *raft.Node) *Client {
	return &Client{node: node}
}

// Node returns the underlying Raft node.
func (c *Client) Node() *raft.Node {
	return c.node
}

// Get executes a quorum-confirmed linearizable read for key (I-016, I-023).
func (c *Client) Get(ctx context.Context, key string) ([]byte, bool, error) {
	return c.node.LinearizableGet(ctx, key)
}

// Write appends a command to the leader's log and blocks until it is committed and applied (I-019).
func (c *Client) Write(ctx context.Context, cmd *raftv1.Command) ([]byte, error) {
	return c.node.Write(ctx, cmd)
}

// Set executes a SET operation and blocks until applied.
func (c *Client) Set(ctx context.Context, key string, value []byte, requestID string) error {
	cmd := &raftv1.Command{
		OperationType: "SET",
		Key:           key,
		Value:         value,
		RequestId:     requestID,
	}
	_, err := c.Write(ctx, cmd)
	return err
}

// Delete executes a DELETE operation and blocks until applied.
func (c *Client) Delete(ctx context.Context, key string, requestID string) error {
	cmd := &raftv1.Command{
		OperationType: "DELETE",
		Key:           key,
		RequestId:     requestID,
	}
	_, err := c.Write(ctx, cmd)
	return err
}
