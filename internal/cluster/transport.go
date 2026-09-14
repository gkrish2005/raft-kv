package cluster

import (
	"context"
	"fmt"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	raftv1 "raftkv/proto/raft/v1"
)

// GRPCTransport performs Raft RPCs outside the Raft package and its mutex.
type GRPCTransport struct{ peers map[string]string }

func NewGRPCTransport(peers []PeerAddr) *GRPCTransport {
	m := make(map[string]string, len(peers))
	for _, p := range peers {
		m[p.ID] = p.Address
	}
	return &GRPCTransport{peers: m}
}
func (t *GRPCTransport) dial(ctx context.Context, peer string) (*grpc.ClientConn, error) {
	addr, ok := t.peers[peer]
	if !ok {
		return nil, fmt.Errorf("unknown peer %q", peer)
	}
	return grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
}
func (t *GRPCTransport) SendRequestVote(ctx context.Context, peer string, req *raftv1.RequestVoteRequest) (*raftv1.RequestVoteResponse, error) {
	c, err := t.dial(ctx, peer)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return raftv1.NewRaftServiceClient(c).RequestVote(ctx, req)
}
func (t *GRPCTransport) SendAppendEntries(ctx context.Context, peer string, req *raftv1.AppendEntriesRequest) (*raftv1.AppendEntriesResponse, error) {
	c, err := t.dial(ctx, peer)
	if err != nil {
		return nil, err
	}
	defer c.Close()
	return raftv1.NewRaftServiceClient(c).AppendEntries(ctx, req)
}
