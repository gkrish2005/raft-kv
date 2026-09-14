package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc"

	"raftkv/internal/cluster"
	"raftkv/internal/raft"
	"raftkv/internal/storage"
	clientv1 "raftkv/proto/client/v1"
	raftv1 "raftkv/proto/raft/v1"
)

type server struct {
	clientv1.UnimplementedClientServiceServer
	nodeID string
	sm     storage.StateMachine
	cfg    *cluster.ClusterConfig
	logger *slog.Logger
}

func newServer(nodeID string, sm storage.StateMachine, cfg *cluster.ClusterConfig, logger *slog.Logger) *server {
	return &server{
		nodeID: nodeID,
		sm:     sm,
		cfg:    cfg,
		logger: logger,
	}
}

func validateRequestID(reqID string) error {
	if reqID == "" {
		return fmt.Errorf("request_id must not be empty")
	}
	if len(reqID) > 64 {
		return fmt.Errorf("request_id exceeds 64 bytes")
	}
	if !utf8.ValidString(reqID) {
		return fmt.Errorf("request_id must be valid UTF-8")
	}
	return nil
}

// Get serves local StateMachine reads.
// GET is explicitly NOT a Raft log command — it calls sm.Get(key) directly.
func (s *server) Get(ctx context.Context, req *clientv1.GetRequest) (*clientv1.GetResponse, error) {
	start := time.Now()
	val, found := s.sm.Get(req.GetKey())
	duration := time.Since(start)

	s.logger.Info("handled Get",
		"key", req.GetKey(),
		"found", found,
		"value_bytes", len(val),
		"duration_us", duration.Microseconds(),
	)

	return &clientv1.GetResponse{
		Status: clientv1.Status_STATUS_SUCCESS,
		Value:  val,
		Found:  found,
	}, nil
}

// Set applies a SET command to the StateMachine.
func (s *server) Set(ctx context.Context, req *clientv1.SetRequest) (*clientv1.SetResponse, error) {
	start := time.Now()

	if err := validateRequestID(req.GetRequestId()); err != nil {
		s.logger.Warn("rejected Set: invalid request_id",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
		)
		return &clientv1.SetResponse{
			Status:       clientv1.Status_STATUS_INVALID_REQUEST,
			ErrorMessage: err.Error(),
		}, nil
	}

	cmd := storage.Command{
		OperationType: storage.Set,
		Key:           req.GetKey(),
		Value:         req.GetValue(),
		RequestID:     req.GetRequestId(),
	}

	_, err := s.sm.Apply(cmd)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("failed Set",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
			"duration_us", duration.Microseconds(),
		)
		return &clientv1.SetResponse{
			Status:       clientv1.Status_STATUS_UNSPECIFIED,
			ErrorMessage: err.Error(),
		}, nil
	}

	s.logger.Info("handled Set",
		"key", req.GetKey(),
		"request_id", req.GetRequestId(),
		"duration_us", duration.Microseconds(),
	)

	return &clientv1.SetResponse{
		Status: clientv1.Status_STATUS_SUCCESS,
	}, nil
}

// Delete applies a DELETE command to the StateMachine.
func (s *server) Delete(ctx context.Context, req *clientv1.DeleteRequest) (*clientv1.DeleteResponse, error) {
	start := time.Now()

	if err := validateRequestID(req.GetRequestId()); err != nil {
		s.logger.Warn("rejected Delete: invalid request_id",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
		)
		return &clientv1.DeleteResponse{
			Status:       clientv1.Status_STATUS_INVALID_REQUEST,
			ErrorMessage: err.Error(),
		}, nil
	}

	cmd := storage.Command{
		OperationType: storage.Delete,
		Key:           req.GetKey(),
		RequestID:     req.GetRequestId(),
	}

	_, err := s.sm.Apply(cmd)
	duration := time.Since(start)

	if err != nil {
		s.logger.Error("failed Delete",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
			"duration_us", duration.Microseconds(),
		)
		return &clientv1.DeleteResponse{
			Status:       clientv1.Status_STATUS_UNSPECIFIED,
			ErrorMessage: err.Error(),
		}, nil
	}

	s.logger.Info("handled Delete",
		"key", req.GetKey(),
		"request_id", req.GetRequestId(),
		"duration_us", duration.Microseconds(),
	)

	return &clientv1.DeleteResponse{
		Status: clientv1.Status_STATUS_SUCCESS,
	}, nil
}

// ClusterStatus returns the current node's view of the cluster.
func (s *server) ClusterStatus(ctx context.Context, req *clientv1.ClusterStatusRequest) (*clientv1.ClusterStatusResponse, error) {
	start := time.Now()

	nodes := []*clientv1.NodeStatus{
		{
			Id: s.nodeID,
			// Placeholder — no real Raft/election exists until Phase 5; do not treat as authoritative.
			Role:        "Leader",
			LastContact: time.Now().UnixMilli(),
		},
	}

	if s.cfg != nil {
		for _, peer := range s.cfg.Peers {
			nodes = append(nodes, &clientv1.NodeStatus{
				Id: peer.ID,
				// Placeholder — no real Raft/election exists until Phase 1; do not treat as authoritative.
				Role:        "Follower",
				LastContact: 0,
			})
		}
	}

	duration := time.Since(start)
	s.logger.Info("handled ClusterStatus",
		"node_count", len(nodes),
		"duration_us", duration.Microseconds(),
	)

	return &clientv1.ClusterStatusResponse{
		Status:   clientv1.Status_STATUS_SUCCESS,
		LeaderId: s.nodeID,
		// Placeholder — no real Raft/election exists until Phase 5; do not treat as authoritative.
		Term:  1,
		Nodes: nodes,
	}, nil
}

func main() {
	addr := flag.String("addr", ":50051", "gRPC listen address")
	nodeID := flag.String("id", "node-1", "node ID")
	configPath := flag.String("config", "", "path to cluster config file (JSON or YAML)")
	dataDir := flag.String("data-dir", ".", "directory for durable Raft metadata")
	flag.Parse()

	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	slog.SetDefault(logger)

	var cfg *cluster.ClusterConfig
	if *configPath != "" {
		var err error
		cfg, err = cluster.LoadConfig(*configPath)
		if err != nil {
			logger.Error("failed to load cluster config", "path", *configPath, "error", err)
			os.Exit(1)
		}
		if cfg.NodeID != "" {
			*nodeID = cfg.NodeID
		}
		logger.Info("loaded cluster config", "node_id", cfg.NodeID, "peer_count", len(cfg.Peers))
	}

	sm := storage.NewKVStateMachine()
	srv := newServer(*nodeID, sm, cfg, logger)
	var peers []cluster.PeerAddr
	if cfg != nil {
		peers = cfg.Peers
	}
	peerIDs := make([]string, 0, len(peers))
	for _, peer := range peers {
		if peer.ID != *nodeID {
			peerIDs = append(peerIDs, peer.ID)
		}
	}
	raftNode, err := raft.NewNode(raft.Config{ID: *nodeID, Peers: peerIDs, Clock: raft.RealClock(), Transport: cluster.NewGRPCTransport(peers), Store: storage.NewFileTermVoteStore(filepath.Join(*dataDir, "termvote"))})
	if err != nil {
		logger.Error("failed to create raft node", "error", err)
		os.Exit(1)
	}
	if err := raftNode.Start(); err != nil {
		logger.Error("failed to start raft node", "error", err)
		os.Exit(1)
	}

	lis, err := net.Listen("tcp", *addr)
	if err != nil {
		logger.Error("failed to listen", "addr", *addr, "error", err)
		os.Exit(1)
	}

	grpcServer := grpc.NewServer()
	clientv1.RegisterClientServiceServer(grpcServer, srv)
	raftv1.RegisterRaftServiceServer(grpcServer, raftNode)

	go func() {
		logger.Info("starting raftkv-node gRPC server", "addr", *addr, "node_id", *nodeID)
		if err := grpcServer.Serve(lis); err != nil && err != grpc.ErrServerStopped {
			logger.Error("gRPC server error", "error", err)
			os.Exit(1)
		}
	}()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	sig := <-sigChan

	logger.Info("shutting down raftkv-node server gracefully", "signal", sig.String())
	grpcServer.GracefulStop()
	raftNode.Stop()
	logger.Info("raftkv-node server stopped")
}
