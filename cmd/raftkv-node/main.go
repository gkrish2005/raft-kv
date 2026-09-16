package main

import (
	"context"
	"errors"
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

const (
	MaxKeyBytes   = 4096
	MaxValueBytes = 1 << 20 // 1 MiB — matches WAL record size limit
)

type server struct {
	clientv1.UnimplementedClientServiceServer
	nodeID   string
	sm       *storage.KVStateMachine
	raftNode *raft.Node
	cfg      *cluster.ClusterConfig
	logger   *slog.Logger
}

func newServer(nodeID string, sm *storage.KVStateMachine, raftNode *raft.Node, cfg *cluster.ClusterConfig, logger *slog.Logger) *server {
	return &server{
		nodeID:   nodeID,
		sm:       sm,
		raftNode: raftNode,
		cfg:      cfg,
		logger:   logger,
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

func validateKey(key string) error {
	if len(key) == 0 {
		return fmt.Errorf("key must not be empty")
	}
	if len(key) > MaxKeyBytes {
		return fmt.Errorf("key exceeds max length of %d bytes", MaxKeyBytes)
	}
	return nil
}

func validateValue(value []byte) error {
	if len(value) > MaxValueBytes {
		return fmt.Errorf("value exceeds max size of %d bytes", MaxValueBytes)
	}
	return nil
}

// Get serves linearizable reads via raftNode or local state machine.
func (s *server) Get(ctx context.Context, req *clientv1.GetRequest) (*clientv1.GetResponse, error) {
	start := time.Now()

	if err := validateKey(req.GetKey()); err != nil {
		s.logger.Warn("rejected Get: invalid key",
			"key", req.GetKey(),
			"error", err.Error(),
		)
		return &clientv1.GetResponse{
			Status:       clientv1.Status_STATUS_INVALID_REQUEST,
			ErrorMessage: err.Error(),
		}, nil
	}

	var val []byte
	var found bool
	if s.raftNode != nil {
		if s.raftNode.Role() != raft.Leader {
			leaderHint := s.raftNode.LeaderHint()
			return &clientv1.GetResponse{
				Status:       clientv1.Status_STATUS_NOT_LEADER,
				LeaderHint:   leaderHint,
				ErrorMessage: "not leader",
			}, nil
		}

		v, f, err := s.raftNode.LinearizableGet(ctx, req.GetKey())
		if err != nil {
			// I-016: never substitute a non-linearizable local read on LinearizableGet
			// failure. Any error — not leader, quorum confirmation failed, leadership
			// lost mid-barrier, term mismatch — must be surfaced to the client so it can
			// retry against the actual leader. Silently falling through to s.sm.Get()
			// here would return potentially stale, unconfirmed state as if it were a
			// successful linearizable read.
			duration := time.Since(start)
			s.logger.Warn("linearizable Get failed",
				"key", req.GetKey(),
				"error", err.Error(),
				"duration_us", duration.Microseconds(),
			)

			leaderHint := s.raftNode.LeaderHint()
			status := clientv1.Status_STATUS_UNSPECIFIED
			if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) || errors.Is(err, raft.ErrLeaderChanged) {
				status = clientv1.Status_STATUS_NOT_LEADER
			} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = clientv1.Status_STATUS_TIMEOUT
			} else if errors.Is(err, raft.ErrNodeStopped) {
				status = clientv1.Status_STATUS_UNSPECIFIED
			}

			return &clientv1.GetResponse{
				Status:       status,
				LeaderHint:   leaderHint,
				ErrorMessage: err.Error(),
			}, nil
		}
		val = v
		found = f
	} else {
		// Single-node dev/test mode: no Raft layer, direct state machine read is correct.
		val, found = s.sm.Get(req.GetKey())
	}
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

// Set applies a SET command through Raft or local StateMachine.
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

	if err := validateKey(req.GetKey()); err != nil {
		s.logger.Warn("rejected Set: invalid key",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
		)
		return &clientv1.SetResponse{
			Status:       clientv1.Status_STATUS_INVALID_REQUEST,
			ErrorMessage: err.Error(),
		}, nil
	}

	if err := validateValue(req.GetValue()); err != nil {
		s.logger.Warn("rejected Set: invalid value",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
		)
		return &clientv1.SetResponse{
			Status:       clientv1.Status_STATUS_INVALID_REQUEST,
			ErrorMessage: err.Error(),
		}, nil
	}

	if s.raftNode != nil {
		if s.raftNode.Role() != raft.Leader {
			leaderHint := s.raftNode.LeaderHint()
			return &clientv1.SetResponse{
				Status:       clientv1.Status_STATUS_NOT_LEADER,
				LeaderHint:   leaderHint,
				ErrorMessage: "not leader",
			}, nil
		}

		cmd := &raftv1.Command{
			OperationType: "SET",
			Key:           req.GetKey(),
			Value:         req.GetValue(),
			RequestId:     req.GetRequestId(),
		}
		_, err := s.raftNode.Write(ctx, cmd)
		duration := time.Since(start)
		if err != nil {
			s.logger.Error("failed Set",
				"key", req.GetKey(),
				"request_id", req.GetRequestId(),
				"error", err.Error(),
				"duration_us", duration.Microseconds(),
			)

			leaderHint := s.raftNode.LeaderHint()
			status := clientv1.Status_STATUS_UNSPECIFIED
			if errors.Is(err, storage.ErrRequestIDReused) {
				// I-017: duplicate RequestID with different payload.
				status = clientv1.Status_STATUS_REQUEST_ID_REUSED
			} else if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) || errors.Is(err, raft.ErrLeaderChanged) {
				status = clientv1.Status_STATUS_NOT_LEADER
			} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = clientv1.Status_STATUS_TIMEOUT
			} else if errors.Is(err, raft.ErrNodeStopped) {
				status = clientv1.Status_STATUS_UNSPECIFIED
			}

			return &clientv1.SetResponse{
				Status:       status,
				LeaderHint:   leaderHint,
				ErrorMessage: err.Error(),
			}, nil
		}
	} else {
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
			status := clientv1.Status_STATUS_UNSPECIFIED
			if errors.Is(err, storage.ErrRequestIDReused) {
				status = clientv1.Status_STATUS_REQUEST_ID_REUSED
			}
			return &clientv1.SetResponse{
				Status:       status,
				ErrorMessage: err.Error(),
			}, nil
		}
	}

	duration := time.Since(start)
	s.logger.Info("handled Set",
		"key", req.GetKey(),
		"request_id", req.GetRequestId(),
		"duration_us", duration.Microseconds(),
	)

	return &clientv1.SetResponse{
		Status: clientv1.Status_STATUS_SUCCESS,
	}, nil
}

// Delete applies a DELETE command through Raft or local StateMachine.
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

	if err := validateKey(req.GetKey()); err != nil {
		s.logger.Warn("rejected Delete: invalid key",
			"key", req.GetKey(),
			"request_id", req.GetRequestId(),
			"error", err.Error(),
		)
		return &clientv1.DeleteResponse{
			Status:       clientv1.Status_STATUS_INVALID_REQUEST,
			ErrorMessage: err.Error(),
		}, nil
	}

	if s.raftNode != nil {
		if s.raftNode.Role() != raft.Leader {
			leaderHint := s.raftNode.LeaderHint()
			return &clientv1.DeleteResponse{
				Status:       clientv1.Status_STATUS_NOT_LEADER,
				LeaderHint:   leaderHint,
				ErrorMessage: "not leader",
			}, nil
		}

		cmd := &raftv1.Command{
			OperationType: "DELETE",
			Key:           req.GetKey(),
			RequestId:     req.GetRequestId(),
		}
		_, err := s.raftNode.Write(ctx, cmd)
		duration := time.Since(start)
		if err != nil {
			s.logger.Error("failed Delete",
				"key", req.GetKey(),
				"request_id", req.GetRequestId(),
				"error", err.Error(),
				"duration_us", duration.Microseconds(),
			)

			leaderHint := s.raftNode.LeaderHint()
			status := clientv1.Status_STATUS_UNSPECIFIED
			if errors.Is(err, storage.ErrRequestIDReused) {
				status = clientv1.Status_STATUS_REQUEST_ID_REUSED
			} else if errors.Is(err, raft.ErrNotLeader) || errors.Is(err, raft.ErrLeadershipLost) || errors.Is(err, raft.ErrLeaderChanged) {
				status = clientv1.Status_STATUS_NOT_LEADER
			} else if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
				status = clientv1.Status_STATUS_TIMEOUT
			} else if errors.Is(err, raft.ErrNodeStopped) {
				status = clientv1.Status_STATUS_UNSPECIFIED
			}

			return &clientv1.DeleteResponse{
				Status:       status,
				LeaderHint:   leaderHint,
				ErrorMessage: err.Error(),
			}, nil
		}
	} else {
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
			status := clientv1.Status_STATUS_UNSPECIFIED
			if errors.Is(err, storage.ErrRequestIDReused) {
				status = clientv1.Status_STATUS_REQUEST_ID_REUSED
			}
			return &clientv1.DeleteResponse{
				Status:       status,
				ErrorMessage: err.Error(),
			}, nil
		}
	}

	duration := time.Since(start)
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

	var term uint64
	var role string
	var leaderHint string

	if s.raftNode != nil {
		t, r, lh := s.raftNode.ClusterView()
		term = t
		role = string(r)
		leaderHint = lh
	} else {
		term = 1
		role = "Leader"
		leaderHint = s.nodeID
	}

	status := clientv1.Status_STATUS_SUCCESS
	if leaderHint == "" {
		status = clientv1.Status_STATUS_NO_LEADER
	}

	nodes := []*clientv1.NodeStatus{
		{
			Id:          s.nodeID,
			Role:        role,
			LastContact: time.Now().UnixMilli(),
		},
	}

	if s.cfg != nil {
		for _, peer := range s.cfg.Peers {
			peerRole := "Follower"
			if peer.ID == leaderHint {
				peerRole = "Leader"
			}
			nodes = append(nodes, &clientv1.NodeStatus{
				Id:          peer.ID,
				Role:        peerRole,
				LastContact: 0,
			})
		}
	}

	duration := time.Since(start)
	s.logger.Info("handled ClusterStatus",
		"node_count", len(nodes),
		"duration_us", duration.Microseconds(),
	)

	// Note: STATUS_OVERLOADED is deferred to Phase 6+; requires backpressure signals
	// from the Raft layer for outstanding PendingWrites/in-flight batches.
	return &clientv1.ClusterStatusResponse{
		Status:     status,
		LeaderId:   leaderHint,
		LeaderHint: leaderHint,
		Term:       term,
		Nodes:      nodes,
	}, nil
}

func main() {
	addr := flag.String("addr", ":50051", "address to listen on")
	nodeID := flag.String("id", "", "node ID (required or in config)")
	configPath := flag.String("config", "", "path to cluster config file")
	dataDir := flag.String("data-dir", "data", "directory for persistent data")
	logLevel := flag.String("log-level", "info", "log level (debug, info, warn, error)")
	flag.Parse()

	var level slog.Level
	switch *logLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	default:
		fmt.Fprintf(os.Stderr, "invalid log level: %s\n", *logLevel)
		os.Exit(1)
	}

	logger := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{
		Level: level,
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

	if *nodeID == "" {
		fmt.Fprintln(os.Stderr, "node ID is required (via -id flag or config file)")
		flag.Usage()
		os.Exit(1)
	}

	sm := storage.NewKVStateMachine()
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
	allNodes := append([]string{*nodeID}, peerIDs...)
	logStore, err := storage.NewFileLogStore(filepath.Join(*dataDir, "wal"))
	if err != nil {
		logger.Error("failed to create log store", "error", err)
		os.Exit(1)
	}
	raftNode, err := raft.NewNode(raft.Config{
		ID:           *nodeID,
		Peers:        peerIDs,
		Clock:        raft.RealClock(),
		Transport:    cluster.NewGRPCTransport(peers),
		Store:        storage.NewFileTermVoteStore(filepath.Join(*dataDir, "termvote"), allNodes),
		LogStore:     logStore,
		StateMachine: sm,
	})
	if err != nil {
		logger.Error("failed to create raft node", "error", err)
		os.Exit(1)
	}
	if err := raftNode.Start(); err != nil {
		logger.Error("failed to start raft node", "error", err)
		os.Exit(1)
	}

	srv := newServer(*nodeID, sm, raftNode, cfg, logger)

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
