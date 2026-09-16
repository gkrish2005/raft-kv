package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"raftkv/internal/cluster"
)

// Test 27: Level 4 SIGKILL smoke test (TestRealCrash_Kill9_RestartAndConverge)
// Spawns a real child process via exec.Command, writes data, sends SIGKILL (kill -9),
// restarts the child on the same data directory, and verifies data survival and convergence.
func TestRealCrash_Kill9_RestartAndConverge(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve node address: %v", err)
	}
	addr := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatalf("release node address: %v", err)
	}

	dataDir := t.TempDir()

	// Build the raftkv-node binary
	nodeBinary := filepath.Join(t.TempDir(), "raftkv-node")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", nodeBinary, "./cmd/raftkv-node")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build raftkv-node: %v\n%s", err, output)
	}

	// Helper to wait until node is listening and has elected a leader
	waitForPort := func(targetAddr string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			conn, dialErr := net.DialTimeout("tcp", targetAddr, 100*time.Millisecond)
			if dialErr == nil {
				_ = conn.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				cmd := exec.CommandContext(ctx, "go", "run", "./cmd/raftkv-cli", "-server", targetAddr, "status")
				cmd.Dir = repoRoot
				out, err := cmd.CombinedOutput()
				cancel()
				if err == nil && strings.Contains(string(out), "Leader ID:") && !strings.Contains(string(out), "Leader ID: \n") {
					return
				}
			}
			time.Sleep(25 * time.Millisecond)
			if time.Now().After(deadline) {
				t.Fatalf("node did not become leader on %s", targetAddr)
			}
		}
	}

	// Helper to run CLI commands
	runCLI := func(args ...string) string {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()

		commandArgs := append([]string{"run", "./cmd/raftkv-cli", "-server", addr}, args...)
		cmd := exec.CommandContext(ctx, "go", commandArgs...)
		cmd.Dir = repoRoot
		output, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("raftkv-cli %s failed: %v\n%s", strings.Join(args, " "), err, output)
		}
		return string(output)
	}

	// 1. Start child node process
	var nodeLogs bytes.Buffer
	nodeCmd := exec.Command(nodeBinary, "-id", "node-restart", "-addr", addr, "-data-dir", dataDir)
	nodeCmd.Stdout = &nodeLogs
	nodeCmd.Stderr = &nodeLogs
	if err := nodeCmd.Start(); err != nil {
		t.Fatalf("failed to start raftkv-node: %v", err)
	}

	waitForPort(addr)

	// 2. Commit a write to the running node
	if got := runCLI("set", "persistent-key", "persistent-val"); got != "OK\n" {
		t.Fatalf("set failed: got %q, want %q\nLogs:\n%s", got, "OK\\n", nodeLogs.String())
	}
	if got := runCLI("get", "persistent-key"); got != "persistent-val\n" {
		t.Fatalf("get failed: got %q, want %q", got, "persistent-val\\n")
	}

	// 3. Hard kill the process with SIGKILL (kill -9) — no graceful shutdown or Stop()
	if err := nodeCmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("failed to send SIGKILL to node: %v", err)
	}
	_ = nodeCmd.Wait()

	// Ensure the port is released
	for {
		conn, dialErr := net.DialTimeout("tcp", addr, 50*time.Millisecond)
		if dialErr != nil {
			break
		}
		_ = conn.Close()
		time.Sleep(25 * time.Millisecond)
	}

	// 4. Restart the node pointing at the same data directory
	var restartedLogs bytes.Buffer
	restartedCmd := exec.Command(nodeBinary, "-id", "node-restart", "-addr", addr, "-data-dir", dataDir)
	restartedCmd.Stdout = &restartedLogs
	restartedCmd.Stderr = &restartedLogs
	if err := restartedCmd.Start(); err != nil {
		t.Fatalf("failed to restart raftkv-node: %v", err)
	}
	t.Cleanup(func() {
		if restartedCmd.Process != nil {
			_ = restartedCmd.Process.Kill()
			_ = restartedCmd.Wait()
		}
	})

	waitForPort(addr)

	// 5. Verify recovered state: the pre-crash committed write MUST be preserved
	if got := runCLI("get", "persistent-key"); got != "persistent-val\n" {
		t.Fatalf("recovered node lost committed write: got %q, want %q\nLogs:\n%s", got, "persistent-val\\n", restartedLogs.String())
	}

	// 6. Verify convergence / continued operation: node accepts subsequent writes
	if got := runCLI("set", "post-crash-key", "post-crash-val"); got != "OK\n" {
		t.Fatalf("post-crash set failed: got %q, want %q", got, "OK\\n")
	}
	if got := runCLI("get", "post-crash-key"); got != "post-crash-val\n" {
		t.Fatalf("post-crash get failed: got %q, want %q", got, "post-crash-val\\n")
	}
}

// Test 35: TestCommitBeforeAckCrash_ExactlyOnce verifies that a committed write whose
// leader crashes before ACK can be retried safely against the recovered node with the
// same RequestID, applying exactly once, and rejecting conflicting payloads (I-017).
func TestCommitBeforeAckCrash_ExactlyOnce(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatalf("resolve repository root: %v", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve node address: %v", err)
	}
	addr := listener.Addr().String()
	_ = listener.Close()

	dataDir := t.TempDir()
	nodeBinary := filepath.Join(t.TempDir(), "raftkv-node")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", nodeBinary, "./cmd/raftkv-node")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build raftkv-node: %v\n%s", err, output)
	}

	waitForLeader := func(targetAddr string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			conn, dialErr := net.DialTimeout("tcp", targetAddr, 100*time.Millisecond)
			if dialErr == nil {
				_ = conn.Close()
				ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
				cmd := exec.CommandContext(ctx, "go", "run", "./cmd/raftkv-cli", "-server", targetAddr, "status")
				cmd.Dir = repoRoot
				out, err := cmd.CombinedOutput()
				cancel()
				if err == nil && strings.Contains(string(out), "Leader ID:") && !strings.Contains(string(out), "Leader ID: \n") {
					return
				}
			}
			time.Sleep(25 * time.Millisecond)
			if time.Now().After(deadline) {
				t.Fatalf("node did not become leader on %s", targetAddr)
			}
		}
	}

	runCLIWithErr := func(args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		commandArgs := append([]string{"run", "./cmd/raftkv-cli", "-server", addr}, args...)
		cmd := exec.CommandContext(ctx, "go", commandArgs...)
		cmd.Dir = repoRoot
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	// 1. Start node
	var nodeLogs bytes.Buffer
	nodeCmd := exec.Command(nodeBinary, "-id", "node-ack", "-addr", addr, "-data-dir", dataDir)
	nodeCmd.Stdout = &nodeLogs
	nodeCmd.Stderr = &nodeLogs
	if err := nodeCmd.Start(); err != nil {
		t.Fatalf("start node: %v", err)
	}

	waitForLeader(addr)

	// 2. Commit a write with explicit RequestID
	out, err := runCLIWithErr("-request-id", "req-commit-ack-1", "set", "ack-key", "ack-val-1")
	if err != nil || out != "OK\n" {
		t.Fatalf("initial set failed: %v, out: %q", err, out)
	}

	// 3. Crash node via SIGKILL
	if err := nodeCmd.Process.Signal(syscall.SIGKILL); err != nil {
		t.Fatalf("SIGKILL failed: %v", err)
	}
	_ = nodeCmd.Wait()

	// 4. Restart node on same data directory
	var restartedLogs bytes.Buffer
	restartedCmd := exec.Command(nodeBinary, "-id", "node-ack", "-addr", addr, "-data-dir", dataDir)
	restartedCmd.Stdout = &restartedLogs
	restartedCmd.Stderr = &restartedLogs
	if err := restartedCmd.Start(); err != nil {
		t.Fatalf("restart node: %v", err)
	}
	t.Cleanup(func() {
		if restartedCmd.Process != nil {
			_ = restartedCmd.Process.Kill()
			_ = restartedCmd.Wait()
		}
	})

	waitForLeader(addr)

	// 5. Retry write with SAME RequestID and SAME payload -> must succeed as dedup hit
	out, err = runCLIWithErr("-request-id", "req-commit-ack-1", "set", "ack-key", "ack-val-1")
	if err != nil || out != "OK\n" {
		t.Fatalf("retried set failed: %v, out: %q", err, out)
	}

	// 6. Attempt write with SAME RequestID but DIFFERENT payload -> must be rejected with STATUS_REQUEST_ID_REUSED (I-017)
	out, err = runCLIWithErr("-request-id", "req-commit-ack-1", "set", "ack-key", "ack-val-2-different")
	if err == nil {
		t.Fatalf("expected error on duplicate RequestID with different payload, got success: %q", out)
	}
	if !strings.Contains(out, "STATUS_REQUEST_ID_REUSED") {
		t.Fatalf("expected STATUS_REQUEST_ID_REUSED in output, got: %q", out)
	}

	// 7. Verify state machine retained original value "ack-val-1"
	out, err = runCLIWithErr("get", "ack-key")
	if err != nil || out != "ack-val-1\n" {
		t.Fatalf("get after recovery returned unexpected value: %v, out: %q", err, out)
	}
}

// Test 36: TestRollingLeaderKill_WriteSurvival verifies that continuous writes across
// rolling leader crashes in a 3-node cluster survive with zero lost or duplicated entries (I-017).
func TestRollingLeaderKill_WriteSurvival(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "raftkv-node")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", binary, "./cmd/raftkv-node")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build raftkv-node: %v\n%s", err, output)
	}

	addresses := make([]string, 3)
	for i := range addresses {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserve node address: %v", err)
		}
		addresses[i] = listener.Addr().String()
		_ = listener.Close()
	}

	dataDirs := make([]string, 3)
	configs := make([]string, 3)
	for i := range configs {
		dataDirs[i] = t.TempDir()
		peers := make([]cluster.PeerAddr, 3)
		for j := range peers {
			peers[j] = cluster.PeerAddr{ID: string(rune('a' + j)), Address: addresses[j]}
		}
		data, err := json.Marshal(cluster.ClusterConfig{NodeID: string(rune('a' + i)), Peers: peers})
		if err != nil {
			t.Fatal(err)
		}
		configs[i] = filepath.Join(t.TempDir(), "node.json")
		if err := os.WriteFile(configs[i], data, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	type procInfo struct {
		id      string
		addr    string
		dataDir string
		cmd     *exec.Cmd
		cancel  context.CancelFunc
	}
	procs := make([]*procInfo, 3)

	startNode := func(i int) {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := exec.CommandContext(ctx, binary, "-addr", addresses[i], "-config", configs[i], "-data-dir", dataDirs[i])
		procs[i] = &procInfo{
			id:      string(rune('a' + i)),
			addr:    addresses[i],
			dataDir: dataDirs[i],
			cmd:     cmd,
			cancel:  cancel,
		}
		if err := cmd.Start(); err != nil {
			cancel()
			t.Fatalf("start node %s failed: %v", procs[i].id, err)
		}
	}

	for i := range procs {
		startNode(i)
	}
	t.Cleanup(func() {
		for _, p := range procs {
			if p != nil && p.cmd != nil && p.cmd.Process != nil {
				_ = p.cmd.Process.Kill()
				_ = p.cmd.Wait()
			}
		}
	})

	runCLIOn := func(targetAddr string, args ...string) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		commandArgs := append([]string{"run", "./cmd/raftkv-cli", "-server", targetAddr}, args...)
		cmd := exec.CommandContext(ctx, "go", commandArgs...)
		cmd.Dir = repoRoot
		output, err := cmd.CombinedOutput()
		return string(output), err
	}

	findActiveLeader := func() (int, string) {
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) {
			for _, p := range procs {
				if p == nil || p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
					continue
				}
				out, err := runCLIOn(p.addr, "status")
				if err == nil && strings.Contains(out, "Leader ID:") {
					for _, line := range strings.Split(out, "\n") {
						if strings.HasPrefix(line, "Leader ID:") {
							lid := strings.TrimSpace(strings.TrimPrefix(line, "Leader ID:"))
							if lid != "" {
								for j, cand := range procs {
									if cand != nil && cand.id == lid && (cand.cmd.ProcessState == nil || !cand.cmd.ProcessState.Exited()) {
										return j, cand.addr
									}
								}
							}
						}
					}
				}
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("no active leader found in cluster")
		return -1, ""
	}

	// 1. Find initial leader and write key 1
	leaderIdx, leaderAddr := findActiveLeader()
	out, err := runCLIOn(leaderAddr, "-request-id", "req-roll-1", "set", "roll-k1", "roll-v1")
	if err != nil || out != "OK\n" {
		t.Fatalf("write to initial leader %s failed: %v, out: %q", procs[leaderIdx].id, err, out)
	}

	// 2. Kill current leader with SIGKILL
	killIdx := leaderIdx
	_ = procs[killIdx].cmd.Process.Signal(syscall.SIGKILL)
	_ = procs[killIdx].cmd.Wait()

	// 3. Wait for new leader among remaining 2 nodes (quorum is 2 of 3)
	newLeaderIdx, newLeaderAddr := findActiveLeader()
	if newLeaderIdx == killIdx {
		t.Fatalf("killed node %d reported as new leader", killIdx)
	}

	// 4. Write key 2 to new leader
	out, err = runCLIOn(newLeaderAddr, "-request-id", "req-roll-2", "set", "roll-k2", "roll-v2")
	if err != nil || out != "OK\n" {
		t.Fatalf("write to second leader %s failed: %v, out: %q", procs[newLeaderIdx].id, err, out)
	}

	// 5. Test dedup on new leader: retry key 1 with SAME RequestID -> dedup hit!
	out, err = runCLIOn(newLeaderAddr, "-request-id", "req-roll-1", "set", "roll-k1", "roll-v1")
	if err != nil || out != "OK\n" {
		t.Fatalf("dedup retry to second leader failed: %v, out: %q", err, out)
	}

	// 6. Test I-017 on new leader: retry key 1 with SAME RequestID but DIFFERENT value -> STATUS_REQUEST_ID_REUSED
	out, err = runCLIOn(newLeaderAddr, "-request-id", "req-roll-1", "set", "roll-k1", "different-payload")
	if err == nil || !strings.Contains(out, "STATUS_REQUEST_ID_REUSED") {
		t.Fatalf("expected STATUS_REQUEST_ID_REUSED from second leader, got: %q, err: %v", out, err)
	}

	// 7. Restart the previously killed node so all 3 nodes are active
	startNode(killIdx)
	time.Sleep(500 * time.Millisecond)

	// 8. Verify all written keys exist across the cluster
	out, err = runCLIOn(newLeaderAddr, "get", "roll-k1")
	if err != nil || out != "roll-v1\n" {
		t.Fatalf("verify roll-k1 failed: %v, out: %q", err, out)
	}
	out, err = runCLIOn(newLeaderAddr, "get", "roll-k2")
	if err != nil || out != "roll-v2\n" {
		t.Fatalf("verify roll-k2 failed: %v, out: %q", err, out)
	}
}
