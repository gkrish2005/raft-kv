package main

import (
	"bytes"
	"context"
	"net"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
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

	// Helper to wait until node is listening
	waitForPort := func(targetAddr string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			conn, dialErr := net.DialTimeout("tcp", targetAddr, 100*time.Millisecond)
			if dialErr == nil {
				_ = conn.Close()
				return
			}
			time.Sleep(25 * time.Millisecond)
			if time.Now().After(deadline) {
				t.Fatalf("node did not listen on %s: %v", targetAddr, dialErr)
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
