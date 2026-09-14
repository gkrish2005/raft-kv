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
	"testing"
	"time"

	"raftkv/internal/cluster"
)

func TestCLISmoke(t *testing.T) {
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

	nodeBinary := filepath.Join(t.TempDir(), "raftkv-node")
	buildCtx, buildCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer buildCancel()
	build := exec.CommandContext(buildCtx, "go", "build", "-o", nodeBinary, "./cmd/raftkv-node")
	build.Dir = repoRoot
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("build raftkv-node: %v\n%s", err, output)
	}

	nodeCtx, stopNode := context.WithCancel(context.Background())
	var nodeLogs bytes.Buffer
	node := exec.CommandContext(nodeCtx, nodeBinary, "-id", "node-smoke", "-addr", addr, "-data-dir", t.TempDir())
	node.Stdout = &nodeLogs
	node.Stderr = &nodeLogs
	if err := node.Start(); err != nil {
		stopNode()
		t.Fatalf("start raftkv-node: %v", err)
	}
	nodeExited := make(chan error, 1)
	go func() { nodeExited <- node.Wait() }()
	t.Cleanup(func() {
		stopNode()
		if err := <-nodeExited; err != nil {
			t.Logf("raftkv-node exit after cancellation: %v\n%s", err, nodeLogs.String())
		}
	})

	deadline := time.Now().Add(10 * time.Second)
	for {
		conn, dialErr := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if dialErr == nil {
			_ = conn.Close()
			break
		}
		<-time.After(25 * time.Millisecond)
		if time.Now().After(deadline) {
			t.Fatalf("raftkv-node did not listen on %s: %v\n%s", addr, dialErr, nodeLogs.String())
		}
	}

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

	if got := runCLI("get", "missing"); got != "(not found)\n" {
		t.Errorf("get missing output = %q, want %q", got, "(not found)\\n")
	}
	if got := runCLI("set", "key", "value"); got != "OK\n" {
		t.Errorf("set output = %q, want %q", got, "OK\\n")
	}
	if got := runCLI("get", "key"); got != "value\n" {
		t.Errorf("get output = %q, want %q", got, "value\\n")
	}
	if got := runCLI("delete", "key"); got != "OK\n" {
		t.Errorf("delete output = %q, want %q", got, "OK\\n")
	}
	if got := runCLI("get", "key"); got != "(not found)\n" {
		t.Errorf("get after delete output = %q, want %q", got, "(not found)\\n")
	}

	status := runCLI("status")
	for _, want := range []string{
		"Leader ID: node-smoke",
		"Nodes (1):",
	} {
		if !strings.Contains(status, want) {
			t.Errorf("status output %q does not contain %q", status, want)
		}
	}
}

type nodeProcess struct {
	id      string
	addr    string
	data    string
	cmd     *exec.Cmd
	cancel  context.CancelFunc
	log     *os.File
	logPath string
}

func TestMultiProcessLeaderKillAndRestart(t *testing.T) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "raftkv-node")
	build := exec.Command("go", "build", "-o", binary, "./cmd/raftkv-node")
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
	configs := make([]string, 3)
	for i := range configs {
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

	start := func(i int, dataDir string) *nodeProcess {
		ctx, cancel := context.WithCancel(context.Background())
		cmd := exec.CommandContext(ctx, binary, "-addr", addresses[i], "-config", configs[i], "-data-dir", dataDir)
		logPath := filepath.Join(dataDir, "node.log")
		logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
		if err != nil {
			cancel()
			t.Fatalf("open node %d log: %v", i, err)
		}
		p := &nodeProcess{id: string(rune('a' + i)), addr: addresses[i], data: dataDir, cmd: cmd, cancel: cancel, log: logFile, logPath: logPath}
		cmd.Stdout = logFile
		cmd.Stderr = logFile
		if err := cmd.Start(); err != nil {
			_ = logFile.Close()
			cancel()
			t.Fatalf("start node %d: %v", i, err)
		}
		return p
	}
	stop := func(p *nodeProcess) {
		if p == nil || p.cmd.ProcessState != nil && p.cmd.ProcessState.Exited() {
			return
		}
		p.cancel()
		_ = p.cmd.Wait()
		_ = p.log.Close()
	}
	processes := make([]*nodeProcess, 3)
	for i := range processes {
		processes[i] = start(i, t.TempDir())
	}
	t.Cleanup(func() {
		for _, p := range processes {
			stop(p)
		}
	})

	logContains := func(path string, wants ...string) bool {
		data, err := os.ReadFile(path)
		if err != nil {
			return false
		}
		for _, want := range wants {
			if !strings.Contains(string(data), want) {
				return false
			}
		}
		return true
	}
	logSize := func(path string) int64 {
		info, err := os.Stat(path)
		if err != nil {
			return 0
		}
		return info.Size()
	}
	logSuffixContains := func(path string, offset int64, want string) bool {
		data, err := os.ReadFile(path)
		return err == nil && int64(len(data)) >= offset && strings.Contains(string(data[offset:]), want)
	}
	findLeader := func(exclude string) *nodeProcess {
		deadline := time.Now().Add(12 * time.Second)
		for time.Now().Before(deadline) {
			for _, p := range processes {
				if p == nil || p.id == exclude {
					continue
				}
				if logContains(p.logPath, `"msg":"raft leader elected"`, `"node_id":"`+p.id+`"`) {
					return p
				}
			}
			time.Sleep(50 * time.Millisecond)
		}
		return nil
	}
	leader := findLeader("")
	if leader == nil {
		t.Fatal("cluster did not elect an initial leader")
	}
	if err := leader.cmd.Process.Kill(); err != nil {
		t.Fatalf("kill leader %s: %v", leader.id, err)
	}
	_ = leader.cmd.Wait()
	_ = leader.log.Close()
	leader.cancel()
	newLeader := findLeader(leader.id)
	if newLeader == nil {
		t.Fatal("surviving nodes did not elect a replacement leader")
	}
	// Allow the replacement leader's first heartbeat rounds to settle before
	// measuring whether the restarted follower causes a further election.
	time.Sleep(750 * time.Millisecond)

	index := int(leader.id[0] - 'a')
	logOffsets := make(map[string]int64, len(processes))
	for _, p := range processes {
		if p != nil {
			logOffsets[p.id] = logSize(p.logPath)
		}
	}
	processes[index] = start(index, leader.data)
	deadline := time.Now().Add(12 * time.Second)
	for time.Now().Before(deadline) {
		if logContains(processes[index].logPath, `"msg":"raft node started"`, `"node_id":"`+leader.id+`"`, `"role":"Follower"`) {
			time.Sleep(500 * time.Millisecond)
			for _, p := range processes {
				if logSuffixContains(p.logPath, logOffsets[p.id], `"msg":"raft leader elected"`) {
					logs, _ := os.ReadFile(p.logPath)
					t.Logf("node %s logs:\n%s", p.id, logs)
					t.Fatalf("restart disrupted leader %s: node %s started another election", newLeader.id, p.id)
				}
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("restarted node %s did not rejoin as follower under leader %s", leader.id, newLeader.id)
}
