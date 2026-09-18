package main

import (
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"raftkv/internal/chaos"
)

func main() {
	duration := flag.Duration("duration", 30*time.Minute, "soak test duration (e.g. 30m, 1m, 30s)")
	seed := flag.Int64("seed", 0, "random seed (0 for auto-generated timestamp seed)")
	nodes := flag.Int("nodes", 3, "number of cluster nodes (minimum 3)")
	rate := flag.Int("rate", 50, "client request rate per second")
	dataDir := flag.String("data-dir", "", "base directory for persistent cluster data")
	scenario := flag.String("scenario", "fuzzer", "scenario to run (fuzzer, leader-crash, follower-crash, minority-partition, majority-partition, repeated-elections, slow-follower-catchup, slow-follower-timeout)")
	flag.Parse()

	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)

	baseDir := *dataDir
	if baseDir == "" {
		tmpDir, err := os.MkdirTemp("", "raftkv-chaos-*")
		if err != nil {
			logger.Error("failed to create temporary data dir", "error", err)
			os.Exit(1)
		}
		defer os.RemoveAll(tmpDir)
		baseDir = tmpDir
	}

	logger.Info("starting chaos runner",
		"scenario", *scenario,
		"duration", duration.String(),
		"seed", *seed,
		"nodes", *nodes,
		"rate", *rate,
		"data_dir", baseDir,
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)

	switch *scenario {
	case "fuzzer":
		cfg := chaos.FuzzerConfig{
			Seed:        *seed,
			Duration:    *duration,
			RequestRate: *rate,
			NodeCount:   *nodes,
			BaseDir:     baseDir,
		}
		fuzzer, err := chaos.NewFuzzer(cfg)
		if err != nil {
			logger.Error("failed to initialize fuzzer", "error", err)
			os.Exit(1)
		}

		go func() {
			sig := <-sigChan
			logger.Warn("interrupted by signal, terminating soak cleanly", "signal", sig.String())
			os.Exit(130)
		}()

		// Fake testing.T wrapper for CLI execution
		logger.Info("running seeded chaos soak...", "seed", *seed, "duration", duration.String())
		// Run standalone soak loop
		start := time.Now()
		// Mock testing adapter
		stats := runCLIFuzzer(fuzzer, *duration, logger)
		logger.Info("chaos soak completed successfully",
			"elapsed", time.Since(start).String(),
			"seed", *seed,
			"cycles", stats.CyclesCompleted,
			"writes_attempted", stats.TotalWritesAttempted,
			"writes_confirmed", stats.TotalWritesConfirmed,
			"faults_injected", stats.FaultsInjected,
		)
		fmt.Printf("CHAOS SOAK RESULT: a %s seeded chaos soak completed with zero observed invariant violations (seed: %d)\n",
			duration.String(), *seed)

	case "leader-crash":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioLeaderCrash(t, baseDir)
		fmt.Printf("SCENARIO RESULT: leader-crash passed with full convergence\n")
	case "follower-crash":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioFollowerCrash(t, baseDir)
		fmt.Printf("SCENARIO RESULT: follower-crash passed with full convergence\n")
	case "minority-partition":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioMinorityPartition(t, baseDir)
		fmt.Printf("SCENARIO RESULT: minority-partition passed with full convergence\n")
	case "majority-partition":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioMajorityPartitionHeals(t, baseDir)
		fmt.Printf("SCENARIO RESULT: majority-partition passed with full convergence\n")
	case "repeated-elections":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioRepeatedElections(t, baseDir)
		fmt.Printf("SCENARIO RESULT: repeated-elections passed with full convergence\n")
	case "slow-follower-catchup":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioSlowFollower_Catchup(t, baseDir)
		fmt.Printf("SCENARIO RESULT: slow-follower-catchup passed with full convergence\n")
	case "slow-follower-timeout":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioSlowFollower_RPCTimeout(t, baseDir)
		fmt.Printf("SCENARIO RESULT: slow-follower-timeout passed with full convergence\n")
	case "storage-failure":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioStorageFailureFailClosed(t, baseDir)
		fmt.Printf("SCENARIO RESULT: storage-failure passed with fail-closed confirmation\n")
	case "mutex-safety":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioMutexNetworkIOSafety(t, baseDir)
		fmt.Printf("SCENARIO RESULT: mutex-safety passed with concurrent lock acquisition confirmed\n")
	case "rolling-crash":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioRollingCrash(t, baseDir)
		fmt.Printf("SCENARIO RESULT: rolling-crash passed with full convergence\n")
	case "process-crash":
		t := &testingAdapter{logger: logger}
		chaos.RunScenarioProcessCrashRecovery(t, baseDir)
		fmt.Printf("SCENARIO RESULT: process-crash passed with monotonic bootIDs and convergence\n")
	default:
		logger.Error("unknown scenario", "scenario", *scenario)
		os.Exit(1)
	}
}

func runCLIFuzzer(f *chaos.Fuzzer, duration time.Duration, logger *slog.Logger) chaos.FuzzerStats {
	t := &testingAdapter{logger: logger}
	return f.Run(t)
}

type testingAdapter struct {
	logger *slog.Logger
}

func (a *testingAdapter) Helper() {}
func (a *testingAdapter) Logf(format string, args ...any) {
	a.logger.Info(fmt.Sprintf(format, args...))
}
func (a *testingAdapter) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	a.logger.Error("FATAL INVARIANT VIOLATION: " + msg)
	panic("CHAOS FAILURE: " + msg)
}
func (a *testingAdapter) Fatal(args ...any) {
	msg := fmt.Sprint(args...)
	a.logger.Error("FATAL ERROR: " + msg)
	panic("CHAOS FAILURE: " + msg)
}
