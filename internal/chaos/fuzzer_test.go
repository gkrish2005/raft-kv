package chaos

import (
	"testing"
	"time"
)

// Test 51: Seeded chaos fuzzer integration test (short duration for CI test suite)
func TestFuzzer_SeededRun(t *testing.T) {
	baseDir := t.TempDir()
	seed := int64(42)

	cfg := FuzzerConfig{
		Seed:        seed,
		Duration:    8 * time.Second,
		RequestRate: 20,
		NodeCount:   3,
		BaseDir:     baseDir,
	}

	fuzzer, err := NewFuzzer(cfg)
	if err != nil {
		t.Fatalf("create fuzzer: %v", err)
	}

	stats := fuzzer.Run(t)
	t.Logf("fuzzer completed with seed %d: writes attempted=%d, confirmed=%d, faults injected=%d, cycles=%d",
		stats.Seed, stats.TotalWritesAttempted, stats.TotalWritesConfirmed, stats.FaultsInjected, stats.CyclesCompleted)

	if stats.FaultsInjected == 0 {
		t.Fatalf("expected faults to be injected during fuzzer run")
	}
}
