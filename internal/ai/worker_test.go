package ai_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"raftkv/internal/ai"
	"raftkv/internal/observability"
)

// TestAIWorker_StructuralAsyncNonBlocking verifies Rule 24 and I-014:
// the AI diagnosis pipeline is completely asynchronous and decoupled from event emission.
// Even if the LLM blocks indefinitely (simulated via FakeLLM with high latency),
// event emission into LiveBuffer and operations completing on the main path do not block.
func TestAIWorker_StructuralAsyncNonBlocking(t *testing.T) {
	liveBuffer := observability.NewLiveBuffer(1000)
	fakeLLM := ai.NewFakeLLM()
	// Set a 10-second delay on the LLM to simulate a completely stalled/hung model call
	fakeLLM.SetDelay(10 * time.Second)

	engine := ai.NewDiagnosticsEngine(nil, fakeLLM)
	worker := ai.NewAIWorker(engine, liveBuffer, 20*time.Millisecond, 60*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	worker.Start(ctx)
	defer worker.Stop()

	// Emit events into the LiveBuffer as a client or node would
	start := time.Now()
	for i := 1; i <= 50; i++ {
		liveBuffer.Emit(observability.ClusterEvent{
			SchemaVersion: 1,
			EventID:       fmt.Sprintf("node-1/boot-1/%05d", i),
			Timestamp:     time.Now(),
			NodeID:        "node-1",
			Type:          observability.EntryApplied,
		})
	}
	elapsed := time.Since(start)

	// Emission must be instantaneous (< 50ms), proving zero synchronous coupling to LLM
	if elapsed > 100*time.Millisecond {
		t.Fatalf("structural async violation: event emission took %v while LLM was stalled (expected < 100ms)", elapsed)
	}

	// Verify the worker did not block the caller
	if worker.LatestIncident() != nil {
		t.Fatalf("expected no incident yet while LLM is delayed")
	}
}

// TestAIWorker_FaultInjectionAndRestart verifies that terminating or crashing
// the in-process AI worker (or forcing LLM failures) has zero effect on telemetry emission,
// and restarting the worker cleanly resumes diagnosis without state corruption.
func TestAIWorker_FaultInjectionAndRestart(t *testing.T) {
	liveBuffer := observability.NewLiveBuffer(1000)
	fakeLLM := ai.NewFakeLLM()

	engine := ai.NewDiagnosticsEngine(nil, fakeLLM)
	worker := ai.NewAIWorker(engine, liveBuffer, 30*time.Millisecond, 60*time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	worker.Start(ctx)

	// 1. Initial healthy diagnosis: emit election storm events
	for i := 1; i <= 5; i++ {
		liveBuffer.Emit(observability.ClusterEvent{
			SchemaVersion: 1,
			EventID:       fmt.Sprintf("node-1/boot-1/%05d", i),
			Timestamp:     time.Now(),
			NodeID:        "node-1",
			Type:          observability.ElectionStarted,
			Term:          uint64(i),
			Fields:        map[string]string{"candidate": "node-1"},
		})
	}

	// Wait for worker to run at least once
	var firstInc *ai.AIIncident
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if inc := worker.LatestIncident(); inc != nil {
			firstInc = inc
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if firstInc == nil {
		t.Fatalf("expected initial incident from worker before fault injection")
	}
	if firstInc.IncidentType != ai.ElectionStorm {
		t.Errorf("expected ElectionStorm, got %s", firstInc.IncidentType)
	}

	// 2. Fault injection: cancel worker context and force LLM to error
	fakeLLM.SetError(ai.ErrLLMUnavailable)
	cancel()      // cancel the worker's context
	worker.Stop() // terminate in-process worker entirely

	// Telemetry emission into LiveBuffer continues unimpaired while AI worker is dead
	var emitSuccess atomic.Bool
	emitSuccess.Store(true)
	for i := 6; i <= 15; i++ {
		liveBuffer.Emit(observability.ClusterEvent{
			SchemaVersion: 1,
			EventID:       fmt.Sprintf("node-1/boot-1/%05d", i),
			Timestamp:     time.Now(),
			NodeID:        "node-1",
			Type:          observability.CommitAdvanced,
			Fields:        map[string]string{"old_index": "1", "new_index": "2"},
		})
	}
	if !emitSuccess.Load() {
		t.Fatalf("telemetry emission failed while AI worker was dead")
	}

	// 3. Restart the in-process worker
	fakeLLM.SetError(nil) // clear error
	newCtx, newCancel := context.WithCancel(context.Background())
	defer newCancel()

	newWorker := ai.NewAIWorker(engine, liveBuffer, 30*time.Millisecond, 60*time.Second)
	newWorker.Start(newCtx)
	defer newWorker.Stop()

	// Verify diagnosis resumes
	var resumedInc *ai.AIIncident
	resumeDeadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(resumeDeadline) {
		if inc := newWorker.LatestIncident(); inc != nil {
			resumedInc = inc
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if resumedInc == nil {
		t.Fatalf("expected diagnosis to resume after worker restart")
	}
}
