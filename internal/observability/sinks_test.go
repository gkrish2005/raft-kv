package observability

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

type panickingSink struct{}

func (s *panickingSink) Emit(e ClusterEvent) {
	panic("deliberate sink failure")
}

func TestDualSinks_DifferentialBurst(t *testing.T) {
	const capacity = 1000
	const totalEvents = 25000

	liveBuffer := NewLiveBuffer(capacity)
	recorder := NewScenarioRecorder()
	multiSink := NewMultiSink(liveBuffer, recorder)

	ee := NewEventEmitter("node-1", 1, multiSink, time.Now)

	// Concurrently emit 25,000 events to stress thread safety and buffer boundaries
	var wg sync.WaitGroup
	workers := 10
	eventsPerWorker := totalEvents / workers

	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			for i := 0; i < eventsPerWorker; i++ {
				ee.Emit(LogAppended, "", "", 1, uint64(i), nil)
			}
		}(w)
	}
	wg.Wait()

	// Verify LiveBuffer dropped exactly totalEvents - capacity
	expectedDropped := uint64(totalEvents - capacity)
	if liveBuffer.Dropped() != expectedDropped {
		t.Errorf("expected LiveBuffer dropped %d, got %d", expectedDropped, liveBuffer.Dropped())
	}

	snapshot := liveBuffer.Snapshot()
	if len(snapshot) != capacity {
		t.Errorf("expected LiveBuffer snapshot length %d, got %d", capacity, len(snapshot))
	}

	// Verify ScenarioRecorder captured ALL events without loss
	if recorder.Count() != totalEvents {
		t.Errorf("expected ScenarioRecorder count %d, got %d", totalEvents, recorder.Count())
	}
}

func TestScenarioRecorder_SaveAndLoadJSON(t *testing.T) {
	recorder := NewScenarioRecorder()
	ee := NewEventEmitter("node-1", 1, recorder, time.Now)

	ee.Emit(ElectionStarted, "", "", 1, 0, nil)
	ee.Emit(LeaderElected, "", "", 1, 0, map[string]string{"leader": "node-1"})
	ee.Emit(RPCFailed, "node-2", "corr-1", 1, 0, map[string]string{
		"rpc_type":    "AppendEntries",
		"peer":        "node-2",
		"error_class": "timeout",
	})

	tmpDir := t.TempDir()
	filePath := filepath.Join(tmpDir, "events.json")

	if err := recorder.SaveToJSON(filePath); err != nil {
		t.Fatalf("failed to save events to JSON: %v", err)
	}

	loaded, err := LoadFromJSON(filePath)
	if err != nil {
		t.Fatalf("failed to load events from JSON: %v", err)
	}

	if len(loaded) != 3 {
		t.Fatalf("expected 3 loaded events, got %d", len(loaded))
	}

	if loaded[0].Type != ElectionStarted || loaded[1].Type != LeaderElected || loaded[2].Type != RPCFailed {
		t.Errorf("loaded events type mismatch: %+v", loaded)
	}
	if loaded[2].Fields["error_class"] != "timeout" {
		t.Errorf("expected error_class timeout, got %s", loaded[2].Fields["error_class"])
	}
}

func TestMultiSink_PanicRecovery(t *testing.T) {
	recorder := NewScenarioRecorder()
	badSink := &panickingSink{}
	multiSink := NewMultiSink(badSink, recorder)

	e := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       "node-1/boot-1/00001",
		NodeID:        "node-1",
		Type:          ElectionStarted,
	}

	// Emit must not panic even though badSink panics
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("MultiSink.Emit panicking: %v", r)
		}
	}()

	multiSink.Emit(e)

	if multiSink.PanicCount() != 1 {
		t.Errorf("expected PanicCount 1, got %d", multiSink.PanicCount())
	}

	// Good sink must still have received the event
	if recorder.Count() != 1 {
		t.Errorf("expected good sink to receive event, got count %d", recorder.Count())
	}
}

func TestLiveBuffer_ChronologicalOrdering(t *testing.T) {
	const capacity = 5
	buf := NewLiveBuffer(capacity)

	for i := 1; i <= 8; i++ {
		buf.Emit(ClusterEvent{
			EventID:  fmt.Sprintf("e-%d", i),
			Sequence: uint64(i),
			Type:     LogAppended,
		})
	}

	snapshot := buf.Snapshot()
	if len(snapshot) != capacity {
		t.Fatalf("expected %d events, got %d", capacity, len(snapshot))
	}

	// Since capacity is 5 and we emitted 1..8, remaining should be 4, 5, 6, 7, 8
	expectedSeq := []uint64{4, 5, 6, 7, 8}
	for i, exp := range expectedSeq {
		if snapshot[i].Sequence != exp {
			t.Errorf("at index %d: expected sequence %d, got %d", i, exp, snapshot[i].Sequence)
		}
	}
}
