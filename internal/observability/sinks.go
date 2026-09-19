package observability

import (
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
)

// NopSink is a no-op implementation of EventSink.
type NopSink struct{}

func (s *NopSink) Emit(event ClusterEvent) {}

// LiveBuffer is an in-memory bounded ring buffer for live operator inspection
// and debug CLI. It is explicitly lossy by design under sustained load: when capacity
// is reached, oldest events are overwritten and an atomic drop counter is incremented.
type LiveBuffer struct {
	mu       sync.RWMutex
	capacity int
	events   []ClusterEvent
	head     int // points to the next write position
	count    int // number of events currently in buffer (up to capacity)
	dropped  atomic.Uint64
}

// NewLiveBuffer creates a LiveBuffer with the given capacity.
// Defaults to 10,000 events if capacity <= 0.
func NewLiveBuffer(capacity int) *LiveBuffer {
	if capacity <= 0 {
		capacity = 10000
	}
	return &LiveBuffer{
		capacity: capacity,
		events:   make([]ClusterEvent, capacity),
	}
}

// Emit appends an event to the ring buffer, overwriting the oldest event
// and incrementing the dropped counter if the buffer is at capacity.
func (b *LiveBuffer) Emit(event ClusterEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.count == b.capacity {
		// Overwriting oldest event
		b.dropped.Add(1)
	} else {
		b.count++
	}

	b.events[b.head] = event
	b.head = (b.head + 1) % b.capacity
}

// Dropped returns the cumulative number of events dropped due to buffer saturation.
func (b *LiveBuffer) Dropped() uint64 {
	return b.dropped.Load()
}

// Snapshot returns a chronological copy of the events currently in the buffer.
func (b *LiveBuffer) Snapshot() []ClusterEvent {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if b.count == 0 {
		return []ClusterEvent{}
	}

	out := make([]ClusterEvent, b.count)
	if b.count < b.capacity {
		// Not yet wrapped: events are from 0 to b.count-1
		copy(out, b.events[:b.count])
	} else {
		// Wrapped: oldest event is at b.head, newest is at (b.head-1+capacity)%capacity
		firstPart := b.capacity - b.head
		copy(out[:firstPart], b.events[b.head:])
		copy(out[firstPart:], b.events[:b.head])
	}
	return out
}

// Clear resets the buffer state.
func (b *LiveBuffer) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.head = 0
	b.count = 0
	b.dropped.Store(0)
}

// ScenarioRecorder captures a complete, lossless event stream for the duration
// of a test or chaos scenario, exporting it as a JSON fixture for replay and AI evaluation.
type ScenarioRecorder struct {
	mu     sync.RWMutex
	events []ClusterEvent
}

// NewScenarioRecorder creates an empty ScenarioRecorder.
func NewScenarioRecorder() *ScenarioRecorder {
	return &ScenarioRecorder{
		events: make([]ClusterEvent, 0, 1024),
	}
}

// Emit appends the event to the recorder without loss.
func (r *ScenarioRecorder) Emit(event ClusterEvent) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, event)
}

// Events returns a slice copy of all events captured by the recorder.
func (r *ScenarioRecorder) Events() []ClusterEvent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]ClusterEvent, len(r.events))
	copy(out, r.events)
	return out
}

// Count returns the number of events captured.
func (r *ScenarioRecorder) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.events)
}

// Clear resets the recorded events.
func (r *ScenarioRecorder) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = r.events[:0]
}

// SaveToJSON writes all recorded events to the specified file path formatted as JSON.
func (r *ScenarioRecorder) SaveToJSON(filePath string) error {
	r.mu.RLock()
	eventsCopy := make([]ClusterEvent, len(r.events))
	copy(eventsCopy, r.events)
	r.mu.RUnlock()

	data, err := json.MarshalIndent(eventsCopy, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal events to JSON: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write events to %s: %w", filePath, err)
	}
	return nil
}

// LoadFromJSON reads and unmarshals a JSON fixture of ClusterEvents.
func LoadFromJSON(filePath string) ([]ClusterEvent, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read fixture file %s: %w", filePath, err)
	}

	var events []ClusterEvent
	if err := json.Unmarshal(data, &events); err != nil {
		return nil, fmt.Errorf("failed to unmarshal fixture JSON: %w", err)
	}
	return events, nil
}

// MultiSink dispatches every emitted event to multiple sinks.
// It wraps each sub-sink in a panic-recovery boundary (Rule 32/24) so that
// any failure or panic inside a sink is dropped rather than propagated to consensus.
type MultiSink struct {
	mu           sync.RWMutex
	sinks        []EventSink
	panicCounter atomic.Uint64
}

// NewMultiSink creates a MultiSink wrapping the provided sinks.
func NewMultiSink(sinks ...EventSink) *MultiSink {
	valid := make([]EventSink, 0, len(sinks))
	for _, s := range sinks {
		if s != nil {
			valid = append(valid, s)
		}
	}
	return &MultiSink{sinks: valid}
}

// AddSink adds a new sink to the MultiSink dispatch list.
func (m *MultiSink) AddSink(sink EventSink) {
	if sink == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.sinks = append(m.sinks, sink)
}

// Emit broadcasts the event to all configured sinks with panic isolation.
func (m *MultiSink) Emit(event ClusterEvent) {
	m.mu.RLock()
	sinks := m.sinks
	m.mu.RUnlock()

	for _, s := range sinks {
		func(sink EventSink) {
			defer func() {
				if r := recover(); r != nil {
					m.panicCounter.Add(1)
				}
			}()
			sink.Emit(event)
		}(s)
	}
}

// PanicCount returns the number of recovered panics from sub-sinks.
func (m *MultiSink) PanicCount() uint64 {
	return m.panicCounter.Load()
}
