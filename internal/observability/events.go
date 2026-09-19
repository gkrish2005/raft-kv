package observability

import (
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"time"
)

const ClusterEventSchemaVersion uint32 = 1

type EventType string

const (
	ElectionStarted   EventType = "ELECTION_STARTED"
	VoteGranted       EventType = "VOTE_GRANTED"
	VoteRejected      EventType = "VOTE_REJECTED"
	LeaderElected     EventType = "LEADER_ELECTED"
	LeaderSteppedDown EventType = "LEADER_STEPPED_DOWN"
	TermAdvanced      EventType = "TERM_ADVANCED"
	RPCFailed         EventType = "RPC_FAILED"
	RPCSucceeded      EventType = "RPC_SUCCEEDED"
	LogAppended       EventType = "LOG_APPENDED"
	LogConflict       EventType = "LOG_CONFLICT"
	CommitAdvanced    EventType = "COMMIT_ADVANCED"
	EntryApplied      EventType = "ENTRY_APPLIED"
	NodeStarted       EventType = "NODE_STARTED"
	NodeStopped       EventType = "NODE_STOPPED"
	NodeRestarted     EventType = "NODE_RESTARTED"
	PartitionCreated  EventType = "PARTITION_CREATED"
	PartitionHealed   EventType = "PARTITION_HEALED"
)

// ClusterEvent defines the typed structured telemetry record per docs/architecture.md.
type ClusterEvent struct {
	SchemaVersion uint32            `json:"schema_version"`
	EventID       string            `json:"event_id"`                 // format: "<NodeID>/boot-<BootID>/%05d"
	Sequence      uint64            `json:"sequence"`                 // strictly increasing within single boot
	Timestamp     time.Time         `json:"timestamp"`                // node-local wall clock (or logical time in tests)
	NodeID        string            `json:"node_id"`
	PeerID        string            `json:"peer_id,omitempty"`        // remote node this event concerns
	CorrelationID string            `json:"correlation_id,omitempty"` // wire-level correlation for matching RPC pairs
	Term          uint64            `json:"term"`
	LogIndex      uint64            `json:"log_index"`
	Type          EventType         `json:"type"`
	Fields        map[string]string `json:"fields,omitempty"`
}

// EventSink is the one-way egress interface implemented by observability sinks
// and called by consensus/storage/chaos layers. Observability never imports Raft types.
type EventSink interface {
	Emit(event ClusterEvent)
}

// Required fields per EventType per docs/architecture.md
var requiredFields = map[EventType][]string{
	RPCFailed:        {"rpc_type", "peer", "error_class"},
	RPCSucceeded:     {"rpc_type", "peer"},
	LogConflict:      {"peer", "index", "term"},
	LeaderElected:    {"leader"},
	LeaderSteppedDown: {"reason"},
	TermAdvanced:     {"old_term", "new_term"},
	CommitAdvanced:   {"old_index", "new_index"},
	VoteGranted:      {"candidate"},
	VoteRejected:     {"candidate"},
	PartitionCreated: {"peers"},
	PartitionHealed:  {"peers"},
}

// ValidateEvent checks schema version, field bounds, required fields, and rejects invalid node-bearing fields.
func ValidateEvent(e ClusterEvent) error {
	if e.SchemaVersion != ClusterEventSchemaVersion {
		return fmt.Errorf("invalid schema version %d, expected %d", e.SchemaVersion, ClusterEventSchemaVersion)
	}
	if e.EventID == "" {
		return errors.New("event_id must not be empty")
	}
	if e.NodeID == "" {
		return errors.New("node_id must not be empty")
	}
	if e.Type == "" {
		return errors.New("event type must not be empty")
	}

	// Defensive bounds: max 10 fields, max 64 bytes per key, max 512 bytes per value
	if len(e.Fields) > 10 {
		return fmt.Errorf("fields count %d exceeds maximum of 10", len(e.Fields))
	}
	for k, v := range e.Fields {
		if len(k) > 64 {
			return fmt.Errorf("field key %q exceeds maximum of 64 bytes", k)
		}
		if len(v) > 512 {
			return fmt.Errorf("field value for %q exceeds maximum of 512 bytes", k)
		}
		// Strict rejection of generic "target" field: only specific semantic node-bearing fields allowed
		if k == "target" {
			return errors.New("generic 'target' field is strictly disallowed; use 'peer', 'candidate', 'leader', or 'peers'")
		}
	}

	// Validate required fields
	if reqs, ok := requiredFields[e.Type]; ok {
		for _, req := range reqs {
			if val, exists := e.Fields[req]; !exists || strings.TrimSpace(val) == "" {
				return fmt.Errorf("event %s missing required field %q", e.Type, req)
			}
		}
	}

	return nil
}

// EventEmitter is a node-local helper for constructing and emitting ClusterEvents
// with strictly increasing sequence numbers, format-compliant EventIDs, and durable BootIDs.
type EventEmitter struct {
	nodeID   string
	bootID   uint64
	sequence atomic.Uint64
	sink     EventSink
	nowFunc  func() time.Time
}

// NewEventEmitter creates a new EventEmitter for a specific node and boot session.
// No events may be emitted until bootID has been durably persisted (I-020).
func NewEventEmitter(nodeID string, bootID uint64, sink EventSink, nowFunc func() time.Time) *EventEmitter {
	if sink == nil {
		sink = &NopSink{}
	}
	if nowFunc == nil {
		nowFunc = time.Now
	}
	return &EventEmitter{
		nodeID:  nodeID,
		bootID:  bootID,
		sink:    sink,
		nowFunc: nowFunc,
	}
}

// Emit creates and emits a ClusterEvent into the configured EventSink.
func (ee *EventEmitter) Emit(
	eventType EventType,
	peerID string,
	correlationID string,
	term uint64,
	logIndex uint64,
	fields map[string]string,
) ClusterEvent {
	if ee == nil || ee.sink == nil {
		return ClusterEvent{}
	}

	seq := ee.sequence.Add(1)
	eventID := fmt.Sprintf("%s/boot-%d/%05d", ee.nodeID, ee.bootID, seq)

	event := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       eventID,
		Sequence:      seq,
		Timestamp:     ee.nowFunc(),
		NodeID:        ee.nodeID,
		PeerID:        peerID,
		CorrelationID: correlationID,
		Term:          term,
		LogIndex:      logIndex,
		Type:          eventType,
		Fields:        fields,
	}

	ee.sink.Emit(event)
	return event
}
