package observability

import (
	"strings"
	"testing"
	"time"
)

func TestValidateEvent_Valid(t *testing.T) {
	e := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       "node-1/boot-1/00001",
		Sequence:      1,
		Timestamp:     time.Now(),
		NodeID:        "node-1",
		PeerID:        "node-2",
		CorrelationID: "corr-123",
		Term:          1,
		LogIndex:      5,
		Type:          RPCFailed,
		Fields: map[string]string{
			"rpc_type":    "AppendEntries",
			"peer":        "node-2",
			"error_class": "timeout",
		},
	}

	if err := ValidateEvent(e); err != nil {
		t.Fatalf("expected valid event, got error: %v", err)
	}
}

func TestValidateEvent_InvalidSchemaVersion(t *testing.T) {
	e := ClusterEvent{
		SchemaVersion: 999,
		EventID:       "node-1/boot-1/00001",
		NodeID:        "node-1",
		Type:          ElectionStarted,
	}
	if err := ValidateEvent(e); err == nil {
		t.Fatalf("expected error for invalid schema version, got nil")
	}
}

func TestValidateEvent_RequiredFields(t *testing.T) {
	cases := []struct {
		eventType     EventType
		missingFields map[string]string
		validFields   map[string]string
	}{
		{
			eventType:     RPCFailed,
			missingFields: map[string]string{"rpc_type": "AppendEntries", "peer": "node-2"}, // missing error_class
			validFields:   map[string]string{"rpc_type": "AppendEntries", "peer": "node-2", "error_class": "io"},
		},
		{
			eventType:     RPCSucceeded,
			missingFields: map[string]string{"rpc_type": "AppendEntries"}, // missing peer
			validFields:   map[string]string{"rpc_type": "AppendEntries", "peer": "node-2"},
		},
		{
			eventType:     LogConflict,
			missingFields: map[string]string{"peer": "node-2", "index": "5"}, // missing term
			validFields:   map[string]string{"peer": "node-2", "index": "5", "term": "2"},
		},
		{
			eventType:     LeaderElected,
			missingFields: map[string]string{}, // missing leader
			validFields:   map[string]string{"leader": "node-1"},
		},
		{
			eventType:     LeaderSteppedDown,
			missingFields: map[string]string{}, // missing reason
			validFields:   map[string]string{"reason": "higher_term"},
		},
		{
			eventType:     TermAdvanced,
			missingFields: map[string]string{"old_term": "1"}, // missing new_term
			validFields:   map[string]string{"old_term": "1", "new_term": "2"},
		},
		{
			eventType:     CommitAdvanced,
			missingFields: map[string]string{"old_index": "3"}, // missing new_index
			validFields:   map[string]string{"old_index": "3", "new_index": "4"},
		},
		{
			eventType:     VoteGranted,
			missingFields: map[string]string{}, // missing candidate
			validFields:   map[string]string{"candidate": "node-2"},
		},
		{
			eventType:     VoteRejected,
			missingFields: map[string]string{}, // missing candidate
			validFields:   map[string]string{"candidate": "node-2"},
		},
		{
			eventType:     PartitionCreated,
			missingFields: map[string]string{}, // missing peers
			validFields:   map[string]string{"peers": "node-2,node-3"},
		},
		{
			eventType:     PartitionHealed,
			missingFields: map[string]string{}, // missing peers
			validFields:   map[string]string{"peers": "node-2,node-3"},
		},
	}

	for _, tc := range cases {
		eMissing := ClusterEvent{
			SchemaVersion: ClusterEventSchemaVersion,
			EventID:       "node-1/boot-1/00001",
			NodeID:        "node-1",
			Type:          tc.eventType,
			Fields:        tc.missingFields,
		}
		if err := ValidateEvent(eMissing); err == nil {
			t.Errorf("expected error for missing required fields on %s, got nil", tc.eventType)
		}

		eValid := ClusterEvent{
			SchemaVersion: ClusterEventSchemaVersion,
			EventID:       "node-1/boot-1/00001",
			NodeID:        "node-1",
			Type:          tc.eventType,
			Fields:        tc.validFields,
		}
		if err := ValidateEvent(eValid); err != nil {
			t.Errorf("expected valid event for %s, got error: %v", tc.eventType, err)
		}
	}
}

func TestValidateEvent_TargetFieldStrictlyDisallowed(t *testing.T) {
	e := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       "node-1/boot-1/00001",
		NodeID:        "node-1",
		Type:          ElectionStarted,
		Fields: map[string]string{
			"target": "node-2", // Generic target field must be strictly rejected
		},
	}
	err := ValidateEvent(e)
	if err == nil || !strings.Contains(err.Error(), "target") {
		t.Fatalf("expected error rejecting generic 'target' field, got: %v", err)
	}
}

func TestValidateEvent_DefensiveBounds(t *testing.T) {
	// Exceeding 10 fields
	tooManyFields := make(map[string]string)
	for i := 0; i < 11; i++ {
		tooManyFields[string(rune('a'+i))] = "val"
	}
	e1 := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       "node-1/boot-1/00001",
		NodeID:        "node-1",
		Type:          ElectionStarted,
		Fields:        tooManyFields,
	}
	if err := ValidateEvent(e1); err == nil {
		t.Errorf("expected error for >10 fields, got nil")
	}

	// Key exceeding 64 bytes
	longKey := strings.Repeat("k", 65)
	e2 := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       "node-1/boot-1/00001",
		NodeID:        "node-1",
		Type:          ElectionStarted,
		Fields:        map[string]string{longKey: "val"},
	}
	if err := ValidateEvent(e2); err == nil {
		t.Errorf("expected error for key >64 bytes, got nil")
	}

	// Value exceeding 512 bytes
	longVal := strings.Repeat("v", 513)
	e3 := ClusterEvent{
		SchemaVersion: ClusterEventSchemaVersion,
		EventID:       "node-1/boot-1/00001",
		NodeID:        "node-1",
		Type:          ElectionStarted,
		Fields:        map[string]string{"key": longVal},
	}
	if err := ValidateEvent(e3); err == nil {
		t.Errorf("expected error for value >512 bytes, got nil")
	}
}

func TestEventEmitter_SequenceAndFormat(t *testing.T) {
	recorder := NewScenarioRecorder()
	fixedTime := time.Date(2026, 9, 19, 12, 0, 0, 0, time.UTC)
	ee := NewEventEmitter("node-A", 7, recorder, func() time.Time { return fixedTime })

	e1 := ee.Emit(ElectionStarted, "", "", 1, 0, nil)
	e2 := ee.Emit(LeaderElected, "", "", 1, 0, map[string]string{"leader": "node-A"})

	if e1.EventID != "node-A/boot-7/00001" {
		t.Errorf("expected e1 EventID node-A/boot-7/00001, got %s", e1.EventID)
	}
	if e1.Sequence != 1 {
		t.Errorf("expected e1 Sequence 1, got %d", e1.Sequence)
	}

	if e2.EventID != "node-A/boot-7/00002" {
		t.Errorf("expected e2 EventID node-A/boot-7/00002, got %s", e2.EventID)
	}
	if e2.Sequence != 2 {
		t.Errorf("expected e2 Sequence 2, got %d", e2.Sequence)
	}

	if recorder.Count() != 2 {
		t.Fatalf("expected recorder to have 2 events, got %d", recorder.Count())
	}
}
