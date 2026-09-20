package ai_test

import (
	"fmt"
	"testing"
	"time"

	"raftkv/internal/ai"
	"raftkv/internal/observability"
)

func TestRules_ElectionStorm(t *testing.T) {
	rule := &ai.ElectionStormRule{}
	now := time.Now()

	// 5 failed election attempts
	var events []observability.ClusterEvent
	for i := 1; i <= 5; i++ {
		events = append(events, observability.ClusterEvent{
			SchemaVersion: 1,
			EventID:       fmt.Sprintf("node-1/boot-1/%05d", i),
			Timestamp:     now.Add(time.Duration(i) * time.Second),
			NodeID:        "node-1",
			Type:          observability.ElectionStarted,
			Term:          uint64(i),
			Fields: map[string]string{
				"candidate": "node-1",
			},
		})
	}

	cand := rule.Evaluate(events, nil)
	if cand == nil {
		t.Fatalf("expected ElectionStorm candidate, got nil")
	}
	if cand.IncidentType != ai.ElectionStorm {
		t.Errorf("expected IncidentType %s, got %s", ai.ElectionStorm, cand.IncidentType)
	}
	if cand.Severity != ai.Critical {
		t.Errorf("expected Severity %s, got %s", ai.Critical, cand.Severity)
	}
	if cand.Confidence != 0.95 {
		t.Errorf("expected Confidence 0.95, got %v", cand.Confidence)
	}
	if cand.ConsistencyImpact != ai.WritesUnavailable {
		t.Errorf("expected ConsistencyImpact %s, got %s", ai.WritesUnavailable, cand.ConsistencyImpact)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(cand, events)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}

	// Below threshold: 4 failed elections then a successful election -> nil
	events = append(events, observability.ClusterEvent{
		SchemaVersion: 1,
		EventID:       "node-1/boot-1/00006",
		Timestamp:     now.Add(6 * time.Second),
		NodeID:        "node-1",
		Type:          observability.LeaderElected,
		Term:          6,
		Fields:        map[string]string{"leader": "node-1"},
	})
	if res := rule.Evaluate(events, nil); res != nil {
		t.Errorf("expected nil after successful leader election, got %v", res)
	}
}

func TestRules_LeaderInstability_Thrash(t *testing.T) {
	rule := &ai.LeaderInstabilityRule{}
	now := time.Now()

	// Thrash: 3 distinct leadership changes within 60s
	var thrashEvents []observability.ClusterEvent
	for i := 1; i <= 3; i++ {
		node := fmt.Sprintf("node-%d", i)
		thrashEvents = append(thrashEvents, observability.ClusterEvent{
			SchemaVersion: 1,
			EventID:       fmt.Sprintf("%s/boot-1/00001", node),
			Timestamp:     now.Add(time.Duration(i*10) * time.Second),
			NodeID:        node,
			Type:          observability.LeaderElected,
			Term:          uint64(i),
			Fields:        map[string]string{"leader": node},
		})
	}

	cand := rule.Evaluate(thrashEvents, nil)
	if cand == nil {
		t.Fatalf("expected LeaderInstability candidate for thrash, got nil")
	}
	if cand.Severity != ai.High {
		t.Errorf("expected High severity for thrash, got %s", cand.Severity)
	}
	if cand.Confidence != 0.90 {
		t.Errorf("expected Confidence 0.90, got %v", cand.Confidence)
	}
	if len(cand.AffectedNodes) != 3 {
		t.Errorf("expected 3 affected nodes, got %v", cand.AffectedNodes)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(cand, thrashEvents)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}
}

func TestRules_LeaderInstability_SingleCrash(t *testing.T) {
	rule := &ai.LeaderInstabilityRule{}
	now := time.Now()

	// Scenario 1: Single leader crash followed by clean re-election: LOW severity
	singleCrashEvents := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			Type:          observability.LeaderSteppedDown,
			Term:          1,
			Fields:        map[string]string{"reason": "crash"},
		},
		{
			SchemaVersion: 1,
			EventID:       "node-2/boot-1/00002",
			Timestamp:     now.Add(100 * time.Millisecond),
			NodeID:        "node-2",
			Type:          observability.LeaderElected,
			Term:          2,
			Fields:        map[string]string{"leader": "node-2"},
		},
	}
	candSingle := rule.Evaluate(singleCrashEvents, nil)
	if candSingle == nil {
		t.Fatalf("expected LeaderInstability candidate for single crash/re-election, got nil")
	}
	if candSingle.Severity != ai.Low {
		t.Errorf("expected Low severity for single leader election, got %s", candSingle.Severity)
	}
	if candSingle.Confidence != 0.85 {
		t.Errorf("expected Confidence 0.85, got %v", candSingle.Confidence)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(candSingle, singleCrashEvents)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}
}

func TestRules_NetworkPartition(t *testing.T) {
	rule := &ai.NetworkPartitionRule{}
	now := time.Now()

	events := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			Type:          observability.PartitionCreated,
			Fields:        map[string]string{"peers": "node-2,node-3"},
		},
	}

	cand := rule.Evaluate(events, nil)
	if cand == nil {
		t.Fatalf("expected NetworkPartition candidate, got nil")
	}
	if cand.IncidentType != ai.NetworkPartition {
		t.Errorf("expected %s, got %s", ai.NetworkPartition, cand.IncidentType)
	}
	if cand.Severity != ai.High {
		t.Errorf("expected High severity, got %s", cand.Severity)
	}
	if cand.Confidence != 0.90 {
		t.Errorf("expected Confidence 0.90, got %v", cand.Confidence)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(cand, events)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}
}

func TestRules_NodeUnreachable(t *testing.T) {
	rule := &ai.NodeUnreachableRule{}
	now := time.Now()

	var events []observability.ClusterEvent
	for i := 1; i <= 3; i++ {
		events = append(events, observability.ClusterEvent{
			SchemaVersion: 1,
			EventID:       fmt.Sprintf("node-1/boot-1/%05d", i),
			Timestamp:     now.Add(time.Duration(i) * time.Second),
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields: map[string]string{
				"rpc_type":    "AppendEntries",
				"peer":        "node-2",
				"error_class": "timeout",
			},
		})
	}

	cand := rule.Evaluate(events, nil)
	if cand == nil {
		t.Fatalf("expected NodeUnreachable candidate, got nil")
	}
	if cand.IncidentType != ai.NodeUnreachable {
		t.Errorf("expected %s, got %s", ai.NodeUnreachable, cand.IncidentType)
	}
	if cand.Severity != ai.Medium {
		t.Errorf("expected Medium severity, got %s", cand.Severity)
	}
	if cand.Confidence != 0.85 {
		t.Errorf("expected Confidence 0.85, got %v", cand.Confidence)
	}
	if len(cand.AffectedNodes) != 1 || cand.AffectedNodes[0] != "node-2" {
		t.Errorf("expected affected node [node-2], got %v", cand.AffectedNodes)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(cand, events)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}
}

func TestRules_SlowFollower(t *testing.T) {
	slowRule := &ai.SlowFollowerRule{}
	now := time.Now()

	// Slow Follower: lag sustained >= 30 seconds
	slowEvents := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00002",
			Timestamp:     now.Add(35 * time.Second),
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
	}

	slowCand := slowRule.Evaluate(slowEvents, nil)
	if slowCand == nil {
		t.Fatalf("expected SlowFollower candidate for >=30s lag, got nil")
	}
	if slowCand.IncidentType != ai.SlowFollower {
		t.Errorf("expected %s, got %s", ai.SlowFollower, slowCand.IncidentType)
	}
	if slowCand.Severity != ai.Medium {
		t.Errorf("expected Medium severity, got %s", slowCand.Severity)
	}
	if slowCand.Confidence != 0.85 {
		t.Errorf("expected Confidence 0.85, got %v", slowCand.Confidence)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(slowCand, slowEvents)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}
}

func TestRules_ReplicationLag(t *testing.T) {
	lagRule := &ai.ReplicationLagRule{}
	slowRule := &ai.SlowFollowerRule{}
	now := time.Now()

	// Replication Lag: lag sustained < 30 seconds
	lagEvents := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00002",
			Timestamp:     now.Add(10 * time.Second),
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
	}

	// Slow rule must NOT fire for < 30s
	if slowCandUnder30 := slowRule.Evaluate(lagEvents, nil); slowCandUnder30 != nil {
		t.Errorf("expected SlowFollower rule to return nil for <30s, got %v", slowCandUnder30)
	}

	// Lag rule should fire
	lagCand := lagRule.Evaluate(lagEvents, nil)
	if lagCand == nil {
		t.Fatalf("expected ReplicationLag candidate for <30s lag, got nil")
	}
	if lagCand.IncidentType != ai.ReplicationLag {
		t.Errorf("expected %s, got %s", ai.ReplicationLag, lagCand.IncidentType)
	}
	if lagCand.Severity != ai.Low {
		t.Errorf("expected Low severity, got %s", lagCand.Severity)
	}
	if lagCand.Confidence != 0.80 {
		t.Errorf("expected Confidence 0.80, got %v", lagCand.Confidence)
	}

	// End-to-end validation: verify candidate passes Validator
	inc, err := ai.CandidateToIncident(lagCand, lagEvents)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass CandidateToIncident validation, got err=%v inc=%v", err, inc)
	}
}

func TestRules_HealthyClusterControl(t *testing.T) {
	engine := ai.NewDeterministicRuleEngine()
	now := time.Now()

	// Simulate 30 minutes of healthy cluster operation:
	// - 1 stable leader elected at start (no further elections)
	// - Periodic successful heartbeats / RPCs
	// - Periodic commit / log append events
	// - 0 errors, 0 partitions, 0 lag
	var events []observability.ClusterEvent
	events = append(events, observability.ClusterEvent{
		SchemaVersion: 1,
		EventID:       "node-1/boot-1/00001",
		Timestamp:     now,
		NodeID:        "node-1",
		Type:          observability.NodeStarted,
	})
	// Initial election at t=0
	events = append(events, observability.ClusterEvent{
		SchemaVersion: 1,
		EventID:       "node-1/boot-1/00002",
		Timestamp:     now.Add(100 * time.Millisecond),
		NodeID:        "node-1",
		Type:          observability.LeaderElected,
		Term:          1,
		Fields:        map[string]string{"leader": "node-1"},
	})

	// 30 minutes of successful RPCs and commits
	// (Check that after initial startup, normal operations generate zero incidents)
	var operationalEvents []observability.ClusterEvent
	for sec := 1; sec <= 30*60; sec += 30 {
		ts := now.Add(time.Duration(sec) * time.Second)
		operationalEvents = append(operationalEvents,
			observability.ClusterEvent{
				SchemaVersion: 1,
				EventID:       fmt.Sprintf("node-1/boot-1/%05d", sec+2),
				Timestamp:     ts,
				NodeID:        "node-1",
				PeerID:        "node-2",
				Type:          observability.RPCSucceeded,
				Fields:        map[string]string{"rpc_type": "AppendEntries", "peer": "node-2"},
			},
			observability.ClusterEvent{
				SchemaVersion: 1,
				EventID:       fmt.Sprintf("node-1/boot-1/%05d", sec+3),
				Timestamp:     ts,
				NodeID:        "node-1",
				Type:          observability.CommitAdvanced,
				Fields:        map[string]string{"old_index": "1", "new_index": "2"},
			},
		)
	}

	cand := engine.Evaluate(operationalEvents, nil)
	if cand != nil {
		t.Fatalf("expected zero incidents on healthy cluster control, got %v (type: %s)", cand, cand.IncidentType)
	}
}

// TestRules_MetricLag_DoesNotCiteUnrelatedEvent verifies Finding 2:
// When metric-based replication lag is observed for node-2, but events only contains
// unrelated events for node-1, the rule must NOT fabricate a citation to events[0]
// that would fail the validator's AffectedNodes grounding check.
func TestRules_MetricLag_DoesNotCiteUnrelatedEvent(t *testing.T) {
	slowRule := &ai.SlowFollowerRule{}
	now := time.Now()

	metrics := []observability.MetricSnapshot{
		{
			SchemaVersion: 1,
			Timestamp:     now,
			NodeID:        "node-1",
			Name:          observability.MetricReplicationLag,
			Value:         50,
			Labels:        map[string]string{"peer": "node-2"},
		},
		{
			SchemaVersion: 1,
			Timestamp:     now.Add(35 * time.Second),
			NodeID:        "node-1",
			Name:          observability.MetricReplicationLag,
			Value:         50,
			Labels:        map[string]string{"peer": "node-2"},
		},
	}

	// Unrelated events: node-1 events only, no mention of node-2
	unrelatedEvents := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			Type:          observability.NodeStarted,
		},
	}

	// Must NOT cite node-1/boot-1/00001 for node-2 lag
	cand := slowRule.Evaluate(unrelatedEvents, metrics)
	if cand != nil {
		for _, eid := range cand.Claims[0].EvidenceIDs {
			if eid == "node-1/boot-1/00001" {
				t.Fatalf("Finding 2 regression: rule cited unrelated event %s for peer node-2", eid)
			}
		}
	}

	// When an event grounding node-2 IS present, it should cite it and pass validation
	groundingEvents := append(unrelatedEvents, observability.ClusterEvent{
		SchemaVersion: 1,
		EventID:       "node-1/boot-1/00002",
		Timestamp:     now.Add(10 * time.Second),
		NodeID:        "node-1",
		PeerID:        "node-2",
		Type:          observability.RPCFailed,
		Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries"},
	})

	groundedCand := slowRule.Evaluate(groundingEvents, metrics)
	if groundedCand == nil {
		t.Fatalf("expected grounded SlowFollower candidate when grounding event is present")
	}
	// Verify it passes full validation
	inc, err := ai.CandidateToIncident(groundedCand, groundingEvents)
	if err != nil || inc == nil {
		t.Fatalf("expected candidate to pass validation, got err=%v inc=%v", err, inc)
	}
}

// TestRules_InitialStartupElection_NotClassifiedAsInstability verifies Finding 3:
// A clean initial startup election at term 1 must not be classified as LEADER_INSTABILITY.
// An instability incident must only fire if preceded by an instability indicator or term > 1.
func TestRules_InitialStartupElection_NotClassifiedAsInstability(t *testing.T) {
	rule := &ai.LeaderInstabilityRule{}
	now := time.Now()

	// Clean initial startup at term 1
	cleanStartupEvents := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			Type:          observability.NodeStarted,
		},
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00002",
			Timestamp:     now.Add(100 * time.Millisecond),
			NodeID:        "node-1",
			Type:          observability.LeaderElected,
			Term:          1,
			Fields:        map[string]string{"leader": "node-1"},
		},
	}

	// Must NOT fire on clean initial startup
	if cand := rule.Evaluate(cleanStartupEvents, nil); cand != nil {
		t.Fatalf("Finding 3 regression: clean initial startup at term 1 classified as instability: %v", cand)
	}

	// Case 2: Election preceded by LEADER_STEPPED_DOWN -> must fire
	instabilityEvents := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "node-1/boot-1/00001",
			Timestamp:     now,
			NodeID:        "node-1",
			Type:          observability.LeaderSteppedDown,
			Term:          1,
			Fields:        map[string]string{"reason": "heartbeat_timeout"},
		},
		{
			SchemaVersion: 1,
			EventID:       "node-2/boot-1/00002",
			Timestamp:     now.Add(200 * time.Millisecond),
			NodeID:        "node-2",
			Type:          observability.LeaderElected,
			Term:          2,
			Fields:        map[string]string{"leader": "node-2"},
		},
	}

	cand := rule.Evaluate(instabilityEvents, nil)
	if cand == nil {
		t.Fatalf("expected LeaderInstability candidate when preceded by step down")
	}
	if cand.IncidentType != ai.LeaderInstability || cand.Severity != ai.Low {
		t.Errorf("expected LeaderInstability Low, got %s %s", cand.IncidentType, cand.Severity)
	}
}

