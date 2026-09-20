package ai_test

import (
	"strings"
	"testing"
	"time"

	"raftkv/internal/ai"
	"raftkv/internal/observability"
)

func TestEnumValidation(t *testing.T) {
	// IncidentType
	validTypes := []ai.IncidentType{
		ai.LeaderInstability, ai.NodeUnreachable, ai.ReplicationLag,
		ai.ElectionStorm, ai.SlowFollower, ai.NetworkPartition,
	}
	for _, it := range validTypes {
		if !ai.IsValidIncidentType(it) {
			t.Errorf("expected %q to be valid IncidentType", it)
		}
	}
	if ai.IsValidIncidentType("UNKNOWN_INCIDENT") || ai.IsValidIncidentType("") {
		t.Errorf("expected invalid IncidentType to be rejected")
	}

	// Severity
	validSeverities := []ai.Severity{ai.Low, ai.Medium, ai.High, ai.Critical}
	for _, s := range validSeverities {
		if !ai.IsValidSeverity(s) {
			t.Errorf("expected %q to be valid Severity", s)
		}
	}
	if ai.IsValidSeverity("SUPER_HIGH") || ai.IsValidSeverity("") {
		t.Errorf("expected invalid Severity to be rejected")
	}

	// ClaimType
	if !ai.IsValidClaimType(ai.Observation) || !ai.IsValidClaimType(ai.Inference) {
		t.Errorf("expected Observation and Inference to be valid ClaimType")
	}
	if ai.IsValidClaimType("FACT") || ai.IsValidClaimType("") {
		t.Errorf("expected invalid ClaimType to be rejected")
	}

	// ConsistencyImpact: service impact only, root-cause strings rejected
	validImpacts := []ai.ConsistencyImpact{
		ai.None, ai.WritesUnavailable, ai.ReadsUnavailable, ai.CommitProgressBlocked,
	}
	for _, ci := range validImpacts {
		if !ai.IsValidConsistencyImpact(ci) {
			t.Errorf("expected %q to be valid ConsistencyImpact", ci)
		}
	}

	// Critical check: root cause strings (e.g. "NETWORK_PARTITION") must be rejected
	invalidImpacts := []ai.ConsistencyImpact{
		"NETWORK_PARTITION", "LEADER_CRASH", "SLOW_DISK", "SPLIT_BRAIN", "",
	}
	for _, inv := range invalidImpacts {
		if ai.IsValidConsistencyImpact(inv) {
			t.Errorf("expected root-cause or invalid ConsistencyImpact %q to be rejected", inv)
		}
	}
}

func TestFormatIncident_RequiredNotices(t *testing.T) {
	inc := &ai.AIIncident{
		IncidentID:        "inc-001",
		DetectedAt:        time.Now(),
		IncidentType:      ai.LeaderInstability,
		Severity:          ai.High,
		AffectedNodes:     []string{"node-1", "node-2"},
		ConsistencyImpact: ai.WritesUnavailable,
		Confidence:        0.90,
		Source:            ai.RuleEngine,
		Claims: []ai.DiagnosisClaim{
			{
				Claim:       "Leadership changed 3 times within 60s",
				EvidenceIDs: []string{"ev-1"},
				ClaimType:   ai.Observation,
			},
		},
		Evidence: []ai.EvidenceRef{
			{
				EventID: "ev-1",
				Event: observability.ClusterEvent{
					Type:   observability.LeaderElected,
					NodeID: "node-1",
				},
			},
		},
		RecommendedActions: []string{
			"Inspect heartbeat network latency",
		},
	}

	formatted := ai.FormatIncident(inc)

	// Verify Confidence notice is present
	if !strings.Contains(formatted, ai.ConfidenceNotice) {
		t.Errorf("formatted incident missing ConfidenceNotice")
	}

	// Verify RecommendedActions notice is present
	if !strings.Contains(formatted, ai.RecommendedActionsNotice) {
		t.Errorf("formatted incident missing RecommendedActionsNotice")
	}
}
