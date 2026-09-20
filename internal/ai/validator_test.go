package ai_test

import (
	"math"
	"strings"
	"testing"

	"raftkv/internal/ai"
	"raftkv/internal/observability"
)

func TestValidator_IngestSanitization(t *testing.T) {
	events := []observability.ClusterEvent{
		{SchemaVersion: 1, EventID: "e-valid", NodeID: "n1", Type: observability.ElectionStarted},
		{SchemaVersion: 999, EventID: "e-invalid-version", NodeID: "n1", Type: observability.ElectionStarted},
	}
	metrics := []observability.MetricSnapshot{
		{SchemaVersion: 1, Name: "valid_metric", Value: 42.0},
		{SchemaVersion: 2, Name: "invalid_schema", Value: 42.0},
		{SchemaVersion: 1, Name: "nan_metric", Value: math.NaN()},
		{SchemaVersion: 1, Name: "inf_metric", Value: math.Inf(1)},
		{SchemaVersion: 1, Name: "neg_inf_metric", Value: math.Inf(-1)},
	}

	sanitizedEvts, sanitizedMetrics := ai.SanitizeTelemetry(events, metrics)

	if len(sanitizedEvts) != 1 || sanitizedEvts[0].EventID != "e-valid" {
		t.Fatalf("expected 1 valid event, got %v", sanitizedEvts)
	}
	if len(sanitizedMetrics) != 1 || sanitizedMetrics[0].Name != "valid_metric" {
		t.Fatalf("expected 1 valid metric, got %v", sanitizedMetrics)
	}
}

func TestValidator_NonEmptiness(t *testing.T) {
	events := []observability.ClusterEvent{
		{SchemaVersion: 1, EventID: "e-1", NodeID: "node-1", Type: observability.ElectionStarted},
	}

	// Zero claims
	respNoClaims := ai.LLMResponse{
		IncidentType:      ai.ElectionStorm,
		Severity:          ai.Critical,
		ConsistencyImpact: ai.WritesUnavailable,
		Confidence:        0.95,
		Claims:            nil,
	}
	if _, err := ai.ValidateLLMResponse(respNoClaims, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for zero claims")
	}

	// Claim with zero evidence IDs
	respNoEvidence := ai.LLMResponse{
		IncidentType:      ai.ElectionStorm,
		Severity:          ai.Critical,
		ConsistencyImpact: ai.WritesUnavailable,
		Confidence:        0.95,
		Claims: []ai.DiagnosisClaim{
			{Claim: "election failed", ClaimType: ai.Inference, EvidenceIDs: nil},
		},
	}
	if _, err := ai.ValidateLLMResponse(respNoEvidence, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for claim with zero evidence IDs")
	}
}

func TestValidator_EnumAndBoundsChecks(t *testing.T) {
	events := []observability.ClusterEvent{
		{SchemaVersion: 1, EventID: "e-1", NodeID: "node-1", Type: observability.ElectionStarted},
	}
	baseResp := ai.LLMResponse{
		IncidentType:      ai.ElectionStorm,
		Severity:          ai.Critical,
		ConsistencyImpact: ai.WritesUnavailable,
		Confidence:        0.90,
		AffectedNodes:     []string{"node-1"},
		Claims: []ai.DiagnosisClaim{
			{Claim: "election storm in progress", ClaimType: ai.Inference, EvidenceIDs: []string{"e-1"}},
		},
	}

	// Invalid IncidentType
	respBadType := baseResp
	respBadType.IncidentType = "BAD_INCIDENT"
	if _, err := ai.ValidateLLMResponse(respBadType, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for invalid IncidentType")
	}

	// Invalid Severity
	respBadSev := baseResp
	respBadSev.Severity = "SUPER_CRITICAL"
	if _, err := ai.ValidateLLMResponse(respBadSev, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for invalid Severity")
	}

	// Root-cause shaped ConsistencyImpact (must be rejected!)
	respRootCause := baseResp
	respRootCause.ConsistencyImpact = "NETWORK_PARTITION"
	if _, err := ai.ValidateLLMResponse(respRootCause, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for root-cause shaped ConsistencyImpact")
	}

	// Confidence < 0
	respNegConf := baseResp
	respNegConf.Confidence = -0.1
	if _, err := ai.ValidateLLMResponse(respNegConf, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for confidence < 0")
	}

	// Confidence > 1
	respHighConf := baseResp
	respHighConf.Confidence = 1.05
	if _, err := ai.ValidateLLMResponse(respHighConf, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for confidence > 1")
	}

	// Confidence NaN
	respNaNConf := baseResp
	respNaNConf.Confidence = math.NaN()
	if _, err := ai.ValidateLLMResponse(respNaNConf, events, ai.LLM); err == nil {
		t.Fatalf("expected rejection for confidence NaN")
	}
}

func TestValidator_EvidenceResolution(t *testing.T) {
	events := []observability.ClusterEvent{
		{SchemaVersion: 1, EventID: "real-event-1", NodeID: "node-1", Type: observability.ElectionStarted},
	}

	resp := ai.LLMResponse{
		IncidentType:      ai.ElectionStorm,
		Severity:          ai.Critical,
		ConsistencyImpact: ai.WritesUnavailable,
		Confidence:        0.90,
		AffectedNodes:     []string{"node-1"},
		Claims: []ai.DiagnosisClaim{
			{Claim: "election occurred", ClaimType: ai.Inference, EvidenceIDs: []string{"nonexistent-event-99"}},
		},
	}

	_, err := ai.ValidateLLMResponse(resp, events, ai.LLM)
	if err == nil {
		t.Fatalf("expected rejection for unresolvable EventID")
	}
	if !strings.Contains(err.Error(), "unresolvable EventID") {
		t.Errorf("expected unresolvable error message, got: %v", err)
	}
}

func TestValidator_ObservationDerivabilityAndAntiCircularity(t *testing.T) {
	events := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "ae-fail-1",
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields: map[string]string{
				"rpc_type":    "AppendEntries",
				"peer":        "node-2",
				"error_class": "timeout",
			},
		},
	}

	// Valid OBSERVATION claim directly derivable from event fields
	validResp := ai.LLMResponse{
		IncidentType:      ai.NodeUnreachable,
		Severity:          ai.Medium,
		ConsistencyImpact: ai.CommitProgressBlocked,
		Confidence:        0.85,
		AffectedNodes:     []string{"node-2"},
		Claims: []ai.DiagnosisClaim{
			{
				Claim:       "AppendEntries RPC failed to peer node-2 with timeout",
				EvidenceIDs: []string{"ae-fail-1"},
				ClaimType:   ai.Observation,
			},
		},
	}

	inc, err := ai.ValidateLLMResponse(validResp, events, ai.LLM)
	if err != nil {
		t.Fatalf("expected valid OBSERVATION claim to pass, got error: %v", err)
	}
	if inc == nil || len(inc.Evidence) != 1 {
		t.Fatalf("expected 1 resolved evidence, got %v", inc)
	}

	// Hallucinated / non-derivable OBSERVATION claim
	invalidResp := ai.LLMResponse{
		IncidentType:      ai.NodeUnreachable,
		Severity:          ai.Medium,
		ConsistencyImpact: ai.CommitProgressBlocked,
		Confidence:        0.85,
		AffectedNodes:     []string{"node-2"},
		Claims: []ai.DiagnosisClaim{
			{
				Claim:       "The server power supply caught fire during evening maintenance",
				EvidenceIDs: []string{"ae-fail-1"},
				ClaimType:   ai.Observation, // Marked OBSERVATION, but not derivable!
			},
		},
	}

	_, valErr := ai.ValidateLLMResponse(invalidResp, events, ai.LLM)
	if valErr == nil {
		t.Fatalf("expected non-derivable OBSERVATION claim to be rejected")
	}

	// Anti-Circularity verification:
	// Verify that the incident was completely rejected, NOT silently reclassified as INFERENCE!
	if !strings.Contains(valErr.Error(), "OBSERVATION claim 0") || !strings.Contains(valErr.Error(), "not derivable") {
		t.Errorf("expected explicit OBSERVATION non-derivable rejection, got: %v", valErr)
	}
}

// TestValidator_ObservationDerivability_EntityAloneDoesNotSatisfy verifies Finding 1:
// an OBSERVATION claim that references an entity (node-2) but describes an unobserved/hallucinated
// event type ("power supply caught fire") must fail derivability. An entity match alone is not enough.
func TestValidator_ObservationDerivability_EntityAloneDoesNotSatisfy(t *testing.T) {
	events := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "ae-fail-1",
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields: map[string]string{
				"rpc_type":    "AppendEntries",
				"peer":        "node-2",
				"error_class": "timeout",
			},
		},
	}

	// Claim contains "node-2" (entity match) but asserts "power supply caught fire" (semantic mismatch)
	resp := ai.LLMResponse{
		IncidentType:      ai.NodeUnreachable,
		Severity:          ai.Medium,
		ConsistencyImpact: ai.CommitProgressBlocked,
		Confidence:        0.85,
		AffectedNodes:     []string{"node-2"},
		Claims: []ai.DiagnosisClaim{
			{
				Claim:       "Server node-2 power supply caught fire during evening maintenance",
				EvidenceIDs: []string{"ae-fail-1"},
				ClaimType:   ai.Observation,
			},
		},
	}

	_, err := ai.ValidateLLMResponse(resp, events, ai.LLM)
	if err == nil {
		t.Fatalf("expected rejection: claim with entity match but semantic mismatch must fail derivability")
	}
	if !strings.Contains(err.Error(), "not derivable") {
		t.Errorf("expected 'not derivable' error, got: %v", err)
	}
}

func TestValidator_AffectedNodesEvidenceBinding(t *testing.T) {
	events := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "e-1",
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries"},
		},
	}

	// Case 1: Node grounded in evidence -> accepted
	groundedResp := ai.LLMResponse{
		IncidentType:      ai.NodeUnreachable,
		Severity:          ai.Medium,
		ConsistencyImpact: ai.CommitProgressBlocked,
		Confidence:        0.85,
		AffectedNodes:     []string{"node-2"},
		Claims: []ai.DiagnosisClaim{
			{
				Claim:       "RPC to node-2 failed",
				EvidenceIDs: []string{"e-1"},
				ClaimType:   ai.Inference,
			},
		},
	}
	if _, err := ai.ValidateLLMResponse(groundedResp, events, ai.LLM); err != nil {
		t.Fatalf("expected grounded node-2 to be accepted, got error: %v", err)
	}

	// Case 2: Node NOT grounded in evidence (node-99 never mentioned in e-1) -> rejected!
	ungroundedResp := groundedResp
	ungroundedResp.AffectedNodes = []string{"node-99"}
	_, err := ai.ValidateLLMResponse(ungroundedResp, events, ai.LLM)
	if err == nil {
		t.Fatalf("expected ungrounded node-99 to be rejected")
	}
	if !strings.Contains(err.Error(), "zero supporting evidence") {
		t.Errorf("expected zero supporting evidence error, got: %v", err)
	}
}

func TestCanonicalNodeBearingFields(t *testing.T) {
	e := observability.ClusterEvent{
		NodeID: "n-emitter",
		PeerID: "n-peer",
		Fields: map[string]string{
			"candidate": "n-cand",
			"leader":    "n-ldr",
			"peers":     "n-p1, n-p2",
		},
	}

	nodes := ai.CanonicalNodeBearingFields(e)
	expected := map[string]bool{
		"n-emitter": true,
		"n-peer":    true,
		"n-cand":    true,
		"n-ldr":     true,
		"n-p1":      true,
		"n-p2":      true,
	}

	for _, n := range nodes {
		if !expected[n] {
			t.Errorf("unexpected node %q in extracted fields", n)
		}
		delete(expected, n)
	}
	if len(expected) > 0 {
		t.Errorf("missing expected nodes: %v", expected)
	}
}
