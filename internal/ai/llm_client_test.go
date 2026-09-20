package ai_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"raftkv/internal/ai"
	"raftkv/internal/observability"
)

func TestLLMClient_RobustnessTable(t *testing.T) {
	// Common telemetry: 3 failed RPCs so NodeUnreachable rule fires as candidate
	events := []observability.ClusterEvent{
		{
			SchemaVersion: 1,
			EventID:       "rpc-1",
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
		{
			SchemaVersion: 1,
			EventID:       "rpc-2",
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
		{
			SchemaVersion: 1,
			EventID:       "rpc-3",
			NodeID:        "node-1",
			PeerID:        "node-2",
			Type:          observability.RPCFailed,
			Fields:        map[string]string{"peer": "node-2", "rpc_type": "AppendEntries", "error_class": "timeout"},
		},
	}

	fakeLLM := ai.NewFakeLLM()
	engine := ai.NewDiagnosticsEngine(nil, fakeLLM)

	validResp := ai.LLMResponse{
		IncidentType:      ai.NodeUnreachable,
		Severity:          ai.Medium,
		ConsistencyImpact: ai.CommitProgressBlocked,
		Confidence:        0.88,
		AffectedNodes:     []string{"node-2"},
		Claims: []ai.DiagnosisClaim{
			{
				Claim:       "Node-2 appears completely unreachable across multiple RPC attempts",
				EvidenceIDs: []string{"rpc-1", "rpc-2", "rpc-3"},
				ClaimType:   ai.Inference,
			},
		},
	}

	// Baseline: Valid LLM response yields Hybrid incident
	t.Run("00_Baseline_ValidLLMResponse_YieldsHybrid", func(t *testing.T) {
		fakeLLM.SetError(nil)
		fakeLLM.SetResponse(validResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil {
			t.Fatalf("expected valid hybrid incident, got inc=%v err=%v", inc, err)
		}
		if inc.Source != ai.Hybrid {
			t.Errorf("expected Source %s, got %s", ai.Hybrid, inc.Source)
		}
		if inc.Confidence != 0.88 {
			t.Errorf("expected Confidence 0.88, got %v", inc.Confidence)
		}
	})

	// 1. LLM call errors / times out -> falls back to rule engine
	t.Run("01_LLMCallErrorOrTimeout_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(errors.New("timeout or rpc error"))
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil {
			t.Fatalf("expected fallback to rule engine, got inc=%v err=%v", inc, err)
		}
		if inc.Source != ai.RuleEngine {
			t.Errorf("expected Source %s on LLM error, got %s", ai.RuleEngine, inc.Source)
		}
		if inc.IncidentType != ai.NodeUnreachable {
			t.Errorf("expected rule candidate NodeUnreachable, got %s", inc.IncidentType)
		}
	})

	// 2. LLM cites nonexistent EventID -> rejected, falls back to rule engine
	t.Run("02_NonexistentEventID_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		badEidResp := validResp
		badEidResp.Claims = []ai.DiagnosisClaim{
			{Claim: "claim citing ghost event", EvidenceIDs: []string{"ghost-event-999"}, ClaimType: ai.Inference},
		}
		fakeLLM.SetResponse(badEidResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for nonexistent EventID, got inc=%v", inc)
		}
	})

	// 3. LLM OBSERVATION claim not derivable -> rejected, falls back to rule engine (NEVER reclassified)
	t.Run("03_NonDerivableObservation_FallsBackToRuleEngine_NeverReclassified", func(t *testing.T) {
		fakeLLM.SetError(nil)
		badObsResp := validResp
		badObsResp.Claims = []ai.DiagnosisClaim{
			{Claim: "Power supply burned out", EvidenceIDs: []string{"rpc-1"}, ClaimType: ai.Observation},
		}
		fakeLLM.SetResponse(badObsResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for non-derivable OBSERVATION, got inc=%v", inc)
		}
	})

	// 4. LLM names ungrounded AffectedNodes -> rejected, falls back to rule engine
	t.Run("04_UngroundedAffectedNodes_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		badNodesResp := validResp
		badNodesResp.AffectedNodes = []string{"node-99"} // node-99 not in any cited events
		fakeLLM.SetResponse(badNodesResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for ungrounded AffectedNodes, got inc=%v", inc)
		}
	})

	// 5. LLM returns unrecognized IncidentType or Severity -> rejected, falls back to rule engine
	t.Run("05_UnrecognizedIncidentTypeOrSeverity_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		badTypeResp := validResp
		badTypeResp.IncidentType = "UNRECOGNIZED_TYPE"
		fakeLLM.SetResponse(badTypeResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for invalid IncidentType, got inc=%v", inc)
		}

		badSevResp := validResp
		badSevResp.Severity = "INVALID_SEVERITY"
		fakeLLM.SetResponse(badSevResp)
		inc, err = engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for invalid Severity, got inc=%v", inc)
		}
	})

	// 6. LLM returns unrecognized ConsistencyImpact -> rejected, falls back to rule engine
	t.Run("06_UnrecognizedConsistencyImpact_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		badImpactResp := validResp
		badImpactResp.ConsistencyImpact = "UNKNOWN_IMPACT"
		fakeLLM.SetResponse(badImpactResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for unrecognized ConsistencyImpact, got inc=%v", inc)
		}
	})

	// 7. LLM returns root-cause shaped ConsistencyImpact ("NETWORK_PARTITION") -> rejected, falls back
	t.Run("07_RootCauseConsistencyImpact_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		rootCauseImpactResp := validResp
		rootCauseImpactResp.ConsistencyImpact = "NETWORK_PARTITION"
		fakeLLM.SetResponse(rootCauseImpactResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for root-cause ConsistencyImpact, got inc=%v", inc)
		}
	})

	// 8. LLM returns zero Claims -> rejected, falls back to rule engine
	t.Run("08_ZeroClaims_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		zeroClaimsResp := validResp
		zeroClaimsResp.Claims = nil
		fakeLLM.SetResponse(zeroClaimsResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for zero Claims, got inc=%v", inc)
		}
	})

	// 9. LLM returns Confidence outside [0, 1] or non-finite -> rejected, falls back to rule engine
	t.Run("09_OutOfBoundsConfidence_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(nil)
		badConfResp := validResp
		badConfResp.Confidence = 1.5
		fakeLLM.SetResponse(badConfResp)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for confidence > 1, got inc=%v", inc)
		}

		badConfResp.Confidence = math.NaN()
		fakeLLM.SetResponse(badConfResp)
		inc, err = engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for confidence NaN, got inc=%v", inc)
		}
	})

	// 10. LLM service unavailable -> falls back to rule engine
	t.Run("10_LLMServiceUnavailable_FallsBackToRuleEngine", func(t *testing.T) {
		fakeLLM.SetError(ai.ErrLLMUnavailable)
		inc, err := engine.Diagnose(context.Background(), events, nil)
		if err != nil || inc == nil || inc.Source != ai.RuleEngine {
			t.Fatalf("expected fallback to rule engine for ErrLLMUnavailable, got inc=%v", inc)
		}
	})

	// 11. Empty telemetry / no rule fired + LLM error -> returns nil, no panic
	t.Run("11_EmptyTelemetry_NoRuleFired_ReturnsNil", func(t *testing.T) {
		fakeLLM.SetError(ai.ErrLLMUnavailable)
		incEmpty, err := engine.Diagnose(context.Background(), nil, nil)
		if err != nil || incEmpty != nil {
			t.Fatalf("expected nil incident for empty telemetry with LLM error, got inc=%v err=%v", incEmpty, err)
		}
	})
}

func TestLLMClient_ContextCancellation(t *testing.T) {
	fakeLLM := ai.NewFakeLLM()
	fakeLLM.SetDelay(5 * time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := fakeLLM.Analyze(ctx, ai.DiagnosisInput{})
	if err == nil {
		t.Fatalf("expected context cancellation error, got nil")
	}
}
