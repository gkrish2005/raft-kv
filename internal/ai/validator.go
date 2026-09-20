package ai

import (
	"errors"
	"fmt"
	"math"
	"strings"
	"sync/atomic"
	"time"

	"raftkv/internal/observability"
)

var incidentSeq atomic.Uint64

// SanitizeTelemetry filters out unsupported schema versions and non-finite metric values
// at the ingest boundary per docs/ai-design.md.
func SanitizeTelemetry(events []observability.ClusterEvent, metrics []observability.MetricSnapshot) ([]observability.ClusterEvent, []observability.MetricSnapshot) {
	var validEvents []observability.ClusterEvent
	for _, e := range events {
		if e.SchemaVersion == observability.ClusterEventSchemaVersion {
			validEvents = append(validEvents, e)
		}
	}

	var validMetrics []observability.MetricSnapshot
	for _, m := range metrics {
		if m.SchemaVersion != observability.MetricSchemaVersion {
			continue
		}
		if math.IsNaN(m.Value) || math.IsInf(m.Value, 0) {
			continue
		}
		validMetrics = append(validMetrics, m)
	}

	return validEvents, validMetrics
}

// CanonicalNodeBearingFields extracts all valid node IDs present in a ClusterEvent per docs/architecture.md:
// - NodeID
// - PeerID
// - Fields["peer"]
// - Fields["candidate"]
// - Fields["leader"]
// - Comma-separated members of Fields["peers"]
func CanonicalNodeBearingFields(e observability.ClusterEvent) []string {
	nodesMap := make(map[string]bool)

	if e.NodeID != "" {
		nodesMap[e.NodeID] = true
	}
	if e.PeerID != "" {
		nodesMap[e.PeerID] = true
	}
	if p, ok := e.Fields["peer"]; ok && p != "" {
		nodesMap[p] = true
	}
	if c, ok := e.Fields["candidate"]; ok && c != "" {
		nodesMap[c] = true
	}
	if l, ok := e.Fields["leader"]; ok && l != "" {
		nodesMap[l] = true
	}
	if peersStr, ok := e.Fields["peers"]; ok && peersStr != "" {
		for _, p := range strings.Split(peersStr, ",") {
			p = strings.TrimSpace(p)
			if p != "" {
				nodesMap[p] = true
			}
		}
	}

	var result []string
	for n := range nodesMap {
		result = append(result, n)
	}
	return result
}

// CheckObservationDerivability checks whether an OBSERVATION claim is directly derivable
// from the cited events' own fields per docs/ai-design.md.
// It requires BOTH an entity match (referencing the node, peer, candidate, or leader)
// AND a semantic match (referencing the event type's action or field values).
func CheckObservationDerivability(claimText string, citedEvents []observability.ClusterEvent) bool {
	if len(citedEvents) == 0 {
		return false
	}

	claimLower := strings.ToLower(claimText)

	eventTypeKeywords := map[observability.EventType][]string{
		observability.ElectionStarted:   {"election", "candidate", "storm"},
		observability.VoteGranted:       {"vote", "granted"},
		observability.VoteRejected:      {"vote", "rejected"},
		observability.LeaderElected:     {"leader", "elected", "leadership", "thrash"},
		observability.LeaderSteppedDown: {"stepped down", "step down", "leader"},
		observability.TermAdvanced:      {"term"},
		observability.RPCFailed:         {"rpc", "fail", "timeout", "unreachable", "lag", "slow"},
		observability.RPCSucceeded:      {"rpc", "success"},
		observability.LogAppended:       {"log", "append"},
		observability.LogConflict:       {"conflict", "log", "lag"},
		observability.CommitAdvanced:    {"commit"},
		observability.EntryApplied:      {"apply", "applied"},
		observability.NodeStarted:       {"start", "node"},
		observability.NodeStopped:       {"stop", "kill", "crash"},
		observability.NodeRestarted:     {"restart"},
		observability.PartitionCreated:  {"partition", "isolated"},
		observability.PartitionHealed:   {"heal", "partition"},
	}

	// Verify that at least one cited event satisfies BOTH entity and semantic matches
	for _, e := range citedEvents {
		entityMatched := false
		for _, n := range CanonicalNodeBearingFields(e) {
			if strings.Contains(claimLower, strings.ToLower(n)) {
				entityMatched = true
				break
			}
		}

		semanticMatched := false
		if keywords, ok := eventTypeKeywords[e.Type]; ok {
			for _, kw := range keywords {
				if strings.Contains(claimLower, kw) {
					semanticMatched = true
					break
				}
			}
		} else {
			// Safe default for unlisted EventTypes:
			// Match against tokens in the EventType name itself (split by '_')
			// and string values in e.Fields.
			typeTokens := strings.Split(strings.ToLower(string(e.Type)), "_")
			for _, tok := range typeTokens {
				if len(tok) >= 3 && strings.Contains(claimLower, tok) {
					semanticMatched = true
					break
				}
			}
			if !semanticMatched {
				for _, v := range e.Fields {
					vLower := strings.ToLower(v)
					if len(vLower) >= 3 && strings.Contains(claimLower, vLower) {
						semanticMatched = true
						break
					}
				}
			}
		}

		// Both must hold for the claim to be a valid restatement of what the event says
		if entityMatched && semanticMatched {
			return true
		}
	}

	return false
}

// ValidateLLMResponse executes the mechanical evidence validation pipeline on an LLMResponse.
// It returns the validated AIIncident if all checks pass, or an error detailing the rejection.
func ValidateLLMResponse(resp LLMResponse, events []observability.ClusterEvent, source Source) (*AIIncident, error) {
	// 1. Non-emptiness check (vacuous-acceptance guard)
	if len(resp.Claims) == 0 {
		return nil, errors.New("incident rejected: zero claims provided")
	}

	// 2. Enum validations
	if !IsValidIncidentType(resp.IncidentType) {
		return nil, fmt.Errorf("incident rejected: unrecognized IncidentType %q", resp.IncidentType)
	}
	if !IsValidSeverity(resp.Severity) {
		return nil, fmt.Errorf("incident rejected: unrecognized Severity %q", resp.Severity)
	}
	if !IsValidConsistencyImpact(resp.ConsistencyImpact) {
		return nil, fmt.Errorf("incident rejected: unrecognized or root-cause-shaped ConsistencyImpact %q", resp.ConsistencyImpact)
	}

	// 3. Confidence bounds: [0, 1] and finite
	if resp.Confidence < 0.0 || resp.Confidence > 1.0 || math.IsNaN(resp.Confidence) || math.IsInf(resp.Confidence, 0) {
		return nil, fmt.Errorf("incident rejected: confidence %v out of bounds or non-finite", resp.Confidence)
	}

	// Build event lookup map from the supplied telemetry window
	eventMap := make(map[string]observability.ClusterEvent, len(events))
	for _, e := range events {
		eventMap[e.EventID] = e
	}

	// 4. Evidence resolution and claim checks
	resolvedEvidenceMap := make(map[string]observability.ClusterEvent)
	for i, c := range resp.Claims {
		if !IsValidClaimType(c.ClaimType) {
			return nil, fmt.Errorf("incident rejected: claim %d has invalid ClaimType %q", i, c.ClaimType)
		}
		if len(c.EvidenceIDs) == 0 {
			return nil, fmt.Errorf("incident rejected: claim %d cites zero evidence IDs", i)
		}

		var citedEvents []observability.ClusterEvent
		for _, eid := range c.EvidenceIDs {
			ev, found := eventMap[eid]
			if !found {
				return nil, fmt.Errorf("incident rejected: unresolvable EventID %q in claim %d", eid, i)
			}
			citedEvents = append(citedEvents, ev)
			resolvedEvidenceMap[eid] = ev
		}

		// 5. OBSERVATION derivability check
		if c.ClaimType == Observation {
			if !CheckObservationDerivability(c.Claim, citedEvents) {
				// Anti-circularity rule: MUST NOT silently reclassify as INFERENCE.
				return nil, fmt.Errorf("incident rejected: OBSERVATION claim %d %q not derivable from cited event fields", i, c.Claim)
			}
		}
	}

	if len(resolvedEvidenceMap) == 0 {
		return nil, errors.New("incident rejected: zero resolved evidence references")
	}

	// Collect canonical evidence references
	var evidenceRefs []EvidenceRef
	for eid, ev := range resolvedEvidenceMap {
		evidenceRefs = append(evidenceRefs, EvidenceRef{
			EventID: eid,
			Event:   ev,
		})
	}

	// 6. AffectedNodes evidence-binding check:
	// Every node in AffectedNodes must appear in the canonical node-bearing fields of at least one cited evidence event.
	groundedNodes := make(map[string]bool)
	for _, evRef := range evidenceRefs {
		for _, n := range CanonicalNodeBearingFields(evRef.Event) {
			groundedNodes[n] = true
		}
	}

	for _, node := range resp.AffectedNodes {
		if !groundedNodes[node] {
			return nil, fmt.Errorf("incident rejected: affected node %q has zero supporting evidence in cited events", node)
		}
	}

	seq := incidentSeq.Add(1)
	incidentID := fmt.Sprintf("incident-%d-%05d", time.Now().Unix(), seq)

	return &AIIncident{
		IncidentID:         incidentID,
		DetectedAt:         time.Now(),
		IncidentType:       resp.IncidentType,
		Severity:           resp.Severity,
		Claims:             resp.Claims,
		Evidence:           evidenceRefs,
		AffectedNodes:      resp.AffectedNodes,
		ConsistencyImpact:  resp.ConsistencyImpact,
		RecommendedActions: resp.RecommendedActions,
		Confidence:         resp.Confidence,
		Source:             source,
	}, nil
}

// CandidateToIncident converts a RuleEngineCandidate directly into an AIIncident
// with Source = RuleEngine, resolving its evidence against the supplied events.
func CandidateToIncident(cand *RuleEngineCandidate, events []observability.ClusterEvent) (*AIIncident, error) {
	if cand == nil {
		return nil, nil
	}

	resp := LLMResponse{
		IncidentType:       cand.IncidentType,
		Severity:           cand.Severity,
		Claims:             cand.Claims,
		AffectedNodes:      cand.AffectedNodes,
		ConsistencyImpact:  cand.ConsistencyImpact,
		RecommendedActions: cand.RecommendedActions,
		Confidence:         cand.Confidence,
	}

	return ValidateLLMResponse(resp, events, RuleEngine)
}
