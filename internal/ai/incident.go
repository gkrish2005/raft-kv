package ai

import (
	"fmt"
	"strings"
	"time"

	"raftkv/internal/observability"
)

// IncidentType represents frozen incident classification enums per docs/ai-design.md.
type IncidentType string

const (
	LeaderInstability IncidentType = "LEADER_INSTABILITY"
	NodeUnreachable   IncidentType = "NODE_UNREACHABLE"
	ReplicationLag    IncidentType = "REPLICATION_LAG"
	ElectionStorm     IncidentType = "ELECTION_STORM"
	SlowFollower      IncidentType = "SLOW_FOLLOWER"
	NetworkPartition  IncidentType = "NETWORK_PARTITION"
)

// Severity represents frozen severity enums per docs/ai-design.md.
type Severity string

const (
	Low      Severity = "LOW"
	Medium   Severity = "MEDIUM"
	High     Severity = "HIGH"
	Critical Severity = "CRITICAL"
)

// Source represents the origin of the incident diagnosis per docs/ai-design.md.
type Source string

const (
	RuleEngine Source = "rule_engine"
	LLM        Source = "llm"
	Hybrid     Source = "hybrid"
)

// ClaimType distinguishes system-generated observations from inference claims per docs/ai-design.md.
type ClaimType string

const (
	Observation ClaimType = "OBSERVATION"
	Inference   ClaimType = "INFERENCE"
)

// ConsistencyImpact describes service/consistency impact only per docs/ai-design.md.
// It must never encode a root cause (e.g. "NETWORK_PARTITION" is not a valid ConsistencyImpact).
type ConsistencyImpact string

const (
	None                  ConsistencyImpact = "NONE"
	WritesUnavailable     ConsistencyImpact = "WRITES_UNAVAILABLE"      // e.g. no quorum to commit
	ReadsUnavailable      ConsistencyImpact = "READS_UNAVAILABLE"       // e.g. no leader to confirm quorum
	CommitProgressBlocked ConsistencyImpact = "COMMIT_PROGRESS_BLOCKED" // e.g. replication stalled on a lagging/unreachable follower
)

const (
	// ConfidenceNotice is required to be documented everywhere confidence is surfaced per docs/ai-design.md.
	ConfidenceNotice = "Confidence is a heuristic score, not a probability of correctness — see docs/ai-design.md's Confidence calibration metric for how it's evaluated against actual outcomes."

	// RecommendedActionsNotice enforces ADR 008's read-only boundary at display time.
	RecommendedActionsNotice = "Informational operator suggestions only — never executed automatically."
)

// DiagnosisClaim represents an individual observation or inference claim citing evidence IDs.
type DiagnosisClaim struct {
	Claim       string    `json:"claim"`
	EvidenceIDs []string  `json:"evidence_ids"`
	ClaimType   ClaimType `json:"claim_type"`
}

// EvidenceRef pairs an EventID with its canonical ClusterEvent looked up by the validator.
// The validator derives these; LLM-authored descriptions are discarded.
type EvidenceRef struct {
	EventID string                     `json:"event_id"`
	Event   observability.ClusterEvent `json:"event"`
}

// RuleEngineCandidate is the deterministic rule engine's candidate diagnosis.
type RuleEngineCandidate struct {
	IncidentType       IncidentType      `json:"incident_type"`
	Severity           Severity          `json:"severity"`
	AffectedNodes      []string          `json:"affected_nodes"`
	Claims             []DiagnosisClaim  `json:"claims"` // OBSERVATION only, rule-generated
	ConsistencyImpact  ConsistencyImpact `json:"consistency_impact"`
	RecommendedActions []string          `json:"recommended_actions"`
	Confidence         float64           `json:"confidence"`
}

// DiagnosisInput carries the telemetry window and optional rule-engine context to the LLM.
type DiagnosisInput struct {
	Events              []observability.ClusterEvent   `json:"events"`
	Metrics             []observability.MetricSnapshot `json:"metrics"`
	RuleEngineCandidate *RuleEngineCandidate           `json:"rule_engine_candidate,omitempty"`
}

// LLMResponse carries the LLM's proposed/refined diagnosis per docs/ai-design.md Option A.
type LLMResponse struct {
	IncidentType       IncidentType      `json:"incident_type"`
	Severity           Severity          `json:"severity"`
	Claims             []DiagnosisClaim  `json:"claims"`
	AffectedNodes      []string          `json:"affected_nodes"`
	ConsistencyImpact  ConsistencyImpact `json:"consistency_impact"`
	RecommendedActions []string          `json:"recommended_actions"`
	Confidence         float64           `json:"confidence"`
}

// AIIncident is an authoritative, validated incident output.
type AIIncident struct {
	IncidentID         string             `json:"incident_id"`
	DetectedAt         time.Time          `json:"detected_at"`
	IncidentType       IncidentType       `json:"incident_type"`
	Severity           Severity           `json:"severity"`
	Claims             []DiagnosisClaim   `json:"claims"`
	Evidence           []EvidenceRef      `json:"evidence"`
	AffectedNodes      []string           `json:"affected_nodes"`
	ConsistencyImpact  ConsistencyImpact  `json:"consistency_impact"`
	RecommendedActions []string           `json:"recommended_actions"`
	Confidence         float64            `json:"confidence"`
	Source             Source             `json:"source"`
}

// IsValidIncidentType checks if the incident type is one of the 6 frozen enums.
func IsValidIncidentType(t IncidentType) bool {
	switch t {
	case LeaderInstability, NodeUnreachable, ReplicationLag, ElectionStorm, SlowFollower, NetworkPartition:
		return true
	default:
		return false
	}
}

// IsValidSeverity checks if the severity is one of the 4 frozen enums.
func IsValidSeverity(s Severity) bool {
	switch s {
	case Low, Medium, High, Critical:
		return true
	default:
		return false
	}
}

// IsValidClaimType checks if the claim type is OBSERVATION or INFERENCE.
func IsValidClaimType(c ClaimType) bool {
	switch c {
	case Observation, Inference:
		return true
	default:
		return false
	}
}

// IsValidConsistencyImpact checks if the impact is one of the 4 frozen enums.
// Notice: Any root-cause-shaped value (e.g. "NETWORK_PARTITION") returns false.
func IsValidConsistencyImpact(ci ConsistencyImpact) bool {
	switch ci {
	case None, WritesUnavailable, ReadsUnavailable, CommitProgressBlocked:
		return true
	default:
		return false
	}
}

// FormatIncident formats an AIIncident into a human-readable display string,
// explicitly including the confidence notice and informational recommended actions notice.
func FormatIncident(inc *AIIncident) string {
	if inc == nil {
		return "No incident diagnosed"
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("Incident: %s (Type: %s, Severity: %s, Source: %s)\n",
		inc.IncidentID, inc.IncidentType, inc.Severity, inc.Source))
	sb.WriteString(fmt.Sprintf("DetectedAt: %s\n", inc.DetectedAt.UTC().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("Affected Nodes: %s\n", strings.Join(inc.AffectedNodes, ", ")))
	sb.WriteString(fmt.Sprintf("Consistency Impact: %s\n", inc.ConsistencyImpact))
	sb.WriteString(fmt.Sprintf("Confidence: %.2f (%s)\n", inc.Confidence, ConfidenceNotice))

	sb.WriteString("\nClaims:\n")
	for i, c := range inc.Claims {
		sb.WriteString(fmt.Sprintf("  %d. [%s] %s (Evidence: %s)\n",
			i+1, c.ClaimType, c.Claim, strings.Join(c.EvidenceIDs, ", ")))
	}

	sb.WriteString("\nEvidence:\n")
	for i, ev := range inc.Evidence {
		sb.WriteString(fmt.Sprintf("  %d. [%s] Type=%s Node=%s Term=%d Index=%d\n",
			i+1, ev.EventID, ev.Event.Type, ev.Event.NodeID, ev.Event.Term, ev.Event.LogIndex))
	}

	if len(inc.RecommendedActions) > 0 {
		sb.WriteString(fmt.Sprintf("\nRecommended Actions (%s):\n", RecommendedActionsNotice))
		for i, act := range inc.RecommendedActions {
			sb.WriteString(fmt.Sprintf("  %d. %s\n", i+1, act))
		}
	}

	return sb.String()
}
