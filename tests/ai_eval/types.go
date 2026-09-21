package aieval

import (
	"time"

	"raftkv/internal/ai"
)

// EvalCase represents a ground-truth synthetic evaluation scenario.
type EvalCase struct {
	ScenarioName          string
	Description           string
	ExpectedIncidentType  ai.IncidentType
	ExpectedAffectedNodes []string
	ExpectedSeverity      ai.Severity // Frozen per docs/ai-design.md's table
	IsHealthyControl      bool
	IsHeldOut             bool
}

// AuditClassification defines the three-way manual audit judgment for INFERENCE claims per docs/ai-design.md.
type AuditClassification string

const (
	ClaimSupported   AuditClassification = "SUPPORTED"
	ClaimUnsupported AuditClassification = "UNSUPPORTED"
	ClaimUncertain   AuditClassification = "UNCERTAIN"
)

// InferenceClaimAudit records the developer's manual audit classification for a single INFERENCE claim.
type InferenceClaimAudit struct {
	ClaimText      string              `json:"claim_text"`
	Classification AuditClassification `json:"classification"` // SUPPORTED | UNSUPPORTED | UNCERTAIN
	Notes          string              `json:"notes,omitempty"`
}

// ScenarioAuditFile stores the human-reviewed audit classifications for a scenario.
type ScenarioAuditFile struct {
	ScenarioName string                `json:"scenario_name"`
	Auditor      string                `json:"auditor"` // e.g. "developer (self-reviewed)"
	AuditedAt    time.Time             `json:"audited_at"`
	Claims       []InferenceClaimAudit `json:"claims"`
}

// EvalResult captures the scored outcome of evaluating one EvalCase.
type EvalResult struct {
	Case                EvalCase
	ActualIncidentType  ai.IncidentType
	ActualAffectedNodes []string
	ActualSeverity      ai.Severity
	ActualConfidence    float64
	AcceptedIncident    *ai.AIIncident

	// Accuracy metrics (kept distinct, never blended)
	Correct         bool // ActualIncidentType == Case.ExpectedIncidentType
	SeverityCorrect bool // ActualSeverity == Case.ExpectedSeverity
	NodesCorrect    bool // ActualAffectedNodes matches Case.ExpectedAffectedNodes

	// Evidence & validator metrics
	EvidenceValid       bool // 100% hard requirement for accepted incidents
	LLMEvidenceRejected bool // Did raw LLM response fail validation and fall back?

	// Manual INFERENCE audit counts (docs/ai-design.md three-way rubric)
	AuditStatus            string // "COMPLETED" | "PENDING_AUDIT" | "NONE (OBS-only)" | "NONE (Healthy Control)" | "REJECTED"
	ObservationClaimsCount int
	InferenceClaimsCount   int
	SupportedClaims        int // Marked SUPPORTED by developer
	UnsupportedClaims      int // Marked UNSUPPORTED by developer
	UncertainClaims        int // Marked UNCERTAIN (excluded from unsupported denominator)

	// Error & confidence metrics
	FalsePositive bool
	FalseNegative bool
	Confidence    float64
	Latency       time.Duration
}

// BucketStats tracks calibration accuracy for a confidence range.
type BucketStats struct {
	Range    string
	Count    int
	Correct  int
	Accuracy float64
}

// EvalReport aggregates the results across all evaluated cases.
type EvalReport struct {
	Mode                   string  // "rules", "recorded", "live"
	TotalScenarios         int
	PrimaryScenarios       int
	HeldOutScenarios       int
	ClassificationAccuracy float64 // count(Correct) / TotalScenarios
	SeverityAccuracy       float64 // count(SeverityCorrect) / TotalScenarios (distinct metric)
	NodeAccuracy           float64 // count(NodesCorrect) / TotalScenarios
	AcceptedEvidenceValid  float64 // Hard 100% on accepted incidents
	LLMRejectionRate       float64 // count(LLMEvidenceRejected) / TotalScenarios

	// Auditability & claim breakdown
	ScenariosObservationOnly     int
	ScenariosWithInference       int
	ScenariosRejected            int
	TotalObservationClaims       int
	TotalInferenceClaims         int
	TotalInferenceClaimsReviewed int
	EvaluableInferenceClaims     int
	UnsupportedClaimRate         float64 // count(UNSUPPORTED) / (count(SUPPORTED) + count(UNSUPPORTED))
	UncertainClaimFraction       float64 // count(UNCERTAIN) / total reviewed claims
	FalsePositiveCount           int     // 0 required on 30m healthy control
	FalseNegativeCount           int
	CalibrationBuckets           map[string]BucketStats // [0-0.5), [0.5-0.8), [0.8-1.0]
	AverageLatency               time.Duration
	LatencyLabel                 string // Explicitly qualified per mode
	Results                      []EvalResult
}
