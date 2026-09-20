package aieval

import (
	"context"
	"fmt"
	"time"

	"raftkv/internal/ai"
)

// RunnerConfig holds configuration for the evaluation runner.
type RunnerConfig struct {
	Mode         string // "rules" | "recorded" | "live"
	FixturesDir  string
	LiveClient   ai.LLMClient
	DumpAudits   bool
}

// RunEvaluation executes the evaluation pipeline across all provided cases.
func RunEvaluation(ctx context.Context, cfg RunnerConfig, cases []EvalCase) ([]EvalResult, error) {
	ruleEngine := ai.NewDeterministicRuleEngine()
	var results []EvalResult

	for _, c := range cases {
		telem, err := LoadTelemetryFixture(cfg.FixturesDir, c.ScenarioName)
		if err != nil {
			return nil, fmt.Errorf("error loading telemetry for %s: %w", c.ScenarioName, err)
		}

		res := EvalResult{
			Case: c,
		}

		startTime := time.Now()

		switch cfg.Mode {
		case "rules":
			// Mode 1: Rules only
			cand := ruleEngine.Evaluate(telem.Events, telem.Metrics)
			if cand != nil {
				inc, err := ai.CandidateToIncident(cand, telem.Events)
				if err == nil && inc != nil {
					res.AcceptedIncident = inc
					res.ActualIncidentType = inc.IncidentType
					res.ActualSeverity = inc.Severity
					res.ActualAffectedNodes = inc.AffectedNodes
					res.ActualConfidence = inc.Confidence
					res.Confidence = inc.Confidence
					res.EvidenceValid = len(inc.Evidence) > 0
				}
			}
			res.Latency = time.Since(startTime)

		case "recorded":
			// Mode 2: Recorded LLM response -> authoritative validator -> fallback if rejected
			cand := ruleEngine.Evaluate(telem.Events, telem.Metrics)
			recordedResp, err := LoadRecordedLLMResponse(cfg.FixturesDir, c.ScenarioName)
			if err != nil {
				return nil, fmt.Errorf("error loading recorded LLM response for %s: %w", c.ScenarioName, err)
			}

			if c.IsHealthyControl {
				// Healthy control: neither rule nor LLM should produce an incident
				if cand != nil {
					inc, _ := ai.CandidateToIncident(cand, telem.Events)
					res.AcceptedIncident = inc
					res.ActualIncidentType = inc.IncidentType
					res.ActualSeverity = inc.Severity
				}
			} else {
				// Validate recorded LLM response
				inc, valErr := ai.ValidateLLMResponse(*recordedResp, telem.Events, ai.Hybrid)
				if valErr != nil || inc == nil {
					// Fallback to rule engine candidate (Rule 27 / fail-open)
					res.LLMEvidenceRejected = true
					if cand != nil {
						fallbackInc, fallbackErr := ai.CandidateToIncident(cand, telem.Events)
						if fallbackErr == nil && fallbackInc != nil {
							res.AcceptedIncident = fallbackInc
							res.ActualIncidentType = fallbackInc.IncidentType
							res.ActualSeverity = fallbackInc.Severity
							res.ActualAffectedNodes = fallbackInc.AffectedNodes
							res.ActualConfidence = fallbackInc.Confidence
							res.Confidence = fallbackInc.Confidence
							res.EvidenceValid = len(fallbackInc.Evidence) > 0
						}
					}
				} else {
					res.AcceptedIncident = inc
					res.ActualIncidentType = inc.IncidentType
					res.ActualSeverity = inc.Severity
					res.ActualAffectedNodes = inc.AffectedNodes
					res.ActualConfidence = inc.Confidence
					res.Confidence = inc.Confidence
					res.EvidenceValid = len(inc.Evidence) > 0
				}
			}
			res.Latency = time.Since(startTime)

		case "live":
			// Mode 3: Live LLM client (informational)
			cand := ruleEngine.Evaluate(telem.Events, telem.Metrics)
			if cfg.LiveClient == nil {
				return nil, fmt.Errorf("live mode requested but LiveClient is nil")
			}
			diagIn := ai.DiagnosisInput{
				Events:              telem.Events,
				Metrics:             telem.Metrics,
				RuleEngineCandidate: cand,
			}
			rawResp, err := cfg.LiveClient.Analyze(ctx, diagIn)
			if err != nil {
				res.LLMEvidenceRejected = true
				if cand != nil {
					inc, _ := ai.CandidateToIncident(cand, telem.Events)
					res.AcceptedIncident = inc
				}
			} else {
				inc, valErr := ai.ValidateLLMResponse(rawResp, telem.Events, ai.Hybrid)
				if valErr != nil || inc == nil {
					res.LLMEvidenceRejected = true
					if cand != nil {
						fallbackInc, _ := ai.CandidateToIncident(cand, telem.Events)
						res.AcceptedIncident = fallbackInc
					}
				} else {
					res.AcceptedIncident = inc
				}
			}
			if res.AcceptedIncident != nil {
				res.ActualIncidentType = res.AcceptedIncident.IncidentType
				res.ActualSeverity = res.AcceptedIncident.Severity
				res.ActualAffectedNodes = res.AcceptedIncident.AffectedNodes
				res.ActualConfidence = res.AcceptedIncident.Confidence
				res.Confidence = res.AcceptedIncident.Confidence
				res.EvidenceValid = len(res.AcceptedIncident.Evidence) > 0
			}
			res.Latency = time.Since(startTime)

		default:
			return nil, fmt.Errorf("unknown evaluation mode %q", cfg.Mode)
		}

		// Accuracy calculations
		if c.IsHealthyControl {
			if res.AcceptedIncident != nil {
				res.FalsePositive = true
				res.Correct = false
				res.SeverityCorrect = false
				res.NodesCorrect = false
			} else {
				res.Correct = true
				res.SeverityCorrect = true
				res.NodesCorrect = true
				res.EvidenceValid = true // Vacuously valid
			}
		} else {
			if res.AcceptedIncident == nil {
				res.FalseNegative = true
				res.Correct = false
				res.SeverityCorrect = false
				res.NodesCorrect = false
			} else {
				res.Correct = (res.ActualIncidentType == c.ExpectedIncidentType)
				res.SeverityCorrect = (res.ActualSeverity == c.ExpectedSeverity)
				res.NodesCorrect = NodesMatch(c.ExpectedAffectedNodes, res.ActualAffectedNodes)
			}
		}

		// Ingest manual INFERENCE claim audit if present
		auditFile, err := LoadAuditFile(cfg.FixturesDir, c.ScenarioName)
		if err != nil {
			return nil, fmt.Errorf("error loading audit file for %s: %w", c.ScenarioName, err)
		}
		if auditFile != nil {
			res.AuditStatus = "COMPLETED"
			for _, cl := range auditFile.Claims {
				switch cl.Classification {
				case ClaimSupported:
					res.SupportedClaims++
				case ClaimUnsupported:
					res.UnsupportedClaims++
				case ClaimUncertain:
					res.UncertainClaims++
				}
			}
		} else {
			res.AuditStatus = "PENDING_AUDIT"
		}

		results = append(results, res)
	}

	return results, nil
}
