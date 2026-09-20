package aieval

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

// CalculateReport compiles an aggregate EvalReport from a slice of EvalResults.
func CalculateReport(mode string, results []EvalResult, latencyLabel string) EvalReport {
	report := EvalReport{
		Mode:               mode,
		TotalScenarios:     len(results),
		CalibrationBuckets: make(map[string]BucketStats),
		LatencyLabel:       latencyLabel,
		Results:            results,
	}

	report.CalibrationBuckets["[0.0, 0.5)"] = BucketStats{Range: "[0.0, 0.5)"}
	report.CalibrationBuckets["[0.5, 0.8)"] = BucketStats{Range: "[0.5, 0.8)"}
	report.CalibrationBuckets["[0.8, 1.0]"] = BucketStats{Range: "[0.8, 1.0]"}

	var totalCorrect, totalSeverityCorrect, totalNodesCorrect int
	var totalAccepted, validEvidenceAccepted int
	var totalLLMRejected int
	var totalSupported, totalUnsupported, totalUncertain int
	var totalLatency time.Duration

	for _, r := range results {
		if r.Case.IsHeldOut {
			report.HeldOutScenarios++
		} else if !r.Case.IsHealthyControl {
			report.PrimaryScenarios++
		}

		if r.Correct {
			totalCorrect++
		}
		if r.SeverityCorrect {
			totalSeverityCorrect++
		}
		if r.NodesCorrect {
			totalNodesCorrect++
		}
		if r.LLMEvidenceRejected {
			totalLLMRejected++
		}
		if r.FalsePositive {
			report.FalsePositiveCount++
		}
		if r.FalseNegative {
			report.FalseNegativeCount++
		}

		if r.AcceptedIncident != nil {
			totalAccepted++
			if r.EvidenceValid {
				validEvidenceAccepted++
			}
		}

		totalSupported += r.SupportedClaims
		totalUnsupported += r.UnsupportedClaims
		totalUncertain += r.UncertainClaims
		totalLatency += r.Latency

		// Calibration bucketing
		bucketKey := getConfidenceBucket(r.Confidence)
		b := report.CalibrationBuckets[bucketKey]
		b.Count++
		if r.Correct {
			b.Correct++
		}
		report.CalibrationBuckets[bucketKey] = b
	}

	if len(results) > 0 {
		report.ClassificationAccuracy = float64(totalCorrect) / float64(len(results)) * 100.0
		report.SeverityAccuracy = float64(totalSeverityCorrect) / float64(len(results)) * 100.0
		report.NodeAccuracy = float64(totalNodesCorrect) / float64(len(results)) * 100.0
		report.LLMRejectionRate = float64(totalLLMRejected) / float64(len(results)) * 100.0
		report.AverageLatency = totalLatency / time.Duration(len(results))
	}

	if totalAccepted > 0 {
		report.AcceptedEvidenceValid = float64(validEvidenceAccepted) / float64(totalAccepted) * 100.0
	} else {
		report.AcceptedEvidenceValid = 100.0
	}

	// Calculate calibration bucket accuracy
	for k, b := range report.CalibrationBuckets {
		if b.Count > 0 {
			b.Accuracy = float64(b.Correct) / float64(b.Count) * 100.0
		}
		report.CalibrationBuckets[k] = b
	}

	// Unsupported-claim rate calculation (docs/ai-design.md formula):
	// unsupported_claim_rate = count(UNSUPPORTED) / (count(SUPPORTED) + count(UNSUPPORTED))
	// UNCERTAIN is explicitly excluded from the denominator.
	evaluableClaims := totalSupported + totalUnsupported
	report.EvaluableInferenceClaims = evaluableClaims
	if evaluableClaims > 0 {
		report.UnsupportedClaimRate = float64(totalUnsupported) / float64(evaluableClaims) * 100.0
	} else {
		report.UnsupportedClaimRate = 0.0
	}

	totalReviewedClaims := totalSupported + totalUnsupported + totalUncertain
	report.TotalInferenceClaimsReviewed = totalReviewedClaims
	if totalReviewedClaims > 0 {
		report.UncertainClaimFraction = float64(totalUncertain) / float64(totalReviewedClaims) * 100.0
	}

	return report
}

func getConfidenceBucket(c float64) string {
	if c < 0.5 {
		return "[0.0, 0.5)"
	}
	if c < 0.8 {
		return "[0.5, 0.8)"
	}
	return "[0.8, 1.0]"
}

// FormatReportMarkdown renders the full evaluation report matching docs/phases/phase-09.md specifications.
func FormatReportMarkdown(report EvalReport) string {
	var sb strings.Builder

	sb.WriteString(fmt.Sprintf("# AI Layer Evaluation Report (Mode: %s)\n\n", report.Mode))
	sb.WriteString(fmt.Sprintf("- **Generated at:** %s\n", time.Now().UTC().Format(time.RFC3339)))
	sb.WriteString(fmt.Sprintf("- **Total scenarios evaluated:** %d (%d primary, %d held-out, 1 healthy control)\n",
		report.TotalScenarios, report.PrimaryScenarios, report.HeldOutScenarios))
	sb.WriteString(fmt.Sprintf("- **Latency measurement (%s):** %v average per scenario\n\n",
		report.LatencyLabel, report.AverageLatency.Round(time.Microsecond)))

	sb.WriteString("## Executive Metric Summary\n\n")
	sb.WriteString("| Metric | Result | Target / Requirement | Status |\n")
	sb.WriteString("|---|---|---|---|\n")
	sb.WriteString(fmt.Sprintf("| **Classification Accuracy** | %.1f%% | Initial project threshold | %s |\n",
		report.ClassificationAccuracy, passStatus(report.ClassificationAccuracy >= 80.0)))
	sb.WriteString(fmt.Sprintf("| **Severity Accuracy (`SeverityCorrect`)** | %.1f%% | Reported separately (not blended) | %s |\n",
		report.SeverityAccuracy, passStatus(report.SeverityAccuracy >= 80.0)))
	sb.WriteString(fmt.Sprintf("| **Affected-Nodes Accuracy** | %.1f%% | Evidence-bound grounding | %s |\n",
		report.NodeAccuracy, passStatus(report.NodeAccuracy >= 80.0)))
	sb.WriteString(fmt.Sprintf("| **Accepted-Output Evidence Validity** | %.1f%% | **100.0%% (hard, non-negotiable)** | %s |\n",
		report.AcceptedEvidenceValid, passStatus(report.AcceptedEvidenceValid == 100.0)))
	sb.WriteString(fmt.Sprintf("| **Raw LLM Evidence Rejection Rate** | %.1f%% | Reported honestly (fail-open) | INFO |\n",
		report.LLMRejectionRate))
	sb.WriteString(fmt.Sprintf("| **False Positive Count** | %d | **0 on 30m healthy control** | %s |\n",
		report.FalsePositiveCount, passStatus(report.FalsePositiveCount == 0)))
	sb.WriteString(fmt.Sprintf("| **False Negative Count** | %d | 0 on synthetic failure set | %s |\n",
		report.FalseNegativeCount, passStatus(report.FalseNegativeCount == 0)))

	sb.WriteString("\n## Unsupported-Claim Rate (Inference Claim Audit)\n\n")
	sb.WriteString("> **Methodology Note (docs/ai-design.md):** `OBSERVATION` claims are mechanically checked with 0% unsupported claims by construction (validator rejects on failure). `INFERENCE` claims are evaluated via a manual audit by a single self-reviewer against a three-way rubric (`SUPPORTED`, `UNSUPPORTED`, `UNCERTAIN`). `UNCERTAIN` claims are explicitly excluded from the denominator of the unsupported rate.\n\n")
	if report.EvaluableInferenceClaims == 0 {
		sb.WriteString("- **Inference Unsupported-Claim Rate:** `N/A` (no evaluable INFERENCE claims / audit pending)\n")
	} else {
		sb.WriteString(fmt.Sprintf("- **Inference Unsupported-Claim Rate:** `%.2f%%` (Formula: `count(UNSUPPORTED) / (count(SUPPORTED) + count(UNSUPPORTED))`)\n", report.UnsupportedClaimRate))
	}
	if report.TotalInferenceClaimsReviewed == 0 {
		sb.WriteString("- **Uncertain Claims Fraction:** `N/A` (no claims reviewed)\n\n")
	} else {
		sb.WriteString(fmt.Sprintf("- **Uncertain Claims Fraction:** `%.2f%%` (reported separately, excluded from unsupported denominator)\n\n", report.UncertainClaimFraction))
	}

	sb.WriteString("## Confidence Calibration\n\n")
	sb.WriteString("| Confidence Bucket | Sample Count | Correct Diagnoses | Accuracy |\n")
	sb.WriteString("|---|---|---|---|\n")
	for _, k := range []string{"[0.0, 0.5)", "[0.5, 0.8)", "[0.8, 1.0]"} {
		b := report.CalibrationBuckets[k]
		sb.WriteString(fmt.Sprintf("| `%s` | %d | %d | %.1f%% |\n", b.Range, b.Count, b.Correct, b.Accuracy))
	}

	sb.WriteString("\n## Detailed Scenario Results\n\n")
	sb.WriteString("| Scenario | Expected Type | Actual Type | Expected Sev | Actual Sev | Type Match | Sev Match | Nodes Match | Evidence Valid | LLM Rejected |\n")
	sb.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
	for _, r := range report.Results {
		sb.WriteString(fmt.Sprintf("| `%s` | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n",
			r.Case.ScenarioName,
			emptyDash(string(r.Case.ExpectedIncidentType)),
			emptyDash(string(r.ActualIncidentType)),
			emptyDash(string(r.Case.ExpectedSeverity)),
			emptyDash(string(r.ActualSeverity)),
			boolCheck(r.Correct),
			boolCheck(r.SeverityCorrect),
			boolCheck(r.NodesCorrect),
			boolCheck(r.EvidenceValid),
			boolCheck(r.LLMEvidenceRejected),
		))
	}

	return sb.String()
}

func passStatus(ok bool) string {
	if ok {
		return "PASS"
	}
	return "FAIL"
}

func boolCheck(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

func emptyDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// NodesMatch compares expected and actual affected node lists ignoring order.
func NodesMatch(expected, actual []string) bool {
	if len(expected) == 0 && len(actual) == 0 {
		return true
	}
	if len(expected) != len(actual) {
		return false
	}
	expSorted := make([]string, len(expected))
	copy(expSorted, expected)
	actSorted := make([]string, len(actual))
	copy(actSorted, actual)
	sort.Strings(expSorted)
	sort.Strings(actSorted)
	for i := range expSorted {
		if expSorted[i] != actSorted[i] {
			return false
		}
	}
	return true
}
