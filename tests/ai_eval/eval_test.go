package aieval_test

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	aieval "raftkv/tests/ai_eval"
)

func TestAIEval_GenerateAndRunRecordedMode(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raftkv-ai-eval-test-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 1. Generate all fixtures
	if err := aieval.GenerateAllFixtures(tempDir); err != nil {
		t.Fatalf("failed to generate fixtures: %v", err)
	}

	// 2. Run recorded evaluation
	cfg := aieval.RunnerConfig{
		Mode:        "recorded",
		FixturesDir: tempDir,
	}
	cases := aieval.AllEvaluationCases()
	results, err := aieval.RunEvaluation(context.Background(), cfg, cases)
	if err != nil {
		t.Fatalf("evaluation run failed: %v", err)
	}

	// 3. Compile report
	report := aieval.CalculateReport("recorded", results, "analysis/evaluation latency (recorded mode)")

	// 4. Assert Exit Criteria
	// Exit Criterion 4: Accepted-output evidence-validity = 100% (hard requirement)
	if report.AcceptedEvidenceValid != 100.0 {
		t.Errorf("expected 100.0%% accepted evidence validity, got %.2f%%", report.AcceptedEvidenceValid)
	}

	// Exit Criterion 6: Healthy-cluster false-positive control run (0 false positives)
	if report.FalsePositiveCount != 0 {
		t.Errorf("expected 0 false positives on healthy control, got %d", report.FalsePositiveCount)
	}

	// Exit Criterion 5: Severity accuracy reported as its own distinct metric
	if report.SeverityAccuracy < 80.0 {
		t.Errorf("expected severity accuracy >= 80%%, got %.2f%%", report.SeverityAccuracy)
	}

	// Classification accuracy
	if report.ClassificationAccuracy < 80.0 {
		t.Errorf("expected classification accuracy >= 80%%, got %.2f%%", report.ClassificationAccuracy)
	}

	// Verify report markdown formatting includes required methodology note
	md := aieval.FormatReportMarkdown(report)
	if !strings.Contains(md, "Methodology Note (docs/ai-design.md)") {
		t.Errorf("report markdown missing mandatory methodology note")
	}
	if !strings.Contains(md, "UNCERTAIN") {
		t.Errorf("report markdown missing UNCERTAIN claim handling")
	}
}

func TestAIEval_RulesMode(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raftkv-ai-eval-test-rules-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := aieval.GenerateAllFixtures(tempDir); err != nil {
		t.Fatalf("failed to generate fixtures: %v", err)
	}

	cfg := aieval.RunnerConfig{
		Mode:        "rules",
		FixturesDir: tempDir,
	}
	cases := aieval.AllEvaluationCases()
	results, err := aieval.RunEvaluation(context.Background(), cfg, cases)
	if err != nil {
		t.Fatalf("rules evaluation run failed: %v", err)
	}

	report := aieval.CalculateReport("rules", results, "deterministic rule engine latency")

	if report.ClassificationAccuracy < 80.0 {
		t.Errorf("expected rules mode classification accuracy >= 80%%, got %.2f%%", report.ClassificationAccuracy)
	}
	if report.AcceptedEvidenceValid != 100.0 {
		t.Errorf("expected 100.0%% evidence validity, got %.2f%%", report.AcceptedEvidenceValid)
	}
}

func TestAIEval_UnsupportedClaimRate_ExcludesUncertainFromDenominator(t *testing.T) {
	// Formula verification test:
	// If Supported = 8, Unsupported = 2, Uncertain = 5:
	// Total reviewed claims = 15
	// Evaluable claims (denominator) = 8 + 2 = 10 (UNCERTAIN excluded!)
	// Unsupported claim rate = 2 / 10 = 20.0%
	// Uncertain claim fraction = 5 / 15 = 33.33%
	results := []aieval.EvalResult{
		{
			Case:              aieval.EvalCase{ScenarioName: "test_scenario"},
			SupportedClaims:   8,
			UnsupportedClaims: 2,
			UncertainClaims:   5,
			Confidence:        0.9,
			Latency:           10 * time.Millisecond,
		},
	}

	report := aieval.CalculateReport("recorded", results, "evaluation latency")

	expectedUnsupportedRate := 20.0 // 2 / (8 + 2) * 100
	if report.UnsupportedClaimRate != expectedUnsupportedRate {
		t.Errorf("expected unsupported claim rate %.2f%%, got %.2f%%", expectedUnsupportedRate, report.UnsupportedClaimRate)
	}

	expectedUncertainFraction := 5.0 / 15.0 * 100.0
	if report.UncertainClaimFraction < expectedUncertainFraction-0.01 || report.UncertainClaimFraction > expectedUncertainFraction+0.01 {
		t.Errorf("expected uncertain claim fraction %.2f%%, got %.2f%%", expectedUncertainFraction, report.UncertainClaimFraction)
	}
}

func TestAIEval_PendingAuditDoesNotCrash(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "raftkv-ai-eval-test-noaudit-*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	if err := aieval.GenerateAllFixtures(tempDir); err != nil {
		t.Fatalf("failed to generate fixtures: %v", err)
	}

	// Remove all audit fixtures to simulate Stage 1 (before human audit)
	auditsDir := filepath.Join(tempDir, "audits")
	if err := os.RemoveAll(auditsDir); err != nil {
		t.Fatalf("failed to remove audits dir: %v", err)
	}

	cfg := aieval.RunnerConfig{
		Mode:        "recorded",
		FixturesDir: tempDir,
	}
	cases := aieval.PrimaryEvaluationCases()
	results, err := aieval.RunEvaluation(context.Background(), cfg, cases)
	if err != nil {
		t.Fatalf("runner should not fail when audit files are missing: %v", err)
	}

	for _, r := range results {
		if r.AuditStatus != "PENDING_AUDIT" {
			t.Errorf("expected PENDING_AUDIT status when audit files missing, got %s", r.AuditStatus)
		}
	}

	report := aieval.CalculateReport("recorded", results, "evaluation latency")
	md := aieval.FormatReportMarkdown(report)
	if !strings.Contains(md, "`N/A` (no evaluable INFERENCE claims / audit pending)") {
		t.Errorf("expected `N/A` for 0/0 unsupported claim rate, got report:\n%s", md)
	}
	if strings.Contains(md, "0.00%") && strings.Contains(md, "Inference Unsupported-Claim Rate: 0.00%") {
		t.Errorf("0/0 must never render as 0.00%%")
	}
}
