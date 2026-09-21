package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	aieval "raftkv/tests/ai_eval"
)

func main() {
	mode := flag.String("mode", "recorded", "Evaluation mode: recorded (official), rules, or live")
	fixturesDir := flag.String("fixtures", "tests/ai_eval/fixtures", "Directory containing evaluation fixtures")
	generate := flag.Bool("generate", false, "Generate synthetic evaluation telemetry fixtures")
	capture := flag.Bool("capture", false, "Run live LLM to capture genuine raw model responses into fixtures/llm_responses")
	dumpAudit := flag.Bool("dump-audit", false, "Extract accepted INFERENCE claims and create draft audit templates (PENDING_AUDIT)")
	apiKey := flag.String("api-key", "", "Google Gemini API key (or read from GEMINI_API_KEY)")
	model := flag.String("model", "gemini-3.5-flash-lite", "Gemini model name")
	outReport := flag.String("out", "", "Optional path to write markdown evaluation report")
	flag.Parse()

	if *generate {
		fmt.Printf("Generating evaluation telemetry fixtures in %s...\n", *fixturesDir)
		if err := aieval.GenerateAllFixtures(*fixturesDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating fixtures: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Telemetry fixtures generated successfully.")
		return
	}

	if *capture {
		fmt.Printf("Capturing genuine live LLM responses using model %s into %s...\n", *model, *fixturesDir)
		client, err := aieval.NewGeminiLiveClient(*apiKey, *model)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error initializing Gemini client: %v\n", err)
			os.Exit(1)
		}
		if err := aieval.CaptureLiveFixtures(context.Background(), *fixturesDir, client); err != nil {
			fmt.Fprintf(os.Stderr, "Error capturing live fixtures: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Live responses captured successfully.")
		return
	}

	if *dumpAudit {
		fmt.Printf("Dumping draft INFERENCE audit templates into %s/audits...\n", *fixturesDir)
		if err := aieval.DumpAuditTemplates(*fixturesDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error dumping audit templates: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Draft audit templates created.")
		return
	}

	// Auto-generate fixtures if the events directory doesn't exist yet
	if _, err := os.Stat(filepath.Join(*fixturesDir, "events")); os.IsNotExist(err) {
		fmt.Printf("Fixtures directory %s not found. Auto-generating fixtures...\n", *fixturesDir)
		if err := aieval.GenerateAllFixtures(*fixturesDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating fixtures: %v\n", err)
			os.Exit(1)
		}
	}

	var latencyLabel string
	switch *mode {
	case "rules":
		latencyLabel = "deterministic rule engine latency"
	case "recorded":
		latencyLabel = "analysis/evaluation latency (recorded mode; NOT representative of real model latency)"
	case "live":
		latencyLabel = "live LLM end-to-end diagnosis latency"
	default:
		fmt.Fprintf(os.Stderr, "Invalid mode: %s. Must be 'rules', 'recorded', or 'live'\n", *mode)
		os.Exit(1)
	}

	cfg := aieval.RunnerConfig{
		Mode:        *mode,
		FixturesDir: *fixturesDir,
	}

	cases := aieval.AllEvaluationCases()
	results, err := aieval.RunEvaluation(context.Background(), cfg, cases)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error during evaluation run: %v\n", err)
		os.Exit(1)
	}

	report := aieval.CalculateReport(*mode, results, latencyLabel)
	reportMd := aieval.FormatReportMarkdown(report)

	fmt.Println(reportMd)

	if *outReport != "" {
		if err := os.WriteFile(*outReport, []byte(reportMd), 0644); err != nil {
			fmt.Fprintf(os.Stderr, "Error writing report to %s: %v\n", *outReport, err)
			os.Exit(1)
		}
		fmt.Printf("Report saved to %s\n", *outReport)
	}
}
