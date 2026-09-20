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
	generate := flag.Bool("generate", false, "Generate all evaluation fixtures into fixtures directory")
	outReport := flag.String("out", "", "Optional path to write markdown evaluation report")
	flag.Parse()

	if *generate {
		fmt.Printf("Generating evaluation fixtures in %s...\n", *fixturesDir)
		if err := aieval.GenerateAllFixtures(*fixturesDir); err != nil {
			fmt.Fprintf(os.Stderr, "Error generating fixtures: %v\n", err)
			os.Exit(1)
		}
		fmt.Println("Fixtures generated successfully.")
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
