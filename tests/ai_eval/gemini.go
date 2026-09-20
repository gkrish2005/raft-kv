package aieval

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"time"

	"raftkv/internal/ai"
)

// GeminiLiveClient implements ai.LLMClient by calling the Google Gemini API.
type GeminiLiveClient struct {
	APIKey     string
	Model      string
	HTTPClient *http.Client
}

// NewGeminiLiveClient creates a new Gemini client using the provided API key or reading GEMINI_API_KEY.
func NewGeminiLiveClient(apiKey, model string) (*GeminiLiveClient, error) {
	if apiKey == "" {
		apiKey = os.Getenv("GEMINI_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("GEMINI_API_KEY is not set in environment or flag")
	}
	if model == "" {
		model = "gemini-2.5-flash"
	}
	return &GeminiLiveClient{
		APIKey: apiKey,
		Model:  model,
		HTTPClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}, nil
}

type geminiRequest struct {
	Contents         []geminiContent `json:"contents"`
	GenerationConfig *genConfig      `json:"generationConfig,omitempty"`
}

type geminiContent struct {
	Parts []geminiPart `json:"parts"`
}

type geminiPart struct {
	Text string `json:"text"`
}

type genConfig struct {
	ResponseMIMEType string `json:"response_mime_type"`
}

type geminiResponse struct {
	Candidates []struct {
		Content struct {
			Parts []struct {
				Text string `json:"text"`
			} `json:"parts"`
		} `json:"content"`
	} `json:"candidates"`
	Error *struct {
		Message string `json:"message"`
		Code    int    `json:"code"`
	} `json:"error,omitempty"`
}

// Analyze sends the diagnosis input to Gemini and parses the response into ai.LLMResponse.
func (c *GeminiLiveClient) Analyze(ctx context.Context, input ai.DiagnosisInput) (ai.LLMResponse, error) {
	prompt := buildDiagnosisPrompt(input)

	reqBody := geminiRequest{
		Contents: []geminiContent{
			{
				Parts: []geminiPart{
					{Text: prompt},
				},
			},
		},
		GenerationConfig: &genConfig{
			ResponseMIMEType: "application/json",
		},
	}

	reqBytes, err := json.Marshal(reqBody)
	if err != nil {
		return ai.LLMResponse{}, fmt.Errorf("failed to serialize Gemini request: %w", err)
	}

	url := fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:generateContent?key=%s", c.Model, c.APIKey)
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(reqBytes))
	if err != nil {
		return ai.LLMResponse{}, fmt.Errorf("failed to create HTTP request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return ai.LLMResponse{}, fmt.Errorf("gemini API call failed: %w", err)
	}
	defer resp.Body.Close()

	respBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return ai.LLMResponse{}, fmt.Errorf("failed to read Gemini response: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return ai.LLMResponse{}, fmt.Errorf("gemini API returned status %d: %s", resp.StatusCode, string(respBytes))
	}

	var geminiResp geminiResponse
	if err := json.Unmarshal(respBytes, &geminiResp); err != nil {
		return ai.LLMResponse{}, fmt.Errorf("failed to parse Gemini response wrapper: %w", err)
	}

	if geminiResp.Error != nil {
		return ai.LLMResponse{}, fmt.Errorf("gemini API error (%d): %s", geminiResp.Error.Code, geminiResp.Error.Message)
	}

	if len(geminiResp.Candidates) == 0 || len(geminiResp.Candidates[0].Content.Parts) == 0 {
		return ai.LLMResponse{}, fmt.Errorf("gemini returned zero candidate parts")
	}

	rawJSONText := geminiResp.Candidates[0].Content.Parts[0].Text

	var llmResp ai.LLMResponse
	if err := json.Unmarshal([]byte(rawJSONText), &llmResp); err != nil {
		return ai.LLMResponse{}, fmt.Errorf("failed to parse model output into LLMResponse: %w (raw output: %s)", err, rawJSONText)
	}

	return llmResp, nil
}

func buildDiagnosisPrompt(input ai.DiagnosisInput) string {
	var sb bytes.Buffer
	sb.WriteString("You are an expert distributed systems reliability engineer diagnosing a Raft cluster incident based on structured telemetry.\n\n")
	sb.WriteString("Analyze the following cluster telemetry events and metrics, and provide a diagnosis formatted as a JSON object matching this exact schema:\n")
	sb.WriteString("{\n")
	sb.WriteString("  \"incident_type\": \"LEADER_INSTABILITY\" | \"NODE_UNREACHABLE\" | \"REPLICATION_LAG\" | \"SLOW_FOLLOWER\" | \"ELECTION_STORM\" | \"NETWORK_PARTITION\",\n")
	sb.WriteString("  \"severity\": \"LOW\" | \"MEDIUM\" | \"HIGH\" | \"CRITICAL\",\n")
	sb.WriteString("  \"affected_nodes\": [\"node-1\", ...],\n")
	sb.WriteString("  \"consistency_impact\": \"NONE\" | \"WRITES_UNAVAILABLE\" | \"READS_UNAVAILABLE\" | \"COMMIT_PROGRESS_BLOCKED\",\n")
	sb.WriteString("  \"confidence\": 0.0 to 1.0,\n")
	sb.WriteString("  \"recommended_actions\": [\"...\"],\n")
	sb.WriteString("  \"claims\": [\n")
	sb.WriteString("    {\n")
	sb.WriteString("      \"claim\": \"...\",\n")
	sb.WriteString("      \"evidence_ids\": [\"event-id-1\", ...],\n")
	sb.WriteString("      \"claim_type\": \"OBSERVATION\" | \"INFERENCE\"\n")
	sb.WriteString("    }\n")
	sb.WriteString("  ]\n")
	sb.WriteString("}\n\n")

	sb.WriteString("CRITICAL GROUNDING RULES:\n")
	sb.WriteString("1. Every node in affected_nodes MUST be mentioned in at least one cited evidence event.\n")
	sb.WriteString("2. Every claim must cite real event_ids from the telemetry below.\n")
	sb.WriteString("3. OBSERVATION claims must be factual restatements of cited event fields. Higher-level deductions must be marked INFERENCE.\n")
	sb.WriteString("4. consistency_impact must describe observable effect on client/consensus progress, NEVER root cause.\n\n")

	if input.RuleEngineCandidate != nil {
		candJSON, _ := json.MarshalIndent(input.RuleEngineCandidate, "", "  ")
		sb.WriteString("Rule engine candidate for context:\n")
		sb.WriteString(string(candJSON))
		sb.WriteString("\n\n")
	}

	eventsJSON, _ := json.MarshalIndent(input.Events, "", "  ")
	sb.WriteString("Cluster events in analysis window:\n")
	sb.WriteString(string(eventsJSON))
	sb.WriteString("\n\n")

	if len(input.Metrics) > 0 {
		metricsJSON, _ := json.MarshalIndent(input.Metrics, "", "  ")
		sb.WriteString("Metric snapshots:\n")
		sb.WriteString(string(metricsJSON))
		sb.WriteString("\n\n")
	}

	sb.WriteString("Provide your diagnosis JSON now.")
	return sb.String()
}
