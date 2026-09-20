package ai

import (
	"context"
	"sync"
	"time"

	"raftkv/internal/observability"
)

// TimeRange defines a time window for telemetry queries.
type TimeRange struct {
	Start time.Time
	End   time.Time
}

// DiagnosticsEngine coordinates the deterministic RuleEngine, optional LLMClient,
// and the authoritative Validator to produce evidence-grounded AIIncidents.
type DiagnosticsEngine struct {
	ruleEngine *DeterministicRuleEngine
	llmClient  LLMClient
}

// NewDiagnosticsEngine creates a DiagnosticsEngine with the given rule engine and optional LLMClient.
func NewDiagnosticsEngine(ruleEngine *DeterministicRuleEngine, llmClient LLMClient) *DiagnosticsEngine {
	if ruleEngine == nil {
		ruleEngine = NewDeterministicRuleEngine()
	}
	return &DiagnosticsEngine{
		ruleEngine: ruleEngine,
		llmClient:  llmClient,
	}
}

// Diagnose executes the rules-first hybrid diagnosis pipeline per docs/ai-design.md:
// 1. Sanitize telemetry at ingest boundary.
// 2. Deterministic rule engine runs first and always.
// 3. If LLMClient is present, query it for refinement/exploration with rule candidate as context.
// 4. Validate LLM response through the authoritative evidence validator.
// 5. On any LLM failure or rejection: fall back fail-open to the rule-engine candidate (or nil).
func (e *DiagnosticsEngine) Diagnose(ctx context.Context, rawEvents []observability.ClusterEvent, rawMetrics []observability.MetricSnapshot) (*AIIncident, error) {
	// 1. Ingest sanitization: exclude unsupported schema versions and non-finite metrics
	events, metrics := SanitizeTelemetry(rawEvents, rawMetrics)

	// 2. Deterministic rule engine always runs
	ruleCandidate := e.ruleEngine.Evaluate(events, metrics)

	// If no LLMClient configured: convert rule candidate directly to incident
	if e.llmClient == nil {
		return CandidateToIncident(ruleCandidate, events)
	}

	// 3. LLM refinement/exploration
	input := DiagnosisInput{
		Events:              events,
		Metrics:             metrics,
		RuleEngineCandidate: ruleCandidate,
	}

	llmResp, err := e.llmClient.Analyze(ctx, input)
	if err != nil {
		// Fail-open: LLM call error/timeout falls back to rule-engine candidate (or nil)
		return CandidateToIncident(ruleCandidate, events)
	}

	// 4. Validate LLM response
	source := LLM
	if ruleCandidate != nil {
		source = Hybrid
	}

	incident, valErr := ValidateLLMResponse(llmResp, events, source)
	if valErr != nil {
		// Fail-open: validation rejection falls back to rule-engine candidate (or nil)
		return CandidateToIncident(ruleCandidate, events)
	}

	return incident, nil
}

// AIWorker is an in-process background worker running inside the raftkv process
// per docs/ai-design.md's deployment model. It periodically reads from an observability
// LiveBuffer and runs diagnosis asynchronously to the Raft consensus / client path.
type AIWorker struct {
	engine     *DiagnosticsEngine
	buffer     *observability.LiveBuffer
	interval   time.Duration
	window     time.Duration
	mu         sync.RWMutex
	latest     *AIIncident
	cancel     context.CancelFunc
	stopCh     chan struct{}
	doneCh     chan struct{}
	running    bool
	workerLock sync.Mutex
}

// NewAIWorker creates an in-process AIWorker.
func NewAIWorker(engine *DiagnosticsEngine, buffer *observability.LiveBuffer, interval, window time.Duration) *AIWorker {
	if interval <= 0 {
		interval = 500 * time.Millisecond
	}
	if window <= 0 {
		window = 60 * time.Second
	}
	return &AIWorker{
		engine:   engine,
		buffer:   buffer,
		interval: interval,
		window:   window,
	}
}

// Start begins the in-process worker loop in a background goroutine.
func (w *AIWorker) Start(ctx context.Context) {
	w.workerLock.Lock()
	defer w.workerLock.Unlock()

	if w.running {
		return
	}

	workerCtx, cancel := context.WithCancel(ctx)
	w.cancel = cancel
	w.stopCh = make(chan struct{})
	w.doneCh = make(chan struct{})
	w.running = true

	go w.runLoop(workerCtx)
}

// Stop gracefully shuts down the worker goroutine.
func (w *AIWorker) Stop() {
	w.workerLock.Lock()
	if !w.running {
		w.workerLock.Unlock()
		return
	}
	w.running = false
	if w.cancel != nil {
		w.cancel()
	}
	close(w.stopCh)
	w.workerLock.Unlock()

	<-w.doneCh
}

// LatestIncident returns the most recently diagnosed incident, or nil.
func (w *AIWorker) LatestIncident() *AIIncident {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.latest
}

func (w *AIWorker) runLoop(ctx context.Context) {
	defer close(w.doneCh)
	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-w.stopCh:
			return
		case <-ticker.C:
			w.runOnce(ctx)
		}
	}
}

func (w *AIWorker) runOnce(ctx context.Context) {
	if w.buffer == nil {
		return
	}

	events := w.buffer.Snapshot()
	if len(events) == 0 {
		return
	}

	diagCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	inc, err := w.engine.Diagnose(diagCtx, events, nil)
	cancel()

	if err == nil {
		w.mu.Lock()
		w.latest = inc
		w.mu.Unlock()
	}
}
