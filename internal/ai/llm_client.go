package ai

import (
	"context"
	"errors"
	"sync"
	"time"
)

// LLMClient defines the pluggable interface for LLM-based incident refinement per docs/ai-design.md.
type LLMClient interface {
	Analyze(ctx context.Context, input DiagnosisInput) (LLMResponse, error)
}

// FakeLLM is a controllable mock implementation of LLMClient for testing and evaluation.
type FakeLLM struct {
	mu           sync.Mutex
	response     LLMResponse
	err          error
	delay        time.Duration
	analyzeCalls int
	lastInput    DiagnosisInput
}

// NewFakeLLM creates a FakeLLM with default empty response.
func NewFakeLLM() *FakeLLM {
	return &FakeLLM{}
}

// SetResponse configures the response returned by Analyze.
func (f *FakeLLM) SetResponse(resp LLMResponse) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.response = resp
	f.err = nil
}

// SetError configures an error returned by Analyze.
func (f *FakeLLM) SetError(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.err = err
}

// SetDelay injects latency into Analyze calls (useful for timeout and structural async tests).
func (f *FakeLLM) SetDelay(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.delay = d
}

// AnalyzeCalls returns the number of times Analyze was called.
func (f *FakeLLM) AnalyzeCalls() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.analyzeCalls
}

// LastInput returns the most recent DiagnosisInput passed to Analyze.
func (f *FakeLLM) LastInput() DiagnosisInput {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastInput
}

// Analyze executes the mock analysis.
func (f *FakeLLM) Analyze(ctx context.Context, input DiagnosisInput) (LLMResponse, error) {
	f.mu.Lock()
	delay := f.delay
	resp := f.response
	err := f.err
	f.analyzeCalls++
	f.lastInput = input
	f.mu.Unlock()

	if delay > 0 {
		select {
		case <-ctx.Done():
			return LLMResponse{}, ctx.Err()
		case <-time.After(delay):
		}
	}

	if err != nil {
		return LLMResponse{}, err
	}

	return resp, nil
}

// ErrLLMUnavailable is a common error simulating LLM service unavailability.
var ErrLLMUnavailable = errors.New("llm service unavailable")
