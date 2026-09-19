package observability

import (
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestHistogram_ObservationsAndPercentiles(t *testing.T) {
	buckets := []float64{0.005, 0.010, 0.025, 0.050, 0.100}
	h := NewHistogram(buckets)

	// Observe values: 50 values at 0.004, 45 values at 0.020, 5 values at 0.080
	for i := 0; i < 50; i++ {
		h.Observe(0.004)
	}
	for i := 0; i < 45; i++ {
		h.Observe(0.020)
	}
	for i := 0; i < 5; i++ {
		h.Observe(0.080)
	}

	if h.Count() != 100 {
		t.Fatalf("expected count 100, got %d", h.Count())
	}

	p50 := h.Percentile(0.50)
	if p50 <= 0 || p50 > 0.005 {
		t.Errorf("expected p50 <= 0.005, got %g", p50)
	}

	p95 := h.Percentile(0.95)
	if p95 < 0.010 || p95 > 0.025 {
		t.Errorf("expected p95 in [0.010, 0.025], got %g", p95)
	}

	p99 := h.Percentile(0.99)
	if p99 < 0.050 || p99 > 0.100 {
		t.Errorf("expected p99 in [0.050, 0.100], got %g", p99)
	}
}

func TestHistogram_NaNAndInfGuards(t *testing.T) {
	h := NewHistogram([]float64{0.01, 0.1, 1.0})
	h.Observe(0.05)

	initialCount := h.Count()
	initialSum := h.Sum()

	// Observe NaN, +Inf, -Inf
	h.Observe(math.NaN())
	h.Observe(math.Inf(1))
	h.Observe(math.Inf(-1))

	if h.Count() != initialCount {
		t.Errorf("expected count %d unchanged after non-finite observations, got %d", initialCount, h.Count())
	}
	if h.Sum() != initialSum {
		t.Errorf("expected sum %g unchanged after non-finite observations, got %g", initialSum, h.Sum())
	}
}

func TestMetricsRegistry_OperationsAndSnapshot(t *testing.T) {
	reg := NewMetricsRegistry()

	reg.IncLeaderChanges()
	reg.IncLeaderChanges()
	reg.SetNodeUp(true)
	reg.IncEventsDropped()
	reg.IncAppendEntriesFailures("node-2")
	reg.SetReplicationLag("node-2", 3)
	reg.ObserveCommitLatency(0.015)
	reg.ObserveReadLatency(0.002)
	reg.ObserveElectionDuration(0.180, "elected")

	snaps := reg.Snapshot("node-1")
	if len(snaps) == 0 {
		t.Fatalf("expected non-empty snapshot")
	}

	foundMetrics := make(map[string]float64)
	for _, s := range snaps {
		foundMetrics[s.Name] = s.Value
	}

	if foundMetrics[MetricLeaderChangesTotal] != 2 {
		t.Errorf("expected leader_changes_total 2, got %g", foundMetrics[MetricLeaderChangesTotal])
	}
	if foundMetrics[MetricNodeUp] != 1 {
		t.Errorf("expected node_up 1, got %g", foundMetrics[MetricNodeUp])
	}
	if foundMetrics[MetricEventsDroppedTotal] != 1 {
		t.Errorf("expected events_dropped_total 1, got %g", foundMetrics[MetricEventsDroppedTotal])
	}
	if foundMetrics[MetricAppendEntriesFailuresTotal] != 1 {
		t.Errorf("expected append_entries_failures_total 1, got %g", foundMetrics[MetricAppendEntriesFailuresTotal])
	}
	if foundMetrics[MetricReplicationLag] != 3 {
		t.Errorf("expected replication_lag 3, got %g", foundMetrics[MetricReplicationLag])
	}
}

func TestMetricsHandler_PrometheusFormat(t *testing.T) {
	reg := NewMetricsRegistry()
	reg.IncLeaderChanges()
	reg.IncAppendEntriesFailures("node-2")
	reg.SetReplicationLag("node-2", 4)
	reg.ObserveCommitLatency(0.012)
	reg.ObserveReadLatency(0.003)
	reg.ObserveElectionDuration(0.150, "elected")

	handler := NewMetricsHandler(reg)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", rec.Code)
	}

	body := rec.Body.String()

	expectedSubstrings := []string{
		"leader_changes_total 1",
		`append_entries_failures_total{peer="node-2"} 1`,
		`replication_lag{peer="node-2"} 4`,
		"commit_latency_bucket{le=",
		"commit_latency_count 1",
		"read_latency_bucket{le=",
		"read_latency_count 1",
		`election_duration_bucket{outcome="elected",le=`,
		`election_duration_count{outcome="elected"} 1`,
	}

	for _, sub := range expectedSubstrings {
		if !strings.Contains(body, sub) {
			t.Errorf("expected body to contain %q, body:\n%s", sub, body)
		}
	}
}

func TestMetricsScrapeUnderLoad(t *testing.T) {
	reg := NewMetricsRegistry()
	handler := NewMetricsHandler(reg)

	const duration = 200 * time.Millisecond
	stop := make(chan struct{})
	time.AfterFunc(duration, func() { close(stop) })

	var wg sync.WaitGroup

	// Metric observation workers
	for w := 0; w < 5; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					reg.IncLeaderChanges()
					reg.ObserveCommitLatency(0.005)
					reg.ObserveReadLatency(0.001)
					reg.IncAppendEntriesFailures("node-2")
					reg.SetReplicationLag("node-2", 2)
				}
			}
		}()
	}

	// Concurrent HTTP scrape workers
	for w := 0; w < 3; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
					rec := httptest.NewRecorder()
					handler.ServeHTTP(rec, req)
					if rec.Code != http.StatusOK {
						t.Errorf("scrape returned non-200: %d", rec.Code)
					}
				}
			}
		}()
	}

	wg.Wait()
}
