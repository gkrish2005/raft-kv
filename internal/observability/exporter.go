package observability

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"sync/atomic"
)

// NewMetricsHandler returns an http.Handler that exports metrics in Prometheus text exposition format.
// It reads exclusively from MetricsRegistry's atomic counters and lock-free histograms,
// never acquiring Raft or StateMachine locks (Rule 32).
func NewMetricsHandler(registry *MetricsRegistry) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")

		// leader_changes_total
		fmt.Fprintf(w, "# HELP %s Number of times this node became leader\n", MetricLeaderChangesTotal)
		fmt.Fprintf(w, "# TYPE %s counter\n", MetricLeaderChangesTotal)
		fmt.Fprintf(w, "%s %d\n", MetricLeaderChangesTotal, registry.leaderChanges.Load())

		// node_up
		fmt.Fprintf(w, "# HELP %s 1 if node is up and running, 0 otherwise\n", MetricNodeUp)
		fmt.Fprintf(w, "# TYPE %s gauge\n", MetricNodeUp)
		fmt.Fprintf(w, "%s %d\n", MetricNodeUp, registry.nodeUp.Load())

		// events_dropped_total
		fmt.Fprintf(w, "# HELP %s Number of events dropped by bounded ring buffer\n", MetricEventsDroppedTotal)
		fmt.Fprintf(w, "# TYPE %s counter\n", MetricEventsDroppedTotal)
		fmt.Fprintf(w, "%s %d\n", MetricEventsDroppedTotal, registry.eventsDropped.Load())

		// append_entries_failures_total
		fmt.Fprintf(w, "# HELP %s AppendEntries RPC failures to peers\n", MetricAppendEntriesFailuresTotal)
		fmt.Fprintf(w, "# TYPE %s counter\n", MetricAppendEntriesFailuresTotal)
		var failurePeers []string
		registry.appendFailures.Range(func(key, _ any) bool {
			failurePeers = append(failurePeers, key.(string))
			return true
		})
		sort.Strings(failurePeers)
		for _, peer := range failurePeers {
			if val, ok := registry.appendFailures.Load(peer); ok {
				fmt.Fprintf(w, "%s{peer=%q} %d\n", MetricAppendEntriesFailuresTotal, peer, val.(*atomic.Uint64).Load())
			}
		}

		// replication_lag
		fmt.Fprintf(w, "# HELP %s Replication log index lag behind leader\n", MetricReplicationLag)
		fmt.Fprintf(w, "# TYPE %s gauge\n", MetricReplicationLag)
		var lagPeers []string
		registry.replicationLag.Range(func(key, _ any) bool {
			lagPeers = append(lagPeers, key.(string))
			return true
		})
		sort.Strings(lagPeers)
		for _, peer := range lagPeers {
			if val, ok := registry.replicationLag.Load(peer); ok {
				fmt.Fprintf(w, "%s{peer=%q} %d\n", MetricReplicationLag, peer, val.(*atomic.Uint64).Load())
			}
		}

		// commit_latency (Histogram)
		formatHistogram(w, MetricCommitLatency, "Commit latency in seconds", registry.commitLatency, nil)

		// read_latency (Histogram)
		formatHistogram(w, MetricReadLatency, "Linearizable read latency in seconds", registry.readLatency, nil)

		// election_duration (Histogram per outcome)
		fmt.Fprintf(w, "# HELP %s Election duration in seconds\n", MetricElectionDuration)
		fmt.Fprintf(w, "# TYPE %s histogram\n", MetricElectionDuration)
		var outcomes []string
		registry.electionDurations.Range(func(key, _ any) bool {
			outcomes = append(outcomes, key.(string))
			return true
		})
		sort.Strings(outcomes)
		for _, outcome := range outcomes {
			if val, ok := registry.electionDurations.Load(outcome); ok {
				h := val.(*Histogram)
				labels := map[string]string{"outcome": outcome}
				writeHistogramSamples(w, MetricElectionDuration, h, labels)
			}
		}
	})
}

// NewEventsHandler returns an http.Handler that serves recent events from a LiveBuffer in JSON format.
func NewEventsHandler(buffer *LiveBuffer) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		events := buffer.Snapshot()
		if err := json.NewEncoder(w).Encode(events); err != nil {
			http.Error(w, fmt.Sprintf("failed to encode events: %v", err), http.StatusInternalServerError)
		}
	})
}

func formatHistogram(w http.ResponseWriter, name, help string, h *Histogram, labels map[string]string) {
	if h == nil {
		return
	}
	fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	fmt.Fprintf(w, "# TYPE %s histogram\n", name)
	writeHistogramSamples(w, name, h, labels)
}

func writeHistogramSamples(w http.ResponseWriter, name string, h *Histogram, labels map[string]string) {
	bounds, counts := h.Buckets()
	baseLabels := formatLabelPairs(labels)

	for i, upper := range bounds {
		lbls := formatBucketLabels(baseLabels, fmt.Sprintf("%g", upper))
		fmt.Fprintf(w, "%s_bucket{%s} %d\n", name, lbls, counts[i])
	}
	lblsInf := formatBucketLabels(baseLabels, "+Inf")
	fmt.Fprintf(w, "%s_bucket{%s} %d\n", name, lblsInf, h.Count())

	if baseLabels == "" {
		fmt.Fprintf(w, "%s_sum %g\n", name, h.Sum())
		fmt.Fprintf(w, "%s_count %d\n", name, h.Count())
	} else {
		fmt.Fprintf(w, "%s_sum{%s} %g\n", name, baseLabels, h.Sum())
		fmt.Fprintf(w, "%s_count{%s} %d\n", name, baseLabels, h.Count())
	}
}

func formatLabelPairs(labels map[string]string) string {
	if len(labels) == 0 {
		return ""
	}
	var pairs []string
	for k, v := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=%q", k, v))
	}
	sort.Strings(pairs)
	res := ""
	for i, p := range pairs {
		if i > 0 {
			res += ","
		}
		res += p
	}
	return res
}

func formatBucketLabels(base, le string) string {
	if base == "" {
		return fmt.Sprintf("le=%q", le)
	}
	return fmt.Sprintf("%s,le=%q", base, le)
}
