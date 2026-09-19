package observability

import (
	"math"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

const MetricSchemaVersion uint32 = 1

// Frozen metric name vocabulary per docs/architecture.md and Pass 1
const (
	MetricLeaderChangesTotal         = "leader_changes_total"
	MetricAppendEntriesFailuresTotal = "append_entries_failures_total"
	MetricReplicationLag             = "replication_lag"
	MetricCommitLatency              = "commit_latency"
	MetricElectionDuration           = "election_duration"
	MetricReadLatency                = "read_latency"
	MetricNodeUp                     = "node_up"
	MetricEventsDroppedTotal         = "events_dropped_total"
)

// MetricSnapshot represents a typed, versioned point-in-time metric reading per docs/architecture.md.
type MetricSnapshot struct {
	SchemaVersion uint32            `json:"schema_version"`
	Timestamp     time.Time         `json:"timestamp"`
	NodeID        string            `json:"node_id"`
	Name          string            `json:"name"`
	Value         float64           `json:"value"`
	Labels        map[string]string `json:"labels,omitempty"`
}

// Histogram is an atomic, lock-free cumulative histogram supporting linear percentile interpolation.
type Histogram struct {
	buckets []float64       // sorted upper bounds (le)
	counts  []atomic.Uint64 // cumulative bucket counts
	count   atomic.Uint64   // total observations
	sumBits atomic.Uint64   // sum of observed values (float64 stored via math.Float64bits)
}

// NewHistogram creates a Histogram with the specified bucket upper bounds.
func NewHistogram(buckets []float64) *Histogram {
	sorted := make([]float64, len(buckets))
	copy(sorted, buckets)
	sort.Float64s(sorted)

	return &Histogram{
		buckets: sorted,
		counts:  make([]atomic.Uint64, len(sorted)),
	}
}

// Observe records a value in the histogram.
// Defensively rejects NaN, +Inf, and -Inf per docs/architecture.md.
func (h *Histogram) Observe(val float64) {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return
	}

	// Update cumulative buckets
	for i, upper := range h.buckets {
		if val <= upper {
			h.counts[i].Add(1)
		}
	}
	h.count.Add(1)

	// Atomic float64 sum addition via CAS
	for {
		oldBits := h.sumBits.Load()
		newSum := math.Float64frombits(oldBits) + val
		newBits := math.Float64bits(newSum)
		if h.sumBits.CompareAndSwap(oldBits, newBits) {
			break
		}
	}
}

// Count returns the total number of observations.
func (h *Histogram) Count() uint64 {
	return h.count.Load()
}

// Sum returns the sum of all observed values.
func (h *Histogram) Sum() float64 {
	return math.Float64frombits(h.sumBits.Load())
}

// Buckets returns a copy of bucket upper bounds and their cumulative counts.
func (h *Histogram) Buckets() ([]float64, []uint64) {
	bounds := make([]float64, len(h.buckets))
	counts := make([]uint64, len(h.buckets))
	copy(bounds, h.buckets)
	for i := range h.counts {
		counts[i] = h.counts[i].Load()
	}
	return bounds, counts
}

// Percentile calculates the estimated p-th percentile (0.0 < p < 1.0) using linear interpolation
// across the cumulative histogram buckets. Returns 0 if no observations exist.
func (h *Histogram) Percentile(p float64) float64 {
	total := h.count.Load()
	if total == 0 || p <= 0 || p >= 1.0 {
		return 0
	}

	targetCount := float64(total) * p
	bounds, counts := h.Buckets()

	// Find the bucket interval containing targetCount
	var prevBound float64 = 0
	var prevCount float64 = 0
	for i, upper := range bounds {
		currCount := float64(counts[i])
		if currCount >= targetCount {
			// Interpolate within [prevBound, upper]
			countDiff := currCount - prevCount
			if countDiff <= 0 {
				return upper
			}
			fraction := (targetCount - prevCount) / countDiff
			return prevBound + fraction*(upper-prevBound)
		}
		prevBound = upper
		prevCount = currCount
	}

	// Target exceeds highest bucket upper bound
	if len(bounds) > 0 {
		return bounds[len(bounds)-1]
	}
	return 0
}

// MetricsRegistry provides non-blocking, lock-free metric storage and collection.
// It never acquires Raft or StateMachine mutexes (Rule 32).
type MetricsRegistry struct {
	leaderChanges atomic.Uint64
	nodeUp        atomic.Uint64
	eventsDropped atomic.Uint64

	appendFailures sync.Map // string (peer) -> *atomic.Uint64
	replicationLag sync.Map // string (peer) -> *atomic.Uint64

	commitLatency     *Histogram
	electionDurations sync.Map // string (outcome: "elected"|"abandoned"|"lost") -> *Histogram
	readLatency       *Histogram
}

// NewMetricsRegistry initializes a registry with pre-calibrated histogram buckets per Pass 1.
func NewMetricsRegistry() *MetricsRegistry {
	r := &MetricsRegistry{
		commitLatency: NewHistogram([]float64{
			0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0, 2.5, 5.0,
		}),
		readLatency: NewHistogram([]float64{
			0.0005, 0.001, 0.002, 0.005, 0.010, 0.025, 0.050, 0.100, 0.250, 0.500, 1.0,
		}),
	}
	// Pre-initialize standard election duration histograms
	for _, outcome := range []string{"elected", "abandoned", "lost"} {
		r.electionDurations.Store(outcome, NewHistogram([]float64{
			0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.400, 0.500, 0.750, 1.0, 2.0,
		}))
	}
	return r
}

func (r *MetricsRegistry) IncLeaderChanges() {
	r.leaderChanges.Add(1)
}

func (r *MetricsRegistry) IncEventsDropped() {
	r.eventsDropped.Add(1)
}

func (r *MetricsRegistry) SetNodeUp(up bool) {
	if up {
		r.nodeUp.Store(1)
	} else {
		r.nodeUp.Store(0)
	}
}

func (r *MetricsRegistry) IncAppendEntriesFailures(peer string) {
	val, _ := r.appendFailures.LoadOrStore(peer, &atomic.Uint64{})
	val.(*atomic.Uint64).Add(1)
}

func (r *MetricsRegistry) SetReplicationLag(peer string, lag uint64) {
	val, _ := r.replicationLag.LoadOrStore(peer, &atomic.Uint64{})
	val.(*atomic.Uint64).Store(lag)
}

func (r *MetricsRegistry) ObserveCommitLatency(seconds float64) {
	r.commitLatency.Observe(seconds)
}

func (r *MetricsRegistry) ObserveReadLatency(seconds float64) {
	r.readLatency.Observe(seconds)
}

func (r *MetricsRegistry) ObserveElectionDuration(seconds float64, outcome string) {
	val, ok := r.electionDurations.Load(outcome)
	if !ok {
		h := NewHistogram([]float64{
			0.050, 0.100, 0.150, 0.200, 0.250, 0.300, 0.400, 0.500, 0.750, 1.0, 2.0,
		})
		val, _ = r.electionDurations.LoadOrStore(outcome, h)
	}
	val.(*Histogram).Observe(seconds)
}

func (r *MetricsRegistry) CommitLatency() *Histogram {
	return r.commitLatency
}

func (r *MetricsRegistry) ReadLatency() *Histogram {
	return r.readLatency
}

func (r *MetricsRegistry) ElectionDuration(outcome string) *Histogram {
	val, ok := r.electionDurations.Load(outcome)
	if !ok {
		return nil
	}
	return val.(*Histogram)
}

// Snapshot returns a slice of MetricSnapshots for all current metrics.
func (r *MetricsRegistry) Snapshot(nodeID string) []MetricSnapshot {
	now := time.Now()
	var snapshots []MetricSnapshot

	// leader_changes_total
	snapshots = append(snapshots, MetricSnapshot{
		SchemaVersion: MetricSchemaVersion,
		Timestamp:     now,
		NodeID:        nodeID,
		Name:          MetricLeaderChangesTotal,
		Value:         float64(r.leaderChanges.Load()),
	})

	// node_up
	snapshots = append(snapshots, MetricSnapshot{
		SchemaVersion: MetricSchemaVersion,
		Timestamp:     now,
		NodeID:        nodeID,
		Name:          MetricNodeUp,
		Value:         float64(r.nodeUp.Load()),
	})

	// events_dropped_total
	snapshots = append(snapshots, MetricSnapshot{
		SchemaVersion: MetricSchemaVersion,
		Timestamp:     now,
		NodeID:        nodeID,
		Name:          MetricEventsDroppedTotal,
		Value:         float64(r.eventsDropped.Load()),
	})

	// append_entries_failures_total
	r.appendFailures.Range(func(key, value any) bool {
		peer := key.(string)
		cnt := value.(*atomic.Uint64).Load()
		snapshots = append(snapshots, MetricSnapshot{
			SchemaVersion: MetricSchemaVersion,
			Timestamp:     now,
			NodeID:        nodeID,
			Name:          MetricAppendEntriesFailuresTotal,
			Value:         float64(cnt),
			Labels:        map[string]string{"peer": peer},
		})
		return true
	})

	// replication_lag
	r.replicationLag.Range(func(key, value any) bool {
		peer := key.(string)
		lag := value.(*atomic.Uint64).Load()
		snapshots = append(snapshots, MetricSnapshot{
			SchemaVersion: MetricSchemaVersion,
			Timestamp:     now,
			NodeID:        nodeID,
			Name:          MetricReplicationLag,
			Value:         float64(lag),
			Labels:        map[string]string{"peer": peer},
		})
		return true
	})

	// commit_latency (report p50, p95, p99 as representative snapshots)
	if r.commitLatency.Count() > 0 {
		snapshots = append(snapshots, MetricSnapshot{
			SchemaVersion: MetricSchemaVersion,
			Timestamp:     now,
			NodeID:        nodeID,
			Name:          MetricCommitLatency,
			Value:         r.commitLatency.Percentile(0.95),
		})
	}

	// read_latency
	if r.readLatency.Count() > 0 {
		snapshots = append(snapshots, MetricSnapshot{
			SchemaVersion: MetricSchemaVersion,
			Timestamp:     now,
			NodeID:        nodeID,
			Name:          MetricReadLatency,
			Value:         r.readLatency.Percentile(0.95),
		})
	}

	return snapshots
}
