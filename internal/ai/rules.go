package ai

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"raftkv/internal/observability"
)

// Rule defines the interface for deterministic incident rules.
type Rule interface {
	Name() string
	Evaluate(events []observability.ClusterEvent, metrics []observability.MetricSnapshot) *RuleEngineCandidate
}

// DeterministicRuleEngine evaluates a set of deterministic rules against a telemetry window.
type DeterministicRuleEngine struct {
	rules []Rule
}

// NewDeterministicRuleEngine constructs the default rule engine with all 6 concrete rules.
func NewDeterministicRuleEngine() *DeterministicRuleEngine {
	return &DeterministicRuleEngine{
		rules: []Rule{
			&ElectionStormRule{},
			&LeaderInstabilityRule{},
			&NetworkPartitionRule{},
			&SlowFollowerRule{},
			&NodeUnreachableRule{},
			&ReplicationLagRule{},
		},
	}
}

// Evaluate runs all rules in order of priority and returns the highest-priority candidate, or nil.
func (re *DeterministicRuleEngine) Evaluate(events []observability.ClusterEvent, metrics []observability.MetricSnapshot) *RuleEngineCandidate {
	var bestCandidate *RuleEngineCandidate

	for _, rule := range re.rules {
		if cand := rule.Evaluate(events, metrics); cand != nil {
			if bestCandidate == nil || severityRank(cand.Severity) > severityRank(bestCandidate.Severity) {
				bestCandidate = cand
			}
		}
	}

	return bestCandidate
}

func severityRank(s Severity) int {
	switch s {
	case Critical:
		return 4
	case High:
		return 3
	case Medium:
		return 2
	case Low:
		return 1
	default:
		return 0
	}
}

// -----------------------------------------------------------------------------
// 1. ElectionStormRule
// Scenario 7: >= 5 consecutive failed election rounds without a leader elected.
// Severity: CRITICAL, Confidence: 0.95, Impact: WRITES_UNAVAILABLE.
// -----------------------------------------------------------------------------
type ElectionStormRule struct{}

func (r *ElectionStormRule) Name() string { return "ElectionStormRule" }

func (r *ElectionStormRule) Evaluate(events []observability.ClusterEvent, _ []observability.MetricSnapshot) *RuleEngineCandidate {
	var failedElectionEvents []observability.ClusterEvent
	nodesMap := make(map[string]bool)
	var minTerm, maxTerm uint64

	for _, e := range events {
		if e.Type == observability.LeaderElected {
			// A leader was successfully elected: reset storm counter
			failedElectionEvents = nil
			nodesMap = make(map[string]bool)
			minTerm, maxTerm = 0, 0
			continue
		}

		if e.Type == observability.ElectionStarted || e.Type == observability.VoteRejected {
			failedElectionEvents = append(failedElectionEvents, e)
			if e.NodeID != "" {
				nodesMap[e.NodeID] = true
			}
			if candidate, ok := e.Fields["candidate"]; ok && candidate != "" {
				nodesMap[candidate] = true
			}
			if minTerm == 0 || e.Term < minTerm {
				minTerm = e.Term
			}
			if e.Term > maxTerm {
				maxTerm = e.Term
			}
		}
	}

	// Threshold: >= 5 consecutive failed election attempts
	if len(failedElectionEvents) >= 5 {
		var evidenceIDs []string
		for _, e := range failedElectionEvents {
			evidenceIDs = append(evidenceIDs, e.EventID)
		}

		var affectedNodes []string
		for n := range nodesMap {
			affectedNodes = append(affectedNodes, n)
		}
		sort.Strings(affectedNodes)

		claimText := fmt.Sprintf("Election storm detected across nodes [%s]: %d consecutive failed election rounds across terms %d-%d with no leader established",
			strings.Join(affectedNodes, ", "), len(failedElectionEvents), minTerm, maxTerm)

		return &RuleEngineCandidate{
			IncidentType:  ElectionStorm,
			Severity:      Critical,
			AffectedNodes: affectedNodes,
			Claims: []DiagnosisClaim{
				{
					Claim:       claimText,
					EvidenceIDs: evidenceIDs,
					ClaimType:   Observation,
				},
			},
			ConsistencyImpact: WritesUnavailable,
			RecommendedActions: []string{
				"Inspect cluster connectivity and network partitions preventing quorum",
				"Check for conflicting election timeouts or split votes among nodes",
			},
			Confidence: 0.95,
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// 2. LeaderInstabilityRule
// Scenario 1: Single leader crash / re-election -> LOW (confidence 0.85)
// Scenario 2: Rapid alternating leadership (thrash) -> >= 3 distinct changes within 60s -> HIGH (confidence 0.90)
// -----------------------------------------------------------------------------
type LeaderInstabilityRule struct{}

func (r *LeaderInstabilityRule) Name() string { return "LeaderInstabilityRule" }

func (r *LeaderInstabilityRule) Evaluate(events []observability.ClusterEvent, _ []observability.MetricSnapshot) *RuleEngineCandidate {
	var leaderEvents []observability.ClusterEvent
	for _, e := range events {
		if e.Type == observability.LeaderElected {
			leaderEvents = append(leaderEvents, e)
		}
	}

	if len(leaderEvents) == 0 {
		return nil
	}

	// Check for thrash: >= 3 distinct leadership changes within a 60-second window
	for i := 0; i < len(leaderEvents); i++ {
		windowEvents := []observability.ClusterEvent{leaderEvents[i]}
		nodesMap := map[string]bool{leaderEvents[i].NodeID: true}
		if ldr, ok := leaderEvents[i].Fields["leader"]; ok && ldr != "" {
			nodesMap[ldr] = true
		}

		for j := i + 1; j < len(leaderEvents); j++ {
			if leaderEvents[j].Timestamp.Sub(leaderEvents[i].Timestamp) <= 60*time.Second {
				windowEvents = append(windowEvents, leaderEvents[j])
				nodesMap[leaderEvents[j].NodeID] = true
				if ldr, ok := leaderEvents[j].Fields["leader"]; ok && ldr != "" {
					nodesMap[ldr] = true
				}
			}
		}

		if len(windowEvents) >= 3 {
			var evidenceIDs []string
			for _, e := range windowEvents {
				evidenceIDs = append(evidenceIDs, e.EventID)
			}
			var affectedNodes []string
			for n := range nodesMap {
				affectedNodes = append(affectedNodes, n)
			}
			sort.Strings(affectedNodes)

			claimText := fmt.Sprintf("Rapid leadership thrash: %d distinct leadership changes observed within 60 seconds (nodes: %s)",
				len(windowEvents), strings.Join(affectedNodes, ", "))

			return &RuleEngineCandidate{
				IncidentType:  LeaderInstability,
				Severity:      High,
				AffectedNodes: affectedNodes,
				Claims: []DiagnosisClaim{
					{
						Claim:       claimText,
						EvidenceIDs: evidenceIDs,
						ClaimType:   Observation,
					},
				},
				ConsistencyImpact: WritesUnavailable,
				RecommendedActions: []string{
					"Check heartbeat network latency and election timeout configurations",
					"Inspect node CPU / disk stalls causing false leader timeouts",
				},
				Confidence: 0.90,
			}
		}
	}

	// Scenario 1: Single isolated leader election / crash
	// To distinguish a real instability event from clean initial cluster bootstrap at term 1,
	// verify there was an instability indicator:
	// - multiple elections across terms, or
	// - an election at term > 1, or
	// - a preceding LEADER_STEPPED_DOWN, NODE_STOPPED, or TERM_ADVANCED event.
	hasInstabilityIndicator := false
	for _, e := range events {
		if e.Type == observability.LeaderSteppedDown || e.Type == observability.NodeStopped || e.Type == observability.TermAdvanced {
			hasInstabilityIndicator = true
			break
		}
	}

	for _, e := range leaderEvents {
		if e.Term > 1 || hasInstabilityIndicator || len(leaderEvents) > 1 {
			var evidenceIDs []string
			evidenceIDs = append(evidenceIDs, e.EventID)
			node := e.NodeID
			if ldr, ok := e.Fields["leader"]; ok && ldr != "" {
				node = ldr
			}

			claimText := fmt.Sprintf("Leader change observed: node %s elected leader at term %d", node, e.Term)

			return &RuleEngineCandidate{
				IncidentType:  LeaderInstability,
				Severity:      Low,
				AffectedNodes: []string{node},
				Claims: []DiagnosisClaim{
					{
						Claim:       claimText,
						EvidenceIDs: evidenceIDs,
						ClaimType:   Observation,
					},
				},
				ConsistencyImpact: None,
				RecommendedActions: []string{
					"Monitor newly elected leader for stable heartbeat progression",
				},
				Confidence: 0.85,
			}
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// 3. NetworkPartitionRule
// Scenario 8: Symmetric network partition (majority/minority split).
// Severity: HIGH, Confidence: 0.90, Impact: WRITES_UNAVAILABLE.
// -----------------------------------------------------------------------------
type NetworkPartitionRule struct{}

func (r *NetworkPartitionRule) Name() string { return "NetworkPartitionRule" }

func (r *NetworkPartitionRule) Evaluate(events []observability.ClusterEvent, _ []observability.MetricSnapshot) *RuleEngineCandidate {
	var partitionEvents []observability.ClusterEvent
	minorityNodesMap := make(map[string]bool)

	for _, e := range events {
		if e.Type == observability.PartitionCreated {
			partitionEvents = append(partitionEvents, e)
			if peersStr, ok := e.Fields["peers"]; ok && peersStr != "" {
				for _, p := range strings.Split(peersStr, ",") {
					p = strings.TrimSpace(p)
					if p != "" {
						minorityNodesMap[p] = true
					}
				}
			}
			if e.PeerID != "" {
				minorityNodesMap[e.PeerID] = true
			}
		}
	}

	if len(partitionEvents) > 0 {
		var evidenceIDs []string
		for _, e := range partitionEvents {
			evidenceIDs = append(evidenceIDs, e.EventID)
		}
		var affectedNodes []string
		for n := range minorityNodesMap {
			affectedNodes = append(affectedNodes, n)
		}
		sort.Strings(affectedNodes)

		claimText := fmt.Sprintf("Network partition active: isolated minority peers [%s]",
			strings.Join(affectedNodes, ", "))

		return &RuleEngineCandidate{
			IncidentType:  NetworkPartition,
			Severity:      High,
			AffectedNodes: affectedNodes,
			Claims: []DiagnosisClaim{
				{
					Claim:       claimText,
					EvidenceIDs: evidenceIDs,
					ClaimType:   Observation,
				},
			},
			ConsistencyImpact: WritesUnavailable,
			RecommendedActions: []string{
				"Heal network partition between cluster nodes",
				"Verify routing and firewall rules between isolated peers",
			},
			Confidence: 0.90,
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// 4. NodeUnreachableRule
// Scenario 3/4: One node fully partitioned away or killed and not restarted.
// Condition: >= 3 consecutive RPC_FAILED targeting peer, or NODE_STOPPED.
// Severity: MEDIUM, Confidence: 0.85, Impact: COMMIT_PROGRESS_BLOCKED.
// -----------------------------------------------------------------------------
type NodeUnreachableRule struct{}

func (r *NodeUnreachableRule) Name() string { return "NodeUnreachableRule" }

func (r *NodeUnreachableRule) Evaluate(events []observability.ClusterEvent, _ []observability.MetricSnapshot) *RuleEngineCandidate {
	peerFailures := make(map[string][]observability.ClusterEvent)

	for _, e := range events {
		if e.Type == observability.RPCFailed {
			peer := e.PeerID
			if p, ok := e.Fields["peer"]; ok && p != "" {
				peer = p
			}
			if peer != "" {
				peerFailures[peer] = append(peerFailures[peer], e)
			}
		} else if e.Type == observability.RPCSucceeded {
			// RPC succeeded resets consecutive failures for that peer
			peer := e.PeerID
			if p, ok := e.Fields["peer"]; ok && p != "" {
				peer = p
			}
			if peer != "" {
				delete(peerFailures, peer)
			}
		}
	}

	for peer, failedEvts := range peerFailures {
		if len(failedEvts) >= 3 {
			var evidenceIDs []string
			var lastErr string
			for _, e := range failedEvts {
				evidenceIDs = append(evidenceIDs, e.EventID)
				if errClass, ok := e.Fields["error_class"]; ok {
					lastErr = errClass
				}
			}
			if lastErr == "" {
				lastErr = "timeout"
			}

			claimText := fmt.Sprintf("Node %s unreachable: %d consecutive RPC failures (error: %s)",
				peer, len(failedEvts), lastErr)

			return &RuleEngineCandidate{
				IncidentType:  NodeUnreachable,
				Severity:      Medium,
				AffectedNodes: []string{peer},
				Claims: []DiagnosisClaim{
					{
						Claim:       claimText,
						EvidenceIDs: evidenceIDs,
						ClaimType:   Observation,
					},
				},
				ConsistencyImpact: CommitProgressBlocked,
				RecommendedActions: []string{
					fmt.Sprintf("Verify process status and host reachability for node %s", peer),
					"Check transport error logs for connection drops or timeouts",
				},
				Confidence: 0.85,
			}
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// 5. SlowFollowerRule
// Scenario 6: Injected replication latency sustained for >= 30 seconds continuously.
// Severity: MEDIUM, Confidence: 0.85, Impact: COMMIT_PROGRESS_BLOCKED.
// -----------------------------------------------------------------------------
type SlowFollowerRule struct{}

func (r *SlowFollowerRule) Name() string { return "SlowFollowerRule" }

func (r *SlowFollowerRule) Evaluate(events []observability.ClusterEvent, metrics []observability.MetricSnapshot) *RuleEngineCandidate {
	// 1. Check metrics for MetricReplicationLag sustained >= 30s
	lagSnapshots := make(map[string][]observability.MetricSnapshot)
	for _, m := range metrics {
		if m.Name == observability.MetricReplicationLag && m.Value > 0 {
			peer := m.Labels["peer"]
			if peer == "" {
				peer = m.NodeID
			}
			lagSnapshots[peer] = append(lagSnapshots[peer], m)
		}
	}

	for peer, snaps := range lagSnapshots {
		if len(snaps) >= 2 {
			duration := snaps[len(snaps)-1].Timestamp.Sub(snaps[0].Timestamp)
			if duration >= 30*time.Second {
				// Find supporting events for this peer that actually ground the peer
				var evidenceIDs []string
				for _, e := range events {
					for _, n := range CanonicalNodeBearingFields(e) {
						if n == peer {
							evidenceIDs = append(evidenceIDs, e.EventID)
							break
						}
					}
				}

				if len(evidenceIDs) > 0 {
					claimText := fmt.Sprintf("Slow follower %s: sustained replication lag (slow RPC response) for %v (>=30s threshold)",
						peer, duration.Round(time.Second))

					return &RuleEngineCandidate{
						IncidentType:  SlowFollower,
						Severity:      Medium,
						AffectedNodes: []string{peer},
						Claims: []DiagnosisClaim{
							{
								Claim:       claimText,
								EvidenceIDs: evidenceIDs,
								ClaimType:   Observation,
							},
						},
						ConsistencyImpact: CommitProgressBlocked,
						RecommendedActions: []string{
							fmt.Sprintf("Inspect disk I/O and network latency on follower %s", peer),
							"Verify follower is not falling behind leader log compaction",
						},
						Confidence: 0.85,
					}
				}
			}
		}
	}

	// 2. Alternatively check events: repeated RPC failures or delayed AE spanning >= 30s
	peerEventTimes := make(map[string][]observability.ClusterEvent)
	for _, e := range events {
		if e.Type == observability.RPCFailed || e.Type == observability.LogConflict {
			peer := e.PeerID
			if p, ok := e.Fields["peer"]; ok && p != "" {
				peer = p
			}
			if peer != "" {
				peerEventTimes[peer] = append(peerEventTimes[peer], e)
			}
		}
	}

	for peer, evts := range peerEventTimes {
		if len(evts) >= 2 {
			duration := evts[len(evts)-1].Timestamp.Sub(evts[0].Timestamp)
			if duration >= 30*time.Second {
				var evidenceIDs []string
				for _, e := range evts {
					evidenceIDs = append(evidenceIDs, e.EventID)
				}

				claimText := fmt.Sprintf("Slow follower %s: sustained replication lag for %v (>=30s threshold)",
					peer, duration.Round(time.Second))

				return &RuleEngineCandidate{
					IncidentType:  SlowFollower,
					Severity:      Medium,
					AffectedNodes: []string{peer},
					Claims: []DiagnosisClaim{
						{
							Claim:       claimText,
							EvidenceIDs: evidenceIDs,
							ClaimType:   Observation,
						},
					},
					ConsistencyImpact: CommitProgressBlocked,
					RecommendedActions: []string{
						fmt.Sprintf("Inspect disk I/O and network latency on follower %s", peer),
					},
					Confidence: 0.85,
				}
			}
		}
	}

	return nil
}

// -----------------------------------------------------------------------------
// 6. ReplicationLagRule
// Scenario 5: Injected replication latency sustained for < 30 seconds.
// Severity: LOW, Confidence: 0.80, Impact: NONE.
// -----------------------------------------------------------------------------
type ReplicationLagRule struct{}

func (r *ReplicationLagRule) Name() string { return "ReplicationLagRule" }

func (r *ReplicationLagRule) Evaluate(events []observability.ClusterEvent, metrics []observability.MetricSnapshot) *RuleEngineCandidate {
	// Check for replication lag < 30s
	// 1. From metrics
	for _, m := range metrics {
		if m.Name == observability.MetricReplicationLag && m.Value > 0 {
			peer := m.Labels["peer"]
			if peer == "" {
				peer = m.NodeID
			}
			var evidenceIDs []string
			for _, e := range events {
				for _, n := range CanonicalNodeBearingFields(e) {
					if n == peer {
						evidenceIDs = append(evidenceIDs, e.EventID)
						break
					}
				}
			}

			if len(evidenceIDs) > 0 {
				claimText := fmt.Sprintf("Follower %s replication lag detected (slow RPC response, lag value %.0f) (<30s threshold)",
					peer, m.Value)

				return &RuleEngineCandidate{
					IncidentType:  ReplicationLag,
					Severity:      Low,
					AffectedNodes: []string{peer},
					Claims: []DiagnosisClaim{
						{
							Claim:       claimText,
							EvidenceIDs: evidenceIDs,
							ClaimType:   Observation,
						},
					},
					ConsistencyImpact: None,
					RecommendedActions: []string{
						fmt.Sprintf("Monitor replication lag on follower %s", peer),
					},
					Confidence: 0.80,
				}
			}
		}
	}

	// 2. From events: 1 or 2 RPCFailed or LogConflict (< 30s span)
	peerEvents := make(map[string][]observability.ClusterEvent)
	for _, e := range events {
		if e.Type == observability.RPCFailed || e.Type == observability.LogConflict {
			peer := e.PeerID
			if p, ok := e.Fields["peer"]; ok && p != "" {
				peer = p
			}
			if peer != "" {
				peerEvents[peer] = append(peerEvents[peer], e)
			}
		}
	}

	for peer, evts := range peerEvents {
		if len(evts) > 0 {
			duration := evts[len(evts)-1].Timestamp.Sub(evts[0].Timestamp)
			if duration < 30*time.Second {
				var evidenceIDs []string
				for _, e := range evts {
					evidenceIDs = append(evidenceIDs, e.EventID)
				}

				claimText := fmt.Sprintf("Follower %s replication lag detected (<30s threshold)", peer)

				return &RuleEngineCandidate{
					IncidentType:  ReplicationLag,
					Severity:      Low,
					AffectedNodes: []string{peer},
					Claims: []DiagnosisClaim{
						{
							Claim:       claimText,
							EvidenceIDs: evidenceIDs,
							ClaimType:   Observation,
						},
					},
					ConsistencyImpact: None,
					RecommendedActions: []string{
						fmt.Sprintf("Monitor replication lag on follower %s", peer),
					},
					Confidence: 0.80,
				}
			}
		}
	}

	return nil
}
