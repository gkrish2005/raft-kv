package aieval

import (
	"raftkv/internal/ai"
)

// PrimaryEvaluationCases returns the exact 8 synthetic failure scenarios from docs/ai-design.md table.
func PrimaryEvaluationCases() []EvalCase {
	return []EvalCase{
		{
			ScenarioName:          "01_single_leader_crash",
			Description:           "Single leader crash followed by clean re-election with no further disruption",
			ExpectedIncidentType:  ai.LeaderInstability,
			ExpectedAffectedNodes: []string{"node-1"},
			ExpectedSeverity:      ai.Low,
		},
		{
			ScenarioName:          "02_leader_thrash",
			Description:           "Rapid alternating leadership: >=3 distinct leadership changes within 60 seconds",
			ExpectedIncidentType:  ai.LeaderInstability,
			ExpectedAffectedNodes: []string{"node-1", "node-2", "node-3"},
			ExpectedSeverity:      ai.High,
		},
		{
			ScenarioName:          "03_node_partitioned",
			Description:           "One node fully partitioned away for the full observation window",
			ExpectedIncidentType:  ai.NodeUnreachable,
			ExpectedAffectedNodes: []string{"node-3"},
			ExpectedSeverity:      ai.Medium,
		},
		{
			ScenarioName:          "04_node_killed",
			Description:           "One node process killed once, not restarted before observation window ends",
			ExpectedIncidentType:  ai.NodeUnreachable,
			ExpectedAffectedNodes: []string{"node-3"},
			ExpectedSeverity:      ai.Medium,
		},
		{
			ScenarioName:          "05_replication_lag_low",
			Description:           "One follower with injected replication latency sustained for < 30 seconds",
			ExpectedIncidentType:  ai.ReplicationLag,
			ExpectedAffectedNodes: []string{"node-2"},
			ExpectedSeverity:      ai.Low,
		},
		{
			ScenarioName:          "06_slow_follower_sustained",
			Description:           "One follower with injected replication latency sustained for >= 30 seconds continuously",
			ExpectedIncidentType:  ai.SlowFollower,
			ExpectedAffectedNodes: []string{"node-2"},
			ExpectedSeverity:      ai.Medium,
		},
		{
			ScenarioName:          "07_election_storm",
			Description:           "Repeated failed elections: >=5 consecutive election rounds with no leader established",
			ExpectedIncidentType:  ai.ElectionStorm,
			ExpectedAffectedNodes: []string{"node-1", "node-2", "node-3"},
			ExpectedSeverity:      ai.Critical,
		},
		{
			ScenarioName:          "08_symmetric_partition",
			Description:           "Symmetric network partition: majority/minority split held full window",
			ExpectedIncidentType:  ai.NetworkPartition,
			ExpectedAffectedNodes: []string{"node-2", "node-3"},
			ExpectedSeverity:      ai.High,
		},
	}
}

// HealthyClusterControlCase returns the 30-minute, 10 req/sec zero-incident control case.
func HealthyClusterControlCase() EvalCase {
	return EvalCase{
		ScenarioName:          "healthy_cluster_30m",
		Description:           "30-minute healthy cluster operation at 10 req/sec with stable leadership and zero errors",
		ExpectedIncidentType:  "", // No incident expected
		ExpectedAffectedNodes: nil,
		ExpectedSeverity:      "",
		IsHealthyControl:      true,
	}
}

// HeldOutEvaluationCases returns the 4 held-out variants for anti-circularity verification.
func HeldOutEvaluationCases() []EvalCase {
	return []EvalCase{
		{
			ScenarioName:          "held_out_leader_thrash_4node",
			Description:           "Held-out Leader Instability: 4-node cluster with 4 distinct leadership changes in 45s",
			ExpectedIncidentType:  ai.LeaderInstability,
			ExpectedAffectedNodes: []string{"node-1", "node-2", "node-3", "node-4"},
			ExpectedSeverity:      ai.High,
			IsHeldOut:             true,
		},
		{
			ScenarioName:          "held_out_asymmetric_partition_5node",
			Description:           "Held-out Partition: 5-node cluster with 2-node isolated minority (node-4, node-5)",
			ExpectedIncidentType:  ai.NetworkPartition,
			ExpectedAffectedNodes: []string{"node-4", "node-5"},
			ExpectedSeverity:      ai.High,
			IsHeldOut:             true,
		},
		{
			ScenarioName:          "held_out_slow_follower_45s",
			Description:           "Held-out Replication Latency: node-3 latency sustained for 45 seconds continuously",
			ExpectedIncidentType:  ai.SlowFollower,
			ExpectedAffectedNodes: []string{"node-3"},
			ExpectedSeverity:      ai.Medium,
			IsHeldOut:             true,
		},
		{
			ScenarioName:          "held_out_election_storm_7rounds",
			Description:           "Held-out Election Storm: 5-node cluster split across 7 consecutive failed election rounds",
			ExpectedIncidentType:  ai.ElectionStorm,
			ExpectedAffectedNodes: []string{"node-1", "node-2", "node-3", "node-4", "node-5"},
			ExpectedSeverity:      ai.Critical,
			IsHeldOut:             true,
		},
	}
}

// AllEvaluationCases returns the complete evaluation set: 8 primary + 1 healthy control + 4 held-out variants.
func AllEvaluationCases() []EvalCase {
	cases := PrimaryEvaluationCases()
	cases = append(cases, HealthyClusterControlCase())
	cases = append(cases, HeldOutEvaluationCases()...)
	return cases
}
