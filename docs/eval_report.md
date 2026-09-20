# AI Layer Evaluation Report (Mode: recorded)

- **Generated at:** 2026-09-20T12:04:09Z
- **Total scenarios evaluated:** 13 (8 primary, 4 held-out, 1 healthy control)
- **Latency measurement (analysis/evaluation latency (recorded mode; NOT representative of real model latency)):** 152µs average per scenario

## Executive Metric Summary

| Metric | Result | Target / Requirement | Status |
|---|---|---|---|
| **Classification Accuracy** | 100.0% | Initial project threshold | PASS |
| **Severity Accuracy (`SeverityCorrect`)** | 100.0% | Reported separately (not blended) | PASS |
| **Affected-Nodes Accuracy** | 100.0% | Evidence-bound grounding | PASS |
| **Accepted-Output Evidence Validity** | 100.0% | **100.0% (hard, non-negotiable)** | PASS |
| **Raw LLM Evidence Rejection Rate** | 0.0% | Reported honestly (fail-open) | INFO |
| **False Positive Count** | 0 | **0 on 30m healthy control** | PASS |
| **False Negative Count** | 0 | 0 on synthetic failure set | PASS |

## Unsupported-Claim Rate (Inference Claim Audit)

> **Methodology Note (docs/ai-design.md):** `OBSERVATION` claims are mechanically checked with 0% unsupported claims by construction (validator rejects on failure). `INFERENCE` claims are evaluated via a manual audit by a single self-reviewer against a three-way rubric (`SUPPORTED`, `UNSUPPORTED`, `UNCERTAIN`). `UNCERTAIN` claims are explicitly excluded from the denominator of the unsupported rate.

- **Inference Unsupported-Claim Rate:** `0.00%` (Formula: `count(UNSUPPORTED) / (count(SUPPORTED) + count(UNSUPPORTED))`)
- **Uncertain Claims Fraction:** `0.00%` (reported separately, excluded from unsupported denominator)

## Confidence Calibration

| Confidence Bucket | Sample Count | Correct Diagnoses | Accuracy |
|---|---|---|---|
| `[0.0, 0.5)` | 1 | 1 | 100.0% |
| `[0.5, 0.8)` | 0 | 0 | 0.0% |
| `[0.8, 1.0]` | 12 | 12 | 100.0% |

## Detailed Scenario Results

| Scenario | Expected Type | Actual Type | Expected Sev | Actual Sev | Type Match | Sev Match | Nodes Match | Evidence Valid | LLM Rejected |
|---|---|---|---|---|---|---|---|---|---|
| `01_single_leader_crash` | LEADER_INSTABILITY | LEADER_INSTABILITY | LOW | LOW | YES | YES | YES | YES | NO |
| `02_leader_thrash` | LEADER_INSTABILITY | LEADER_INSTABILITY | HIGH | HIGH | YES | YES | YES | YES | NO |
| `03_node_partitioned` | NODE_UNREACHABLE | NODE_UNREACHABLE | MEDIUM | MEDIUM | YES | YES | YES | YES | NO |
| `04_node_killed` | NODE_UNREACHABLE | NODE_UNREACHABLE | MEDIUM | MEDIUM | YES | YES | YES | YES | NO |
| `05_replication_lag_low` | REPLICATION_LAG | REPLICATION_LAG | LOW | LOW | YES | YES | YES | YES | NO |
| `06_slow_follower_sustained` | SLOW_FOLLOWER | SLOW_FOLLOWER | MEDIUM | MEDIUM | YES | YES | YES | YES | NO |
| `07_election_storm` | ELECTION_STORM | ELECTION_STORM | CRITICAL | CRITICAL | YES | YES | YES | YES | NO |
| `08_symmetric_partition` | NETWORK_PARTITION | NETWORK_PARTITION | HIGH | HIGH | YES | YES | YES | YES | NO |
| `healthy_cluster_30m` | - | - | - | - | YES | YES | YES | YES | NO |
| `held_out_leader_thrash_4node` | LEADER_INSTABILITY | LEADER_INSTABILITY | HIGH | HIGH | YES | YES | YES | YES | NO |
| `held_out_asymmetric_partition_5node` | NETWORK_PARTITION | NETWORK_PARTITION | HIGH | HIGH | YES | YES | YES | YES | NO |
| `held_out_slow_follower_45s` | SLOW_FOLLOWER | SLOW_FOLLOWER | MEDIUM | MEDIUM | YES | YES | YES | YES | NO |
| `held_out_election_storm_7rounds` | ELECTION_STORM | ELECTION_STORM | CRITICAL | CRITICAL | YES | YES | YES | YES | NO |
