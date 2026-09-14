# Evidence-Grounded Incident Diagnosis (AI Layer)

## Deployment model — explicit, since this document previously left it ambiguous

**The AI layer runs in-process, as a goroutine/worker inside the same `raftkv` binary as the Raft node — not as a separate OS process, service, or binary.** This is a deliberate scope decision, not an oversight: a genuinely separate AI service would require its own IPC/RPC transport, its own lifecycle management, its own deployment story, and its own failure-mode documentation, none of which teaches anything additional about the Raft-vs-AI architectural boundary this project actually cares about demonstrating. The boundary that matters — `internal/ai` has no import path to `internal/raft`/`storage`/`cluster`, and cannot mutate any of them (I-015, `docs/adr/008-ai-read-only.md`) — is a **compile-time/structural** property, and is exactly as strong whether the AI code runs in-process or out-of-process. Consequently, wherever this project's docs describe "killing the AI service" as a fault-injection scenario, that means **terminating/blocking the in-process AI worker** (e.g. cancelling its goroutine's context, or forcing its `LLMClient` calls to hang/error) — not sending `SIGKILL` to a separate process, since there isn't one. `docs/phases/phase-08.md` and `docs/adr/008-ai-read-only.md` use this corrected wording.

## Why this exists
Raft telemetry is legible to an expert staring at a dashboard, but synthesizing many events into "what's probably wrong" suits an LLM **when properly grounded**. The rule engine and validator make it grounded.

## Architecture: rules first, LLM on top

```
Structured telemetry ([]ClusterEvent, []MetricSnapshot)
        ↓
Deterministic rule engine (always runs)
        ↓
candidate incident (or none)
        ↓
Optional LLM refinement/exploration (read-only, fail-open, fully async)
        ↓
Evidence Validator — AUTHORITATIVE, not the LLM
        ↓
AIIncident
```
Full comparison of approaches: `docs/adr/007-rules-llm-hybrid.md`.

## Data boundary
Exactly `[]ClusterEvent` and `[]MetricSnapshot` from `internal/observability`. Never raw text logs, KV payload contents, or a `Node`/`StateMachine`/`Storage` object. Both `ClusterEvent.SchemaVersion` and `MetricSnapshot.SchemaVersion` (`docs/architecture.md`) are checked on ingest — data carrying an unsupported schema version is excluded from the analysis window (and, if that leaves an incident under-evidenced, the incident is rejected/falls back per the validation pipeline below) rather than interpreted under an assumed, possibly-wrong shape. **`MetricSnapshot.Value` is also checked on ingest and excluded if `NaN`/`+Inf`/`-Inf`** (`docs/architecture.md`'s metric-vocabulary section) — the same non-finite-value concern already enforced on `AIIncident.Confidence` below, applied at the ingest boundary instead.

## OBSERVATION claims are system-generated, not LLM-generated

**The strongest fix for evidence-grounding is architectural, not just a stronger validator: `OBSERVATION`-typed claims are produced by the deterministic rule engine's own templates, never authored by the LLM.** A rule that fires (e.g. "3+ `RPC_FAILED` events against node-B in 60s") emits its `OBSERVATION` claim from a fixed template bound directly to the event's own typed fields — e.g. *"AppendEntries to node-B failed 3 times in the last 60 seconds"* — mechanically derived from `EventType`/`Fields`, not composed in natural language by a model that could subtly editorialize (e.g. turning "AppendEntries failed" into "node B experienced a network failure," which is a stronger and unsupported claim — an RPC failure has several possible causes, of which network disruption is only one). The LLM's role is `INFERENCE` claims: connecting rule-engine `OBSERVATION`s into a higher-level hypothesis ("repeated AppendEntries failures against a single node, combined with no corresponding failures elsewhere, are consistent with a localized network or process issue on that node"), explicitly labeled as inference, not fact.

**If a raw LLM response nonetheless includes an `OBSERVATION`-typed claim**, it is still run through the field-derivability check below rather than trusted outright — the architectural preference for rule-engine-authored observations reduces how often this path is exercised, it doesn't eliminate the need for the check.

## LLM client interface

**The LLM proposes/refines the complete diagnosis, not just narrative claims — Option A of the two designs this project considered.** The alternative (LLM produces only `INFERENCE` claims/narrative, with `IncidentType`/`Severity`/`AffectedNodes`/`Confidence` owned exclusively by the rule engine) was rejected because Phase 9's evaluation explicitly scores classification accuracy, affected-node accuracy, severity, and confidence calibration — those metrics are meaningless if the LLM never actually produces the fields being scored. Concretely: the rule engine's candidate (if any) is passed to the LLM as *context* it can confirm, refine, or override; the LLM's response is what actually gets evaluated, always subject to the Evidence Validator below being the final authority on what's accepted.

```go
type LLMClient interface {
    Analyze(ctx context.Context, input DiagnosisInput) (LLMResponse, error)
}
type DiagnosisInput struct {
    Events              []ClusterEvent
    Metrics             []MetricSnapshot
    RuleEngineCandidate *RuleEngineCandidate // the rule engine's own candidate, as context for the LLM
                                              // to confirm/refine/override — nil if no rule fired
                                              // (the LLM may still explore and propose an incident
                                              // in that case; the validator is what actually gates
                                              // whether anything gets accepted, not rule-firing alone)
}
type RuleEngineCandidate struct {
    IncidentType  IncidentType
    Severity      Severity
    AffectedNodes []string
    Claims        []DiagnosisClaim // OBSERVATION only, rule-generated (see below)
}
type LLMResponse struct {
    IncidentType       IncidentType
    Severity            Severity
    Claims              []DiagnosisClaim // raw, NOT yet validated — OBSERVATION (rare — see below)
                                           // and INFERENCE
    AffectedNodes       []string
    ConsistencyImpact   ConsistencyImpact // fixed enum, NOT free text — see below
    RecommendedActions  []string
    Confidence          float64
}
```

**`ConsistencyImpact` is a fixed enum, not a free-text channel — this is deliberate, and closes a gap an earlier draft of this document left open.** Every other externally-influenced field on `AIIncident` (`Claims`, `AffectedNodes`, `IncidentType`, `Severity`) is either evidence-bound or checked against a frozen enum by the Evidence Validator; `ConsistencyImpact` as a bare `string` would have let the LLM assert something like *"the partition caused acknowledged writes to be lost"* with zero evidence-grounding requirement, undermining the whole point of an evidence-grounded design for exactly this one field. Fixed instead:
```go
type ConsistencyImpact string
const (
    None                  ConsistencyImpact = "NONE"
    WritesUnavailable     ConsistencyImpact = "WRITES_UNAVAILABLE"      // e.g. no quorum to commit
    ReadsUnavailable      ConsistencyImpact = "READS_UNAVAILABLE"       // e.g. no leader to confirm quorum
    CommitProgressBlocked ConsistencyImpact = "COMMIT_PROGRESS_BLOCKED" // e.g. replication stalled on a lagging/unreachable follower
)
```
Validated the same way `IncidentType`/`Severity`/`ClaimType` are (see the validation pipeline below): an unrecognized/empty `ConsistencyImpact` value causes the incident to be rejected.

**`ConsistencyImpact` describes service/consistency impact ONLY — it must never encode a root cause.** This distinction is easy to blur (an LLM asked to classify "impact" may naturally reach for a cause-shaped label like `"NETWORK_PARTITION"` instead of an effect-shaped one like `WritesUnavailable`), so it's stated explicitly: `ConsistencyImpact`'s four values above describe *what the cluster can no longer do* (accept writes, serve linearizable reads, make commit progress — or nothing observable, `None`), never *why* (a partition, a crashed node, a slow disk, an election storm — that's what `IncidentType` and the evidence-bound `Claims` are for). A value like `"NETWORK_PARTITION"` is not a valid `ConsistencyImpact` and must be rejected by the same enum-validity check as any other unrecognized value — root-cause attribution belongs exclusively in an evidence-bound `INFERENCE` claim (e.g. *"the affected nodes' inability to reach quorum is consistent with a network partition"*, citing the relevant `RPC_FAILED`/`PARTITION_CREATED` evidence), never in this field. If the LLM wants to make a more nuanced consistency-impact claim beyond what these four values capture, it does so as an ordinary evidence-bound `INFERENCE` claim in `Claims` — `ConsistencyImpact` itself stays a coarse, mechanically-checkable classification of *impact*, not a place for open-ended narrative or causal attribution of any kind.
Tests inject `FakeLLM` to simulate: timeout, malformed JSON, a nonexistent-EventID citation, a valid grounded response, low confidence, out-of-bounds confidence, empty output, an invalid/unrecognized `IncidentType` string, an `AffectedNodes` entry with no supporting evidence, and a `ConsistencyImpact` value that encodes a root cause (e.g. `"NETWORK_PARTITION"`) rather than a service-impact effect — assert this is rejected the same as any other unrecognized `ConsistencyImpact` value.

## Evidence model — the validator derives fields, the LLM only points at events

```go
type DiagnosisClaim struct {
    Claim       string
    EvidenceIDs []string
    ClaimType   ClaimType // OBSERVATION | INFERENCE — set by the ORIGINATOR (rule engine or LLM)
                           // at creation time; never silently reassigned during validation, see below
}
type EvidenceRef struct {
    EventID string
    Event   ClusterEvent // looked up by the validator — never trusted from the LLM's own description
}
type AIIncident struct {
    IncidentID         string
    DetectedAt         time.Time
    IncidentType        IncidentType
    Severity            Severity
    Claims              []DiagnosisClaim
    Evidence            []EvidenceRef
    AffectedNodes       []string
    ConsistencyImpact   ConsistencyImpact // fixed enum — see "LLM client interface" above
    RecommendedActions  []string // see "Recommended actions are informational only" below
    Confidence          float64  // see "Confidence semantics" below — validated bounds, defined meaning
    Source              Source
}
```

**Validation pipeline, extended:**
```
for each DiagnosisClaim.EvidenceIDs:
    lookup EventID in the supplied telemetry window
    if not found: REJECT the entire incident (fall back to rule-engine-only or none)
    else: construct EvidenceRef from the CANONICAL event, discard any LLM-authored description

require every DiagnosisClaim to cite >= 1 EvidenceID
require ClaimType to be a valid enum value

# IncidentType/Severity/ConsistencyImpact check (new — LLMResponse now carries these, per "LLM
# client interface" above): require LLMResponse.IncidentType to be a valid IncidentType enum
# value, LLMResponse.Severity to be a valid Severity enum value, and LLMResponse.ConsistencyImpact
# to be a valid ConsistencyImpact enum value; reject (fall back to rule-engine-only or none) on
# an unrecognized/empty value for any of the three. Structurally identical checks, same reasoning,
# same failure mode if skipped.

# OBSERVATION-specific check: an OBSERVATION claim must be directly derivable from the cited
# event(s)' own fields — not merely "cites an event ID" but "the claim text is a restatement
# of what that event's Type/Fields actually say," checked via a simple substring/field-match
# heuristic against the canonical event, not full NLP verification.
#
# If an OBSERVATION-typed claim fails this check: REJECT the entire incident (fall back to
# rule-engine-only or none). Do NOT reclassify it as INFERENCE and re-validate it under those
# (weaker) rules — silently downgrading a failed OBSERVATION into an accepted INFERENCE would
# let any claim bypass observation-grounding simply by failing the check, which defeats the
# purpose of having two claim types with different evidentiary bars in the first place. If the
# LLM wants to make an inference, it must label the claim ClaimType=INFERENCE from the start,
# not have the validator relabel a failed observation into one after the fact.

# AffectedNodes check: every node listed in AffectedNodes must appear as one of the CANONICAL
# node-bearing fields (docs/architecture.md's "Canonical node-bearing fields" list: NodeID,
# PeerID, Fields["peer"], Fields["candidate"], Fields["leader"], or a member of the sorted
# comma-separated Fields["peers"] list) of at least one cited Evidence event. There is no
# generic Fields["target"] key in this schema — the validator checks against this exact,
# frozen list, never a guessed/assumed field name. An incident naming a node with zero
# supporting evidence (by this exact check) is rejected — this doesn't prove the node is
# correctly implicated, but it does prevent the model from naming an arbitrary node with no
# grounding at all.

# Confidence bounds: reject (fall back) if Confidence < 0, > 1, NaN, or +/-Inf.

# Non-emptiness check (new — closes a gap an earlier draft left open): require >= 1
# DiagnosisClaim AND >= 1 resolved EvidenceRef on every accepted AIIncident. An incident with
# zero claims and zero evidence would trivially satisfy every check above (there's nothing to
# fail) while contributing nothing "evidence-grounded" about it at all — this check exists
# specifically to close that vacuous-acceptance loophole. Reject (fall back) an incident with
# an empty Claims slice or an empty Evidence slice.
```

### Enums
```go
type IncidentType string
const (
    LeaderInstability IncidentType = "LEADER_INSTABILITY"; NodeUnreachable IncidentType = "NODE_UNREACHABLE"
    ReplicationLag    IncidentType = "REPLICATION_LAG";    ElectionStorm   IncidentType = "ELECTION_STORM"
    SlowFollower      IncidentType = "SLOW_FOLLOWER";      NetworkPartition IncidentType = "NETWORK_PARTITION"
)
type Severity string
const ( Low Severity = "LOW"; Medium Severity = "MEDIUM"; High Severity = "HIGH"; Critical Severity = "CRITICAL" )
type Source string
const ( RuleEngine Source = "rule_engine"; LLM Source = "llm"; Hybrid Source = "hybrid" )
type ClaimType string
const ( Observation ClaimType = "OBSERVATION"; Inference ClaimType = "INFERENCE" )
```

## The 8 evaluation scenarios — exact mapping, not left implicit

Phase 9 evaluates against **8 synthetic failure scenarios**, deliberately more than the **6** `IncidentType` enum values above — some incident types are exercised by more than one distinct scenario, which is fine and expected; what must not be ambiguous is which scenario maps to which expected type and which nodes. This table is authoritative; `docs/phases/phase-09.md`'s `EvalCase` fixtures are generated from it, not the reverse.

| # | Scenario | Expected `IncidentType` | Affected nodes (example 3-node cluster) | Expected Severity | Quantitative parameters (distinguishes overlapping scenarios) |
|---|---|---|---|---|---|
| 1 | Single leader crash, repeated | `LEADER_INSTABILITY` | leader node, e.g. `node-A` | `LOW` | Exactly 1 leader crash per experiment run, followed by a clean re-election with no further disruption — a single, isolated instability event |
| 2 | Rapid alternating leadership (thrash) | `LEADER_INSTABILITY` | all nodes that hold leadership during the window | `HIGH` | **≥3 distinct leadership changes within a 60-second window** — this is what distinguishes "thrash" from scenario 1's single isolated crash; below this threshold within that window is scenario 1's pattern, not scenario 2's |
| 3 | One node fully partitioned away | `NODE_UNREACHABLE` | the partitioned node, e.g. `node-C` | `MEDIUM` | Partition induced and held for the full observation window; the node is never reachable by any peer during that window |
| 4 | One node's process killed, not restarted | `NODE_UNREACHABLE` | the killed node | `MEDIUM` | Process killed once, no restart before the observation window ends |
| 5 | One follower with injected replication latency, below thrash threshold | `REPLICATION_LAG` | the lagging follower | `LOW` | Injected latency sustained for **< 30 seconds** — below the `SLOW_FOLLOWER` threshold in row 6 |
| 6 | Same as #5 but sustained long enough to cross the slow-follower threshold | `SLOW_FOLLOWER` | the lagging follower | `MEDIUM` | Injected latency sustained for **≥ 30 seconds** continuously — this specific numeric threshold is what separates rows 5 and 6, not merely "longer" |
| 7 | Repeated failed elections (split votes / no candidate reaches quorum for several rounds) | `ELECTION_STORM` | all participating nodes | `CRITICAL` | **≥5 consecutive election rounds** with no candidate reaching quorum |
| 8 | Symmetric network partition (majority/minority split) | `NETWORK_PARTITION` | the minority-side node(s) | `HIGH` | Partition induced and held for the full observation window, split such that no side alone is a majority-of-the-whole-cluster minus one (i.e. an actual majority/minority split, not a single-node partition, which is scenario 3) |

**These `ExpectedSeverity` values are project-defined evaluation ground-truth labels for this synthetic evaluation harness — they are not objective probabilities, universally correct severity classifications, or a claim about how any particular production operator would triage these situations.** They're frozen here (rather than left to be assigned "reasonably" whenever Phase 9's fixtures happen to get built) for the same reason the `IncidentType`/affected-node columns are frozen: `docs/phases/phase-09.md`'s `EvalCase` fixtures must be generated directly from this table, including severity, so there is exactly one authoritative source for what "correct" means in this evaluation, decided once, here, rather than implicitly re-decided per fixture at implementation time. The rough reasoning behind each assignment (a single self-healing event is `LOW`; a fully leaderless cluster during an election storm is `CRITICAL` since no writes can commit at all; the rest fall between based on how much of the cluster's availability is actually impaired) is illustrative, not a rule the values must be re-derivable from — the frozen value in the table is what matters, not a formula.

**These thresholds exist specifically so scenarios 1↔2 and 5↔6 are quantitatively, not just qualitatively, distinct** — without a fixed number, an evaluator could plausibly classify the same underlying event stream as either scenario in a pair, which would make the pair's separate existence in this table meaningless for evaluation purposes. The rule engine's own `ELECTION_STORM` and `LEADER_INSTABILITY`/thrash rules (`docs/phases/phase-08.md`'s ≥5 concrete-rules requirement — a count of *rules*, distinct from this table's *thresholds*) should use these same numeric thresholds where the mapping is direct, so the evaluation fixtures and the production rule engine aren't quietly using different definitions of the same incident type.

Each row gets its own saved `ClusterEvent` fixture and recorded-LLM-response fixture (`docs/phases/phase-09.md`), and `docs/phases/phase-09.md`'s `EvalCase.ExpectedSeverity` for that row is populated directly from this table's `Expected Severity` column — Phase 9 does not independently choose severity values at fixture-build time. At least one held-out variant per major incident category (`Anti-circularity`, below) is a *ninth-plus* fixture, not a replacement for any of the 8 above.

## Confidence semantics — defined, not left implicit

**What confidence is NOT:** a probability of correctness, and not automatically `1.0` just because a deterministic rule fired.

**Rule-engine confidence:** each rule defines its own fixed confidence score reflecting how specific/unambiguous its trigger pattern is (e.g. "≥3 `LEADER_CHANGED` events in 60s" → a high but not maximal fixed score like `0.9`, since even a specific pattern isn't literally certainty about root cause) — set per-rule in the rule definition, not hardcoded to `1.0` for "a rule matched."

**LLM confidence:** whatever the model reports, subject to the bounds validation above.

**Documented everywhere confidence is surfaced (CLI output, reports):** *"Confidence is a heuristic score, not a probability of correctness — see `docs/ai-design.md`'s Confidence calibration metric for how it's evaluated against actual outcomes."*

## (Optional, forward-looking) Telemetry completeness signal
**Not required for the Phase 0-10 MVP scope** — noted here as a well-scoped future enhancement rather than built now, per `CLAUDE.md`'s scope-discipline rule: if events were dropped (`docs/architecture.md`'s `observability_events_dropped_total`) during the window a diagnosis is based on, an overconfident diagnosis built on an incomplete picture is worse than no diagnosis. A future `DiagnosisInput` could carry `Complete bool` / `DroppedEventCount uint64` so the rule engine/LLM can lower confidence or decline to diagnose under known-incomplete telemetry. Flagged for `docs/adr/` if picked up later; not part of this project's committed scope.

## Recommended actions are informational only
Every surface displaying `AIIncident.RecommendedActions` (CLI, reports, any future dashboard) must label them explicitly as **informational operator suggestions only — never executed automatically.** This isn't just a UI nicety; it reinforces the same boundary as `docs/adr/008-ai-read-only.md` at the point where a human is actually reading the output, which is exactly where "isn't this just an LLM suggesting things" concerns get raised in an interview.

## Evaluation metrics — reported separately

| Metric | Bar | Notes |
|---|---|---|
| Incident classification accuracy | Reported honestly | Initial project threshold, not a benchmark |
| Affected-node accuracy | Reported per-scenario | Now partially mechanically enforced — see the AffectedNodes evidence-binding check above |
| **Accepted-output evidence validity** | **100%, hard, mechanically enforced** | Property of the validator |
| **LLM evidence rejection rate** | Reported, not gated | How often raw LLM output failed validation before fallback |
| Unsupported-claim rate | `OBSERVATION`: mechanically checked, 0% by construction (validator rejects the whole incident on failure, per the validation pipeline above — not "target 0%," but structurally 0% among accepted output). `INFERENCE`: reported as a **manually audited unsupported-claim rate over the evaluation set**, using the three-way rubric and exact formula below. Do not report an unqualified `unsupported_claim_rate = X%` without this methodology note attached — always state it as "manually audited unsupported-claim rate," per the rubric below. | |

**`INFERENCE` audit rubric — three-way classification, not a binary supported/unsupported call:**
- **`SUPPORTED`:** the claim logically follows from the cited telemetry/evidence — a reviewer can trace the claim's reasoning back to what the cited `ClusterEvent`(s)/`MetricSnapshot`(s) actually show, without needing to assume additional facts not present in the citation.
- **`UNSUPPORTED`:** the claim introduces a factual event or state that is not supported by the cited evidence — e.g. asserting a specific cause, timeline detail, or consequence that the cited evidence doesn't actually establish, even if the general topic is in the right neighborhood.
- **`UNCERTAIN`:** the reviewer cannot confidently determine whether the inference is supported — the claim is ambiguous, borderline, or requires a judgment call the reviewer isn't confident making either way.

**Reviewer:** one reviewer (the project author, self-reviewed and stated as such — this is a portfolio project, not a peer-reviewed study), reviewing every accepted `INFERENCE` claim across the full 8-scenario + healthy-control evaluation set (not a sample), marking each `SUPPORTED` / `UNSUPPORTED` / `UNCERTAIN` against the standard: *"does this claim's specific content follow from exactly the cited evidence, without assuming additional outside facts?"*

**Reported metric, exact formula — `UNCERTAIN` claims are excluded from the denominator, not treated as either supported or unsupported:**
```
unsupported_claim_rate = count(UNSUPPORTED) / (count(SUPPORTED) + count(UNSUPPORTED))
```
`UNCERTAIN` claims are reported separately (their own count, and as a fraction of all reviewed claims) rather than folded into either side of the ratio above — including them in the denominator would silently treat "reviewer couldn't tell" as evidence of support, and including them in the numerator would overstate the unsupported rate with claims that were never actually judged unsupported. The methodology (three-way rubric, single self-reviewer, exact formula, `UNCERTAIN` exclusion) is printed alongside the metric every time it's reported — never a bare percentage. This rubric is deliberately simple (a one-line judgment call per claim, not a scoring model or NLP pipeline) — building an automated or multi-dimensional claim-support checker is explicitly out of scope for this project.
| False-positive rate | Healthy-cluster control run (defined precisely below, including duration) | |
| False-negative rate | Measured across the synthetic set | |
| **Diagnosis latency — reported per evaluation mode, never blended** | `--mode=rules`: rule-engine-only latency. `--mode=recorded`: fixture-load + validation + scoring latency (NOT representative of real model latency — label it "analysis/evaluation latency, recorded mode" explicitly). `--mode=live`: real end-to-end LLM diagnosis latency, benchmarked and reported separately as "live LLM end-to-end latency." | These three numbers measure different things; a report that averages them together is misleading. |
| Confidence calibration | Bucketed accuracy: for each scenario + the healthy control, record `(predicted_confidence, correct?)`, bucket into `[0-0.5)`/`[0.5-0.8)`/`[0.8-1.0]`, report accuracy per bucket | |

**"Healthy cluster," defined precisely, including duration:** N=3 nodes, stable leadership (no leader changes) for the **full 30-minute run duration**, a fixed normal client workload (**the request rate is set in the eval harness config, e.g. 10 req/sec, and stated in the report**), no injected faults, replication lag below the `SLOW_FOLLOWER` rule's threshold throughout. Under these conditions, **zero** `AIIncident`s is required.

**Resume framing:** *"evaluated against 8 controlled failure scenarios with 100% accepted-output evidence-grounding validation."*

## Required robustness tests

| Condition | Required behavior |
|---|---|
| LLM call errors/times out | Fall back to rule-engine-only (or none) |
| LLM returns invalid JSON | Rejected, same fallback |
| LLM cites a nonexistent `EventID` | Rejected by the validation pipeline |
| LLM's `OBSERVATION` claim isn't derivable from its cited event's fields | Rejected — the entire incident falls back per the validation pipeline above, never silently reclassified as `INFERENCE` |
| LLM names an `AffectedNodes` entry with zero supporting evidence | Rejected |
| LLM returns an unrecognized/empty `IncidentType` or `Severity` | Rejected |
| LLM returns an unrecognized/empty `ConsistencyImpact` | Rejected |
| LLM returns a root-cause-shaped value for `ConsistencyImpact` (e.g. `"NETWORK_PARTITION"`) instead of a service-impact effect | Rejected — same enum-validity check; root cause belongs only in an evidence-bound `INFERENCE` claim |
| LLM returns an incident with zero `Claims` or zero resolved `Evidence` | Rejected (vacuous-acceptance guard) |
| LLM returns `Confidence` outside `[0,1]` or non-finite | Rejected |
| LLM service unavailable | Zero effect on cluster operation, fully async |
| Healthy cluster (precise definition above) | Zero `AIIncident`s |

## Evaluation modes
```
make ai-eval --mode=rules      # deterministic, no LLM call at all
make ai-eval --mode=recorded   # fixture -> RECORDED LLM response -> validator -> scored (the OFFICIAL, reported evaluation)
make ai-eval --mode=live       # real LLM call, informational only, NOT the scored/reported evaluation
```

## Replayable telemetry
`Chaos scenario → ClusterEvent stream → saved JSON fixture → AI evaluator (--mode=recorded) → diagnosis → ground truth comparison`. CLI: `raftkv-cli events replay <fixture>`, `raftkv-cli diagnose --events <fixture>`.

## Anti-circularity
At least one held-out variant per major incident category.

## Rule engine reliability claim
*"The deterministic rule engine achieves a 100% pass rate on the defined validation scenarios"* — never an unqualified "100% reliable."
