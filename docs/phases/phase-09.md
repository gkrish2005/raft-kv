# Phase 9 — AI Evaluation

## Goal
Evaluate the AI layer's diagnoses against ground truth, using replayable fixtures and a fixed `--mode=recorded` evaluation path (never live), with the full corrected multi-metric report from `docs/ai-design.md`.

## Scope
Synthetic-incident harness covering the **exact 8 scenarios enumerated in `docs/ai-design.md`'s "The 8 evaluation scenarios" table** (scenario name, expected `IncidentType`, affected nodes, and `ExpectedSeverity` are all frozen there — this phase implements fixtures directly against that table, it does not re-derive or independently assign any of these), each with a saved `ClusterEvent` fixture and a **recorded** LLM response fixture (`docs/ai-design.md`'s `--mode=recorded` — the official reported evaluation never calls a live LLM, for reproducibility). Scoring against ground truth using the full metric set: classification accuracy, **severity accuracy (`SeverityCorrect`, reported as its own distinct metric, not blended into classification accuracy)**, affected-node accuracy (now partly mechanically enforced via the evidence-binding check), **accepted-output evidence validity** (100%, hard) reported separately from **LLM evidence rejection rate**, unsupported-claim rate (mechanically 0% by construction for `OBSERVATION`; a **manually audited unsupported-claim rate over the evaluation set**, per `docs/ai-design.md`'s stated rubric, for `INFERENCE` — always reported with that methodology attached, never as a bare unqualified percentage), false-positive/negative rate (using the precise "healthy cluster" definition from `docs/ai-design.md`, **including its 30-minute duration and configured request rate — both must be stated in the report, not left implicit**), **latency reported separately per mode — `--mode=rules` latency, `--mode=recorded` "analysis/evaluation latency" (explicitly NOT representative of real model latency), and a separately-benchmarked `--mode=live` "end-to-end LLM diagnosis latency" — never blended into one number**, and confidence calibration (bucketed accuracy, as concretely defined in `docs/ai-design.md`).

## Non-goals
No new AI capability — this phase evaluates Phase 8's system.

## Files allowed to change (expected — additional files require Pass-1 approval)
`tests/ai_eval/` (harness + event fixtures + recorded-LLM-response fixtures), report-generation tooling (`make ai-eval`, supporting `--mode=rules|recorded|live`).

## Interfaces
```go
type EvalCase struct {
    ScenarioName          string
    ExpectedIncidentType  IncidentType
    ExpectedAffectedNodes []string
    ExpectedSeverity      Severity  // new — see "Severity is now actually evaluated" below
}
type EvalResult struct {
    Case                EvalCase
    ActualIncidentType   IncidentType
    ActualAffectedNodes  []string
    ActualSeverity        Severity  // new
    Correct              bool
    SeverityCorrect       bool      // new — ActualSeverity == Case.ExpectedSeverity; tracked
                                      // separately from Correct (IncidentType match) since a
                                      // response can get the type right and severity wrong or
                                      // vice versa, and conflating them would hide that signal
    EvidenceValid        bool   // accepted-output validity — always true for anything actually accepted
    LLMEvidenceRejected  bool   // did the raw LLM output fail validation before fallback?
    SupportedClaims      int    // count of INFERENCE claims marked SUPPORTED in the manual audit
                                 // (docs/ai-design.md's three-way rubric)
    UnsupportedClaims    int    // count of INFERENCE claims marked UNSUPPORTED in the manual audit
    UncertainClaims      int    // count marked UNCERTAIN — reported separately, EXCLUDED from the
                                 // unsupported_claim_rate denominator (docs/ai-design.md); OBSERVATION
                                 // claims never appear in any of these three counts — OBSERVATION is
                                 // mechanically 0 by construction (rejected, not counted, on failure)
    FalsePositive        bool
    FalseNegative        bool
    Confidence           float64
    Latency              time.Duration
}
```
**Severity is now actually evaluated, not merely justified as a reason `LLMResponse` carries the field.** An earlier draft of this document argued for `LLMResponse.Severity` existing partly on the grounds that "Phase 9 evaluates severity," while `EvalCase`/`EvalResult` (above) had no severity fields at all — that inconsistency is fixed here. **`ExpectedSeverity` for each of the 8 scenarios is frozen in `docs/ai-design.md`'s scenario table itself, not assigned during this phase's fixture-building** — an earlier draft of this document said the opposite (assign at fixture-build time); that was corrected because leaving severity as a per-implementation judgment call, unlike the already-frozen `IncidentType`/affected-node columns, would have made this one column of an otherwise-frozen table inconsistent with the rest. This phase's `EvalCase` construction reads `ExpectedSeverity` directly from that table, the same way it reads `ExpectedIncidentType` and `ExpectedAffectedNodes`. `SeverityCorrect` is reported as its own accuracy metric in the eval report, distinct from (not blended into) `Correct` (`IncidentType` accuracy).

## State changes
None.

## Invariants affected
I-015 indirectly — the healthy-cluster control run and evidence-validity checks continuously re-verify Phase 8's grounding claims.

## Tests required
`make ai-eval --mode=recorded` run from saved fixtures (no live cluster or live LLM needed) as the official, reported evaluation. At least one held-out variant per major incident category. The healthy-cluster control run using the precise definition from `docs/ai-design.md` (N=3, stable leadership, fixed normal workload, no injected faults, replication lag below the `SLOW_FOLLOWER` threshold).

## Failure cases to handle
Eval scenarios too similar to what rule thresholds were tuned against (held-out variants exist to catch this). Accidentally scoring against `--mode=live` output, making the reported numbers non-reproducible run to run.

## Acceptance criteria
Accepted-output evidence-validity = 100% (hard, non-negotiable). LLM evidence rejection rate reported honestly alongside it, whatever it is. Zero incidents on the healthy-cluster control run. Classification accuracy reported honestly with an explicit note it's an initial project threshold. Document any misses with root-cause analysis.

## Interview concepts
"How do you evaluate an AI system when 'is this a good diagnosis' isn't normally unit-testable" — ground-truth synthetic incidents + a validator-enforced 100% accepted-output evidence guarantee + a separately-reported LLM rejection rate + a documented, honest rubric is a genuinely strong, precise answer — noticeably stronger than a single blended "AI accuracy" number.

## Exit criteria
- [ ] all 8 synthetic scenarios from `docs/ai-design.md`'s scenario table implemented as replayable event + recorded-LLM-response fixtures, matching that table's expected `IncidentType`/affected-nodes exactly, each additionally assigned a concrete `ExpectedSeverity`
- [ ] report generation automated (`make ai-eval --mode=recorded` as the official path)
- [ ] `EvalResult` includes every field the metric set requires, including `ActualSeverity`/`SeverityCorrect`
- [ ] accepted-output evidence-validity = 100% achieved; LLM evidence rejection rate reported
- [ ] severity accuracy reported as its own distinct metric alongside classification accuracy
- [ ] healthy-cluster false-positive control run (precise definition) included and passing
- [ ] misses (if any) documented with root-cause analysis
- [ ] `INFERENCE` unsupported-claim rate reported as `unsupported / (supported + unsupported)`, `UNCERTAIN` claims excluded from that denominator and reported separately, with the manual-audit methodology (three-way rubric, single self-reviewer) stated alongside the number, not as a bare percentage
