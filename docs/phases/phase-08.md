# Phase 8 — Evidence-Grounded Incident Diagnosis (Rules + LLM)

## Goal
Build the AI layer per `docs/ai-design.md`: deterministic rules first, optional LLM refinement on top, structurally read-only, fully asynchronous to the client path, with the validator (not the LLM) as the authority on evidence content.

## Scope
- Rule engine: ≥5 concrete rules over the rolling event window (leader instability, node unreachable, high replication lag, repeated elections/election storm, slow-follower drift), each producing an `AIIncident` with `Source: RuleEngine` (enum) and evidence citing real `EventID`s. **Rule-fired `OBSERVATION` claims are generated from fixed, code-controlled templates bound to the triggering event's own typed fields (`docs/ai-design.md`) — never composed as free text**, so the claim text is mechanically traceable to exactly what the event says.
- **`LLMClient` interface** (`docs/ai-design.md`) — Phase 8 must not couple directly to a real API; a `FakeLLM` implementing the same interface is required for tests.
- **Evidence model, exactly per `docs/ai-design.md`:** the LLM's `DiagnosisClaim` output cites `EvidenceIDs` only — it never authors the descriptive fields of a piece of evidence. The validator looks up each `EventID`'s canonical `ClusterEvent` itself and constructs the accepted `EvidenceRef`. `OBSERVATION`/`INFERENCE` claim typing using the frozen enums in `docs/ai-design.md`, with `OBSERVATION` claims specifically checked for direct derivability from the cited event's own fields (not merely citing a real `EventID`). **A claim that fails the `OBSERVATION`-derivability check causes the whole incident to be rejected/fall back — it must never be silently reclassified as `INFERENCE` to salvage it** (`docs/ai-design.md`'s explicit anti-circularity rule for this check). **The LLM proposes/refines the complete diagnosis** — `LLMResponse` carries `IncidentType`/`Severity`/`AffectedNodes`/`Confidence` alongside its `Claims`, not just narrative text over rule-engine-owned fields, per `docs/ai-design.md`'s "LLM client interface" section — validated with the same rigor (valid-enum checks, evidence-binding) as the claims themselves.
- **`AffectedNodes` evidence-binding:** every node named in `AffectedNodes` must appear in at least one cited piece of evidence — reject incidents naming ungrounded nodes.
- **Confidence bounds validation:** reject `Confidence` values outside `[0,1]` or non-finite (NaN/Inf). **Confidence semantics defined per `docs/ai-design.md`:** rule-engine incidents use a per-rule fixed confidence score (not automatically `1.0` just because a rule fired — a rule matching a specific pattern still isn't literal certainty about root cause); confidence is documented everywhere it's surfaced as a heuristic score, not a probability of correctness.
- **Recommended actions labeled informational-only** wherever `AIIncident.RecommendedActions` is displayed (CLI, reports) — reinforces `docs/adr/008-ai-read-only.md`'s boundary at the point a human actually reads the output.
- Mechanical validation pipeline rejecting any incident with an unresolvable `EventID`, an ungrounded `AffectedNodes` entry, an out-of-bounds confidence, or a non-derivable `OBSERVATION` claim — falling back to rule-engine-only or no incident.
- **Fully asynchronous, non-blocking:** the AI layer is never on the client request path (`docs/architecture.md`) — verify this structurally, not just by convention.
- Fail-open behavior for every LLM failure mode per `docs/ai-design.md`'s robustness table.

## Non-goals
No evaluation harness/scoring yet (Phase 9 — though the robustness tests below are this phase's own acceptance criteria). No mutation capability, ever.

## Files allowed to change (expected — additional files require Pass-1 approval)
`internal/ai/{rules,llm_client,incident,validator}.go`. **Hard constraint, enforced by CI: this phase must never add an import from `internal/ai` to `internal/raft`, `internal/storage`, or `internal/cluster`.**

## Interfaces
`Diagnose(window TimeRange) (*AIIncident, error)`. `LLMClient{ Analyze(ctx, DiagnosisInput) (LLMResponse, error) }` per `docs/ai-design.md`.

## State changes
None — read-only by construction.

## Invariants affected
I-015 (AI cannot mutate cluster state; dependency direction correctness inherited from Phase 7's `EventSink` design).

## Tests required
Unit tests per rule (synthetic event windows, assert correct/no incident, using the frozen `IncidentType` enums). Schema-validation and evidence-resolution tests via `FakeLLM`, including the new checks: `OBSERVATION`-derivability (asserting rejection, never reclassification, on failure), `AffectedNodes` evidence-binding, confidence-bounds rejection, and invalid/unrecognized `IncidentType`/`Severity` rejection (now meaningful checks since `LLMResponse` carries these fields directly). All robustness scenarios from `docs/ai-design.md`'s table (now including the ungrounded-node and out-of-bounds-confidence cases). Healthy-cluster zero-incident control (using the precise "healthy" definition from `docs/ai-design.md`, including its stated duration and request rate). **A structural (not timing-threshold) test proving the client write/read path does not depend on the AI layer:** with a `FakeLLM` that blocks for several seconds, client writes/reads must complete without waiting on it — assert this via a structural mechanism (e.g. the client path's own timing bound is unaffected, or the AI call is provably off the synchronous call graph), not by asserting "write latency is exactly unchanged," since scheduling/load variance makes exact-equality timing assertions flaky and don't actually prove the independence claim. **A process-level test:** with a live chaos scenario running, terminate/block the **in-process** AI worker entirely — e.g. cancel its goroutine's context and force its `LLMClient` calls to hang/error, simulating the AI subsystem being unavailable (not just injecting a single `FakeLLM` failure) — assert zero effect on cluster reads/writes, then restart the AI worker and confirm diagnosis resumes; this is a stronger demonstration of the architectural boundary than a single in-process fault injection alone. (The AI layer runs in-process, per `docs/ai-design.md`'s "Deployment model" section — there is no separate OS process to `kill -9`; "killing the AI service" means terminating this worker, not sending a signal to a separate binary.)

## Failure cases to handle
Trusting LLM-authored evidence descriptions instead of validator-derived ones (the specific hallucination class `docs/ai-design.md`'s Evidence model exists to close — do not regress to the simpler-but-weaker `EvidenceRef{EventID, ObservedField, Explanation}` shape from earlier drafts of this project's design). Rules firing on noise (tune thresholds against Phase 6/7's real chaos-scenario fixtures).

## Acceptance criteria
Each of the ≥5 rule-engine scenarios produces the correct incident type with a 100% pass rate on the defined validation scenarios (precise wording, per `docs/ai-design.md` — not an unqualified "100% reliable" claim). The LLM layer never produces an accepted incident with unresolvable evidence in the test set. AI-service-down fault injection during a live chaos scenario has zero effect on cluster operation, including the process-kill variant. The structural async/non-blocking test passes.

## Interview concepts
The rules-vs-LLM-vs-hybrid tradeoff (`docs/adr/007-rules-llm-hybrid.md`). Why the validator, not the LLM, is the authority on what a piece of evidence actually says — and why "the EventID exists" alone isn't sufficient grounding. Why read-only + fail-open + fully-async is the correct reliability boundary for an assistive AI layer in an infra system.

## Exit criteria
- [ ] ≥5 rules implemented and unit tested against frozen enums
- [ ] `LLMClient`/`FakeLLM` interface in place, all robustness scenarios pass through it
- [ ] evidence validation confirmed to use validator-derived fields, never LLM-authored descriptions
- [ ] import-graph CI check in place and passing
- [ ] async/non-blocking structural test passes
