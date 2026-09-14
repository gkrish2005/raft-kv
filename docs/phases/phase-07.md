# Phase 7 — Observability

## Goal
Turn chaos into structured, machine-readable telemetry per the exact `ClusterEvent`/`MetricSnapshot` schema and event-coverage matrix in `docs/architecture.md` — the foundation the AI layer (Phase 8) depends on — with an explicit failure policy so observability can never affect Raft correctness.

## Scope
- `ClusterEvent` emission at every transition in `docs/architecture.md`'s minimum event coverage matrix (14 required transition→event mappings — treat this as a checklist, not a vague "meaningful transitions" judgment call).
- **Dependency direction, enforced from this phase's first line of code:** `internal/observability` depends only on an `EventSink` interface that `internal/raft`/`storage`/`cluster` emit *into* — it must never import a concrete `Node`/`StateMachine`/`Storage` type from those packages. This is what keeps the AI boundary (Phase 8) genuinely strong rather than technically-true-but-weakened (see `docs/architecture.md`'s corrected dependency diagram).
- **`BootID`-gated startup, carried forward from Phase 1's `Node.Start()` sequencing (I-020):** no event may be emitted through `EventSink` until the current boot's `BootID` has been durably persisted via `TermVoteStore` — this phase is where that gate actually has observable consequences (nothing emits events before Phase 7), so it's called out explicitly here rather than assumed. `ClusterEvent.Sequence` itself is in-memory-only per boot (`docs/architecture.md`) — do not attempt to persist it or carry it across a restart; only `BootID` (already durable via `TermVoteStore`) provides cross-restart identity.
- Prometheus-style `/metrics` endpoint using atomic counters/snapshot copies — **must never acquire the Raft mutex to serve a scrape** (I-014-adjacent rule, `CLAUDE.md` rule 32).
- **Observability failure policy, explicit:** a failed event emission is dropped (with an internal drop-counter metric incremented), never retried or escalated in a way that could block or fail Raft's own processing.
- **Two distinct event sinks, not one, because they serve different and partly incompatible purposes:**
  - **Live debug buffer:** in-memory **bounded** ring buffer (e.g. last 10k events — explicit scope decision), for the debug CLI and live operator inspection. Lossy by design under sustained high event volume — this is an accepted, explicit tradeoff for production/live use, not a bug.
  - **Scenario recorder:** a **separate**, per-chaos-run recorder that captures the **complete** event stream for the duration of a Phase 6 scenario, written out as a JSON fixture (`docs/ai-design.md`'s replayable-telemetry pipeline). This is what Phase 6/9's "fully explainable after the fact purely from the event stream" claim actually depends on — a claim that would be false if it depended on the same bounded, potentially-overflowing ring buffer used for live debugging. The recorder can fail a test outright if it cannot capture the full stream (e.g. write failure), since an incomplete recorded fixture silently undermines Phase 9's evaluation.
  Both sinks receive every emitted event via the same `EventSink` interface; which sink(s) are active is a matter of what's wired up for a given run (production wires only the live buffer; a Phase 6 chaos run wires both).
- Debug CLI for event inspection (reads from the live buffer). Events-replay mode (`raftkv-cli events replay <fixture>`) per `docs/ai-design.md` (reads from a recorder-produced fixture).

## Non-goals
No AI reasoning over these events yet (Phase 8). No external time-series DB.

## Files allowed to change (expected — additional files required for instrumentation call-sites across `internal/raft/`, `internal/storage/`, `internal/cluster/` are expected for this phase and don't need individual Pass-1 sign-off beyond the phase's own approval, since adding event emission at existing transition points is this phase's whole point — just don't change any *logic* in those files, only add emission calls)
`internal/observability/{events,metrics,exporter}.go`; event-emission call sites added into `internal/raft/`, `internal/storage/`, `internal/cluster/`.

## Interfaces
`EventSink` interface (implemented by `internal/observability`, called by `raft`/`storage`/`cluster` — never the reverse). `ClusterEvent`/`MetricSnapshot`/`EventType` per `docs/architecture.md`.

## State changes
None to Raft correctness state — purely additive instrumentation.

## Invariants affected
None directly, but I-015's strength (Phase 8) depends on this phase's dependency direction being correct from the start.

## Tests required
For each Phase 6 chaos scenario, assert the **scenario recorder's** event stream contains the expected event types per the coverage matrix (e.g., leader-crash scenario produces `ELECTION_STARTED`/`LEADER_ELECTED`/`LEADER_STEPPED_DOWN` as appropriate) — validates instrumentation correctness and produces Phase 9's fixtures. A test that the `/metrics` scrape path never blocks under concurrent Raft load (e.g., scrape while a write is in flight, assert no measurable stall). A test that a simulated event-recorder failure (e.g., a full **live** ring buffer) does not propagate any error back into the Raft/storage call path that emitted it. A test that the scenario recorder captures every event from a sustained high-volume run without loss, distinct from the live buffer's accepted lossiness under the same load.

## Failure cases to handle
Missing events on rare code paths (e.g., `LEADER_STEPPED_DOWN` only emitted on election loss but not on discovering a higher term via AppendEntries). Event storms during extended chaos flooding the bounded store. An `internal/observability` import accidentally reaching into `internal/raft` internals instead of going through `EventSink` — treat this as a build-breaking violation, not a style nit.

## Acceptance criteria
Every Phase 6 chaos scenario is fully explainable after the fact purely from the **scenario recorder's** complete event stream (not the live bounded ring buffer, which is explicitly allowed to be lossy under load — see Scope above). All 14 entries in the event coverage matrix have at least one exercised test. `/metrics` never touches the Raft mutex. Event-recording failures never propagate into Raft/storage code paths.

## Interview concepts
Why structured events (not grep-able text) are the right substrate for both human operators and an AI layer. Why the dependency direction (Raft emits into an interface; observability never reaches back in) matters even when the AI layer itself already has its own import-graph check — defense in depth on the architectural boundary.

## Exit criteria
- [ ] all 14 event-coverage-matrix transitions emit correctly, each with a test
- [ ] `internal/observability` has zero imports of concrete `internal/raft`/`storage`/`cluster` types, only the `EventSink` interface
- [ ] `/metrics` scrape-under-load test passes with no measurable stall
- [ ] event-recorder-failure-is-dropped-not-propagated test passes
- [ ] live buffer and scenario recorder implemented as two distinct sinks, with a test proving the recorder captures a complete stream where the live buffer would legitimately drop events under the same load
- [ ] saved fixtures exist for each chaos scenario (from the scenario recorder), ready for Phase 9
