# RaftKV — Development Rules

Binding on every commit, human or AI. This file is the enforceable summary; the *why* behind each rule lives in `docs/` — specifically `docs/invariants.md` (canonical invariant IDs), `docs/architecture.md`, `docs/failure-model.md`, `docs/client-semantics.md`, `docs/ai-design.md`, and `docs/testing.md`.

## Source-of-truth hierarchy

When documents disagree, resolve in this order:

```
1. AGENTS.md               — implementation constraints (this file)
2. docs/invariants.md      — safety invariants (canonical IDs)
3. docs/phases/phase-NN.md — current-phase scope
4. docs/architecture.md    — architecture / wire protocol / data structures
5. docs/client-semantics.md — client contract
6. docs/failure-model.md   — failure assumptions
7. docs/ai-design.md       — AI contract
8. docs/adr/*.md           — rationale / history (informative, not binding)
```

**If two authoritative documents conflict, or the specification is ambiguous on a point that affects correctness: do not guess.** Stop the current pass, report the ambiguity (cite both conflicting statements by file and section) and wait for developer resolution before writing or changing any code. Guessing and being wrong here is much more expensive than stopping and asking.

## Standard workflow — every phase, five passes

```
PASS 1 — DESIGN
  - read the relevant docs/*.md sections + docs/phases/phase-NN.md
  - inspect existing relevant files
  - explain the approach
  - identify which invariant IDs (docs/invariants.md) this phase touches
  - list the files you expect to change (docs/phases/phase-NN.md's
    "Expected files" is a starting point, not an exhaustive allow-list —
    if implementation genuinely requires touching an unlisted file,
    say so explicitly in this pass and get approval before Pass 2)
  - do NOT modify any files in this pass

PASS 2 — IMPLEMENT
  - implement only the scope approved in Pass 1
  - do not implement anything from a later phase, even if convenient

PASS 3 — AUDIT
  - inspect your own implementation
  - compare it against every invariant ID identified in Pass 1
  - compare it against the rules below
  - do NOT modify any files in this pass — report findings only

PASS 4 — FIX
  - write or reproduce a failing test for each issue found in Pass 3
  - fix only those issues
  - re-run tests

PASS 5 — VERIFY
  - go test ./...
  - go test -race ./...
  - report results, including any known limitations, honestly
  - STOP — do not proceed to the next phase without explicit developer approval
```

## Phase discipline
1. Never implement more than the current phase's scope, as defined in `docs/phases/phase-NN.md`, even if the architecture anticipates future phases. If a future phase's need tempts you to add something now, flag it in the Pass-1 or Pass-5 report instead of building it.
2. **Never modify an existing invariant (docs/invariants.md), API contract, persistence format, or externally observable behavior without explicit developer approval.** ("I didn't change the semantics, I just changed the WAL record format" is still a compatibility-breaking change — treat any of these four categories as requiring sign-off.)
3. Before writing code for a new phase, complete Pass 1 and wait for approval.
4. Complete Pass 3 (audit) before Pass 4 (fix) — do not fix issues you haven't first identified and reported.
5. Stop after Pass 5. Do not proceed to the next phase until the developer explicitly approves.

## Scope discipline
6. **Do not introduce snapshots, cluster membership changes, sharding, transactions, multi-Raft, leases, ReadIndex, external databases, Kubernetes, service meshes, or additional consensus mechanisms unless the current `docs/phases/phase-NN.md` explicitly assigns them.** When in doubt, under-build and flag the gap in the Pass-5 report rather than over-build.
7. Do not invent or estimate benchmark numbers. Every number in `docs/benchmarks.md` must come from an actual measured run, with methodology stated (Phase 10 only).

## Correctness — invariant IDs refer to `docs/invariants.md`; never violate these, even to make a test pass
8. Never apply an uncommitted log entry to the state machine (I-005). Application happens strictly after `commitIndex` advances past an entry. An application-level error from a committed command (e.g. `REQUEST_ID_REUSED` detected at apply time) does NOT roll back `commitIndex` or block `lastApplied` from advancing — see `docs/client-semantics.md`'s "Application errors are still applied" rule.
9. Never advance `commitIndex` without the current-term restriction (I-006): only count majority `matchIndex ≥ N` toward commit if `log[N].term == currentTerm`.
10. Never truncate a log entry at or below `commitIndex` (I-011). A conflict detected at or below `commitIndex` is a fatal invariant violation — log and halt / panic in test builds.
11. Never count a follower's entry toward `matchIndex` before that follower's own WAL fsync for that entry has completed (I-013).
12. Never grant a vote, or respond to any RPC that depends on `currentTerm`/`votedFor`, before that term/vote change is durably persisted (I-012). This applies to **all four** places `currentTerm` can change: an incoming RequestVote, an incoming AppendEntries, a RequestVote *response* revealing a higher term, and an AppendEntries *response* revealing a higher term — not just incoming requests. **This also covers a candidate's own self-vote:** persist `{currentTerm+1, votedFor=self}` before sending any outgoing `RequestVote` RPC, not only before responding to incoming ones. **On the RequestVote-response touchpoint specifically: the higher-term check must be evaluated and acted on before any `role`/`electionTerm` election-generation filtering, never gated behind it** (`docs/architecture.md`) — a response carrying a higher term must trigger persist+step-down even if it would otherwise be filtered out as belonging to an abandoned candidacy.
13. Never serve a linearizable read without **all three** of: (a) quorum leadership confirmation, (b) `lastApplied ≥ readCommitIndex` captured at confirmation time, and (c) re-validating immediately before returning that `role == Leader` and `currentTerm` still equals the term captured at confirmation time (I-016). A leader that loses leadership while a read is waiting on the apply barrier must fail/retry the read, not return a value. Steps (c) and the KV read itself must be performed while continuously holding the StateMachine mutex, acquired **after** the Raft mutex, never before (`docs/architecture.md`'s Concurrency model, rule 34) — a write landing between an unsynchronized "check" and "read" would make the declared linearization point false. **A newly elected leader must also not serve any linearizable read until it has committed and applied a no-op entry in its own current term (I-023), enforced via an explicit `readReadyTerm` gate checked *before* the read barrier is even captured — never relying on the ordinary barrier wait alone, which is trivially satisfiable against a fresh leader's `commitIndex=0` and cannot enforce this on its own.**
14. Never treat a client-facing write `TIMEOUT` as a failure. See `docs/client-semantics.md`'s Result Semantics table.
15. Never silently resolve a duplicate `RequestID` that arrives with a *different* payload than the original (I-017) — reject explicitly (`REQUEST_ID_REUSED`), comparing the **canonical-command hash** (`SHA-256` of `{OperationType, Key, Value}`, deterministically serialized), never a raw/ambiguous payload byte comparison (see `docs/client-semantics.md`).
31. **Never weaken, remove, skip, or reinterpret an invariant merely because the current implementation makes it difficult to satisfy.** Reproduce, inspect, fix the implementation — never relax the invariant. Any change to `docs/invariants.md` itself requires explicit developer approval, prior to and separate from any code change.
33. Never update `nextIndex`/`matchIndex` from an AppendEntries response unless **all three** hold: the node is still `Leader`, `response.Term == currentTerm`, and the response belongs to the currently active replication attempt for that follower — a timed-out request's late response can still arrive after a newer request to the same follower is already in flight, even under the at-most-one-in-flight rule (I-021).
34. When code needs both the Raft mutex and the StateMachine mutex, always acquire the Raft mutex first, StateMachine mutex second — never the reverse (`docs/architecture.md`'s Concurrency model). Reversing this order risks deadlock against the applier.
35. Any durable write's in-memory-state mutation happens strictly after that write's `fsync` succeeds, never before (I-018) — this applies uniformly to `LogStore.Append`, `LogStore.TruncateFrom`, and `TermVoteStore.Save`, on both leader and follower code paths (`docs/architecture.md`'s Write path).
36. `TermVoteStore.Save()` must replace the on-disk record via temp-file-write + fsync + rename + parent-directory-fsync (I-024), never an in-place overwrite — and first-boot `Load()` must durably persist the synthesized initial record before returning, not merely hold it in memory (I-020). `Node.Start()` must not emit any vote, RPC, or `ClusterEvent` until that `BootID` persistence completes (I-020).
37. `ClusterEvent.Sequence` is in-memory only, resets on every restart, and is never persisted — do not implement it as, or describe it as, surviving across restarts; cross-restart identity/ordering comes from `BootID` (part of `EventID`, and comparable alongside `Sequence` as a `(BootID, Sequence)` pair), never from `Sequence` alone (`docs/architecture.md`).

## Concurrency
16. Never perform network I/O — direct or indirect — while holding the Raft mutex (I-014). If you're unsure whether a call chain touches the network, treat it as if it does.
17. Disk I/O (WAL append/fsync) **is** allowed under the Raft mutex — a documented, intentional MVP tradeoff (`docs/adr/005-concurrency-model.md`).
18. Never manipulate the election timer directly from more than one goroutine.
19. `go test -race ./...` must be green after every phase's Pass 5.

## Determinism
20. Production code under `internal/raft/` must never call `time.Now()`, `time.After()`, `time.Sleep()`, `time.NewTimer()`, or `time.NewTicker()` directly — only the production `Clock` implementation may do so.
21. `internal/raft/` must depend only on the `Transport` interface and must never construct a gRPC client or dial a peer directly.
22. Level 1/2 tests must drive time via explicit `clock.Advance(...)` and messages via explicit `cluster.Step()`/`DeliverPending(...)` calls — never `time.Sleep`. Core election/commit safety tests (`docs/testing.md`) must use the single-threaded deterministic simulator, not real goroutine scheduling, to determine protocol event ordering.

## Observability & AI boundary
23. `internal/ai` must never import `internal/raft`, `internal/storage`, or `internal/cluster`. It depends only on the typed `[]ClusterEvent` / `[]MetricSnapshot` surface exported by `internal/observability` (I-015). `internal/observability` itself must depend only on an `EventSink` interface that `internal/raft`/`storage`/`cluster` emit into — never the reverse, and never a concrete Raft object passed into observability code.
24. The AI layer is read-only, always, and fully asynchronous to the client write/read path — a client request must never wait on the AI layer for any reason. `Raft → commit/apply → respond to client` and `Events → AI` are separate, non-blocking paths.
25. Every accepted `AIIncident`'s evidence must resolve to a real `EventID` in the supplied telemetry window — mechanically validated by the observability layer itself (not authored/asserted by the LLM), rejected if it doesn't.
26. Distinguish `OBSERVATION` claims from `INFERENCE` claims (`docs/ai-design.md`) in every `AIIncident`.
27. If the LLM is unavailable, times out, returns malformed output, or returns output that fails evidence validation: fall back to the rule-engine result (or no incident) and continue. Cluster operation must be completely unaffected — covered by an explicit fault-injection test.
32. Observability itself must never block or break Raft correctness: event-recording failures are dropped (with a metric incremented) rather than propagated; the metrics/`/metrics` endpoint must never acquire the Raft mutex or otherwise stall consensus — use atomic counters or snapshot copies.

## Engineering hygiene
28. Do not claim linearizability for reads unless the full read-barrier + term-revalidation sequence (rule 13) actually passes its test.
29. Add tests before fixing a correctness bug found during an audit pass.
30. After coding, run the relevant tests, explain every non-obvious Raft/concurrency decision made, report any known limitations honestly, and stop (Pass 5).
