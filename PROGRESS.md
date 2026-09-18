# RaftKV — Progress

Source of truth for "what's actually done" vs. what the phase docs plan. Update this
after every pass (Design/Implement/Audit/Fix/Verify), not just at phase completion.
Nothing gets checked off here unless Pass 5 actually verified it — this file should
never be ahead of reality.

**Structure of this file:** completed, developer-approved phases are compressed into
short checkpoint summaries (§ Completed Phases). The active phase gets full pass-by-pass
detail (§ Current Phase). Not-started phases are listed by name only (§ Upcoming Phases).
Cross-cutting tracking (open questions, invariant coverage) lives at the bottom and
applies across all phases.

## How to read this file
- **Phase status**: Not started / Design approved / In progress / Pass 5 complete / Approved for next phase
- A phase is only "Approved for next phase" once the developer (not the agent) explicitly
  signs off per `AGENTS.md`'s phase discipline rule 5.

---

## Phase status at a glance

| Phase | Name | Status |
|---|---|---|
| 0 | Foundation + Single-Node KV Store | ✅ Approved — 2026-09-13 |
| 1 | Leader Election + Heartbeats | ✅ Approved — 2026-09-13 |
| 2 | Replicated Log + Minimal Durable Log | ✅ Approved — 2026-09-15 |
| 3 | Commit, Apply, and Reads | ✅ Approved — 2026-09-16 |
| 4 | Crash Recovery + Durable Metadata + WAL Hardening | ✅ Approved — 2026-09-16 |
| 5 | Client Semantics + Replicated Dedup | ✅ Approved — 2026-09-17 |
| 6 | Chaos Testing Framework | ✅ Approved for next phase (2026-09-18) |
| **7** | **Observability** | In progress (Pass 1 Design) |
| 8 | Evidence-Grounded Incident Diagnosis | Not started |
| 9 | AI Evaluation | Not started |
| 10 | Benchmarking, Hardening & Final Demo | Not started |

---

## Completed Phases (checkpoint summaries)

### Phase 0 — Foundation + Single-Node KV Store
**Status:** Approved for next phase (2026-09-13)
Single-node in-memory KV over gRPC; `StateMachine{Apply, Get}` interface seam established
(`Get` deliberately not a Raft log command — the seam Phase 3 later wraps in linearizability).
No Raft, no persistence yet.

### Phase 1 — Leader Election + Heartbeats + Minimal Durable Term/Vote
**Status:** Approved for next phase (2026-09-13)
Follower/Candidate/Leader roles, randomized election timeout, RequestVote with exact
log-freshness formula, all four I-007 term-change touchpoints, minimal durable
`TermVoteStore` (fsync-before-response, self-vote persisted before outgoing RequestVote).
100+ repeated Level-2 deterministic elections, zero flakiness.

### Phase 2 — Replicated Log + Minimal Durable Log
**Status:** Approved for next phase (2026-09-15)
`LogStore`/`FileLogStore`/`InMemoryLogStore`, fast-backtrack conflict resolution,
at-most-one-in-flight AppendEntries per follower with attempt-ID staleness protection (I-021).
**Pass 3/4 fix:** `peerInFlight` clearing was unconditional — a stale RPC from an older
term could clear the in-flight flag of a fresh attempt. Keyed the clear to
`role==Leader && term match && attempt match`.
Verified: `go test`, `go test -race`, `go vet` all green (2026-09-15).

### Phase 3 — Commit, Apply, and Reads
**Status:** Approved for next phase (2026-09-16)
Full 3-step linearizable read protocol (I-016: quorum confirm → barrier wait →
term/role revalidation under StateMachine RLock), commit-index advancement with I-006
current-term restriction, request-identity-aware `PendingWrite` lifecycle (I-019),
new-leader NOOP commit + `readReadyTerm` gate (I-023), idempotent graceful shutdown.
**Pass 3/4 fix:** `replication_convergence_test.go` expected `lastIndex` updated 3→4 to
account for the Phase 3 NOOP entry now occupying index 1.
Verified: `go test`, `go test -race`, `go vet` all green (2026-09-16).

### Phase 4 — Crash Recovery + Durable Metadata + WAL Hardening
**Status:** Approved for next phase (2026-09-16)
`FileWAL` with CRC32 length-framing, torn-tail recovery, and 1 MiB oversized-record guard.
`TermVoteStore` full replacement atomicity (temp-write → fsync → rename → dir-fsync,
4-stage crash-point hooks). `FileLogStore` wrapping `FileWAL`. `Node.Recover()` recovery
sequence (volatile commitIndex/lastApplied reset to 0; durable term/vote reload).
**Pass 3/4 fix (I-011, safety-critical):** `FileLogStore.SetCommitIndex` was never called
from the Raft layer — the committed-entry truncation guard sat permanently armed to `0`
in production (zero runtime enforcement). Wired `SetCommitIndex` into all three Raft-layer
sites: leader `tryAdvanceCommitIndexLocked` (`commit.go`), follower `leaderCommit` clamp
(`replication.go`), and `Recover()` reset (`node.go`). New end-to-end test (Test 28)
confirmed the pre-fix `"I-011 VIOLATION"` failure and post-fix pass.
**Pass 3/4 fix (recovery.go, minor):** `ReconstructLog` discarded `wal.TruncateAt` error
with `_ = ...`; replaced with proper error propagation. Test 29 confirms.
Also fixed pre-Pass-3 (on developer instruction): `server.Get` I-016 violation — error
path now returns `STATUS_NOT_LEADER` instead of falling back to `s.sm.Get()`.
Verified: `go vet` clean; `go test -count=1 ./...` green; `go test -race -count=1 ./...`
green, zero DATA RACE reports (2026-09-16). Tests 1–29, all packages.

> **Note on commit history:** The Phase 4 Pass 4 fix (I-011 `SetCommitIndex` wiring, Tests 28–29, `recovery.go` `TruncateAt` error surfacing) was validated during the 2026-09-16 session, but was committed on 2026-09-17 during Phase 5 preparation, extracted into standalone commit `be2eb9e` for bisectability and honest chronology.

### Phase 5 — Client Semantics + Replicated Dedup
**Status:** Approved for next phase (2026-09-17)
Replicated request-table deduplication in `KVStateMachine` keyed by `RequestID` with `PayloadHash` (`SHA-256` of frozen canonical command encoding, excluding `RequestID`).
Application-error semantics verified: `ErrRequestIDReused` (I-017) advances `lastApplied` without Raft rollback (I-005, Test 33). Full client-facing error mapping (`STATUS_NOT_LEADER` with `leaderHint`, `STATUS_TIMEOUT`, `STATUS_INVALID_REQUEST`, `STATUS_REQUEST_ID_REUSED`, Tests 37–40) and `ClusterStatus` dynamic status RPC.
Level-4 crash tests: commit-before-ack-crash exactly-once (Test 35) and rolling-leader-kill write survival (Test 36) with robust leader-polling test harness.
**Pass 2 rework / Pass 3 fix (I-016):** `ctx.Err()` enforcement at entry of `LinearizableGet` and `confirmLeadershipQuorum` ensuring timeout errors on single-node or instant-quorum reads (Test 41).
Verified: `go vet` clean; `go test -count=1 ./...` green; `go test -race -count=1 ./...` green across all 41 tests (2026-09-17).

### Phase 6 — Chaos Testing Framework
**Status:** Approved for next phase (2026-09-18)
Built fault-injection at Levels 3–5 (`docs/testing.md`) with 11 named deterministic chaos scenarios, `FaultTransport` supporting 5 discrete calibrated regimes (CleanSlow, TimeoutSlow, FullPartition, LightDrop, NodeChurn) with quorum-safety guard, 5-point post-heal state convergence engine (quiescence barrier, CommitIndex/LastApplied equality, entry-by-entry prefix comparison, KV state snapshot equality, RequestTable equality), and randomized `Fuzzer` with periodic quiescent intervals.
Real ungraceful crash tests (Tests 51 and 52) run on `ProcessCluster` using real subprocesses and `SIGKILL`, proving rolling crash survival (I-024) and fail-closed corrupt startup with on-disk state repair (I-020). Concurrent network I/O probe verified I-014.
**Pass 3/4 fixes:** Async-dispatch scoped to finite delay with `delayedCancel` in `HealAll()`; `IssueSyncWrite` refactored to wait strictly on `PendingWrite.Done`; `FindLeader` and `WaitForLeader` updated to track live leader by term; dynamic final leader commit snapshot in `AssertConvergence`; synchronous barrier write in periodic quiescence; semantic polling for stable read-ready cluster leader post-heal; stopped node RPC isolation.
Verified: `go vet` clean; `go test -race ./...` green across all 52 tests in repo (2026-09-18).

---

## Current Phase (full detail)

### Phase 7 — Observability
**Status:** In progress (Pass 1 Design) (2026-09-18)

**Goal:** Turn chaos into structured, machine-readable telemetry per the exact `ClusterEvent`/`MetricSnapshot` schema and event-coverage matrix in `docs/architecture.md` — the foundation the AI layer (Phase 8) depends on — with an explicit failure policy so observability can never affect Raft correctness.

**Invariants touched:**
- `I-015`: Strong architectural boundary between Raft/consensus and AI/observability (enforced by one-way `EventSink` interface; `internal/observability` has zero imports of concrete `internal/raft`/`storage`/`cluster` types).
- `I-020`: `BootID`-gated startup; no events may be emitted until `BootID` is durably persisted via `TermVoteStore`.
- `I-014` / Rule 32: Observability must never block or break Raft correctness; failed emissions are dropped and metric-counted; `/metrics` scrape endpoint must never acquire the Raft mutex.
- Rule 23: `internal/ai` never imports Raft/storage/cluster; `internal/observability` depends only on `EventSink` interface.
- Rule 24: AI/observability path is fully asynchronous and read-only.
- Rule 37: `ClusterEvent.Sequence` is in-memory only per boot and resets on restart. Cross-restart identity comes from `BootID`.

#### Pass 1 — Design (In progress)
- Under review: see Pass 1 Design report.

---

## Upcoming Phases
7. Observability
8. Evidence-Grounded Incident Diagnosis (Rules + LLM)
9. AI Evaluation
10. Benchmarking, Hardening & Final Demo

---

## Open questions / flagged gaps
*(Running log — anything flagged instead of guessed, anything intentionally deferred,
anything under-built on purpose per the "under-build and flag" rule.)*

1. **[Phase 4 — unresolved]** 20-minute test hang, root cause unconfirmed. A `go test ./...`
   invocation hung for 20+ minutes during Phase 4 Pass 2 integration; killed before a goroutine
   stack dump could be captured — no direct evidence of cause. Not reproduced across 20×
   uncached subprocess-restart tests + 5× race-instrumented full-suite runs. **Watch during
   Phase 10 soak test** (continuous writes + chaos injection). If it recurs, send `SIGQUIT`
   first (not `kill`) to capture a full goroutine stack dump. Standing rule: always pass
   `-timeout` to every `go test` invocation.

2. **[Phase 5 — OVERLOADED backpressure, intentionally deferred]** `STATUS_OVERLOADED`
   requires a backpressure signal from the Raft layer (e.g. too many outstanding
   `PendingWrites`). No such signal exists yet. Will return `STATUS_UNSPECIFIED` for this
   case in Phase 5 with an explicit comment; proper backpressure is Phase 6+ scope.

3. **[Phase 5 — RequestTable unbounded growth, stated MVP tradeoff]** `RequestTable` in
   the replicated state machine grows indefinitely. Bounded retention/TTL is future work
   (`docs/adr/009-request-identity-model.md`). Snapshotting alone does not shrink it.
   Explicitly not a Phase 5 correctness issue — a resource/scalability concern.

4. **[Phase 4 — bug fixed pre-Pass-3, on developer instruction]** `server.Get` previously
   fell back to `s.sm.Get()` and returned `STATUS_SUCCESS` on `LinearizableGet` error —
   an I-016 violation (exposed stale/unconfirmed state). Fixed: error path now returns
   `STATUS_NOT_LEADER`. Regression-tested in `server_test.go`.

5. **[Audit pattern — watch in future phases]** I-011's tracker "yes" entry was false: the
   test called `SetCommitIndex` directly on an isolated store object, bypassing the Raft
   layer — the guard mechanism worked in isolation but was never armed in production. Fixed
   in Phase 4 Pass 4. **Rule going forward: before marking any invariant "yes," verify at
   least one test exercises the property through the real Raft code path.** Other "yes"
   entries resting solely on isolated storage-layer tests should be re-reviewed in Phase 6.

6. **[Phase 5 — proto status codes resolved]** Four proto status codes
   (`STATUS_REQUEST_ID_REUSED`, `STATUS_TIMEOUT`, `STATUS_NO_LEADER`, `STATUS_OVERLOADED`)
   were verified to already exist in `proto/client.proto` and generated Go code; used as-is.

7. **[Phase 7 flag — RequestTable snapshotting correctness gap]** `RequestTable` is not
   included in the snapshot scope deferred to Phase 7. Phase 7 must explicitly re-verify
   I-017 once snapshotting exists — a node restoring from a snapshot plus a truncated log
   tail must not lose dedup history for compacted entries. This is a correctness gap,
   not just a performance one, and needs to survive five phases without getting lost.

8. **[Phase 10 flag — RequestTable soak test memory tracking]** Noted in `docs/benchmarks.md`
   methodology that the soak test's memory-growth check should track `RequestTable` size
   specifically, since it's the one structure in the system designed to grow unbounded by MVP decision.

9. **[Phase 10 flag — Production Write() commit-latency heartbeat floor]** In production
   `Node.Write(ctx, cmd)`, local WAL append does not immediately dispatch `AppendEntries` to
   followers; replication is driven by the leader's background heartbeat loop
   (`HeartbeatInterval = 50ms`). This introduces a worst-case ~50ms latency floor on committed
   client writes. Noted for Phase 10 commit-latency benchmarking; an immediate-dispatch
   trigger on write can be benchmarked as an optimization in Phase 10.

10. **[Phase 6 flag — Fuzzer Substrate & CI Duration Calibration]**
    - **Substrate split**: The randomized fuzzer (`TestFuzzer_SeededRun` and `raftkv-chaos -scenario=fuzzer`) operates on `InProcessCluster` using `FaultTransport` to dynamically inject transport-level faults (drop rates, latency jitter, partition matrices) without external proxies. In this substrate, `NodeChurn` is simulated via `Node.Stop()` and transport unregistration, rather than OS `SIGKILL`. Real ungraceful crash recovery (Level 4/5 `ProcessCluster` with real `raftkv-node` subprocesses, `syscall.SIGKILL`, and on-disk recovery) is provided specifically by `TestScenarioRollingCrash_10x` (Test 51) and `TestScenarioProcessCrashRecovery_10x` (Test 52).
    - **CI duration calibration**: `TestFuzzer_SeededRun` was intentionally calibrated to 8s active fault injection (~10.05s total wall-clock with cycle quiescence and convergence check) rather than Pass 1's preliminary 60s design estimate, keeping total repository CI runtime under ~2 minutes with race detection enabled while still validating all 5 fault regimes; long-running multi-minute and 30-minute soaks are driven via the standalone CLI (`raftkv-chaos -duration=30m`).

---

## Invariant coverage tracker
*(Which of I-001..I-024 are implemented + which have a test that would fail if violated —
two different things, tracked separately.)*

| ID | Implemented in | Test exists | Notes |
|---|---|---|---|
| I-001 | 1 | yes | At-most-one-leader-per-term; 100× Level-2 election test. |
| I-002 | 2 | yes | Leader append-only, never truncates. |
| I-003 | 2 | yes | Log Matching via `prevLogIndex`/`prevLogTerm`; ≥6 diverging-log scenarios. |
| I-004 | 1 | yes | Log-freshness formula incl. empty-log case. |
| I-005 | 3 | yes | Only committed entries applied; applier respects `commitIndex`. |
| I-006 | 3 | yes | Current-Term Commit Rule; Figure-8 scenario test. |
| I-007 | 1 | yes | All four term-change touchpoints; higher-term response beats role/attempt filtering. |
| I-008 | 3 | yes | Follower passively adopts `leaderCommit` clamped to `lastNewEntryIndex`. |
| I-009 | 3 | yes | `lastApplied ≤ commitIndex` maintained monotonically. |
| I-010 | 3 | yes | `commitIndex ≤ lastLogIndex` enforced. |
| I-011 | 2, **4** | yes | Guard mechanism since Phase 2; **Phase 4 fix wired `SetCommitIndex` into all 3 Raft-layer sites (leader/follower/recovery) — was unarmed in production before.** End-to-end proof: Test 28. |
| I-012 | 1 | yes | Save-before-grant and save-before-outgoing-RequestVote. |
| I-013 | 2 | yes | Follower fsync before ACK; delayed-fsync test. |
| I-014 | 1, **6** | yes | RPCs sent after releasing Raft mutex. Verified on real Raft nodes with concurrent mutex availability probes under 200ms delay: Test 50 (`ScenarioMutexNetworkIOSafety_10x`). |
| I-016 | 3 | yes | Full 3-step linearizable read; regression-tested after Phase 4 `server.Get` fix. |
| I-017 | **5** | yes | Canonical-hash dedup in replicated SM; `ErrRequestIDReused`; `lastApplied` still advances. Real Raft code-path proof: Test 33 (`dedup_test.go`, real Raft apply loop) and Tests 35–36 (Level 4 subprocess tests). Unit: Tests 30–32, 34. |
| I-018 | 2 | yes | Fail-closed on durable write failure; disk-before-memory ordering. |
| I-019 | 3 | yes | Request-identity-aware `PendingWrite` lifecycle. |
| I-020 | 1, 4, **6** | yes | First-boot persist + full corruption-handling (Tests 10–15, 18–26); Level-4/5 `ProcessCluster` (real subprocesses, SIGKILL) multi-boot progression (1->2->3), corruption fail-closed, on-disk repair, and convergence: Test 52 (`ScenarioProcessCrashRecovery_10x`). |
| I-021 | 2 | yes | Stale/out-of-order response triple-check (role/term/attempt). |
| I-022 | 2, 3 | yes | `matchIndex[self]==lastLogIndex`; `commitIndex`/`lastApplied` monotonic. |
| I-023 | 3 | yes | New-leader NOOP commit + `readReadyTerm` gate. |
| I-024 | 1, 4, **6** | yes | Temp-file+rename replacement atomicity; 4-stage crash-point hooks (Phase 4); Level-4/5 `ProcessCluster` (real subprocesses, SIGKILL) sequential rolling crash & recovery under continuous writes with term/log preservation and cluster convergence: Test 51 (`ScenarioRollingCrash_10x`). |
