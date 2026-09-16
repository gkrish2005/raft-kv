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
| **5** | **Client Semantics + Replicated Dedup** | 🔶 **In progress — Pass 2 (Implement)** |
| 6 | Chaos Testing Framework | Not started |
| 7 | Observability | Not started |
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

---

## Current Phase (full detail)

### Phase 5 — Client Semantics + Replicated Dedup
**Status:** Pass 2 (Implement) in progress (Pass 1 approved with developer additions on 2026-09-17)

**Goal:** Close two correctness gaps: (1) duplicate write on leader failover — a local
per-node dedup cache cannot survive the scenario where a leader commits but crashes before
ACK-ing; (2) coarse error mapping and missing client redirect contract in the gRPC server.

**Invariants touched:**
- **I-017 (new invariant):** duplicate `RequestID` + different payload → reject by canonical
  hash (`STATUS_REQUEST_ID_REUSED` / `ErrRequestIDReused`), never silently resolve.
- **I-005 (regression-critical):** `lastApplied` advances unconditionally even on application
  error (`ErrRequestIDReused`). Test 33 serves as explicit proof.
- **I-016 (regression-critical):** Linearizable reads — Pass 3 audit must include explicit
  check on `LinearizableGet` error mapping so no error path returns `STATUS_SUCCESS` with
  unconfirmed data.
- **I-019 (regression-critical):** Dedup-hit reuse of `resolvePendingWriteLocked` must not alter
  (RequestID, Index, Term) keying semantics or allow stale write resolution.
Rules 8 (application errors still applied), 14 (TIMEOUT ≠ failure), 15 (I-017 canonical hash comparison).

**Stated MVP Tradeoff:** Dedup is apply-time only, not append-time — a retried write still
pays a full replication round before being deduped in `ApplyLocked`.

#### Pass 1 — Design ✅ (approved 2026-09-17)

Full design in artifact `phase5_pass1_design.md`. Summary:

**Group 1 — Canonical command encoding** (`internal/storage/kv_statemachine.go`):
`CanonicalEncode(cmd Command) []byte` implementing the exact frozen format from
`docs/client-semantics.md` — `version(u8) | opTypeLen(u8) | opType | keyLen(u32LE) |
key | valueLen(u32LE) | value`. `RequestID` deliberately excluded. `CommandPayloadHash`
wraps `SHA-256(CanonicalEncode(cmd))`.

**Group 2 — `RequestTable` in replicated SM** (`internal/storage/kv_statemachine.go`):
`AppliedRequest{RequestID, PayloadHash, Result}` folded into `KVStateMachine`. Dedup
logic in `ApplyLocked`: table lookup → hash-compare → dedup hit / `ErrRequestIDReused` /
fresh apply + table write. `NOOP` never reaches the table. `lastApplied` still advances
on `ErrRequestIDReused` (application error, not Raft-level rollback — rule 8 / I-005).

**Group 3 — Resource bounds** (`cmd/raftkv-node/main.go`): max key 4 KiB, max value 1 MiB,
`STATUS_INVALID_REQUEST` on violation before log entry. `STATUS_OVERLOADED` deferred — no
backpressure signal from Raft layer yet (flagged in Open Questions).

**Group 4 — Result semantics + error mapping** (`cmd/raftkv-node/main.go`): remove busy-
wait spin-check; add volatile `n.state.leaderID` tracking and `Node.LeaderHint()` accessor;
map all write/read errors to precise status codes per `docs/client-semantics.md` table.
`ErrRequestIDReused` mapped at gRPC server handler level only (single path, I-019 safe).

**Group 5 — `ClusterStatus`** (`cmd/raftkv-node/main.go`): replace hardcoded stub with
live Raft state via new `Node.ClusterView()` accessor.

**Pass 1 Resolutions (approved):**
- Q1: Proto status codes (`STATUS_REQUEST_ID_REUSED`, `STATUS_TIMEOUT`, `STATUS_NO_LEADER`,
  `STATUS_OVERLOADED`) already exist in `proto/client/v1` — use as-is.
- Q2: `n.state.leaderID string` approved — volatile, reset to `""` on election win and step-down.
- Q3: `ErrRequestIDReused` single-path propagation at gRPC server handler level approved.
- Q4: Tests 35–36 located in `cmd/raftkv-node/restart_test.go` (Level 4).
- Test 33 located in new `internal/raft/dedup_test.go` exercising real Raft apply path.

**Tests planned (Tests 30–40):** canonical encoding determinism (30), disambiguation (31),
dedup same-payload (32), dedup different-payload I-017 + I-005 proof (33 in `dedup_test.go`),
never-committed retry (34), commit-before-ack-crash exactly-once (35, Level 4 in `restart_test.go`),
rolling-leader-kill write-survival (36, Level 4 in `restart_test.go`), error mapping — not-leader (37),
timeout (38), invalid request (39), request-id-reused server mapping (40).

#### Pass 2 — Implement 🔶 (in progress)
#### Pass 3 — Audit ⬜ (not started)
#### Pass 4 — Fix ⬜ (not started)
#### Pass 5 — Verify ⬜ (not started)

---

## Upcoming Phases
6. Chaos Testing Framework
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
| I-014 | 1 | review | RPCs sent after releasing Raft mutex — code-review only, no dedicated test yet. |
| I-016 | 3 | yes | Full 3-step linearizable read; regression-tested after Phase 4 `server.Get` fix. |
| I-017 | **5** | pending | Canonical-hash dedup in replicated SM; `ErrRequestIDReused`; `lastApplied` still advances. Tests 30–34 (unit) + 35–36 (Level 4 failover). Pending Phase 5 Pass 2. |
| I-018 | 2 | yes | Fail-closed on durable write failure; disk-before-memory ordering. |
| I-019 | 3 | yes | Request-identity-aware `PendingWrite` lifecycle. |
| I-020 | 1, **4** | yes | First-boot persist + full corruption-handling; Tests 10–15, 18–26. |
| I-021 | 2 | yes | Stale/out-of-order response triple-check (role/term/attempt). |
| I-022 | 2, 3 | yes | `matchIndex[self]==lastLogIndex`; `commitIndex`/`lastApplied` monotonic. |
| I-023 | 3 | yes | New-leader NOOP commit + `readReadyTerm` gate. |
| I-024 | 1, **4** | yes | Temp-file+rename replacement atomicity; 4-stage crash-point hooks (Phase 4). |
