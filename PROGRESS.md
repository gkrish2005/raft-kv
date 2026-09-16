# RaftKV — Progress

Source of truth for "what's actually done" vs. what the phase docs plan. Update this
after every pass (Design/Implement/Audit/Fix/Verify), not just at phase completion.
Nothing gets checked off here unless Pass 5 actually verified it — this file should
never be ahead of reality.

## How to read this file
- **Phase status**: Not started / Design approved / In progress / Pass 5 complete / Approved for next phase
- A phase is only "Approved for next phase" once the developer (not the agent) explicitly signs off per `CLAUDE.md`'s phase discipline rule 5.

## Phase status

| Phase | Name | Status | Notes |
|---|---|---|---|
| 0 | Foundation + Single-Node KV Store | Approved for next phase | Developer approved Phase 0 close-out on 2026-09-13. |
| 1 | Leader Election + Heartbeats | Approved for next phase | Developer approved Phase 1 close-out on 2026-09-13. |
| 2 | Replicated Log + Minimal Durable Log | Approved for next phase | Developer approved Phase 2 close-out on 2026-09-15. |
| 3 | Commit, Apply, and Reads | Approved for next phase | Developer approved Phase 3 close-out on 2026-09-16. Shipped: full 3-step linearizable read protocol (I-016), commit-index advancement with I-006 current-term restriction, request-identity-aware PendingWrite lifecycle (I-019), new-leader no-op commit with the readReadyTerm/leaderNoOpTerm gate (I-023), and idempotent graceful shutdown — all verified under -race. |
| 4 | Crash Recovery + Durable Metadata + WAL Hardening | In progress | Pass 1 design approved at Rev 4 by developer on 2026-09-16. Pass 2 in progress. |
| 5 | Client Semantics + Replicated Dedup | Not started | |
| 6 | Chaos Testing Framework | Not started | |
| 7 | Observability | Not started | |
| 8 | Evidence-Grounded Incident Diagnosis | Not started | |
| 9 | AI Evaluation | Not started | |
| 10 | Benchmarking, Hardening & Final Demo | Not started | |

---

## Current phase detail

### Phase 0 — Foundation + Single-Node KV Store
...

### Phase 1 — Leader Election + Heartbeats + Minimal Durable Term/Vote
...

### Phase 2 — Replicated Log + Minimal Durable Log

**Pass 1 — Design**
- [x] Read phase-02.md, invariants I-002/I-003/I-011/I-013/I-018/I-021/I-022, architecture write path, replication concurrency, and stale response handling.
- [x] Pass 1 design submitted and approved by developer with clarifications on fast-backtrack, mutex release before network I/O, I-013 delayed fsync test, and minimal StorageFailed role.

**Pass 2 — Implement**
- [x] Implemented `LogStore` interface and `FileLogStore` with WAL framing, CRC32, offset mapping, durable truncation (`TruncateFrom`), and fail-closed disk-before-memory ordering.
- [x] Implemented `InMemoryLogStore` with hook support for deterministic fault injection tests.
- [x] Implemented replication engine in `internal/raft/replication.go`: at most one in-flight RPC per follower, per-follower monotonic attempt ID (`replicationAttempt[peer]++`), fast-backtrack conflict discovery and resolution, follower log-matching check, truncate conflicting uncommitted entries, and durable append.
- [x] Added unit tests in `internal/storage/log_store_test.go`, `internal/raft/replication_test.go`, and `internal/raft/stale_response_test.go`.
- [x] Added replication convergence integration test with fault injection and decoded `LogEntry` equality assertions in `internal/cluster/replication_convergence_test.go`.

**Pass 3 — Audit**
- [x] Audited implementation against invariants I-002, I-003, I-011, I-013, I-018, I-021, I-022.
- [x] Investigated finding #4 on attempt-counter isolation: verified that unconditional `peerInFlight[peer] = false` on RPC completion could allow a delayed RPC from an older term to clear the in-flight flag of a fresh term/attempt, violating the at-most-one-in-flight design rule.
- [x] Identified missing explicit test for empty `Entries` slice heartbeat/probe behavior (finding #2).

**Pass 4 — Fix**
- [x] Fixed `replicateToPeer` in `internal/raft/replication.go`: keyed `peerInFlight[peer] = false` clearing to `n.state.role == Leader && n.state.currentTerm == req.Term && n.replicationAttempt[peer] == attempt`.
- [x] Added reproduction test `TestStaleRPCDoesNotClearPeerInFlightInNewTerm` in `internal/raft/replication_test.go`.
- [x] Added `TestAppendEntriesEmptyEntriesHeartbeat` in `internal/raft/replication_test.go` covering empty-batch and heartbeat matching / mismatching scenarios.

**Pass 5 — Verify**
- [x] `go test -count=1 ./...` green (2026-09-15).
- [x] `go test -race -count=1 ./...` green (2026-09-15).
- [x] `go vet ./...` clean (2026-09-15).
- Known limitations:
  - Phase 2 focuses purely on log replication and minimal durable log storage. State machine `Apply()`, `commitIndex` advance past entries, and client reads remain in Phase 3.
- **Developer approval to proceed to Phase 3:** [x] Approved by developer on 2026-09-15.

### Phase 3 — Commit, Apply, and Reads

**Pass 1 — Design**
- [x] Read phase-03.md, invariants I-005/I-006/I-008/I-009/I-010/I-016/I-019/I-022/I-023, architecture write path, write completion, and 3-step read protocol.
- [x] Pass 1 design submitted and approved by developer at Rev 10 with clarifications on follower commitIndex clamp, stepDownLocked term capture and I-018 StorageFailed fatality, channel-based signaling (option b), shutdown handling in read path, and confirmedAttempt I-021 gating.

**Pass 2 — Implement**
- [x] Implement `commitIndex` advancement in `internal/raft/commit.go` with I-006 current-term restriction.
- [x] Implement follower `leaderCommit` adoption with `min(leaderCommit, lastNewEntryIndex)` in `internal/raft/replication.go`.
- [x] Implement dedicated `applierLoop` in `internal/raft/apply.go` advancing `lastApplied` strictly after `sm.applyLocked` (I-005, I-009).
- [x] Implement `PendingWrite` with buffered capacity-1 `Done` channel and request-identity verification in `resolvePendingWriteLocked` (I-019).
- [x] Implement new-leader NOOP append and `readReadyTerm` gate in `election.go` and `apply.go` (I-023).
- [x] Implement full 3-step linearizable read protocol in `internal/client/api.go` (quorum heartbeat confirmation, readReadyTerm gate, barrier wait, role/term revalidation under SM RLock) (I-016).
- [x] Restructure `KVStateMachine` to embed `sync.RWMutex` with external locking (`applyLocked`/`getLocked`).

**Pass 3 — Audit**
- [x] Audit implementation against invariants I-005, I-006, I-008, I-009, I-010, I-016, I-019, I-022, I-023. Identified Finding 1 (pre-existing replication_convergence_test expected lastIndex adjusted for I-023 NOOP).

**Pass 4 — Fix**
- [x] Updated replication_convergence_test.go expected lastIndex from 3 to 4 with explanatory comment citing I-023 leader current-term NOOP.

**Pass 5 — Verify**
- [x] `go test -count=1 ./...` green (2026-09-16).
- [x] `go test -race -count=1 ./...` green (2026-09-16).
- [x] `go vet ./...` clean (2026-09-16).
- [x] Developer approval to proceed to Phase 4: [x] — approved 2026-09-16. Shipped: full 3-step linearizable read protocol (I-016), commit-index advancement with I-006 current-term restriction, request-identity-aware PendingWrite lifecycle (I-019), new-leader no-op commit with the readReadyTerm/leaderNoOpTerm gate (I-023), and idempotent graceful shutdown — all verified under -race.

### Phase 4 — Crash Recovery + Durable Metadata + WAL Hardening

**Pass 1 — Design**
- [x] Read phase-04.md, invariants I-005/I-011/I-012/I-018/I-020/I-022/I-024, recovery sequence, replacement atomicity, and fail-closed storage error handling.
- [x] Pass 1 design submitted and approved by developer at Rev 4 with clarifications on never calling Node.Stop() for pre-crash states, volatile commitIndex needing no sync hook, constructor injection for FileTermVoteStore allowedNodes, 4-stage replacement atomicity hooks (AfterTmpWrite, AfterTmpFsync, AfterRename, AfterDirFsync), and restored Test 13 alongside Test 26 defense-in-depth.

**Pass 2 — Implement**
- [x] Group 1: WAL implementation (`FileWAL` in `internal/storage/wal.go`) with 1 MiB framing, sequential scan, and Tests 1–9 in `internal/storage/wal_test.go`.
- [x] Group 2: `TermVoteStore` hardening in `internal/storage/term_vote_store.go` with constructor injection, 4-stage atomicity hooks, semantic validation, and Tests 10–15 in `internal/storage/term_vote_store_test.go`.
- [x] Group 3: `FileLogStore` refactor wrapping `FileWAL` with preconditions, recovery, and Tests 16–17 in `internal/storage/log_store_test.go`.
- [x] Group 4: Node recovery sequence (`Node.Recover()`) and fail-closed handling in `internal/raft/node.go`, and Tests 18–26 in `internal/raft/crash_test.go`.
- [x] Group 5: Level 4 SIGKILL smoke test in `cmd/raftkv-node/restart_test.go` (Test 27) and `cmd/raftkv-node/main.go` constructor call update.

**Pass 3 — Audit**
- [ ] Audit implementation against invariants I-005, I-011, I-012, I-018, I-020, I-022, I-024 and AGENTS.md rules.

**Pass 4 — Fix**
- [ ] Fix any issues identified in Pass 3 after writing/reproducing failing tests.

**Pass 5 — Verify**
- [ ] Verify test suite: `go test -count=1 ./...`, `go test -race -count=1 ./...`, `go vet ./...`.

---

## Open questions / flagged gaps
_(Running log — anything the agent flagged instead of guessing, anything intentionally deferred to a later phase, anything under-built on purpose per the "under-build and flag" rule.)_

- **Phase 3 / Pass 3 Finding 1**: `internal/cluster/replication_convergence_test.go` expected `lastIndex == 3` after 3 client writes in Phase 2. In Phase 3, I-023 mandates that a new leader appends a NOOP at index 1 upon election, shifting the 3 client entries to indices 2–4 (`lastIndex == 4`). Flagged for developer sign-off to update expected index from 3 to 4 in Pass 4.

## Invariant coverage tracker
_(Fill in as phases progress — which of I-001..I-024 are implemented + which have a test that would fail if violated. Not the same thing — track both.)_

| Invariant ID | Implemented in phase | Test exists | Notes |
|---|---|---|---|
| I-001 | 1 | yes | At-most-one-leader-per-term via durable one-vote-per-term; Level-2 100× election test asserts a single leader after the stepped timeout. |
| I-002 | 2 | yes | Leader only appends to its own log; never truncates. Tested in `TestLeaderLocalAppendUpdatesMatchIndexSelf`. |
| I-003 | 2 | yes | Log Matching enforced by AppendEntries `prevLogIndex`/`prevLogTerm` validation. Covered by $\ge 6$ diverging log scenarios in `replication_test.go`. |
| I-004 | 1 | yes | Log-freshness formula applied (empty-log zeros); `TestRequestVoteRejectsStaleLogUsingExactFormula`. Completeness as a log-contents property is Phase 2+. |
| I-005 | 3 | yes | State Machine Safety: Only committed entries applied; applier strictly respects `commitIndex`. Tested in `commit_test.go`. |
| I-006 | 3 | yes | Current-Term Commit Rule: Leader only commits current-term entries directly. Tested in `TestCommitIndexAdvanceRequiresCurrentTerm` and `TestFigure8ScenarioCurrentTermRestriction`. |
| I-007 | 1 | yes | All four term-change touchpoints; higher-term vote/append response handled before role/attempt filtering. |
| I-008 | 3 | yes | Follower passively adopts leaderCommit clamped to `lastNewEntryIndex`. Tested in `TestFollowerCommitIndexClampedToLastNewEntryIndex`. |
| I-009 | 3 | yes | `lastApplied <= commitIndex` maintained monotonically; applier advances strictly after apply. Tested in `TestCommitIndexAdvanceTransientGapWithLastApplied`. |
| I-010 | 3 | yes | `commitIndex <= lastLogIndex` enforced in `tryAdvanceCommitIndexLocked`. |
| I-011 | 2 | yes | Committed entries never truncated; `LogStore.TruncateFrom` guards `index <= commitIndex`. Tested in `log_store_test.go`. |
| I-012 | 1 | yes | Save-before-grant and save-before-outgoing-RequestVote tests. |
| I-013 | 2 | yes | Follower WAL fsync before ACK; tested in `TestFollowerFsyncDelayedMatchIndexDoesNotAdvanceUntilComplete`. |
| I-014 | 1 | review | Vote/heartbeat/replication RPCs are sent after releasing the Raft mutex. |
| I-016 | 3 | yes | Full 3-step linearizable read: quorum confirmation, barrier wait, term/role revalidation under SM RLock. Tested in `read_test.go`. |
| I-018 | 2 | yes | Durable write failure fails closed (`StorageFailed`), disk-before-memory mutation strictly enforced. |
| I-019 | 3 | yes | Request-identity-aware PendingWrite waiter lifecycle. Tested in `TestPendingWriteIdentitySuperseded` and `TestPendingWriteLeadershipLossCleanup`. |
| I-020 | 1 | partial | First-boot persist lives in `TermVoteStore.Load`; full corruption-handling coverage is Phase 4. |
| I-021 | 2 | yes | Stale/out-of-order AppendEntries responses ignored via triple check: `role == Leader`, `Term == currentTerm`, `attempt == replicationAttempt[peer]`. Tested in `stale_response_test.go` and `TestStaleAppendEntriesResponseDoesNotUpdateConfirmedAttempt`. |
| I-022 | 2, 3 | yes | `matchIndex[self] == lastLogIndex` maintained on leader local appends (Phase 2, `TestLeaderLocalAppendUpdatesMatchIndexSelf`); `commitIndex` and `lastApplied` monotonic non-decreasing (Phase 3, `TestCommitIndexAdvanceTransientGapWithLastApplied`). |
| I-023 | 3 | yes | New-leader current-term NOOP commit and `readReadyTerm` gate. Tested in `TestNewLeaderNoOpReadTestA` and `TestNewLeaderReadBlockedBeforeNoOpTestB`. |
| I-024 | 1 | yes | `TermVoteStore` tests cover temp-file + rename replacement. |
