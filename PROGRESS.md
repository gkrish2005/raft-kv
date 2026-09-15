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
| 2 | Replicated Log + Minimal Durable Log | Pass 5 complete | Re-verified 2026-09-15 with finding #2 (empty entries) and #4 (attempt-keyed peerInFlight clearing) fixes and tests. |
| 3 | Commit, Apply, and Reads | Not started | |
| 4 | Crash Recovery + Durable Metadata + WAL Hardening | Not started | |
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
- **Developer approval to proceed to Phase 3:** ☐ not yet — stop here.

---

## Open questions / flagged gaps
_(Running log — anything the agent flagged instead of guessing, anything intentionally deferred to a later phase, anything under-built on purpose per the "under-build and flag" rule.)_

- None yet.

## Invariant coverage tracker
_(Fill in as phases progress — which of I-001..I-024 are implemented + which have a test that would fail if violated. Not the same thing — track both.)_

| Invariant ID | Implemented in phase | Test exists | Notes |
|---|---|---|---|
| I-001 | 1 | yes | At-most-one-leader-per-term via durable one-vote-per-term; Level-2 100× election test asserts a single leader after the stepped timeout. |
| I-002 | 2 | yes | Leader only appends to its own log; never truncates. Tested in `TestLeaderLocalAppendUpdatesMatchIndexSelf`. |
| I-003 | 2 | yes | Log Matching enforced by AppendEntries `prevLogIndex`/`prevLogTerm` validation. Covered by $\ge 6$ diverging log scenarios in `replication_test.go`. |
| I-004 | 1 | yes | Log-freshness formula applied (empty-log zeros); `TestRequestVoteRejectsStaleLogUsingExactFormula`. Completeness as a log-contents property is Phase 2+. |
| I-007 | 1 | yes | All four term-change touchpoints; higher-term vote/append response handled before role/attempt filtering. |
| I-011 | 2 | yes | Committed entries never truncated; `LogStore.TruncateFrom` guards `index <= commitIndex`. Tested in `log_store_test.go`. |
| I-012 | 1 | yes | Save-before-grant and save-before-outgoing-RequestVote tests. |
| I-013 | 2 | yes | Follower WAL fsync before ACK; tested in `TestFollowerFsyncDelayedMatchIndexDoesNotAdvanceUntilComplete`. |
| I-014 | 1 | review | Vote/heartbeat/replication RPCs are sent after releasing the Raft mutex. |
| I-018 | 2 | yes | Durable write failure fails closed (`StorageFailed`), disk-before-memory mutation strictly enforced. |
| I-020 | 1 | partial | First-boot persist lives in `TermVoteStore.Load`; full corruption-handling coverage is Phase 4. |
| I-021 | 2 | yes | Stale/out-of-order AppendEntries responses ignored via triple check: `role == Leader`, `Term == currentTerm`, `attempt == replicationAttempt[peer]`. Tested in `stale_response_test.go`. |
| I-022 | 2 | yes | `matchIndex[self] == lastLogIndex` maintained immediately on leader local appends. |
| I-024 | 1 | yes | `TermVoteStore` tests cover temp-file + rename replacement. |
