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
| 1 | Leader Election + Heartbeats | Pass 5 complete | Re-verified 2026-09-13 after the post-restart liveness fix. Not approved for Phase 2 until the developer signs off. |
| 2 | Replicated Log + Minimal Durable Log | Not started | |
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

**Pass 1 — Design**
- [x] Docs read: CLAUDE.md, PRD.md, invariants.md, phase-00.md, architecture.md
- [x] Approach explained
- [x] Invariant IDs touched: _(Phase 0 predates Raft — expect none)_
- [x] Files to create/change listed and approved by developer
- Ambiguities/conflicts flagged: _(none)_

**Pass 2 — Implement**
- [x] Dependency normalization completed: `go get gopkg.in/yaml.v3 && go mod tidy` (no `go.mod` or `go.sum` diff).
- [x] Build and test targets green: `make build`, `make test`, `make test-race`, `make vet` (2026-09-13).
- [x] Scope implemented matches Pass 1 exactly, nothing extra (`Noop` was confirmed as part of the approved Command contract.)

**Pass 3 — Audit**
- [x] Implementation checked against invariants identified in Pass 1 _(none; Phase 0 predates Raft)_
- [x] Implementation checked against CLAUDE.md rules and Phase 0 exit criteria
- Findings:
  - Automated CLI smoke test initially used an in-process node server; corrected in Pass 4 to run the real `raftkv-node` binary.
  - `storage.Noop` and its test were confirmed as explicitly approved Pass 1 scope; no issue.
  - `ClusterStatus` reports hardcoded `Role` and `Term` placeholders because Phase 0 has no Raft/election state. The server labels them non-authoritative; Phase 1 must replace them with real election state. The smoke test asserts only Phase 0-owned structural status output.
  - Structured logging is present on every RPC-handler outcome path; no issue found.
  - `Get` correctly distinguishes missing keys from stored empty values; no issue found.

**Pass 4 — Fix**
- [x] Corrected `TestCLISmoke` to build and run the real `raftkv-node` binary with `-id` and `-addr`, poll until it listens, validate CLI `get`, `set`, `delete`, and `status` output, then cancel and wait for the child process.
- [x] Loosened status smoke assertions to exclude hardcoded Raft placeholders while preserving Phase 0 structural coverage.
- Issues fixed: required real-node CLI smoke-test coverage.

**Pass 5 — Verify**
- [x] `go test ./...` green _(fresh final Pass 5 run after status-placeholder correction, 2026-09-13)_
- [x] `go test -race ./...` green _(fresh final Pass 5 run after status-placeholder correction, 2026-09-13)_
- [x] `go vet` / lint clean _(fresh final Pass 5 run after status-placeholder correction, 2026-09-13)_
- Known limitations: _(none yet)_
- **Developer approval to proceed to Phase 1:** ☑ Approved 2026-09-13

### Phase 1 — Leader Election + Heartbeats + Minimal Durable Term/Vote

**Pass 1 — Design**
- [x] Read phase-01.md, required architecture sections, invariants I-001/I-004/I-007/I-012/I-014/I-020/I-024, and deterministic-testing requirements.
- [x] Design approved by developer, including an empty `AppendEntries` heartbeat RPC and deferred `ClusterStatus` wiring until Phase 5.
- [x] Additional generated-proto, test, and Makefile support files approved.

**Pass 2 — Implement**
- [x] Scope implemented matches approved Pass 1 exactly, nothing extra.
- Implemented Raft state/election loop, injectable Clock/Timer, gRPC transport, crash-atomic TermVoteStore, generated Raft RPCs, and deterministic election/persistence tests.

**Pass 3 — Audit**
- [x] Re-opened after the first Pass 5 wired client-facing `ClusterStatus` (Phase 5 scope). `ClusterStatus` remains a labeled placeholder; real election state is asserted from process logs, not the client RPC.
- [x] Re-audit of the Level-4 kill/restart path found a real liveness bug, not just a test-timing issue: after winning an election, `handleElectionTimeout` re-armed the leader's timer with a randomized election timeout (`250–400ms`) instead of `HeartbeatInterval` (`50ms`). Heartbeats then depended on a droppable `resetTimer` send. If that send was lost, a surviving follower could time out and start a new election, disrupting the replacement leader when the killed node later rejoined.

**Pass 4 — Fix**
- [x] Added `TestHandleElectionTimeoutReturnsHeartbeatIntervalAfterWin` (failed before the production change: next timer was `388ms`, want `50ms`).
- [x] `handleElectionTimeout` now returns `HeartbeatInterval` whenever the node is leader after the timeout, including immediately after winning.
- [x] Election loop prefers a pending heartbeat/vote-grant reset over a raced timer expiry; timer `Reset` drains stale firings; a newer reset overwrites a queued one.
- [x] `TestMultiProcessLeaderKillAndRestart -count=5` re-run after the fix: PASS (3.30s, 2.83s, 3.46s, 3.78s, 2.95s). The pre-fix `-count=5` run in this session also passed, so that test alone was not treated as proof; the unit test is what demonstrated the heartbeat-interval bug.

**Pass 5 — Verify**
- [x] `go test ./...` green (2026-09-13, after the heartbeat-interval fix)
- [x] `go test -race ./...` green (2026-09-13, after the heartbeat-interval fix)
- [x] `go vet ./...` clean (2026-09-13, after the heartbeat-interval fix)
- Known limitations:
  - Client-facing `ClusterStatus` is still a Phase 0/5 placeholder (`Role`/`Term`/`LeaderId` are not Raft state). Phase 1 acceptance uses process logs (`raft leader elected` / `raft node started`).
  - `Node.Start()` still arms the election timer before `gRPC Serve` in `cmd/raftkv-node`. The listen gap is milliseconds versus a `250ms` minimum election timeout, so a restarted node can receive a leader heartbeat before campaigning; this was checked and not the failure mode above.
  - The Level-2 simulator elects by stepping node `a`'s timeout rather than a full per-node scheduler; it still runs 100 iterations each for 3- and 5-node clusters.
- **Developer approval to proceed to Phase 2:** ☐ not yet — stop here.

**Exit criteria (from phase-01.md)**
- [x] election converges reliably across 100+ repeated Level-2 deterministic-simulator runs
- [x] step-down on higher term proven by test, covering all four touchpoints
- [x] `TermVoteStore` durability proven by both crash-point variants (plus the self-vote-before-outgoing-RequestVote variant)
- [x] heartbeats suppress follower timeouts (unit coverage plus Level-4 kill/restart; leader re-arms at `HeartbeatInterval` after winning)
- [x] `go test -race ./...` clean
- [x] no direct `time.*` calls in `internal/raft/` outside the Clock impl
- [x] no direct gRPC construction in `internal/raft/` outside the Transport impl

---

## Open questions / flagged gaps
_(Running log — anything the agent flagged instead of guessing, anything intentionally deferred to a later phase, anything under-built on purpose per the "under-build and flag" rule.)_

- None yet.

## Invariant coverage tracker
_(Fill in as phases progress — which of I-001..I-024 are implemented + which have a test that would fail if violated. Not the same thing — track both.)_

| Invariant ID | Implemented in phase | Test exists | Notes |
|---|---|---|---|
| I-001 | 1 | yes | At-most-one-leader-per-term via durable one-vote-per-term; Level-2 100× election test asserts a single leader after the stepped timeout. |
| I-004 | 1 | yes | Log-freshness formula applied (empty-log zeros); `TestRequestVoteRejectsStaleLogUsingExactFormula`. Completeness as a log-contents property is Phase 2+. |
| I-007 | 1 | yes | All four term-change touchpoints; higher-term vote-response is handled before candidacy filtering. |
| I-012 | 1 | yes | Save-before-grant and save-before-outgoing-RequestVote tests. |
| I-014 | 1 | review | Vote/heartbeat RPCs are sent after releasing the Raft mutex. |
| I-020 | 1 | partial | First-boot persist lives in `TermVoteStore.Load`; full corruption-handling coverage is Phase 4. |
| I-024 | 1 | yes | `TermVoteStore` tests cover temp-file + rename replacement. |
