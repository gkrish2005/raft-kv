# Phase 1 — Leader Election + Heartbeats + Minimal Durable Term/Vote

## Goal
Implement Raft roles/terms/votedFor *through* a working election loop — built on the injectable Clock/Transport from `docs/architecture.md` from the start — **with `currentTerm`/`votedFor` durably persisted from this phase onward**, not deferred.

## Scope
- Follower/Candidate/Leader roles, randomized election timeout, RequestVote RPC using the exact log-freshness formula in `docs/architecture.md` (§RequestVote log-freshness formula — this is the protocol rule, applies identically to empty and non-empty logs, not a special case). **Use the frozen timing values from `docs/architecture.md`'s "Heartbeat/election-timeout relationship" section (`heartbeat_interval = 50ms`, `election_timeout` randomized in `[250ms, 400ms]`) — don't independently pick different numbers.**
- Heartbeats (empty AppendEntries) suppressing further elections; step-down on higher term seen on **any** of the four term-change touchpoints: incoming RequestVote, incoming AppendEntries, RequestVote *response*, AppendEntries *response* (`docs/invariants.md` I-007).
- **A minimal `TermVoteStore`**, durable, fsync-before-response, for `currentTerm`/`votedFor` (I-012). This is required *in this phase*, not deferred to Phase 4 — Phase 4 hardens it (corruption handling, full recovery integration with the log), it does not introduce term/vote durability from nothing. **Ordering requirement, explicit:** on starting an election, persist `{currentTerm+1, votedFor=self}` via `Save()` **before sending any outgoing `RequestVote` RPC**, not only before responding to incoming RPCs — a crash after sending votes but before that fsync completes must not let the node vote again in the same term on restart (I-012).

```go
type TermVoteStore interface {
    Save(term uint64, votedFor string, bootID uint64) error  // must fsync before returning
    Load() (term uint64, votedFor string, bootID uint64, err error)
}
```
Persisted as a single checksummed record (full format, atomicity, and corrupt-record-fails-startup contract in `docs/architecture.md`'s TermVoteStore section, I-020) — this phase implements the basic save/load/fsync path; full corruption-detection test coverage is Phase 4's job, but the interface and the "corrupt record → refuse to start, never guess" behavior should already be correct here, since Phase 4 hardens rather than introduces it.

## Non-goals
No log replication (Phase 2), no LogStore/WAL for log entries (Phase 2/4), no commit/apply. Do not build a full crash-recovery sequence here — that's Phase 4's `Node.Recover()`; this phase only needs `TermVoteStore` to survive a restart on its own, tested at Level 2/4 (below), not via the full chaos framework (which doesn't exist until Phase 6).

## Files allowed to change (expected — additional files require Pass-1 approval, see `CLAUDE.md`)
`internal/raft/{node,election,state}.go`, `proto/raft.proto`, `internal/cluster/transport.go`, `internal/raft/clock.go` (Clock/Timer + fake impl), `internal/storage/term_vote_store.go` (new).

## Interfaces
- `RaftService` gRPC service exposing `RequestVote`.
- `Clock`/`Timer`, `Transport` per `docs/architecture.md`.
- `TermVoteStore` per above.
- `Node{ Start(), Stop(), Role() Role, Term() uint64 }`.

## State changes
`NodeState` (`currentTerm`, `votedFor`, `role`) — `votedFor`/`currentTerm` now durable via `TermVoteStore`. No `log[]`/`commitIndex`/`lastApplied`/`nextIndex`/`matchIndex` activity yet.

## Invariants affected
I-001 (Election Safety), I-004 (Leader Completeness — the *property* the election restriction exists to guarantee; the log-freshness formula that implements the check, exercised even on empty logs, lives in `docs/architecture.md` and should be cited separately from I-004 itself — see `docs/invariants.md`'s note on this distinction, which this phase previously got wrong), I-007 (Term Monotonicity, all four touchpoints), I-012 (durable `currentTerm`+`votedFor` persisted together as one atomic record, including the self-vote-before-outgoing-RequestVote ordering — the defining constraint of this phase's persistence work, renamed/broadened from earlier drafts that described this only in terms of `votedFor`), I-014 (no network I/O under lock), I-020 (corrupt term/vote metadata fails startup rather than guessing, including the first-boot-must-persist-immediately requirement — basic version required here, full corruption-handling test coverage lands in Phase 4), I-024 (`TermVoteStore.Save()`'s replacement mechanism must be crash-atomic via temp-file+fsync+rename+directory-fsync, not an in-place overwrite — required from this phase's first implementation of `Save()`, since Phase 4 hardens corruption *detection*, not the atomicity of the write path itself).

## Tests required
- **Level 1:** term-comparison unit tests covering all four term-change touchpoints; vote-granting rules using the exact log-freshness formula (deny stale term, deny already-voted-for-someone-else, deny stale log per the formula — including the all-zero/empty-log case, which the formula already handles without a special case). **Stale-vote-response tests (new, required, mirroring the AppendEntries attempt-ID tests in Phase 2):** a response arriving after `role != Candidate` is ignored; a response from an abandoned earlier candidacy (this node started a second election, bumping `electionTerm`) is ignored via the `electionTerm` check, not merely a bare term comparison. **Higher-term-on-vote-response test (new, required, P0 — `docs/architecture.md`'s "RequestVote responses follow..." section, I-007):** a candidate in term 5 receives a `RequestVoteResponse` with `Term = 6` — assert `currentTerm` becomes 6, `role` becomes `Follower`, the term/vote transition is durably persisted, the in-progress election is abandoned, and the response does not count as a vote. This must pass **regardless of `electionTerm`** — the higher-term check is evaluated before, and is never suppressed by, the election-generation filter.
- **Level 2 (mandatory, deterministic simulator per `docs/testing.md`):** 3/5-node cluster converges to exactly one leader, repeated 100+ times with zero flakiness.
- **Level 2/4, `TermVoteStore` durability:** save term/vote, simulate/perform a process restart, load and assert unchanged. Two explicit crash-point variants (both required — this is a strong, specific test, not generic): (a) crash **before** the fsync completes — on restart, the vote must not have been granted (or, if using a real crash simulation that can't roll back an in-flight syscall, this variant documents the boundary condition rather than testing an un-testable partial state); (b) crash **after** fsync completes but **before** the RPC response is sent — on restart, the vote **must** be preserved (this is the case that actually matters and must be verified). **A third, distinct variant specifically for self-votes:** crash after the self-vote's fsync completes but **before any `RequestVote` RPC is sent** — on restart, the candidate's self-vote must be preserved and the node must not be able to grant a second vote to a different candidate in that same term (this is the outgoing-RPC-side analogue of variant (b), and catches the specific bug of persisting the self-vote after sending votes rather than before).
- Do **not** make this phase's acceptance test depend on the full chaos/process-kill framework (Phase 6) — a lightweight Level 4 `kill`+restart smoke test (real OS process, not the fault-injection framework) is sufficient here.

## Failure cases to handle
Non-randomized/too-narrow timeout range causing repeated split votes. Forgetting to reset the election timer on granting a vote. Race between the election-timer goroutine and RPC handlers touching `NodeState` without the lock. Persisting term/vote *after* sending an RPC response instead of before (violates I-012's ordering requirement). **Persisting the self-vote *after* sending `RequestVote` RPCs instead of before** — this is the same class of ordering bug as the response-side case, but on the outgoing/candidate side, and is easy to miss because it doesn't involve responding to anything.

## Acceptance criteria
Kill the leader process (Level 4 real-process smoke test) → new leader eventually elected (liveness — convergence time measured, not promised as a fixed multiple of the timeout, per `docs/failure-model.md`), verified via `ClusterStatus`/logs on all survivors. Restart the killed node → rejoins as follower without disrupting the current leader, with its `currentTerm`/`votedFor` correctly recovered from `TermVoteStore`.

## Interview concepts
Why randomized timeouts avoid synchronized re-elections. Why the log-freshness check applies identically to empty logs — there's no special case, the general formula already produces the right answer. Why term/vote must be durable *before* responding, covering all four places a term can change, not just incoming requests — the classic bug is forgetting the response-side cases.

## Exit criteria
- [ ] election converges reliably across 100+ repeated Level-2 deterministic-simulator runs
- [ ] step-down on higher term proven by test, covering all four touchpoints
- [ ] `TermVoteStore` durability proven by both crash-point variants
- [ ] heartbeats suppress follower timeouts
- [ ] `go test -race ./...` clean
- [ ] no direct `time.*` calls in `internal/raft/` outside the Clock impl
- [ ] no direct gRPC construction in `internal/raft/` outside the Transport impl
