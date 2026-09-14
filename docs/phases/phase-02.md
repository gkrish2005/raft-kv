# Phase 2 — Replicated Log + Minimal Durable Log

## Goal
Get log entries flowing from leader to followers with correct conflict resolution, backed by a minimal durable `LogStore` with a fully-specified truncation contract — no application to the state machine happens in this phase.

## Scope
- `AppendEntries` with real entries; `nextIndex`/`matchIndex` tracking (initialized exactly per `docs/architecture.md`'s formula on becoming leader — `nextIndex[peer] = lastLogIndex+1`, `matchIndex[peer] = 0`, `matchIndex[self] = lastLogIndex`, updated on every leader append, not only via follower responses); log-matching consistency check; conflict detection + truncation using the exact `AppendEntriesResponse{Term, Success, ConflictIndex, ConflictTerm}` shape and fast-backtrack algorithm in `docs/architecture.md`.
- **At most one AppendEntries RPC in flight per follower at a time** (`docs/architecture.md`'s "Replication concurrency" rule), with the stale/out-of-order response handling from `docs/architecture.md`'s "Stale response handling" section (I-021): ignore responses with `Term < currentTerm`, step down without touching replication indices on `Term > currentTerm`, ignore a same-term response if `role != Leader`, and — via a per-follower monotonic `attemptID` counter incremented on every send (transport/replicator-internal, not a wire field) — ignore a same-term response that doesn't belong to the currently active attempt for that follower. **All three of `role == Leader`, `Term == currentTerm`, and `attempt == current attempt` are required before mutating `nextIndex`/`matchIndex` — the at-most-one-in-flight rule alone does not make this unnecessary, since a timed-out attempt's response can still arrive after a newer attempt to the same follower is already outstanding.**
- `LogStore` interface + a **minimal** `FileLogStore`: append-only-with-durable-truncation, fsync-before-return on both `Append` and `TruncateFrom`, per `docs/architecture.md`'s "Durable Conflict Truncation" section (byte-offset map rebuilt from WAL replay on startup, truncate-then-fsync contract, guarded so it's unreachable for any index ≤ `commitIndex`).

```go
type LogStore interface {
    Append(entries []LogEntry) error   // must be durable (fsync) before returning
    TruncateFrom(index uint64) error   // must be durable (fsync) before returning
    Get(index uint64) (LogEntry, error)
    LastIndex() uint64
}
```

## Non-goals
**No application to the KV state machine happens in this phase.** No commitIndex/lastApplied activity. No corruption/torn-write handling beyond what the WAL record format (`docs/architecture.md`) already specifies structurally — deep corruption-recovery testing is Phase 4. No client-visible write success yet.

## Files allowed to change (expected — additional files require Pass-1 approval)
`internal/raft/replication.go`, `internal/storage/log_store.go` (new, includes `TruncateFrom`), `internal/storage/in_memory_log_store.go` (Level 1/2 tests).

## Interfaces
`AppendEntries` per `docs/architecture.md`'s RPC contract and exact response shape. `LogStore` as above.

## State changes
`NodeState.Log[]` (real entries), `NodeState.NextIndex[]`/`MatchIndex[]` (leader-only, reset exactly per the initialization formula on becoming leader).

## Invariants affected
I-002 (Leader Append-Only), I-003 (Log Matching — including that the entries themselves, not just preceding entries, are identical when index+term match), I-011 (truncation never touches `commitIndex` or below), I-013 (a follower's replication is not counted until its own fsync completes), I-018 (a `LogStore.Append`/`TruncateFrom` failure fails the node closed — no ACK, no replication-state update; in-memory `log[]`/offset-map mutation strictly after the fsync succeeds, never before), I-021 (stale/out-of-order AppendEntries responses are never applied to `nextIndex`/`matchIndex` — enforced via the `role == Leader` + `Term == currentTerm` + `attempt == current attempt` triple-check, not term alone), I-022 (`matchIndex[self]` only ever increases during normal operation, updated on every append).

## Tests required
Unit: log-matching check with hand-constructed diverging logs (leader ahead, follower ahead-and-wrong, empty follower log, conflicting term at various positions) — invest real time here. `LogStore.TruncateFrom` unit tests: truncate mid-log, truncate to the very start, truncate-then-append, and the crash-immediately-after-truncate scenario (verify the WAL replay scan recovers to a safe, intact prefix — see `docs/architecture.md` and `docs/testing.md`'s WAL test list, some of which land here and some in Phase 4). Integration: using the convergence-test scenario shape from `docs/testing.md` (inject fault → replicate under fault → heal → assert eventual convergence, never "assert convergence while still faulted"), a SET issued to the leader eventually appears in every follower's **raw log**. **Assert this via decoded `LogEntry` equality (index, term, command, RequestID, payload) — not raw serialized-byte comparison.** Byte-for-byte WAL serialization correctness (including CRC/truncation behavior) is tested separately, at the storage/WAL level (`docs/testing.md`'s WAL test list) — coupling the Raft replication test to exact protobuf-encoded bytes would make it brittle against encoding details that have nothing to do with Raft correctness. **Stale-response test (new, required):** delay a follower's AppendEntries response until after the leader's term has advanced past the request's term — assert the response is ignored and does not mutate `nextIndex`/`matchIndex`. **Attempt-ID test (new, required):** send a request to a follower, let it time out locally, send a second request to the same follower, then deliver the **first** request's same-term response — assert it is ignored (does not mutate `nextIndex`/`matchIndex`) because it doesn't match the currently active attempt. **Role-check test (new, required):** deliver a same-term AppendEntries response after the node has already stepped down to Follower (e.g. via a same-term AppendEntries from another leader) — assert it does not mutate replication state.

## Failure cases to handle
Truncating at or below `commitIndex` (guarded, should be structurally unreachable at this phase since nothing advances commitIndex yet — but the guard itself belongs to `LogStore` and must exist regardless). Off-by-one index arithmetic. Leader's own log entry not counting toward its own `matchIndex` immediately (must be updated on append, not only via follower RPC responses — see `docs/architecture.md`'s write-path diagram). **Mutating the in-memory `log[]`/offset map before the corresponding `LogStore.Append`/`TruncateFrom` fsync has succeeded** — `docs/architecture.md`'s write path specifies disk-then-memory ordering exactly once and applies it uniformly; do not append to memory first "for simplicity" and only check the durable-write error afterward.

## Acceptance criteria
A SET operation issued to the leader eventually exists as a logically-equal `LogEntry` in every follower's raw log (decoded-value comparison, not raw bytes), verified under the explicit inject-fault→heal→assert-convergence scenario shape (`docs/testing.md`). A stale AppendEntries response never mutates replication state.

## Interview concepts
Why `prevLogIndex`/`prevLogTerm` alone is sufficient given Log Matching holds inductively. Why truncation is safe only for the uncommitted suffix, and why that's enforced as a property of the storage layer itself (a guard in `LogStore`), not only as a property of higher-level Raft logic — defense in depth.

## Common bug to explicitly avoid
Do not apply-on-append "just to see it work end to end faster." Use a debug endpoint dumping raw log contents instead.

## Exit criteria
- [ ] log-matching unit tests cover ≥6 divergence scenarios
- [ ] `LogStore.TruncateFrom` fully tested including the crash-after-truncate scenario
- [ ] fast-backtrack conflict optimization implemented exactly per `docs/architecture.md`'s algorithm and tested
- [ ] integration test uses the inject→heal→assert-convergence scenario shape and decoded-`LogEntry` equality, not raw-byte comparison
- [ ] stale-response test passes (ignored on lower term, no replication-state mutation on higher term)
- [ ] zero state-machine `Apply()` calls anywhere in this phase's code paths
- [ ] `go test -race ./...` clean
