# Phase 4 — Crash Recovery + Durable Metadata + WAL Hardening

## Goal
Harden Phase 1's `TermVoteStore` and Phase 2's minimal `LogStore` into a fully crash-safe WAL: complete corruption/torn-write handling and the full startup recovery sequence. This phase *extends* already-durable term/vote and log storage — it does not invent durability from nothing (both started in Phase 1/2 respectively).

## Scope
- Full WAL record format per `docs/architecture.md`: `[length: uint32 LE][payload: protobuf][crc32: uint32 LE]`, `MAX_WAL_RECORD_SIZE = 1 MiB` enforced during recovery scanning (reject before allocating, not after).
- Recovery scan algorithm: sequential scan, stop and truncate at first invalid/truncated/oversized record.
- Full recovery sequence per `docs/architecture.md`: replay WAL → reconstruct log[] and the `LogStore` byte-offset map → start as Follower with commitIndex=0 → learn `leaderCommit` from the current leader → advance commitIndex via the normal follower path → **only then** does the applier begin applying (I-005 — applying the raw replayed log before commitIndex is established from the leader would apply uncommitted entries; this ordering is a hard recovery requirement, not an implementation detail).
- **`TermVoteStore` full atomicity/corruption contract** per `docs/architecture.md` (I-020, I-024): `currentTerm`+`votedFor`+`bootID` persisted together as one checksummed record via the temp-file+fsync+rename+directory-fsync mechanism (never an in-place overwrite); a missing/truncated/checksum-failing record on startup causes the node to **refuse to start** rather than guess a value; first boot synthesizes and durably persists `{term=0, votedFor="", bootID=1}` immediately rather than merely returning it in memory.
- **Storage-failure fail-closed behavior** per `docs/architecture.md` (I-018), now fully tested: any `LogStore.Append`/`TruncateFrom`/`TermVoteStore.Save` failure must result in no ACK, no vote grant, no replication-state advance, and the node transitioning to a stopped/failed state — never continued operation on state that may not match durable storage.
- Additional required crash-point tests beyond Phase 1's basic term/vote survival:
  1. **Commit-advanced-then-crash-before-apply:** `commitIndex` advances on a node, process crashes before the applier runs for that advance. Verify recovery resets `commitIndex` to 0 and correctly re-establishes it from `leaderCommit` post-recovery.
  2. **Term/vote fsync boundary, both variants:** crash before the fsync for a `RequestVote` response completes (vote must not be considered granted on restart), and crash after fsync but before the response is sent (vote **must** be preserved on restart).
  3. **Corrupt/truncated term-vote record on startup (I-020):** deliberately corrupt the on-disk `TermVoteStore` record, attempt to start the node, assert it refuses to start cleanly (a clear startup error) rather than starting with a guessed or zero-valued term/vote. **Semantic-validity variant (new, required):** construct a record that passes its checksum but fails semantic validation (`docs/architecture.md`'s `TermVoteStore` section — e.g. `bootID=0`, or `votedFor` set to a string that matches no configured `NodeID`), assert it's treated identically to a checksum failure (fatal, refuses to start).
  4. **Storage-failure fail-closed (I-018):** inject a failure into `LogStore.Append`/`TruncateFrom`/`TermVoteStore.Save` (e.g. a fault-injecting wrapper that returns an error), assert no ACK is sent, no vote is granted, no replication state advances, and the node stops rather than continuing with potentially-inconsistent in-memory state.
  5. **`TermVoteStore` replacement atomicity (I-024):** force a crash at each stage of the temp-write+fsync+rename+directory-fsync sequence, assert recovery always yields either the complete old record or the complete new one, never a torn hybrid. **Orphan-temp-file variants (new, required):** (a) leave a valid `termvote.tmp` alongside a valid `termvote` — assert `Load()` uses only `termvote`, ignoring `termvote.tmp` entirely; (b) remove `termvote` but leave `termvote.tmp` present — assert startup fails (treated as corrupt) rather than promoting the orphaned temp file.
  6. **First-boot persistence (I-020):** on a node directory with no existing `TermVoteStore` record, start the node, assert `{term=0, votedFor="", bootID=1}` was durably persisted (not just held in memory). **Verified correctly, per `docs/invariants.md`'s I-020 (the earlier version of this test description was itself wrong and has been corrected):** stop the node and perform an actual **second** `Node.Start()`/`Load()` against the same on-disk directory — assert it observes `bootID=2`, not `1`. Observing `2` (not `1`) on this second, genuine boot is exactly what proves the first boot's `bootID=1` was truly durable rather than being silently regenerated as `1` again. A third boot must then observe `bootID=3`.

## Non-goals
No snapshotting (documented in `docs/architecture.md`, not implemented). No change to Phase 3's commit/apply logic itself.

## Files allowed to change (expected — additional files require Pass-1 approval)
`internal/storage/wal.go`, `internal/storage/recovery.go`, `internal/storage/term_vote_store.go` (extend Phase 1's version with corruption handling), `internal/storage/log_store.go` (extend Phase 2's version — file-backed with full recovery-scan integration), `internal/raft/node.go` (startup sequence — `Recover()` before `Start()`).

## Interfaces
`WAL.Append(record) error`, `WAL.ReplayAll() ([]WALRecord, error)`, `Node.Recover() error`.

## State changes
None to `NodeState` shape — this phase hardens durability of what already exists.

## Invariants affected
I-005 (applier must not start before commitIndex is established from the leader — the recovery-ordering requirement this phase makes explicit), I-011 (committed entries never truncated, even under a torn-write scenario), I-012 (durable term+vote, now with full crash-point test coverage), I-018 (storage-failure fail-closed, now fully tested), I-020 (corrupt term/vote metadata fails startup, first-boot immediate persistence, now fully tested), I-024 (`TermVoteStore` replacement atomicity via temp+fsync+rename+directory-fsync, now fully tested).

## Tests required
Full WAL test list per `docs/testing.md`: empty WAL, one record, many records, truncated final record (expected torn tail — `docs/architecture.md`'s WAL record format section), **mid-log corruption on a non-final record (distinct case, required — assert it's discarded identically to a torn tail, not treated as unreachable or skipped-past)**, corrupt checksum, corrupt length prefix (including oversized/garbage length triggering the `MAX_WAL_RECORD_SIZE` guard), crash between append and fsync, crash immediately after a `TruncateFrom` call — **for the last of these, assert the general safety properties from `docs/architecture.md`'s "Durable Conflict Truncation" section (WAL structurally valid, recovered log is a valid prefix, no committed entry lost, node can reconcile via normal Raft replication), not one specific byte-exact pre/post-truncation outcome, since either can legitimately result depending on filesystem durability timing.** The four crash-point tests above. Plus the full sequence: write data → `kill -9` (real crash) → restart → verify recovered state matches pre-crash state → verify node rejoins and converges. Full `TermVoteStore` test list from `docs/testing.md`.

## Failure cases to handle
fsync-ordering bugs. Partial/corrupt final WAL record, including one with a corrupted length field large enough to otherwise trigger a huge allocation (must be rejected via the size guard before allocating, per `docs/architecture.md`). Assuming `commitIndex`/`lastApplied` can be recomputed from log length alone rather than from the leader's authoritative `leaderCommit`.

## Acceptance criteria
**Data acknowledged as committed after a successful fsync survives a hard `kill -9`, subject to the durability guarantees of the underlying filesystem/storage stack** (this exact qualified phrasing — not an unqualified "survives kill -9" claim, per `docs/failure-model.md`). A node behind before crashing catches up correctly via normal AppendEntries after rejoining. All four crash-point tests (commit-before-apply, term/vote fsync boundary, corrupt-term-vote-fails-startup, storage-failure-fail-closed) pass.

## Interview concepts
Why term/vote must be durable before responding, and why the "crash after fsync, before response" variant is the one that actually has to work (the other variant is really about proving you *didn't* accidentally grant/persist too early). Why `commitIndex` is never recovered from the WAL directly — this phase's commit-before-apply crash test is the concrete demonstration of that design decision, not just an assertion about it. O(n) recovery-time limitation and how snapshotting would fix it.

## Exit criteria
- [ ] kill -9 + restart test passes repeatedly
- [ ] all 9 WAL test cases pass, using the general-safety-properties framing for the crash-after-truncate case, including the mid-log-corruption case treated identically to a torn tail
- [ ] all 4 `TermVoteStore` basic test cases pass, including the corrupt-record-fails-startup case (I-020)
- [ ] `TermVoteStore` replacement-atomicity test passes across all crash points in the temp+fsync+rename+directory-fsync sequence (I-024)
- [ ] first-boot immediate-persistence test passes (I-020)
- [ ] storage-failure fail-closed test passes (I-018)
- [ ] recovery-ordering test confirms the applier never runs before commitIndex is established from the leader (I-005)
- [ ] fsync-before-response ordering verified in code review for all four term-change touchpoints (`docs/invariants.md` I-007)
- [ ] `go test -race ./...` clean
