# Failure Model

## Network assumptions
The implementation assumes an **asynchronous, unreliable network**: messages may be delayed, dropped, duplicated, or reordered, and may become unavailable for extended periods. RPCs carry local timeouts **purely for liveness/resource management**. **Correctness must never depend on any assumed bound on message latency.**

## Node failure assumptions
Nodes may crash (lose all volatile state), restart (recover persistent state per `docs/architecture.md`), become partitioned from some or all peers, or become slow. These are **crash-stop / omission** failures.

## Explicitly out of scope
- **Byzantine failures.** Nodes never forge, corrupt, or maliciously contradict protocol messages.
- **Disk corruption beyond what the WAL's CRC32 can detect** (`docs/architecture.md`'s WAL record format).
- **Clock synchronization assumptions.** No mechanism depends on cross-node clock sync; each node's `Clock` only needs to be locally monotonic. `ClusterEvent` timestamps are explicitly node-local and never used for cross-node causal ordering (`docs/architecture.md`).

## Safety vs. liveness

**Safety — must never happen, checked as hard assertions in every chaos/fuzz run:**
- Two leaders in the same term (I-001).
- A committed entry gets overwritten (I-002, I-011).
- Two nodes apply a different entry at the same index (I-005).
- `currentTerm` decreases on any node (I-007).
- A minority partition independently establishes new commitment (I-008 — note the precise wording: passively adopting a legitimate `leaderCommit` is not a violation, see `docs/invariants.md`).
- A stale vote is granted (I-001, I-004).
- A follower's `matchIndex` advances before its own WAL fsync completes (I-013).
- A linearizable read returns a value after leadership/term changed mid-wait (I-016).
- The AI layer mutates any cluster state (I-015).

**Liveness — should eventually happen under a partial-synchrony assumption, checked as bounded-time expectations with generous timeouts, never as instant assertions or hard correctness thresholds:**
- A new leader is eventually elected after the current one fails. **Not "within ~1 election timeout" as a correctness promise** — split votes, delayed messages, and candidate collisions can require multiple election rounds. Election convergence time is *measured* (p50/p95/p99, Phase 10) and reported, not baked into Raft as a pass/fail threshold.
- Committed entries are eventually applied on every reachable node.
- A lagging follower eventually catches up once faults clear.
- The majority side eventually continues serving writes during a partition.
- The cluster eventually reconverges after a partition heals — see `docs/testing.md` for the precise convergence-check definition used to decide "reconverged."

**Why the distinction matters:** Raft can only guarantee liveness under partial synchrony, never under arbitrarily adversarial timing — it guarantees safety unconditionally. Tests should reflect this precisely.

## Linearizability scope (CAP stance)
The system provides linearizable writes and quorum-confirmed linearizable reads (`docs/client-semantics.md`) **contingent on a leader being able to communicate with a quorum** — a more precise statement than "a majority of nodes remaining reachable" in the abstract, since an isolated former leader can be technically "part of a cluster where a majority exists" while itself being unable to serve any linearizable operation. Without a leader that can reach a quorum, writes and quorum-confirmed reads block or fail rather than return stale/incorrect data. Deliberate **consistency-over-availability**.

## What this system does *not* guarantee
- Exactly-once client delivery — see the operational "at most once state-machine effect per RequestID" statement in `docs/client-semantics.md`.
- Cross-cluster/multi-shard transactions, sharding.
- Availability without a leader-reachable quorum.
- Byzantine fault tolerance.
- Durability stronger than the underlying disk/filesystem's own fsync guarantees — every "survives a crash" claim in this project (including Phase 4's acceptance criteria) is implicitly qualified by this.

## Resource bounds
- Max key size: 1 KB. Max value size: 256 KB. Max single `Command` size: ~257 KB.
- Max WAL record size: 1 MiB (`docs/architecture.md`) — comfortably above max Command size plus serialization overhead, and used defensively during WAL recovery to reject corrupt length fields before allocating memory.
- Max entries per AppendEntries batch: 1000, or a byte-size cap, whichever binds first.
- Requests exceeding size limits are rejected client-side with `INVALID_REQUEST` before becoming a log entry.
- The leader bounds outstanding writes/in-flight replication batch size; requests beyond the limit get a retryable `OVERLOADED` rather than unbounded queueing.

## Failure matrix

| Failure | Quorum reachable from a leader? | Writes | Quorum-confirmed reads | Expected outcome |
|---|---|---|---|---|
| Follower crash | Yes | Continue | Continue | Healthy, unaffected |
| Leader crash | Yes (new leader can reach quorum) | Brief interruption, then continue | Brief interruption, then continue | New leader eventually elected — convergence time measured, not promised as a fixed bound |
| Minority-side partition (1 of 3, 2 of 5) | Yes (majority side) | Continue on majority side | Continue on majority side | Minority side cannot independently commit (I-008); a minority node still correctly adopts `leaderCommit` once reconnected |
| Majority-side partition (2 of 3, 3 of 5) | No | Fail/block everywhere | Fail/block everywhere | CP behavior — no side has a leader-reachable quorum |
| AI/LLM service down | Yes (unrelated) | Continue, unaffected | Continue, unaffected | AI diagnosis unavailable only — fully async, never on the client path (`docs/architecture.md`) |
| WAL corruption on one node | Node-specific | Node recovers to last valid record and rejoins, or fails safely and stays out of quorum | Cluster continues if quorum otherwise holds | No silent data corruption ever accepted as valid |
