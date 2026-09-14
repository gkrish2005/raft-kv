# Testing Strategy

## Five-level deterministic hierarchy

| Level | What it is | What it catches | Rule |
|---|---|---|---|
| 1 | Unit tests, fake `Clock` + fake `Transport`, single Go process | Pure logic bugs | No `time.Sleep`, ever |
| 2 | Deterministic multi-node cluster via the **mandatory single-threaded simulator** for core election/commit safety tests | Cross-node sequencing bugs, without real timing/goroutine-scheduling nondeterminism | Drive time via `clock.Advance(...)`, messages via `cluster.Step()`/`DeliverPending(...)` — never sleep, never rely on real goroutine scheduling |
| 3 | Fault-injection transport (in-memory drop/delay/partition), same simulator underneath | Replication/recovery correctness under adversarial, seed-reproducible conditions | Same as Level 2 + seeded randomness |
| 4 | Real multi-process cluster, real gRPC, real clock, real goroutine scheduling | **Genuine concurrency bugs Level 1-3 cannot produce by construction: mutex races, goroutine deadlocks, concurrent-RPC-handler interleavings, real cancellation behavior.** See caveat below — this level is not optional just because Levels 1-3 exist. | Runs on PR merge / nightly |
| 5 | Docker/OS-level chaos | Final-mile realism, 30-min fuzzer soak, live demos | Manual/scheduled |

**Explicit caveat on the deterministic simulator (Levels 1-3):** it validates protocol state-machine behavior — the logical correctness of election, replication, and commit under controlled event ordering. **It does not replace real concurrent integration testing.** Passing every deterministic test does not by itself prove the absence of races, deadlocks, or concurrent-handler bugs — that's what Level 4 (real processes, real scheduling) plus `go test -race ./...` across all levels is for. Do not treat "all deterministic tests green" as "concurrency is proven."

## The deterministic single-threaded simulator — mandatory for Level 2/3 core safety tests

```
DeterministicCluster { Node 1, Node 2, Node 3 (fake Clock, fake Transport), FakeClock, FakeTransport, EventQueue }
```
```go
sim.Advance(150 * time.Millisecond)
sim.DeliverPending("RequestVote")
sim.Advance(10 * time.Millisecond)
sim.DeliverPending("RequestVoteResponse")
assert sim.LeaderOf(term=2) == "node-B"
```
**Model:** `event → node state transition → generated messages → explicit delivery`, single-threaded — no background goroutines run node logic during a test.

**Fake-clock tie-breaking:** when `Advance(d)` expires more than one timer, the simulator processes them in `(deadline, NodeID)` sorted order, deterministically.

**Fake timer `Stop()`/`Reset()` semantics — specified explicitly, this is a classic fake-timer bug source:**
```
Reset(newDuration) on a timer that has an outstanding (not-yet-fired) scheduled event:
    the ORIGINAL scheduled event is cancelled and must NOT fire, even if a later
    Advance() call would have crossed its original deadline
    a NEW event is scheduled at (current fake time + newDuration)

Stop() on an outstanding timer:
    cancels the scheduled event; it must not fire on any subsequent Advance()
```
**Required test:** schedule a timer, call `Reset()` before it would have expired, then `Advance()` past the *original* (pre-reset) deadline — assert the original timer event does **not** fire (only the reset one does, at its own later deadline). Without this test, a naive fake-timer implementation can produce phantom election-timeout events that don't correspond to any real timer state, causing spurious elections in otherwise-deterministic tests.

## Testing pyramid

| Layer | Covers | Maps to level(s) |
|---|---|---|
| Unit | Individual Raft behaviors in isolation | 1 |
| Integration | Multi-node cluster behavior | 2–3 |
| Failure | Crashes and restarts | 3–4 |
| Network | Partitions, delays, drops | 3 (injected) and 5 (real OS) |
| Property/Invariant | Section-level Raft safety properties, fuzzed | 1–3, seeded randomness |
| Race | Go concurrency correctness | All levels, `-race` on |
| Chaos | Randomized combined failures | 4–5 |
| AI Evaluation | Incident diagnosis correctness | Separate harness, `docs/ai-design.md` |

## Fixture determinism — timestamps
Per `docs/architecture.md`, `ClusterEvent`/`MetricSnapshot` timestamps in Level 1-3 fixtures are the **fake clock's logical time**, not real wall-clock time — this is required for byte-identical fixture regeneration from the same seed. Level 4-5 tests and live demos use real timestamps. Do not compare a Level 1-3 fixture's timestamps against a Level 4-5 run's timestamps as if they were the same kind of value.

## Safety vs. liveness in test design
See `docs/failure-model.md`. Safety properties are asserted as hard invariants. Liveness properties are checked with generous bounded timeouts and reported as measured distributions (e.g. election convergence p50/p95/p99, Phase 10), never as fixed pass/fail thresholds.

## Convergence/replication acceptance-test scenario (Phase 2, Phase 6)
```
1. inject fault (drop/delay/partition)
2. issue writes to the leader during the fault
3. HEAL the fault
4. assert eventual convergence within a generous bounded timeout (liveness check)
```
Never assert convergence while still faulted.

## Chaos fuzzer stopping condition and convergence definition (Phase 6)
Fixed duration (≥30 min, with a stated request rate and node count in the harness config for reproducibility) injecting randomized faults with continuous writes; **all fault injection stops and is healed** before the final convergence check. "Full state convergence": across every reachable/healed node — identical `commitIndex`, identical applied log prefix, identical KV state, identical `RequestTable` state.

**Reporting wording — be precise:** the result of a successful run is *"a 30-minute seeded chaos soak completed with zero observed invariant violations,"* never *"proved correctness for 30 minutes."* A finite, even seeded-random, test run demonstrates absence of observed violations under that specific run, not a formal correctness proof — state it that way everywhere this result is referenced (README, demos, interview answers).

## Required WAL test cases (Phase 4)
Empty WAL, one record, many records, truncated final record (expected torn tail), mid-log corruption on a non-final record (distinct case — must be discarded identically to a torn tail, per `docs/architecture.md`'s recovery-scan section, not skipped-past), corrupt checksum, corrupt/oversized length prefix, crash between append and fsync, crash immediately after `TruncateFrom` (assert the general safety properties from `docs/architecture.md`, not one specific byte-exact outcome).

## Required TermVoteStore test cases (Phase 1 basic, Phase 4 hardened)
Save/Load round-trip. Crash before fsync (vote must not be considered granted). Crash after fsync, before RPC response (vote must be preserved). Crash after self-vote's fsync, before the outgoing `RequestVote` is sent (self-vote must be preserved, no second vote grantable in that term — Phase 1). Corrupt/truncated record on startup (must fail startup cleanly, per I-020 — never guess). First-boot record is durably persisted immediately, not merely returned in memory (I-020). Replacement-atomicity: crash at each stage of temp-write/fsync/rename/directory-fsync, recovery always yields the complete old or complete new record, never a torn hybrid (I-024, Phase 4).

## Live debug buffer vs. scenario recorder (Phase 7)
These are two distinct `EventSink` consumers, not one: the **live debug buffer** is a bounded ring buffer, explicitly lossy under sustained load — acceptable for live operator debugging, never assumed complete. The **scenario recorder** captures the complete event stream for a chaos run's duration and is what Phase 6/9's "fully explainable from the event stream" and AI-evaluation fixtures actually depend on. A test asserting completeness must exercise the recorder, not the live buffer — asserting "the live ring buffer contains every event" under a high-volume scenario is not a valid test, since the live buffer is allowed to drop events by design.

## Required stale-response test cases (Phase 2)
Response with `Term < currentTerm` (ignored). Response with `Term > currentTerm` (step down, no replication-state mutation). Same-term response that is stale relative to the current per-follower replication attempt — i.e. a request timed out, a new request to the same follower is sent, then the old request's same-term response arrives (must be ignored via the attempt-ID check, not just the term check — I-021). Same-term response arriving after this node has already stepped down to Follower (must be ignored via the `role == Leader` check, independent of the term check). **The RequestVote analogue (Phase 1, `docs/architecture.md`'s `electionTerm` mechanism):** a vote response arriving after `role != Candidate`, and a vote response belonging to an abandoned earlier candidacy (checked via `electionTerm`, not a bare term comparison) — both ignored. **Higher-term ordering test (P0, required):** a same-response higher-term check (`resp.Term > currentTerm`) must be evaluated and acted on (persist + step down + abandon election) BEFORE the `role`/`electionTerm` filters are applied — a response carrying a higher term must never be silently dropped by those filters.

## Chaos scenarios (Phase 6, automated, ≥10 repeated runs each)
Leader crash, follower crash, minority partition, majority-partition-heals, repeated elections, slow follower, plus the randomized fuzzer.

## `go test -race ./...`
Must be green across the entire suite after every phase.
