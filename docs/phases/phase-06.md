# Phase 6 — Chaos Testing Framework

## Goal
Build fault-injection at Levels 3–5 (`docs/testing.md`) and demonstrate, through deterministic and chaos testing, that everything from Phases 1–5 survives real chaos, including combinations and randomization, with precisely-defined convergence criteria. **Deliberately not phrased as "prove"** — a finite, however extensive, test suite demonstrates the absence of the specific failures it was designed to inject and check for; it is not a formal correctness proof. `docs/PRD.md`'s success-criteria wording reflects this same distinction.

## Scope
Fault-injecting transport wrapper (drop probability, added latency, "block traffic between node sets" for partitions) at the gRPC dial/interceptor level. Process-level kill/restart harness. Named scenarios: leader-crash, follower-crash, minority-partition, majority-partition-heals, repeated-elections, slow-follower — each following the inject→heal→assert-convergence scenario shape from `docs/testing.md`, never an indefinitely-faulted scenario. A randomized fuzzer combining faults over a sustained duration, stopping and healing all faults before the final convergence check, per `docs/testing.md`'s precise convergence definition (identical commitIndex, applied log prefix, KV state, and RequestTable across every reachable/healed node).

## Non-goals
No AI involvement yet (Phase 8). No benchmarking (Phase 10).

## Files allowed to change (expected — additional files require Pass-1 approval; this phase legitimately may need to touch `internal/cluster/`, `tests/`, or the `Makefile` for process-harness/CLI wiring — get Pass-1 sign-off rather than contorting the implementation to avoid it)
`internal/chaos/{faults,scenarios}.go`, `cmd/raftkv-chaos/`.

## Interfaces
`ChaosScenario{Name, Steps []ChaosStep}`, `ChaosStep{Type, TargetNodes, Duration, Params}`.

## State changes
None to Raft/storage internals.

## Invariants affected
All invariants applicable to a given scenario are checked as hard safety assertions during every scenario per `docs/failure-model.md`'s safety-vs-liveness split; liveness properties (e.g. election convergence) are checked as bounded-time, measured expectations, not hard pass/fail thresholds. **`I-001` through `I-024` (`docs/invariants.md`) are collectively covered by the combined deterministic (Level 1-3), storage, client, chaos, and AI test suites — not every invariant needs to be independently re-asserted inside every individual chaos scenario; each scenario asserts the invariants it's actually positioned to exercise (e.g. a partition scenario is where I-008 gets its real workout, not a WAL-corruption scenario).**

## Tests required
Each named scenario automated, run ≥10x for flakiness detection, using the inject→heal→assert-convergence shape. Fuzzer mode: ≥30 min continuous run with writes, all faults explicitly healed before the final convergence check, verified against `docs/testing.md`'s precise convergence definition.

## Failure cases to handle
Chaos framework itself introducing nondeterminism that obscures real bugs (mitigate with seeded randomness). "Partitions" that don't actually block bidirectional traffic. A scenario that never heals its injected faults before checking convergence (invalid test by construction, per `docs/testing.md`).

## Acceptance criteria
All named scenarios pass reliably across repeated runs. A 30-minute seeded chaos soak completes with zero **observed** invariant violations (per `docs/testing.md`'s precise reporting-wording guidance — this is not claimed as a formal correctness proof) and full state convergence (per the precise definition) across every healed node.

## Interview concepts
Difference between "the code runs" and "the safety invariants provably hold under adversarial conditions." Why a convergence check needs a precise definition (commitIndex + applied prefix + KV + RequestTable, not just "looks the same") to actually mean something.

## Exit criteria
- [ ] ≥6 named scenarios automated and passing reliably, using inject→heal→assert-convergence
- [ ] fuzzer mode implemented with seedable randomness
- [ ] 30-minute seeded chaos soak completed, zero observed violations, output saved as evidence (including the random seed used, for reproducibility), convergence checked per the precise definition
