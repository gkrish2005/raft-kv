# Phase 10 — Benchmarking, Hardening & Final Demo

## Goal
Put real, measured (never invented) numbers on the system, run the full stress/race/soak gauntlet, and prepare the final demo + interview materials.

## Scope
Latency/throughput benchmarks for SET, quorum-confirmed GET (reported separately and compared against SET once measured — see `docs/adr/004-read-consistency.md` for why they're expected to be in the same ballpark, and `docs/benchmarks.md` for why that's stated as a hypothesis to explain, not an assumed result), plus one explicitly-labeled benchmark-only "unsafe local read" comparison point (never exposed via the client API). Commit latency, election time (reported as measured p50/p95/p99, per `docs/failure-model.md`'s liveness-not-correctness-threshold framing — not validated against a fixed "~1 timeout" bound), replication latency, recovery time, performance-under-failure. `go test -race ./...` across the full suite. A long-running soak test with continuous writes and periodic chaos injection. ADR write-up completion. Rehearsed run-through of all 10 demo scenarios (`docs/demos.md`).

## Non-goals
No new features. This phase is measurement, hardening, and polish only.

## Files allowed to change (expected — additional files require Pass-1 approval; this phase legitimately may need to touch `Makefile`, `README.md`, or CI configuration for benchmark commands and final demo tooling — get Pass-1 sign-off rather than avoiding it)
`tests/bench/` (new), `docs/benchmarks.md` (results), `docs/adr/*.md` (finalize), `docs/demos.md` (finalize).

## Interfaces
None new.

## State changes
None.

## Invariants affected
All — this phase's soak/race/chaos sweep is a final re-verification pass across every invariant in `docs/invariants.md`.

## Tests required
Full suite (unit + integration + chaos + ai_eval) green with `-race`. Soak test (hours if feasible) checking correctness, goroutine leaks, and memory growth.

## Failure cases to handle
None new expected — budget real time for whatever the soak/race sweep turns up.

## Acceptance criteria
All benchmark categories in `docs/benchmarks.md` have real measured numbers with stated methodology (payload size, key distribution, read/write ratio, concurrency, warmup, duration, node count, storage medium, hardware — per `docs/benchmarks.md`'s methodology template), including election-convergence time reported as measured percentiles, not validated against a fixed correctness bound, and the GET-vs-SET cost comparison reported as an actual measured result with its gap (if any) explained, not asserted in advance as "comparable." Race detector clean across the full suite. Soak test completed and documented. All 10 demos runnable live, cold.

## Interview concepts
How to talk about your own benchmark numbers honestly. Why election convergence is reported as a measured distribution rather than asserted against a fixed threshold — ties directly into the safety-vs-liveness distinction from `docs/failure-model.md`.

## Exit criteria
- [ ] full suite green with `-race`
- [ ] soak test complete and documented
- [ ] benchmark table populated with real numbers + methodology, including the GET-vs-SET cost explanation and election-time percentiles
- [ ] all 10 demos rehearsed and reliable
- [ ] all ADRs finalized
