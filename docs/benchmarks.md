# Benchmarks

**Status: not measured yet.** No benchmark numbers may be added here until Phase 10 (`docs/phases/phase-10.md`). This file exists now, as a placeholder with the required structure, specifically so `CLAUDE.md`'s "do not invent benchmark numbers" rule has a real target and so `README.md`'s claim that every referenced file exists remains true throughout the project, not just at the end.

## Planned categories (see `docs/phases/phase-10.md` and `docs/adr/004-read-consistency.md`)
- SET latency (p50/p99), throughput
- GET (quorum-confirmed) latency (p50/p99) — **measured and compared against SET once real numbers exist; the two are not assumed comparable in advance.** Both pay roughly one round-trip's worth of coordination (SET: replication + quorum + commit + apply; GET: quorum confirmation + read barrier + local read), which is *why* they're expected to be in the same ballpark, but the actual numbers may differ meaningfully depending on implementation details (e.g. batching, local-apply cost) — the report must explain any observed gap based on what the implementation actually does, not assert the hypothesis as if it were already a result.
- GET (unsafe local read, benchmark-only, never exposed via the API) — comparison point only
- Commit latency
- Election convergence time — reported as a measured distribution (p50/p95/p99), not validated against a fixed threshold (`docs/failure-model.md`)
- Replication latency
- Recovery time (and its O(committed log length) growth trend, motivating the documented-but-unimplemented snapshot design)
- Performance under failure (throughput/latency during an active chaos scenario vs. baseline)

## Methodology (to be filled in once measurements are taken)
Every category above must state, per measurement, not just once generically:
- **Payload:** key size, value size (or distribution, if varied)
- **Key distribution:** uniform random, hotspot, sequential, etc. — whichever was actually used
- **Read/write ratio:** for any mixed workload
- **Concurrency:** number of concurrent clients/goroutines issuing requests
- **Warmup:** duration/requests discarded before measurement starts
- **Duration:** how long the measurement window ran
- **Cluster size:** number of nodes
- **Storage medium:** disk type (SSD/NVMe/tmpfs/etc.) — fsync latency depends heavily on this
- **Hardware/environment:** CPU, available RAM, Go version, OS
- Failure scenarios used, for the performance-under-failure category specifically (which fault, injected how, healed when relative to the measurement window)
- **Soak test memory tracking:** The soak test's memory-growth check must track `RequestTable` size specifically, since it is the one structure in the system designed to grow unbounded by MVP decision (see `PROGRESS.md` and `docs/adr/009-request-identity-model.md`).

This is a template, not yet filled in — Phase 10 populates it with the values actually used for each measurement, per `docs/phases/phase-10.md`.
