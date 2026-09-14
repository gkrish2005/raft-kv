# ADR 003 — WAL Design

**Context:** Need durable persistence for the Raft log and term/vote metadata, with defined crash-consistency behavior.

**Decision:** Length-prefixed, checksummed WAL records (`[length][payload][CRC32]`); sequential-scan recovery that stops and truncates at the first invalid/truncated record. `LogStore` interface introduced minimally in Phase 2, hardened in Phase 4 (see `docs/architecture.md` for why the split exists).

**Alternatives considered:** A more sophisticated segmented WAL with compaction (deferred — see snapshot design, also documented but not implemented); no length prefix (rejected — makes torn-write detection unreliable).

**Trade-offs:** fsync happens under the Raft mutex for ordering-reasoning simplicity (`docs/architecture.md`'s concurrency model), bounding throughput by disk latency — accepted, measured explicitly in Phase 10 benchmarks.

**Consequences:** Recovery is O(committed log length) without snapshotting — documented as a known limitation, not silently accepted.
