# ADR 006 — Deterministic Testing Architecture

**Context:** Randomized election timeouts are correct in production but make naive tests flaky.

**Decision:** Injectable `Clock`/`Timer` and `Transport` interfaces (`docs/architecture.md`); a 5-level test hierarchy (`docs/testing.md`) from pure in-memory deterministic tests (Levels 1–2) up through real OS-level chaos (Level 5).

**Alternatives considered:** Real-timer tests with generous margins and re-run-on-flake CI policy (rejected — treats a real signal, timing bugs, as noise to suppress rather than something to catch).

**Trade-offs:** more upfront interface design work before any Raft logic is written.

**Consequences:** enables 100+-repeated-run election tests that complete in milliseconds and are genuinely deterministic, not "usually passes." Also enables the optional deterministic single-threaded simulator for reproducing exact scenarios like Figure 8 instantly.
