# ADR 002 — Implement Raft From Scratch (not hashicorp/raft or etcd/raft)

**Context:** Production-grade Raft libraries exist and are battle-tested.

**Decision:** Implement Raft from scratch.

**Alternatives considered:** `hashicorp/raft`, `etcd/raft` — both would ship faster and be more production-ready.

**Why:** The project's stated goal is implementation-level understanding of consensus for interview purposes, not the fastest path to a working KV store. A library would deny exactly the understanding being built. This is stated outright, not hidden — see PRD §9.3 framing.

**Trade-offs:** Slower to build, more likely to contain subtle bugs than a battle-tested library — mitigated by the deterministic testing hierarchy (`docs/testing.md`) and the chaos/fuzz suite (Phase 6).

**Consequences:** Every invariant in `docs/invariants.md` had to be independently reasoned about and tested, which is exactly the point.
