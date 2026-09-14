# ADR 009 — Request Identity Model

**Context:** Idempotent writes need some notion of request identity for deduplication (`docs/client-semantics.md`).

**Decision:** UUID-based `RequestID`s, one random ID per logical write, no client-side session state required.

**Alternatives considered:** `(ClientID, SequenceNumber)` pairs — gives ordering/session semantics and lets the server bound the dedup table per-client (evict everything below the client's last-acknowledged sequence) rather than needing a global unbounded table.

**Why UUIDs for MVP:** simpler client library, no session establishment/renewal logic needed, sufficient to demonstrate the core replicated-dedup correctness story (`docs/client-semantics.md`).

**Trade-offs:** `RequestTable` grows unboundedly with UUIDs and no session structure to bound it — explicit, stated MVP limitation, naturally resolved by snapshotting (`docs/architecture.md`) or a future move to session-based identity, not implemented now.

**Consequences:** a legitimate, prepared answer to "how would you make this production-ready" that doesn't require rearchitecting the dedup mechanism itself, only the identity/bounding scheme around it.
