# ADR 001 — Go + gRPC

**Context:** Need a language/RPC stack for a from-scratch Raft implementation, interview-focused, single developer.

**Decision:** Go + gRPC + Protocol Buffers.

**Alternatives considered:** Rust (stronger compile-time guarantees, steeper velocity cost for this timeline); Java/Kotlin (verbose for this scope); plain JSON-over-HTTP instead of gRPC (loses free, typed RPC contracts for RequestVote/AppendEntries).

**Trade-offs:** Go's goroutines make the concurrency model (`docs/architecture.md`) natural to express but also easy to get subtly wrong without discipline — mitigated by the explicit single-mutex-per-node rule and CLAUDE.md's guardrails.

**Consequences:** gRPC gives typed RPC contracts "for free," which matters for a project whose core value is demonstrating protocol correctness, not inventing a wire format.
