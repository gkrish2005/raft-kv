# ADR 008 — AI Is Read-Only

**Context:** An AI layer reasoning over cluster telemetry could, in principle, be given actuation capability (e.g., auto-restart a node it diagnoses as unhealthy).

**Decision:** The AI layer is strictly read-only, always, with no code path capable of mutating Raft state, WAL/log contents, KV state, or triggering elections/restarts/kills — enforced both structurally (`internal/ai` has no import path to `internal/raft`/`storage`/`cluster`, depends only on `internal/observability`'s typed surface, I-015) and behaviorally (fault-injection test: terminate/block the **in-process** AI worker mid-chaos-scenario — see `docs/ai-design.md`'s "Deployment model" section for why this is in-process rather than a separate OS process — assert zero effect on Raft).

**Alternatives considered:** AI-driven auto-remediation (rejected outright — this is explicitly a non-goal; an AI hypothesis about cluster health should never be trusted enough to act on autonomously in a consensus system, and building this in would undermine the entire "Raft remains authoritative" story this project is built to demonstrate).

**Trade-offs:** none accepted here — this boundary is treated as non-negotiable, not a cost/benefit tradeoff.

**Consequences:** the answer to "what happens if the AI is wrong" is genuinely reassuring: worst case is a misleading operator suggestion, cluster correctness is completely unaffected.
