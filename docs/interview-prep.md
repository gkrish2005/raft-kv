# Interview Preparation

Question bank, organized by difficulty, cross-referenced against `docs/invariants.md`'s IDs so each concept maps to a specific, testable claim rather than a vague description. Full detail carried over from the project's earlier consolidated PRD draft; this file exists as its own document per `docs/PRD.md`'s self-containment principle rather than being referenced-but-missing.

## Beginner
What is Raft, and what problem does it solve? Why do we need a leader? What is a term? What is a quorum/majority (I-001, I-008)?

## Intermediate
What happens when the leader crashes? How does log replication work (I-003)? Why can't a minority independently commit (I-008 — note the precise wording, not the looser "minority can never advance commitIndex at all")? What happens during a network partition?

## Advanced
How does Raft preserve committed entries across leader changes (I-004)? How does log conflict resolution work, and what does `LogStore.TruncateFrom` actually guarantee about durability (`docs/architecture.md`)? What happens if a leader crashes after replicating to a majority but before responding to the client — walk through the `TIMEOUT` + same-`RequestID`-retry + replicated-dedup path end to end (`docs/client-semantics.md`)? How does persistence interact with consensus correctness across all four term-change touchpoints (I-007)? What race conditions can occur, and how does the concurrency model (`docs/architecture.md`) prevent them? Why isn't "at most one AppendEntries RPC in flight per follower" by itself sufficient to make every same-term response safe to apply (I-021) — what specific interleaving (a timed-out request followed by a new one, with the old one's response arriving late) does the per-follower attempt counter close that the in-flight rule alone doesn't?

## Read consistency (a cluster of its own — this project has unusually deep material here)
Why does confirming leadership alone not guarantee a fresh read? What is the read barrier, precisely? Why is a third step (term/role revalidation immediately before returning) required on top of the barrier wait — what specific failure does it prevent (I-016)? **Why does Leader Completeness (I-004) alone not make a freshly elected leader's reads safe** — walk through the scenario where a new leader's volatile `commitIndex` hasn't yet caught up to what a previous leader actually committed, even though the entry is sitting right there in the new leader's log — and why committing a no-op in the new leader's own current term (I-023) closes that gap by giving the Current-Term Commit Rule (I-006) something current-term to anchor on. How does this project's no-op approach relate to, and differ from, the formal ReadIndex optimization (`docs/adr/004-read-consistency.md`)? Why must the term/role revalidation and the actual KV read be performed under one held lock (the StateMachine mutex, `docs/architecture.md`) rather than as a check followed by a separate read — what specific interleaving would otherwise break the claimed linearization point?

## AI-specific
Why use an LLM here at all, and why rules-first? Why isn't "the EventID exists" sufficient grounding — what's the specific hallucination class the validator-derives-fields design closes? What's the difference between accepted-output evidence validity (always 100%, a property of the validator) and the LLM evidence rejection rate (variable, tells you how often the model actually tried to hallucinate)? What happens if the AI is wrong? Can the AI change cluster state, and how is that enforced at both the import-graph and dependency-direction level (`docs/architecture.md`)?

## Idempotency
Why is a local per-node dedup cache insufficient? Walk through the specific failover scenario it can't handle. Why store a payload hash instead of the full payload? What's the precise, operational statement of the guarantee this system provides (`docs/client-semantics.md`) — and why is that statement stronger and more defensible than saying "effectively-once"?
