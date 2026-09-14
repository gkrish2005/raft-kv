# RaftKV — Product Requirements Document

**This file supersedes the original high-level project description.** It is the authoritative top-level spec. Detailed content that used to live only in prose form now lives in the linked documents below, each of which is real and present in this repo — `CLAUDE.md`'s references to them are no longer aspirational.

```
docs/
├── PRD.md                 (this file)
├── architecture.md         system diagrams, write path, recovery path, WAL format, concurrency model, snapshot design
├── invariants.md           canonical invariant IDs (I-001..I-024), referenced by every phase doc and CLAUDE.md
├── failure-model.md        network/node failure assumptions, safety vs liveness, failure matrix, non-guarantees
├── client-semantics.md     result semantics, replicated idempotency/dedup design, read protocol
├── ai-design.md            evidence-grounded incident diagnosis: architecture, claims schema, evaluation metrics
├── testing.md               5-level deterministic test hierarchy, testing pyramid, required test lists
├── benchmarks.md            results (populated in Phase 10 — see docs/phases/phase-10.md; empty until then)
├── demos.md                  10 demo scenarios (to be finalized in Phase 10 from the working system)
├── phases/                   phase-00.md .. phase-10.md — one self-contained implementation spec per phase
└── adr/                      001..009 — architecture decision records, doubling as interview material
```

## 1. Product definition

**Name:** RaftKV. **One-line description:** a persistent, Raft-consensus-based distributed key-value store in Go, with WAL persistence, replicated idempotent writes, deterministic fault-injection testing, and an evidence-grounded incident-diagnosis layer structurally isolated from consensus.

**Problem statement:** most portfolio "distributed systems" projects are a REST API in front of a database, or a Raft implementation that's never been made to survive a killed leader, a partition, or a crash-restart. This project closes that gap: **critical safety, recovery, and consistency claims (the invariants in `docs/invariants.md`) are backed by targeted tests and invariant checks designed to fail when those properties are violated** — a more defensible, honest framing than claiming every conceivable claim the project makes is backed by a test that would fail if it were false, which is stronger than any finite test suite can actually guarantee.

**Target users:** primarily an interview artifact (SDE / backend / distributed-systems roles). Secondarily, a readable, test-covered Raft reference implementation.

**Core use cases:** normal writes/reads against a 3–5 node cluster; leader crash → automatic election; network partition → provable minority-cannot-commit behavior; node crash → WAL recovery and rejoin; operator (or the AI layer) diagnoses cluster health from structured telemetry alone.

**Non-goals:** not a production database (no sharding, no SQL, no secondary indexes); not competing with etcd/TiKV on throughput; the AI layer is not a chatbot over logs — see `docs/ai-design.md`.

**Success criteria:**
- Every invariant in `docs/invariants.md` holds under the full chaos suite (`docs/phases/phase-06.md`), including a ≥30-minute randomized fuzzer run with continuous writes.
- All 10 demos (`docs/demos.md`) runnable live, cold, with an accurate explanation of what's happening at the protocol level.
- Evidence validity for the AI layer is 100% across the full evaluation set (`docs/ai-design.md`) — a hard requirement, not a soft target.
- Every question in the interview question bank is answerable from lived implementation experience, not memorized definitions.

**Why this project is differentiated:** getting past leader election into WAL persistence + crash recovery + chaos-proven fault tolerance + a correctly-specified linearizable read path is where most student Raft implementations stop short. The read-barrier requirement (`docs/client-semantics.md`) and the replicated-dedup design (also `docs/client-semantics.md`) are both places where the "obvious" implementation is subtly wrong — this project's documentation history (see `docs/adr/`) records exactly why the correct versions look the way they do, which is itself strong interview material.

**Why the AI component is meaningful, not cosmetic:** it never touches consensus, never sees raw logs, and reasons only over a typed, versioned telemetry schema that a deterministic rule engine partially interprets first. See `docs/ai-design.md` for the full architecture and evaluation design, and `docs/adr/007-rules-llm-hybrid.md` / `docs/adr/008-ai-read-only.md` for why it's built this way.

## 2. Architecture, invariants, failure model, client semantics, AI design, testing strategy

Fully specified in the linked documents above — this PRD does not duplicate that content, to avoid the exact source-of-truth drift that caused earlier drafts of this project's documentation to fall out of sync with `CLAUDE.md`. If you're implementing a phase, read that phase's `docs/phases/phase-NN.md` plus the specific linked docs it references — you should not need the entire PRD in context for any single phase.

## 3. Phase roadmap (authoritative — supersedes any prior day-by-day schedule)

| Phase | Name | Doc |
|---|---|---|
| 0 | Foundation + Single-Node KV Store | `docs/phases/phase-00.md` |
| 1 | Leader Election + Heartbeats | `docs/phases/phase-01.md` |
| 2 | Replicated Log + Minimal Durable Log | `docs/phases/phase-02.md` |
| 3 | Commit, Apply, and Reads | `docs/phases/phase-03.md` |
| 4 | Crash Recovery + Durable Metadata + WAL Hardening | `docs/phases/phase-04.md` |
| 5 | Client Semantics + Replicated Dedup | `docs/phases/phase-05.md` |
| 6 | Chaos Testing Framework | `docs/phases/phase-06.md` |
| 7 | Observability | `docs/phases/phase-07.md` |
| 8 | Evidence-Grounded Incident Diagnosis (Rules + LLM) | `docs/phases/phase-08.md` |
| 9 | AI Evaluation | `docs/phases/phase-09.md` |
| 10 | Benchmarking, Hardening & Final Demo | `docs/phases/phase-10.md` |

**The phase sequence and each phase doc's Scope/Non-goals/Exit-criteria are authoritative.** A rough day-by-day calendar schedule is useful for personal planning but must never be used to justify compressing a phase's scope (especially Phases 2–4, where the project's real correctness content lives) to hit a date. If you're behind, the time comes from later polish (Phase 10's benchmarking/rehearsal), never from skipping a Phase 2–5 test.

## 4. How to work through a phase with Claude Code
See `CLAUDE.md`'s five-pass workflow (Design → Implement → Audit → Fix → Verify). Each session should be handed: the current `docs/phases/phase-NN.md`, the specific `docs/*.md` files it references, a summary of current repo state, and nothing more — not this entire PRD, not prior phases' full documents unless directly relevant.

## 5. Do-not-overbuild list
Do not add (without an explicit, developer-approved phase reassignment): sharding, multi-Raft groups, cluster membership changes, transactions, SQL support, distributed locks, a service mesh, Kubernetes orchestration beyond local Docker Compose for demos, a fancy web frontend, a vector database, RAG, agentic AI, or AI-driven autonomous remediation. Every one of these is individually impressive-sounding, which is exactly why each is flagged explicitly in `CLAUDE.md` rule 6 — they're the most likely things to derail the project's actual differentiator (a correctness-first, chaos-tested consensus implementation) into a shallower but broader one.

## 6. Interview preparation
See `docs/interview-prep.md` — question bank organized by difficulty (beginner/intermediate/advanced/read-consistency/AI-specific/idempotency), cross-referenced against `docs/invariants.md`'s ID system so each concept maps to a specific, testable claim rather than a vague description.

## 7. Document consistency
Per `CLAUDE.md`'s source-of-truth hierarchy: if any two documents in this repo disagree, that is a documentation bug — report it rather than silently picking one interpretation. This PRD, and every doc it links to, is kept internally consistent as a standing requirement, re-verified whenever a correction is made to any one of them (see this project's own review history for examples of exactly this kind of drift being caught and fixed).
