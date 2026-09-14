# RaftKV

A persistent, Raft-consensus-based distributed key-value store in Go — deterministic fault-injection testing, WAL-based crash recovery, replicated idempotent writes, quorum-confirmed linearizable reads, and an evidence-grounded incident-diagnosis layer structurally isolated from consensus.

**Start here:**
- `docs/PRD.md` — product definition, architecture pointers, phase roadmap (authoritative)
- `CLAUDE.md` — binding development rules for any implementation session, human or AI
- `docs/phases/phase-00.md` — the first thing to actually implement

**Status:** documentation complete and internally consistent as of this commit — every file referenced by `CLAUDE.md`, `docs/PRD.md`, and each phase doc actually exists under `docs/`, including `docs/benchmarks.md` and `docs/demos.md` (both explicit placeholders, populated in Phase 10) and `docs/interview-prep.md`. No implementation exists yet. Do not begin Phase 0 until `CLAUDE.md`'s source-of-truth hierarchy and its referenced docs have been read once, end to end, by whoever — human or AI — is about to start. If you find a place where two documents disagree, that's a real bug in the documentation, not a judgment call to resolve silently — see `CLAUDE.md`'s ambiguity rule.
