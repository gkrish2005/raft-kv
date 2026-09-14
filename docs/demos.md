# Demo Scenarios

**Status: final scenario wording to be locked in Phase 10 (`docs/phases/phase-10.md`)**, once the working system exists to rehearse each one against and confirm the described behavior actually matches what's implemented. Placeholder list below (from earlier project drafts) — treat every claim in it as provisional until Phase 10 confirms it against the real system, especially anything involving timing ("eventually," "within X") which must be checked against `docs/failure-model.md`'s liveness framing before being stated as a promise in a live demo.

1. Normal 3-node write.
2. Leader crash → automatic election, followed immediately by a `GET` proving the new leader's current-term no-op commit (I-023) makes the previous leader's committed write visible right away — not just "a new leader gets elected," but "reads against it are actually correct from the start."
3. Follower crash → cluster continues.
4. Network partition → minority cannot independently commit (I-008), but a minority node correctly adopts `leaderCommit` on reconnect.
5. Leader partition → majority elects new leader; old leader steps down and truncates conflicting uncommitted entries only once it regains contact (not instantaneously).
6. Node restart → WAL recovery.
7. Follower falls behind → log catches up.
8. Chaos fuzzer run → cluster recovers/converges (per `docs/testing.md`'s precise convergence definition).
9. AI detects a replication problem (using `--mode=recorded` fixtures or, for a live demo specifically, `--mode=live` per `docs/ai-design.md`).
10. AI diagnoses a network partition using cited, validator-derived telemetry evidence.

Each demo, once locked in Phase 10, gets: setup, exact commands, expected behavior, what the event stream should show, and what specifically proves correctness (referencing the relevant invariant ID from `docs/invariants.md` where applicable).
