# Phase 5 — Client Semantics + Replicated Dedup

## Goal
Make the client-facing surface robust: leader redirect, retry, and replicated idempotent deduplication per `docs/client-semantics.md` — closing the leader-failover duplicate gap a local cache can't handle.

## Scope
- `NOT_LEADER`/`NO_LEADER`/`TIMEOUT`/`INVALID_REQUEST`/`REQUEST_ID_REUSED`/`OVERLOADED` result semantics exactly per `docs/client-semantics.md`'s table.
- `RequestTable` folded into `StateMachineState` (KV + RequestTable), keyed by `RequestID`, storing `PayloadHash` (SHA-256 of the **canonical command encoding** — `{OperationType, Key, Value}` deterministically serialized, **not** `cmd.Payload` taken as opaque bytes, and **not** the full payload — see `docs/client-semantics.md`'s "Canonical command hashing" section for why raw-payload hashing is ambiguous), applied atomically in the same apply-loop step as the KV write.
- **Application-error handling in the apply loop, per `docs/client-semantics.md`:** a `REQUEST_ID_REUSED` detection at apply time is an application-level error, not a Raft-level rollback — `lastApplied` still advances, `commitIndex` is unaffected, exactly per Phase 3's general "application errors are still applied" rule, now exercised for real with this specific error case.
- `ClusterStatus` RPC + CLI command.
- Resource bounds: max key/value/command size, max AppendEntries batch, backpressure/`OVERLOADED` per `docs/failure-model.md`.

## Non-goals
No bounded retention/TTL for `RequestTable` — MVP retains indefinitely (explicit, stated tradeoff). No `(ClientID, SequenceNumber)` request identity model (`docs/adr/009-request-identity-model.md`).

## Files allowed to change (expected — additional files require Pass-1 approval)
`internal/client/{api,router}.go`, `internal/storage/kv_statemachine.go` (extend `StateMachineState` with `RequestTable`), `internal/raft/apply.go` (dedup check in the apply loop, using `PayloadHash`).

## Interfaces
`StateMachineState{KV, RequestTable}`, `AppliedRequest{RequestID, PayloadHash []byte, Result}` per `docs/client-semantics.md` — **note the field is `PayloadHash`, not `Payload`; do not store the full payload.**

## State changes
State machine's applied state now includes `RequestTable` — replicated, not a side cache.

## Invariants affected
I-017 (duplicate RequestID + different payload-hash → reject, never silently resolve).

## Tests required
Rolling-leader-kill write-survival test: a scripted client issuing writes continuously survives a rolling leader kill without losing or duplicating any write. Duplicate-`RequestID`-same-payload test (dedup hit, verified via matching canonical-command hash). Duplicate-`RequestID`-different-payload test (`REQUEST_ID_REUSED`, verified via mismatched canonical-command hash — and assert `lastApplied` still advances per the application-error rule). **Canonical-encoding disambiguation test (new, required, I-017):** construct two commands with different `OperationType`/`Key`/`Value` that would collide under a naive raw-payload-byte hash (e.g. contrived overlapping serializations), assert their canonical-command hashes differ. **Canonical-encoding determinism test (new, required, I-017, `docs/client-semantics.md`'s exact frozen byte format):** encode the same semantic command twice (independently), assert byte-identical `CanonicalEncode` output and matching `SHA-256` hashes both times — this is what pins the encoding down as a testable contract rather than a description with multiple valid implementations. **The specific failover scenario this phase exists to fix:** leader commits an entry, crashes before responding, client retries against the new leader using the SAME `RequestID` — assert exactly one applied write.

## Failure cases to handle
Treating `TIMEOUT` as failure anywhere in the client library (must trigger a same-RequestID retry). Storing the full payload instead of its hash in `RequestTable` (defeats the memory-growth mitigation this design specifically exists to provide). Rolling back `commitIndex`/blocking `lastApplied` on a `REQUEST_ID_REUSED` detection (violates the application-error rule).

## Acceptance criteria
Rolling-leader-kill test passes with zero lost or duplicated writes. Consistency guarantees documented using `docs/client-semantics.md`'s precise operational wording ("at most once state-machine effect per RequestID..."), not a looser "effectively-once with caveats" framing.

## Interview concepts
Why a local per-node dedup cache is insufficient (the leader-failover-without-restart gap) and why folding dedup into the replicated state machine fixes it. Why storing a hash instead of the full payload is the right call — same dedup guarantee, meaningfully less memory pressure. Why a committed entry that produces an application-level error still consumes `lastApplied` — Raft-level bookkeeping and application-level results are separate concerns.

## Exit criteria
- [ ] rolling-leader-kill write-survival test passes
- [ ] same-payload dedup test passes (hash comparison)
- [ ] different-payload rejection test passes (hash comparison), with `lastApplied` still advancing
- [ ] the specific commit-before-ack-crash scenario test passes
- [ ] `go test -race ./...` clean
