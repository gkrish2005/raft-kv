# Client Semantics

## Client API
- `Set(key, value, request_id) -> SUCCESS | error`
- `Delete(key, request_id) -> SUCCESS | error`
- `Get(key) -> value, found` (see Read Protocol below)
- `ClusterStatus() -> {leader, term, nodes: [{id, role, lastContact}]}`

## Command model — formal, including NOOP

Every log entry's payload is one `Command`, with exactly three `OperationType` values — this is the complete set, not an open-ended one:
```go
type OperationType string
const (
    Set    OperationType = "SET"
    Delete OperationType = "DELETE"
    Noop   OperationType = "NOOP"  // internal-only, see below — never client-issued
)
type Command struct {
    OperationType OperationType
    Key           string  // empty/unused for NOOP
    Value         []byte  // empty/unused for NOOP and DELETE
    RequestID     string  // empty/unused for NOOP — see below
}
```
**`NOOP` is a formally defined command, not an ad hoc phantom entry the applier has to special-case implicitly.** It is never issued by a client and never appears on the client API surface above — it is generated internally, exactly once per successful leader election, by the mechanism described in `docs/architecture.md`'s "New-leader current-term commit" section (I-023). Its exact contract:
- **Has no KV effect.** The applier recognizes `OperationType == Noop` and returns/no-ops without touching `KV`.
- **Has no `RequestID`** (empty string) and **is never inserted into `RequestTable`** — it has no client waiting on it via `PendingWrite`, and there is no dedup concern since it's never retried by a client (a leader only ever emits one no-op per election, deterministically, not in response to any external request).
- **Produces no client-visible result** — nothing is returned to any client for a `NOOP` entry, because no client ever submitted it.
- **Participates fully in ordinary Raft log replication** — it is appended, replicated, ACKed, and persisted exactly like any `SET`/`DELETE` entry; there is nothing special about how it moves through the log.
- **Can establish current-term commitment** under the Current-Term Commit Rule (I-006) — this is the entire reason it exists: being a current-term entry, its commitment is exactly what lets the new leader conclude that its `commitIndex` has caught up to (at least) everything previously committed.
- **Advances `commitIndex`/`lastApplied` normally** through the ordinary commit/apply path — no special-cased advancement logic.

`Set`/`Delete` are always client-issued and always carry a non-empty `RequestID` (validated per "RequestID validation" below); `Noop` is always leader-internal and always carries an empty one. The applier's dispatch on `OperationType` is a plain switch over these three values — there is no fourth case, and no other code path is permitted to append an entry with an empty `RequestID` other than the `Noop` path.

## Result semantics (affects I-016, I-017, I-019, I-023)

| Result | Meaning | Retry? |
|---|---|---|
| `SUCCESS` | Command definitely committed and applied. Response includes the result. | No |
| `NOT_LEADER` | This node isn't the leader; response includes a `leader_hint` if known. | Yes, against the hinted leader |
| `NO_LEADER` | No known leader; response includes a `retry_after` hint. | Yes, with backoff |
| `TIMEOUT` | **Outcome unknown.** Includes the case where the original entry was superseded before commit (`docs/architecture.md`'s write-completion design, I-019) — from the client's point of view this is indistinguishable from any other timeout, and is handled identically: retry with the same `request_id`. | **Yes, with the SAME `request_id`** |
| `INVALID_REQUEST` | Malformed or oversized request. | No |
| `REQUEST_ID_REUSED` | Same `request_id`, different payload (compared by hash) than the original. | No |
| `OVERLOADED` | Leader has too many outstanding writes/replication in flight. | Yes, with backoff |

**The critical rule: `TIMEOUT` is never treated as failure.** Retry with the identical `request_id`.

## Write completion is request-identity-aware, not index-only (I-019)

This is a correctness property of the underlying write path, fully specified in `docs/architecture.md`'s "Write completion" section (`PendingWrite{RequestID, Index, Term}`), summarized here for the client-facing contract: a client's write request is only ever reported `SUCCESS` if the *specific command it submitted* (matched by `RequestID`) is the one that actually got applied at the index the leader originally appended it to. If a leader loses leadership before that entry commits and a different leader's entry ends up at the same index, the original request's waiter fails rather than resolving — the client observes this as `TIMEOUT` and retries safely with the same `request_id`, per the table above. **A client library built against this API never needs to distinguish "true timeout" from "my write got superseded" — both produce `TIMEOUT`, and the safe response is identical: retry with the same ID.** The waiter's full lifecycle — including client-cancellation cleanup and leadership-loss/shutdown handling, so waiters never accumulate indefinitely — is specified in `docs/architecture.md`'s `PendingWrite` lifecycle section.

## Idempotency — replicated dedup state

A local per-node cache cannot survive a leader crashing between commit and client-ack. Deduplication is part of the **replicated state machine**:

```go
type StateMachineState struct {
    KV           map[string][]byte
    RequestTable map[string]AppliedRequest
}

type AppliedRequest struct {
    RequestID   string
    PayloadHash []byte // SHA-256 of the CANONICAL command encoding — see "Canonical command
                        // hashing" below — NOT the full payload, and NOT SHA256(cmd.Payload)
                        // taken as opaque bytes
    Result      CommandResult
}
```

## Canonical command hashing — hash the command's identity, not an ambiguous byte blob (I-017)

`AppliedRequest.PayloadHash` (and the identical dedup check in the apply loop below) hashes a **canonical, deterministic serialization of the command's identity fields** — `{OperationType, Key, Value}` for `SET`/`DELETE` (the only two `OperationType`s that ever reach `RequestTable` — `NOOP`, per "Command model" above, never does) — never `cmd.Payload` treated as an opaque byte string. Hashing the raw payload directly is ambiguous: two different wire-level encodings of the *same* logical command could hash differently (a false `REQUEST_ID_REUSED`), and, more importantly, two different *commands* that happen to share an encoding prefix or representation could theoretically hash the same way if the encoding isn't disambiguated by field.

**The exact canonical byte encoding, frozen — one specific deterministic binary format, not "some canonical encoding" left to the implementer to invent:**
```
CanonicalEncode(cmd Command) []byte:
    version              : uint8       = 1               // bumped only on a breaking encoding change
    operationTypeLength   : uint8                          // byte length of the OperationType string
    operationType         : bytes (UTF-8)                   // e.g. "SET", "DELETE" — exactly as the
                                                              // OperationType constant's string value
    keyLength             : uint32, little-endian
    key                   : bytes (UTF-8)
    valueLength           : uint32, little-endian           // 0 for DELETE, which carries no value
    value                 : bytes                            // omitted entirely (zero bytes follow)
                                                              // when valueLength == 0
```
All multi-byte integer fields are little-endian, matching this project's WAL record format (`docs/architecture.md`) for consistency. Fields are concatenated in exactly this order with no separators, no padding, and no length field for `version` (it's always exactly 1 byte). `RequestID` is **never** included in this encoding — it identifies the *request*, not the command's semantic content, and including it would make two genuinely-identical retried commands (same `RequestID`, resubmitted) hash differently from a *legitimately different* command that happened to reuse the same `RequestID` maliciously or by bug, which is backwards: the whole point of `PayloadHash` is to detect exactly that mismatch by comparing command content independently of `RequestID`. `cmd.Payload` (an opaque wire/transport-level byte blob, if one exists elsewhere in the client-facing wire protocol) is never hashed directly, and this format is not JSON and not an unspecified/default protobuf serialization — protobuf's own wire format is not guaranteed byte-stable across encodings of semantically-identical messages (field ordering, unknown fields, varint encoding choices), which would make it unsuitable for a hash that must be exactly reproducible.

Then: `PayloadHash = SHA-256(CanonicalEncode(cmd))`.

**Required test (new): canonical-encoding determinism.** Construct the same semantic command (same `OperationType`, `Key`, `Value`) via two different code paths or independently in two separate test invocations, assert `CanonicalEncode` produces byte-identical output both times, and assert the resulting `SHA-256` hashes match. Combined with the existing canonical-encoding disambiguation test (`docs/phases/phase-05.md` — two different commands must hash differently even under a contrived overlapping raw encoding), this pair of tests is what actually pins the encoding down as an implementation contract rather than a description an implementer could satisfy multiple incompatible ways.

**`RequestID` format, frozen:** a client-supplied string, recommended as a UUID (`docs/adr/009-request-identity-model.md`), **must be valid UTF-8, 1–64 bytes** (empty is invalid — treated the same as oversized, below). Requests with an oversized, empty, or non-UTF-8 `RequestID` are rejected client-side with `INVALID_REQUEST` before becoming a log entry (`docs/failure-model.md`'s resource bounds). Treating `RequestID` as a UTF-8 string rather than opaque bytes is a deliberate, narrow choice: it keeps the canonical-command encoding above unambiguous (a fixed string-length-prefixed encoding needs a defined notion of "length" — byte length of valid UTF-8 is well-defined and matches what most client libraries' native string types already enforce) without requiring clients to think about byte-level escaping.

**Apply-loop behavior (performed while holding the StateMachine `RWMutex`'s write lock — `docs/architecture.md`):**
```
on applying a committed Command:
    entry := RequestTable[cmd.RequestID]
    if entry exists:
        if entry.PayloadHash != SHA256(CanonicalEncode(cmd)):
            result := REQUEST_ID_REUSED error   # application error — still applied, see below
        else:
            result := entry.Result              # dedup hit
    else:
        result := apply cmd to KV
        RequestTable[cmd.RequestID] = AppliedRequest{cmd.RequestID, SHA256(CanonicalEncode(cmd)), result}
    return result
```

### Application errors are still applied — do not roll back commitment
Once a command is committed (I-006) and reaches the applier, applying it and getting an application-level error (e.g. `REQUEST_ID_REUSED`) does **not** roll back `commitIndex`, does **not** block `lastApplied` from advancing, and does **not** remove the entry from the log — it's consumed deterministically exactly once, with an error result rather than a KV mutation.

### Idempotency scenario table

| Scenario | Behavior |
|---|---|
| Duplicate `request_id`, entry already applied (any node, including after failover) | Dedup hit — return cached result. |
| Duplicate `request_id`, **different** payload (hash mismatch) | `REQUEST_ID_REUSED` — entry is still committed/applied per above. |
| Duplicate `request_id`, original never actually committed | No dedup entry exists — retry is applied fresh. |
| Original entry superseded before commit (I-019) | No dedup entry was ever created for that `RequestID` (nothing committed under it) — client sees `TIMEOUT`, retries with the same `request_id`, which applies fresh, correctly. |
| `RequestTable` growth over time | **MVP decision: retain indefinitely** — a real, stated tradeoff. Snapshotting does not by itself shrink it (`docs/architecture.md`). Bounded retention is future work (`docs/adr/009-request-identity-model.md`). |

**Precise guarantee, stated operationally:**
> For a given `RequestID`, every committed occurrence of the command produces the same state-machine effect **at most once**. Retries may create additional log entries (at-least-once delivery), but duplicate committed entries are detected via the replicated `RequestTable` and do not repeat the KV mutation. Remaining caveat: unbounded `RequestTable` growth (resource/scalability, not correctness).

## Read protocol (Quorum-Confirmed Linearizable Reads)

**Precondition (I-023), enforced via an explicit gate — NOT an implicit consequence of the barrier wait:** the leader must already have committed and applied a no-op entry in its own current term (`docs/architecture.md`'s "New-leader current-term commit" section) before any `GET` can complete. **This is checked directly, as its own step, via a `readReadyTerm` marker — it is not something that "falls out" of the ordinary read barrier.** An earlier draft of this document claimed the barrier wait alone was sufficient; it is not: a fresh leader's `commitIndex` can start at `0` (or otherwise below the previous leader's last committed index), and a `GET`'s captured `barrier.CommitIndex` would then be `0` too — `lastApplied >= 0` is trivially already true, so the barrier wait would complete **immediately**, before the no-op has committed, and return a stale read. The gate below closes this exactly.

**Leader-readiness gate, set once per election, independent of any individual read's barrier:**
```go
// Raft-mutex-protected leader-only volatile state, alongside nextIndex[]/matchIndex[]
readReadyTerm uint64  // 0, or the term for which this node has committed+applied its own no-op
```
```
on becoming Leader in term T:
    readReadyTerm = 0                          // NOT ready yet — reset every election, never carried over
    append NOOP(T) to the log
    replicate to quorum, commit, apply          // ordinary commit/apply path
    on that no-op's apply completing:
        readReadyTerm = T                       // now, and only now, ready to serve reads in term T
```

```
GET(key):
1. Client sends GET to the (believed) leader.
2. Leader performs a quorum leadership confirmation — one round of heartbeats, majority ACK.
   **This round goes through the same per-follower replication lane as ordinary log replication
   (`docs/architecture.md`'s "Replication concurrency" / "Read-confirmation heartbeats" sections)
   — it is never sent as an independent RPC that could create a second outstanding AppendEntries
   to the same follower, which would violate the at-most-one-in-flight-per-follower rule.**
3. Leader checks readReadyTerm == currentTerm (under the Raft mutex). If not yet ready
   (readReadyTerm != currentTerm — either 0, or stale from a previous term), the leader
   BLOCKS/RETRIES this GET rather than proceeding — it must not fall through to the barrier
   wait below, since that wait can complete trivially against a not-yet-caught-up commitIndex.
4. Leader captures barrier := ReadBarrier{ Term: currentTerm, CommitIndex: commitIndex }.
5. Leader BLOCKS until lastApplied >= barrier.CommitIndex.        <-- read barrier
6. Leader acquires the Raft mutex, THEN, still holding it, acquires the StateMachine `RWMutex`'s
   **read lock** (`RLock`) — `docs/architecture.md`'s fixed Raft-mutex-before-StateMachine-mutex
   lock order — never the reverse — and RE-VALIDATES: if role != Leader OR currentTerm != barrier.Term OR
   readReadyTerm != barrier.Term, releases both locks and FAILS/RETRIES — do NOT return a value.
   ═══════════════════════════════════════════════════════════════════════════════
   THE SUCCESSFUL COMPLETION OF STEP 6's REVALIDATION IS THE READ'S LINEARIZATION POINT (I-016).
   Everything before it (steps 2-5) establishes the precondition for reading; the read
   is considered to have logically occurred at the instant step 6's revalidation succeeds
   — not at the moment the local KV lookup in step 7 physically happens a few instructions
   later. Holding the StateMachine mutex continuously from validation through the KV read
   in step 7 (released only after step 7) is what makes this true rather than aspirational:
   without it, a write could commit and apply in the gap between "revalidation succeeded"
   and "KV lookup happened," and the read would observe post-linearization-point state
   despite the comment claiming otherwise. Acquiring Raft-mutex-then-StateMachine-mutex here,
   rather than the reverse, is what keeps this consistent with the applier's own lock order
   (docs/architecture.md's Concurrency model) and avoids the deadlock a reversed order risks.
   ═══════════════════════════════════════════════════════════════════════════════
7. Leader reads local KV state (still holding the StateMachine mutex from step 6).
8. Leader releases the StateMachine mutex, then the Raft mutex, and returns the value.
```
**Note on step 5's wait:** the leader does not hold the Raft mutex for the duration of this wait — that would serialize all concurrent activity behind one slow read, which is the concurrency failure this design specifically avoids. The Raft mutex (and, immediately after it, the StateMachine mutex) is acquired only for the short, bounded critical section in step 6 — the wait itself polls/blocks on a condition variable or channel outside the lock, and the locks are acquired fresh, in the documented order, once the wait completes.

```go
type ReadBarrier struct {
    Term        uint64
    CommitIndex uint64
}
```
**Not implemented, named explicitly as future work:** ReadIndex, leader lease — `docs/adr/004-read-consistency.md`. The no-op-commit mechanism above (I-023) is this project's own approach and is explicitly not claimed to be the formal ReadIndex algorithm, consistent with `docs/adr/004-read-consistency.md`.

**Interview line:** *"Confirming leadership doesn't mean my local state is fresh — it means my log is, at that instant, and only once I've committed something in my own term, because commitIndex is volatile and Leader Completeness only guarantees my log has the old entries, not that I know they're committed yet. I track that explicitly with a readReadyTerm flag set only once my current-term no-op has applied — I don't just trust the barrier wait to enforce it, because a barrier captured against a not-yet-caught-up commitIndex can trivially satisfy itself before the no-op ever lands. Once I'm read-ready, I wait until my apply loop catches up to the commit index established at confirmation time, and I re-check immediately before returning — under the Raft mutex first, then the state-machine mutex, in that fixed order — that I'm still the leader in the same term and still read-ready. That successful re-check is the read's linearization point, and holding the state-machine lock across the handoff to the actual KV read is what keeps that claim actually true instead of just documented."*
