# Architecture

## System diagram

```
                         CLIENT
                            │
                            ▼
                    ┌──────────────┐
                    │  Client API  │   retry / leader routing / RequestID
                    └──────┬───────┘
                           │
                           ▼
                 ┌──────────────────┐
                 │   RAFT CORE      │   election, replication, commit, read barrier
                 └────────┬─────────┘
                          │
             ┌────────────┼────────────┐
             ▼            ▼            ▼
          Node 1        Node 2       Node 3
             │            │            │
            WAL          WAL          WAL
             │            │            │
             ▼            ▼            ▼
        State Machine (KV + Replicated RequestTable, docs/client-semantics.md)
                          │
                          │  emits into EventSink (one-way)
                          ▼
                 Structured Telemetry (ClusterEvent / MetricSnapshot)
                          │
                          ▼
                    Rule Engine (deterministic, always runs)
                          │
                          ▼
                    Optional LLM (read-only, fail-open, fully async)
                          │
                          ▼
                    Evidence Validator
                          │
                          ▼
                    AIIncident
```

**Hard architectural boundary — dependency direction:**
```
Raft  ──emits──►  EventSink (interface)  ◄──implements──  Observability
                                                              │
                                                              ▼
                                                             AI
```
`internal/observability` depends only on the `EventSink` interface those packages emit into — never a concrete `Node`/`StateMachine`/`Storage` type. `internal/ai` depends only on `internal/observability`'s typed surface. See I-015.

**The AI/observability path is fully asynchronous and never on the client's critical path:**
```
Client request → Raft → commit/apply → respond to client        (path 1, synchronous)
                              │
                              └──► emit ClusterEvent → Observability → AI   (path 2, async)
```

## `ClusterEvent` schema and event coverage matrix

```go
type EventType string
const (
    ElectionStarted EventType = "ELECTION_STARTED"; VoteGranted EventType = "VOTE_GRANTED"
    VoteRejected EventType = "VOTE_REJECTED"; LeaderElected EventType = "LEADER_ELECTED"
    LeaderSteppedDown EventType = "LEADER_STEPPED_DOWN"; TermAdvanced EventType = "TERM_ADVANCED"
    RPCFailed EventType = "RPC_FAILED"; RPCSucceeded EventType = "RPC_SUCCEEDED"
    LogAppended EventType = "LOG_APPENDED"; LogConflict EventType = "LOG_CONFLICT"
    CommitAdvanced EventType = "COMMIT_ADVANCED"; EntryApplied EventType = "ENTRY_APPLIED"
    NodeStarted EventType = "NODE_STARTED"; NodeStopped EventType = "NODE_STOPPED"
    NodeRestarted EventType = "NODE_RESTARTED"
    PartitionCreated EventType = "PARTITION_CREATED"; PartitionHealed EventType = "PARTITION_HEALED"
)

const ClusterEventSchemaVersion uint32 = 1  // bump on any breaking change to ClusterEvent's shape or Fields vocabulary
const MetricSchemaVersion uint32 = 1        // bump on any breaking change to MetricSnapshot's shape or Name vocabulary

type ClusterEvent struct {
    SchemaVersion uint32            // ClusterEventSchemaVersion at emission time — consumers (the AI
                                     // layer especially) reject/ignore events with an unsupported
                                     // version rather than guessing how to interpret an unknown shape
    EventID   string            // format: "<NodeID>/boot-<BootID>/<node-local monotonic sequence>"
                                 // e.g. "node-B/boot-7/00417" — see "EventID uniqueness" below for why
                                 // the boot/session component exists
    Sequence  uint64            // node-local monotonic sequence number, strictly increasing WITHIN
                                 // A SINGLE BOOT — resets on restart, deliberately NOT persisted (see
                                 // "Why Sequence is not persisted" below). Cross-restart uniqueness and
                                 // ordering come from combining it with BootID (below), exactly as
                                 // EventID already does: BootID provides cross-restart identity, Sequence
                                 // provides within-boot ordering for events without a meaningful
                                 // Term/LogIndex (e.g. ELECTION_STARTED, RPC_FAILED)
    Timestamp time.Time         // node-LOCAL wall clock in production; a FAKE-CLOCK logical timestamp
                                 // in Level 1-3 deterministic tests/fixtures — see "Timestamp semantics"
    NodeID    string
    PeerID    string            // the remote node this event concerns, if any (e.g. RPC_FAILED's
                                 // target, VOTE_GRANTED's recipient) — empty when not applicable
    CorrelationID string        // identifies a single logical RPC attempt, and — unlike attemptID
                                 // (replicator-internal bookkeeping, deliberately NOT on the wire,
                                 // see "Replication concurrency"/"Stale response handling" above) —
                                 // CorrelationID IS a wire field, generated by the sender and sent
                                 // AS PART OF the AppendEntries/RequestVote RPC request itself (see
                                 // "CorrelationID is a wire field" below). This is what actually lets
                                 // it correlate a leader-side event with the corresponding peer-side
                                 // event for the same logical RPC attempt (e.g. a leader's
                                 // RPC_SUCCEEDED and the receiving follower's LOG_APPENDED) — an
                                 // earlier draft of this document described this correlation as
                                 // CorrelationID's purpose while also saying it wasn't a wire field,
                                 // which is a contradiction: a value generated only on the sender's
                                 // side, never transmitted, cannot be reproduced by the receiver to
                                 // tag its own events with the same ID. There is deliberately no
                                 // separate RPC_SENT event in the EventType enum below (keeping event
                                 // volume down is a real, intentional scope decision) — CorrelationID
                                 // surfaces on whichever outcome event each side actually emits for
                                 // that attempt, not on a dedicated request-event pair.
    Term      uint64
    LogIndex  uint64
    Type      EventType
    Fields    map[string]string // small, bounded, code-controlled vocabulary — never raw client payload;
                                 // see "Event field dictionary" below for the exact per-EventType schema
}

type MetricSnapshot struct {
    SchemaVersion uint32     // MetricSchemaVersion at emission time — same rationale as
                              // ClusterEvent.SchemaVersion above: this project's telemetry
                              // boundary is described throughout as "typed, versioned
                              // telemetry" (docs/ai-design.md), and that description was
                              // only true of ClusterEvent until this field was added here too
    Timestamp time.Time  // same production-vs-fixture rule as ClusterEvent.Timestamp
    NodeID    string
    Name      string
    Value     float64
    Labels    map[string]string
}
```

**Metric name vocabulary — frozen set for the MVP, not an open namespace:** `leader_changes_total`, `append_entries_failures_total`, `replication_lag` (in log entries, follower behind leader), `commit_latency` (seconds, client-write-received to committed), `election_duration` (seconds, election-started to leader-elected or election-abandoned), `node_up` (1/0 gauge). `Name` values outside this set should not appear from production code without updating this list first — an AI/rule-engine consumer reading `Name`/`Value` needs a frozen meaning to reason about, not an ad hoc string.

**Metric label vocabulary — also frozen, per metric, not an arbitrary `map[string]string`:** leaving `Labels` unconstrained just moves the same ambiguity `Name` had before it was frozen into a different field. Allowed labels, exhaustive per metric — any other key on a given metric's `Labels` map is invalid:
```
leader_changes_total          → (none — cluster-wide counter, no labels)
append_entries_failures_total → peer         (which follower the failure was against)
replication_lag               → peer         (which follower this lag measurement is for)
commit_latency                → (none)
election_duration             → outcome      ("elected" | "abandoned" | "lost")
node_up                       → (none — NodeID on the struct itself already identifies the node)
```
**Reject non-finite `MetricSnapshot.Value`:** the same rule already applied to `AIIncident.Confidence` (below) applies here — a `Value` of `NaN`, `+Inf`, or `-Inf` must never enter the AI reasoning pipeline (or, ideally, must never be emitted by production code in the first place; the AI ingest boundary rejects it defensively regardless). `docs/ai-design.md`'s data-boundary validation is extended to check this alongside the `SchemaVersion` check already specified there.

**Event field dictionary — per-`EventType` required `Fields` keys, so validation (including the AI evidence validator) is deterministic rather than guessing which keys might be present:**

| EventType | Required `Fields` keys |
|---|---|
| `RPC_FAILED` | `rpc_type`, `peer`, `error_class` |
| `RPC_SUCCEEDED` | `rpc_type`, `peer` |
| `LOG_CONFLICT` | `peer`, `index`, `term` |
| `LEADER_ELECTED` | `leader` |
| `LEADER_STEPPED_DOWN` | `reason` |
| `TERM_ADVANCED` | `old_term`, `new_term` |
| `COMMIT_ADVANCED` | `old_index`, `new_index` |
| `VOTE_GRANTED` / `VOTE_REJECTED` | `candidate` |
| `PARTITION_CREATED` / `PARTITION_HEALED` | `peers` — canonical format: a comma-separated, ascending-sorted list of `NodeID`s on the partitioned/healed side (e.g. `"node-B,node-C"`), never free-form text — see "Canonical node-bearing fields" below |

Events not listed above (`ELECTION_STARTED`, `LOG_APPENDED`, `ENTRY_APPLIED`, `NODE_STARTED`/`NODE_STOPPED`/`NODE_RESTARTED`) currently define no required keys beyond the struct's own typed fields (`NodeID`/`Term`/`LogIndex`); add a row here before adding new required keys to any event's `Fields`, so the table stays authoritative. **Bounds, defensively enforced:** max 10 `Fields` entries per event, max 64 bytes per key, max 512 bytes per value — a code-controlled vocabulary should never approach these, so hitting the bound indicates a bug, not a legitimate large event.

**Canonical node-bearing fields — the complete, frozen set of places a `NodeID` can appear on a `ClusterEvent`, used by the AI evidence validator's `AffectedNodes`-binding check (`docs/ai-design.md`) instead of an undefined generic `Fields["target"]`:**
```
ClusterEvent.NodeID              — the node that emitted/owns this event
ClusterEvent.PeerID              — the remote node this event concerns (struct field, above)
ClusterEvent.Fields["peer"]      — RPC_FAILED, RPC_SUCCEEDED, LOG_CONFLICT
ClusterEvent.Fields["candidate"] — VOTE_GRANTED, VOTE_REJECTED
ClusterEvent.Fields["leader"]    — LEADER_ELECTED
ClusterEvent.Fields["peers"]     — PARTITION_CREATED, PARTITION_HEALED (comma-separated, sorted
                                    NodeID list per the canonical format above — parsed by
                                    splitting on "," against this fixed format, never by
                                    free-text search)
```
There is no `Fields["target"]` key anywhere in this schema — an earlier draft of this document's AI evidence-validation section referenced one, which was a documentation bug (the field never existed in the dictionary above); the validator must be implemented against this exact list, not a generic/assumed key name.

**Minimum event coverage matrix (Phase 7 requires all of these):**

| Transition | Event |
|---|---|
| follower → candidate | `ELECTION_STARTED` |
| candidate → leader | `LEADER_ELECTED` |
| leader → follower (any reason) | `LEADER_STEPPED_DOWN` |
| any node adopts a higher term | `TERM_ADVANCED` |
| vote granted / rejected | `VOTE_GRANTED` / `VOTE_REJECTED` |
| RPC failure / success | `RPC_FAILED` / `RPC_SUCCEEDED` |
| entry appended | `LOG_APPENDED` |
| conflict + truncation | `LOG_CONFLICT` |
| commitIndex advances | `COMMIT_ADVANCED` |
| entry applied | `ENTRY_APPLIED` |
| node starts / stops / restarts | `NODE_STARTED` / `NODE_STOPPED` / `NODE_RESTARTED` |
| chaos partition injected/healed | `PARTITION_CREATED` / `PARTITION_HEALED` |

**`NODE_STARTED` vs. `NODE_RESTARTED`, frozen condition:** `NODE_STARTED` is emitted on a genuine first-ever boot (no prior durable `TermVoteStore` record existed — `bootID` is being initialized to `1` for the first time, `docs/architecture.md`'s TermVoteStore section). `NODE_RESTARTED` is emitted whenever the process starts and finds an *existing* durable `TermVoteStore` record (`bootID` is being incremented from some prior value) — this covers both a clean restart and a crash-restart identically; the event stream doesn't distinguish "was the previous shutdown clean" from the startup side alone (a `NODE_STOPPED` immediately preceding it in the same recorded stream is what tells a reader it was clean — see below).

**`NODE_STOPPED` cannot be emitted by an abrupt process kill (`kill -9`) — this is expected, not a bug, and worth stating explicitly so it isn't mistaken for a coverage gap:** `NODE_STOPPED` is emitted only by `Node.Stop()`'s graceful shutdown sequence (`docs/architecture.md`'s "Graceful shutdown contract" section, step 8) — a process that's abruptly killed never runs that sequence and therefore never emits it. The event stream's model of this, made explicit:
```
Graceful Node.Stop()          → NODE_STOPPED emitted (if the observability path is still reachable)
Abrupt kill/crash             → NO NODE_STOPPED from the killed process — the killed process is
                                 gone before it can emit anything about its own death
                                 ↓
                               evidence of the crash instead comes from OTHER nodes' events:
                               peer-side RPC_FAILED events against the now-dead node, followed
                               eventually by a NODE_RESTARTED (or LEADER_ELECTED if it was the
                               leader) once/if it comes back — the event stream reconstructs
                               "this node crashed" from its absence and peers' reactions, not
                               from a self-reported death event
```
This makes the telemetry model realistic (a real crash genuinely cannot self-report) and is directly relevant to the AI layer's evidence: a `NODE_UNREACHABLE` diagnosis (`docs/ai-design.md`) is expected to cite peer-side `RPC_FAILED` events, never a `NODE_STOPPED` from the unreachable node itself, since a genuinely crashed/partitioned node by definition cannot emit one.

**EventID uniqueness, including across restarts:** a node-local monotonic sequence *alone* resets to near-zero on every restart, which would make IDs collide across a node's lifetime (e.g. `node-B-00417` from before a crash and `node-B-00417` from after). The `BootID` component — **a counter, incremented once per process start, persisted alongside `currentTerm`/`votedFor` in the same `TermVoteStore` record** (see below) — makes `EventID` unique across the node's entire lifetime, not just within one run. This is the one and only mechanism: an earlier draft of this document also floated a fresh-UUID-per-boot alternative, which is removed here per the project's own no-unresolved-ambiguity rule — `BootID` is strictly simpler given `TermVoteStore` already exists and is already durable, and a monotonically increasing integer (`NodeID/boot-7/00417`) is more inspectable in logs/fixtures than a UUID. For **deterministic fixtures** (Level 1–3), the `BootID` is itself deterministic (e.g. derived from the seeded scenario), so fixture regeneration from the same seed produces identical `EventID`s.

**Hard rule: `Node.Start()` must not expose the node to normal operation or emit any normal-path telemetry until its incremented `BootID` has been durably persisted.** This closes a subtle re-use window: if a node calculated `bootID = 8`, began the temp-write/fsync/rename/directory-fsync sequence (I-024), and started emitting events tagged `boot-8` *before* that sequence's directory-fsync had actually landed, a crash in that window could leave the durable record still showing `boot-7` on disk — the next restart would then load `bootID=7`, increment to `8` again, and re-emit `boot-8/1`, colliding with (already-emitted, possibly externally observed) events from the crashed run that also claimed to be `boot-8`. Requiring the full persistence of the new `BootID` to complete, successfully, before the node does anything else — no votes granted, no RPCs sent, no events emitted — makes this collision structurally unreachable: any `EventID` a node emits is guaranteed to belong to a `BootID` that was durably committed before that node did anything externally observable under it.

**Timestamp semantics:** in production, node-local wall clock, never used for cross-node causal ordering (`docs/failure-model.md`) — causal ordering across replicated-log operations comes from `Term`+`LogIndex`. **For events that don't carry a meaningful `Term`/`LogIndex` (e.g. `ELECTION_STARTED`, `RPC_FAILED`), node-local relative ordering within a single boot comes from `Sequence`**, not `Timestamp` — wall-clock time is for human/debugging display only, never for ordering logic. Cross-node correlation of a specific request/response pair uses `CorrelationID`, not timestamp proximity. **In Level 1–3 deterministic tests and their saved fixtures, `Timestamp` is the fake clock's logical time, not real wall-clock time** — this is required for fixture regeneration to be byte-identical across runs (a fixture containing real wall-clock timestamps could never be reproduced exactly). Level 4–5 tests and live demos use real timestamps, consistent with production.

**Why `Sequence` is not persisted, and why it doesn't need to be:** an earlier draft of this document described `Sequence` as surviving restarts, which is wrong and was never actually backed by any durable-write mechanism — the only durably persisted identity this node has across restarts is `TermVoteStore`'s `{currentTerm, votedFor, bootID}` record, and fsyncing on every single telemetry event just to maintain a lifetime-global sequence number would be a real, unwarranted cost this project explicitly doesn't want to pay. The correct, and simpler, model: `Sequence` is purely in-memory, resets to 0 (or 1) on every process start, and is strictly increasing only *within* that boot. Cross-restart uniqueness and ordering are entirely `BootID`'s job — `EventID`'s `<NodeID>/boot-<BootID>/<Sequence>` format (above) already composes the two correctly; `Sequence` alone was never meant to carry cross-restart meaning on its own, and this document previously implied otherwise. To compare two events' relative order across a restart boundary, compare `(BootID, Sequence)` lexicographically, never `Sequence` alone.

## `ClusterStatus.lastContact`
The reporting node's own local wall-clock time of the last RPC it successfully received from that peer. Never used for cross-node causal reasoning.

## Raft log vs. WAL — one system, not two sequential stages
The Raft log *is* the thing being persisted; the WAL is its persistence mechanism — appending to the log means appending to the WAL, together, before the entry is durable.

## Log indexing — frozen, no off-by-one ambiguity
**Client log entries are 1-based. Index 0 is the empty-log sentinel and is never a real command entry.** An empty log has `lastLogIndex = 0, lastLogTerm = 0`. `nextIndex` for a peer is initialized to `lastLogIndex + 1`. This applies uniformly — no special-casing an empty log anywhere in election, replication, or truncation logic (see also the RequestVote log-freshness formula below, which already handles the all-zero case without a special case).

## Storage failure behavior — fail closed (I-018)

**If any required durable write returns an error — `LogStore.Append`, the underlying `fsync`, `LogStore.TruncateFrom`, or `TermVoteStore.Save` — the node fails closed:**
```
durable write fails
   ↓
do NOT send a success ACK for the corresponding RPC
do NOT grant the dependent vote
do NOT count the entry toward matchIndex/replication
do NOT advance commitIndex/lastApplied based on it
do NOT continue normal consensus operation as if durable and in-memory state still agree
   ↓
transition the node to a stopped/failed state — a distinct internal `Role`/status value,
`StorageFailed`, separate from Follower/Candidate/Leader: in this state the node grants no
votes, sends/ACKs no replication RPCs, accepts no client writes, and reports itself as failed
via `ClusterStatus`
```
This is deliberately the simple, safe answer rather than a sophisticated attempt to roll back partially-persisted in-memory state — reconciling a torn write against in-memory state correctly is its own hard problem, and "stop rather than risk operating on state that may not match disk" is both simpler to implement correctly and simpler to reason about for this project's scope. **The exact ordering is fully specified in "Write path" below and applies uniformly everywhere: durable write first, in-memory mutation only after it succeeds.** Every call site that performs a durable write must check its error return and trigger this behavior before doing anything else with the write's result — never append to the in-memory log (or mutate `log[]`/offset maps) before the corresponding durable write has succeeded, and never let an in-memory mutation stand if the durable write it was contingent on failed. The ordering of "attempt durable write, then decide what to do" must never be inverted into "mutate in-memory state unconditionally, then maybe undo it."

## Write path

**Durability ordering (I-018): disk is authoritative before memory becomes visible.** For every newly appended entry, `LogStore.Append` (and its `fsync`) happens *before* the in-memory `log[]`/`matchIndex[self]` are mutated — never the reverse. This is the one exact model, stated once here and never contradicted elsewhere in this document: construct the `LogEntry` → `LogStore.Append(entry)` → `fsync` → **only on success**, mutate `NodeState.Log` → `matchIndex[self] = lastLogIndex`. If `LogStore.Append`/`fsync` fails, fail closed (above) and do not mutate the in-memory log at all — do not append to the in-memory log first and only then check whether the durable write succeeded.

```
Client write
   ↓
Leader: construct LogEntry
   ↓
Leader: LogStore.Append (fsync) — on error, fail closed (above), do NOT mutate in-memory log, do not proceed
   ↓ success
Leader: append to in-memory log
   ↓
   matchIndex[self] = lastLogIndex
   ↓
AppendEntries → followers (at most ONE in-flight AppendEntries RPC per follower at a time — see
                            "Replication concurrency" below; simplifies nextIndex/matchIndex reasoning)
   ↓
Follower: validate (log-matching check), determine conflicting suffix (if any)
   ↓
Follower: TruncateFrom (fsync) if conflicting — on error, fail closed, do not ACK
   ↓
Follower: LogStore.Append the new suffix (fsync) — on error, fail closed, do not ACK
   ↓ success
Follower: mutate in-memory log to match
   ↓
Follower: ACK (only sent AFTER the follower's own fsync completes — I-013)
   ↓
Leader: response handling — see "Stale response handling" below before touching nextIndex/matchIndex
   ↓
Leader: advance matchIndex for that follower
   ↓
Leader: is there a highest index N such that a majority (including self) have matchIndex >= N
         AND log[N].term == currentTerm?  (I-006)
   ↓ yes
advance commitIndex
   ↓
applier: apply entries through commitIndex
   ↓
advance lastApplied
   ↓
resolve the PENDING WRITE WAITER for that entry, if any — see "Write completion" below;
this is NOT simply "index N became applied," it is request-identity-aware
   ↓
respond to client: SUCCESS (or TIMEOUT if the waiter never resolves — see below)
```

## Write completion — request-identity-aware, never index-only (I-019)

**The gap this closes:** if a leader appends an entry at index 10 for a client's request, then loses leadership before that entry commits, a new leader can append a *different* entry at index 10 (the old, uncommitted one gets truncated away on the old node once it reconnects and the log-matching check fails, per I-002/I-011's normal operation). If the client-write-completion mechanism is naively "wait for index 10 to be applied," it will observe index 10 eventually being applied — just with the *wrong* command in it — and incorrectly report `SUCCESS` for a write that was never actually committed.

**Fix:**
```go
type PendingWrite struct {
    RequestID string
    Index     uint64  // the index the leader originally appended this request's entry at
    Term      uint64  // the term the leader was in when it appended — THIS IS the leadership
                       // epoch identifier (Fix 2, below): no separate LeadershipEpoch field
                       // exists, or is needed, because currentTerm already uniquely identifies
                       // a leadership epoch in Raft
    Done      chan CommandResult  // buffered, capacity 1 — see "Done channel safety" below
}
```
**`PendingWrite.Term` IS the leadership-epoch identifier — stated explicitly so it isn't mistaken for redundant bookkeeping.** `PendingWrite.Term` records the `currentTerm` in which the leader that appended this request was leading at the time it appended it. "This leadership epoch" (used throughout this section and `docs/client-semantics.md`) means exactly "the set of `PendingWrite`s whose `Term` matches the term this node was leading in before it lost leadership" — there is no separate `LeadershipEpoch` field, and none is needed, because a Raft `currentTerm` value already uniquely identifies a leadership epoch (I-001: at most one leader per term). On leadership loss, the leadership-loss resolution path (below) fails every `PendingWrite` whose `Term` equals the term being stepped down from.

**Registry key, explicit:** `pendingWrites map[uint64]*PendingWrite`, keyed by `Index` (the same `Index` stored inside the value) — not by `RequestID`. This is intentional, not redundant: lookup during the applier's normal apply-loop path (below) is naturally by index (`applying the entry at index N` → `is there a waiter for index N?`), and the uniqueness invariant this key relies on is **at most one `PendingWrite` exists for a given local log index at a time** — true because a leader only ever appends one entry at a given index once, and by the time a *new* leader's different entry could occupy that same index (after the original was superseded and the old leader has stepped down), the old leader's own `PendingWrite` for that index has already been failed/removed via the leadership-loss path below, so the map slot is free for reuse without ambiguity. (A secondary `RequestID`-keyed lookup is not needed anywhere in this design — cancellation and leadership-loss cleanup both operate over the whole registry or a leadership-epoch's entries via a scan/iteration over the `Index`-keyed map, not a `RequestID`-targeted lookup. This project deliberately keeps the single `Index`-keyed map; do not add a second, `RequestID`-keyed registry.)

### `Done` channel safety — terminal notification must never block a Raft-mutex-holding or StateMachine-mutex-holding goroutine

**`Done` is a buffered channel with capacity exactly 1, created at `PendingWrite` registration time.** This single fact is what makes every terminal-resolution code path below safe to reason about:

- **A send on `Done` never blocks**, because the buffer has room for exactly the one result value any given `PendingWrite` will ever receive (a `PendingWrite` reaches exactly one terminal outcome — see the lifecycle diagram below — so it is sent to at most once). This holds **even if the client has already given up and nobody is ever going to receive from `Done`** — the buffered send still completes immediately rather than blocking on a reader that may never show up.
- **Consequence: no code path may perform a blocking (unbuffered-style) send on `Done`, and none needs to** — every terminal-resolution path (the applier resolving `SUCCESS`, the leadership-loss handler failing an epoch's waiters, the shutdown handler failing all remaining waiters) may safely send on `Done` **while still holding the Raft mutex and/or the StateMachine mutex**, because the send is guaranteed non-blocking by the buffer. This is precisely why capacity must be `1`, not `0`: an unbuffered channel's send blocks until a receiver is ready, which would mean the applier (holding both mutexes per `docs/architecture.md`'s Concurrency model) could stall waiting on a client that has already walked away — a lock-held-while-potentially-blocked bug this design avoids structurally rather than by convention.
- **Registry removal and terminal resolution happen together, exactly once, for exactly one outcome per `PendingWrite`:** the code path that decides a `PendingWrite`'s terminal outcome removes it from `pendingWrites` **and** sends on `Done` as a single logical step (both performed while holding the map's own protecting lock, so no other path can observe or act on a `PendingWrite` that's mid-resolution) — never one without the other, and never twice. A `PendingWrite` that has already been removed/resolved is simply absent from the map for any other code path that might otherwise try to resolve it again.
- **Client-side cancellation is symmetric, and non-blocking in the other direction too:** the client's own wait on `Done` is done with a `select` against its own context's `Done()`/timeout channel, not a bare blocking receive — so a client that gives up waits on nothing Raft-internal, and cancels without blocking the applier or any consensus goroutine. If a client cancels/times out, it removes its own `PendingWrite` from the registry (or marks it abandoned) so that a *later* terminal resolution attempt from the applier finds nothing to resolve.
- **If a waiter has already been cancelled/abandoned by the time a terminal-resolution path would otherwise fire (e.g. the applier reaches the relevant index after the client already gave up and removed its entry), that terminal notification is a no-op** — there is nothing left in the registry to resolve, and no send is attempted; this is the expected, race-free outcome of "removal and resolution happen together" above, not a special case requiring extra logic.

Resolution logic in the applier, on applying the entry at index N:
```
if a PendingWrite is registered for index N (this node was the leader that appended it):
    if the applied entry's RequestID matches the PendingWrite's RequestID:
        resolve the waiter with the actual result (SUCCESS path)
    else:
        # a different command ended up at this index — this request's original entry
        # was superseded before it committed. Do NOT resolve as SUCCESS.
        fail/abandon this waiter (it will surface to the client as TIMEOUT, per
        docs/client-semantics.md — the client retries with the same RequestID, which
        is safe because nothing was ever actually committed under that RequestID)
```
**Explicit rule: a pending client write must never resolve as `SUCCESS` solely because its original log index became applied.** See I-019.

### `PendingWrite` lifecycle — every waiter must be removed, never accumulate indefinitely

A `PendingWrite` is registered when a client write is submitted and must reach exactly one of these terminal outcomes, after which it is removed from the registry:

```
PendingWrite created
       ↓
 ┌─────┴─────────┬───────────────────┬────────────────────┐
 ↓                ↓                   ↓                    ↓
SUCCESS       TIMEOUT/CANCEL    leadership lost      node shutdown
(applied,        (client            while pending      (Node.Stop())
RequestID       context           (this leadership
matches)        cancelled,        epoch's waiters
                 or explicit       are failed)
                 client timeout)
       ↓                ↓                   ↓                    ↓
      remove from registry, in every case — a waiter must never remain indefinitely registered
```
- **Client cancellation/timeout:** the waiter is removed when the client's own request context is cancelled or its timeout elapses — it is not left registered hoping a later apply will resolve it (if that later apply does happen, resolving an already-abandoned waiter is a no-op per "Done channel safety" above; the client has already moved on and will retry with the same `RequestID` per `docs/client-semantics.md`).
- **Leadership loss:** on losing leadership (stepping down, discovering a higher term, etc.), every `PendingWrite` whose `Term` field equals the term being stepped down from — i.e. every waiter from that leadership epoch, per "`PendingWrite.Term` IS the leadership-epoch identifier" above — is failed immediately, via a non-blocking send on each `Done` (safe per "Done channel safety," even while this handler may be running with the Raft mutex held). They do not remain registered hoping the entry commits under a different leader (it may or may not; either way this waiter must not silently succeed for it, per I-019). This surfaces to the client as `TIMEOUT`, which is always safe to retry with the same `RequestID`.
- **Node shutdown:** `Node.Stop()`'s shutdown sequence (below) fails/abandons any still-pending waiters (non-blocking sends, same mechanism) rather than blocking shutdown on them.
- Phase 10's soak test explicitly checks for `PendingWrite` registry growth over time (a leaked-waiter class of bug) as a resource-leak signal, the same way it checks for goroutine leaks.

## Replication concurrency — at most one AppendEntries RPC in flight per follower

**MVP rule:** the leader never has more than one outstanding AppendEntries RPC to a given follower at a time. The replication loop for a follower waits for the previous RPC's response (or its timeout) before sending the next one. This is a deliberate simplification — pipelining multiple in-flight requests per follower is a legitimate production optimization, explicitly out of scope here — because it removes an entire class of response-ordering bugs (see "Stale response handling" below) without meaningfully hurting correctness-focused demo throughput.

**This bounds outstanding *requests*, but does not by itself make every same-term response safe to apply — see "Stale response handling" below for why an explicit per-follower attempt identifier is still required.** A request that has *timed out* locally is no longer counted as "in flight" by this rule, which means a second request can already be outstanding by the time the first one's (now-stale) response finally arrives; a bare `response.Term == currentTerm` check cannot distinguish that late response from the current one.

**Read-confirmation heartbeats use this exact same per-follower lane, never a second independent RPC channel to the same follower.** A quorum-confirmation round for a `GET` (`docs/client-semantics.md`'s Read protocol, step 2) is, mechanically, "confirm that a heartbeat-equivalent AppendEntries round-trips successfully to a majority" — and a naive implementation could be tempted to fire that round via its own code path (e.g. a `readConfirmation.SendHeartbeat()` independent of `replicator.SendAppendEntries()`), which would create a **second concurrent outstanding AppendEntries RPC to the same follower**, directly violating the at-most-one-in-flight rule above. **This must not happen.** The correct model: a read-confirmation round piggybacks on / is coordinated through the same per-follower replication lane and its `replicationAttempt[follower]` counter — concretely, a read confirmation either (a) waits for the next AppendEntries round the ordinary replication/heartbeat loop was already going to send for that follower and observes its outcome, or (b) if it needs to trigger one immediately rather than wait for the next scheduled heartbeat, does so *through* the replication loop's own send path (incrementing the same `replicationAttempt[follower]` counter, going through the same stale-response protection), never by constructing and sending an independent RPC directly. There is exactly one code path per follower that is allowed to have an AppendEntries RPC in flight to it, and read confirmation is a *consumer* of that path's outcomes, not a second sender.

## Stale response handling (I-021)

**Per-follower attempt tracking (transport/replicator-internal — need not be on the wire):**
```go
replicationAttempt[follower]++   // incremented every time a new AppendEntries is sent to this follower —
                                  // this single counter is shared by BOTH ordinary log-replication sends
                                  // and read-confirmation sends to that follower; there is no separate
                                  // counter or lane for read confirmation (see "Read-confirmation
                                  // heartbeats" above)
```
```
send attempt 17 to follower F
attempt 17 times out locally (no longer "in flight" per the at-most-one rule above)
send attempt 18 to follower F           // now the one in flight
response for attempt 17 arrives late
        ↓
attempt 17 != replicationAttempt[F] (18)  → IGNORE, even though response.Term == currentTerm
response for attempt 18 arrives
        ↓
attempt 18 == replicationAttempt[F] (18)  → process normally
```

```
on receiving an AppendEntriesResponse for follower F, attempt A:
    if response.Term < currentTerm:
        ignore entirely — this response is from a stale request, our term has since advanced
    else if response.Term > currentTerm:
        adopt the higher term, persist (I-007/I-012), step down to Follower —
        do NOT update nextIndex/matchIndex based on this response
    else:  # response.Term == currentTerm
        if role != Leader:
            ignore — this node has already stepped down (e.g. accepted a same-term AppendEntries
            from another leader) even though the response's term still matches; a same-term
            response is only meaningful while this node is still the leader that sent the request
        else if A != replicationAttempt[F]:
            ignore — a stale response to an earlier, already-timed-out attempt to this follower
            (see "Replication concurrency" above for exactly how this arises even under the
            at-most-one-in-flight rule)
        else:
            proceed with normal matchIndex/nextIndex update logic
```
**All three conditions — `role == Leader`, `response.Term == currentTerm`, and `attempt == replicationAttempt[follower]` — must hold before any `nextIndex`/`matchIndex` mutation.** See I-021 for the full statement and required tests.

**RequestVote responses follow the same term-comparison discipline as AppendEntries responses above — term comparison happens FIRST, before any election-generation/role filtering, since a higher term observed on ANY RPC response (I-007's fourth touchpoint) mandates an immediate step-down regardless of what this node currently believes about its own candidacy:**
```go
// Raft-mutex-protected candidate-only volatile state
electionTerm uint64  // the currentTerm value at the moment THIS candidacy started (recorded
                      // once, when transitioning to Candidate and incrementing currentTerm)
```
```
on receiving a RequestVoteResponse(resp):
    if resp.Term > currentTerm:
        # MANDATORY higher-term step-down (I-007 touchpoint 4) — this check comes BEFORE
        # role/electionTerm filtering and is NEVER suppressed by them. A node must react to
        # a higher term observed on ANY RPC response, including a RequestVote response,
        # regardless of whether that response even belongs to this node's current candidacy.
        persist {currentTerm = resp.Term, votedFor = ""}   # durable write, I-012 — before
                                                              # continuing any further processing
        role = Follower
        abandon the current election (this response never counts as a vote toward it)
        return
    if resp.Term < currentTerm:
        ignore — stale response, our term has since advanced
        return
    # resp.Term == currentTerm from here on — now, and only now, apply role/election-scope
    # filtering to decide whether this SAME-TERM response belongs to the current candidacy
    if role != Candidate:
        ignore — already became Leader, stepped down, or started a newer election
        return
    if electionTerm != currentTerm:
        # structurally unreachable in practice, since currentTerm only advances alongside a
        # fresh electionTerm assignment (never independently while role == Candidate) — kept
        # as an explicit defensive check rather than an assumed invariant
        ignore
        return
    process normally (count toward majority, etc.)
```
**The critical principle, stated once so it can't be re-derived incorrectly: term comparison always comes before election-generation/role filtering, never after.** The election-generation check (`electionTerm`) answers "does this same-term response belong to my current candidacy" — it is not, and must never be used as, a substitute for the mandatory higher-term reaction. An earlier draft of this document got the ordering backwards (checking `electionTerm` before the term comparison), which would silently swallow a legitimate higher-term response whenever it happened not to match the current `electionTerm` — exactly the response that most urgently needs to be acted on.

**Required test (new):** a candidate in term 5 sends `RequestVote`; a response arrives carrying `Term = 6` — assert `currentTerm == 6`, `role == Follower`, the term/vote transition was durably persisted before any further processing, the in-progress election is abandoned, and the response does not count toward the (now-abandoned) election's vote tally.

## `nextIndex`/`matchIndex` initialization, on becoming leader
```go
for each peer:
    nextIndex[peer]  = lastLogIndex + 1
    matchIndex[peer] = 0
matchIndex[self] = lastLogIndex
```

## New-leader current-term commit — required before serving linearizable reads (I-023)

**The gap this closes:** Leader Completeness (I-004) guarantees a newly elected leader's *log* already contains every entry a previous leader committed. It says nothing about `commitIndex`, which is **volatile** (§Recovery path below) — a new leader's `commitIndex` starts wherever its own recovery/election path leaves it, and quorum-heartbeat leadership confirmation alone only proves "a majority currently accepts me as leader," not "my `commitIndex` reflects everything previously committed." A read served against a not-yet-caught-up `commitIndex` can return a stale value even though the correct value was sitting right there in the leader's own log, uncounted as committed yet — a linearizability violation.

**Fix — immediately upon election, before unblocking any read barrier:**
```
Leader elected in term T
        ↓
readReadyTerm = 0                     // explicit gate — NOT ready yet, reset every election
leaderNoOpIndex = 0                   // explicit "have I already appended my no-op" marker —
                                       // reset every election, alongside readReadyTerm
        ↓
append EXACTLY ONE no-op log entry for term T   ← guarded by leaderNoOpIndex, see below
        ↓
leaderNoOpIndex = that entry's index
        ↓
replicate to quorum (ordinary AppendEntries path)
        ↓
commit the no-op (I-006: it's a current-term entry, so it can establish commitment directly)
        ↓
apply through the no-op (ordinary applier path)
        ↓
readReadyTerm = T                     // NOW, and only now, ready to serve reads in term T
```
**`leaderNoOpIndex` exists to guarantee exactly one no-op per leader election, not merely "at least one."** Without an explicit marker, it's easy for an implementation to append a second no-op — e.g. some retry/event-driven path re-triggers the "leader just became ready, make sure the no-op is in flight" logic a second time, not realizing one was already appended. A duplicate no-op wouldn't violate any safety invariant on its own (it's still a legitimate current-term entry), but it does violate this design's own stated intent of exactly one no-op per election, wastes a log slot, and is exactly the kind of ambiguity that leads an AI coding tool toward two subtly different implementations.

**The exact lifecycle, frozen:**
```
On transition to Leader in term T:
    readReadyTerm   = 0
    leaderNoOpIndex = 0
        ↓
    append exactly one NOOP(T)
        ↓ (after the durable append — i.e. LogStore.Append's fsync, per the Write path's
        ↓  disk-before-memory ordering — actually succeeds; if it fails, fail closed per
        ↓  the storage-failure behavior, and leaderNoOpIndex remains 0)
    leaderNoOpIndex = that entry's index
        ↓
    (ordinary replicate → commit → apply path continues; once the no-op applies, readReadyTerm = T)
```
**No other code path may append another `NOOP` for this same leadership term, under any circumstance.** Any ensure/readiness retry path (e.g. something re-checking "is the leader ready to serve reads yet") must, before appending anything, check **all three** of:
```
role == Leader              (still leading — otherwise this logic doesn't apply at all)
currentTerm == T             (still the same term this leaderNoOpIndex was recorded for —
                              guards against a stale retry from a previous, already-ended
                              leadership term acting on this term's state)
leaderNoOpIndex != 0          (a no-op for this term has already been appended)
```
If all three hold, the retry path does **not** append another no-op — it operates on the existing `leaderNoOpIndex` (e.g. re-checking whether the entry at that index has committed/applied yet, to decide whether `readReadyTerm` can now be set). Only when `leaderNoOpIndex == 0` (this term's no-op genuinely hasn't been appended yet — the durable append either hasn't been attempted or previously failed) may the retry path attempt the append. **`leaderNoOpIndex` is leader-only volatile state, reset to `0` on every transition to Leader (i.e., every new leadership term) — it never carries a value across leadership terms or survives a restart, exactly like `readReadyTerm`.**
**This must be an explicit gate, not an implicit consequence of the ordinary read barrier — an earlier draft of this document got this wrong.** It is tempting to reason "any `GET` received before the no-op completes just blocks on the same read-barrier mechanism as an ordinary read, since `lastApplied` hasn't caught up yet" — **but that reasoning is false.** A fresh leader's `commitIndex` can start at `0` (or otherwise below whatever the previous leader last committed), so a `GET` arriving before the no-op completes captures `barrier.CommitIndex = 0` (or similarly low), and `lastApplied >= 0` is trivially already true — the barrier wait completes **immediately**, before the no-op has committed, and the read proceeds despite `commitIndex` not yet reflecting the previously committed entry. The read barrier alone cannot close this gap because it's defined relative to whatever `commitIndex` currently is, and `commitIndex` is exactly the volatile, not-yet-trustworthy value in question.

**The fix is a separate, explicit `readReadyTerm` leader-readiness marker** (Raft-mutex-protected leader-only volatile state, alongside `nextIndex[]`/`matchIndex[]`), checked as its own gate **before** a `GET` is allowed to even capture a `ReadBarrier`: if `readReadyTerm != currentTerm`, the read blocks/retries rather than falling through to the barrier wait. Full protocol and required test: `docs/client-semantics.md`'s Read protocol section.

**Why not ReadIndex instead:** a formal ReadIndex mechanism (confirming leadership via a heartbeat round *without* an extra log entry) is a legitimate alternative, but this project's PRD explicitly names ReadIndex as future work — the no-op-commit approach achieves the same correctness property using only mechanisms this project already has (log replication, the commit/apply path), at the cost of one extra committed no-op entry per leader election, which is negligible.

**Required tests:** (1) old leader commits a write and crashes; a new leader is elected; issue `GET` **immediately** after election (no intervening client write in the new term) — it must observe the old leader's committed write. (2) A new leader is elected; before its no-op has committed/applied, issue a `GET` — assert it does **not** complete (blocks/retries) until the no-op commits and applies, at which point it completes and returns correctly. Test (2) is the one that specifically catches an implementation (or documentation) that relies on the barrier wait alone rather than the explicit `readReadyTerm` gate — test (1) alone can pass even with the buggy "implicit" version in some timing-lucky cases, which is exactly why this document previously and incorrectly described the gate as unnecessary. See I-023.

## `AppendEntriesResponse`
```go
type AppendEntriesResponse struct {
    Term          uint64
    Success       bool
    ConflictIndex uint64  // meaningful only if Success == false
    ConflictTerm  uint64  // meaningful only if Success == false; 0 if the follower's
                            // log doesn't even contain prevLogIndex
}
```
**The per-follower `attemptID` used in "Stale response handling" above is deliberately NOT a wire field.** It's leader-local replicator bookkeeping — the leader already knows which attempt it's waiting on for a given follower without the follower needing to echo anything back; adding it to the wire protocol would be redundant. Keep it internal to the replicator/transport layer.

## `CorrelationID` is a wire field — distinct from, and not to be confused with, `attemptID`

**These are two different mechanisms serving two different purposes, and this project deliberately keeps them separate rather than reusing one for both:**
- **`attemptID`** (above): a **correctness** mechanism, internal to the leader's replicator, never transmitted, used to reject stale same-term responses (I-021).
- **`CorrelationID`**: an **observability/correlation** mechanism, transmitted on the wire, used purely so a `ClusterEvent` on one side of an RPC can be matched up with the corresponding `ClusterEvent` on the other side during telemetry analysis or scenario replay. It has no bearing on Raft correctness whatsoever — a node that ignored `CorrelationID` entirely would still be a perfectly correct Raft implementation; it exists solely to make the event stream more legible.

Concretely, both `AppendEntriesRequest` and `RequestVoteRequest` carry a sender-generated `CorrelationID` field (a fresh, opaque, code-generated string per RPC attempt — not a UUID requirement, just uniqueness for the sender's own lifetime is enough):
```go
type AppendEntriesRequest struct {
    // ... Term, LeaderID, PrevLogIndex, PrevLogTerm, Entries, LeaderCommit (standard Raft fields) ...
    CorrelationID string  // generated by the sender; echoed into whichever ClusterEvent(s) either
                           // side emits about this specific RPC attempt (e.g. the leader's own
                           // RPC_SUCCEEDED/RPC_FAILED, and the receiving follower's LOG_APPENDED/
                           // LOG_CONFLICT) — this is what makes cross-node correlation during
                           // telemetry analysis actually possible, since the receiver now has
                           // the same ID the sender generated, rather than needing to guess it
}
type RequestVoteRequest struct {
    // ... Term, CandidateID, LastLogIndex, LastLogTerm (standard Raft fields) ...
    CorrelationID string  // same purpose, for VOTE_GRANTED/VOTE_REJECTED correlation
}
```
The response types (`AppendEntriesResponse`/`RequestVoteResponse`) do **not** need their own `CorrelationID` field — the responder already received it on the request and can tag its own events with it directly; there's nothing for the response to additionally carry back. (This mirrors why `attemptID` doesn't need to be echoed back either, for the same underlying reason: the side that needs the value already has it.)

**Fast-backtrack algorithm on `Success == false`:** if the follower's log doesn't contain `prevLogIndex` at all, it returns `ConflictIndex = len(log)+1, ConflictTerm = 0`. If it contains an entry there with a different term, it returns `ConflictIndex = <first index in that conflicting term>, ConflictTerm = <that term>`. The leader: if it has an entry with `ConflictTerm`, sets `nextIndex = <index after the leader's last entry with ConflictTerm>`; otherwise `nextIndex = ConflictIndex`.

## Durable conflict truncation (`LogStore.TruncateFrom`)

```go
type LogStore interface {
    Append(entries []LogEntry) error   // durable (fsync) before returning
    TruncateFrom(index uint64) error   // durable before returning
    Get(index uint64) (LogEntry, error)
    LastIndex() uint64
}
```
**`Append` precondition — only accepts a contiguous suffix immediately following the current log:** `Append(entries)` is only valid when `entries[0].Index == LastIndex()+1` **and** every subsequent entry in the batch is contiguous with the one before it (`entries[i].Index == entries[i-1].Index + 1` for all `i > 0`) — `[6,7,8]` is a valid batch, `[6,8]` (an internal gap) is an error, even though `6` alone would have been a valid starting index. Concretely: with `LastIndex()==5`, `Append([6,7,8])` is valid; `Append([8])` (a gap before the batch even starts) is an error; `Append([5])` or anything starting at or below `LastIndex()` is an error — it must never silently overwrite. A caller that needs to replace existing entries must go through `TruncateFrom` first, explicitly. **`Append(nil)` / `Append([]LogEntry{})` — an empty batch — is a defined no-op that returns success without touching the log or performing any I/O**, rather than an error; this keeps call sites that compute a (possibly-empty) batch of new entries from needing a special case before calling `Append`.

**`TruncateFrom(index)` edge cases, defined exactly:**
```
index == LastIndex() + 1        → no-op (nothing to truncate)
index >  LastIndex() + 1        → error (would leave a gap; not a valid truncation point)
index <= commitIndex            → fatal invariant violation (I-011) — must be unreachable in
                                   correct code; halt/panic in test builds if reached
1 <= index <= LastIndex()       → truncate the log to keep entries [1, index-1], removing
                                   index..LastIndex()
index == 0                      → error (index 0 is the empty-log sentinel, never a real
                                   truncation target — see "Log indexing" below)
```
1. In-memory `index → byte-offset` map, rebuilt from the WAL replay scan on every startup.
2. `TruncateFrom(N)`: look up N's byte offset, `file.Truncate(offset)`, `file.Sync()` before returning. On error, fail closed (I-018).
3. Update in-memory `log[]`/offset map after fsync succeeds.
4. **Crash between `Truncate()` and fsync:** the WAL replay scan (below) validates records sequentially and stops at the first invalid one — a partially-truncated file recovers to whatever prefix is actually intact and checksummed. The required test for this doesn't assert "recovers to exactly the pre-truncation or exactly the post-truncation state" (either can legitimately happen depending on filesystem durability timing) — it asserts the **general safety properties**: after restart, the WAL is structurally valid, the recovered log is a valid prefix (no gaps, no corrupt trailing garbage), no *committed* entry was lost, and the node can reconcile fully through normal Raft replication once it rejoins. That's the real property that matters, not pinning down one specific byte-for-byte outcome.
5. **Only ever invoked for indices strictly above `commitIndex`** (I-011) — a call targeting `index <= commitIndex` is a fatal invariant violation.

## Recovery path

```
1. Read persisted currentTerm, votedFor from the TermVoteStore (§below — fails closed on
   corruption, per I-020, rather than guessing).
2. Replay the WAL to reconstruct the in-memory log[] AND the LogStore offset map.
   commitIndex is volatile and is never recovered from disk.
3. Start the node as a Follower, commitIndex = 0, lastApplied = 0.
4. Rejoin the cluster; wait for contact from the current leader.
5. The leader's AppendEntries carries leaderCommit — the AUTHORITATIVE source of commit progress.
6. Advance local commitIndex from leaderCommit (bounded by local log length).
7. ONLY NOW does the applier loop begin: apply entries lastApplied+1..commitIndex to the state
   machine, in order. The applier MUST NOT begin applying anything from the replayed WAL before
   step 6 has established commitIndex from the current leader — applying the raw replayed log
   without this gate would apply uncommitted entries, directly violating I-005. This ordering is
   a hard recovery requirement, not an implementation detail: replaying the WAL only reconstructs
   what entries EXIST (step 2); it must never be treated as authorization to apply them.
```
**Commit-advanced-then-crash-before-apply:** if `commitIndex` advanced on a node and the process then crashed before the applier ran for that advance, recovery's step 3 resets `commitIndex` to 0 regardless (it was volatile, never persisted) and step 5/6 re-establishes it correctly — demonstrating the durable-log/volatile-commitIndex separation holds under this specific timing (Phase 4 required test).

**One consequence of this ordering worth stating explicitly, since it can otherwise look like a bug to an implementer:** immediately after step 2 (WAL replay) and before step 6 (leaderCommit re-establishes `commitIndex`), a restarted node can have a perfectly valid, fully-replayed WAL containing entries it (or the previous leader) had already committed — yet its serving KV state is still whatever it was left at before the crash, not yet reflecting those entries. This is intentional, not a recovery bug to "optimize away": **until the restarted node receives authoritative `leaderCommit` information from a current leader, its replayed WAL is treated only as durable log *history* (what entries exist, for replication purposes) and is not reflected into the serving state machine.** Do not be tempted to "fast-path" recovery by applying the replayed WAL directly into `KV` based on the log's own contents — the log alone cannot tell a recovering node which of its own entries were actually committed cluster-wide (that authority belongs only to `leaderCommit`, per I-006), and applying speculatively here would risk exposing an uncommitted (and potentially about-to-be-truncated) entry through `GET`, directly violating I-005.

**Known limitation:** without snapshots, recovery time is O(number of committed log entries). See Snapshot Design below. Data acknowledged after a successful fsync survives a crash, **subject to the underlying filesystem/storage stack's own durability guarantees**.

## `TermVoteStore` — atomicity and corruption contract (I-020, I-024)

```go
type TermVoteStore interface {
    Save(term uint64, votedFor string, bootID uint64) error  // fsync before returning
    Load() (term uint64, votedFor string, bootID uint64, err error)
}
```
Persisted as a single checksummed record, same shape as a WAL record (§below): `[length: uint32 LE][payload: protobuf TermVoteRecord][crc32: uint32 LE]`, where
```protobuf
message TermVoteRecord {
    uint64 current_term = 1;
    string voted_for   = 2;
    uint64 boot_id      = 3;  // incremented once per process start; feeds EventID uniqueness (above)
}
```
`currentTerm` and `votedFor` are always written **together, as one atomic record** — there is no way to durably update one without the other, which eliminates an entire class of "term persisted but vote wasn't" partial-update bugs.

**Ordering requirement for self-votes (I-012):** on starting an election, a node persists `{currentTerm+1, votedFor=self}` via `Save()` **before sending any `RequestVote` RPC to peers** — not merely before responding to incoming RPCs. `currentTerm++; votedFor = self; persist; THEN send RequestVote` — never the reverse. Sending votes before the fsync completes and then crashing would let the node vote again in the same term on restart, a direct I-001 risk.

**`Save()`'s replacement mechanism must itself be crash-atomic (I-024) — `fsync` after an in-place overwrite is not sufficient,** because an in-place write can be torn by a crash mid-write. The exact mechanism:
```
write new record to termvote.tmp (same directory as the real record)
        ↓
fsync(termvote.tmp)
        ↓
rename(termvote.tmp, termvote)          — atomic on POSIX filesystems
        ↓
fsync(parent directory)                  — makes the rename itself durable
```
After a crash at any point in this sequence, recovery observes either the complete **old** record (crash before or during the temp-file write/fsync, or before the rename lands) or the complete **new** record (crash after the rename, whether or not the directory fsync completed) — never an arbitrary partially-overwritten record. (A two-slot journal is an alternative; temp+fsync+rename+directory-fsync is the simpler one to explain and implement for this project's scope, so it's the one specified here.)

**Orphan `termvote.tmp` handling on recovery — specified explicitly, since the mechanism above can legitimately leave a stray temp file behind:** a crash after `fsync(termvote.tmp)` but before `rename()` completes leaves both `termvote` (the old, still-valid canonical record) and `termvote.tmp` (a complete, checksum-valid, but never-promoted new record) present on disk simultaneously. `Load()`'s handling of this, frozen:
- **`termvote` (the canonical file) is always authoritative when it is present and valid.** `Load()` never inspects, compares, or promotes `termvote.tmp` in this case — only `Save()`'s own rename step is ever allowed to promote a temp file to canonical. A `termvote.tmp` found alongside a valid `termvote` is treated purely as debris from an interrupted `Save()` that never completed; `Load()` may delete it (best-effort cleanup, not required for correctness) but must never read values out of it. **This is a deliberate safety choice, not an oversight:** automatically promoting an orphaned temp file during `Load()` would mean a write that was never confirmed complete (the crash happened before its `rename()`) could still end up governing the node's behavior — indistinguishable, from the node's own perspective, from having actually completed that `Save()` call. Only `Save()`'s own atomic `rename()` may ever make a temp file's contents authoritative.
- **`termvote` missing, `termvote.tmp` present:** this is an **invalid/corrupt startup state**, handled identically to a corrupted canonical record (fatal, refuse to start) — never a state `Load()` attempts to recover from by promoting the temp file. This state should only be reachable via manual tampering with the data directory (the documented `Save()`/first-boot sequence never removes `termvote` without `termvote.tmp` having already been successfully renamed over it) — but `Load()` still must not guess in the face of it.

**On startup, `Load()`:**
- **No record exists yet (first-ever boot):** this is a legitimate, non-error initial state — but it must not merely be *returned* as `term=0, votedFor="", bootID=1` in memory. `Load()` must **synthesize `{term=0, votedFor="", bootID=1}` and durably persist it via the same atomic `Save()` mechanism above before returning**. If that persistence fails, startup fails (same as any other durable-write failure, I-018). **Why this matters:** without immediate persistence, a first boot returns `bootID=1` from memory, the node starts, and if it restarts before anything else ever calls `Save()` (e.g. before its first election), the second boot again finds no record and again returns `bootID=1` — a silent `bootID` collision that breaks `EventID` uniqueness across the node's lifetime (`ClusterEvent` schema, above). Persisting immediately on first boot closes this gap: the *next* boot loads the persisted record, increments `bootID` to 2, and re-saves.
- **Record exists and passes its checksum:** return it, with `bootID` incremented and immediately re-saved (so the *next* boot gets a fresh `bootID`).
- **Record exists but is truncated or fails its checksum:** **return an error. `Node.Recover()` treats this as fatal — the node refuses to start.** Do not guess a term/vote pair, do not fall back to `term=0`, do not attempt partial recovery of a corrupt record. Guessing wrong here can directly violate Election Safety (I-001) in a way that's far worse than a node simply failing to start.
- **Record checksum-valid but semantically invalid:** a checksum only proves the bytes weren't corrupted in transit/on-disk — it says nothing about whether the *values* those bytes decode to make sense. `Load()` additionally validates, after checksum verification succeeds, and treats a failure of any of these the same as a checksum failure (fatal, refuse to start):
  - `bootID >= 1` — `bootID == 0` is invalid for any record that was ever actually persisted post-initialization (the first-boot path above always persists `bootID=1`, never `0`; a `0` here means something wrote a record without going through the documented first-boot path).
  - `votedFor == "" OR votedFor is one of the configured cluster NodeIDs` — an arbitrary non-empty string that matches no configured peer is invalid (it can't correspond to any vote this node could actually have legitimately cast).
  - the record decodes to exactly one well-formed `TermVoteRecord` — no trailing bytes after the checksum, no ability to interpret the payload as more than one record.
  - **Deliberately not validated as an error condition:** `currentTerm == 0` with a non-empty `votedFor`, or `currentTerm > 0` with an empty `votedFor` — both are legitimate reachable states (term 0 with no vote cast yet is the initial state before any election in this cluster's history; a node can persist a higher term it *learned of* via an RPC without having voted in that specific term). Only the two checks above (bootID, votedFor-matches-a-configured-node) are enforced; this project deliberately does not attempt to validate term/vote consistency beyond that, since a broader semantic model would need more cluster-configuration context than `TermVoteStore` alone has access to.

## Concurrency model

Single mutex per node owns all mutable Raft state.

| Field | Owner | Lock required? |
|---|---|---|
| `currentTerm` | Raft state | Yes |
| `votedFor` | Raft state | Yes |
| `log[]` | Raft state | Yes |
| `commitIndex` | Raft state | Yes |
| `lastApplied` | Raft state | Yes |
| `role` | Raft state | Yes |
| `nextIndex[]` / `matchIndex[]` | Leader-only volatile | Yes |
| `readReadyTerm` | Leader-only volatile (I-023) | Yes — same Raft mutex as the rest of this table |
| `leaderNoOpIndex` | Leader-only volatile (I-023) | Yes — same Raft mutex as the rest of this table |

**A second, distinct mutex owns the state machine (KV + `RequestTable`, `docs/client-semantics.md`) — concretely a `sync.RWMutex`, not a plain `sync.Mutex`, since `GET` only needs read access while `Apply()` needs exclusive write access:**
```go
type StateMachine struct {
    sync.RWMutex               // guards the field below — embedded so callers write
                                // sm.RLock()/sm.RUnlock()/sm.Lock()/sm.Unlock() directly
    state StateMachineState    // KV + RequestTable — the actual guarded data
}
// GET (read path):    sm.RLock()  / sm.RUnlock()
// Apply (write path): sm.Lock()   / sm.Unlock()
```
The Raft mutex and the StateMachine mutex are two separate locks with two separate purposes: the Raft mutex protects consensus bookkeeping (above); the StateMachine mutex protects `KV`/`RequestTable`, which are written by the applier (`Apply()`, taking the write lock) and read by `GET` (`Get()`, taking the read lock). Both the applier and the read path must hold the appropriate side of the StateMachine `RWMutex` for their respective operation. (Multiple concurrent `GET`s may hold the read lock simultaneously — `RWMutex` is chosen specifically to allow this, rather than serializing reads against each other unnecessarily; only a `GET` versus an in-progress `Apply()` needs to be mutually exclusive.)

**Lock ordering (hard rule, must never be reversed): Raft mutex is acquired before StateMachine mutex, never the other way around.** Concretely:
```
Raft mutex
    ↓
StateMachine mutex
```
A code path that acquires StateMachine mutex → Raft mutex (the reverse order) can deadlock against a path that acquires them in the documented order — e.g. the applier (holds Raft mutex while advancing `lastApplied`, then needs StateMachine mutex to call `Apply()`) versus a hypothetical read path that acquired StateMachine mutex first and then tried to re-enter the Raft mutex to revalidate term/role. **The linearizable read path (`docs/client-semantics.md`) is the specific place this matters most:** term/role revalidation (Raft mutex) and the KV read itself (StateMachine mutex) must be performed as one synchronized sequence in the documented order, with the StateMachine mutex held across the revalidation-to-read handoff so no write can land in the gap — see I-016 and `docs/client-semantics.md`'s read protocol for the precise sequencing. This is the mechanism that makes I-016's declared linearization point actually true, not just a comment claiming it is.

**Hard rule (I-014): no network I/O — direct or indirect — while holding the Raft mutex.**

**Disk I/O under the mutex** — intentional MVP tradeoff, bounds single-node throughput by disk latency, measured in Phase 10.

**Election timer ownership:** a single election-loop goroutine owns the actual timer.

**Heartbeat/election-timeout relationship, with frozen values:** the leader's heartbeat interval must be strictly less than the minimum of the randomized election-timeout range — `heartbeat_interval < min(election_timeout_range)`. If this doesn't hold (e.g. a 200ms heartbeat against a 150ms minimum election timeout), followers can spuriously start elections between heartbeats even with a healthy leader. **Frozen MVP values, satisfying that inequality with a comfortable margin: `heartbeat_interval = 50ms`, `election_timeout` randomized uniformly in `[250ms, 400ms]` per election attempt.** These are not architecturally load-bearing — any values satisfying the inequality above (with enough margin to absorb realistic scheduling/network jitter in this project's test environment) would work — but freezing specific numbers here, rather than leaving them as a Phase 1 implementation detail, exists specifically so that an AI coding tool implementing this project doesn't independently pick different values across files, and so deterministic tests have a fixed, reproducible timing baseline to reason about.

**`commitIndex`/`lastApplied` monotonicity (I-022):** both are monotonic non-decreasing during a node's continuous uptime. The sole exception is the reset to 0 at the start of the recovery sequence (step 3, above) after a crash-restart — that's volatile-state re-initialization, not a violation, and happens strictly before the node rejoins normal operation.

**Metrics/observability must never acquire the Raft mutex.** Event-recording failures are dropped (with `observability_events_dropped_total` incremented) rather than propagated; a full **live debug ring buffer** (bounded, lossy by design — distinct from the complete per-scenario recorder used for chaos-fixture capture, `docs/phases/phase-07.md`) **drops the oldest event to make room for the newest** (frozen policy — the buffer represents recent operational history, so keeping the newest and discarding the oldest is the only choice consistent with that purpose; this project's own no-unresolved-ambiguity rule means "implementation's choice" is not an acceptable answer here) and increments `observability_ring_buffer_overflows_total` rather than blocking the emitter.

### Goroutine ownership table

| Goroutine | Owns | Cancellation path |
|---|---|---|
| Election loop | the election timer/deadline | context cancellation on `Node.Stop()` |
| Replicator (one per follower) | that follower's outstanding AppendEntries RPC (at most one in flight, §above) | context cancellation on `Node.Stop()` |
| Applier | applying committed entries, advancing `lastApplied` | context cancellation on `Node.Stop()`, drains any in-flight apply before exiting |
| gRPC server | inbound RPC handlers | `grpcServer.GracefulStop()` |
| Observability worker | event ingestion/ring-buffer management | context cancellation, drops (doesn't block) in-flight events on shutdown |
| AI worker | rule engine / LLM analysis requests | context cancellation; in-flight LLM calls are allowed to be abandoned (fire-and-forget), never blocking shutdown |

**Every goroutine has an explicit owner and a cancellation path** — this table is the reference for "who's responsible for this goroutine" during Phase 10's soak-test goroutine-leak checking.

## Graceful shutdown contract (`Node.Stop()`)
**`Stop()` is idempotent** — calling it more than once (or calling it on a node already in the `StorageFailed` state above) must not panic, double-close resources, or race; a second call is a no-op once the first has completed (or is itself a no-op if shutdown is already in progress). A failed (`StorageFailed`) node may always be safely `Stop()`'d — this matters concretely for chaos harnesses that need a uniform "shut this node down" call regardless of why the node is no longer healthy.
```
1. stop accepting new client writes (return NOT_LEADER-style rejection or similar to new requests)
2. stop the election loop
3. stop replication loops (allow in-flight RPCs to finish or time out, don't start new ones)
4. drain/abandon pending RPCs per the goroutine table above
5. fail/abandon any still-registered PendingWrite waiters (§above) rather than leaving them
   registered past shutdown
6. flush/close storage (WAL, TermVoteStore) cleanly
7. close the gRPC server (GracefulStop)
8. emit a NODE_STOPPED event if the observability path is still reachable; don't block shutdown on it
```
This matters concretely for Phase 10's soak test, where goroutine leaks across many start/stop cycles would otherwise be easy to miss.

## Determinism: injectable Clock and Transport, plus the deterministic simulator

```go
type Clock interface {
    Now() time.Time
    NewTimer(d time.Duration) Timer
}
type Timer interface {
    C() <-chan time.Time
    Stop() bool
    Reset(d time.Duration) bool
}

type Transport interface {
    SendRequestVote(ctx context.Context, peer string, req *RequestVoteRequest) (*RequestVoteResponse, error)
    SendAppendEntries(ctx context.Context, peer string, req *AppendEntriesRequest) (*AppendEntriesResponse, error)
}
```
Core election/commit safety tests **must** use the single-threaded deterministic simulator (full specification, including fake-timer `Stop()`/`Reset()` semantics and the required phantom-timer-doesn't-fire test, in `docs/testing.md`). **Explicit caveat: deterministic simulation validates protocol state-machine behavior — it does not replace real concurrent integration testing.** Passing every Level 1–3 deterministic test proves the protocol logic is correct under controlled event ordering; it does not by itself prove the absence of mutex races, goroutine deadlocks, or concurrent-RPC-handler bugs, which is exactly why Level 4 (real processes, real scheduling) and `-race` remain required, not optional once the simulator exists.

## RPC timeout and cancellation semantics
`Transport` calls carry a `context.Context` with a bounded timeout. On timeout: the call returns an error, no Raft state is mutated based on a "successful response" that never arrived, and the caller (replication loop or election logic) proceeds per its own retry/backoff logic — a timed-out RequestVote simply doesn't count as a granted vote; a timed-out AppendEntries is handled identically to any other failure per "Stale response handling" above (i.e., mostly: try again later, subject to the at-most-one-in-flight rule).

## RequestVote log-freshness formula
```
grant vote IF:
    candidate.lastLogTerm > voter.lastLogTerm
    OR (candidate.lastLogTerm == voter.lastLogTerm
        AND candidate.lastLogIndex >= voter.lastLogIndex)
```
This is the mechanism that *enforces* I-004 (Leader Completeness) — it is not itself I-004; see `docs/invariants.md`'s note on this distinction. Applies identically to empty logs (`lastLogIndex=0, lastLogTerm=0` for every node) — no special case.

## WAL record format
```
[length: uint32, little-endian][payload: bytes][crc32: uint32, little-endian]
```
`length` = byte length of `payload` only. `payload` = protobuf-encoded `WALRecord`. `crc32` = `hash/crc32.ChecksumIEEE(payload)`. **`MAX_WAL_RECORD_SIZE` = 1 MiB** — validated before allocating a read buffer, so a corrupted length field (e.g. `0xFFFFFFFF`) is rejected the same way a checksum failure is, rather than attempted as a huge allocation.

**Recovery scan:** sequential scan from offset 0; on any failure (EOF mid-read, checksum mismatch, oversized length) — stop, truncate the WAL at the last valid record's end offset. **Two distinct failure shapes reach this same stop-and-truncate behavior, and it's worth naming both explicitly rather than treating "any failure" as one undifferentiated case:**
- **Expected torn tail** — a valid record, valid record, ..., then `EOF` mid-read on what would have been the *next* record (the process crashed mid-write of that last record, before its length/payload/checksum fully landed). This is the routine, expected shape of an unclean shutdown: some suffix of the last in-flight write simply never finished.
- **Mid-log corruption** — valid record, valid record, then a checksum mismatch or oversized length on a record that is *not* the last one physically present in the file (there are more, possibly-valid-looking bytes after it). This shouldn't occur under a correctly implemented `Append`/fsync/rename discipline (`docs/architecture.md`'s Write path) — it would indicate on-disk bit rot, a filesystem-level bug, or a logic error that violated the write-path ordering. This project's MVP behavior treats it identically to a torn tail: **stop and discard everything from the corrupt record onward**, even though there might be recoverable-looking valid records physically further into the file. This is a deliberate, stated scope decision, not an oversight: attempting to skip past a corrupt record and recover a later "valid" one risks silently reconstructing a log with a *gap*, which is far more dangerous than conservatively truncating to the last-known-good prefix and letting Raft's own replication reconstruct the (potentially larger) missing/uncommitted suffix from a majority of the other nodes — which is exactly the mechanism Raft already has for this. **State this explicitly, so it can't be silently reinterpreted as a bug later:** *a corrupt record encountered after previously valid records is treated as the end of the recoverable WAL prefix, regardless of whether it represents a torn tail or genuine mid-log corruption; subsequent bytes are discarded and the node relies on Raft to reconstruct the missing/uncommitted suffix, not on any attempt at partial in-file recovery.*

**Required test cases:** empty WAL, one record, many records, truncated final record, corrupt checksum, corrupt/oversized length prefix, crash between append and fsync, crash immediately after `TruncateFrom`.

## Snapshot design (documented, not implemented)
```go
type Snapshot struct {
    LastIncludedIndex uint64
    LastIncludedTerm  uint64
    StateMachineState []byte // serialized StateMachineState (KV + RequestTable)
}
```
Fixes log/recovery-time growth. **Does not by itself shrink `RequestTable`** (`docs/client-semantics.md`). **Snapshotting is future work and is not implemented in the Phase 0–10 MVP scope.** When it is eventually implemented, it requires redefining log-index storage around `LastIncludedIndex` — once a snapshot exists, the log no longer starts at index 1 (§"Log indexing" above), and every place that currently assumes "index 1 is the first real entry" (recovery, `TruncateFrom` bounds, `nextIndex` initialization) would need to account for a `LastIncludedIndex` offset instead. The current MVP assumes the full log begins at index 1 throughout; this is a real, stated scope boundary, not an oversight.

## Quorum math
`⌊N/2⌋ + 1`. 3 nodes → quorum 2. 5 nodes → quorum 3.
