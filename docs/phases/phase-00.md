# Phase 0 — Foundation + Single-Node KV Store

## Goal
Stand up the repo, config, and a trivial single-node KV store so every later phase has an API surface and test harness. No Raft yet.

## Scope
- Repo scaffold per `docs/architecture.md`'s package layout.
- In-memory `GET/SET/DELETE` served over gRPC on one process.
- Static cluster config file (node IDs + addresses), unused by consensus logic yet.
- Structured logging (`slog` or equivalent) wired from day one.
- CLI (`raftkv-cli get/set/delete`).
- **`GET` is a local `StateMachine` read, not a Raft log command, even conceptually — this seam must be right from Phase 0, since it's the exact boundary Phase 3 later wraps in the quorum-confirmation/read-barrier protocol.** See "Interfaces" below.

## Non-goals
No Raft, no persistence, no replication, no AI, no chaos. Do not build anything from later phases even if convenient.

## Files allowed to change
`cmd/raftkv-node/`, `cmd/raftkv-cli/`, `internal/storage/kv_statemachine.go`, `internal/cluster/config.go`, `proto/client.proto`.

## Interfaces
```go
type StateMachine interface {
    Apply(cmd Command) (result CommandResult, err error)
    Get(key string) (value []byte, found bool)
}
```
**`GET`/`Get(key)` is explicitly NOT a Raft log command, from this phase forward — this is a permanent property of the design, not a Phase 0 simplification later replaced:**
- `Get(key)` does **not** call `Apply()`.
- `Get(key)` does **not** create a log entry.
- `Get(key)` does **not** participate in Raft replication in any way — it is a direct, local read of this node's own in-memory `KV` state.
- In Phase 0 (no Raft yet), `Get(key)` is trivially linearizable because there's only one node and no concurrent writers besides the local client handler.
- **Starting in Phase 3, this same `Get(key)` call becomes the last step of the quorum-confirmed linearizable read protocol** (`docs/client-semantics.md`'s Read protocol, I-016/I-023) — Phase 3 wraps this unchanged local read with quorum leadership confirmation, the read-barrier wait, and term/role revalidation under the StateMachine lock; it does not change what `Get(key)` itself does or turn it into a replicated operation. The seam Phase 0 establishes here (a plain local read, separate from `Apply`) is exactly what lets Phase 3 add linearizability *around* the read without modifying the read itself.
Designed now so Phase 3 onward plugs Raft-driven application into `Apply()`, and Raft-driven read-safety around `Get()`, instead of the client handler calling the map directly for either.

## State changes
Introduces `Command` (see `docs/client-semantics.md` for the field set this will grow into) and `ClusterConfig{NodeID, Peers []PeerAddr}`.

## Invariants affected
None yet — this phase predates Raft.

## Tests required
Unit tests for `StateMachine` (get/set/delete/overwrite/delete-missing-key). CLI smoke test against a running server.

## Failure cases to handle
Concurrent map access (must be safe — use a mutex even though Raft's mutex doesn't exist yet). Distinguish "key not found" from "empty value" in the API.

## Acceptance criteria
Single node serves GET/SET/DELETE correctly over gRPC. CLI round-trips work. `go test ./...` green. `go vet`/lint clean.

## Interview concepts
Why separate the state-machine interface from the transport layer at all — it's exactly the seam Raft plugs into later. **Why `Get` and `Apply` are two separate methods on `StateMachine`, not one** — `Apply` is the only thing Raft ever drives (every write goes through consensus); `Get` is a plain local read that Phase 3 later wraps in an external protocol (quorum confirmation, read barrier, revalidation) without changing the read itself — the interface shape decided in Phase 0 is what makes that later addition non-invasive.

## Exit criteria (Definition of Done)
- [ ] proto compiles
- [ ] server runs
- [ ] CLI works
- [ ] unit tests pass
- [ ] structured logging present on every request path
