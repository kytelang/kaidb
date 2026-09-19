# Design: PROMOTE / DEMOTE (runtime role transition)

Status: design, not yet implemented. Goal: flip a live node's replication role over
SQL so a follower can be promoted to primary (planned or on primary failure) and an old
primary can be demoted to follow the new one, without a restart and without split-brain.
Written against `main` (`src/schema/database.zig`, `src/query/query_executor.zig`,
`src/query/replication.zig`).

## 1. What already exists (the primitives)

- **Write admission is the fence epoch, nothing else.** `guardWrite` (`database.zig:2437`)
  rejects a write iff `fencing_epoch < max_epoch_seen`; it is called on every write
  (`query_executor.zig:3050`). So a node is "writable" exactly when
  `fencing_epoch == max_epoch_seen` (its own epoch is the highest it has seen).
- `setWriteEpoch(e)` (`:2449`): sets `fencing_epoch = e`, and if `e > max_epoch_seen`
  raises + **persists** `max_epoch_seen` (`fence_epoch.json`). Grants write rights at `e`.
- `observeEpoch(e)` (`:2463`): raises + persists `max_epoch_seen` **without** granting
  writes. This is how a following node fences itself below a newer leader.
- `becomeFollower(host,port,wal_dir,auth_key,tls)` (`:2662`): stands up a `Follower` +
  `ReplServer` listener that applies the leader's stream.
- `becomeDurableLeader(...)` (`:2687`): stands up a `DurableReplicator` and wires
  `wal.ship_callback`.
- `stopFollower()` (`:existing`, used by `close`): stops the `ReplServer` and tears down
  the follower cleanly at runtime. **`SET FENCE EPOCH <n>`** is already an admin-gated
  runtime SQL command (`query_executor.zig:2876`), proving the fence can be moved live.

Missing: (a) a symmetric `stopDurableLeader()`; (b) the two role-transition orchestrations;
(c) the SQL surface.

## 2. The safety property and why it holds

**Invariant to preserve:** at most one node in a replica set is writable at any epoch, and a
node that was writable at epoch `e` can never commit a client write again once another node
is writable at epoch `> e`.

The fence epoch already enforces this monotonically:
- A promoted node writes at a **strictly greater** epoch than the old leader ever used.
- The old leader, the instant it observes/ships at (or is told about) that higher epoch,
  has `max_epoch_seen > fencing_epoch` and every subsequent `guardWrite` fails.

**Key convenience:** a follower has been calling `observeEpoch` as it applied the leader's
stream, so its persisted `max_epoch_seen` already equals the old leader's epoch. Promotion
can therefore pick `max_epoch_seen + 1` locally — strictly greater, no external epoch
coordination needed, and durably persisted before the first write is admitted.

## 3. New Database methods

```
stopDurableLeader(self):           // symmetric to stopFollower
    if wal: wal.ship_callback = null; wal.replication_manager = null;
    if durable_repl: dr.deinit(); destroy; durable_repl = null;

promote(self):                     // follower/standby -> writable primary
    self.stopFollower();                       // stop applying any leader stream (no-op if none)
    const e = self.max_epoch_seen + 1;         // strictly greater than any epoch seen
    try self.setWriteEpoch(e);                 // persists max_epoch_seen=e BEFORE writes admitted
    // (optional, deferred) becomeDurableLeader(...) to ship to remaining replicas

demote(self, host, port, auth_key, tls):       // primary -> follower of a new leader
    // 1. fence self so NEW client writes are rejected immediately
    if self.fencing_epoch == self.max_epoch_seen:
        try self.observeEpoch(self.max_epoch_seen + 1);   // now fencing_epoch < max_epoch_seen
    // 2. stop shipping if we were a leader
    self.stopDurableLeader();
    // 3. start following the new primary (it will observeEpoch >= our fence as it applies)
    try self.becomeFollower(host, port, self.fenceDir(), auth_key, tls);
```

Ordering is the whole game:
- **promote**: stop-follow → bump+persist epoch → (only then) writable. Persist-before-admit
  is what makes a crash mid-promote safe (on restart the higher `max_epoch_seen` is already
  on disk).
- **demote**: fence-first (reject new writes) → stop shipping → become follower. Fencing
  before following means there is never a window where the node both accepts client writes
  and applies a leader's stream.

## 4. SQL surface (admin-gated, mirrors `SET FENCE EPOCH`)

Handled as string-prefix commands in `executeWrapped` (like `SET FENCE EPOCH` /
`SET DURABLE COMMIT`), each behind `adminGate`:

- `PROMOTE` — promote this node to a writable primary at a fresh epoch. Returns the new
  epoch in the status.
- `DEMOTE TO FOLLOWER OF 'host:port'` — fence, stop shipping, follow the given primary.

Rationale for keeping it as admin string-commands rather than parsed AST: it matches the
existing control-plane verbs, needs no grammar changes, and these are operator/orchestrator
commands, not query-language surface.

Preconditions / idempotency:
- `PROMOTE` on a node that is already a non-following writable primary: still valid — it
  bumps to a higher epoch (fencing any stale leader). Harmless and idempotent-ish.
- `DEMOTE` requires a target address; demoting a node that is already following re-points it
  (stopFollower inside becomeFollower path) — or reject with "already a follower of X".

## 5. The honest data-loss window (must document, not hide)

With **async** replication, a promote after primary failure can lose writes that were
committed on the old primary but not yet shipped. This is identical to async failover in
Postgres/MySQL and is fundamental, not a kaidb defect. Two mitigations, both already present:
- **Zero-loss:** run synchronous replication (`SET DURABLE COMMIT ON` / `await_quorum`) so a
  client commit implies the replica already has it; then a promote loses nothing acknowledged.
- **Bounded-loss:** watch `kaidb_replication_lag_frames` (added this session) and only
  auto-promote when lag is 0 or below a threshold.

`DEMOTE` of a *live* old primary has a subtler case: writes that committed locally after the
new leader was promoted but before this node fenced. The fence-first ordering shrinks this to
"in-flight transactions already past `guardWrite`"; those drain and are **not** shipped to the
new leader, so they are lost on the demoted node's side (it will be overwritten by the new
leader's stream on `becomeFollower` resync). This is correct for failover (the new leader is
authoritative) but means a demoted-then-followed node must accept the new leader's history.
Implementation should force a snapshot/resync path on demote rather than assume the local WAL
can fast-forward.

## 6. Test plan (red -> green)

1. **Split-brain fence (the core safety test).** Leader L at epoch 1 ships to follower F.
   `PROMOTE` F (-> epoch 2). Assert: (a) F now accepts a write; (b) L's next write returns
   `FencedWrite` once L observes epoch 2 (ship a heartbeat / set fence). Mirrors the existing
   `P3 SET FENCE EPOCH` test.
2. **Promote persists before admit.** After `PROMOTE`, read back `fence_epoch.json`; assert
   `max_epoch_seen` was raised on disk before the first post-promote commit.
3. **Demote fences then follows.** Primary P at epoch 1; `DEMOTE TO FOLLOWER OF ...`; assert a
   client write on P is now rejected, and P applies the new leader's subsequent writes.
4. **Round-trip.** promote F, demote old L to follow F, write on F, read-back on L after apply.

## 7. Scope / non-goals for v1

- **No automatic failure detection or auto-promote.** These commands are the *mechanism*; the
  *policy* (who decides, when) belongs in the orchestrator (which already runs HA leases). A
  human or the orchestrator issues `PROMOTE`/`DEMOTE`.
- **No automatic client re-routing.** Same as read-routing: a `proxyd`/driver concern.
- **Single-replica model** (the chosen deployment): promote the one replica, optionally demote
  the recovered old primary to follow it. Multi-replica promotion (electing among N) is out of
  scope and would want a real election, not manual verbs.

## 8. Risk register

- **Data-safety-adjacent:** this drives leader/follower transitions + the fence epoch. Every
  step must be behind the red->green split-brain test before merge; no "looks right" merges.
- **Teardown races:** `stopFollower`/`stopDurableLeader` must fully quiesce the listener /
  ship callback before the role flips, or a stray applied batch / shipped frame crosses the
  transition. `ReplServer.stop` + nulling `wal.ship_callback` are the existing seams; verify
  they are synchronous w.r.t. the transition.
- **Crash mid-transition:** promote persists the epoch before admitting writes (safe);
  demote should persist the raised fence before it stops shipping (so a crash leaves it
  fenced, not a silent writable primary).
