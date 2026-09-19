# Design: N-replica read-scaling fan-out

Status: design, not yet implemented. Target: let one primary ship its WAL stream to
several followers so reads can be spread across a replica set. Written against the
replication code on `main` (`src/query/replication.zig`, `src/schema/database.zig`,
`src/main.zig`).

## 1. Where it is single-follower today

A follower is already a full, queryable read-only copy (`Database.becomeFollower` +
`Follower` + `ReplServer`), so the *read-replica* concept exists. The limit is purely on
the primary's shipping side: `DurableReplicator` bundles exactly one transport.

- `DurableReplicator` holds one `client: ReplClient`, one `follower_id: u64 = 1`, one
  `host`/`port`, one `connected` flag (`replication.zig:1848`-`1877`).
- `connect` remembers a single peer (`:1971`); `shipPending` ships each batch to that one
  client (`:2189`); `reconnectAndCatchUp` recovers that one client (`:1997`).
- `becomeDurableLeader(host, port, total_replicas, ...)` takes a single `host:port`
  (`database.zig:2687`), wired from a single `config.replica.{address,port}` (`main.zig:729`).

## 2. The invariant that makes fan-out cheap

The replication sequence stream is **one contiguous per-primary counter**. `next_seq`, the
in-RAM `sent` ring, and the on-disk `BackfillLog` all describe that single stream. Every
follower consumes the *same* stream from its own cursor; a follower's position is just a
`confirmed_seq`. So:

- **Shared, stays as-is:** `next_seq`, `sent` ring, `backfill`, `epoch`, and the
  `QuorumTracker` (already keyed by `follower_id`, already does `minConfirmed` over a
  follower count — `:1799`).
- **Per-follower, needs to become a set:** the transport (`client`), `follower_id`,
  `host`/`port`, and `connected`.

This is why fan-out is a *transport* refactor, not a change to the durability/seq model.

## 3. Data model

Extract the per-follower transport into a `FollowerLink`:

```
const FollowerLink = struct {
    id: u64,                 // stable follower id, key into QuorumTracker
    host: []const u8,        // owned
    port: u16,
    client: ReplClient,
    connected: bool = false,
};
```

`DurableReplicator` keeps all the shared stream state and replaces the single-transport
fields with `links: std.ArrayList(FollowerLink)`. `follower_id`/`host`/`port`/`client`/
`connected` are removed from the struct (moved into links).

Backward compatibility: keep a one-arg `connect(host, port)` that appends a single link
with `id = 1`, so existing call sites and the single-replica config path are unchanged.
Add `addFollower(id, host, port)`.

## 4. shipPending: assign once, ship to all

The seq assignment and history retention happen **once** (shared stream), then the batch is
shipped to every connected link independently. Pseudocode replacing `replication.zig:2166`:

```
// unchanged: assign seq, retain in ring + backfill
const base = self.next_seq;
const last = base + self.buf.items.len - 1;
self.next_seq = last + 1;
try self.retain(base, last);
if (self.backfill) |bl| bl.append(base, last, self.buf.items) catch { ...ring-only... };

const frames = proto.ReplFrames{ .epoch = self.epoch, .base_seq = base, .frames = self.buf.items };
for (self.links.items) |*link| {
    if (!link.connected) {
        self.reconnectAndCatchUpLink(link) catch |err| {
            if (await_quorum) { /* remember, don't early-return: another link may satisfy quorum */ }
            log.warn("replica {d}: reconnect failed: {any}; will catch up later", .{ link.id, err });
            continue;
        };
    } else {
        _ = link.client.shipAndRecord(frames, &self.tracker, link.id) catch |err| {
            link.connected = false;
            log.warn("replica {d}: ship failed: {any}; committed locally, will catch up", .{ link.id, err });
            continue;
        };
    }
}
if (await_quorum) try self.tracker.awaitQuorum(self.io, last, self.timeout_ms);
self.checkpointBackfill();
```

Key semantic choices:
- **A slow/dead replica never blocks the others.** Each link ships independently; a failure
  marks *that* link disconnected and continues. This is what makes adding read replicas
  safe: they cannot degrade the primary or each other.
- **Quorum is evaluated once, globally**, after shipping to all links, via the existing
  `awaitQuorum(last)`. Because `total_replicas` now reflects the real fleet, `quorum()` =
  `floor(N/2)+1` including the primary is already correct. In async mode (`await_quorum =
  false`, the read-scaling default) quorum is not awaited at all — exactly today's behaviour.
- The `sent` ring / backfill are appended once regardless of link count, so history depth is
  unchanged.

## 5. Per-link catch-up

`reconnectAndCatchUp` (`:1997`) becomes `reconnectAndCatchUpLink(link)` operating on one
link's `client`/`host`/`port` and shipping under `link.id`. The logic is otherwise
identical — heartbeat to learn that follower's `confirmed_seq`, replay from the ring or the
backfill, or `error.SnapshotRequired` if too far behind. Nothing here is shared, so each
follower catches up on its own cursor. `checkpointBackfill` already prunes to
`minConfirmed(expected = links.len)` — with the link set it prunes only when *every* live
follower has confirmed, which is the correct safe watermark.

## 6. Config + wiring

Add an explicit replica list, keeping the current single-replica keys as a compatibility
shim:

```
replica: struct {                 // unchanged: legacy single-replica
    enabled: bool = false,
    address: []const u8 = "127.0.0.1",
    port: u16 = 3010,
    ...
} = .{},
replicas: []const struct {        // new: explicit fleet (wins when non-empty)
    address: []const u8,
    port: u16,
    uid: []const u8 = "",
} = &.{},
```

`main.zig`: if `replicas` is non-empty, `total_replicas = replicas.len + 1` and
`becomeDurableLeader` is called with the set; otherwise fall back to the single
`replica.{address,port}` path (today's behaviour). `becomeDurableLeader` grows a variant
that takes a slice of `{host, port, id}` and calls `addFollower` per entry before wiring the
WAL `ship_callback` (a leader that reaches *no* replica still serves, as today).

## 7. Read routing (explicitly OUT of scope for this change)

Fan-out puts the *data* on N replicas. Deciding which replica a given read hits is a
routing concern that belongs in `proxyd`/the driver, not the engine:
- writes -> primary; reads -> a replica (round-robin, or least-lag using
  `kaidb_replication_lag_frames` per replica);
- evict a replica from the read pool when its lag crosses a threshold;
- read-your-writes: pin to primary, or have the client wait for its write's LSN via the
  exposed `confirmed_seq`.

This design deliberately stops at "the replicas exist and are fed"; routing is a separate,
smaller piece layered on top.

## 8. Failure and consistency semantics (unchanged in spirit)

- Async by default (read-scaling): a write commits locally and is shipped best-effort to all
  links; replicas are eventually consistent, lag is observable per replica.
- Sync/quorum (opt-in, existing `await_quorum` + `SET DURABLE COMMIT ON`): a write waits for
  `floor(N/2)+1` acks across the fleet — now genuinely meaningful with N>1.
- Fence epoch is unchanged: it stamps every batch (`epoch`) and every follower fences a
  stale leader independently, so a fan-out leader that is demoted is rejected by all
  followers uniformly.

## 9. Staged plan (each stage builds + tests green before the next)

1. **Extract `FollowerLink`**, move the transport fields into a one-element `links` list,
   keep `connect`/behaviour identical. Pure refactor; existing single-follower tests
   (`P7`/`P8` in `root.zig`) must stay green with zero behavioural change.
2. **`addFollower` + ship-to-all** in `shipPending`, per-link reconnect. New test: two
   in-process followers, one write, assert both apply it and `confirmedSeq()` advances for
   both; kill one mid-stream, assert the other keeps receiving and the killed one catches up
   on reconnect (ring), or `SnapshotRequired` past the ring.
3. **Config `replicas[]` + `main.zig` wiring + `becomeDurableLeader` set variant.** Test the
   config parse + that `total_replicas` and quorum math match the fleet size.
4. **Docs:** update `prod-fitness.md` 4.5 (replication no longer single-follower) and note
   the routing follow-up.

## 10. Risks / watch-items

- **`ReplClient` lifetime**: each link owns a client with its own buffers; `deinit` must free
  every link's client + duped host. (Today's single-client `deinit` becomes a loop.)
- **Head-of-line during sync quorum**: shipping to links serially in `shipPending` means a
  slow link delays reaching a later link within one batch. Fine for async; if sync-quorum
  latency matters with a large fleet, ship concurrently (spawn per-link on the io group and
  join) — a follow-up, not needed for read-scaling.
- **Backfill pruning correctness**: `minConfirmed(expected = links.len)` must use the live
  link count, not a stale `total_replicas`, or a removed replica could stall pruning. Tie the
  expected count to `links.items.len`.
- **No dynamic membership** in this design: the replica set is fixed at leader start.
  Add/remove-at-runtime is a further follow-up (needs a membership record + snapshot
  bootstrap for a newly added replica).
