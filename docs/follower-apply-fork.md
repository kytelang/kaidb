# Follower apply: logical re-execute vs physical page shipping

Status: DECISION doc (2026-08-01). The two-node harness (root.zig "replication: two-node apply
consistency") proved that the naive R1 approach -- reuse the crash-recovery per-record apply on a fresh
follower -- does NOT work cross-node. This doc frames the fork for how a follower should actually apply a
leader's changes, with a recommendation. Companion: `replication-ha-design.md` (the overall HA design),
which this resolves the APPLY step of.

## 1. What the harness proved (the constraint)

Applying shipped WAL records to a FRESH follower fails: `InvalidChecksum ... page_id=27 beyond_eof`.
Root cause: the catalog records (sys.objects / sys.tables) carry the LEADER's root PAGE IDs. A follower
lays out its own B-tree pages independently, so a leader page id (e.g. 27) does not exist in the
follower's file. `recover()` works only because the WAL and the db file are the SAME node's pages.

Key observation that shapes the fork: the DATA is logical, only the STRUCTURE is physical.
- USER-TABLE DML records (`insert`/`update`/`delete`, key + row-value bytes) are LOGICAL. The row value
  is serialized row data (fixed + heap), NOT page references. These transfer across nodes fine.
- CATALOG records (sys.objects/sys.tables) embed the leader's root PAGE ID. These do NOT transfer.
So the failure is narrow: the follower inherits leader root page ids instead of allocating its own.

## 2. Option A: LOGICAL apply (follower re-executes structure, replays data)

The follower treats a shipped stream as logical intent and applies it through its OWN write path so it
allocates its OWN pages.

- CATALOG record for a table (sys.tables/sys.objects): the follower ENSURES a local table with that
  schema exists -- create it via the local create-table path (which allocates a fresh root page) if
  absent -- and records ITS OWN root id in ITS catalog. The leader's embedded root page id is IGNORED.
  This is the whole fix: intercept catalog apply, do a local create instead of a verbatim value insert.
- DML records: unchanged. `applyDmlRecord` reads the FOLLOWER's catalog (now with the follower's own
  root), gets the follower's table tree, and applies the logical key/row-value. Because the value is
  logical, it applies cleanly once the follower owns the tree. The existing `applyStream` already does
  this half correctly.
- System tables (sys.users etc.): already handled -- the follower bootstraps its own, and applyStream
  skips leader system-catalog records it cannot/should not apply.

Properties:
- Ship path: UNCHANGED. `ReplWal.ship` already ships logical records; `applyStream` is already logical.
  The only new work is the catalog-create interception. Smallest delta from what is built.
- Bandwidth: compact (only changed rows). Good for a config store.
- Coupling: TOLERANT. Follower may differ in physical layout, page fill, even (within the WAL/record
  format) engine minor version. Followers are logical replicas, promotable independently.
- Correctness surface: the follower re-derives structure, so index maintenance, version chains, and page
  allocation all run through the follower's own, already-tested write path. The risk is that the logical
  apply must faithfully reproduce visibility (MVCC xmin/xmax) -- `applyDmlRecord` already threads
  committed_txns for this.
- Cost: needs a "create table from catalog metadata" entry the follower can call, and the interception
  in the catalog-apply path. Bounded, and mostly reuses existing DDL execution.

## 3. Option B: PHYSICAL page-image shipping (follower is a byte clone)

The leader ships PAGE IMAGES (the bytes of modified pages, keyed by page id); the follower writes those
bytes to the SAME page ids. The follower file becomes byte-identical, so page ids match by construction.

- Ship path: NEW. Today the ship side is logical (`ReplWal` ships records). Physical shipping means
  streaming page images -- either an ARIES-style physical/physiological log, or piggybacking the
  existing doublewrite buffer + checkpoint (which already materialize physical page images at known
  LSNs). The WAL-callback seam would carry page images, not `LogRecord`s.
- Follower apply: trivial and obviously correct -- write page bytes at their page id, verify checksum.
  No catalog remap, no re-execution, no MVCC re-derivation.
- Bandwidth: HEAVIER. A one-cell change ships a whole page unless a page-delta scheme is added.
- Coupling: RIGID. Follower must be byte-compatible: same page size, same on-disk format, same engine
  version. A version skew between leader and follower corrupts the follower. Followers cannot diverge.
- Correctness surface: smallest to reason about (byte copy = identical bits), but it moves the risk to
  the ship side (must ship a consistent, gap-free set of page images honoring WAL-before-page ordering),
  and to operational rigidity (lock-step versions).

## 4. Comparison

| Dimension | A: Logical re-execute | B: Physical page images |
|---|---|---|
| New ship infrastructure | none (reuse ReplWal logical stream) | new (page-image stream or checkpoint piggyback) |
| Delta from what is built | small (catalog-create interception) | large (new ship + apply path) |
| Bandwidth | low (changed rows) | high (whole pages) unless delta |
| Leader/follower coupling | tolerant (layout, minor version) | rigid (byte-identical, same version) |
| Correctness reasoning | re-derive structure via tested write path | byte copy (simplest bits), rigid ship ordering |
| Follower divergence / independence | yes (logical replica) | no (byte clone) |
| Fit for a low-volume CONFIG store | strong | works, but over-engineered + rigid |

## 5. Recommendation: Option A (logical re-execute)

For the config-store use case and for what is already built, LOGICAL apply is the right choice:
1. It reuses the existing logical ship (`ReplWal.ship`) and the existing `applyStream` -- the remaining
   work is bounded to the catalog-create interception, not a whole new ship+apply path.
2. It is version- and layout-tolerant, which matters for rolling upgrades (a follower on a newer engine
   can still apply an older leader's logical records; a byte clone cannot).
3. Bandwidth suits a config plane (tiny, low write rate).
4. The correctness path is the follower's OWN, already-tested write path, so index/MVCC behavior is not
   re-implemented, only re-driven.

Physical shipping wins only when the workload is write-heavy (page images amortize) AND followers are
guaranteed byte-identical and version-locked -- not the config-store profile. Keep it as a documented
alternative if a high-throughput data-plane replica is ever needed.

## 6. Concrete next step for Option A (the bounded work)

1. Add/locate a follower-side "ensure local table from catalog metadata" call: given a shipped sys.tables
   record (schema + columns), create the table locally if absent, allocating a fresh root page and
   recording it in the follower's catalog. This REPLACES the verbatim catalog-value insert for
   table-defining records.
2. In `Database.applyStream` Phase 1, route table-defining catalog records to that call (keep the
   sys.objects master-tree handling only for the follower's own bookkeeping, with follower page ids).
3. Keep `applyDmlRecord` unchanged -- it already applies logical key/row-value against the follower's
   own tree.
4. Flip the harness assertion from SkipZigTest back to the real leader/follower consistency compare;
   that test is the gate for this change.
5. Only then move to R2 (fencing epoch) and the wire/server pieces in `replication-ha-design.md`.
