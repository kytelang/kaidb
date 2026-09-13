# NovaDB — Execution Plan (go-forward)

_Created 2026-07-22. This is the FORWARD plan: what to build to make NovaDB a real, concurrent,
multi-model database. For the original full-engine audit (storage, MVCC, query, catalog, wire),
see [`btree_readiness_plan.md`](./btree_readiness_plan.md) — this document does not duplicate it._

> **Honest status (2026-07-22):** NovaDB is an architecturally ambitious but **functionally
> single-writer** engine. The data structures are real (slotted pages, CLOCK buffer pool with
> doublewrite + CRC, B+tree with split/merge/borrow, MVCC+undo, WAL, volcano iterator, joins,
> durable catalog). But it **crashes under concurrent writes** and has **no write concurrency**.
> We are far behind a production DB on the concurrency + multi-model axes. This plan is the path.

---

## 0. What the Nova concurrency benchmark proved (2026-07-22)

Driven by the multi-process concurrency-scaling harness in the Nova repo
(`lang/repro/ycsb/conc_scaling.sh` + `client_btree.nova`), on an 8-core box:

- **Read-only:** scales to ~3.2× then plateaus (~105K ops/s) at C≈4–8. On a single box this is
  largely client-CPU/core saturation, not cleanly the engine — PostgreSQL plateaus similarly.
- **Concurrent writes: SERVER CRASH.** The **2nd concurrent writer** kills the server:
  `error(wal): Failed to write WAL header during rotation: NotOpenForWriting`. A single writer is
  fine (~55K ops/s mixed). Machine-independent, reproducible. **This is the headline blocker** — the
  "faster than Postgres" single-thread numbers are moot until concurrent writes are safe.

### Root cause (traced through the code this session)
Not one bug — an **inconsistent locking discipline** across three layers:

1. **`WriteAheadLog` has NO internal lock.** `append` / `flush` / `rotate` / `sync`
   (`src/durability/write_ahead_log.zig`) freely mutate `current_file`, `buffer`, `file_size`,
   `header`. `rotate()` *closes and reopens* `current_file`; anything touching the WAL during that
   async window sees a closed handle → `NotOpenForWriting`.
2. **The background writer runs OUTSIDE the DB lock.** `runBgWriterTask` → `pool.flushAllPages()`
   (`src/schema/database.zig:449`) every 500 ms, guarded only by `doublewrite_mutex` +
   a per-storage-instance `rw_lock` — **never** `db.rw_lock`. Foreground writes are guarded by
   `db.rw_lock` (exclusive, `src/query/query_executor.zig:730`). So the foreground path and the
   BgWriter guard the **same page frames + adjacent WAL/pager state with DIFFERENT locks** → they run
   genuinely concurrently (sessions are async tasks on a thread pool, `tcp_server.zig:95 group.async`).
3. **The whole write path serializes on one global `db.rw_lock`.** Correct-ish for a single writer,
   but it is *the* reason writes don't scale (the ~5-thread ceiling), and it does not compose with the
   BgWriter's separate lock domain.

Why C=1 is fine but C=2 crashes: two writers double WAL volume → the log hits `max_file_size` and
`rotate()` actually fires inside the test window, landing in the BgWriter's unlocked flush.

---

## Phase 1 — Correctness: stop the crash (make concurrent writes SAFE) ⭐ P0
_Small, shippable. Turns "crashes" into "works, serialized." ~1–2 days._

Does **not** redesign anything — just makes the existing single-writer model correct under concurrency.

- [ ] **WAL gets its own `Mutex`.** Hold it in `append`/`flush`/`rotate`/`sync`/`checkpoint`
      (`write_ahead_log.zig`). The WAL file handle must never be mutated by two paths at once.
- [ ] **BgWriter under the DB lock.** `runBgWriterTask` must acquire `db.rw_lock` (or a dedicated
      checkpoint lock shared with the write path) before `flushAllPages`/`purgeStaleUndoPages`.
      Right now it is a free radical racing every foreground writer.
- [ ] **Unify page-frame locking.** The foreground write path (`db.rw_lock`) and `flushAllPages`
      (`doublewrite_mutex` + `inst.rw_lock`) guard the *same* frames with *different* locks — pick one
      discipline so dirtying a page and flushing it can never overlap.
- **Acceptance:** `conc_scaling.sh` mixed 50/50 at C=2..32 completes with NO server crash (throughput
  need not scale yet). ARC/ASAN clean on the Nova client side (already hardened against dead sockets).

After Phase 1, NovaDB is honestly **single-writer-serialized** — like SQLite's default. A respectable,
shippable position for an embedded engine. Ship this before claiming any concurrency story.

---

## Phase 2 — Write concurrency: lightweight latches + group-commit ⭐ P1
_The real engine work. Weeks. Turns the plateau into a scaling curve._

The single global `db.rw_lock` is the ceiling. Replace it with fine-grained concurrency, the way
Postgres/InnoDB do it. MVCC+undo already exist, so this is about *latching* and *log-insert*, not
adding versioning.

- [ ] **Lightweight latches (the user's ask).** Per-page/per-node latches with **B+tree latch coupling
      ("crabbing")**: a traversal latches child before releasing parent, so writers to *different*
      pages proceed in parallel. Latches are short, non-blocking spin/rw primitives (not the heavy
      global `Io.RwLock`). This is the single highest-leverage change for write throughput.
    - Buffer-pool frame latches (pin + rw-latch per frame) replace the per-instance `inst.rw_lock`.
    - A **lock/latch ordering protocol** documented and enforced (deadlock-free by ordering).
- [ ] **Group-commit WAL.** The log insert becomes a *tiny* critical section: reserve an LSN
      (atomic), copy the record into a shared ring buffer; a single flusher fsyncs **batches**.
      Many committers share one fsync → both concurrency AND higher throughput. The whole statement
      is no longer serialized — only the log-append is, briefly.
- [ ] **Snapshot reads that don't block writers.** With MVCC, `SELECT` should read a snapshot
      lock-free (or with a short latch), not take a shared lock that an exclusive writer blocks on.
      Readers and writers stop contending.
- [ ] **Row/predicate conflict detection** via the existing MVCC/undo (write-write conflicts abort/
      retry a txn) instead of a coarse exclusive lock.
- **Acceptance:** `conc_scaling.sh` mixed shows aggregate throughput *climbing* with C (target: near-
  linear to core count), per-client degrading gracefully — matching PostgreSQL's shape, not a plateau.

---

## Phase 3 — Multi-model: SQL **and** NoSQL/document ⭐ user-vision P2
_NovaDB as a multi-model store: relational SQL + document/key-value, on one engine._

The pieces already lean this way — the Nova side has BSON + a MongoDB driver (D3) + a document
`runCommand` seam idea, and NovaDB has a JSON/HTTP bridge. Make it first-class:

- [ ] **Document/collection model** on the B+tree: a collection is a keyed store of BSON/JSON
      documents; secondary indexes over document fields (functional indexes on JSON paths).
- [ ] **A document command surface** (insert/find/update/delete/aggregate) alongside SQL — the Nova
      `Connection` seam should grow a `runCommand`-shaped path (the D3 finding: the SQL-shaped
      exec/query seam fits document DBs poorly). One engine, two query surfaces.
- [ ] **Unified storage**: relational rows and documents share the pager/MVCC/WAL — a document is just
      a row whose value is a BSON blob with indexed extracted keys. No second storage engine.
- [ ] **Cross-model**: SQL over document collections (JSON path expressions in SQL), and document
      queries that see relational tables. This is the differentiator vs SQLite/RocksDB/Mongo.
- **Positioning:** this is the story that makes NovaDB *not* a Postgres competitor but a
      **multi-model embedded engine** (SQLite-with-documents-and-WASM), which is a category it can win.

---

## Sequencing & positioning

1. **Phase 1 first, always** — a crashing DB has no story. Small, do it next time NovaDB is picked up.
2. **Phase 2** is the real project; schedule deliberately. Until it lands, describe NovaDB honestly as
   *single-writer-serialized* (SQLite-class), not "beats Postgres under load."
3. **Phase 3** is the vision that reframes the whole comparison: stop fighting Postgres on multi-user
   RDBMS turf; be the **multi-model, WASM-native, Nova-integrated embedded engine** — competing with
   SQLite/RocksDB/DuckDB, where µs-latency + zero-ops + documents + language integration actually win.

See `btree_readiness_plan.md` for the rest of the audit (P1 correctness, storage/MVCC hardening,
recovery validation, catalog, wire protocol) — those remain valid and complement this plan.

## Verification harness (already built, in the Nova repo)
- **Concurrency:** `lang/repro/ycsb/conc_scaling.sh <client> <readpct> <dur> <records> "<levels>"` —
  multi-process load, aggregate ops/sec vs C. The Phase 1/2 acceptance gate.
- **Correctness/throughput:** `lang/repro/ycsb/ycsb_btree.nova` (YCSB A–F over the Nova driver).
- Build btree: `~/zig/zig build` in `btree/`; run: from a data dir with `db.json` (`base_dir`, `port`).
