# NovaDB Readiness Plan

> **⚠️ THIS DOC IS HISTORICAL (pre-Stage-3). Read `architecture.md` + `CLAUDE.md` for current state.**
>
> This plan was written against the July engine, which was **functionally single-writer** behind a
> global `db.rw_lock`. Since then (Stage 3, 2026-08-08 onward) much of what it lists as OPEN/[CRIT]
> has landed and is test-gated. Do NOT rate the engine from the OPEN items below without cross-checking
> the source + `src/root.zig` tests. Reconciled 2026-08-25 during a robustness audit:
>
> - **Concurrency** — global write lock REMOVED. Per-table `GroupLock` (SELECT=read, INSERT=write,
>   UPDATE/DELETE=exclusive, DDL=db-wide) + per-tree `structure_lock` (shared in-place / exclusive
>   split-merge). Gated by `STRESS:` consistency tests, the `GroupLock` invariant stress, and TWO
>   default-on concurrent fuzzers (disjoint-namespace + overlapping-key) plus a parallel-insert
>   completeness gate. `NOVADB_FUZZ=1` scales the fuzzers up. §1's "~5-thread ceiling" is improved but
>   not fully eliminated (UPDATE/DELETE + FK/join still take the table/db exclusive) — that residue is
>   the real remaining item, not the whole global lock the doc describes.
> - **Crash recovery / durability** — kill-9 recovery is real and tested: WAL rebuild of committed rows,
>   torn-WAL-tail clean recovery, ENOSPC-never-false-ACK, group-commit WAL, crash-safe checkpoints
>   (`CHECKPOINT.tmp`+fsync+rename). Closes §2.4's [CRIT] #7/#8/#9. Remaining tail: full ARIES-style
>   WAL truncation + per-page-LSN redo idempotency.
> - **MVCC / isolation** — SERIALIZABLE (SSI) + REPEATABLE READ snapshots are implemented and tested,
>   superseding §2.3's "Read-Committed only" note.
> - **SQL** — GROUP BY/HAVING/DISTINCT/ORDER BY, savepoints, multi-row INSERT, IN/scalar subqueries,
>   UNION, server-side bound params, typed DECIMAL/DATE ordering: all gated.
>
> The gate (`./gate.sh` = `zig build test`) passes on Darwin-arm64. What follows is the ORIGINAL July
> audit, kept for its analysis; treat its status labels as superseded.

_Original audit: 2026-07-14 — full engine audit (~19.2K LOC Zig) across concurrency, storage, query
execution, durability/MVCC, SQL, schema/catalog, and the wire protocol (five parallel subsystem
deep-dives + direct inspection of `std.Io.Threaded`)._
_Last updated: 2026-07-14 — after the P1 query-correctness batch, the networking rebuild (Postgres-
inspired binary wire protocol), the Nova driver, and the HTTP JSON bridge landed. See §A (changelog)._

---

## 0. Executive summary

NovaDB is an **architecturally ambitious, functionally-single-threaded** B+Tree engine. The core
data structures are real and mostly correct in isolation — slotted pages with two-way growth and
in-place compaction, a 16-segment CLOCK buffer pool with doublewrite + CRC32 torn-page protection,
a genuine B+tree with working split/merge/borrow/root-collapse, overflow pages, MVCC-with-undo,
a WAL, a volcano iterator, nested-loop/hash joins, and durable catalog/DDL persistence.

It is **not yet production-ready**, and the remaining gaps cluster into three themes:

1. **The whole database still runs under one global lock.** Every statement takes a process-wide
   `db.rw_lock` (exclusive for writes, shared for reads). This is the true cause of the "~5-thread
   ceiling" — a **contention ceiling, not a capacity ceiling**. The lock is **load-bearing for
   correctness**: the layers beneath it (B+tree structure modifications, the WAL, background flush,
   undo purge) have real data races that are only safe *because* the global lock serializes everyone.
   **You cannot simply remove the lock — the lower layers must be hardened first, in order.** _(Open.)_

2. **The durability protocol is not correct end-to-end.** The WAL is not truly write-ahead (data
   pages can flush before their log is durable), the undo log is not reconstructed on boot (recovery
   can erase previously-committed rows), and commits are async by default. The pieces are fine; the
   protocol wiring is not. _(Open.)_

3. **The SQL surface and wire protocol were thin and partly non-functional.** _(Substantially
   improved — see §A.)_ Query-result correctness (numeric compares, compound `WHERE`, PK ranges,
   `ORDER BY`) is now fixed; the wire protocol was rebuilt into a single Postgres-inspired binary
   protocol with real parameter binding and cursor streaming, plus a thin HTTP/JSON adapter. Remaining
   SQL gaps: `GROUP BY`/hash-agg, multi-row `VALUES`, full type map + NULL bitmap, constraint
   enforcement.

**Maturity by subsystem (current):** Catalog/DDL persistence — Beta. Storage engine (single-threaded)
— Beta. Query/iterator engine — **Alpha→Beta** (result correctness fixed; joins/agg still immature).
Wire protocol — **Alpha→Beta** (rebuilt, param binding + cursors work; polish + auth pending).
Concurrency — **Alpha (global-lock serialized)**. Durability/MVCC protocol — **Alpha (not
crash-correct)**.

---

## A. Changelog / progress (what has been done)

### A.1 Query-result correctness — DONE & verified (2026-07-14)
The four CRIT/HIGH "wrong answers" bugs are fixed and verified live over the HTTP bridge:

- **#5 Numeric comparisons done as strings → FIXED.** All cell values are stored as strings (even
  ints). `compareValues` used to stringify both sides and compare lexicographically (`"10" < "5"`).
  Replaced with a typed 3-way `orderJson` (integer → float → lexical fallback) in `iterator.zig`;
  NULL compares as false (SQL 3-valued logic). The duplicate copy in `query_executor.zig` now
  delegates to it. Range predicates on numeric columns are now correct.
- **#6 Compound `WHERE a AND b` returned zero rows → FIXED.** `evalExpr` / `evaluateExpr` only
  handled a single `col op literal`; a nested `binary_op` operand yielded `null` → false. Both now
  recurse on `.AND` / `.OR`. Verified: AND, OR, and mixed predicates.
- **#6-adjacent PK range wrong → FIXED (correctness) / optimization deferred.** PKs are stored as
  decimal strings and the B+tree orders them lexicographically (`1,10,2,20,3`), so the lexicographic
  start-key seek made `id>5` return nothing and `id>=2` skip `10`. Fix: the `iteratorAfter` start-key
  seek is now gated on the PK being TEXT/BLOB; numeric PKs fall back to a full scan + the (now correct)
  typed filter. EQ point lookups were always correct. **Follow-up (deferred):** order-preserving
  numeric key encoding restores index-accelerated numeric ranges and numeric result ordering — see §5.
- **#19 (part) `ORDER BY` parsed-but-ignored → NOW EXECUTED.** The materialize loop collects a
  parallel typed sort-key array (keys pulled from the full row so ORDER BY columns need not be
  projected), defers the inline `LIMIT`, sorts an index permutation via `orderJson` (ascending,
  multi-column tiebreak), then truncates to `LIMIT`. Verified: numeric ordering, text, tiebreak,
  LIMIT-after-sort, non-projected sort column. (`order_by` AST is column-names only, ASC — no DESC
  token yet.)
- **INSERT ergonomics:** implicit column list — a bare `INSERT INTO t VALUES (...)` now maps values
  positionally to the table's columns; the explicit-column form is unchanged.

`zig build test` passes after these changes.

### A.2 Wire protocol rebuild — DONE & verified (2026-07-14)
Replaced the old **two-protocols-on-one-port** situation (a working JSON one + a broken binary one)
with **one Postgres-*inspired* binary wire protocol** (not wire-compatible), plus a thin HTTP adapter:

- **`src/proto/wire.zig`** — framing `[type:u8][len:u32 BE][payload]`, message tags, type OIDs,
  encoders + decoders incl. extended-query `Parse`/`Bind`. (unit tests pass)
- **`src/proto/oidmap.zig`** — `ColumnType` ↔ OID mapping / field descriptors.
- **`src/proto/command.zig`** — safe **typed parameter substitution** (injection-safe; tests include
  injection attempts). Resolves the "no parameter binding anywhere / injection-only" CRIT for the
  binary path: `Bind` does server-side substitution because the executor takes SQL text only.
- **`src/proto/session.zig`** — per-connection frame-loop state machine over `std.Io` reader/writer:
  Startup → AuthOk/ParameterStatus/ReadyForQuery → simple `Query` + extended `Parse`/`Bind`/`Execute`/
  `Sync`/`Describe`/`Close`/`Terminate`; emits `RowDescription`/`DataRow`/`CommandComplete`/`Error`.
  **Cursor streaming**: a portal caches the materialized response; `Execute max_rows` pages `DataRow`s
  then `PortalSuspended`/`CommandComplete`; `Describe` sends `RowDescription` lazily.
- Wired into `tcp_server.zig` (`:3009`); verified end-to-end with a Python client (handshake,
  CREATE/INSERT/SELECT, bound `$1` parameter) and 5-rows-paged-2+2+1 cursor streaming.

### A.3 Nova driver + HTTP JSON bridge — DONE & verified (2026-07-14)
- **Nova driver** `lang/src/std/data/btree/client.nova` (registered as `data/btree/client`): blocking
  TCP sockets, framing writers, buffered frame reader, payload cursor, `connect`/`query`/`close`.
  Verified live vs the engine: CREATE/INSERT (tag `OK 1`), SELECT (`[1,nova],[2,planck]`). (Fixed a
  Nova compiler `await`-return-type bug and the `.error`/`.ok`/`.value` field special-case footgun
  along the way.)
- **HTTP JSON bridge** (already in `main.zig` `handleHttp`, verified): schnell server on `:3008`,
  `POST /query {sql, session_token}` → `executor.execute` → JSON `{columns, column_types, rows,
  rows_affected, error_message}`; `GET` serves static files. This is the "HTTP connector for queries"
  use case — **not** a workbench (the workbench is separate software).

### A.7 Perf truth + release-build bugs — DONE & verified (2026-07-15)
**⚠️ BUILD RELEASE FOR ANY BENCHMARK OR DEPLOY: `zig build -Doptimize=ReleaseFast`.** Plain `zig build`
is Debug (`build.zig:5` `standardOptimizeOption`), and `main.zig:148` then uses `std.heap.DebugAllocator`
(release uses `c_allocator`). DebugAllocator captures a **full stack-trace unwind on every allocation**
and is `thread_safe` → **one global mutex serialises every alloc/free**. **Every throughput number in this
plan's history was a debug artifact.** Measured: **~292 → ~11.5k ops/s single-worker (~40×)**; 16 workers
= **25.6k reads/s, 19.1k writes/s**; the 16×300 concurrency gate went **116s → 1s**.

**ROADMAP CHANGE — the global write lock is NOT the ceiling.** With process-based clients (the Python
*threaded* client is itself GIL-bound past ~10k ops/s), reads scale **2.3×** and writes **1.9×** from
1→16 workers — near-identical. If the global exclusive write lock were the write ceiling, writes would be
**flat at ~1.0×** while reads (shared lock) scaled. They track each other, so both are hitting a common
limit — CPU saturation, with 16 client processes competing with the server for the same 8 cores. **Deep-P3
(latch-safe SMOs + lock removal) is therefore deprioritised**: it is the riskiest work in the project and
the evidence no longer supports it as the bottleneck. Isolating the true server ceiling needs a
non-co-located, non-Python client (folds into P6/YCSB).

**Bugs the release build exposed (all fixed & verified):**
- **[CRIT] Query-cache use-after-free → SIGSEGV.** `executeWrapped` handed out `cached.stmt` under
  `query_cache_mutex`, released the mutex, then executed the AST — while the eviction path
  (`count() >= 10000`) freed an arbitrary entry's `parser` (and its arena). A concurrent fiber walking
  that AST segfaulted (`getVal` iterator.zig:32, NULL deref). Needs >10k **distinct** SQL strings, and the
  HTTP/JSON bridge has **no param binding**, so every unparameterised INSERT is distinct → any real app
  hits it. Debug hid it: 40× too slow to reach the cap, and the allocator's global mutex shrank the race
  window. **Fix:** cached parsers are never freed while running; the cache is bounded by *declining to
  cache* past `max_cached_statements`, and anything not cached is owned + freed per-execution.
- **[HIGH] schnell had no keep-alive and no TCP_NODELAY** (the vendored copy was degraded vs
  `plancksystems/schnell-zig`, which has `setTcpNoDelay`/`setTcpKeepAlive` + a `runRequestLoop`). Every
  request paid a fresh TCP setup (~5ms). **Fix:** ported both sockopts + a persistent-connection request
  loop (`Connection:` parsing, HTTP/1.1 keep-alive default, `max_requests_per_connection` with a
  `Connection: close` on the final response, separate read/write buffers). ~2× on its own.
- **[HIGH] async commit lost committed rows on a plain process kill** (crash-async: 150/200). Commits sat
  in the WAL's *user-space* buffer until a size/interval trigger; debug was slow enough that the interval
  always fired, release finishes 200 inserts in ~20ms so it never did. Weaker than the standard async
  contract (PostgreSQL writes WAL to the OS at commit, skipping only the fsync). **Fix:** new
  `appendAndDrain` — async commit now `write()`s to the OS without fsync, so it survives a process crash
  and only risks loss on power failure. crash-async 200/200 PASS.

New harness `tests/harness/throughput.py`: keep-alive, process-based clients, reports read-vs-write
scaling (the read/write scaling *split* is what identifies whether the write lock is the ceiling).

### A.6 Group-commit WAL (Phase 3, step 1) — DONE & verified (2026-07-15)
The WAL is now thread-safe and commits are group-committed. `write_ahead_log.zig`: added `commit_mutex`
(Io.Mutex), `flush_cond` (Io.Condition), `flushing` flag, `buffered_lsn`. All buffer mutations go through
`commit_mutex`; a single **leader** drains the buffer under the mutex then fsyncs **outside** it (never
fsync-under-mutex — that was the earlier hang), publishes `flushed_lsn`, and `broadcast`s; **followers**
that appended during the fsync are covered by the next leader's single fsync. New surface: `appendLocked`
(buffered, no fsync, interval-drains to the OS in async mode), `ensureDurableLocked`/`ensureDurableTo`
(leader/follower group fsync to a target LSN — **caps the wait at `buffered_lsn`** so it never spins asking
for an LSN that was never buffered), `syncFileLocked`, `rotateLocked`. `append`/`appendAndSync`/`flush`/
`sync`/`checkpoint`/`rotate` reimplemented on these; the WAL-before-page gate (`poolWalFlushTo`) now calls
`ensureDurableTo(page.lsn)`. This fixes the latent #12 begin/commit WAL race (begin/commit records were
appended outside the global lock) and is the **thread-safe-WAL prerequisite for removing the global lock**.
Verified: 23/23 unit, crash sync+async 200-row durable, concurrency gate 16×300 = 3600/3600, pk_ordering.

**KEY FINDING (reframes the throughput roadmap):** fsync is **NOT** the write bottleneck. Measured per-op:
SELECT ~4.8ms (sync==async → pure HTTP-connection-per-request overhead in the harness), INSERT sync 4.15ms
vs async 2.72ms (fsync only ~1.4ms). Concurrency throughput is **flat ~140 ops/s from 1→16 workers**
(async==sync) → writes are fully serialized by the **global write lock**, and the harness is
connection-bound. So group commit is correct + necessary but does **not** independently raise throughput;
the real lift needs the global lock removed. Real throughput numbers need a keep-alive/TCP client (YCSB, P6).

### A.5 Query maturity (Phase 5 surface) — DONE & verified (2026-07-15)
- **Lexer:** added `HAVING`, `OFFSET`, `ASC`, `DESC` tokens + keyword map entries.
- **AST:** `OrderByItem { col, desc }`; `SelectStmt` gained `order_by: []OrderByItem`, `distinct: bool`,
  `offset: ?u32` (alongside existing `group_by`/`having_expr`).
- **Parser:** `SELECT [DISTINCT]`; `HAVING <expr>` after GROUP BY; `ORDER BY col [ASC|DESC], ...`;
  `LIMIT n OFFSET m`. `parsePrimary` and the ORDER BY parser accept an aggregate in expression position
  (`COUNT(*)`, `SUM(amt)`, bare or full form) and resolve it to its **output header** name
  (`COUNT`/`SUM`/`AVG`/`MIN`/`MAX`), so `HAVING COUNT(*) > 5` and `ORDER BY SUM DESC` parse.
- **Executor** (`query_executor.zig`): file-level `applyOffsetLimit` (offset-drop then limit-truncate,
  frees dropped cells). Scalar path: DISTINCT dedupe (StringHashMap, keeps first, dedupes parallel
  sort-keys), multi-key sort comparator with per-item `desc` invert, inline-LIMIT gated off when
  order/offset/distinct present, `applyOffsetLimit` at the tail. Hash-aggregation path (`runAggregation`,
  signature slimmed to `(sel, iter, group_cols, headers)`): HAVING filter via `evalHaving`/`havingOperand`
  (AND/OR recurse; compares against output-header cells), DISTINCT dedupe, header-index ORDER BY with
  `desc` invert, then `applyOffsetLimit`.
- **Verified live** (HTTP bridge): ORDER BY DESC, multi-key `dept ASC, amt DESC`, LIMIT+OFFSET,
  DISTINCT (+ DISTINCT with ORDER BY), `HAVING COUNT(*)>1`, `HAVING SUM(amt)>=300 ORDER BY SUM DESC`,
  `ORDER BY SUM(amt) DESC` full form, `ORDER BY COUNT ASC`. **Harnesses:** 23/23 unit,
  concurrency 3600/3600, pk_ordering, crash 200-row durable — all green (no regressions).

---

## B. Architecture note — co-located WASM apps (context for §4)

NovaDB's HTTP layer exists for **two** purposes: (1) the JSON query connector above; (2) **co-located
WASM apps** — a Nova-authored app compiled to WASM and deployed *into* NovaDB's embedded wasmer
runtime (`src/wasm/`), where the end user hits the app over HTTP and **data + app share one address
space**. Request flow (`src/wasm/pool.zig` `processRaw` + `host.zig`): HTTP bytes are written into the
guest's linear memory → the exported `process` is called → the guest writes response bytes back into
linear memory → the host reads them out. The guest reaches the DB via the host import
`host_request(query_ptr, query_len, dest_ptr, dest_cap)` — today it passes a JSON `QueryRequest`
through linear memory and the host runs `executor.execute`.

**Implication for the driver:** the Nova DB driver must be **transport-abstracted** — TCP sockets for
an external client, or a **linear-memory host call** when co-located, pushing the *same* binary wire
frames through `host_request` so both transports share one execution path.

> **ON HOLD:** the entire WASM path (co-located driver transport, SSE-to-apps) is blocked until Nova's
> own WASM compiler is ready — clang-based Nova→WASM is explicitly rejected. Do not build the
> linear-memory transport or the SSE hub's app-facing side until then.

---

## 1. THE ~5-THREAD CEILING — root cause & fix (the flagged issue) — OPEN

**It is not a thread-pool cap.** `std.Io.Threaded` is initialized with `.async_limit=.unlimited,
.concurrent_limit=.unlimited` (`main.zig:154`), the pool grows worker threads on demand, and this
machine has 8 CPUs (so even the *default* would be 7, not 5). Two compounding causes:

### 1a. A single global `db.rw_lock` serializes every statement — the real wall
`query_executor.zig` (`executeStatement`) wraps **every** statement:
```
if (is_read) db.rw_lock.lockShared(io);   // all SELECTs share one lock
else         db.rw_lock.lock(io);          // every INSERT/UPDATE/DELETE/DDL = DB-WIDE EXCLUSIVE
```
`db.rw_lock` is one process-wide `Io.RwLock`, held across the **entire** statement — B+tree traversal,
page mutation, undo append, and WAL append+fsync. One writer blocks all writers **and** all readers.
The segmented-pool latches and per-page crabbing latches sit *underneath* it and add **zero** write
concurrency. `Io.RwLock` itself is cheap (atomic fast-path; futex-park only on contention) — so it is
not lock *overhead*, it is **statement-wide serialization**. Verify: a 100%-SELECT workload scales
(shared lock); `top -H` shows many threads but only ~5 `running` (rest parked in futex).

### 1b. The thread/concurrency config knob is not wired to anything
`main.zig:154` hard-codes the limits. `config.pool_size` is **buffer-pool pages**;
`max_sessions`/`max_connections` never touch `std.Io.Threaded`. So "no matter what thread count I
configure, it won't exceed 5" is literally true — the knob is disconnected.

### The fix (must be done in this ORDER — the global lock hides races beneath it)
1. **Harden the lower layers first** (only safe today because of the global lock):
   - **WAL internal thread-safety** — `wal.append`/`incrementLSN` have no lock and `incrementLSN` is a
     non-atomic RMW. Add a short WAL append mutex (or `fetchAdd` the LSN) + group-commit the fsync.
   - **Latch-safe structure modifications** — split/merge/borrow run **completely unlatched** (§2.2).
     Implement real lock-coupling; fix the descent-order (parent→child) vs split-order (child→parent)
     inversion that will deadlock once concurrent.
   - **Coordinate background flush with page latches** — `flushAllPages` reads page bytes while a
     writer holding only the per-frame latch mutates them → torn pages.
   - **Per-statement snapshot** instead of `isVisible` taking the txn mutex on *every row* — the next
     bottleneck the instant the global lock is gone.
2. **Then remove the global `db.rw_lock`** and rely on page latches + MVCC + a synchronized WAL.
3. **Wire real config** into `std.Io.Threaded.init` (`concurrent_limit=.limited(N)` from config) and
   per-connection dispatch, so the knob works and connection floods can't spawn unbounded threads.
4. **Delete debug `std.debug.print`** on hot paths (schnell per-byte, replication spam).

> Bottom line: the ceiling is contention, not capacity. Removing the global lock is the unlock — but
> it is step 2, gated on making the engine actually concurrency-safe (step 1).

---

## 2. Subsystem findings (open items; DONE items struck through)

### 2.1 Concurrency / threading — OPEN
- **[CRIT]** Global `db.rw_lock` per statement — the parallelism ceiling (`query_executor.zig`).
- **[CRIT]** Thread-pool size not configurable / not wired (`main.zig:154`, `config.zig`).
- **[HIGH]** WAL append + LSN unsynchronized; safe only under the global lock.
- **[HIGH]** Split/underflow latch-ordering inversion + unlatched window → deadlock once concurrent.
- **[HIGH]** Background flush races live page mutations → torn pages.
- **[MED]** Periodic global-exclusive vacuum/checkpoint = stop-the-world stalls.
- **[MED]** `unpinPage` CAS silently clamps at 0 — hides double-unpins instead of asserting.

### 2.2 Storage engine  _(functionally complete, single-threaded)_ — OPEN
- **[CRIT]** SMOs (split/merge/borrow) execute with **no latch** — crabbing only covers descent. Safe
  only under the global lock (`btree.zig`).
- **[HIGH, data loss]** `splitAndInsert` does `op.clear()` **before** re-inserting, with a count-based
  (not byte-based) split point; a skewed half exceeding page capacity throws `PageFull` mid-rebuild
  after the original was wiped → page contents lost, tree inconsistent (`btree.zig:451-476`).
  **← highest-severity remaining single-threaded bug.**
- **[HIGH]** `compact()` hard-caps at **512 cells** (`page.zig:279`); dense 16KB pages exceed that →
  `error.TooManyCells` breaks every delete/borrow/merge that compacts.
- **[HIGH]** Buffer pool gives **4 frames/segment** at min pool size; recursive SMOs pin
  parent+child+2 siblings+overflow → `error.NoFreeFrames`, no wait/retry.
- **[MED]** Merge-vs-pinned-iterator: `discardPage` can fail after the page was emptied/unlinked →
  orphaned leaked page; iterators can read a recycled page.
- **[MED]** No WAL-before-page ordering; `page.lsn`/`BPlusTree.lsn` always 0.
- **[MED]** `system.zig` stale — won't compile against the current tree (dead code; real path is
  `schema/database.zig`).

### 2.3 Query execution  _(volcano scans real; joins/aggregation immature)_
- ~~**[CRIT]** Numeric comparisons done as strings~~ → **FIXED** (§A.1).
- ~~**[CRIT]** PK range start-key lexicographic~~ → **FIXED (correctness)**; index-accelerated numeric
  range deferred to order-preserving keys (§5).
- ~~**[CRIT]** Compound `WHERE ... AND ...` returns zero rows~~ → **FIXED** (§A.1).
- ~~**[HIGH]** `ORDER BY` never executed~~ → **DONE** (§A.1).
- ~~`GROUP BY` ignored; no `OFFSET`/`DISTINCT`/`HAVING`/`DESC`~~ → **DONE (Phase 5, §A.5).** `HAVING`
  (incl. aggregate refs `HAVING COUNT(*)>n`), `OFFSET`, `DISTINCT`, `ORDER BY ... DESC`, multi-key
  ORDER BY with mixed ASC/DESC, and aggregate refs in ORDER BY (`ORDER BY SUM(amt) DESC`, bare or
  full form) all execute on both scalar and hash-aggregation paths. Subqueries still open.
- **[HIGH]** Joins fully materialize + deep-clone the right side; nested-loop is O(n²); hash join gated
  to right<100 rows. No predicate pushdown. _(Open.)_
- **[MED]** MVCC is Read-Committed with a live set (not snapshot); divergent visibility rules between
  query path and `stats.zig`; `committed_txns` grows unbounded. _(Open.)_
- **[MED]** Secondary indexes are append-only on UPDATE — stale entries never deleted. _(Open.)_
- Iterator latch/pin double-unlock: **verified correctly handled** via `pinned_page_id` guards. ✓

### 2.4 Durability / MVCC  _(pieces present, protocol not crash-correct)_ — OPEN
- **[CRIT, data loss]** Undo log **not rebuilt on boot** — recovery Phase-3 reconstructs from the
  inline leaf version only → a crash during UPDATE can **erase the previously-committed row**.
- **[CRIT, atomicity]** No write-ahead ordering — bg writer flushes dirty pages every 500ms with **no
  preceding WAL flush** and no page-LSN gate; crash before the commit record is fsynced leaves unlogged
  mutations on disk → uncommitted data silently "commits".
- **[CRIT, durability]** `synchronous_commit` defaults **false**; the server flips it on (`main.zig`),
  but embedded/library use loses committed txns.
- **[HIGH]** Eviction & fast-flush **bypass the double-write buffer** → torn pages under memory
  pressure (the common path under load).
- **[HIGH]** Purge↔reader TOCTOU on undo pages — validity checked under mutex, page read after
  releasing it; concurrent `purgeStaleUndoPages` can discard+recycle it → reader returns garbage.
- **[MED]** WAL `lastLSN` persisted only on rotation → LSN regression after crash; free-list not
  crash-safe → freed pages leak across crashes; no periodic checkpoint → WAL grows unbounded.

### 2.5 SQL, schema, wire protocol
- ~~**[CRIT, security]** No parameter binding anywhere (injection-only)~~ → **RESOLVED for the binary
  path** via `proto/command.zig` typed server-side substitution (§A.2). **The HTTP/JSON bridge still
  builds SQL text** — keep strict client escaping there, or add params to `POST /query`. _(Partial.)_
- ~~**[CRIT, driver]** Two wire protocols on one port; binary path broken~~ → **REPLACED** by one
  Postgres-inspired binary protocol (§A.2). **Cleanup owed:** delete the dead old paths — `runLoop` /
  `runBinaryProtocolStream` in `tcp_server.zig`, `proto/protocol.zig`, and the `common/proto.zig`
  JSON+binary `Packet` types once nothing references them. _(Open — cleanup.)_
- **[HIGH]** Aggregates parse partially — `COUNT`/`SUM` work in the materialize loop; `AVG/MIN/MAX`
  and `GROUP BY` do not. _(Open.)_
- **[HIGH]** `SELECT *` emits **one JSON-blob cell** against an N-column header — cardinality mismatch.
  _(Open — verify against current materialize path.)_
- **[HIGH]** Parser panics (`catch unreachable`) on numeric overflow — `LIMIT 9999999999999999999`
  crashes the connection thread. _(Open.)_
- **[MED]** NULL is a stringly-typed `"NULL"` sentinel with no null bitmap — collides with real text
  `"NULL"`; NULL into a numeric column errors. _(Open.)_
- **[MED]** Unknown SQL types silently become TEXT (`FLOAT/TIMESTAMP/BIGINT/BLOB`); every column stored
  with `size=255`. _(Open.)_
- **[MED]** Constraints parsed but **unenforced**: NOT NULL, UNIQUE, DEFAULT-on-insert, AUTO_INCREMENT.
  PK-less INSERT keys on `now().ms()` → same-ms collisions. _(Open.)_
- **[MED]** Multi-row `INSERT ... VALUES (..),(..)` unsupported (single tuple only) — needs an AST
  change (`values` → rows). _(Open.)_
- **[MED]** Catalog serialization has no version tag → any format change breaks existing DBs. _(Open.)_
- Catalog/DDL persistence (WAL + system B+Trees, rebuilt on boot) is the **most mature** part. ✓

---

## 3. Consolidated severity table (current)

| # | Finding | Sev | Status |
|---|---|---|---|
| 1 | Global `db.rw_lock` serializes all statements (parallelism ceiling) | **CRIT** | OPEN |
| 2 | Thread/concurrency config not wired to std.Io.Threaded | **CRIT** | OPEN |
| 3 | SMOs (split/merge/borrow) run unlatched (lock is load-bearing) | **CRIT** | OPEN |
| 4 | Split `clear()`-before-rebuild + count-based split → data loss | **CRIT** | ✅ DONE |
| 5 | Numeric comparisons done as strings → range queries wrong | **CRIT** | ✅ DONE |
| 6 | Compound `WHERE ... AND ...` returns zero rows | **CRIT** | ✅ DONE |
| 6b | PK range start-key lexicographic (wrong rows) | **CRIT** | ✅ DONE (opt deferred) |
| 7 | Recovery crashed / could erase committed rows (fetched unflushed pages) | **CRIT** | ✅ DONE (rebuild trees from WAL) |
| 8 | No write-ahead ordering → crash "commits" uncommitted data | **CRIT** | ✅ page-LSN + WAL-before-page gate DONE; WAL truncation still needs ARIES/eviction-coupling |
| 9 | Async commit default loses committed txns | **CRIT** | ✅ DONE (sync default + safe recovery) |
| 10 | No parameter binding (injection-only) | **CRIT** | ✅ binary path; HTTP still text |
| 11 | Two wire protocols; binary path broken | **CRIT** | ✅ REPLACED (cleanup owed) |
| 12 | WAL append/LSN unsynchronized | HIGH | ~ LSN atomic (fetchAdd); append-buffer mutex OPEN |
| 13 | Latch-ordering inversion / deadlock once concurrent | HIGH | OPEN |
| 14 | Background flush races page mutations → torn pages | HIGH | OPEN |
| 15 | Eviction/fast-flush bypass double-write | HIGH | OPEN |
| 16 | `compact()` 512-cell cap breaks delete/merge on dense pages | HIGH | ✅ DONE |
| 17 | Buffer-pool frame exhaustion under recursive SMO | HIGH | ✅ DONE (≥8 frames/segment floor) |
| 18 | Joins materialize + deep-clone; O(n²); no pushdown | HIGH | OPEN |
| 19 | ORDER BY + GROUP BY + COUNT/SUM/AVG/MIN/MAX | HIGH | ✅ DONE |
| 20 | `SELECT *` cardinality mismatch | HIGH | ✅ DONE |
| 21 | Parser panics on numeric overflow | HIGH | ✅ DONE |
| 22 | Read-Committed not snapshot; purge↔reader TOCTOU; unbounded committed set | HIGH | OPEN |
| — | unknown-type→TEXT + size=255 (✅ full type map), multi-row VALUES (✅), NOT NULL + DEFAULT (✅ enforced) | MED | ✅ DONE |
| 23 | Reserved words (ROLE/USER/KEY/…) can't be column/table names → parse error | MED | ✅ DONE |
| 24 | HTTP shares ONE global_executor across connections → concurrent-request tx-state race (data loss/corruption under load) | **CRIT** | ✅ DONE (per-request executor) |
| — | NULL sentinel / no NULL bitmap, AUTO_INCREMENT + UNIQUE unenforced, stale indexes, catalog no version tag, LSN regression, free-list not crash-safe, no checkpoint, debug prints, BETWEEN (✅ added) | MED | mixed |

---

## 4. Roadmap (phased, dependency-ordered) — with current position

**Phase 0 — Make it measurable & honest.** _(Open.)_ Wire thread/concurrency config to
`std.Io.Threaded`; strip debug `std.debug.print` (schnell per-byte, replication); add lock-wait
instrumentation around `db.rw_lock`; replace every parser `catch unreachable` with recoverable errors.

**Phase 1 — Single-threaded correctness.** _(In progress — ~half done.)_
✅ Typed numeric comparison (#5); compound boolean `WHERE` AND/OR (#6); PK range correctness (#6b);
`ORDER BY` execution (#19-part); implicit INSERT column list.
◻ Remaining: byte-aware + rollback-safe split (#4, **next**); `compact()` capacity (#16); order-
preserving numeric keys (§5); aggregate `GROUP BY`/`AVG/MIN/MAX` + re-verify `SELECT *` (#19,#20);
full SQL type map + NULL bitmap + basic constraint enforcement; multi-row `VALUES`; buffer-pool frame
sizing (#17); parser overflow (#21).

> **Update (2026-08-07) — #8 WAL-before-page ORDERING gate DONE (`3401d3a`).** The bg-writer/eviction/
> flush paths wrote dirty pages with no preceding WAL flush; now a decoupled WalGate hook makes the pool
> flush the WAL durable before ANY dirty page reaches disk (flushAllPages/Fast, flushPage, and a dirty
> eviction victim in fetchPage/newPage). Gate cleared on WAL teardown to avoid UAF; hardened a latent
> WAL.flushBuffer underflow (drop-with-warn instead of panic). Full suite green. This is the ordering
> invariant — the correctness core. STILL OPEN in #8: the per-page `page_lsn` field for finer per-page
> gating + recovery redo-idempotency. THEN checkpoint-safe WAL truncation + the recovery rebuild (see the
> kill -9 finding above) close out Phase 2.
>
> **Update 2 (2026-08-07) — page_lsn field + write-ahead groundwork DONE (`c3fcb37`); the per-page gate
> is BLOCKED on a mutation reorder.** Added `PageHeader.page_lsn`, pool `current_lsn` + stamping,
> `WAL.flushed_lsn` advancement on fsync, a `durable_lsn` accessor + a page-aware `walBeforePageLsn`
> helper, and `Database.reserveLsn`. The eviction gate stays COARSE for now. KEY FINDING that gates the
> rest: NovaDB mutations are **apply-then-log** (page is dirtied, THEN `wal.incrementLSN()`+`append` a few
> lines later — confirmed in the primary INSERT and the catalog paths). So a page stamped at dirty-time
> gets an LSN BELOW its own record → an unsafe skip. The per-page gate needs TRUE write-ahead: reserve
> each mutation's lsn(s) BEFORE applying (reserveLsn is ready), then append. That is ~17 `incrementLSN`
> sites, several PAIRED (two records/two applies per op) — a dedicated, per-site-verified reorder, not a
> batch edit. Until it lands, `current_lsn` is 0, `page_lsn` is inert, and the coarse gate holds
> correctness.

**Phase 2 — Crash-safe durability (independent of parallelism).** _(Started; found the items are
COUPLED.)_ Page-LSN + WAL-before-page gate on eviction/flush (#8 — ordering DONE, per-page-lsn open);
rebuild the undo log on boot (#7);
`synchronous_commit=true` default + force-log before page flush (#9); route ALL page writes through
double-write (#15); crash-safe free-list + periodic checkpoint + WAL truncation; group commit.

> **⚠ Finding (2026-07-14): #9 is blocked on #8 + recovery robustness.** Empirically flipping
> `synchronous_commit=true` makes a hard `kill -9` **reproducibly crash recovery on next boot**: Phase-2
> replay of a committed INSERT does `table_tree.insert` → `fetchPage(root)` → **InvalidChecksum beyond
> EOF** (database.zig:1118), because the catalog persisted the table's root page-id (via WAL) but the
> bg-flush never wrote that data page before the kill. This is latent with async commit too (whenever the
> bg-writer flushed the WAL pre-crash) — just timing-dependent. **So do them together:** (a) recovery must
> **rebuild each user table's tree from the WAL** (reset to a fresh empty root before Phase-2 replay — safe
> because there is no checkpoint truncation yet, so the WAL holds full history) rather than fetch
> maybe-unflushed pages; (b) add the page-LSN WAL-before-page gate so a data page never reaches disk before
> its log record; (c) then flip `synchronous_commit=true`. Reverted the default to false for now (a
> deterministically-crashing recovery is worse than silent async loss). Needs a crash-test harness
> (insert → kill-9 → boot → verify), not a batch edit. Graceful SIGTERM restart persists correctly today.

> **⚠ Update (2026-08-07) — reproduction built + the diagnosis is DEEPER than "#9 unflushed root".** Added
> a crash-test harness: `Database.crashSimulate()` + `PagePool.deinitNoFlush()` tear the db down like
> `close()` but WITHOUT the WAL checkpoint / header write / page flush, i.e. exactly a `kill -9` (WAL
> durable, all dirty pages discarded). The gated test `D7: kill -9 with unflushed pages ...` in `root.zig`
> drives it. **Finding: recovery does not reconstruct the user table AT ALL** — `SELECT` returns
> `TableNotFound`, and a probe of `table_roots` right after `loadCatalog` shows the table absent. So the
> failure is not merely a data page: the **DDL/catalog does not survive a crash where the master-tree
> (sys.objects/sys.tables) pages never flushed** (the header still points at the pre-split master root, and
> the WAL-replayed catalog inserts do not resurface the table). Two consequences: (a) the doc's earlier
> "reset user trees to fresh roots before Phase 2" idea is insufficient here — the table is not in the
> catalog to reset; and (b) an *unconditional* reset is outright WRONG because `close()` calls
> `wal.checkpoint()` which TRUNCATES the WAL, so on a clean restart the reset would drop committed rows the
> WAL no longer holds (verified: it broke the clean-recovery tests). **Correct fix = the coupled Phase-2
> set below, done together:** page-LSN WAL-before-page gate so the catalog pages + header never lag the
> WAL, checkpoint-safe WAL truncation (only past LSNs whose pages are durably flushed), then a recovery
> that trusts flushed pages + replays the safe WAL tail. This is a dedicated storage-engine effort; the
> harness above is the reproduction to gate it on. Reverted the recovery-logic experiments; the harness +
> the gated (skipped) reproduction are committed so the next pass starts from a red test, not a blank page.

**Phase 3 — Unlock parallelism (the flagged goal — gated on Phase 1/2 hardening).** _(Started: safe
prerequisites done.)_ ✅ WAL LSN atomicity (#12 partial, `fetchAdd`); ✅ P0 debug-print strip (per-byte
schnell print + REC: spam). ◻ Remaining and HARD: WAL append-buffer mutex (#12); latch-safe SMOs with
correct lock-coupling (#3,#13); flush↔latch coordination (#14); per-statement MVCC snapshot + bounded
committed set (#22); **then remove the global `db.rw_lock`** (#1). Re-benchmark YCSB. Model on Planck (§7).

> **Reality check (2026-07-15):** reads already scale (shared lock); the ceiling is on WRITES. Lifting it
> means CONCURRENT B+tree structure modifications (latch-safe split/merge/borrow) — the hardest, highest-
> risk piece, untestable without concurrency-stress + invariant-check infrastructure, and a wrong move
> silently corrupts the now-correct+durable engine. `main.zig` already runs `async/concurrent_limit=
> .unlimited`, so it is pure contention, not a config cap. Do this as a DEDICATED effort: build the stress
> harness first, then SMOs, then lock removal. Everything up to here (correctness + crash-safe durability)
> is banked and independent of it.

**Phase 3.5 — Session layer.** _(Largely DONE.)_ ✅ First-class pooled `Session` (owns executor +
prepared/portal maps + inline buffers) + `SessionPool` with reset-on-reuse + `MessageBufferPool` for
per-frame buffers (all Planck-modeled, in `proto/`); wired into `tcp_server`. Keep-alive loop already
existed. Verified: TCP multi-query, cross-connection reuse clears leaked tx state, harnesses green. ◻
Follow-ups (assessed 2026-07-15 — no clean high-value win): per-session **auth** is REDUNDANT (the
executor already enforces token auth + per-object permissions when `security` is on, and the binary
protocol routes through it); idle-timeout **reaper** needs an Io read-deadline primitive (absent) or a
hazardous cross-fiber stream close; encoder **output** pooling is perf-only (per-frame arena, moderate
friction). Deferred as low-value/high-friction — core session layer already delivers the value.

**Phase 4 — Wire protocol & driver.** _(Mostly DONE — see §A.2/§A.3.)_ Postgres-inspired binary
protocol built + wired; param binding + cursor streaming verified; Nova driver + HTTP JSON bridge
live. ✅ Dead-code cleanup: removed the uncalled old dual-protocol path from `tcp_server.zig` (`runLoop`,
`runBinaryProtocolStream`, `sendBinary*`, `writeQueryResponse`; 624→193 lines) + deleted stale
`storage/system.zig`. Kept `MessageBufferPool` (pub) for the P3.5 pooled Session layer. `proto/protocol.zig`
+ `common/proto.zig` can't be deleted yet — still used by `cli.zig`/`root.zig`/`replication.zig`. ◻
Remaining polish: TLS on the binary frame loop; protocol-level auth (stateful, per §3.5); NULL bitmap in
the wire (engine-side null support). ⛔ **On hold:** linear-memory driver transport
for co-located WASM (§B) — blocked on the Nova WASM compiler.

**Phase 5 — Query maturity & perf.** _(Query surface DONE; perf/iso still open.)_
✅ **Done (§A.5):** `GROUP BY`/`HAVING`/`OFFSET`/`DISTINCT`/`DESC` + `AVG/MIN/MAX` across scalar and
hash-aggregate paths; aggregate refs usable in `HAVING`/`ORDER BY`; multi-key ORDER BY mixed ASC/DESC.
Verified live via HTTP bridge + all four harnesses (23/23 unit, concurrency 3600/3600, pk_ordering,
crash 200-row durable). ⏳ **Still open:** predicate pushdown + streaming joins + build-side-by-
cardinality; snapshot isolation; secondary-index cleanup on UPDATE/DELETE; stats caching; per-row
allocation cuts.

**Phase 6 — Product goals (after the engine is solid).** YCSB benchmarks driven from Nova; a real Nova
web app ↔ NovaDB end-to-end; SSE hub (`ssehub`-modeled) — **the SSE-to-WASM-apps side is on hold**
with the WASM compiler; `nova init app` template refresh to the new minimal-API stack.

---

## 5. Deferred deep-dive: order-preserving numeric keys

> **Update (2026-07-15): order-preserving keys is now PERFORMANCE-ONLY, not correctness.** The PK
> regression harness (`tests/harness/pk_ordering.py`) PASSES on the current engine: numeric point
> lookups, `id>5` / `BETWEEN` ranges, `ORDER BY id` (numeric), update/delete-by-PK, and text PKs are all
> correct — because ORDER BY sorts via the typed `orderJson`, ranges full-scan + typed filter, and point
> lookups exact-match. So the only thing order-preserving keys would add is **index-accelerated numeric
> range scans** (YCSB-E) + natural numeric order without `ORDER BY`. Given its ~8-site silent-failure
> risk, it stays deferred until range-scan throughput actually matters — do it then as the dedicated pass
> below, guarded by `pk_ordering.py`.


PK range correctness (#6b) is fixed by disabling the lexicographic start-key seek for numeric PKs, at
the cost of full-scanning numeric ranges and returning rows in lexicographic key order when there is no
`ORDER BY`. The **proper** fix restores both index-accelerated numeric ranges and natural numeric
ordering: encode integer PKs with an **order-preserving** scheme so `memcmp` byte order == numeric
order — either **fixed-width big-endian i64 with the sign bit flipped** (handles negatives, exact,
8 bytes) or **zero-padded decimal** (simpler, non-negative-only). This must be applied consistently at
every PK-key build site and gated on the PK column type (TEXT/BLOB keep lexical).

**Binary-key safety — CONFIRMED SAFE (2026-07-14):** WAL `LogRecord.serialize` (common.zig) writes
`key`/`value` length-prefixed (`writeInt(u32,len)` + `writeAll`) and reads back exactly `key_len` bytes
— no null-termination/printability assumption, so `0x00` bytes are safe. Slotted pages store keys with an
explicit `key_len:u16`; the B+tree compares via `mem.order` (memcmp). MVCC packs versions into the
*value*, not the key. So the storage + durability layers are ready for binary keys.

**Why still DEFERRED:** the change must touch **~8 spread PK-key sites**, each gated on the PK column
type — INSERT (`final_pk`, query_executor.zig:876), import-INSERT (:1376), UPDATE (`search`:1493,
`writeNewVersion`:1599, `task.key`), DELETE (`search`:1660, `deleteRowVersion`:1666), point lookup
(`getEqualityValueForCol` → `PrimaryKeyScanIterator`), range (`getStartKeyForCol`). The failure mode is
**silent** (miss one site → lookups miss the row → looks like data loss), it changes the on-disk key
format (existing DBs need migration), and correctness is *already achieved* via the full-scan fallback.
So do it as a dedicated pass with a single `encodePkKey(col_type, val)` helper wired into all sites at
once + a regression harness (insert/point/range/update/delete across numeric + text + 2-digit PKs) — not
folded into a batch. Tracked, not dropped.

---

## 6. What's solid — do NOT rewrite
- B+tree split/merge/borrow/root-collapse + overflow pages (single-threaded correct).
- Slotted page layout, two-way growth, in-place compaction (bar the 512 cap).
- 16-segment CLOCK buffer pool structure; doublewrite + CRC32 torn-page algorithm.
- Catalog/DDL persistence via WAL + system B+Trees, rebuilt on boot (the most mature subsystem).
- Iterator latch/pin discipline (`pinned_page_id` guards — verified no double-unlock).
- WAL record framing + CRC + torn-tail-tolerant replay (the *format* is fine; the *protocol wiring*
  is what §2.4 fixes).
- The new `proto/` wire stack (wire/oidmap/command/session) — the standardized protocol going forward.

---

## 7. TCP / session layer — Planck (LSM) as the reference for thread scaling

Planck (LSM-tree DB, `plancksystems/plancks/planck/src/tcp`) scales worker threads with load; NovaDB
flat-lines at ~5. Both use the **same** dispatch primitive — `group.async(io, handleConnection, …)`
per accepted connection over `std.Io.Threaded`. So the difference is **not** the TCP dispatch; it is
(a) engine lock granularity and (b) session handling.

### 7a. Engine lock granularity (→ Phase 3)
`std.Io.Threaded` spawns a worker whenever a new async task arrives and all current threads are busy —
so thread count tracks **how many connection-fibers make real parallel progress**.
- **Planck** uses **several short-held, purpose-specific locks** (`wal_mutex`, `db_mutex`,
  `primary_index_mutex`, `catalog_mutex`, `seq_mutex`) each around a small critical section, plus
  `getUnlocked` read paths (LSM immutable SSTables → lock-free reads). Many fibers proceed → the pool
  grows.
- **NovaDB** takes **one `db.rw_lock` EXCLUSIVE across the entire statement** (traverse + mutate +
  undo + WAL + fsync). Writes are single-file; the pool grows to a handful and stops.

**Fix:** replace the statement-wide `db.rw_lock` with several short-held locks (`wal_mutex` for append
+ LSN; the existing page latches for tree mutation — currently dead weight; a `catalog_mutex` for DDL)
+ short/lock-free read paths — **after** the P3 latch-safe-SMO hardening so the page latches are
actually correct. Then remove `db.rw_lock`.

### 7b. Session handling (→ Phase 3.5)
**Planck** has a first-class pooled `Session` (own read/write buffers, `authenticated`,
`idle_timeout_ms` + `last_activity_ms`, `security_session`, reset-on-reuse via `SessionPool`,
`active_connections` backpressure, keep-alive `session.run()` loop). **NovaDB** builds a whole
`QueryExecutor` per connection (unpooled), has no `Session` object, no idle-timeout, no per-session
auth flag. Adopt Planck's pattern: first-class `Session` + `SessionPool` (reset-on-reuse), stateful
per-session auth, idle reaping, keep-alive loop; keep the existing `active_connections` backpressure.

> Net: the thread cap and "sessions handled incorrectly" are two sides of the same coin — the engine
> serializes so fibers can't run in parallel, and there's no real per-connection session object to
> carry state. Planck fixes both with fine-grained short locks + a pooled Session model.
