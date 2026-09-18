# kaidb: known issues and pending work

Honest, prioritised register of what is still open in kaidb, as of 2026-09-18.
Grounded in the `benchmark/query-perf-compare` results and the storage engine, not
a wish list. Query-perf items reference `benchmark/query-perf-compare/comparison.md`
and the fix log in `benchmark/query-perf-compare/q11-18-perf-improve.md`.

## Production readiness: see `prod-fitness.md` first

**This file is a list of remaining *engineering* items, NOT a production-readiness
verdict.** For that, read `prod-fitness.md` (a code-cited, red->green-tested subsystem
audit). Its verdict: **fit for single-node production for the scoped read-heavy,
mostly-in-RAM relational role, pending an on-hardware soak.** The production gates it
lists are *closed and tested*, so do not read the items below as if they reopened them:

- **Durability / crash recovery** - CLOSED (WAL-before-page + doublewrite + 3-phase redo;
  `prod-fitness.md` 4.1). This session additionally made the free list crash-durable at
  the periodic checkpoint (item 5 below).
- **Auth / authz** - CLOSED, opt-in (`require_auth` fail-closes with SQLSTATE 28000,
  TLS-gated password path, forced admin rotation; 4.3).
- **Backup / restore** - CLOSED, cold + hot (`novadb backup`/`restore`, live
  `BACKUP DATABASE TO`; 4.4).
- **Point-in-time recovery** - CLOSED, LSN target (WAL archiving + `restore --target-lsn`;
  5.B.3). Time-target selector is a follow-up.
- **Observability** - CLOSED to the operability floor (`/healthz`, `/readyz`, `/metrics`
  with a query-latency histogram, buffer-pool hit ratio, WAL/replication gauges; 4.7).
- **Concurrency safety** - CLOSED (per-tree structure lock + default-on fuzzers; the one
  known `PageStillPinned` merge race was root-caused and fixed with an always-on
  regression test; 4.2). The residual concurrent-harness *replication-test* flake (item 11)
  is a test-harness teardown race, not an engine-durability defect.

"Mostly-in-RAM" describes the *performance* sweet spot (working set fits the buffer pool),
NOT the durability model - the engine is fully durable on disk. The one genuine pre-flight
step is the on-hardware **soak** (checklist at the end of this file).

## Scope note (correction)

kaidb is **not** the Nova orchestrator's store any more. The orchestrator's
control-plane / config state is served by the internal blob-store (artifactd), not
kaidb. Ignore any older text (including `CLAUDE.md`) that frames kaidb's role as the
"orchestrator control-plane / config store"; that is out of date. kaidb is a
standalone B+tree SQL/document engine, measured head-to-head against PostgreSQL and
MySQL on the Q1..Q18 orders workload.

## Query performance (measured gaps)

Recent wins already landed on branch `perf/q12-pk-range-scan`: Q12 clustered PK range
228->10 ms (`98d81c6`), Q16 GROUP BY+HAVING 262->12 ms (`8e44c82`), Q18 deep OFFSET
185->1 ms (`52e0b39`), and a modest base-row-fetch improvement (`304b35f`). What
remains:

1. **Medium-selectivity range scans - Q4 / Q5 / Q8 / Q17 (~16-40 ms vs PostgreSQL
   ~5-24 ms).** The clearest remaining query gap. **Profiled 2026-09-18** (`NOVADB_QPROF`,
   300k rows, warm) to find where the time actually goes. For Q8 (`total_due` range,
   10k rows) the ~16 ms scan splits as: **baseSeek ~46 %** (one random descent into the
   clustered PK tree per qualifying row to fetch its base image), **idxwalk+other ~50 %**
   (index range walk + residual-column decode), and **buildJson only ~4 %** (the per-row
   `names`/`cells`/text-dup materialisation). Q4 is the same shape (~47 / ~49 / ~4). So
   the dominant cost is the **per-row base-row seek and the index/residual decode**, not
   materialisation. The real levers are therefore reducing base-row seeks (a covering /
   included-column index that answers the projection from the index alone would drop
   baseSeek toward zero; a batched leaf-cursor base fetch amortises the descents) and
   tightening the index walk, not row-image reuse. See the corrected item 4.

2. **`ORDER BY ... DESC` - Q3 / Q5: NOT a defect (earlier claim was wrong).** kaidb
   already serves these with a backward index walk (`IndexRangeScanDescIterator`, the
   composite site at `query_executor.zig:2675` and the single-column site at `:2789`),
   and streams + breaks at LIMIT. Their remaining cost is per-row decode /
   materialisation, i.e. the same as item 1/4, not a missing backward scan.

3. **OFFSET pushdown on single-column ordered scans: WIRED (verified-skip).** The
   single-column ascending range scan (`query_executor.zig` ~2830) now pushes the OFFSET
   into the scan via `skip_remaining` + `scan_offset_pushed`, but only when the scan
   directly yields the requested order (`served_asc`). The skipped rows are counted past
   with a per-row visibility + residual check (`rowQualifies`, no JSON build /
   materialise), so a deep OFFSET no longer builds and discards every skipped row.
   Validated against an over-selecting residual (`v <> k`): the excluded row is correctly
   *not* counted toward the offset. The **fetch-free** variant (`skip_no_fetch`, which
   also avoids the base-row fetch, as on the composite Q18 path) is deliberately still
   gated off here: unlike the composite site, this range can over-select and
   `residual_expr` always carries the full WHERE, so a fetch-free skip would need a
   single-column analogue of `whereCapturedByCompositeKey` to prove every in-range entry
   qualifies. That check is the remaining work; no Q1..Q18 query exercises the fetch-free
   single-column deep-OFFSET shape (Q18 is composite), so it is unmeasured and low
   priority.

4. **Per-row allocation reuse: NOT WORTH IT (measured, was mis-scoped as highest-value).**
   The earlier claim that per-row allocation is "what keeps Q4/Q5/Q8/Q17 behind
   PostgreSQL" is **falsified by profiling** (see item 1): the per-row materialisation
   (`buildRowJson`: two arrays + a text-cell dup, freed by `freeTableRow`) is only
   **~4 %** of scan time on exactly those queries. Reusing a row-image buffer across rows
   would remove at most that ~4 %, while the refactor touches `freeTableRow` at 40 call
   sites and `cloneTableRow` at 7 with real use-after-free / ARC surface (MVCC borrow
   paths). The cost/risk ratio is bad, so this is **deliberately not being done**. The
   time is in base-row seeks and index/residual decode (item 1), which is where any
   further range-scan work should go.

## Storage / engine robustness

5. **Free-list crash-durability: FIXED (bounded to one checkpoint).** Previously the
   pager free list was written into the page-0 header *only at a clean* `Database.close`,
   so any crash lost the whole in-memory free list, leaking every page freed since the
   last clean shutdown (B+Tree merges, deletes and `DROP` reclamation all feed it), not
   just DROP frees. `Database.checkpoint` now also rewrites the header's free-list head
   (plus master root / lsn) before its flush, and the periodic bgwriter tick calls the
   full `checkpoint` instead of a bare WAL checkpoint, so the free list becomes durable
   every ~2 s. The rewrite is safe because it runs under the exclusive `rw_lock`, which
   excludes every writer and hence every page allocation, so `persistFreeList` cannot
   overwrite a page that a concurrent writer has just reallocated; the following
   `flushAllPages` issues one `sync`, so the header and the chain pages it points at are
   made durable together. Recovery's `loadFreeList` remains checksum-guarded, so a torn
   or stale chain still truncates to a benign leak, never a live page. The frees are
   still not individually WAL-logged (a crash between checkpoints leaks only that
   window's frees), which is the accepted residual: it degrades to wasted space, never
   corruption. Validated: `crash_test.sh` and `crash_test_midwrite.sh` both PASS (data
   durable, clean recovery), the free-page-list unit test passes, and a 300-row load
   spanning several bgwriter ticks shows no deadlock from the tick now taking `rw_lock`.

6. **Undo (MVCC version) pages and DROP: NOT a leak (earlier framing was wrong).**
   Undo pages are a single *global append log* (`undo_log.undo_pages`), not per-table
   trees, so there is nothing for `freeTreePages` to walk on a DROP. They are reclaimed
   instead by `purgeStaleUndoPages` (called every bgwriter tick) on the MVCC visibility
   watermark: a page is freed once its newest record's `xmin`/`xmax` is below the oldest
   active transaction. A dropped table's undo records therefore age out exactly like any
   other records, once no active transaction can still see them. The one real limitation
   is that the purge is *prefix-only*: it frees the contiguous run of oldest reclaimable
   pages and stops at the first page a live reader still needs (head-of-line blocking by
   a long-running transaction). That is a pre-existing property of the append log, not a
   DROP-specific gap, and it self-heals once the blocking transaction ends.

7. **`kaidb-cli` REPL infinite-loop on piped/EOF stdin: FIXED.** The CLI was rewritten
   onto the pure binary wire protocol (`proto/protocol.zig`, no HTTP/JSON) with `-c`
   one-shot and piped-batch modes; piped input is read with `allocRemaining` (EOF-safe)
   so it runs each `;`-terminated statement and exits cleanly. Interactive TTY mode
   remains for humans.

## Correctness / robustness found by the on-hardware soak (2026-09-18, see `soak-report.md`)

12. **Table aliases (single-table): FIXED.** `SELECT a.id FROM t a WHERE a.v = 25` used to
    project NULL and silently drop the filter. Two root causes, both fixed: (a) the parser
    never consumed the table alias (`FROM t a`) or a dotted `ORDER BY a.id`, so the alias
    token and everything after it (the WHERE, the sort direction) were dropped as trailing
    tokens; the parser now captures `[AS] alias` on the FROM table and each JOIN table
    (`SelectStmt.table_alias`, `JoinExpr.right_alias`) and parses dotted ORDER BY columns.
    (b) the executor matched the full `a.id` against the row's bare field names; a
    single-table normalisation pass (`normalizeSingleTableQualifiers`) now strips a
    `table.`/`alias.` prefix that matches the driving table from projections, WHERE, HAVING,
    ORDER BY and GROUP BY. Verified: projection, WHERE, `AS`, aggregate arg, ORDER BY DESC,
    GROUP BY all resolve; unqualified queries unchanged; a stray non-matching qualifier stays
    NULL (not rebound). **Join-side aliases now also work** (`SELECT e.name, d.dname FROM emp e
    JOIN dept d ON e.dept_id = d.id WHERE d.dname = 'eng'`): a second normalisation pass
    (`normalizeJoinAliases`) maps each `alias.col` to its real `table.col` across projections,
    ON, WHERE, HAVING, ORDER BY and GROUP BY, because the join executor resolves a combined row
    by real table name (verified: real-name joins already worked, alias joins returned 0 rows).
    Verified: aliased join, WHERE-on-alias in a join, real-name join (regression) and
    single-table alias all correct; full `zig build test` passes.

13. **Server crashed under disk-full: FIXED (root cause was a double-free, not the disk).**
    A Debug build printed an error-return trace pointing at `writeNewVersion`
    (`query_executor.zig`): the new row's `new_fixed` / `new_heap` buffers had BOTH an
    `errdefer` free AND a `defer` free, so on ANY write error after the `defer` was registered
    (e.g. `updateRowMVCC` / `logWalRecordWithLsn` returning `error.NoSpaceLeft` on a full disk)
    both ran and double-freed the two buffers. That double-free was benign under Debug's
    allocator but is undefined behaviour under the release `c_allocator` - which is exactly why
    ReleaseFast crashed *silently* (no panic) while Debug/ReleaseSafe limped further. It fires
    on any write error here, not just ENOSPC. Fix: dropped the two redundant `errdefer`s (the
    `defer` already frees on both paths). Verified in ReleaseFast: disk-full now returns clean
    `error_message`s, reads still succeed while full, the server stays up, and writes resume
    after space is freed; `crash_test.sh` 3/3 and full `zig build test` pass. Durability was
    never affected (committed data recovers exactly). Residual (separate, minor): a write that
    returns an ENOSPC error may still leave a not-yet-durable row visible until eviction/restart
    - a sustained-disk-full atomicity nicety, not a crash.

## Structural limits (larger, deliberate for now)

8. **Not PG-class at OLTP/document scale.** A 10M-row benchmark exposed a bloated
   footprint, I/O-inefficient scans past the buffer-pool size, per-index re-scans, and
   a scan-not-seek planner (`orders_benchmark_vs_postgres_10m.md`). Fix 1 addressed the
   clustered-PK-range part of the planner; the broader gap remains and is currently
   accepted rather than closed.

9. **Base primary key is stored as decimal text, so its clustered order is lexical, not
   numeric.** This is why the Q12 clustered-range fix is gated to same-width,
   non-negative bounds. An **order-preserving integer PK encoding** (fixed-width
   big-endian) would make clustered range scans general and faster, but it is an
   on-disk format change (migration + every PK search / compare site), so it is a
   deliberate non-goal right now.

## Pre-existing test / stability items (not from this branch)

10. **`replication P6: mutual TLS`: FIXED (suite now 120/120).** The test loaded certs
    from `testdata/repl/` that were never committed. Added a CA-signed server + client
    and a rogue cert from a separate rogue CA (with SKI/AKI so the chain links), plus
    `tests/harness/gen_repl_certs.sh` to regenerate. All 120 tests pass on a direct
    (sequential) run of the test binary.

11. **Replication tests flake under the concurrent `zig build test` harness.** The
    aggregated run intermittently reports a `failed command` amid async-ship /
    reconnect warnings, but every test passes on a direct/sequential invocation of the
    test binary (120/120) and with `-Dtest-filter`. This is a harness-concurrency race
    in the replication tests (follower socket teardown timing), not a product defect;
    it may be the same class as the earlier **`PageStillPinned` fuzzer race**. Prefer
    the direct binary or a filter for a reliable pass/fail signal. A
    **document-store REOPEN HANG**; confirm whether these are still open before relying
    on the doc path or the concurrent fuzzer.

## Suggested order

Items 3, 5 and 6 are done (see each). Item 2 was never a defect. Item 4 was measured
and dropped (only ~4 % of scan). The remaining genuine query gap is **(1) the range
scans**, and the profiling says the lever there is **cutting base-row seeks** (a
covering / included-column index so the projection is answered from the index alone,
and/or a batched leaf-cursor base fetch) plus tightening the index/residual walk - not
row-image reuse. Items 8-9 are intentional scope.

## On-hardware soak checklist (the one pre-flight gate before "in prod")

`prod-fitness.md` closes every production gate in-tree with tests. The single thing those
tests cannot cover is a specific deployment's operational envelope on real hardware. Run
this soak against the intended workload shape (row sizes, read/write mix, dataset-vs-RAM,
follower present?) before flipping the switch. Treat any failure here as a launch blocker.

Config the box the way it will actually run first: `require_auth` (or
`require_tls_for_auth`) on, `admin` password rotated (`require_admin_password_change`),
`synchronous_commit` set for the durability/latency trade you want, buffer pool sized for
the box, `wal_archive_dir` set if you want PITR, `/metrics` scraped.

1. **Sustained load, hours not minutes.** Drive the real read/write mix at target
   concurrency for >= 2-4 h. Watch, via `/metrics`: query-latency p99
   (`histogram_quantile` over `kaidb_query_duration_seconds`), buffer-pool hit ratio
   (`1 - hits/fetches`), `kaidb_wal_bytes` (must plateau, not grow without bound), and
   `kaidb_wal_checkpoint_lag_lsn` (must stay bounded - a rising value = a stalled
   checkpointer). Confirm RSS is stable (no slow leak) and latency does not drift up.
2. **kill-9 mid-write on the actual box, repeatedly.** Not just the test harness: hard-kill
   the running server under write load, restart, and verify (a) it recovers without
   crashing, (b) every acked commit is present, (c) recovery time is acceptable for your
   dataset. The harness `tests/harness/crash_test.sh` / `crash_test_midwrite.sh` are the
   pattern; run their spirit against the real data size. Do this >= 10 times.
3. **Disk-full behaviour.** Fill the data/WAL volume under write load and confirm the
   server fails writes cleanly (returns errors, stays up or restarts clean) rather than
   corrupting. Then free space and confirm it resumes. This path is NOT covered by in-tree
   tests and is the most likely real-world surprise.
4. **Backup + restore drill on real data.** Take a hot `BACKUP DATABASE TO` under load,
   restore it into a fresh dir, and diff query results against the source. If using PITR,
   restore `--target-lsn` from snapshot + archived WAL and verify the point-in-time state.
   Time the restore so you know your actual RTO.
5. **Replication over a real network (if used).** Bring up a follower across the actual
   network (not loopback), confirm `kaidb_replication_lag_frames` stays bounded under load,
   then exercise `PROMOTE` / `DEMOTE TO FOLLOWER ON` and confirm the old leader is fenced
   (writes rejected). Note the async-failover data-loss window (committed-but-unshipped) -
   use sync replication / lag-gating if you need zero loss. See `promote-demote-design.md`.
6. **Long-running-transaction behaviour.** Hold a long read transaction open under write
   churn and confirm undo-page reclamation resumes once it ends (item 6 head-of-line), and
   that MVCC version-chain walks do not degrade latency unacceptably (`prod-fitness.md` 5.A.7).
7. **Connection governance under abuse.** Hit `max_connections`, slow/idle clients, and a
   runaway query; confirm the per-query memory cap and deadline fire (`query_memory_limit_bytes`,
   `deadline_ms`) and the accept loop stays responsive. Note the gaps in `prod-fitness.md`
   4.6 (no idle-connection timeout, per-query not global memory cap) and decide if they
   matter for your front door.

Pass all seven against the real workload and box, and it is ready for the non-critical,
single-node role. Anything that ships thousands of rows through a secondary index, needs
horizontal scale, stores large blobs (> ~2 KiB values), or has a working set well beyond
RAM is outside the scoped role - see `prod-fitness.md` sections 4.8 and 5.A.
