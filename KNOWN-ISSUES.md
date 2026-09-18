# kaidb: known issues and pending work

Honest, prioritised register of what is still open in kaidb, as of 2026-09-18.
Grounded in the `benchmark/query-perf-compare` results and the storage engine, not
a wish list. Query-perf items reference `benchmark/query-perf-compare/comparison.md`
and the fix log in `benchmark/query-perf-compare/q11-18-perf-improve.md`.

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
   ~5-24 ms).** The clearest remaining query gap. The PK-sorted base-row fetch only
   trimmed these; the dominant cost is per-row **decode / materialisation**, not the
   index descent (`buildRowJson` in `src/query/query_executor.zig` allocates a `names`
   and a `cells` array per row and dups every text cell). See item under "Allocation".

2. **`ORDER BY ... DESC` - Q3 / Q5.** kaidb scans ascending then reverses, so it cannot
   stream and MySQL/PostgreSQL win via a backward index walk. Needs a **backward index
   scan**; it would also let these queries stream (break at LIMIT) instead of
   buffering and reversing.

3. **OFFSET pushdown is only half-wired.** The Q18 fix pushes the offset into the
   *composite* ordered index scan only. **Single-column ordered range scans still
   buffer-and-discard** on a deep OFFSET - the same `skip_remaining` /
   `scan_offset_pushed` mechanism needs wiring at that planner site too.

4. **Per-row allocation reuse (was "Fix 5", not started).** Every emitted row
   allocates two arrays plus a dup per text cell and frees the prior row - a real cost
   on any multi-thousand-row scan (this is what keeps Q4/Q5/Q8/Q17 behind PostgreSQL).
   Reuse a row-image buffer across rows. Broad win, but riskier (ownership / ARC
   interactions, MVCC borrow paths), so it was deliberately left for last. This is the
   highest-value next perf item.

## Storage / engine robustness

5. **DROP page-freeing is not WAL-logged.** `dropTable` now reclaims the base + index
   tree pages (commit `1fc8ecb`, `Database.freeTreePages`), but the frees are not
   logged. A crash mid-drop leaks the not-yet-freed pages (the pager persists its free
   list only at a checkpoint). This degrades to wasted space, never corruption, but
   logging it would make reclamation crash-durable.

6. **Undo (MVCC version) pages are not reclaimed by DROP.** `freeTreePages` walks the
   base and secondary-index trees; undo-log pages holding prior row versions are a
   separate reclamation path. Harmless for a load-once workload, but real for
   update / delete-heavy churn.

7. **`kaidb-cli` REPL infinite-loop on piped/EOF stdin: FIXED.** The CLI was rewritten
   onto the pure binary wire protocol (`proto/protocol.zig`, no HTTP/JSON) with `-c`
   one-shot and piped-batch modes; piped input is read with `allocRemaining` (EOF-safe)
   so it runs each `;`-terminated statement and exits cleanly. Interactive TTY mode
   remains for humans.

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

10. **`replication P6: mutual TLS`** test fails (suite runs 119/120). A cert-path issue
    in the TLS replication test, unrelated to the query engine.

11. Earlier triage flagged a residual **`PageStillPinned` fuzzer race** and a
    **document-store REOPEN HANG**; confirm whether these are still open before relying
    on the doc path or the concurrent fuzzer.

## Suggested order

The highest-value next item is **(4) per-row allocation reuse** - it is the actual
bottleneck on the range scans where PostgreSQL still wins, and it helps every scan.
Then **(2) backward index scan** for the `ORDER BY DESC` queries, and **(3)** the
single-column OFFSET pushdown. Items 5-7 are low-severity robustness; 8-9 are
intentional scope.
