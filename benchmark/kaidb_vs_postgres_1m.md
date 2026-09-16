# kaidb vs Postgres 18.4, 1M orders, head-to-head

Same workload (identical schema, same 4 indexes, same generator data), 1M rows,
on the same 8 GB machine. Both measured as **full client round-trip** fetching
all result rows, **warm** (cache hot; 1M fits entirely in RAM on both sides).

- kaidb: default config (auto buffer pool = 4 GB / ~50% RAM, async prefetch ON),
  measured by the YCSB harness (`orders-sql-query`), steady-state pass.
- Postgres 18.4 (`novabench.orders`, default config), measured with psql
  `\timing`, output discarded, warm (2nd) pass.

| Query | kaidb (ms) | PG (ms) | winner |
|-------|-----------:|--------:|--------|
| Q1 employee_id=279 LIMIT 10000          |  91 | 22 | PG 4.1x |
| Q2 emp + total_due>10k LIMIT 10000      |  68 | 18 | PG 3.8x |
| Q3 Q2 ORDER BY total_due DESC LIMIT 10000 | 148 | 19 | PG 7.8x |
| Q4 total_due>50k LIMIT 5000             |  79 | 14 | PG 5.6x |
| Q5 Q4 ORDER BY total_due DESC LIMIT 5000|  65 |  9 | PG 7.2x |
| Q6 customer_id=1045 LIMIT 10000         |  22 |  3 | PG 7.3x |
| Q7 employee_id IN (3 vals) LIMIT 10000  |  31 | 16 | PG 1.9x |
| Q8 total_due 10k..50k LIMIT 10000       | 139 | 10 | PG 13.9x |
| Q9 COUNT(*) employee_id=279             |   1 |  2 | kaidb 2.0x |
| Q10 AVG(total_due) GROUP BY emp top 5   |  29 | 61 | kaidb 2.1x |

## After ripping std.json out of the SQL row path

The result-row path previously built a `std.json.Value` object per row (an
`ObjectMap` hashmap + per-column key dups + string-wrap) and read it back by
name. That was replaced with a positional `TableRow` (borrowed column names +
text cells, no hashmap, no per-row key dups); the expression evaluator gets a
transient scalar through a thin seam so its logic is unchanged. Full SQL test
suite stays green; result row counts unchanged.

Effect at 1M warm (kaidb ms, before -> after, vs PG):

| Query | before | after | PG | gap now |
|-------|-------:|------:|---:|--------:|
| Q1 employee_id=279       |  91 | 54 | 22 | PG 2.5x |
| Q2 emp + total_due       |  68 | 54 | 18 | PG 3.0x |
| Q3 emp ORDER DESC        | 148 | 73 | 19 | PG 3.8x |
| Q4 total_due>50k         |  79 | 37 | 14 | PG 2.6x |
| Q5 Q4 ORDER DESC         |  65 | 37 |  9 | PG 4.1x |
| Q6 customer_id=1045      |  22 | 12 |  3 | PG 4.0x |
| Q7 employee_id IN        |  31 | 27 | 16 | PG 1.7x |
| Q8 total_due range       | 139 | 86 | 10 | PG 8.6x |
| Q9 count                 |   1 |  0 |  2 | kaidb   |
| Q10 group by             |  29 | 25 | 61 | kaidb 2.4x |

The rip roughly **halved** the row-returning latencies (Q1 1.7x, Q3/Q4 ~2x). The
remaining gap to Postgres on wide row-return (Q8 worst, a range scan shipping
10000 rows each carrying the large `details` text column) is now the per-cell
text encoding and shipping the untouched `details` blob through the wire, not a
JSON object build. Next levers: binary result encoding for numerics, and passing
the stored `details` bytes straight to the wire without recopying.

## Binary numeric wire encoding (opt-in)

With the row cells now typed (`query_iter.Cell`), numeric columns can ship as
big-endian fixed-width binary on the wire instead of decimal text, skipping the
per-cell `dtoa`/`itoa` and shrinking bytes. Gated behind `NOVADB_BINARY_RESULTS`
(default text, so existing clients are unaffected); the RowDescription marks the
numeric columns binary. Measured on the single-table `SELECT *` path, 1M warm,
same-run text vs binary:

| Query | text | binary | delta |
|-------|-----:|-------:|------:|
| Q1 employee_id=279  | 30 | 24 | -20% |
| Q2 emp + total_due  | 39 | 32 | -18% |
| Q3 emp ORDER DESC   | 41 | 34 | -17% |
| Q4 total_due>50k    | 19 | 16 | -16% |
| Q5 Q4 ORDER DESC    | 20 | 17 | -15% |
| Q6 customer_id=1045 |  6 |  4 | -33% |
| Q7 employee_id IN   | 28 | 21 | -25% |
| Q8 total_due range  | 40 | 31 | -22% |
| Q9 count            |  0 |  0 |  0%  |
| Q10 group by        | 24 | 24 |  0%  |

15-33% off the wide-row SELECTs; row counts identical (correctness preserved).
Q9/Q10 ship no numeric row cells so are unaffected. The `kyte-kaidb` driver now
decodes binary cells: `decodeRowDesc` captures the per-column format code,
`decodeDataRow` branches on it, and `typemap.decodeCellBinary` reads big-endian
int4/int8/uint/float4/float8/bool (floats via the `kyte_ieee_le_to_str` builtin);
a `test_datarow_binary_decode` unit test covers int/float/bool round-trips. The
remaining gap to Postgres is
now dominated by the per-row base-table search+decode and the `details` blob
copy, not number formatting.

## Base-cursor reuse and column-offset caching (SQLite/PG-inspired, opt-in)

Studying SQLite (`sqlite3BtreeTableMoveto` reuses a persistent cursor and can
start the search on the current page, bypassing a root descent) and Postgres
(the index stores a physical TID so `heap_fetch` is a direct page read, no
descent) pointed at two levers for the per-row base fetch:

- **Lever 1, base-cursor reuse** (`NOVADB_BASE_CURSOR`, default off): the
  index-scan operator holds one `BPlusTree.LeafReuseSearcher` across all its base
  lookups; if the next PK falls within the currently-latched leaf's key range it
  skips the root-to-leaf descent entirely, mirroring SQLite's current-page fast
  path.
- **Lever 2, column-offset decode** (always on): the all-columns `SELECT *` path
  now decodes each cell straight from `schema.Column.offset`/type
  (`readCellFromCol`) instead of the per-cell linear `Table.getColumn` name scan,
  mirroring PG `attcacheoff` / SQLite `aOffset[]`.

Measured at 1M warm (cache hot), base-cursor OFF vs ON, both binary results,
identical row counts on every query (10000/10000/10000/5000/5000/1573/10000/10000/1/5):

| Query | off (ms) | on (ms) |
|-------|---------:|--------:|
| Q1 employee_id=279  | 25 | 23 |
| Q2 emp + total_due  | 32 | 31 |
| Q3 emp ORDER DESC   | 34 | 33 |
| Q4 total_due>50k    | 17 | 16 |
| Q5 Q4 ORDER DESC    | 17 | 17 |
| Q6 customer_id=1045 |  5 |  4 |
| Q7 employee_id IN   | 20 | 20 |
| Q8 total_due range  | 34 | 31 |
| Q9 count            |  0 |  0 |
| Q10 group by        | 24 | 24 |

Honest verdict: base-cursor reuse is **correct and never regresses**, but the
warm gain is 1-3ms, within measurement noise. The reason is structural: a
secondary index yields PKs in random order relative to the base B+tree, so
consecutive lookups seldom hit the same base leaf and the reuse fast path rarely
fires (the same lesson as the bitmap PK-reorder: fetch ORDER is not the lever
here). It is kept env-gated, default off, because it does help when PKs cluster
(covering-index or PK-ordered scans) and in the disk-bound regime. Lever 2 is
free and stays on but is also marginal warm, since `getColumn` was only ~5% of
samples. The warm gap to Postgres remains the per-row base search + cell decode +
the untouched `details` blob copy, not descent count or name lookups. The
Postgres-style fix (a physical row locator in the secondary index, so the base
fetch is a direct page read with no descent at all) is the real endgame lever.

## Blob zero-copy on the wire path (details TEXT column)

First: the comparison is genuinely apples-to-apples. Both sides run identical
`SELECT *` (the whole row, including the `details` TEXT column), and both
datasets came from the same generator, so PG's `details` (avg 341 B, max 628 B,
zero NULLs) matches kaidb's byte-for-byte. PG returns the same bytes and still
wins, so the gap is a real per-row-pipeline difference, not PG shipping less.

Profiling the wide-row losers showed the `details` blob was copied **twice** per
row on the way out: once when the row was materialised (`readCellFromCol` dupes
the heap bytes into an owned `.text` cell) and again at wire-emit (`toTextAlloc`
re-dupes the cell into the `row_cells` buffer that becomes the DataRow payload).
Postgres materialises a tuple into its output buffer roughly in place; it does
not pay that second copy. The fix: on the hot single-table `SELECT *` drain,
**move** the already-owned `.text` slice straight into the wire buffer instead of
re-duping it, and null the source cell so the iterator's `freeTableRow` does not
double-free the transferred bytes (the row is fully drained before the iterator
advances, so the cell is never read again). This removes one full blob copy and
one heap allocation per row, with no change to the `Cell` ownership model
anywhere else (joins, WHERE eval, aggregates untouched).

Measured 1M warm, binary results, row counts identical
(10000/10000/10000/5000/5000/1573/10000/10000/1/5):

| Query | before (ms) | after (ms) |
|-------|------------:|-----------:|
| Q1 employee_id=279  | 25 | 22 |
| Q2 emp + total_due  | 32 | 30 |
| Q3 emp ORDER DESC   | 34 | 33 |
| Q4 total_due>50k    | 17 | 15 |
| Q5 Q4 ORDER DESC    | 17 | 16 |
| Q6 customer_id=1045 |  5 |  5 |
| Q7 employee_id IN   | 20 | 18 |
| Q8 total_due range  | 34 | 30 |
| Q9 count            |  0 |  0 |
| Q10 group by        | 24 | 24 |

~8-12% off the row-returning queries (Q8 the headline, -12%), none regressed.
That brings kaidb's blob copy-count to parity with Postgres (one copy each), so
the residual Q8 gap to PG (30 vs 10 ms) is no longer the blob copy: it is the
per-row base search + cell decode + DataRow framing. A further true-zero-copy
step (borrow the blob straight from the row's heap buffer, eliminating even the
materialisation copy) is possible but needs the `.text` cell to become borrowed,
which changes the ownership model broadly; deferred as higher-risk, lower-return.

## Honest read

- **kaidb matches/beats Postgres when little or no row data crosses the wire**:
  Q9 (count, 1 row out) and Q10 (group-by, 5 rows out) are kaidb wins. The scan,
  index selection, and aggregation machinery are competitive.
- **kaidb loses 2-14x on every query that returns thousands of rows** (Q1-Q8).
  The result-row count is LIMIT-bound (5000-10000) and identical on both sides,
  and the data is fully cached, so this is **not** an I/O or planning gap: it is
  the per-row pipeline (decode base row -> build JSON value -> encode each cell
  to Postgres-text `DataRow` -> wire). Postgres's tuple->wire path is far leaner.

## Relationship to this session's prefetch work

The async prefetch shipped this session targets the **disk-bound** regime (data
exceeds RAM, random base-page reads) and delivered a verified 2-6x there. It does
**not** help this in-RAM comparison, because at 1M everything is cached and the
bottleneck moves to row materialisation + wire encoding, a different layer.

## Where "match Postgres" actually requires work next

The row-return pipeline, in order of likely payoff:
1. Skip the intermediate `std.json.Value` per row: decode the stored row and
   encode wire cells directly (the JSON tree alloc/format per row is pure
   overhead for a `SELECT` that just ships columns).
2. Binary result encoding instead of Postgres-text for numeric/int columns.
3. Avoid re-emitting the large untouched `details` text through a JSON round-trip
   (pass the stored bytes straight to the wire).

These are row-pipeline changes in the wire/executor layer, independent of the
storage-engine prefetch work.
