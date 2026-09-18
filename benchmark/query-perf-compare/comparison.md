# query-perf-compare: kaidb vs PostgreSQL vs MySQL (1,000,000 rows)

Head-to-head on the Q1..Q18 "orders" workload, all three engines driven from the
**same Kyte program** through their respective driver packages
(`kyte-kaidb`/`kyte-postgres`/`kyte-mysql`), over loopback TCP. The dataset is
generated with a seeded RNG so every engine loads **byte-identical rows**; the
built-in result-row cross-check (identical data must give identical counts and
aggregates) is the correctness gate, and it passes on every query for all three
engines (row counts match, and Q10's AVG matches to full precision).

Numbers below are **medians over three warm runs** on one machine (Apple silicon),
using a **`--release` build** of both the kaidb server and the harness. (A debug
build of either leaves work unoptimised and inflates every timing; do not quote
debug numbers.) kaidb is measured with **result streaming on** (the default). All
three engines show occasional single-query spikes (a background flush / checkpoint
/ vacuum tick), so any one run has outliers; the medians are what to read.

kaidb here includes the query-perf branch `perf/q12-pk-range-scan` (Q12 bounded
clustered range scan, Q16 HAVING on the index-only aggregate, PK-sorted base-row
fetch, Q18 offset pushed into the ordered index scan) plus this session's later fixes
(free-list crash-durability at the periodic checkpoint, single-column ordered-scan
OFFSET pushdown); see `q11-18-perf-improve.md` and `KNOWN-ISSUES.md` for the fix log
and what is still open. These figures are the medians of a fresh three-run pass on
2026-09-18.

## Load + index build

**The load path is not apples-to-apples by default.** All three build the identical
multi-row INSERT batches (same generator, batch size and rows), but the *dispatch*
differs: PostgreSQL and MySQL run a synchronous `exec` loop (one batch, wait for the
reply, next batch), while kaidb by default uses a **pipelined** driver path
(`execPipelineLoad`) with up to 64 INSERT batches in flight on one connection. So the
default kaidb load number below reflects a driver capability (pipelining), not a
faster server. To make it comparable, we also ran kaidb through the *same synchronous
path* as the SQL engines (`ORDERS_PIPELINE=1`).

| Engine | 1M load | throughput | index build | dispatch |
|---|--:|--:|--:|:--|
| **kaidb (pipelined, default)** | **~5.6 s** | ~178k rows/s | ~4.7 s | 64 batches in flight |
| kaidb (synchronous, same path) | ~57 s | ~18k rows/s | ~4.7 s | one batch at a time |
| PostgreSQL | ~10.2 s | ~98k rows/s | ~1.2 s | one batch at a time |
| MySQL (InnoDB) | ~10.4 s | ~96k rows/s | ~2.9 s | one batch at a time |

(The synchronous-kaidb row is carried from the earlier run; it is a driver-dispatch
property, not a server change, so it was not re-measured this pass.)

Read this honestly, both ways:

- **Same dispatch (synchronous):** kaidb is the *slowest* loader at ~57 s, roughly 5x
  PostgreSQL and MySQL. Per batch, kaidb pays a full round-trip and commit that the
  SQL engines absorb more cheaply, so on an equal synchronous footing it loses badly.
- **As the driver actually ships (pipelined):** overlapping 64 batches hides that
  per-batch park and takes kaidb to ~5.6 s, the fastest of the four and ~1.8x
  PostgreSQL / MySQL. This is a real, usable win, but it is the *kaidb driver*
  pipelining, not the *kaidb server* out-inserting InnoDB. PostgreSQL's wire protocol
  supports pipelining too; this harness simply does not use it for PG/MySQL.

kaidb's load figure is now stable across repeated runs in one server process: its
`DROP` reclaims the table's pages (base + indexes) back to the pager free list, so
`nova.db` stays flat (~412 MB over three 1M runs) instead of growing ~1 GB per run.
No fresh data directory or restart is needed between runs.

## Query latency (ms, warm, full client round-trip, all rows fetched)

Q1-Q10 are the original mixed set; Q11-Q18 probe distinct planner paths (primary-key
access, whole-table aggregates, DISTINCT, high-cardinality grouping, composite index,
deep pagination).

| Query | kaidb | PostgreSQL | MySQL | fastest |
|---|--:|--:|--:|:--|
| Q1  `emp = 279 LIMIT 10000` | 13 | 13 | 17 | ~tie (kaidb/PG) |
| Q2  `emp = 279 AND total_due > 10000 LIMIT 10000` | 14 | 14 | 18 | ~tie (kaidb/PG) |
| Q3  Q2 `ORDER BY total_due DESC` | 27 | 29 | 25 | ~tie (kaidb/MySQL) |
| Q4  `total_due > 50000 LIMIT 5000` | 15 | **5** | 12 | PostgreSQL |
| Q5  Q4 `ORDER BY total_due DESC` | 14 | **5** | 13 | PostgreSQL |
| Q6  `customer_id = 1045 LIMIT 10000` | 8 | **5** | 12 | PostgreSQL |
| Q7  `emp IN (279,281,283) LIMIT 10000` | 14 | 14 | 17 | ~tie (kaidb/PG) |
| Q8  `total_due 10000..50000 LIMIT 10000` | 32 | **22** | 27 | PostgreSQL |
| Q9  `COUNT(*) WHERE emp = 279` | **1** | 13 | 6 | kaidb |
| Q10 `AVG(total_due) GROUP BY emp ORDER BY a DESC LIMIT 5` | **29** | 50 | 128 | kaidb |
| Q11 PK point `id = 500000` | **~0** | **~0** | **~0** | tie |
| Q12 PK range `id 200000..210000` (10k rows) | **10** | **10** | 16 | kaidb / PG tie |
| Q13 `COUNT(*)` whole table | **4** | 17 | 71 | kaidb |
| Q14 `MIN/MAX(total_due)` whole table | **~0** | **~0** | **~0** | tie |
| Q15 `DISTINCT employee_id` | **~0** | 40 | **~0** | kaidb / MySQL tie |
| Q16 `GROUP BY customer_id HAVING count > 4900` top 10 | **12** | 44 | 105 | kaidb |
| Q17 `emp = 279 AND total_due 20000..40000 LIMIT 10000` | 32 | **11** | 23 | PostgreSQL |
| Q18 `emp = 279 ORDER BY total_due LIMIT 100 OFFSET 50000` | **1** | 33 | 98 | kaidb |

Across all 18, on an optimised (`--release`) client:

- **kaidb is fastest or tied on ~13.** Clear wins: the aggregates / counts (Q9, Q10,
  Q13, Q16), DISTINCT (Q15), the deep OFFSET (Q18, now ~1 ms after pushing the offset
  into the index scan), and the load. Tied first: the indexed point / `IN` lookups
  (Q1, Q2, Q7) and the PK/endpoint queries (Q11, Q12, Q14). Q3 is a three-way ~tie.
- **PostgreSQL is fastest on the range scans** (Q4, Q5, Q6, Q8, Q17) at ~5-24 ms, where
  its warm heap + B-tree path is tighter than kaidb's; and it is level with kaidb on the
  point lookups.
- **MySQL is not clearly fastest on anything** here (it ties Q3) and trails badly on the
  grouped aggregates (Q10, Q16) and the whole-table count (Q13).

The indexed point / `IN` lookups (Q1, Q2, Q7) are close enough that they swap leader
run-to-run between kaidb and PostgreSQL (single-digit-ms differences within the flush /
checkpoint noise), so read them as ties rather than a win either way. The stable,
structural results are: kaidb wins the aggregate / count / DISTINCT family, the
index-only and PK queries, the deep OFFSET and the load; PostgreSQL wins the
medium-selectivity range scans (Q4/Q5/Q6/Q8/Q17).

## Where kaidb wins

- **Aggregates and counts.** Q9 (filtered count, ~1 ms, index-only), Q13 (whole-table
  `COUNT(*)`, ~4 ms vs MySQL ~71), Q10 (grouped `AVG`, ~29 ms vs PG ~50 / MySQL ~128),
  Q16 (grouped `COUNT` + HAVING, ~12 ms vs PG ~44). These read the index without
  materialising base rows and are its clearest strength.
- **Point / range on the primary key.** Q11 (PK seek) and Q14 (MIN/MAX endpoints) are
  instant for all three; Q12 (10k-row clustered range) now ties PostgreSQL at ~10 ms
  after the bounded seek+stop scan (it was ~228 ms as a full-table scan).
- **DISTINCT.** Q15 is ~0 ms (loose index scan), beating PostgreSQL's ~40 ms.
- **Deep pagination (Q18, OFFSET 50000): ~1 ms vs PG ~33 ms** - the offset is pushed
  into the ordered index scan and counted past from the index alone (no base-row
  fetch, no materialisation), then only the LIMIT window is built. It was ~185 ms when
  it fetched and discarded every skipped row.

## Where kaidb still loses

- **Medium range scans (Q4/Q5/Q8/Q17): ~14-32 ms vs PG ~5-22 ms.** PostgreSQL's warm
  heap + B-tree path is simply tighter. Profiling these (`NOVADB_QPROF`) shows the cost
  is ~46 % per-row base-row seek (one clustered-PK descent per qualifying row) and ~50 %
  index-walk + residual decode; per-row materialisation is only ~4 %, so a covering /
  included-column index (answering the projection from the index alone) is the real
  lever here, not row-image reuse. This is the clearest remaining gap. See `KNOWN-ISSUES.md`.
- **`ORDER BY ... DESC` (Q3, Q5):** kaidb already serves these with a backward index
  scan (`IndexRangeScanDescIterator`) that streams and breaks at LIMIT, so the residual
  gap is the same per-row decode / materialisation cost as the range scans above, not a
  missing backward scan.

## Reproduce

Build `--release` (a debug build inflates the client half of every round-trip and,
for the server, the whole engine):

```sh
# from this directory, with kaidb on :3009, PostgreSQL on :5432, MySQL on :3306
kyte build --release && codesign -s - -f build/release/bin/query-perf-compare   # macOS
ORDERS_ROWS=1000000 ORDERS_ENGINES=kaidb,postgres,mysql ./build/release/bin/query-perf-compare
```

The report prints to the console (stdout); set `ORDERS_OUT=report.md` to also write a
file. Env knobs: `ORDERS_ROWS`, `ORDERS_ENGINES` (comma list), `ORDERS_BATCH`,
`ORDERS_SEED`, `ORDERS_PIPELINE` (1 = synchronous load), `KAIDB_URL`/`PG_URL`/`MYSQL_URL`.
Server-side `NOVADB_NOSTREAM=1` forces the old buffer-then-send path;
`NOVADB_QEXEC`/`NOVADB_QPROF` enable per-stage profiling. The harness only ever
creates/drops/queries the `orders` table in the database named in each engine's URL
(PostgreSQL and MySQL both use `bench`). Take medians over three runs.
