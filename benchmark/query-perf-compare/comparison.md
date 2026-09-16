# query-perf-compare: kaidb vs PostgreSQL vs MySQL (1,000,000 rows)

Head-to-head on the Q1..Q18 "orders" workload, all three engines driven from the
**same Kyte program** through their respective driver packages
(`kyte-kaidb`/`kyte-postgres`/`kyte-mysql`), over loopback TCP. The dataset is
generated with a seeded RNG so every engine loads **byte-identical rows**; the
built-in result-row cross-check (identical data must give identical counts and
aggregates) is the correctness gate, and it passes on every query for all three
engines (row counts match, and Q10's AVG matches to full precision).

Numbers below are **medians over three warm runs** on one machine (Apple silicon),
using a **`--release` build of the harness** (the debug build leaves the client-side
driver decode unoptimised, which inflates every round-trip and, importantly, changes
the standings, see the note at the end). All three engines show occasional
single-query spikes (a background flush / checkpoint / vacuum / autovacuum tick
landing during a query), so any one run has outliers; the medians are what to read.
kaidb is measured with **result streaming on** (the default).

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
| **kaidb (pipelined, default)** | **~6.8 s** | ~148k rows/s | ~5.7 s | 64 batches in flight |
| kaidb (synchronous, same path) | ~57 s | ~18k rows/s | ~5.1 s | one batch at a time |
| PostgreSQL | ~10.8 s | ~93k rows/s | ~1.4 s | one batch at a time |
| MySQL (InnoDB) | ~13.6 s | ~73k rows/s | ~3.0 s | one batch at a time |

Read this honestly, both ways:

- **Same dispatch (synchronous):** kaidb is the *slowest* loader at ~57 s, roughly 5x
  PostgreSQL and ~4x MySQL. Per batch, kaidb pays a full round-trip and commit that the
  SQL engines absorb more cheaply, so on an equal synchronous footing it loses badly.
- **As the driver actually ships (pipelined):** overlapping 64 batches hides that
  per-batch park entirely and takes kaidb to ~6.8 s, the fastest of the four and ~1.6x
  PostgreSQL / ~2x MySQL. This is a real, usable win, but it is the *kaidb driver*
  pipelining, not the *kaidb server* out-inserting InnoDB. PostgreSQL's wire protocol
  supports pipelining too; this harness simply does not use it for PG/MySQL, so the
  comparison would tighten if it did.

kaidb's load figure drifts upward across repeated runs in the same server process
(each run's `DROP` does not reclaim space, so `nova.db` bloats, see the kaidb note in
`README.md`); a fresh server loads at the low end of the range. The three runs here
measured 6.6-6.8 s.

## Query latency (ms, warm, full client round-trip, all rows fetched)

Q1-Q10 are the original mixed OLTP-style set; Q11-Q18 were added to probe distinct
planner paths (primary-key access, whole-table aggregates, DISTINCT, high-cardinality
grouping, composite index, deep pagination).

| Query | kaidb | PostgreSQL | MySQL | fastest |
|---|--:|--:|--:|:--|
| Q1  `emp = 279 LIMIT 10000` | 18 | **16** | 18 | PostgreSQL |
| Q2  `emp = 279 AND total_due > 10000 LIMIT 10000` | 16 | **14** | 18 | PostgreSQL |
| Q3  Q2 `ORDER BY total_due DESC` | 31 | 31 | **26** | MySQL |
| Q4  `total_due > 50000 LIMIT 5000` | 17 | **5** | 13 | PostgreSQL |
| Q5  Q4 `ORDER BY total_due DESC` | 18 | **5** | 13 | PostgreSQL |
| Q6  `customer_id = 1045 LIMIT 10000` | 9 | **6** | 13 | PostgreSQL (kaidb 2nd; both beat MySQL) |
| Q7  `emp IN (279,281,283) LIMIT 10000` | **13** | 16 | 18 | kaidb |
| Q8  `total_due 10000..50000 LIMIT 10000` | 31 | **23** | 26 | PostgreSQL |
| Q9  `COUNT(*) WHERE emp = 279` | **1** | 14 | 5 | kaidb |
| Q10 `AVG(total_due) GROUP BY emp ORDER BY a DESC LIMIT 5` | **30** | 49 | 131 | kaidb |
| Q11 PK point `id = 500000` | **~0** | **~0** | 1 | kaidb / PG tie |
| Q12 PK range `id 200000..210000` (10k rows) | 228 | **10** | 18 | PostgreSQL |
| Q13 `COUNT(*)` whole table | **5** | 16 | 75 | kaidb |
| Q14 `MIN/MAX(total_due)` whole table | **~0** | **~0** | **~0** | tie |
| Q15 `DISTINCT employee_id` | **~0** | 39 | **~0** | kaidb / MySQL tie |
| Q16 `GROUP BY customer_id HAVING count > 4900` top 10 | 262 | **45** | 114 | PostgreSQL |
| Q17 `emp = 279 AND total_due 20000..40000 LIMIT 10000` | 40 | **12** | 27 | PostgreSQL |
| Q18 `emp = 279 ORDER BY total_due LIMIT 100 OFFSET 50000` | 213 | **34** | 81 | PostgreSQL |

Across all 18, on an optimised (`--release`) client:

- **PostgreSQL is the overall leader**, fastest on **10** (Q1, Q2, Q4, Q5, Q6, Q8, Q12,
  Q16, Q17, Q18) and tied first on Q11/Q14. Its heap + B-tree path from a warm cache is
  the tightest of the three on almost every scan, range, and pagination shape.
- **kaidb is fastest on 4** (Q7 indexed `IN`; Q9 filtered count; Q10 grouped aggregate;
  Q13 whole-table count) and tied first on **3** (Q11 PK point, Q14 MIN/MAX, Q15
  DISTINCT). It also wins the load. Its wins cluster on **aggregates and counts**, where
  it is genuinely strong.
- **MySQL is fastest on 1** (Q3, `ORDER BY ... DESC` via its backward index scan) and is
  otherwise mid-pack or last, notably slow on the grouped aggregate (Q10) and the
  whole-table count (Q13).

The expanded set is deliberately unflattering where kaidb is weak, so the numbers are
trustworthy rather than cherry-picked.

## Where kaidb wins

- **Aggregates and counts.** Q9 (filtered count, ~1 ms, index-only), Q13 (whole-table
  `COUNT(*)`, ~5 ms vs MySQL ~75), Q10 (grouped `AVG`, ~30 ms vs PG ~49 / MySQL ~131),
  and Q15 (`DISTINCT`, ~0 ms, beating PG's 39). kaidb's aggregate and distinct paths
  read the index without materialising base rows and are its clearest strength.
- **Point lookups.** Q11 (PK seek) and Q14 (MIN/MAX index endpoints) are effectively
  instant for all three; Q7 (indexed `IN`) edges ahead on the result-streaming overlap.

## Where kaidb loses (the real gaps the new queries exposed)

- **Clustered PK range scan (Q12): ~228 ms vs PostgreSQL ~10 ms** - a 20x gap. Walking a
  contiguous 10k-row slice of the clustered primary key is far more expensive in kaidb
  than it should be; this is the single largest gap and the first thing to fix.
- **Deep pagination (Q18, OFFSET 50000): ~213 ms vs PG ~34 ms** - kaidb walks and
  discards the skipped rows rather than seeking past them.
- **High-cardinality grouping (Q16, GROUP BY over ~200 customer groups): ~262 ms vs PG
  ~45 ms** - the grouped-aggregate path that wins on the 17-group Q10 does not scale to
  hundreds of groups.
- **Medium range scans (Q4/Q5/Q8/Q17): ~17-40 ms vs PG ~5-23 ms** - PostgreSQL's warm
  heap + B-tree path is simply tighter.
- **`ORDER BY ... DESC` (Q3, Q5):** kaidb scans ascending then reverses (so it cannot
  stream the result), whereas MySQL/PG walk the index backwards. A **backward index
  scan** would help here and let these stream.

In short: kaidb is competitive-to-winning on aggregates, counts, DISTINCT and point
lookups, but PostgreSQL is ahead on the range-scan, pagination and grouping paths that
dominate this expanded set. The clustered PK range scan (Q12) is the standout defect.

## Reproduce

Build `--release` (a debug build inflates the client half of every round-trip and
changes the standings):

```sh
# from this directory, with kaidb on :3009, PostgreSQL on :5432, MySQL on :3306
kyte build --release && codesign -s - -f build/release/bin/query-perf-compare   # macOS
ORDERS_ROWS=1000000 ORDERS_ENGINES=kaidb,postgres,mysql ./build/release/bin/query-perf-compare
```

Env knobs: `ORDERS_ROWS`, `ORDERS_ENGINES` (comma list), `ORDERS_BATCH`, `ORDERS_SEED`,
`KAIDB_URL`/`PG_URL`/`MYSQL_URL`, `ORDERS_OUT`. Server-side `NOVADB_NOSTREAM=1` forces the
old buffer-then-send path; `NOVADB_QEXEC`/`NOVADB_QPROF` enable per-stage profiling.
The harness only ever creates/drops/queries the `orders` table in the database named in
each engine's URL (PostgreSQL and MySQL both use `bench`). Take medians over three runs.
