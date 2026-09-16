# orders-compare: kaidb vs PostgreSQL vs MySQL (1,000,000 rows)

Head-to-head on the Q1..Q10 "orders" workload, all three engines driven from the
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
| **kaidb (pipelined, default)** | **~5.4 s** | ~185k rows/s | ~4.8 s | 64 batches in flight |
| kaidb (synchronous, same path) | ~57 s | ~18k rows/s | ~5.1 s | one batch at a time |
| PostgreSQL | ~10.4 s | ~96k rows/s | ~1.7 s | one batch at a time |
| MySQL (InnoDB) | ~13.5 s | ~74k rows/s | ~2.9 s | one batch at a time |

Read this honestly, both ways:

- **Same dispatch (synchronous):** kaidb is the *slowest* loader at ~57 s, roughly 5x
  PostgreSQL and ~4x MySQL. Per batch, kaidb pays a full round-trip and commit that the
  SQL engines absorb more cheaply, so on an equal synchronous footing it loses badly.
- **As the driver actually ships (pipelined):** overlapping 64 batches hides that
  per-batch park entirely and takes kaidb to ~5.4 s, the fastest of the four and ~1.9x
  PostgreSQL / ~2.5x MySQL. This is a real, usable win, but it is the *kaidb driver*
  pipelining, not the *kaidb server* out-inserting InnoDB. PostgreSQL's wire protocol
  supports pipelining too; this harness simply does not use it for PG/MySQL, so the
  comparison would tighten if it did.

## Query latency (ms, warm, full client round-trip, all rows fetched)

| Query | kaidb | PostgreSQL | MySQL | fastest |
|---|--:|--:|--:|:--|
| Q1  `emp = 279 LIMIT 10000` | **14** | 15 | 17 | kaidb |
| Q2  `emp = 279 AND total_due > 10000 LIMIT 10000` | 15 | **14** | 18 | PostgreSQL (~tie) |
| Q3  Q2 `ORDER BY total_due DESC` | 30 | 31 | **24** | MySQL |
| Q4  `total_due > 50000 LIMIT 5000` | 15 | **5** | 13 | PostgreSQL |
| Q5  Q4 `ORDER BY total_due DESC` | 14 | **5** | 13 | PostgreSQL |
| Q6  `customer_id = 1045 LIMIT 10000` | 9 | **6** | 11 | PostgreSQL (kaidb 2nd; both beat MySQL) |
| Q7  `emp IN (279,281,283) LIMIT 10000` | **13** | 14 | 17 | kaidb |
| Q8  `total_due 10000..50000 LIMIT 10000` | 28 | **23** | 28 | PostgreSQL |
| Q9  `COUNT(*) WHERE emp = 279` | **~0** | 13 | 5 | kaidb |
| Q10 `AVG(total_due) GROUP BY emp ORDER BY a DESC LIMIT 5` | **48** | 51 | 129 | kaidb |

On an optimised (`--release`) client the field is tight and splits three ways:

- **kaidb** is fastest on **4** and tied on Q2: the indexed point / `IN` lookups (Q1,
  Q7), the index-only count (Q9, effectively free), and the grouped aggregate (Q10,
  where it clearly beats both). It also wins the load decisively.
- **PostgreSQL** is fastest on **5**: the medium-selectivity range scans (Q4, Q5, Q6)
  at ~5-6 ms, the between-range Q8, and Q2 by a hair. Its heap + B-tree path from a
  warm cache is the tightest of the three on scans of a few thousand rows.
- **MySQL** wins **1** (Q3, an `ORDER BY total_due DESC` that its backward index scan
  serves well) and otherwise trails, badly so on the grouped aggregate (Q10).

Note how this differs from a debug build: in debug, client-side row decode is slow,
so kaidb's result streaming (which overlaps decode with the server scan) hides more
cost and kaidb appears to win 7 of 10. Under `--release` the decode is cheap for every
driver, the overlap saves less, and PostgreSQL's engine advantage on range scans shows
through. The release picture above is the fair one.

## Why kaidb wins where it wins

- **Q9 (index-only count)** is effectively free: kaidb answers `COUNT(*)` from the
  index without touching base rows.
- **Q10 (grouped aggregate)** is kaidb's clearest engine win (~48 ms vs PG ~51,
  MySQL ~129) - its group-by path is materially faster than MySQL's and edges PG.
- **Q1 / Q7 (indexed point / `IN`, 10k rows)** stay ahead by a few ms. This is the
  **server-side result streaming** payoff: the scan-order `SELECT *` path sends the
  RowDescription up front and emits each row to the socket during the scan, so the
  client decodes concurrently with the server scan (max, not sum) instead of waiting
  for a buffered result set. The margin is small on a release client because the
  decode it overlaps is now cheap; it was much larger in a debug build.

## Where PostgreSQL wins

Q4/Q5/Q6/Q8 are medium-selectivity range/equality scans returning 5000-10000 rows.
PostgreSQL serves these from a warm buffer cache over a very tight heap + B-tree path
in ~5-6 ms; kaidb lands at ~9-15 ms on the same queries (still ahead of MySQL on Q6).
This is the clearest remaining engine gap. Q3 (`ORDER BY total_due DESC`) is MySQL's
one win, and also kaidb's weakest indexed query: kaidb does an ascending scan then
reverses (so it cannot stream that result), whereas MySQL walks the index backwards.
A **backward index scan** in kaidb would close both the Q3 gap and let the `ORDER BY
DESC` queries stream. Tightening the range-scan path (Q4/Q5/Q6/Q8) is the other
priority.

## Reproduce

Build `--release` (a debug build inflates the client half of every round-trip and
changes the standings):

```sh
# from this directory, with kaidb on :3009, PostgreSQL on :5432, MySQL on :3306
kyte build --release && codesign -s - -f build/release/bin/orders-compare   # macOS
ORDERS_ROWS=1000000 ORDERS_ENGINES=kaidb,postgres,mysql ./build/release/bin/orders-compare
```

Env knobs: `ORDERS_ROWS`, `ORDERS_ENGINES` (comma list), `ORDERS_BATCH`, `ORDERS_SEED`,
`KAIDB_URL`/`PG_URL`/`MYSQL_URL`, `ORDERS_OUT`. Server-side `NOVADB_NOSTREAM=1` forces the
old buffer-then-send path; `NOVADB_QEXEC`/`NOVADB_QPROF` enable per-stage profiling.
The harness only ever creates/drops/queries the `orders` table in the database named in
each engine's URL (PostgreSQL and MySQL both use `bench`). Take medians over three runs.
