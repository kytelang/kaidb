# orders-compare: kaidb vs PostgreSQL vs MySQL (1,000,000 rows)

Head-to-head on the Q1..Q10 "orders" workload, all three engines driven from the
**same Kyte program** through their respective driver packages
(`kyte-kaidb`/`kyte-postgres`/`kyte-mysql`), over loopback TCP. The dataset is
generated with a seeded RNG so every engine loads **byte-identical rows**; the
built-in result-row cross-check (identical data must give identical counts and
aggregates) is the correctness gate, and it passes on every query for all three
engines (row counts match, and Q10's AVG matches to full precision).

Numbers below are **medians over four warm runs** on one machine (Apple silicon),
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
| **kaidb (pipelined, default)** | **~6.5 s** | ~154k rows/s | ~5.2 s | 64 batches in flight |
| kaidb (synchronous, same path) | ~57 s | ~18k rows/s | ~5.1 s | one batch at a time |
| PostgreSQL | ~11.0 s | ~91k rows/s | ~1.4 s | one batch at a time |
| MySQL (InnoDB) | ~12.4 s | ~81k rows/s | ~3.0 s | one batch at a time |

Read this honestly, both ways:

- **Same dispatch (synchronous):** kaidb is the *slowest* loader at ~57 s, roughly 5x
  PostgreSQL and ~4x MySQL. Per batch, kaidb pays a full round-trip and commit that the
  SQL engines absorb more cheaply, so on an equal synchronous footing it loses badly.
- **As the driver actually ships (pipelined):** overlapping 64 batches hides that
  per-batch park entirely and takes kaidb to ~6.5 s, the fastest of the four and ~1.7x
  PostgreSQL / ~1.9x MySQL. This is a real, usable win, but it is the *kaidb driver*
  pipelining, not the *kaidb server* out-inserting InnoDB. PostgreSQL's wire protocol
  supports pipelining too; this harness simply does not use it for PG/MySQL, so the
  comparison would tighten if it did.

kaidb's load figure drifts upward across repeated runs in the same server process
(each run's `DROP` does not reclaim space, so `nova.db` bloats, see the kaidb note in
`README.md`); a fresh server loads at the low end of the range. The four runs here
measured 6.3-6.6 s.

## Query latency (ms, warm, full client round-trip, all rows fetched)

| Query | kaidb | PostgreSQL | MySQL | fastest |
|---|--:|--:|--:|:--|
| Q1  `emp = 279 LIMIT 10000` | **14** | **14** | 16 | kaidb / PG tie |
| Q2  `emp = 279 AND total_due > 10000 LIMIT 10000` | 16 | **14** | 17 | PostgreSQL |
| Q3  Q2 `ORDER BY total_due DESC` | 31 | 29 | **26** | MySQL |
| Q4  `total_due > 50000 LIMIT 5000` | 14 | **5** | 12 | PostgreSQL |
| Q5  Q4 `ORDER BY total_due DESC` | 15 | **6** | 13 | PostgreSQL |
| Q6  `customer_id = 1045 LIMIT 10000` | 8 | **6** | 12 | PostgreSQL (kaidb 2nd; both beat MySQL) |
| Q7  `emp IN (279,281,283) LIMIT 10000` | **14** | **14** | 17 | kaidb / PG tie |
| Q8  `total_due 10000..50000 LIMIT 10000` | 30 | **22** | 28 | PostgreSQL |
| Q9  `COUNT(*) WHERE emp = 279` | **~0** | 13 | 6 | kaidb |
| Q10 `AVG(total_due) GROUP BY emp ORDER BY a DESC LIMIT 5` | **30** | 50 | 129 | kaidb |

On an optimised (`--release`) client the field is tight and splits three ways:

- **kaidb** is fastest outright on **2** and tied with PostgreSQL on **2**: the
  index-only count (Q9, effectively free) and the grouped aggregate (Q10, where it
  clearly beats both) are outright wins; the indexed point / `IN` lookups (Q1, Q7) are
  dead heats with PostgreSQL at ~14 ms. It also wins the load decisively.
- **PostgreSQL** is fastest on **5** (plus the two ties): the medium-selectivity range
  scans (Q4, Q5, Q6) at ~5-6 ms, the between-range Q8, and Q2. Its heap + B-tree path
  from a warm cache is the tightest of the three on scans of a few thousand rows.
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
- **Q10 (grouped aggregate)** is kaidb's clearest engine win (~30 ms vs PG ~50,
  MySQL ~129) - its group-by path is materially faster than both.
- **Q1 / Q7 (indexed point / `IN`, 10k rows)** hold level with PostgreSQL (~14 ms) and
  beat MySQL. This is the **server-side result streaming** payoff: the scan-order
  `SELECT *` path sends the RowDescription up front and emits each row to the socket
  during the scan, so the client decodes concurrently with the server scan (max, not
  sum) instead of waiting for a buffered result set. The margin over the others is
  small on a release client because the decode it overlaps is now cheap; it was much
  larger in a debug build, which is why kaidb appeared to win these outright there.

## Where PostgreSQL wins

Q4/Q5/Q6/Q8 are medium-selectivity range/equality scans returning 5000-10000 rows.
PostgreSQL serves these from a warm buffer cache over a very tight heap + B-tree path
in ~5-6 ms (Q8 ~22 ms); kaidb lands at ~8-30 ms on the same queries (still ahead of
MySQL on Q6). This is the clearest remaining engine gap. Q3 (`ORDER BY total_due DESC`) is MySQL's
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
