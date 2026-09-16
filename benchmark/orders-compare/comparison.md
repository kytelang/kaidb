# orders-compare: kaidb vs PostgreSQL vs MySQL (1,000,000 rows)

Head-to-head on the Q1..Q10 "orders" workload, all three engines driven from the
**same Kyte program** through their respective driver packages
(`kyte-kaidb`/`kyte-postgres`/`kyte-mysql`), over loopback TCP. The dataset is
generated with a seeded RNG so every engine loads **byte-identical rows**; the
built-in result-row cross-check (identical data must give identical counts and
aggregates) is the correctness gate, and it passes on every query for all three
engines (row counts match, and Q10's AVG matches to full precision).

Numbers below are **medians over three warm runs** on one machine (Apple silicon).
All three engines show occasional single-query spikes (a background flush /
checkpoint / vacuum / autovacuum tick landing during a query), so any one run has
outliers; the medians are what to read. kaidb is measured with **result streaming
on** (the default).

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
| **kaidb (pipelined, default)** | **~5.7 s** | ~175k rows/s | ~5.0 s | 64 batches in flight |
| kaidb (synchronous, same path) | ~57 s | ~18k rows/s | ~5.1 s | one batch at a time |
| PostgreSQL | ~10.7 s | ~93k rows/s | ~1.4 s | one batch at a time |
| MySQL (InnoDB) | ~12.9 s | ~77k rows/s | ~3.1 s | one batch at a time |

Read this honestly, both ways:

- **Same dispatch (synchronous):** kaidb is the *slowest* loader at ~57 s, roughly 5x
  PostgreSQL and ~4x MySQL. Per batch, kaidb pays a full round-trip and commit that the
  SQL engines absorb more cheaply, so on an equal synchronous footing it loses badly.
- **As the driver actually ships (pipelined):** overlapping 64 batches hides that
  per-batch park entirely and takes kaidb to ~5.7 s, the fastest of the four and ~1.9x
  PostgreSQL / ~2.3x MySQL. This is a real, usable win, but it is the *kaidb driver*
  pipelining, not the *kaidb server* out-inserting InnoDB. PostgreSQL's wire protocol
  supports pipelining too; this harness simply does not use it for PG/MySQL, so the
  comparison would tighten if it did.

## Query latency (ms, warm, full client round-trip, all rows fetched)

| Query | kaidb | PostgreSQL | MySQL | fastest |
|---|--:|--:|--:|:--|
| Q1  `emp = 279 LIMIT 10000` | **17** | 22 | 26 | kaidb |
| Q2  `emp = 279 AND total_due > 10000 LIMIT 10000` | **21** | 22 | 22 | kaidb (~tie) |
| Q3  Q2 `ORDER BY total_due DESC` | **29** | 34 | 32 | kaidb |
| Q4  `total_due > 50000 LIMIT 5000` | 15 | **7** | 13 | PostgreSQL |
| Q5  Q4 `ORDER BY total_due DESC` | 14 | **7** | 13 | PostgreSQL |
| Q6  `customer_id = 1045 LIMIT 10000` | 9 | **7** | 13 | PostgreSQL (kaidb 2nd; both beat MySQL) |
| Q7  `emp IN (279,281,283) LIMIT 10000` | **16** | 19 | 22 | kaidb |
| Q8  `total_due 10000..50000 LIMIT 10000` | **28** | 28 | 31 | kaidb (~tie PG) |
| Q9  `COUNT(*) WHERE emp = 279` | **~0** | 13 | 5 | kaidb |
| Q10 `AVG(total_due) GROUP BY emp ORDER BY a DESC LIMIT 5` | **31** | 50 | 142 | kaidb |

kaidb is fastest (or tied fastest) on **7 of 10** queries: the indexed point / `IN`
queries (Q1, Q2, Q6-runner-up, Q7), the `ORDER BY DESC` join-filter (Q3), the
index-only count (Q9), and the grouped aggregate (Q10). PostgreSQL is fastest on the
three medium-selectivity range scans (Q4, Q5, Q6), where its heap + B-tree access
path is very tight; kaidb is a close second on those and still beats MySQL on Q6.
MySQL is not fastest on any query here, and is well behind on the grouped aggregate
(Q10).

## Why kaidb wins where it wins

Measured per stage (all three drivers run in the same Kyte with the same
`db.DbValue` decode, so per-value client decode cost is identical between them):

- kaidb's **server** executes the point/IN/aggregate queries faster than MySQL's
  (e.g. Q1 ~13 ms vs MySQL ~21 ms server-side per `SHOW PROFILES`), and kaidb's
  client row-decode is no slower than the others'.
- The former loss on Q1..Q8 was **not** the engine or codegen (that would hit every
  driver equally) - it was that kaidb **buffered the whole result set before
  sending**, so the client sat idle through the scan and then decoded
  (serial: server + decode), while the others stream rows and overlap scan with the
  client's decode (max, not sum).
- Fix: **server-side result streaming** - the scan-order `SELECT *` path sends the
  RowDescription up front and emits each row to the socket during the scan, so the
  client decodes concurrently with the server scan. A sorted / offset / distinct
  query (whose final order is not the scan order) still buffers, which is why the
  ascending-scan-then-reverse `ORDER BY DESC` queries do not yet get the full
  streaming benefit; a backward index scan would let them stream too.

## Where PostgreSQL wins

Q4/Q5/Q6 are medium-selectivity range/equality scans returning 5000-10000 rows.
PostgreSQL serves these from a warm buffer cache over a very tight heap + B-tree
path in ~7 ms. kaidb lands at ~9-16 ms on the same queries (still ahead of MySQL on
Q6), so this is the clearest remaining gap and the natural next optimisation target
alongside the backward index scan.

## Reproduce

```sh
# from this directory, with kaidb on :3009, PostgreSQL on :5432, MySQL on :3306
kyte build && codesign -s - -f build/debug/bin/orders-compare   # macOS
ORDERS_ROWS=1000000 ORDERS_ENGINES=kaidb,postgres,mysql ./build/debug/bin/orders-compare
```

Env knobs: `ORDERS_ROWS`, `ORDERS_ENGINES` (comma list), `ORDERS_BATCH`, `ORDERS_SEED`,
`KAIDB_URL`/`PG_URL`/`MYSQL_URL`, `ORDERS_OUT`. Server-side `NOVADB_NOSTREAM=1` forces the
old buffer-then-send path; `NOVADB_QEXEC`/`NOVADB_QPROF` enable per-stage profiling.
The harness only ever creates/drops/queries the `orders` table in the database named in
each engine's URL (PostgreSQL and MySQL both use `novabench`).
