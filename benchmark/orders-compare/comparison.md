# orders-compare: kaidb vs MySQL vs PostgreSQL (1,000,000 rows)

Head-to-head on the Q1..Q10 "orders" workload, all three engines driven from the
**same Kyte program** through their respective driver packages
(`kyte-kaidb`/`kyte-mysql`/`kyte-postgres`), over loopback TCP. The dataset is
generated with a seeded RNG so every engine loads **byte-identical rows**; the
built-in result-row cross-check (identical data must give identical counts and
aggregates) is the correctness gate, and it passes on every query
(row counts match, and Q10's AVG matches to full precision).

Numbers below are representative medians over several warm runs on one machine.
Both engines show occasional single-query spikes (a background flush / checkpoint /
vacuum tick landing during a query), so any one run has outliers; the medians are
what to read. kaidb is measured with **result streaming on** (the default).

## Load + index build

| Engine | 1M load | throughput | index build |
|---|--:|--:|--:|
| **kaidb** | **~5.7 s** | **~175k rows/s** | ~5 s |
| MySQL (InnoDB) | ~13 s | ~77k rows/s | ~3 s |
| PostgreSQL | not measured | | |

kaidb loads ~2.3x faster than MySQL. The win is the driver's **pipelined bulk load**
(`execPipelineLoad`): up to N INSERT batches in flight on one connection instead of a
synchronous round-trip per batch, so the client feeds the server at the server's true
insert rate.

## Query latency (ms, warm, full client round-trip, all rows fetched)

| Query | kaidb | MySQL | winner |
|---|--:|--:|:--|
| Q1  `emp = 279 LIMIT 10000` | **17** | 24 | kaidb |
| Q2  `emp = 279 AND total_due > 10000 LIMIT 10000` | **16** | 22 | kaidb |
| Q3  Q2 `ORDER BY total_due DESC` | 28 | 25 | ~tie (MySQL) |
| Q4  `total_due > 50000 LIMIT 5000` | 14 | 13 | ~tie |
| Q5  Q4 `ORDER BY total_due DESC` | 16 | 14 | ~tie (MySQL) |
| Q6  `customer_id = 1045 LIMIT 10000` | **9** | 12 | kaidb |
| Q7  `emp IN (279,281,283) LIMIT 10000` | **16** | 22 | kaidb |
| Q8  `total_due 10000..50000 LIMIT 10000` | 28 | 25 | ~tie (MySQL) |
| Q9  `COUNT(*) WHERE emp = 279` | **1** | 5 | kaidb |
| Q10 `AVG(total_due) GROUP BY emp ORDER BY a DESC LIMIT 5` | **30** | 130 | kaidb |

kaidb wins the indexed point/`IN` queries (Q1, Q2, Q6, Q7), the index-only count (Q9),
and the grouped aggregate (Q10); it is level with MySQL on the wide-range and
`ORDER BY DESC` queries (Q3, Q4, Q5, Q8).

## Why

Measured per stage (both drivers run in the same Kyte, so per-value decode cost is
identical between them):

- kaidb's **server** executes these queries faster than MySQL's (e.g. Q1 ~13 ms vs
  MySQL ~21 ms server-side per `SHOW PROFILES`), and kaidb's client row-decode is also
  no slower.
- The former loss on Q1..Q8 was **not** the engine or codegen — it was that kaidb
  buffered the whole result set before sending, so the client sat idle through the scan
  and then decoded (serial: server + decode), while MySQL streams rows and overlaps its
  scan with the client's decode (max, not sum).
- Fix: **server-side result streaming** — the scan-order `SELECT *` path sends the
  RowDescription up front and emits each row to the socket during the scan, so the
  client decodes concurrently with the server scan. A sorted / offset / distinct query
  (whose final order is not the scan order) still buffers, which is why the
  `ORDER BY DESC` queries (Q3, Q5) do not yet benefit; a backward index scan would let
  them stream too.

## Reproduce

```sh
# from this directory, with kaidb server on :3009 and MySQL on :3306
kyte build && codesign -s - -f build/debug/bin/orders-compare   # macOS
ORDERS_ROWS=1000000 ORDERS_ENGINES=kaidb,mysql ./build/debug/bin/orders-compare
```

Env knobs: `ORDERS_ROWS`, `ORDERS_ENGINES` (comma list), `ORDERS_BATCH`, `ORDERS_SEED`,
`KAIDB_URL`/`MYSQL_URL`/`PG_URL`, `ORDERS_OUT`. Server-side `NOVADB_NOSTREAM=1` forces the
old buffer-then-send path; `NOVADB_QEXEC`/`NOVADB_QPROF` enable per-stage profiling.

## PostgreSQL note

PostgreSQL was **not measured** in this run: the local server was in recovery mode
(`FATAL: the database system is in recovery mode`) and rejected the load, so the harness
skipped it. To include it, bring the server up writable (e.g. restart the service) with a
throwaway `novabench` database, then re-run with
`ORDERS_ENGINES=kaidb,postgres,mysql`. The harness only ever creates/drops/queries the
`orders` table in the database named in each engine's URL.
