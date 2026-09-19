# Orders benchmark: SQL vs document (NoSQL) on NovaDB

> **Scope note (2026-08-30).** This benchmark is the evidence for scoping NovaDB
> to the **Nova orchestrator's control-plane store** (small, bounded config data),
> not a general-purpose database. At 10M rows the limits are structural: bloated
> on-disk footprint, disk-thrashing I/O once the working set exceeds the pool,
> per-index re-scans on build, and a planner that scans where it should seek.
> Those are real gaps and a large systems project to close. Recovery correctness
> was fixed (see `../CLAUDE.md`); performance at scale was not, and that is by
> acceptance, not oversight.


A NovaDB port of the MongoDB `mongodb-perf-compare` test. It generates orders from
the same source data (employees, customers, products), loads them into NovaDB in
either **SQL** or **document** mode, indexes `EmployeeID` / `CustomerID` /
`TotalDue`, and runs the same ten queries the reference runs against MongoDB.

Harness (historical): this SQL-vs-document comparison was produced by the old Zig
YCSB harness (`benchmark/ycsb`, commands `orders-sql` / `orders-doc`), which has
since been removed. The cross-engine Q1..Q10 comparison (kaidb vs PostgreSQL vs
MySQL) now lives in the Kyte harness `benchmark/orders-compare`. The numbers below
are kept as the historical SQL-vs-document evidence. Same generated data and query
set in both modes; the document side stores a real nested `SalesOrderDetails`
array, the SQL side stores the line items as a `TEXT` JSON column plus typed
columns (`employee_id`/`customer_id` INTEGER, `total_due` etc. DOUBLE).

## Results (100,000 orders, fresh server each, indexes created after load)

| Query | SQL rows | SQL time | Doc results | Doc time | Doc advantage |
|---|---|---|---|---|---|
| Q1  EmployeeID=279, limit 10000 | 6030 | 9,802 ms | 6030 | 15 ms | ~650x |
| Q2  EmployeeID=279 AND TotalDue>10000 | 3918 | 8,729 ms | 3918 | 15 ms | ~580x |
| Q3  Q2 + sort TotalDue desc | 3918 | 8,966 ms | 3918 | 21 ms | ~430x |
| Q4  TotalDue>50000, limit 5000 | 5000 | 15,794 ms | 5000 | 19 ms | ~830x |
| Q5  Q4 + sort TotalDue desc | 5000 | 8,893 ms | 5000 | 24 ms | ~370x |
| Q6  CustomerID=1045, limit 10000 | 162 | 9,036 ms | 162 | 1 ms | ~9000x |
| Q7  EmployeeID in (279,281,283) | 10000 | 4,925 ms | 10000 | 121 ms | ~40x |
| Q8  TotalDue 10000..50000 | 10000 | 1,418 ms | 10000 | 109 ms | ~13x |
| Q9  count EmployeeID=279 | (count) | 870 ms | 6030 | 7 ms | ~120x |
| Q10 avg TotalDue by EmployeeID top 5 | 17* | 8,445 ms | 5 | 136 ms | ~60x |

Load: SQL ~10,150 orders/s, document ~42,000/s. Index build: SQL **36,688 ms**,
document 580 ms.

Result counts agree between the two modes (Q1 6030 = Q9 6030, Q4/Q5 hit the 5000
limit, Q6 162, Q7/Q8 hit 10000), which confirms the two harnesses run equivalent
queries over equivalent data.

## What this exposes (the honest read)

**Document mode uses its secondary indexes; SQL mode does not.** Every SQL query
here runs in seconds, which is full-collection-scan territory for 100k rows, while
the same predicate in document mode is answered from the index in single-digit to
low-hundreds of milliseconds. The point queries are the tell: Q6 (`CustomerID=1045`,
162 matches) takes 9 seconds in SQL and 1 ms in document. The planner is not
selecting the index for these `WHERE col = <int>` predicates on the orders table
even though the indexes exist and were built.

This is the opposite of the YCSB result, where SQL point reads are only ~1.5x
behind document. The difference is that YCSB queries by the **primary key** (which
SQL does seek via `PrimaryKeyScanIterator`), whereas these queries filter on
**secondary-indexed** columns, and that path is not being taken.

Two more SQL issues this surfaced:

1. **Index build is pathologically slow** — 36.7 s to index 100k rows on three
   columns (document: 0.58 s). That alone makes the SQL side impractical to scale
   to 1M/10M until it is fixed.
2. **`LIMIT` after `GROUP BY` is not applied** — Q10 returns 17 rows (all
   employees) instead of the top 5; the document aggregate correctly returns 5.

## A crash bug this benchmark found and fixed

The first SQL run crashed the server (SIGABRT) on `SELECT *`. Root cause: a
double-free in the `SELECT *` row-materialisation (`executeStatementInternal`,
the `.star` branch): the merged row object was released both by an explicit
`merged_obj.deinit` and by `freeClonedJson(merged_val)` wrapping the same map, so
the map's backing array was freed twice. YCSB never hit it because it only ever
selects explicit columns; `SELECT *` over a table with non-TEXT columns aborts
under the C allocator. Fixed by dropping the redundant `deinit` (the fix is in
`src/query/query_executor.zig`).

## Bottom line

On this complex, secondary-index-driven query set, **document (NoSQL) mode is
dramatically faster** — one to nearly four orders of magnitude per query — because
it actually uses its indexes and builds them quickly. NovaDB's SQL engine has the
index machinery (`IndexScanIterator`) but is not applying it to these queries and
builds indexes far too slowly; those are the two things to fix to make SQL
competitive here, and they are follow-up engine work, not benchmark artifacts.
