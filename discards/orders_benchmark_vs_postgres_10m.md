
---

## Update (2026-09-13): the gap was the buffer pool — auto-sizing closes it

The original comparison was **memory-unfair**: KaiDB ran with its fixed default
buffer pool of 10000 pages (160 MB at a 16 KB page) while PostgreSQL had 128 MB
shared_buffers **plus** the OS page cache holding the working set in RAM. Profiling
pointed at per-row base-table lookups going to disk (`PagePool.fetchPage → preadv`)
once the working set exceeded the tiny pool. It was pool thrashing, not the wire
protocol and not primarily the per-row re-search.

Evidence (3M orders, ~3 GB data, same 8 GB machine):

| Query (ms) | 160 MB pool | 4 GB pool (resident) |
|---|---|---|
| Q1 | 520 | 91 |
| Q3 sort | 1021 | 61 |
| Q4 | 503 | 31 |
| Q6 | 662 | 40 |
| Q8 | 1045 | 59 |
| Q10 | 214 | 67 |

With the working set resident, KaiDB's SQL queries are **Postgres-competitive**
(compare PG's 10M numbers: Q1 120, Q3 74, Q4 117, Q6 89, Q8 44, Q10 730).

**Change made:** `pool_size` now defaults to `0` = **auto**, sizing the buffer
pool to ~50% of physical RAM (leaving 1 GB headroom, floor 160 MB, cap 8 GB) at
server startup (`config.zig` `effectivePoolPages`/`autoPoolPages`; logged as
`buffer pool: N pages (M MiB) [auto]`). An explicit `pool_size` still pins a
fixed count. On the 8 GB box this selects a 4 GB pool and the numbers above are
produced with no manual tuning.

**Still open (data > RAM):** at 10M on 8 GB the heap (6.5 GB) cannot fully fit,
and KaiDB remains ~5-30x behind PG on range/find queries because its per-row
root-descent re-search touches more pages, in random order, than PG's
index-scan-then-heap-fetch. Closing the data-exceeds-RAM case needs the W2
ordered/bitmap base-table access (gather PKs, sort, walk the heap sequentially,
decode in place). Pool sizing matches Postgres whenever the working set fits RAM,
which is the common case for KaiDB's scoped role.
