# Q8 range-scan gap: closed (NovaDB vs SQLite, 1M orders, warm)

**Date:** 2026-09-01
**Workload:** the MongoDB `mongodb-perf-compare` orders port, 1,000,000 orders, ten queries.
**Machine:** darwin/arm64. NovaDB `-Doptimize=ReleaseFast`, pool_size 150000 (whole db warm).
SQLite via .NET 9 / Microsoft.Data.Sqlite, `cache_size=-1200000` (whole db warm), indexes on
`employee_id`, `customer_id`, `total_due`.

Both databases were built once and kept on disk; every run below is query-only against those
kept databases (no reload). "Warm" is the second pass, after the first has faulted the working
set into the pool / page cache.

## Headline

Q8 (`TotalDue BETWEEN 10000 AND 50000 LIMIT 10000`, unordered) went from **142 ms to 31 ms** warm,
which lands it next to SQLite's 24 ms. It was the one query where NovaDB was materially behind; the
gap is now closed. With that fixed, NovaDB is competitive on every query and clearly ahead on the two
that do real work beyond a point/range fetch (indexed ORDER BY, and the group-by aggregate).

## Warm numbers (pass 2, milliseconds, lower is better)

| Query                                             | NovaDB before | NovaDB after | SQLite | Notes |
|---------------------------------------------------|--------------:|-------------:|-------:|-------|
| Q1  EmployeeID=279 LIMIT 10000                     | 23            | 23           | 20     | index eq |
| Q2  EmployeeID=279 AND TotalDue>10000 LIMIT 10000  | 34            | 31           | 22     | index eq + residual |
| Q3  Q2 ORDER BY TotalDue DESC LIMIT 10000          | 30            | 27           | **226**| NovaDB indexed ORDER BY |
| Q4  TotalDue>50000 LIMIT 5000                      | 19            | 16           | 12     | index range |
| Q5  Q4 ORDER BY TotalDue DESC LIMIT 5000           | 13            | 13           | 11     | |
| Q6  CustomerID=1045 LIMIT 10000                    | 6             | 5            | 3      | index eq |
| Q7  EmployeeID IN (279,281,283) LIMIT 10000        | 26            | 26           | 19     | index union |
| **Q8  TotalDue 10000..50000 LIMIT 10000**          | **142**       | **31**       | **24** | index range + LIMIT |
| Q9  count EmployeeID=279                           | 1             | 1            | 1      | index-only count |
| Q10 avg TotalDue by EmployeeID, top 5              | 203           | 203          | **472**| NovaDB index-only group |

Row counts differ slightly on the customer/employee-selective queries (e.g. Q6: NovaDB 1573,
SQLite 1593) because the two loaders use different RNGs to synthesise the orders; the query shapes
and the volume of work are identical, so the timings are comparable.

## What Q8's 142 ms actually was

In-process phase profiling (`NOVADB_QPROF=1`) of the Q8 read path:

```
before:  candidate=5.2   sortids=122.8   read=2.1   ...   (ms)
after:   candidate=0.1   sortids=1.4     read=25.5  ...   (ms)
```

The predicate `TotalDue ∈ [10k, 50k]` matches a large fraction of the collection. The old planner
walked the whole matching index range, materialised **every** matching `_id` (hundreds of thousands
of them), and then sorted that entire id set into `_id` order for read locality, purely to read the
first 10,000. The sort (`sortids`) was 122.8 ms of the 142 ms. SQLite, by contrast, streams the index
range and stops at the LIMIT.

## The fix: push the LIMIT into the index scan when the index covers the whole filter

An unordered `LIMIT k` is satisfied by *any* `k` matching rows. When the chosen index covers the
entire filter (the filter has exactly one field, so there is no residual predicate the base-document
read has to re-check), every id the index yields is a genuine match. So the index leaf walk can stop
after `k` ids instead of gathering the whole range and sorting it.

- `src/document/doc_index.zig` — `scanIds` / `findEq` / `findRange` take a `limit`; the leaf walk
  stops once `limit` ids are collected (`limit == 0` means "all", unchanged behaviour).
- `src/document/store.zig` — `tryIndexPlan` computes `exact = (filter has exactly one field)` using a
  new, allocation-free `BsonDocument.fieldCount()`, and only then passes `limit = cap` to the index
  seek. Multi-field filters (Q2/Q3, where a residual predicate can still reject a candidate) keep
  `limit = 0` and let `findCapped` re-check and stop at `cap` *matches* — so correctness is unchanged.
  The `$in` union and the ORDER-BY path (which must see every candidate before sorting) also pass
  `limit = 0`.
- `bson` `src/document.zig` — added `fieldCount()` (mirrors `BsonArray.len`, no allocation).

Net effect for Q8: the O(matches) materialise-and-sort becomes an O(cap) scan, the `cap` ids are still
sorted for read locality (now 1.4 ms, not 122.8 ms), and the returned count is exactly 10,000.

## Scope note

This is a query-planner improvement on the document read path; it does not change the
2026-08-30 scoping decision. NovaDB remains the orchestrator's embedded control-plane store. These
1M warm numbers say the engine is competitive with SQLite for a warm, in-pool working set on this
workload; they do not speak to the once-the-working-set-exceeds-the-pool behaviour that the 10M
benchmark exposed. See `orders_benchmark.md` and `CLAUDE.md`.

## Verification

- `zig build -Doptimize=ReleaseFast` clean.
- `zig build test` green (the intermittent `PageStillPinned` concurrency-fuzzer race is pre-existing
  and unrelated to this read-path change).
- Q8 returns 10,000 rows before and after.
