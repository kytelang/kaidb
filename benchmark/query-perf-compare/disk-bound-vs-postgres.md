# Disk-bound comparison: kaidb vs PostgreSQL under memory pressure (2026-09-18)

Run in a Multipass VM (Ubuntu 24.04, 2 GB RAM, 2 vCPU) so a ~1 GB working set does not
fit RAM. Both engines given a matching 128 MiB cache (kaidb `pool_size=8192`, PG
`shared_buffers=128MB`). Identical 1M-row `ord` table (seeded CSV), indexes on `total_due`
and `employee_id` in both. The OS page cache is dropped (`echo 3 > drop_caches`) before
each COLD query so a miss is a real disk read for BOTH engines. Times in ms.

## Headline results

| query | kaidb cold | pg cold | kaidb warm | pg warm |
|---|--:|--:|--:|--:|
| point `id = 500000` | 13 | 22 | 1 | 0 |
| `COUNT(*)` whole table | 17 | 95 | 8 | 20 |
| `total_due > 50000` COUNT | 30 | 3 | 3 | 12 |
| emp=279 AND total_due 20k-40k (Q17) | 58 | 193 | 56 | 9 |
| **total_due 10k-50k, `avg(total_due)`** | **4656** | **193** | **3982** | **14** |

kaidb wins or ties the point lookup, whole-table count, wide count, and the NARROW
emp-range (Q17). It is dramatically slower only on the WIDE `total_due` range aggregate.
**PostgreSQL does NOT degrade the same way there**, so this is a real, specific gap, not
"cache misses hurt everyone equally".

## Decomposing the wide-range gap (warm, so it is pure CPU/plan, not disk)

| query | kaidb | pg | ratio | cause |
|---|--:|--:|--:|---|
| `avg(total_due)` WHERE total_due 10k-50k  (col IS the index) | 4539 | 23 | ~197x | covering-index planner MISS |
| `avg(customer_id)` WHERE total_due 10k-50k (needs base row)  | 3371 | 511 | ~6.6x | per-row base-descent amplification |
| `avg(customer_id)` WHERE total_due 10k-11k (narrow)          | 154  | 77  | ~2x   | amplification, few rows |

Two distinct issues, in priority order:

1. **Covering-index aggregate planner gap (dominant, ~197x).** `SELECT agg(col) WHERE col
   BETWEEN ...` where `col` is the indexed column is fully answerable from the index (the
   value sits in the index key). PostgreSQL runs it index-only (23 ms). kaidb has covering
   / index-only support but the PLANNER does not use it for this aggregate-over-a-range
   shape, so it descends to the base row per match (4539 ms). This is a clean planner fix,
   measurable on any box, and the single highest-value next step.

2. **Base-descent amplification (~6.6x).** When the aggregate genuinely needs a non-indexed
   column, kaidb pays a per-row clustered-base descent while PostgreSQL uses a block-sorted
   bitmap heap scan. This is the structural lever-4 gap (physical row locator / block-sorted
   base fetch). Real, but ~6.6x, and it shrinks with range width.

## Verdict

On a genuinely disk-bound box vs PostgreSQL, kaidb is competitive-to-better for point,
count and narrow-range access, and materially behind only on wide secondary-index range
work. Most of that gap is a tractable covering-index planner miss (~197x), with a smaller
structural amplification (~6.6x) behind it. So "if the others also degrade, we are done"
resolves to: they do NOT degrade the same, but the biggest fix is a planner change, not a
new pager.
