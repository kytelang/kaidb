# >RAM / small-pool baseline (2026-09-18)

Measures kaidb's miss-path cost by capping the buffer pool far below the dataset,
isolating what lever 1 (async I/O) and lever 4 (fewer descents) each target. Same box,
same ReleaseFast build, 1M-row orders workload, kaidb only.

## Q1..Q18: 64 MiB pool vs 4 GiB (auto) pool, ~1 GB dataset (~16x over the small pool)

| Query | 64 MiB pool (ms) | 4 GiB pool (ms) | penalty |
|---|--:|--:|--:|
| Q1  emp point            | 25  | 13 | 1.9x |
| Q3  range + ORDER BY     | 225 | 95 | 2.4x |
| Q4  total_due range      | 115 | 18 | 6.4x |
| Q5  range + ORDER BY     | 137 | 38 | 3.6x |
| Q6  customer_id          | 71  | 8  | 8.9x |
| Q8  total_due range      | 280 | 37 | 7.6x |
| Q12 PK range             | 40  | 11 | 3.6x |
| Q13 COUNT(*) whole       | 22  | 4  | 5.5x |
| Q17 emp AND range        | 469 | 31 | 15x  |
| Load / index build       | 12.0s / 18.4s | 5.5s / 5.4s | ~2-3x |
| PK/agg/index-only (Q9/11/14/15/18) | ~unchanged | | ~1x |

Buffer-pool hit ratio: 99.2% at 64 MiB, 100% at 4 GiB. BOTH runs issue the identical
~23.47M cumulative page fetches - the query plans generate the same fetch volume
regardless of pool size; the pool only decides whether each fetch is resident or a
re-read.

## Root-cause probe: one range scan in isolation (64 MiB pool, 300k rows)

`SELECT id, comment FROM ord WHERE total_due BETWEEN 10000 AND 50000 LIMIT 10000`
- Page fetches for the query: **49,889** (~5 fetches per returned row = index-leaf +
  per-row base-tree descent).
- Misses: 3,637 (7%). Hits: 46,252.
- Wall: 90 ms => ~1.8 us/fetch, dominated by the FETCH VOLUME (CPU: latch, hash probe,
  refcount, descent), not by waiting on the 3,637 misses.

## Interpretation (what to optimise first)

The >RAM penalty has two parts:
1. **Fetch amplification** (~5 page touches per row): constant at every pool size,
   CPU-bound. Addressed by **lever 4** - a physical row-locator hint in the secondary
   index (1-2 fetches/row like a heap TID) and/or a block-sorted + deduped base fetch.
   Directly measurable on this box.
2. **Real disk misses**: only dominate on a genuinely disk-bound (dataset > RAM)
   workload. Addressed by **lever 1** (async overlapped reads) + **lever 2** (preadv
   combining). Their win cannot be measured on this box because the OS page cache
   absorbs a 1 GB file; needs a >RAM dataset or a dropped OS cache to validate.

**Conclusion:** on this hardware the dominant, measurable cost is fetch amplification, so
**lever 4 is the higher-impact next implementation**; lever 1 remains the right lever for
a true disk-bound deployment and should be validated on a >RAM rig.
