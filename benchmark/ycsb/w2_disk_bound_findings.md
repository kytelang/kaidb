# W2: closing the disk-bound gap versus Postgres

The 10M-orders gap versus Postgres was buffer-pool thrash: once the working set
exceeds the pool, each matching row triggers a random `table_tree.search(pk)`
into the base table, and cold those random fetches hit disk one seek at a time.
Two techniques were tried against this. The first did not work; the second does.

Test rig throughout: 10M orders loaded in SQL mode, base + indexes = **9.3 GB**
on an **8 GB** box, query server restarted with a small **62 MB** buffer pool so
the base table cannot be pool-resident. OS file cache is controlled by reading a
24 GB junk stream before each cold run so the reads genuinely hit disk. A/B is
the same binary toggled by env var, so only the one change varies.

## Attempt 1 (rejected): bitmap-style PK reorder

Gather a batch of matching PKs, sort them by primary key, fetch the base rows in
physical order (Postgres "bitmap heap scan"). Measured no speedup cold or warm,
and a small penalty on point queries. Reason: at ~0.1% selectivity the matching
rows almost never share a 16 KB page, so sorted and random order touch the same
number of distinct pages and cost the same I/O while the sort is pure overhead.
Conclusion: fetch **order** is not the lever; page **residency** and fetch
**latency** are. The reorder is kept, defaulted OFF, env-gated
(`NOVADB_BITMAP_BATCH`) for the rare workload where matches cluster on a page.

## Attempt 2 (shipped): async prefetch of base-table leaves

Each index-scan operator now gathers a look-ahead batch of the upcoming matching
PKs (default 256, `NOVADB_PREFETCH_BATCH`), resolves them to their base-table
**leaf page ids without reading the leaves** (descend internal nodes only, which
stay cached, and read the last internal node's child pointer), and issues one OS
readahead per coalesced run of page ids (`readahead` on Linux, `F_RDADVISE` on
macOS). The device then services those random reads **in parallel** while the
scan materialises the batch, so per-row fetch latency overlaps instead of
serialising into one seek per row. Purely a hint: a stale/wrong id just wastes a
harmless readahead; the real read still goes through the normal search path.

Wired into every operator that does per-row base fetches: equality index scan,
ascending range scan, and descending (ORDER BY served-from-index) range scan.
Batch order is preserved (prefetch does not reorder), so ORDER-BY-from-index
stays correct.

### Results (ms, 10M, 62 MB pool, cold cache)

| Query | PF-OFF | PF-ON | speedup |
|-------|-------:|------:|--------:|
| Q1 employee_id=279            |  432 | 181 | **2.39x** |
| Q2 employee_id + total_due    |  286 | 159 | **1.80x** |
| Q3 emp + total_due, ORDER DESC| 1502 | 492 | **3.05x** |
| Q4 total_due>50k              |  668 | 171 | **3.91x** |
| Q5 Q4 ORDER DESC              |  682 | 160 | **4.26x** |
| Q6 customer_id=1045           | 1981 | 320 | **6.19x** |
| Q7 employee_id IN (...)       |  258 | 122 | **2.11x** |
| Q8 total_due range            | 1351 | 344 | **3.93x** |
| Q9 count employee_id=279      |   25 |  24 | 1.04x (no base fetch) |
| Q10 group by                  |  761 | 746 | 1.02x (no base fetch) |

Every query that does per-row base fetches gains 1.8-6.2x on a cold cache. Cold
PF-ON is now within ~2x of the warm (fully-cached) numbers, versus 5-11x before,
so the prefetch closes most of the disk-bound gap. Q9/Q10 are index-only /
aggregate and correctly unaffected.

## Where the lever lives

- **Data fits in RAM:** buffer-pool auto-sizing (default pool ~50% RAM, in
  `src/common/config.zig`) keeps the working set resident; queries run at warm
  speed with no disk I/O.
- **Data exceeds RAM (this test):** async prefetch overlaps the unavoidable disk
  reads. Combined, these are the two levers that make kaidb competitive at 10M.

Implementation: `Pager.prefetchPages` (`src/storage/pager.zig`),
`BPlusTree.collectLeafPageIds` / `internalDepth` / `leafPageIdForDepth`
(`src/storage/btree.zig`), and the batch+prefetch path in the three index-scan
operators (`src/query/iterator.zig`).
