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

## Lever-4 investigation (2026-09-18): root cause pinned, fix blocked on a disk rig

Instrumented the leaf-reuse cursor to decompose the ~48k fetches of one 10k-row range
scan (64 MiB pool):
- **Serve pass is already minimal.** The `LeafReuseSearcher` did 2,775 re-descends +
  7,225 reuse hits (72% reuse) for 10k rows => ~8k base fetches. Working as intended.
- **The amplification is the redundant PREFETCH descent.** `collectLeafPageIds` does a
  fresh root-to-leaf descent PER PK (~10k) just to compute OS-readahead hints, on top of
  the serve descent. `shouldPrefetch` fires whenever the pool hit ratio dips below 98%,
  which a scattered secondary-index range scan trips even on a warm-ish pool. Those ~10k
  redundant descents (each ~3 internal-node touches) are the bulk of the ~48k fetches.

**Attempted fix (reverted):** make `collectLeafPageIds` reuse the leaf across the sorted
batch (descend once per distinct leaf). Result at a 20k batch: fetch COUNT halved
(48k -> 25k) but wall time DOUBLED (70 -> 132 ms), because reading each leaf page to learn
its key range costs more than the internal-node-only descent it replaces, and most of the
"saved" fetches were already cheap cached internal-node hits. Net-negative, reverted.

**The real blocker.** On this box the OS page cache holds the whole ~1 GB file, so a pool
miss is an OS-cache memcpy (microseconds), not a disk seek. The prefetch's entire purpose
is to overlap real disk-seek latency, which is invisible here, so the small-pool test only
exposes the prefetch's CPU overhead, not its I/O benefit. Any change that trims prefetch
overhead helps THIS box but could hurt a genuinely disk-bound deployment, and vice versa.
**Optimising the >RAM miss path correctly requires a true disk-bound rig** (a dataset
larger than RAM on the target box, a RAM-constrained VM, or dropping the OS cache per
query), so the fix is measured against real disk behaviour instead of OS-cache artefacts.

**Candidate fixes to measure on such a rig, in order:**
1. Parent-reuse in `collectLeafPageIds` (cache the last internal node and re-pick the
   child; no leaf read) so the redundant descent costs ~1 internal touch, not ~3.
2. Piggyback prefetch on the serve cursor (issue readahead for the next sorted PKs' leaves
   as the `LeafReuseSearcher` advances) so there is no separate descent at all.
3. A better prefetch gate (trigger on genuine eviction/thrash, not a raw hit-ratio
   threshold) so the redundant work only runs when real disk I/O is being overlapped.
