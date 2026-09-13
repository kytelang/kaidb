# Matching SQLite's page management

## Why this document exists

A MongoDB `mongodb-perf-compare` port (10M orders, ten complex queries; see
`orders_benchmark.md`) measured NovaDB's document surface against PostgreSQL and
SQLite. After a run of query planner and storage fixes this session (index only
count, `$in` over the index, indexed and compound ordered scans, index only
aggregation, and a storage compaction pass that took leaf fill from 65 percent to
97 percent and the on disk footprint from 13 GB to 9.6 GB, near SQLite's 8.6 GB),
the catastrophic minutes long queries are gone and most queries run in the
low hundreds of milliseconds.

What remains is a steady **2x to 4x gap versus SQLite on the read heavy queries**,
measured fairly. A fair measurement is one where the buffer pool is smaller than
the data, so pages genuinely evict, rather than a pool larger than the data (which
makes every page resident and is not a real comparison). At a 384 MB pool over a
990 MB collection (about 38 percent resident):

| Query | NovaDB (384 MB pool) | SQLite | Ratio |
|---|---:|---:|---:|
| Q1 EmployeeID point | 726 ms | 174 ms | 4.2x |
| Q2 + TotalDue filter | 381 ms | 186 ms | 2.0x |
| Q3 sort by other field | 348 ms | 127 ms | 2.7x |
| Q4 TotalDue range | 130 ms | 63 ms | 2.1x |
| Q5 sort | 190 ms | 58 ms | 3.3x |
| Q6 CustomerID point | 62 ms | 163 ms | 0.4x (faster) |
| Q7 `$in` | 71 ms | 4 ms | 18x |
| Q8 range | 211 ms | 117 ms | 1.8x |
| Q9 count | ~0 ms | 16 ms | faster |
| Q10 aggregate | 305 ms | 549 ms | 0.6x (faster) |

The gap is not in the query planner any more. It is in the **page layer**: reading
pages and caching them. This document records the exact difference between how
SQLite manages pages and how NovaDB does, and lays out a plan to close it.

The scope note from `CLAUDE.md` still holds: NovaDB's supported role is the Nova
orchestrator's control plane store (small, bounded data), where none of this
matters because the whole store fits in the pool. The work below is about a
general document or OLTP workload, which is explicitly out of that scope. It is
recorded here because the analysis is sound and the fixes are real, so a future
decision to widen the scope has a costed plan to start from.

## How SQLite manages pages (the reference)

Studied from `sqlite-master/src/pager.c`, `pcache.c`, `pcache1.c`, `os_unix.c`.
Entry point is `sqlite3PagerGet` (pager.c:5788), a single indirect call to a
pre bound getter, so there is no per get mode branch.

### A cache hit is almost free

`getPageNormal` -> `sqlite3PcacheFetch` -> `pcache1Fetch` -> `pcache1FetchNoMutex`
(pcache1.c:1005). In the default single connection configuration the page group
mutex is null, so **a hit takes no lock at all** (pcache1.c:1069-1074). The lookup
is a bitmask hash index (`apHash[iKey & (nHash-1)]`, pcache1.c:1017; `nHash` is a
power of two, so it is an AND, not a divide). The pin is `nRef++`
(pcache.c:541-542) plus an O(1) unlink from the LRU list. The returned page's
`pData` already points at the frame buffer, so **there is no copy**. A hit is a
few dozen instructions: hash probe, a couple of pointer swaps, two counter bumps.

### Eviction is O(1) with no scan

Pinned versus unpinned is encoded purely by LRU list membership:
`PAGE_IS_PINNED(p)` is `p->pLruNext==0` (pcache1.c:133). A pinned page is simply
not on the list, so it can never be chosen and there is no victim scan. Recycling
takes the LRU tail (`pGroup->lru.pLruPrev`, pcache1.c:908). Eviction does not run
on a hit, and does not run on a miss while the cache is below its maximum.

### The mmap path is the important one

`getPageMMap` (pager.c:5703) is chosen when the page is greater than 1 and no
write transaction is open. It calls `sqlite3OsFetch` -> `unixFetch`
(os_unix.c:5704), which returns a pointer straight into the memory mapped file
region:

```c
*pp = &((u8 *)pFd->pMapRegion)[iOff];   // os_unix.c:5722
```

`pagerAcquireMapPage` (pager.c:4124) then allocates only a small `PgHdr` header
(reused from a free list) and sets `p->pData = pData`, pointing at the mapped
region. It never allocates a page sized frame and never calls the page cache.

So a mapped get avoids **both** the `pread` syscall (the page is already in the
process address space through the file mapping and the OS page cache) **and** the
memcpy into a private frame. The page bytes are faulted in lazily by the CPU on
first touch. This is the single largest structural advantage: in mmap mode the
**OS page cache is the buffer pool**. There is no fixed pool ceiling and no second
copy. SQLite falls back to the normal copy in path only for page 1, for pages in
an open write transaction, for offsets beyond the configured mmap size, or when a
WAL frame shadows the page (pager.c:5735-5767).

### A miss is one page, one read

On a real miss (non mmap), `pcache1AllocPage` takes a frame from a per cache free
list (a slab allocated up front) or recycles the LRU tail, and `readDbPage`
issues exactly one `pread` of one page into that frame (pager.c:3094). There is
no read ahead.

## How NovaDB manages pages (the current state)

Studied from `src/storage/pool.zig`, `src/storage/pager.zig`, `src/storage/btree.zig`.
Hot path is `PagePool.fetchPage` (pool.zig:470).

### A cache hit takes a lock and two atomics, per level, per read

The pool is sharded into 16 instances (4 for a tiny pool), each with its own
`page_table` hashmap, free list, CLOCK hand, and `rw_lock` (pool.zig:164-184).
A hit (pool.zig:474-481):

```zig
inst.rw_lock.lockShared(self.pager.io);                   // real reader/writer lock
if (inst.page_table.get(page_id)) |fid| {                 // hashmap get (modulo)
    const f = &self.frames[fid];
    _ = @atomicRmw(u32, &f.pin_count, .Add, 1, .seq_cst);
    @atomicStore(bool, &f.is_referenced, true, .seq_cst);
    inst.rw_lock.unlockShared(self.pager.io);
    return f;
}
```

So every fetch, including a hit, takes the shard reader/writer lock (a genuine
futex or atomic round trip, not a free threaded no op) plus two `seq_cst` atomics.
On top of that the B+Tree takes a per frame `RwLock` shared latch for every page
it descends through (`findLeafShared`, btree.zig:736 and 749). `unpinPage`
(pool.zig:581) re takes the shard shared lock and does a CAS loop.

### There is no cursor: every document re descends from the root

`docReadVisible` -> `tree.search` -> `findLeafShared` starts from
`self.root_page_id` and re fetches the root and every branch page on **every
call** (btree.zig:733-757). There is no `BtCursor` equivalent that holds the
descent path. Reading 10,000 documents on a height 3 tree does 30,000 `fetchPage`
calls, and the root page is fetched, locked, pinned, and unpinned 10,000 times.
These are cache hits, so no I/O, but each still pays the full hit cost above.

### Every read copies the page; there is no mmap

`pager.readPage` is `file.readPositionalAll(...)` into the frame's slab window
(pager.zig:193, pool.zig:505). The pager header itself says "There is no page
cache here" and it is "a thin, correct file mapper". So a miss always `pread`s a
full 16 KB page into a private pool frame that shadows the OS page cache. This is
classic double buffering, and it is why a 160 MB pool over a multi GB collection
thrashes even though the machine has the data in the OS cache: the pool re fetches
and re copies rather than borrowing the resident OS page.

### About five heap allocations per document read

`tree.search` dups the raw record bytes (btree.zig:467); `reconstructVersionChain`
allocates a versions array and dups `fixed` and `heap` (database.zig:1687-1697);
`docReadVisible` dups the winning `fixed` a third time into the output
(database.zig:4589). SQLite hands back a pointer into the pinned page.

### Per document read cost summary

For a height H tree (typically 3 or 4), one visible recent version:

| Cost | Count | Where |
|---|---|---|
| `fetchPage` calls | H, root re fetched every read | btree.zig:735, 748 |
| Shard `rw_lock` shared pairs | about 2H (fetch plus unpin) | pool.zig:474/479, 585 |
| Frame `RwLock` shared pairs | H | btree.zig:736, 749 |
| seq_cst atomics | 2 per fetch, so 2H, plus CAS per unpin | pool.zig:477-478 |
| Heap allocations | about 5 (fast path) | btree.zig:467; database.zig:1687,1695,1697,4589 |
| Page bytes | copied via `pread`, never mapped | pool.zig:505, pager.zig:193 |

The three structural differences, in order of impact: no cursor (re descent and
its locks per document), no mmap (copy and double buffer per miss, plus the pool
ceiling), and a locked, allocation heavy hit path.

## The plan

Phased so each phase is independently shippable, verifiable, and reversible. Each
phase states what, where, expected impact, risk, and how to verify. Correctness is
non negotiable: every phase must keep the full unit suite green and the orders
benchmark result counts unchanged.

### Phase 1: cursor reuse on the sorted id read path (biggest contained win)

**Status: DONE** (commit `5b762f0`). Implemented as `CursorReader` in
`src/document/store.zig`, wired into `findCapped` and `count`, with
`Database.docVisibleFromRaw` and a `PagePool.fetch_count` counter for the tests.
Two Phase 1 tests are green: a full find over 150 multi-leaf documents returns
them all and costs fewer than N page fetches (impossible with a per-id descent),
and a selective filter returns the exact scattered subset with `count()`
agreeing. Orders benchmark result counts unchanged at 100k.

**What.** When a query has already produced a sorted list of candidate `_id`s
(the index plans do this, and the materialiser sorts for locality), read the
documents by stepping ONE held B+Tree cursor forward with a seek or step policy,
instead of calling `tree.search` (a fresh root to leaf descent) per id.

Seek or step, following SQLite's `OP_SeekScan` idea: keep the cursor positioned;
for the next target id, if it is within a small number of cells ahead, step the
cursor (an in leaf index bump, only crossing to the next leaf when the current one
is exhausted); if it is far ahead, re seek (a fresh descent). A tuned threshold
(about one to two leaves' worth of cells) picks the cheaper option per gap, so the
path is optimal for both dense and scattered id sets.

**Where.** A new reader in `src/document/store.zig` used by `findCapped`,
`count`'s fallback, and `orderedSortWindow`. It walks `tree.rangeScan(start, null)`
and steps; on a hit it copies the raw cell value and, after releasing the leaf
latch, reconstructs the visible version (a new `Database.docVisibleFromRaw`
helper, mirroring the index backfill decode) to avoid holding a leaf latch across
an overflow re read. Overflow cells fall back to the existing `readValue`.

**Impact.** Removes the per document re descent: the root and branch pages are no
longer fetched, locked, pinned, and unpinned once per document. For a 10,000 doc
read on a height 3 tree that is roughly 20,000 fewer `fetchPage` calls and their
shard locks, frame latches, and atomics. This directly attacks the cache resident
2x to 4x, since it is pure per hit overhead, not I/O.

**Risk.** Medium. Latch ordering: reconstruct visible versions after releasing the
scan leaf latch, never during. MVCC visibility must match `readValue` exactly
(same `isVisible` check). The seek or step threshold needs tuning but is not a
correctness concern.

**Verify.** Result counts unchanged on the orders benchmark at 100k and 1M.
`docs_scanned` metric drops. A microbenchmark of a 10,000 id read shows fewer
`fetchPage` calls (add a counter behind a debug flag). Full unit suite green.

### Phase 2: fewer allocations per read

**Status: DONE** (commit `69ad81e`). `Database.docVisibleFromRaw` now takes a
single-copy fast path when the newest packed version is visible (the common
case), via a pure allocation-free `Database.peekNewestVersion`, falling back to
full reconstruction otherwise. Phase 2 test packs a version and asserts the
decode fields, that `fixed` borrows the buffer, and the overrun guard. Suite
green; 100k counts unchanged.

**What.** Cut the per read allocations from about five to one or two. On the fast
path (single recent visible version), decode the visible bytes directly from the
raw cell without the intermediate `reconstructVersionChain` array and its `fixed`
and `heap` dups, and without the third dup into the output when the caller can
borrow.

**Where.** `Database.docReadVisible` and `reconstructVersionChain`
(src/schema/database.zig). Add a fast path that, when the newest packed version is
already visible (the common case, `xmin` committed and `xmax` zero), returns its
`fixed` slice with a single copy for the response, skipping the chain
reconstruction entirely.

**Impact.** Removes three to four allocations per document read. On a
cache resident scan this is a measurable fraction of the per read cost.

**Risk.** Low to medium. Must preserve exact MVCC semantics for the non fast path
(older versions, tombstones); the fast path must fall through to the full chain
whenever the newest version is not the visible one.

**Verify.** Result counts unchanged. Allocation count per read drops (ASAN or a
counting allocator in a unit test). Suite green.

### Phase 3: cheaper cache hit

**Status: PARTIAL** (commit for Part 1 below). Part 1 (relax the hit-path
atomics) is DONE: the `pin_count` add and `is_referenced` store on the
`fetchPage` hit path, and the `pin_count` CAS in `unpinPage`, are now
`.monotonic` rather than `.seq_cst`. They run under the shard SHARED lock and are
only observed by the evictor under the EXCLUSIVE lock, so the rwlock's
acquire/release already provides the ordering; the atomics only need atomicity.
On ARM64 this drops the per-hit memory barriers `.seq_cst` emits (identical on
x86). The `is_dirty` store is deliberately left `.seq_cst` (durability). Verified
by the concurrency fuzzer (`NOVADB_FUZZ=1`) and a deterministic pin/unpin balance
+ underflow-guard test. Part 2 below (the lock-free hit) is NOT done: it needs a
concurrent-safe page table (the current `std.HashMap` can realloc its buckets on
a resize, so an optimistic reader cannot safely race a writer without an
RCU/epoch scheme or a fixed open-addressing table), which is a larger sub-project
and is deferred. Note that Phase 1 (cursor reuse) already amortised the per-hit
lock cost for the read path from O(N * height) to O(leaves), so Part 2's marginal
value is now lower than the analysis originally implied.

**What.** Make the read hit path in `fetchPage` cheaper, closer to SQLite's
lock free hit. Options, in increasing difficulty:

1. Reduce atomics: `is_referenced` can be a relaxed store rather than `seq_cst`;
   `pin_count` can use a cheaper ordering where correctness allows.
2. An optimistic, lock free hit: read the shard's hashmap under an seqlock or an
   epoch scheme so a hit that finds the page does not take the shard reader/writer
   lock, falling back to the locked path only on a miss or a concurrent resize.

**Where.** `src/storage/pool.zig` `fetchPage` and `unpinPage`, and the shard
`page_table` access.

**Impact.** Removes one real lock acquire and release pair per page per read
(fetch and unpin), which at 30,000 fetches per query is significant. This is the
`pcache1FetchNoMutex` idea.

**Risk.** High if the lock free hit is attempted (concurrency correctness with the
CLOCK evictor and resize). The atomic ordering relaxation (option 1) is low risk
and can ship first. The server insert and read path is effectively single reactor,
so contention is low, but the lock acquire cost itself is what we are removing.

**Verify.** Concurrency fuzzer (`NOVADB_FUZZ=1`) stays green, including the
`GroupLock` and `STRESS` consistency tests. No `PageStillPinned` regressions.
Throughput gate unchanged or better.

### Phase 4: mmap backed reads (the deep, structural fix)

**Status: BUILT, CORRECT, VERIFIED, FLAGGED OFF - measurement refuted the
hypothesis.** The mmap read path is fully implemented and gated behind the
`mmap_reads` config flag (POSIX only): a read-only `MAP_SHARED` map of the file
(`pager.zig`), clean read misses bind the frame view to the map with no `pread`
copy (`pool.fetchPage`), a `beginWrite` copy-out hook makes a page writable
before any modify (the map is `PROT_READ`, so a missed hook SIGBUSes rather than
corrupts), growth is handled by a retire-not-unmap remap so borrowed pointers
stay valid, and the whole thing is exercised end to end via `NOVADB_MMAP=1`.
Verified: full unit suite, the concurrency fuzzer (`NOVADB_FUZZ=1`), and the
crash-recovery tests all pass with mmap ON; a 100k orders load + query with the
server in mmap mode stays alive (no SIGBUS, so every write-after-borrow path is
hooked) with result counts unchanged.

**But measurement showed no speed-up (and some regressions), so it is OFF by
default.** On the fair 384 MB-pool / 990 MB-data test the mmap numbers were
mixed-to-worse versus the copy-in path. The reason is concrete: at a scale where
the file is warm in the OS page cache, a `pread` is already a fast in-memory copy,
and the buffer pool's real per-fetch cost is its eviction machinery (the CLOCK
sweep plus the shard and frame locks), NOT the page copy. Replacing the copy with
a borrow-plus-page-fault removes the cheap part and adds a `map_lock` per fetch, so
it does not help this workload. The copy was never the bottleneck; the pool's
fixed FRAME COUNT (eviction churn) is. Removing that ceiling needs the frame/slab
decoupling (many cheap frames that borrow the map), for which this mmap layer is
the base; that decoupling, and the disk-bound case where mmap would earn its keep,
remain the follow-on. Two constraints found earlier still hold and shaped the
build (kept below for the record):

1. **The portable map is not write coherent by contract.** The available
   cross platform wrapper `std.Io.File.MemoryMap` documents its `memory` slice as
   one that "may or may not remain consistent with file contents. Use `read` and
   `write` to ensure synchronization points." That is an explicit sync model, not
   a live `MAP_SHARED` view. Serving a page read from that map could therefore
   return STALE bytes after a concurrent `writePage`, unless the whole map is
   resynced on every write, which defeats the purpose. The only coherent option is
   a raw `std.posix.mmap(MAP_SHARED)`, but that (a) bypasses the engine's
   Io native design (the pager speaks only `std.Io.File`), (b) is platform
   specific, and (c) is unverified on the Windows target. In a storage core a
   stale read is silent data corruption, so this is a hard stop, not a tuning
   knob.
2. **The value (removing the pool ceiling) needs the frame model change too.** A
   memcpy from a coherent map would only remove the read syscall; it still copies
   into a pool frame and is still bounded by `pool_size`, so it does NOT deliver
   Phase 4's actual goal (the OS cache becoming the effective cache, which is what
   the fair 384 MB test showed to be the real lever). Delivering that needs the
   read path to return a BORROWED pointer into the map with no pool frame, which
   in turn needs the B+Tree read descent (`findLeafShared`, `RangeIterator`) and
   the `Frame`/latch model to accept a frame whose bytes are not pool owned and
   whose "latch" is a no op for an immutable mapped read. That is a change to the
   most safety critical code in the engine, plus the file growth remap must be
   safe while readers hold borrowed pointers (SQLite bumps a generation and falls
   back).

**Conclusion.** Phase 4 done correctly is a dedicated, carefully tested
storage engine project (coherent mapping strategy per platform, the frame model
change, growth remap safety, and re verification of crash recovery and the
concurrency fuzzer with the flag on), not a safe increment to fold into a session
that also touched the read path. It is the right next investment for a general
workload but must be scheduled as its own effort with its own test plan. The
gains from Phases 1 to 3 (cursor reuse, single copy reads, cheaper hits) are
independent of it and stand on their own.

**What (the eventual design, constrained by the evidence above).** Three code
facts, established by reading the engine, shape the only correct design:

- A read still needs a FRAME for its latch. `search` runs under `structure_lock`
  SHARED (`btree.zig:451`), but so does an in-place cell rewrite (`update`,
  `btree.zig:480`); the two are coordinated by the per-leaf `latch` on the frame.
  A map-backed read with no frame cannot coordinate with a concurrent in-place
  writer, so it could observe a torn page. Therefore reads cannot bypass frames
  entirely; the map removes the COPY, not the frame.
- Writes must go through the WAL, never the map. A committed-but-not-checkpointed
  change lives in a dirty pool frame, not in the file; and the eviction path
  `pwrite`s dirty pages to the file (`pool.zig` flush). So a resident page is
  authoritative in the pool and a read must check residency first (exactly
  SQLite's fallback when a page is dirty / a WAL frame shadows it).
- The `slab` is 1:1 with frames AND backs the doublewrite crash-recovery staging
  area (pages 2..). Removing the pool ceiling means decoupling frame COUNT from
  slab BYTES, which therefore also intersects the doublewrite path.

Given those, the complete design is:

1. Memory map the file READ ONLY (`PROT_READ`, `MAP_SHARED` for coherence with
   `pwrite` on the run-verified POSIX hosts; a raw `std.posix.mmap` guarded to
   non-Windows with a `pread` fallback, since the portable `std.Io.File.MemoryMap`
   is not write coherent by contract). Read-only is deliberate: it is the fail
   fast net. Any write to a still-mapped page SIGBUSes (a loud crash in tests)
   instead of silently corrupting the file.
2. A clean, non-resident read binds the frame's `data` to the map window (no
   `pread`, no copy); the checksum is validated against the mapped bytes. A dirty
   or resident page uses the pool copy as today.
3. Decouple frame count from the slab: clean frames borrow the map and own no
   slab window, so the pool can hold far more frames (the read cache becomes the
   OS page cache, removing the ceiling); dirty pages draw a writable window from a
   separate, smaller write-slab pool.
4. Copy on write hook: before the B+Tree modifies a page (the exclusive-latch
   write descent and every in-place cell rewrite / split), it calls a new
   `pool.beginWrite(frame)` that, if the frame borrows the map, copies the map
   window into a write-slab window and repoints `data` there. The read-only map
   guarantees a missed hook faults rather than corrupts.
5. Growth remap: on file extension, remap (or extend) the mapping; borrowed
   pointers held by in-flight readers must stay valid or be revalidated (SQLite
   bumps a generation and falls back to the copy path across a remap).

**Where.** `src/storage/pager.zig` (the mmap region + `mapView(page_id)` returning
a borrowed read-only window or null; growth remap), `src/storage/pool.zig` (the
frame/slab decoupling, `fetchPage` routing clean non-resident reads to `mapView`,
`beginWrite` copy-out, eviction of borrowed frames with no write-back, and keeping
the doublewrite staging area on its own dedicated buffers), and the B+Tree write
sites in `src/storage/btree.zig` that must call `beginWrite`. Behind a
`mmap_reads` config flag, default off.

**Impact.** Removes the 16 KB copy per miss and the second resident copy per page,
and removes the artificial pool size ceiling: a query over a multi GB collection
that fits in RAM is served from the OS page cache without pool thrash. This is what
would let NovaDB lean on the OS cache the way SQLite does, and is the largest
remaining lever on the miss path.

**Risk.** High. This is a storage core change. Correctness concerns: safe remap on
file growth while readers hold borrowed pointers (SQLite bumps a generation and
falls back); interaction with the WAL, checkpoint, and the MVCC undo log; page
checksums (validated on copy in today; a mapped read must decide when to verify);
and cross platform mmap (macOS, Linux, and the Windows target which is not run
verified). Land it behind a config flag (`mmap_reads`), default off, so the copy
in path stays the safe default until it is proven.

**Verify.** Crash recovery tests (kill 9, torn WAL) still pass. Concurrency fuzzer
green with `mmap_reads` on. Recovery correctness at 200k and the orchestrator live
store, lease, and reconcile tests pass with the flag on. The orders benchmark at
10M with a modest pool shows the miss heavy queries drop toward SQLite, because
misses no longer copy and the OS cache holds the working set.

### Phase 5: frame/slab decoupling - INVESTIGATED, REVERTED (not an optimization)

**Status: tried, measured, reverted. It is not an optimization over simply sizing
the pool to the working set.** The idea was to keep a small write/materialise
budget but a large mmap-backed, demand-zero frame pool, so a working set larger
than the budget would be served churn-free from borrowed (non-resident) windows -
"a big cache at small RAM". A deterministic micro-benchmark did show the eviction
count drop from ~118000 to 0 when the frame count covered the set. But the
end-to-end measurement (1M orders, macOS physical footprint, not `ps rss`) refuted
the memory premise: the decoupled config and a plain large coupled pool both sat
at ~1.4 GB. The working set has to be resident in RAM to be served fast, whichever
scheme holds it; the decoupling reaches a large pool's speed at a large pool's RAM,
so it adds config and complexity for no gain over "make the pool big enough".

**What the investigation DID find and fix: the mmap read path was effectively
dead.** mmap was being enabled AFTER `Database.open` returned, but recovery and
tree-cache population run inside open and read the working set first, so those
reads had already copied every page into a slab window - nothing was ever served
borrowed (`borrow_serves=17` vs `pread_serves=5492` on a cold query pass). Moving
the enable INSIDE open, before any page is read (`Database.openWithMmap`), fixed it:
the same pass now borrows (`borrow_serves=5508`, `pread_serves=1`). That ordering
fix is kept. Instrumentation counters (`borrow_serves` / `pread_serves` /
`copyout_writes` / `evict_count`, logged on shutdown when mmap is on) are kept as
diagnostics. The `read_cache_frames` config knob, the demand-zero slab, and the
"Phase 5" framing were reverted.

**Honest conclusion for the read-heavy gap.** To close it, size the buffer pool to
the working set - that is the whole answer for query speed. mmap reads remain an
opt-in with ONE real, un-benchmarked-here benefit: their residency is reclaimable
clean file cache rather than private dirty pool pages, so under memory pressure or
co-tenancy the OS can drop and re-fault them instead of the pool holding RAM
hostage. That is why SQLite and LMDB map the file; it is a graceful-degradation
property, not a steady-state speed or footprint win.

### Optional Phase 6: smaller page size

**What.** Evaluate a 4 KB or 8 KB page against the current 16 KB. A random point
read of a roughly 1 KB document currently faults a full 16 KB page, wasting 15 KB
of I/O and cache residency; SQLite's default is 4 KB. A smaller page raises cache
residency for point reads at the cost of a taller tree and more per page header
overhead.

**Where.** `src/storage/page.zig` `PAGE_SIZE` and the format version. This is an on
disk format change, so it needs a new file `VERSION` and a migration or reload.

**Impact.** Lower read amplification for scattered point reads, higher residency
for the same pool bytes. Uncertain without measurement; treat as an experiment,
not a commitment.

**Risk.** Medium to high (format change, migration). Do only if Phases 1 to 4 do
not close the gap and measurement shows read amplification is the residual cost.

## Ordering and expected outcome

1. Phase 1 (cursor reuse) and Phase 2 (fewer allocations) first: contained, no
   format or pager change, together they should take a large bite out of the
   cache resident 2x to 4x, which the fair test showed is mostly per read
   overhead rather than I/O.
2. Phase 3 (cheaper hit): incremental, ships the low risk atomic relaxation first.
3. Phase 4 (mmap): behind a flag, for the miss heavy and larger than pool case.
   Now correct (enabled before startup reads so they borrow), but measurement
   shows it is not a speed or steady-state RAM win over a right-sized pool; its
   only real benefit is reclaimable cache under memory pressure.
4. Phase 5 (frame/slab decoupling): INVESTIGATED and REVERTED - it reaches a large
   pool's speed at a large pool's RAM, so it is not an optimization over sizing the
   pool to the working set. The one thing it surfaced (mmap was enabled too late to
   ever borrow) is fixed and kept.
5. Phase 6 (page size): only if measurement still shows read amplification.

Honest expectation: Phases 1 to 3 plausibly bring the read heavy queries from 2x
to 4x into roughly 1.5x to 2x of SQLite on cache resident data. Phase 4 is what
closes the larger than pool case and removes the pool ceiling entirely. Even after
all of this, SQLite's exceptional outliers (Q7 `$in` at 4 ms for 10,000 rows,
answered from a covering index) need covering indexes as well, tracked separately.
The goal of this document is page management parity, not covering indexes.

## Verification discipline (applies to every phase)

- Full unit suite green (`zig build test`), including the concurrency fuzzer under
  `NOVADB_FUZZ=1` and the crash recovery tests.
- Orders benchmark result counts unchanged at 100k and 1M before any 10M run.
- Orchestrator live store, lease, and reconcile tests green
  (`packages/nova-orchestrator/run-live-tests.sh`), since NovaDB's supported role
  is that store and it must not regress.
- Read `architecture.md` before any storage or execution change; it specifies the
  page layout, B+Tree, and concurrency invariants this plan must preserve.
