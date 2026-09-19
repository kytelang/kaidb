# Production B+Tree Database Engine Architecture

This document serves as the comprehensive architectural reference specification for the database engine. It outlines the core design systems, data structures, and concurrency protocols implemented across the storage, execution, and transaction layers.

---

## 1. Core Slotted Page Layout & Space Management

Every B+Tree node (leaf and internal), plus every overflow and undo page, is one fixed-size `PAGE_SIZE` block laid out as a *slotted page*, so variable-length records (cells) coexist with an ordered directory without moving payloads on each insert. `page.zig` owns this byte-level format; `btree.zig` drives tree structure, splitting, locking, the pool and the WAL on top of it.

A page grows from *both ends toward the middle*:
*   **Slot directory** — a densely packed array of fixed-size `CellPtr` entries, one per live cell, kept in **key order**. It begins immediately after the header (offset `@sizeOf(PageHeader)`) and grows toward higher offsets.
*   **Cell payloads** — each cell's `key` bytes followed by its `value` bytes, stored *unordered*. They begin at the top of the page (`PAGE_SIZE`) and grow toward lower offsets.

```
 0                                                          PAGE_SIZE
 +--------------+-------------------+..........+---------------------+
 |  PageHeader  |  CellPtr[0..n) ->  | free gap | <- cell payloads    |
 |  (fixed)     |  slot directory   |          |    key || value     |
 +--------------+-------------------+..........+---------------------+
 ^              ^                   ^          ^
 0     sizeOf(PageHeader)   free_space_start  free_space_end
```

The `PageHeader` (an `extern struct` at offset 0) carries the `checksum`, `page_type`, `num_cells`, the two free-gap boundaries (`free_space_start` = the byte where the directory ends and the gap begins; `free_space_end` = the offset of the lowest payload), the tree links (`parent_page_id`, the `next_page_id` leaf-sibling chain, `leftmost_child_id`), and `page_lsn` for recovery. Each `CellPtr` slot is `{ offset, key_size, value_size, flags }`. The free space is the gap `[free_space_start, free_space_end)`; a cell fits only if that gap can hold its payload **plus one more `CellPtr`**.

Because the directory is ordered but the payloads are not, inserting a cell in the middle shifts only the small fixed-size `CellPtr` entries, never the variable payload bytes.

### Deletion
Deleting a cell removes its `CellPtr` from the directory and shifts the following slots down so the directory stays densely packed and index-addressable with no holes. The cell's payload bytes are simply left as dead space; they are not reclaimed until a compaction runs.

### Zero-Heap In-Place Compaction
When the free gap cannot fit an insertion despite sufficient *cumulative* free bytes (payload space fragmented by earlier deletions), `SlottedPage.compact` reclaims it entirely within the page's own byte array, with no allocator (the routine sorts into a fixed stack array, so it adds no heap allocation or runtime fragmentation):
1.  Directory indices are sorted by their payload offset, highest first.
2.  Walking that order, each payload is slid up against the top of the page (`PAGE_SIZE`) and packed contiguously, using `copyBackwards` so a payload not yet moved is never overwritten.
3.  Each moved cell's `CellPtr.offset` is rewritten to its new location.
4.  `free_space_end` is lowered to the new lowest payload and `free_space_start` is set to `@sizeOf(PageHeader) + num_cells * @sizeOf(CellPtr)`, maximising the contiguous free gap.

---

## 2. Segmented Buffer Pool & Latch Crabbing

To achieve high concurrency and maximize throughput, the buffer pool cache and the tree's locking layer are both decentralized.

### Segmented PagePool
The `PagePool` is split into `num_instances` independent shards (16 for a normal pool, 4 for a tiny one; the pool must hold at least `MIN_POOL_SIZE = 64` frames). Each frame is owned by exactly one shard.
*   **Hashing**: a page request for `page_id` is routed to a shard by `page_id % num_instances`.
*   **Lock isolation**: each shard has its OWN `rw_lock`, `page_table` (resident-page hash map), free list and CLOCK hand. A cache hit takes the shard's `rw_lock` **shared** (many hits proceed concurrently, each just bumping the frame's pin count); only a miss/eviction takes it **exclusively**. Contention on a fetch is therefore localized to one shard. See section 4 for the full miss path and the CLOCK eviction policy.

```
                   [page_id]
                       |
              (page_id % num_instances)
                       |
        +--------------+--------------+
        |                             |
   [Shard 0]                     [Shard 1] ...
   - rw_lock                     - rw_lock
   - page_table (hash map)       - page_table (hash map)
   - free list + CLOCK hand      - free list + CLOCK hand
```

### Tree Descent Latching
Concurrent access to one `BPlusTree` is protected by two independent locks: a per-tree `structure_lock` (next subsection) and per-frame latches taken during the root-to-leaf descent. Three descent routines implement the latching, chosen by the operation:

*   **Reads — `findLeafShared`.** Point lookups and range-scan positioning crab down with **shared** latches: the child is latched before the parent is released, so the path can never be observed half-modified.
*   **Restructures — `findLeafExclusive`.** A split or merge descends with the identical crabbing but **exclusive** latches. It still releases the parent as soon as the child is latched; it does *not* hold the chain up an "unsafe" path. The guarantee that no other writer restructures the tree concurrently comes from the exclusive `structure_lock`, not from holding latches.
*   **In-place write fast path — `findLeafOptimistic`.** A non-splitting insert, non-merging delete, or in-place update takes **no latches on internal nodes at all** (betting the leaf will not need restructuring) and latches only the leaf at the end. This is safe because it runs under a shared `structure_lock`, which freezes the internal levels (see below). The leaf-reuse scan cursor uses the read-only twin `findLeafOptimisticShared`.

Every descended page is pinned in the pool for the duration of the access and unpinned exactly once on every path; `MAX_TREE_DEPTH` bounds the descent so a corrupted parent pointer fails cleanly instead of looping.

### Per-Tree Structure Lock (multi-writer concurrency)

Page latches alone are not sufficient to let several writers mutate the same tree at once, because a structure-modifying operation (SMO: a split or a merge/borrow) touches pages that are not on the single root-to-leaf path it was reached by (newly allocated siblings, re-parented children, discarded pages). To make concurrent single-tree writes safe without rewriting every SMO to latch its full working set, each `BPlusTree` carries one `structure_lock` (a reader/writer lock), and operations are classified:

*   **In-place operations (structure_lock held SHARED):** a point lookup, a range scan, an in-place update, an insert into a leaf that has room, and a delete from a leaf that will not underflow. Internal nodes are mutated only by SMOs, so while any operation holds the structure lock shared the internal levels are immutable. Reads still crab the internal nodes with shared latches (`findLeafShared`); the in-place *write* fast path exploits that immutability to descend with **no internal latch at all** (`findLeafOptimistic`), taking only the **leaf** latch (exclusive). Writers on different leaves proceed fully in parallel; writers on the same leaf serialise on that leaf's latch.
*   **Structure-modifying operations (structure_lock held EXCLUSIVE):** any split, any merge/borrow, and any insert or delete of an overflowed value (which allocates or frees pages). The exclusive lock drains all in-place holders first, so the SMO runs alone and the existing single-writer split/merge code (and the per-tree scratch arena it uses) is safe unchanged.

A write first attempts the in-place fast path under the shared lock; if the target leaf would split or underflow it releases everything, re-acquires the structure lock exclusively, and re-descends via `findLeafExclusive` (another writer may have already made room, in which case no SMO is needed). The fast path holds the shared lock plus at most one leaf latch and never couples two page latches, and the slow path holds no page latch while it waits for the exclusive lock, so the protocol is deadlock-free. DDL remains gated by the database-wide exclusive lock a level above.

### Per-Table Access Lock (executor level)

Above the storage engine, the query executor guards each user table with a per-table **group lock** (`GroupLock` in `common/sync.zig`) so that statements on one table are admitted at the right concurrency while other tables proceed independently. The group lock has two shareable modes and a solo mode; same-shareable-mode holders run together, everything else is mutually exclusive:

*   **SELECT -> read mode.** Many reads run together. Because a reader never overlaps a writer, a SELECT's tree iterators never observe a concurrent structure modification, so the read path needs no tree-level lock or iterator change.
*   **INSERT -> write mode.** Inserts are scan-free on the table tree, so many run together; the per-tree structure lock above provides the actual single-tree write safety (splits serialise within the tree, in-place inserts on different leaves parallelise).
*   **UPDATE / DELETE -> exclusive mode.** These scan the tree with an iterator to find their target rows, so they run alone on that one table (other tables are unaffected), which keeps their iterator away from any concurrent structure modification without needing to lock the iterator against the tree.

The group lock is a monitor over `Io.Mutex` + counting `Io.Semaphore`s (there is no `Io.Condition`); priority when it frees is exclusive, then writers, then readers, and the wait predicates make each class yield to queued higher-priority classes so none is starved. DDL still takes the database-wide lock exclusively, and a statement always takes the database lock shared before the table lock, so the ordering is fixed and deadlock-free. Foreign-key schemas and joins fall back to the database-wide lock, since they touch more than one table.

### Deadlock Prevention & Iterator Latch Tracking
To prevent deadlocks and latch state corruption:
*   **Iterator Single Unlock**: The `Iterator` and `RangeIterator` track the active pinned frame using `pinned_page_id`. When an iterator reaches the end of a page, it unlocks the page and resets `pinned_page_id` to `null`.
*   During deinitialization, `deinit()` performs a conditional unlock only if `pinned_page_id` is non-null, preventing double-unlocking corruption (which underflows lock reader counts and causes exclusive locks to block indefinitely).

---

## 3. Decoupled MVCC & Undo Logging

Multi-Version Concurrency Control (MVCC) is decoupled to optimize index performance and reduce write amplification in leaf pages.

### Inline Latest Version Storage
B+Tree leaf cells store *only the single latest version* of a row inline. The encoded value is a fixed 32-byte version header followed by the row image:
*   `xmin` (u64): transaction id that created this version.
*   `xmax` (u64): transaction id that deleted/superseded this version (0 if live).
*   `roll_ptr` (u64): roll pointer to the previous version's record in the undo log (0 if none).
*   `fixed_len` (u32) / `heap_len` (u32): byte lengths of the two payload regions that follow.

```
B+Tree Leaf Cell Value:
+--------------------------------------------------------------------------------------+
| xmin:u64 | xmax:u64 | roll_ptr:u64 | fixed_len:u32 | heap_len:u32 | fixed | heap      |
+--------------------------------------------------------------------------------------+
                            |
                            +--->  previous version in the undo log (if any)
```

### Undo Log Record Layout
Older versions are archived in a sequential, append-only run of undo pages managed by the `UndoLog`. Each record is `[UndoRecordHeader][fixed bytes][heap bytes]` laid out contiguously, where the header carries:
*   `xmin`: original creator transaction id.
*   `xmax`: transaction id that superseded/deleted this version.
*   `roll_ptr`: roll pointer to the next-older version in the chain (0 at the tail).
*   `fixed_len` / `heap_len`: lengths of the fixed-width and heap payload regions that follow.

A roll pointer is a `u64` locating one record: the page id in the high bits and the byte offset within that page in the low 16 bits, i.e. `(page_id << 16) | offset`.

### Historical Row Version Reconstruction
When reading a row (scans and lookups) under a transaction snapshot with id `current_tx`:
1.  The reader inspects the inline version in the B+Tree cell.
2.  If that version passes the visibility test against `current_tx` (its `xmin`/`xmax`), it is returned.
3.  If not, the reader follows `roll_ptr` into the undo log and walks the chain backward until a visible version is found or the chain terminates (`roll_ptr = 0`, or an invalid pointer; see below).

```
[B+Tree Leaf]
(Latest: xmin=10, xmax=0) --roll_ptr--> [Undo Log Page]
                                         (Prev: xmin=5, xmax=10) --roll_ptr--> [Undo Log Page]
                                                                                (Oldest: xmin=1, xmax=5, roll_ptr=0)
```

### Post-Crash Stale Roll Pointer Handling
Pre-crash roll pointers can address undo pages that the current run has not re-adopted:
*   The database validates a roll pointer with `isValidRollPtr(roll_ptr)`.
*   If the pointer's page id is not present in the active `undo_pages` list, the pointer is treated as `0`, terminating the version chain safely rather than following a dangling reference.

### Background Undo Log Purging
The background writer runs a reclamation phase on its periodic cycle:
1.  It reads the oldest active transaction id, `T_oldest`, from the transaction manager.
2.  It scans the undo pages from the front (oldest first), stopping at the active page.
3.  A page is reclaimable only when the maximum `xmin`/`xmax` of every record on it is below `T_oldest`; such a page is discarded with `PagePool.discardPage(pid)` and dropped from the `undo_pages` list.

The purge is prefix-only: it frees the contiguous leading run of reclaimable undo pages and stops at the first page a still-open transaction needs. A long-running reader therefore holds the reclamation watermark back (head-of-line), and reclamation resumes once that transaction ends. This is a property of the append-log design, not a leak.

---

## 4. Pager and the Cache-Miss Path

The storage engine is disk-based, not in-memory. Every page lives on disk and is brought into a bounded buffer pool on demand. How cheaply the pager services a miss is the single biggest factor in performance once the working set exceeds the pool, so this section documents the miss path in detail. It is the baseline against which future I/O work is measured.

### Page size and the pool

*   **Page size:** 16 KiB (`PAGE_SIZE`), four times SQLite's 4 KiB default and twice PostgreSQL's 8 KiB. A larger page amortises per-page overhead over more rows and shortens the tree (fewer internal levels, so fewer page touches per descent), at the cost of more read amplification for a tiny point lookup.
*   **Auto-sized pool:** the buffer pool defaults to roughly 50% of system RAM (`pool_size = 0` means auto, resolved from `hw.memsize` at startup, with a headroom reserve and a cap). It is sharded into `num_instances` independent instances (16 for a pool of 64 frames or more, otherwise 4), each owning the pages where `page_id % num_instances == instance_ordinal`, each with its own page table, free list, CLOCK hand and `rw_lock`. Contention on a fetch is localised to one shard.

### Eviction: CLOCK / second-chance

Each shard replaces pages with the CLOCK algorithm, an O(1) approximation of LRU that needs no per-access list surgery. A `clock_hand` sweeps the frames of the shard: a referenced-but-unpinned frame is given a second chance (its `is_referenced` bit is cleared and the hand moves on); the first unpinned frame whose bit is already clear is the victim. Pinned frames (`pin_count > 0`) are never evicted. A hit sets the reference bit; that is the entire per-access cost.

This is the same family as PostgreSQL's clock-sweep and, like it, avoids the global-lock list churn that makes strict LRU scale badly under concurrency. It gives one-shot scan pages a single second chance before reclaim, so a scan does not immediately evict the hot interior nodes, but true scan resistance for a table larger than the pool would need a dedicated bulk-scan ring (see section 7, not yet implemented).

### The miss path, step by step

1.  `PagePool.fetchPage(page_id)` hashes to a shard and takes the shard `rw_lock` **shared**. A hit returns the pinned frame immediately: a hash probe plus a refcount, no page-sized allocation, no copy.
2.  On a miss the shard lock is released and re-taken **exclusively**, and the page table is re-checked (a concurrent fetch may have installed the page in the gap). Otherwise `findVictimFrame` returns a free frame or a CLOCK victim. Frame buffers are pre-allocated in a single slab, so reusing a victim is an in-place overwrite with no `malloc`/`free` (matching SQLite's buffer-recycle technique).
3.  If the victim is dirty it is written back first, honouring the WAL-before-page rule (`walBeforePageLsn`) so the log record that describes the page is durable before the page itself is overwritten on disk.
4.  The page is read with a single positioned read: `pager.readPage` issues one `readPositionalAll` (`pread`) at offset `page_id * PAGE_SIZE`. One syscall per page, no `lseek`.

### mmap read path (optional)

When enabled (server config, or `NOVADB_MMAP=1`), the pager memory-maps the file `MAP_SHARED` and a clean read borrows a pointer straight into the mapping with no `pread` and no `memcpy`, exactly like SQLite's mmap mode. `MAP_SHARED` keeps the map coherent with the pager's `pwrite`s. It is a pure optimisation: any page not covered by the mapping (or the header page, page 0), or any write, falls back to the `pread` path; when the file has grown past the mapping the map is grown once and retried. It is opt-in rather than default because the map must be grown carefully around outstanding borrows.

### Torn-write protection: doublewrite buffer

A crash mid-write can leave a page half-updated ("torn"). The flush paths use a doublewrite buffer: the batch of dirty pages is first written to a fixed staging area (a header page plus copies) and `fsync`ed, then written in place. On recovery, any page whose checksum fails is restored from its doublewrite copy. This is the same mechanism InnoDB uses, and it is what lets a `kill -9` mid-write recover cleanly.

### Prefetch and descent amortisation (the clustered-lookup optimisers)

A secondary-index scan yields primary keys, and each PK must be resolved to its row by descending the clustered base tree. Two mechanisms keep that from being one full root-to-leaf descent plus one blocking read per row:

*   **Adaptive base-leaf prefetch** (`iterator.zig`): the index scan gathers a look-ahead batch of matching PKs (`DEFAULT_PREFETCH_BATCH = 256`, grown to an MRR window of up to `262144` when the scan order is free, so an unbounded aggregate sorts a large window and a tight `LIMIT` keeps the small default), resolves them to their base-table leaf page IDs, and issues OS readahead (`pager.prefetchPages`, using Linux `readahead()` / the BSD `F_RDADVISE` equivalent) so those base pages are being fetched by the kernel before the scan consumes them. It is adaptive on two axes. First, the explicit per-PK prefetch descent runs only for the *scattered* (range-ordered) path: when a batch is sorted into clustered primary-key order (`may_reorder`), the base fetches walk the tree in ascending key order and the kernel's own sequential read-ahead already stages those pages, so the explicit descent is skipped as pure overhead. Second, on the range-ordered path it is gated on the cumulative pool hit ratio, so it runs only while the base pages are actually missing (a cold or disk-bound scan) and a warm scan pays nothing for the prefetch machinery.
*   **Base-leaf cursor reuse** (`LeafReuseSearcher`): the scan keeps a base-table leaf cursor across fetches, so consecutive PKs that land on the same base leaf are answered without re-descending the tree. For a range of nearby PKs this collapses the per-row descent to a single leaf walk.

Both are real, shipped optimisations. What is not yet present is genuinely asynchronous, overlapped I/O (the reads after the readahead hint are still synchronous `pread`s) and vectored `preadv` that combines physically adjacent pages into one syscall. Those are the frontier items in section 7.

---

## 5. Durability: WAL, Checkpointing, Recovery, Free List

*   **Write-ahead log.** Every page mutation is preceded by its WAL record (`walBeforePageLsn` on the flush/eviction paths, `walBeforePage` up front on whole-pool flushes). WAL segments are `fsync`ed; an atomic `CHECKPOINT` marker (temp-file plus rename) records the replay boundary. `synchronous_commit` is configurable to trade per-commit `fsync` latency for durability.
*   **Recovery.** On open, torn pages are first repaired from the doublewrite buffer, then a three-phase redo replays the WAL: catalog, user DML plus index rebuild, then repair. A torn tail WAL record (for example a write interrupted by a full disk) is detected by its length/checksum and the replay stops there, discarding only the incomplete tail. Committed-transaction visibility is persisted to a `COMMITTED` sidecar at each checkpoint so that truncating old WAL segments does not lose the knowledge of which transactions had committed.
*   **Checkpointing.** A background writer flushes dirty pages, truncates the WAL to bound its size, persists the committed-transaction set, and rewrites the page-0 header (master-tree root, LSN, and the pager free-list head). Running this periodically (rather than only at clean shutdown) bounds the crash window for freed-page reclamation: pages freed by B+Tree merges, deletes and `DROP` become durable at the next checkpoint. The recovery-side free-list walk is checksum-guarded, so a torn or stale free-list chain degrades to a benign space leak, never a handed-out live page.
*   **Backup and PITR.** `exportSnapshot` produces a consistent physical snapshot (rewrite header from the live master tree, flush, `fsync`, copy every page, copy the WAL) usable as a cold (`novadb backup`) or hot (`BACKUP DATABASE TO`) backup. Point-in-time recovery replays a base snapshot forward over archived WAL segments to a target LSN (`restore --archive --target-lsn`).

---

## 6. Clustered Storage Model and the Index-to-Row Cost

kaidb is **index-organised (clustered)**: a user table *is* its primary-key B+Tree, and the full row lives in the leaf. This has a direct consequence for the cost of a lookup measured in page reads.

*   **Primary-key point or range lookup: the row is in the leaf you already reached.** There is no separate "fetch the row" step. A heap-organised engine like PostgreSQL, by contrast, descends its index to a tuple id and then does a *second, unrelated* random read of the heap page. So for PK-driven access the clustered design does strictly less I/O.
*   **Secondary-index lookup: this is where the clustered design pays.** A secondary index stores `(indexed-value, primary-key)`; resolving each match to its row requires descending the base PK tree (`fetchVisibleFilteredJson`). For scattered (non-correlated) matches, each is a fresh root-to-leaf descent, i.e. O(tree height) page touches. The prefetch and leaf-cursor reuse in section 4 amortise the *sequential/correlated* case; the *random* case is the open lever (section 7).
*   **Key encoding.** Secondary-index keys are encoded as order-preserving, delimiter-safe fixed tokens (`encodeIndexValueAlloc` in `src/schema/types.zig`), so index range scans are true ordered seeks. The clustered base primary key, however, is currently stored as decimal **text**, so its physical order is lexical, not numeric. This is why the fast clustered-PK range path (`pkClusteredWindow`) is gated to same-digit-width, non-negative bounds, where lexical order equals numeric order. A fixed-width big-endian integer PK encoding would make clustered range scans general; it is an on-disk format change and is deliberately deferred.
*   **Page density.** The denser the pages -- fewer bytes per row, so more rows per leaf -- the more of the dataset fits the pool and the shorter the tree, so a given query takes fewer misses. Density is therefore a first-order lever, and the storage-format choices (compact cell layout, order-preserving keys) target it directly.

### Recommended practice: cover wide secondary-index reads with a composite index

For a query that filters on a secondary-index column but returns or aggregates a *different* column, the clustered design must descend the base tree once per matched row (the double-lookup above). **The fix is the one a DBA reaches for on any clustered engine (InnoDB and SQLite included): a covering composite index** that carries every column the query needs, so it is answered index-only with no base-row descent at all.

```sql
-- Instead of relying on a single-column index that forces the double-lookup:
CREATE INDEX ix_td ON ord (total_due);

-- Add the aggregated / projected column to the index so the query is covered:
CREATE INDEX ix_td_cust ON ord (total_due, customer_id);
```

With the covering index the read is answered index-only: kaidb reads the aggregated or projected value straight from the composite key and never touches the clustered base tree. The planner selects this path automatically -- the index-only fast paths (`tryIndexOnlyCount`, `tryIndexMinMax`, `tryIndexGroupAgg`, `tryIndexOnlyScalarAgg`, `tryIndexOnlyScalarAggComposite`) are tried before the general scan -- so it covers scalar aggregates (`avg`/`sum`/`min`/`max`/`count` over a trailing column filtered on the lead), `GROUP BY` on an indexed column, and projections of the covered columns, with no hint required.

Rules of thumb:

*   Put the **filter column first** and the **selected/aggregated columns after it** in the composite index. The order-preserving key encoding then serves the range on the lead and carries the trailing values for free.
*   A covering index costs extra write amplification and disk, so add it for the read patterns that matter, not universally.
*   Without a covering index, kaidb still runs correctly, and when a range matches a large fraction of the table the cost-based planner switches from the index to a single sequential clustered scan: `estimateRangeFraction` estimates the matched fraction from the index's value span and drops the index above roughly 30%. The covering index is the way to turn such a read from "scan the table" into "read only the index".

---

## 7. Cache-Miss Handling: kaidb vs SQLite vs PostgreSQL

Cache misses are normal for every disk database; none can hold an arbitrary dataset in RAM. What separates a fast disk engine from a slow one is how cheaply the pager services a miss. This section is the honest, code-grounded comparison of kaidb's miss path against two proven engines, and it defines the roadmap.

### Where kaidb already stands

Against **SQLite** (`src/pager.c`, `src/pcache1.c`), kaidb matches or exceeds the miss-path techniques:

| Technique | SQLite | kaidb |
|---|---|---|
| Alloc-free miss (recycle victim buffer in place) | yes (slab) | yes (pre-allocated slab) |
| One positioned `pread` per page | yes | yes |
| Never evict a dirty page without logging/writing first | yes (stress spill) | yes (WAL-before-page) |
| Keep interior nodes hot to amortise the index-to-row descent | yes (LRU keeps them MRU) | yes (CLOCK) |
| Zero-copy mmap read (read-only, with `pread` fallback) | yes (opt-in) | yes (opt-in) |
| Covering indexes to skip the second descent | yes | yes |
| Application-level prefetch of the base rows | **no** (relies on OS readahead) | **yes** (adaptive batch prefetch) |
| Base-leaf cursor reuse across a batch | via cursor stack | yes (`LeafReuseSearcher`) |

SQLite deliberately does no application prefetch and relies on the OS plus order-preserving page allocation. kaidb does more than SQLite here, not less.

### The frontier: what modern PostgreSQL does that kaidb does not yet

PostgreSQL's advantage past `shared_buffers` is not clock-sweep or `posix_fadvise` (kaidb has equivalents). It is four newer things, in impact order, and they are the roadmap to making kaidb fully general-purpose at scale:

1.  **Streaming reads with true asynchronous, overlapped I/O** (`read_stream.c` plus the AIO / `io_uring` backend). PostgreSQL keeps a ring of pinned buffers and a parallel ring of in-flight I/Os, prefetches ahead, and only blocks when the consumer actually reaches a page, with look-ahead distance that grows on real misses and decays on hits. kaidb prefetches via fire-and-forget OS `readahead()` and then reads synchronously (`pread` per page). **kaidb already has an async reactor (kqueue / epoll / io_uring); routing the base-row and scan fetches through genuinely overlapped async reads is the single highest-impact lever, and the infrastructure exists.**
2.  **I/O combining (vectored `preadv`).** PostgreSQL merges physically-contiguous page reads into one `preadv` of up to `io_combine_limit` pages, collapsing per-page syscall overhead on cold scans. kaidb issues one `pread` per page.
3.  **Bulk-scan ring buffer** (`BufferAccessStrategy` / `BAS_BULKREAD`, 256 KiB, capped at 1/8 of the pool). A scan of a table larger than the pool is confined to a tiny fixed set of frames so it cannot evict the hot working set. kaidb relies on CLOCK alone, so a large cold scan can still churn the pool.
4.  **Random-probe remedy: block-sorted / bitmap fetch.** For scattered secondary-index matches, PostgreSQL collects the target tuple ids, sorts them by physical block, dedups, and walks the heap in physical order so the reads pipeline. kaidb already does the first half: when a scan's order is free (`may_reorder`) it sorts each primary-key look-ahead batch into clustered order and walks the base tree in that physical order (`IndexScanIterator`, the sorted-batch "bitmap" walk). What remains to complete the pattern is the vectored / overlapped-async read of lever 1 (and PK dedup). The alternative structural fix is a **physical row locator**: store a validated base-leaf page hint in each secondary-index entry so a random probe is a one-page fetch (like a PostgreSQL heap tuple id), instead of a full descent.

The one asymmetry no engine removes: sequential and batched-random misses pipeline; a single pointer-chasing descent (each index level's address depends on reading the previous level) cannot be prefetched and serialises. The goal is to make the sequential and batched cases fast, which is exactly what levers 1 to 4 do.

### Where the base-fetch cost goes

For the wide secondary-index read (filter on an indexed column, return or aggregate a different one), the work decomposes into four phases; the proportions are the architectural point:

| Phase | Relative cost | What it is |
|---|---|---|
| base seek | dominant (~¾) | the clustered base-tree lookup per matched PK: descent + leaf binary search |
| index walk | secondary (~¼) | the secondary-index range walk, batch refill and PK sort |
| materialisation | negligible | decoding the row into typed cells |
| projection | small | encoding the output for the wire |

The structural conclusion is that **the base seek is the whole gap, and row materialisation is not a factor.** The `Cell` union carries numerics inline (no per-row `itoa`/`dtoa`), so there is no "per-row JSON" cost to remove; borrowing the column-name array and the inline row image rather than copying them per row lowers allocator pressure but cannot move a query whose time is elsewhere.

Crucially the base-seek cost is the **leaf work** — fetching the 16 KiB base leaf and binary-searching it — not the framing of the descent. The descent crabs through internal nodes that stay hot in the pool, so it is nearly free. That is why the obvious descent-avoidance ideas do not help a scattered scan:

*   A **sibling-hop cursor** (follow `next_page_id` instead of re-descending on a leaf change) touches a full leaf page per hop; with scattered matches (roughly one per leaf) the target is a leaf or two away, so hopping reads *more* leaves than a cached-internal descent, not fewer.
*   An **adaptive hash index** (`key -> leaf` cache) still has to fetch and search the same leaf on a hit, so its probe cost is pure overhead here; it pays off only for *repeated hot-key point lookups* (Zipfian OLTP), not a scan of many distinct keys.

Because the leaf fetch is irreducible by descent tricks, the levers that actually close the gap are structural and match levers 1 to 4 above plus the key encoding:

*   **(a)** overlapped async base-row reads through the reactor, for the cold / miss case;
*   **(b)** a fixed-width integer primary key, so `findChildPageId` and `findCellByKey` compare machine words instead of decimal **text** across 16 KiB pages (also the correctness fix for general clustered range scans, and the binary search's cache behaviour is the dominant warm cost);
*   **(c)** denser leaves, so a given range spans fewer pages.

The one cheap win the cost model does allow, and which is implemented, is the sorted-batch case: when a look-ahead batch is already in clustered order the base fetches are sequential, so the kernel's own read-ahead covers them and the explicit per-PK prefetch descent is skipped as redundant (section 4).
