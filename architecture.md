# Production B+Tree Database Engine Architecture

This document serves as the comprehensive architectural reference specification for the database engine. It outlines the core design systems, data structures, and concurrency protocols implemented across the storage, execution, and transaction layers.

---

## 1. Core Slotted Page Layout & Space Management

Leaf and internal database pages utilize a slotted-page architecture to store variable-length records (cells) dynamically. Space within a page is managed using a two-way growth model:
*   **Slot Directory**: Grows *downward* from the page header end.
*   **Cell Payloads**: Grow *upward* from the page bottom.

```
+-------------------------------------------------------------+
| PageHeader (24 bytes)                                       |
+-------------------------------------------------------------+
| Slot 0 (Offset, Size) | Slot 1 | Slot 2 ...                 |
| ---------> Grows Downwards                                  |
+-------------------------------------------------------------+
|                      <--- Free Space --->                   |
+-------------------------------------------------------------+
|                                 ... | Cell 2 | Cell 1 | Cell 0 |
|                                       Grows Upwards <------ |
+-------------------------------------------------------------+
```

### Contiguous Slot Pointer Shifting
On record deletion:
1.  The slot entry corresponding to the target record is removed from the directory.
2.  All subsequent slots are shifted left by 1 entry size (4 bytes) to ensure that the slot directory remains strictly contiguous and indexable without holes.
3.  The cell payload space is marked as fragmented free space.

### Zero-Heap In-Place Compaction
When free space is fragmented and cannot fit a new insertion despite having sufficient cumulative free bytes, the page performs an in-place compaction:
*   **Zero Heap Allocation**: Compaction operates purely within the page's raw memory byte array to prevent allocator overhead or runtime fragmentation.
*   **Compaction Algorithm**:
    1.  A temporary array of slot indices sorted by their payload offsets is constructed.
    2.  Starting from the bottom of the page (`PAGE_SIZE`), payloads are copied/shifted downwards, packing them contiguously.
    3.  Offsets in the slot directory are updated to reflect the new contiguous locations.
    4.  `free_space_start` (denoting the start of the payload space) is adjusted upwards to maximize contiguous free bytes.

---

## 2. Segmented Buffer Pool & Latch Crabbing

To achieve high concurrency and maximize throughput, the buffer pool cache and locking layers are fully decentralized.

### Segmented PagePool
The `PagePool` is segmented into $N$ independent instances (`num_instances`, configured from the pool size; the pool holds at least `MIN_POOL_SIZE = 64` frames).
*   **Hashing**: A page request for `page_id` is mapped to an instance index via:
    $$\text{instance\_idx} = \text{page\_id} \pmod N$$
*   **Lock Isolation**: Each instance has its own `rw_lock`, hash table (`page_table`), free list and CLOCK hand. A cache hit takes the lock **shared** (readers concurrent); only a miss/eviction takes it exclusively. Thread contention is localized to $1/N$ of the database page space. See section 4 for the full miss path and the CLOCK eviction policy.

```
                   [page_id]
                       |
                 (page_id % N)
                       |
        +--------------+--------------+
        |                             |
  [Instance 0]                  [Instance 1] ...
  - local mutex                 - local mutex
  - page_table (hash map)       - page_table (hash map)
  - local free_list             - local free_list
```

### Page-Level Latch Crabbing Protocol
B+Tree traversals use the latch crabbing protocol to ensure safe, deadlock-free concurrent tree mutations:
1.  **Read Operations (Scans/Lookups)**:
    *   Acquire a **Shared Lock** on the root page.
    *   Pin the child page and acquire a **Shared Lock** on it.
    *   Release the lock on the parent page ("crab" down).
2.  **Write Operations (Inserts/Updates/Deletes)**:
    *   Acquire an **Exclusive Lock** on the parent page.
    *   Pin and acquire an **Exclusive Lock** on the child page.
    *   If the child page is "safe" (will not split or merge/underflow), release the lock on the parent page. Otherwise, hold the lock chain until split/merge operations are completed.

### Per-Tree Structure Lock (multi-writer concurrency)

Page latches alone are not sufficient to let several writers mutate the same tree at once, because a structure-modifying operation (SMO: a split or a merge/borrow) touches pages that are not on the single root-to-leaf path it was reached by (newly allocated siblings, re-parented children, discarded pages). To make concurrent single-tree writes safe without rewriting every SMO to latch its full working set, each `BPlusTree` carries one `structure_lock` (a reader/writer lock), and operations are classified:

*   **In-place operations (structure_lock held SHARED):** a lookup, a range scan, an in-place update, an insert into a leaf that has room, and a delete from a leaf that will not underflow. Internal nodes are mutated only by SMOs, so while any writer holds the structure lock shared the internal levels are immutable. The descent therefore reads internal nodes with only a pin (no page latch) and takes a single **leaf** latch (exclusive for a write, shared for a read). Writers on different leaves proceed fully in parallel; writers on the same leaf serialise on that leaf's latch.
*   **Structure-modifying operations (structure_lock held EXCLUSIVE):** any split, any merge/borrow, and any insert or delete of an overflowed value (which allocates or frees pages). The exclusive lock drains all in-place holders first, so the SMO runs alone and the existing single-writer split/merge code (and the per-tree scratch arena it uses) is safe unchanged.

A write first attempts the in-place fast path under the shared lock; if the target leaf would split or underflow it releases everything, re-acquires the structure lock exclusively, and re-descends (another writer may have already made room, in which case no SMO is needed). The fast path holds the shared lock plus at most one leaf latch and never couples two page latches, and the slow path holds no page latch while it waits for the exclusive lock, so the protocol is deadlock-free. DDL remains gated by the database-wide exclusive lock a level above.

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
B+Tree leaf cells store *only the single latest version* of a row inline. This latest version includes:
*   `xmin`: Transaction ID that created this version.
*   `xmax`: Transaction ID that deleted/modified this version (0 if active).
*   `roll_ptr`: A 64-bit roll pointer pointing to the previous version's entry in the Undo Log.

```
B+Tree Leaf Cell Value:
+-------------------------------------------------------+
| xmin (u64) | xmax (u64) | roll_ptr (u64) | fixed/heap |
+-------------------------------------------------------+
      |
      +---> Points to previous version in Undo Log (if any)
```

### Undo Log Record Layout
Older versions are archived in a sequential, append-only segment of undo pages managed by the `UndoLog`. Each Undo Record contains:
*   `xmin`: Original creator transaction ID.
*   `xmax`: Transaction ID that superseded/deleted this version.
*   `roll_ptr`: Roll pointer pointing to the next previous version in the chain (0 if tail).
*   `fixed_len`/`heap_len` + raw payload bytes.

A roll pointer is structured as:
$$\text{roll\_ptr} = (\text{page\_id} \ll 16) \mid \text{offset}$$

### Historical Row Version Reconstruction
When reading a row (scans and lookups) under a transaction snapshot with ID `current_tx`:
1.  The reader inspects the inline version in the B+Tree cell.
2.  If the inline version is visible (`isVisible(current_tx, xmin, xmax)`), it is returned.
3.  If not visible, the reader follows `roll_ptr` to the Undo Log and traverses the historical chain backward until a visible version is found or the chain terminates (`roll_ptr = 0`).

```
[B+Tree Leaf]
(Latest: xmin=10, xmax=0) --roll_ptr--> [Undo Log Page]
                                         (Prev: xmin=5, xmax=10) --roll_ptr--> [Undo Log Page]
                                                                                (Oldest: xmin=1, xmax=5, roll_ptr=0)
```

### Post-Crash Stale Roll Pointer Handling
Since the Undo Log is initialized fresh on boot, pre-crash roll pointers are stale:
*   The database verifies the validity of a roll pointer using `isValidRollPtr(roll_ptr)`.
*   If the page ID of the roll pointer is not present in the current active `undo_pages` list, the roll pointer is treated as `0` (terminating the version chain safely).

### Background Undo Log Purging
The background writer task runs a garbage-collection phase periodically:
1.  It retrieves the oldest active transaction ID ($T_{\text{oldest}}$) from the transaction manager.
2.  It inspects sequential undo pages starting from the oldest.
3.  If the maximum transaction ID ($xmin$/$xmax$) of all records on an undo page is less than $T_{\text{oldest}}$, the page is safely discarded using `PagePool.discardPage(pid)` and removed from the active undo log pages list.

The purge is prefix-only: it frees the contiguous run of oldest reclaimable undo pages and stops at the first page a still-open transaction needs. A long-running reader therefore holds the reclamation watermark back (head-of-line), and reclamation resumes once that transaction ends. This is a property of the append-log design, not a leak.

---

## 4. Pager and the Cache-Miss Path

The storage engine is disk-based, not in-memory. Every page lives on disk and is brought into a bounded buffer pool on demand. How cheaply the pager services a miss is the single biggest factor in performance once the working set exceeds the pool, so this section documents the miss path in detail. It is the baseline against which future I/O work is measured.

### Page size and the pool

*   **Page size:** 16 KiB (`PAGE_SIZE`), four times SQLite's 4 KiB default and twice PostgreSQL's 8 KiB. A larger page amortises per-page overhead over more rows and shortens the tree (fewer internal levels, so fewer page touches per descent), at the cost of more read amplification for a tiny point lookup.
*   **Auto-sized pool:** the buffer pool defaults to roughly 50% of system RAM (`pool_size = 0` means auto). It is sharded into `num_instances` independent instances, each owning the pages where `page_id % num_instances == instance_ordinal`, each with its own page table, free list, CLOCK hand and `rw_lock`. Contention on a fetch is localised to one shard.

### Eviction: CLOCK / second-chance

Each shard replaces pages with the CLOCK algorithm, an O(1) approximation of LRU that needs no per-access list surgery. A `clock_hand` sweeps the frames of the shard: a referenced-but-unpinned frame is given a second chance (its `is_referenced` bit is cleared and the hand moves on); the first unpinned frame whose bit is already clear is the victim. Pinned frames (`pin_count > 0`) are never evicted. A hit sets the reference bit; that is the entire per-access cost.

This is the same family as PostgreSQL's clock-sweep and, like it, avoids the global-lock list churn that makes strict LRU scale badly under concurrency. It gives one-shot scan pages a single second chance before reclaim, so a scan does not immediately evict the hot interior nodes, but true scan resistance for a table larger than the pool would need a dedicated bulk-scan ring (see section 7, not yet implemented).

### The miss path, step by step

1.  `PagePool.fetchPage(page_id)` hashes to a shard and takes the shard `rw_lock` **shared**. A hit returns the pinned frame immediately: a hash probe plus a refcount, no page-sized allocation, no copy.
2.  On a miss the shard lock is upgraded to exclusive and `findVictimFrame` returns a free frame or a CLOCK victim. Frame buffers are pre-allocated in a single slab, so reusing a victim is an in-place overwrite with no `malloc`/`free` (matching SQLite's buffer-recycle technique).
3.  If the victim is dirty it is written back first, honouring the WAL-before-page rule (`walBeforePageLsn`) so the log record that describes the page is durable before the page itself is overwritten on disk.
4.  The page is read with a single positioned read: `pager.readPage` issues one `readPositionalAll` (`pread`) at offset `page_id * PAGE_SIZE`. One syscall per page, no `lseek`.

### mmap read path (optional)

When enabled (server config, or `NOVADB_MMAP=1`), the pager memory-maps the file `MAP_SHARED` and a clean read borrows a pointer straight into the mapping with no `pread` and no `memcpy`, exactly like SQLite's mmap mode. `MAP_SHARED` keeps the map coherent with the pager's `pwrite`s. It is a pure optimisation: any page not covered by the mapping, or any write, falls back to the `pread` path. It is opt-in rather than default because the map must be grown carefully around outstanding borrows.

### Torn-write protection: doublewrite buffer

A crash mid-write can leave a page half-updated ("torn"). The flush paths use a doublewrite buffer: the batch of dirty pages is first written to a fixed staging area (a header page plus copies) and `fsync`ed, then written in place. On recovery, any page whose checksum fails is restored from its doublewrite copy. This is the same mechanism InnoDB uses, and it is why a `kill -9` mid-write recovers cleanly (verified by the crash-test harness).

### Prefetch and descent amortisation (the clustered-lookup optimisers)

A secondary-index scan yields primary keys, and each PK must be resolved to its row by descending the clustered base tree. Two mechanisms keep that from being one full root-to-leaf descent plus one blocking read per row:

*   **Adaptive base-leaf prefetch** (`iterator.zig`): the index scan gathers a look-ahead batch of matching PKs (`DEFAULT_PREFETCH_BATCH = 256`, grown to an MRR window of up to `262144` when the scan order is free, so an unbounded aggregate sorts a large window and a tight `LIMIT` keeps the small default), resolves them to their base-table leaf page IDs, and issues OS readahead (`pager.prefetchPages`, using Linux `readahead()` / the BSD `F_RDADVISE` equivalent) so those base pages are being fetched by the kernel before the scan consumes them. It is adaptive on two axes. First, the explicit per-PK prefetch descent runs only for the *scattered* (range-ordered) path: when a batch is sorted into clustered primary-key order (`may_reorder`), the base fetches walk the tree in ascending key order and the kernel's own sequential read-ahead already stages those pages, so the explicit descent is skipped as pure overhead (measured ~1.8x on the warm wide aggregate). Second, on the range-ordered path it is gated on the cumulative pool hit ratio, so it runs only while the base pages are actually missing (a cold or disk-bound scan) and a warm scan pays nothing for the prefetch machinery.
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

kaidb is **index-organised (clustered)**: a user table *is* its primary-key B+Tree, and the full row lives in the leaf. This has a direct, and mostly favourable, consequence for the cost of a lookup measured in page reads.

*   **Primary-key point or range lookup: the row is in the leaf you already reached.** There is no separate "fetch the row" step. A heap-organised engine like PostgreSQL, by contrast, descends its index to a tuple id and then does a *second, unrelated* random read of the heap page. So for PK-driven access the clustered design does strictly less I/O. This is a structural advantage, and it is visible in the benchmark: kaidb wins or ties the PK, count, aggregate and DISTINCT queries.
*   **Secondary-index lookup: this is where the clustered design pays.** A secondary index stores `(indexed-value, primary-key)`; resolving each match to its row requires descending the base PK tree. For scattered (non-correlated) matches, each is a fresh root-to-leaf descent, i.e. O(tree height) page touches, and this is the amplification that shows up on the medium-selectivity range scans PostgreSQL wins. The prefetch and leaf-cursor reuse in section 4 amortise the *sequential/correlated* case; the *random* case is the open lever (section 7).
*   **Key encoding.** Secondary-index keys are encoded as order-preserving, delimiter-safe fixed tokens (`encodeIndexValueAlloc` / `index_key.zig`), so index range scans are true ordered seeks. The clustered base primary key, however, is currently stored as decimal **text**, so its physical order is lexical, not numeric. This is why the fast clustered-PK range path is gated to same-width, non-negative bounds (where lexical order equals numeric order). A fixed-width big-endian integer PK encoding would make clustered range scans general; it is an on-disk format change and is deliberately deferred.
*   **Page density.** A 2026-09 footprint pass took a 1M-row load from about 15 GB to about 993 MB on disk. Density matters directly here: the denser the pages, the more of the dataset fits the pool and the fewer misses a given query takes.

### Recommended practice: cover wide secondary-index reads with a composite index

For a query that filters on a secondary-index column but returns or aggregates *another* column, the clustered design must descend the base tree once per matched row (the double-lookup above). On the RAM-pressure benchmark, `SELECT count(*), avg(customer_id) FROM ord WHERE total_due BETWEEN a AND b` (about 40% of the table) runs at ~365 ms as a scan and ~800 ms via the plain `total_due` index, versus ~150 ms on InnoDB. **The fix is the same one a DBA reaches for on any clustered engine (InnoDB and SQLite included): a covering composite index** that carries every column the query needs, so it is answered index-only with no base-row descent at all.

```sql
-- Instead of relying on a single-column index that forces the double-lookup:
CREATE INDEX ix_td ON ord (total_due);

-- Add the aggregated / projected column to the index so the query is covered:
CREATE INDEX ix_td_cust ON ord (total_due, customer_id);
```

With the covering index the same query runs **index-only at ~11 ms** (ahead of PostgreSQL and InnoDB on this shape), because kaidb reads the aggregated value straight from the index key and never touches the clustered base tree. This applies to scalar aggregates (`avg`/`sum`/`min`/`max`/`count` over a trailing column filtered on the lead), `GROUP BY` on an indexed column, and projections of the covered columns. The planner selects the covering path automatically when a suitable composite index exists; no hint is needed.

Rules of thumb:

*   Put the **filter column first** and the **selected/aggregated columns after it** in the composite index. The order-preserving key encoding then serves the range on the lead and carries the trailing values for free.
*   This is the standard trade: a covering index costs extra write amplification and disk, so add it for the read patterns that matter, not universally.
*   Without a covering index, kaidb still runs correctly and, for high-selectivity ranges, its cost-based planner already switches from the index to a single sequential clustered scan (the cheaper plan). The covering index is the way to turn such a read from "scan the table" into "read only the index".

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
4.  **Random-probe remedy: block-sorted / bitmap fetch.** For scattered secondary-index matches, PostgreSQL collects the target tuple ids, sorts them by physical block, dedups, and walks the heap in physical order so the reads pipeline. kaidb already gathers PKs and prefetches their base leaves; adding a block-sort plus dedup plus the vectored/async read above completes the same pattern. The alternative structural fix is a **physical row locator**: store a validated base-leaf page hint in each secondary-index entry so a random probe is a one-page fetch (like a PostgreSQL heap tuple id), instead of a full descent.

The one asymmetry no engine removes: sequential and batched-random misses pipeline; a single pointer-chasing descent (each index level's address depends on reading the previous level) cannot be prefetched and serialises. The goal is to make the sequential and batched cases fast, which is exactly what levers 1 to 4 do.

### Measured breakdown and ruled-out levers (2026-09-18)

A RAM-pressure profiling pass (2 GB VM, 128 MB pool, OS cache dropped between cold runs, `NOVADB_QPROF=1`) put concrete numbers on the wide secondary-index aggregate `avg(customer_id) WHERE total_due BETWEEN ...` over 1M rows (400,069 matched, about 40% of the table). The warm per-query phase split is:

| Phase | Warm time | Share | What it is |
|---|---|---|---|
| `baseSeek` | ~530 ms | 73% | the clustered base-tree lookup per matched PK (descent + leaf binary search) |
| `idxwalk + other` | ~170 ms | 24% | the secondary-index range walk, batch refill and PK sort |
| `buildJson` | ~20 ms | ~3% | row materialisation into typed cells |
| `project` | ~60 ms | | text-encode the output for the wire |

The headline finding: **`baseSeek` is the whole gap, and materialisation is not a factor.** The `Cell` union already carries numerics inline (no per-row `itoa`/`dtoa`), so what was historically "per-row JSON" is ~3% and not worth optimising. Two landed changes are correct and reduce allocator pressure but, as the profile predicts, do not move the query time: borrowing the projection column-name array instead of copying it per row, and borrowing the inline row image straight from the pinned leaf instead of a per-row `dupe`+`free`.

Within `baseSeek` itself, three descent- or copy-avoidance strategies were implemented, measured, and **reverted as net-negative or no-ops**, because the cost is the leaf work, not the descent framing:

*   **Sibling-hop cursor** (follow `next_page_id` instead of re-descending when a key leaves the current leaf): *regressed* 533 → 678 ms. A root descent crabs through internal nodes that are already hot/cached (≈ one leaf read of real cost); a hop touches a full leaf page each, and with scattered matches (≈ 1 per leaf) the target is usually 1–2 leaves away, so hopping reads *more* leaf pages than a descent.
*   **Adaptive hash index** (`key → leaf page id` cache, InnoDB-style, epoch/clear-invalidated, verify-on-probe): *regressed* warm 693 → 1038 ms and cold-populate to 3166 ms. The cache probe (a hash lookup over a large map with poor locality, plus its lock and a verify-fetch) costs as much as the cheap cached-internal descent it removes, while still doing the same leaf fetch. An AHI helps *repeated hot-key point lookups* (Zipfian OLTP), not a scan of hundreds of thousands of distinct keys — which is what this query is.
*   **Borrowed value (no per-row `dupe`)**: correct and kept (removes ≈ 400k alloc/free pairs), but `baseSeek` was unchanged (531 vs 533), confirming the value copy was never the cost.

What remains, in impact order, is therefore genuinely structural and matches levers 1–4 above plus the key encoding: **(a)** overlapped async base-row reads through the existing reactor for the cold/miss case; **(b)** a fixed-width integer primary-key encoding so the descent's `findChildPageId` and the leaf's `findCellByKey` compare machine words instead of decimal **text** across 16 KiB pages (this is also the correctness fix for general clustered range scans, and the cache-locality of the binary search is the dominant warm cost); **(c)** denser leaves so a given range spans fewer pages. The cheap wins are exhausted; closing the last ~3.5x to InnoDB requires one of these.

The one change that *did* land as a real win this pass: skipping the redundant explicit per-PK base-leaf prefetch descent for already-sorted batches (the kernel's own read-ahead covers the now-sequential access), which cut this query's warm time from 1275 ms to ~690 ms (1.84x).

---

## 8. Capability Baseline: What kaidb Is and Can Do

This is the authoritative statement of current capability, verified against the code and the test/benchmark suites, and it is deliberately neither over- nor under-stated.

### What kaidb is

A **standalone, disk-based, index-organised (clustered) B+Tree storage engine** with SQL and document surfaces, reached over a binary wire protocol, written in Zig. It is fully durable: write-ahead logging, doublewrite torn-write protection, crash recovery, cold and hot backup, and point-in-time recovery to an LSN. It is **not** an in-memory database; data lives on disk and is paged in on demand through a bounded, auto-sized buffer pool.

### Verified capabilities (each has a red-to-green test or a measured benchmark)

*   **Durability under failure.** Committed data survives hard kill (`kill -9`), torn mid-write, and disk-full (ENOSPC), and recovers exactly with no corruption. The tail torn WAL record is correctly discarded.
*   **Concurrency.** Per-tree structure lock plus per-frame latches plus a per-table group lock give multi-writer safety; a default-on concurrency fuzzer guards it. The `:3009` binary data-plane handled 80 concurrent clients and a deliberately runaway self-join without OOM or crash.
*   **MVCC.** Snapshot isolation with inline latest version plus an undo chain; READ COMMITTED, REPEATABLE READ and SERIALIZABLE (SSI) isolation.
*   **SQL surface.** Point, range, `IN`, index-only count, MIN/MAX endpoints, grouped aggregates with HAVING, DISTINCT, ORDER BY (forward and backward index scans), deep OFFSET pushed into the ordered scan, table aliases (single-table and joins), covering indexes, nested-loop and hash joins.
*   **Operability.** `require_auth` / TLS-gated password path / forced admin rotation; `/healthz`, `/readyz`, and a Prometheus `/metrics` with a query-latency histogram, buffer-pool hit ratio, WAL-size and checkpoint-lag gauges, and replication lag; backup/restore/PITR CLI; primary/follower replication with quorum confirmation and `PROMOTE`/`DEMOTE` role transition (validated live over loopback).

### The honest scope boundary, and why it is a roadmap and not a wall

The one regime that has not been re-measured with the current build is a working set that substantially **exceeds the buffer pool**. The concern there is specific and bounded: the random secondary-index descent (section 6) turns cached page touches into random disk seeks, and kaidb does not yet have the streaming-async-I/O, I/O-combining, and bulk-scan-ring machinery that keeps PostgreSQL fast in that regime (section 7). None of those require a redesign, and the highest-impact one (async overlapped reads) reuses the reactor kaidb already has. So "general-purpose at scale" is an engineering roadmap (section 7, levers 1 to 4), not a structural limit. The correct next step is to quantify the current gap with a small-pool, working-set-exceeds-cache benchmark, then implement lever 1.

### Deliberate non-goals (today)

Horizontal sharding (scale is instances behind a proxy), large out-of-line blobs beyond the overflow cutoff (about 2 KiB inline), and automatic HA failover policy (the engine provides the `PROMOTE`/`DEMOTE` mechanism; fencing an un-notified old leader is the orchestrator's job).
