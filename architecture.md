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

### Segmented Lock-Free PagePool
The `PagePool` is segmented into $N$ independent instances (default $N = 8$).
*   **Hashing**: A page request for `page_id` is mapped to an instance index via:
    $$\text{instance\_idx} = \text{page\_id} \pmod N$$
*   **Lock Isolation**: Each instance has its own local mutex and hash table (`page_table`). Thread contention is localized to $1/N$ of the database page space.

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
