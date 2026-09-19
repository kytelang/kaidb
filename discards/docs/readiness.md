# Production Readiness Roadmap

This document outlines the architectural changes, systems, and features required to transition the relational B+Tree database engine from its current functional MVP/prototype state to a production-ready, enterprise-grade database system.

---

## 1. MVCC Garbage Collection (Vacuuming) (Implemented)

In our current Multiversion Concurrency Control (MVCC) implementation, updates and deletes append new versions and write deletion markers (`xmax`). These versions remain on disk indefinitely.

> [!WARNING]
> Without garbage collection, the database will experience severe "version bloat," exhausting disk space and degrading read performance because the engine must traverse long version lists in leaf page cells.

### Proposed Architecture
* **Background Vacuum Thread**: A background worker daemon that periodically scans table pages.
* **Visibility Boundary**: Identify rows where the deleting transaction (`xmax`) is committed and older than the oldest active transaction in the system.
* **Compaction**: Safely remove expired row versions from B+Tree leaf pages and trigger slotted page compaction to reclaim space.

---

## 2. WAL Checkpointing & Truncation (Implemented)

Currently, our Write-Ahead Log (WAL) grows indefinitely, and recovery requires replaying the log from the very beginning of the database's lifetime.

### Proposed Architecture
* **Fuzzy Checkpointer**: A background thread that periodically:
  1. Identifies all dirty pages currently in the `PagePool`.
  2. Writes a `CHECKPOINT` record to the WAL containing the list of dirty page IDs and the minimum uncommitted LSN.
  3. Flushes the WAL to disk, then forces dirty pages to be written back to table files.
* **WAL Truncation**: Reclaims disk space by safely truncating or archiving WAL segments older than the checkpoint LSN.

---

## 3. Asynchronous Disk I/O & Torn Write Protection (Implemented)

Right now, disk writes happen synchronously within transaction execution or on-demand when the page pool evicts pages.

### Proposed Architecture
* **Background Page Writer (BgWriter)**: A dedicated thread that continually flushes dirty pages to disk, keeping clean pages available in the `PagePool` to avoid blocking user transactions on eviction writes.
* **Doublewrite Buffer**:
  - To prevent database corruption from partial page writes (torn writes) caused by power failures, pages are first written to a contiguous disk block (Doublewrite Buffer) and flushed.
  - The page is then written to its actual storage location. If a crash occurs, the original page can be restored from the doublewrite buffer.

---

## 4. Query Engine Enhancements (Implemented)

Our current SQL execution path is simple and direct (evaluates filters dynamically, supports simple table scans and single-column index scans).

### Proposed Architecture
* **Volcano Iterator Model**: Refactor the query executor to return dynamic iterators (`Scan`, `Filter`, `Project`, `Join`) instead of buffering full result sets in memory.
* **Relational Joins**: Implement standard join algorithms:
  - **Nested Loop Join** (ideal for small tables).
  - **Hash Join** or **Sort-Merge Join** (for high-volume tables).
* **Cost-Based Optimizer (CBO)**: Analyze table statistics (e.g. row counts, index cardinality) to dynamically choose the optimal execution path (e.g., deciding between an Index Scan vs. full Table Scan).

---

## 5. Page Integrity & Corruption Detection (Implemented)

Enterprise databases must guarantee that data read from disk has not been corrupted by hardware failures or filesystem issues.

### Proposed Architecture
* **Page Checksums**: Calculated a CRC32 checksum (`std.hash.Crc32.hash`) for every slotted page header before writing to disk, and verified on reads to fail fast on corruption.
* **Catalog Sanitizers**: Run boot-time integrity checks of the master catalog (`sys.objects`, `sys.tables`) to detect corruption before startup.

---

## 6. Resource Governance & Limits (Implemented)

To prevent a single bad query or runaway client connection from exhausting host system resources.

### Proposed Architecture
* **Memory Quotas**: Abort queries that attempt to allocate too much memory for sorting or join tables (via a custom tracking `MemoryLimitAllocator`).
* **Statement Timeouts**: Abort transactions/queries exceeding a configured execution time (via deadline checks using `std.Io.Clock`).
* **Connection Pooling & TCP Wire Protocol**: Implemented a length-prefixed binary wire protocol over TCP, enforcing connection limits and connection-isolated transaction sessions.
