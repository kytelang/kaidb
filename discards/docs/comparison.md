# Architecture Comparison: B+Tree Database Engine vs. SQLite

This document compares the architectural design, concurrency models, deployment models, and feature sets of this B+Tree Database Engine (Nova DB) against SQLite.

---

## High-Level Comparison

| Feature | This B+Tree Engine | SQLite |
| :--- | :--- | :--- |
| **Primary Use-Case** | Networked, concurrent relational database | Embedded, in-process local device storage |
| **Concurrency** | Non-blocking MVCC (Multi-Version) | Lock-based (Single writer) |
| **Deployment Model** | Client-Server (Plain TCP / TLS) | Embedded Library |
| **Replication** | Built-in WAL shipping over TCP | None (Requires Litestream / LiteFS) |
| **Crash Protection** | Doublewrite Buffer & transaction log | Rollback journal / filesystem atomic sector guarantees |
| **Security / Auth** | Dynamic RBAC (`sys.roles` & `sys.privileges`) | File-level OS access controls |
| **Extensibility** | Sandboxed WebAssembly (WASM) UDFs | Native C extension loading |

---

## Architectural Deep Dive

### 1. Concurrency: MVCC vs. Locking
* **This Engine (MVCC)**: Implements slot-level **Multi-Version Concurrency Control (MVCC)**. Each record has header tags for `xmin` (transaction that inserted/updated the record) and `xmax` (transaction that deleted/updated the record) packed directly into slotted database pages. Readers never block writers, and writers never block readers. An active background **Vacuum worker** automatically scans and reclaims obsolete non-visible versions.
* **SQLite (DB/Table Locks)**: Uses database-level locks. Writing blocks the entire database file by default. Even under WAL (Write-Ahead Log) mode, it supports concurrent readers alongside only a *single* active writer; multiple concurrent writers will block each other.

### 2. Access and Deployment: Client-Server vs. In-Process
* **This Engine (Network-Native)**: Built to run as a standalone database daemon listening on a network port. Features a custom framed network protocol with TLS encryption, connection pooling, and authenticated session management.
* **SQLite (Embedded)**: Designed strictly as an in-process library compiled directly into the parent application. It has no network layer, socket listeners, or client-server protocol.

### 3. High Availability: Native Replication vs. External Tools
* **This Engine**: Includes native, authenticated, primary-replica **Write-Ahead Log (WAL) replication** over TCP. The replica automatically handles catalog syncs, boots local B+Tree roots, and replays streamed transactions in real-time.
* **SQLite**: Has no built-in network replication. Replicating SQLite databases requires third-party tools (like Litestream or LiteFS) to hook filesystem level writes or replicate WAL segments to cloud storage.

### 4. Crash Durability: Doublewrite Buffer
* **This Engine**: Protects against partial page writes (torn pages) using a **Doublewrite Buffer**. Dirty database pages are first flushed sequentially to a Doublewrite file before being written to their final locations in the database. In the event of an OS crash mid-write, pages are restored from the Doublewrite Buffer during recovery.
* **SQLite**: Relies on rollback journals (writing sectors to a journal before updating the page) or standard OS/drive atomic write guarantees to prevent torn page corruption.

### 5. Sandboxed Extensibility: WebAssembly (WASM)
* **This Engine**: Integrates a dynamic WebAssembly host runtime (**Wasmer**) to run compiled user-defined functions (UDFs) in isolated, secure sandboxes directly inside the database process.
* **SQLite**: Supports loading custom extensions compiled as native C dynamic libraries, which run with full process privileges (no sandboxing).
