# CLAUDE.md — NovaDB

## What this is (and what it is NOT)

**NovaDB** is an embedded **B+Tree storage engine with SQL and document surfaces**, written in **Zig**.
Its intended, supported role is the **control-plane / config store for the Nova orchestrator**
(`packages/nova-orchestrator`): small, bounded, low-churn data such as workload specs, leader leases,
membership and manifests, reached over the binary wire protocol. In that role it is solid and verified
(the orchestrator's live store/lease/reconcile tests pass against it, and it recovers correctly across a
restart).

**It is NOT a general-purpose OLTP or document database at scale.** A 2026-08-30 benchmark (a MongoDB
`mongodb-perf-compare` port, 10M orders) made the limits concrete: the data footprint is bloated
(~3x the logical size), the buffer pool and scan/read paths are not I/O-efficient once the working set
exceeds the pool, index builds re-scan per index, and the planner scans where it should seek. Those are
real "designed for in-RAM, small data" gaps, and closing them is a large systems project, not a patch.
Treat NovaDB as the orchestrator's embedded store; do not size a general workload onto it. See
`orders_benchmark.md` and `benchmark_report.md` for the honest numbers.

Recovery correctness for the store's own use was fixed on 2026-08-30 (bounded WAL via runtime
checkpointing; committed-transaction set persisted at checkpoint and restored on open; collection B+Tree
root persisted on split so a collection is fully reachable after restart). It is the storage engine behind
the **Nova** language ecosystem (a *separate* project — Nova connects over the binary wire protocol).

Core systems:
- **Slotted-page B+Tree** — variable-length records (cells) in two-way-growth pages (slot directory grows
  down, cell payloads grow up); overflow pages for large values. O(log n) lookups/inserts/range scans.
- **Segmented buffer pool / pager** — page cache over the file, checkpointing.
- **MVCC** — multi-version concurrency control for consistent reads; undo log.
- **Durability** — write-ahead log (WAL) + checkpoints.
- **SQL** — parser + query executor + schema/catalog.
- **Binary wire protocol** — sessions, message-buffer pool, oid map, typed decode (the intended path for
  Nova's driver; there is also/was a JSON path).

(WASM/wasmer embedding was **removed** — the wasm plan is dropped for now; the DB builds with no wasmer
dependency.)

## Build / run

```bash
cd novadb
zig build                 # builds the `novadb` + `novadb-cli` executables (deps: yaml, tls, bson, utils modules)
zig build run             # or run ./zig-out/bin/novadb
zig build test            # unit tests
```
Config lives in `db.json`. The server listens for binary-protocol clients (see `src/proto/`).

### Cross-compiling (host build matrix)

NovaDB is pure Zig, so it cross-compiles from any host (macOS, Windows, WSL/Linux) with just the
bundled Zig toolchain. Pass `-Dtarget=<triple>` to build one target (installs to `zig-out/bin`), or
run `zig build cross` to stamp out all of them into `zig-out/cross/<triple>/`:

```bash
zig build -Dtarget=x86_64-macos        # macOS x86_64 (intel)
zig build -Dtarget=aarch64-macos       # macOS aarch64 (arm64)
zig build -Dtarget=x86_64-windows      # Windows x86_64
zig build -Dtarget=aarch64-windows     # Windows aarch64
zig build -Dtarget=x86_64-linux-gnu    # Linux x86_64
zig build -Dtarget=aarch64-linux-gnu   # Linux aarch64
zig build cross                        # all of the above at once (ReleaseSafe)
```

Add `-Doptimize=ReleaseFast` (or `ReleaseSafe`) for an optimised binary. Release builds link libc (the
server uses `std.heap.c_allocator`), which is wired in `build.zig`.

**Windows server:** all six targets now build BOTH binaries (`novadb` + `novadb-cli`), including
`novadb.exe`. The raw `std.posix` socket options that Zig 0.16 rejects for the Windows target
(`setsockopt` for RCVTIMEO/SNDTIMEO in `replication.zig connectRaw`, and TCP_NODELAY/SO_KEEPALIVE in
`tcp_server.zig listen`) are best-effort tunables, so they are comptime-guarded behind
`builtin.os.tag != .windows` — the guard prunes the posix branch for the Windows build, and the native
path is unchanged. This unblocks the CROSS-BUILD; the Windows server runtime is still **not run-verified**.
When it is brought up for real, add the winsock equivalents (SO_RCVTIMEO/SO_SNDTIMEO take a DWORD of
milliseconds, not a `timeval`; TCP_NODELAY/SO_KEEPALIVE via `ws2_32`).

## Layout (`src/`)

- `storage/` — `btree.zig`, `pager.zig`, `page.zig`, `pool.zig`, `overflow.zig` — the slotted-page B+Tree
  + buffer pool. **This is the core.**
- `durability/` — `write_ahead_log.zig`, `checkpoint.zig`, `wal_error.zig` — WAL + recovery.
- `proto/` — `protocol.zig`, `wire.zig`, `session.zig`, `session_pool.zig`, `command.zig`, `oidmap.zig`,
  `message_buffer_pool.zig` — the binary server protocol.
- `schema/` — `database.zig`, catalog/schema.
- `src/main.zig`, `src/cli.zig`, `src/root.zig` — entry + CLI.
- `architecture.md` — the authoritative design spec (page layout, B+Tree, concurrency protocols).
- `btree_readiness_plan.md`, `btree_network_design.md`, `benchmark_report.md` — planning/perf docs.

## Key facts / gotchas

- **Concurrency (as of Stage 3, 2026-08-08)** — the old global `db.rw_lock` write bottleneck is gone. The
  db-wide lock now only gates DDL (exclusive) and FK/join statements; per user table there is a **`GroupLock`**
  (`common/sync.zig`): SELECT = read mode (readers concurrent), INSERT = write mode (writers concurrent),
  UPDATE/DELETE = exclusive (they scan, so alone on that table). Concurrent writers on ONE tree are made safe
  by a per-tree **`structure_lock`** in `btree.zig` (shared for in-place ops, exclusive for split/merge). See
  `architecture.md` section 2 ("Per-Tree Structure Lock" + "Per-Table Access Lock"). Guardrails live in the
  test suite: the Stage-3 concurrency fuzzer (`FUZZER (concurrent)` under `NOVADB_FUZZ=1`), the `GroupLock`
  invariant stress, and the `STRESS:` consistency tests.
- **`nova.db`** is a test DB artifact (binary) — regenerated by running the engine; don't hand-edit.
- Read `architecture.md` FIRST for any storage/execution/transaction change — it specifies the invariants.
- Zig version: matches the toolchain in `build.zig.zon`.

## Relationship to Nova

Nova (the `lang` repo) talks to NovaDB via its **binary protocol** through `packages/nova-novadb`
(the Nova driver). NovaDB is intentionally independent — build and version it separately.
