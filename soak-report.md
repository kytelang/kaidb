# kaidb on-hardware soak run (2026-09-18)

Executed the `KNOWN-ISSUES.md` seven-point on-hardware soak checklist against the
benchmark box (Apple silicon, macOS, single node), ReleaseFast build. This records
exactly what was run, what passed, what failed, and what could not be validly run on
this box, so the result can be trusted rather than assumed.

**Honest scope caveat up front.** Two of the seven items were driven, for convenience,
through the **:3008 HTTP/JSON debug endpoint** (curl/python), NOT the **:3009 binary
data-plane** that real drivers and the `max_connections` governance actually use. The
storage-level items (durability, backup) are port-independent and their results stand;
the front-door items (concurrency, throughput) are NOT valid against the debug endpoint
and are marked so. Re-run those via the binary protocol before trusting them.

## Result table

| # | Item | Verdict | Notes |
|---|---|---|---|
| 2 | kill-9 mid-write, repeated | **PASS** | 10/10 committed-data-durable + 8/8 torn-mid-write, clean recovery each time. Storage-level, port-independent. |
| 4 | Backup + restore drill on real data | **PASS (after fixing 2 bugs)** | Found + fixed a silent-empty-backup footgun and a snapshot-header-currency gap; round-trip now byte-identical over 50k rows. |
| 1 | Sustained load (engine stability) | **PASS (engine), abbreviated** | 90s (not hours). WAL sawtooths bounded by checkpoints (never unbounded), `ckpt_lag` resets to 0, RSS stable (~150 MB, no leak), 100% buffer-pool hit ratio, ~65k queries, zero errors. Throughput NOT representative (ran via :3008). |
| 3 | Disk-full behaviour | **INCONCLUSIVE / needs rig** | On ENOSPC (~96k rows on a 64 MB RAM disk) the server process exited; it did NOT degrade to read-only. Clean-restart-over-full-disk could not be observed because the disk was so full even the restart log could not be written. Needs a volume with log headroom (or logs on a separate disk) to get a real verdict. This was the checklist's flagged highest-risk item and it remains the one real coverage gap. |
| 7 | Connection governance | **NOT VALIDLY RUN** | Driven against the :3008 HTTP debug endpoint, which went down under 100 concurrent connections. That is the naive debug server, not the governed :3009 data-plane (`TcpServer.max_connections`, per-query memory cap + deadline). Re-run via the binary protocol required for a real verdict. |
| 5 | Replication over a real network | **NOT RUN** | Only loopback exists on this box; the checklist requires a real network follower. Loopback replication is separately covered by the in-tree replication tests. |
| 6 | Long-running-transaction behaviour | **NOT RUN** | Needs a persistent binary-protocol session to hold a transaction open; the :3008 HTTP endpoint is stateless per request. Re-run via the binary protocol / CLI. |

## Bugs found and fixed this run

Both were found by item 4 (backup/restore) and are committed:

1. **`novadb backup <dir>` silently backed up an empty database** when `<dir>` had no
   `nova.db`. `Database.open` creates the file on `FileNotFound`, so pointing the CLI at
   the deployment root (which holds `db.json`) instead of the server's `base_dir` (default
   `data`, which holds `nova.db` + `wal/`) produced an **empty** snapshot with a
   `backup complete` message, discovered only at restore. For a backup tool this is a
   serious footgun. Fixed: `backup` now checks the source exists and errors with a message
   naming the correct path (commit `43973ab`).

2. **`exportSnapshot` did not rewrite the page-0 header** from the live master tree before
   copying pages, so a snapshot taken between checkpoints could carry a stale catalog root
   (same class as the free-list-durability gap). Fixed: it now refreshes root/lsn/free-list
   head under the exclusive `rw_lock` the hot path already holds, so the snapshot is
   self-consistent even if restore-time recovery skips WAL replay (commit `43973ab`).

The backup/restore mechanism itself is correct: hot `BACKUP DATABASE TO` under load, then
`restore` into the `base_dir`, gave byte-identical `COUNT`/point-lookup/`SUM` over 50k rows.

## What the pass results actually establish

- **Durability under hard kill is solid** (item 2) — the most important production property,
  exercised 18 times with zero data loss and clean recovery.
- **Backup/restore + PITR tooling works** (item 4) once the path footgun is fixed.
- **The engine is stable under sustained write load** (item 1) — bounded WAL, no memory leak.

## What must still be done before "in prod" (this box could not close these)

1. **Disk-full (item 3): build a proper rig** — a data volume with a few MB of headroom (or
   logs on a separate disk), fill it under write load, and confirm the server fails writes
   cleanly and recovers intact on restart. Current observation (server exits on ENOSPC, no
   read-only degrade) is a real robustness question, not yet a pass.
2. **Connection governance + throughput (items 1-throughput, 7): re-run via the :3009 binary
   protocol** (the kyte driver or CLI), not the :3008 HTTP debug endpoint. Verify
   `max_connections`, the per-query memory cap and deadline fire, and the accept loop stays
   responsive under a runaway query.
3. **Replication over a real network (item 5)** and **long-transaction behaviour (item 6)**:
   need a real second host and a persistent binary-protocol session respectively.

Net: the durability-critical items pass, two real backup bugs are now fixed, and three items
(disk-full, data-plane governance, real-network replication) still need a proper rig or the
binary protocol before the single-node non-critical role can be signed off.
