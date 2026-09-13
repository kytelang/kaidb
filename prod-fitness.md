# kaidb (NovaDB) Production Fitness Audit

Status: verified against the source tree at commit `87a8221` (branch `main`).
Every claim below cites `file:line` so it can be checked. Where a statement is a
measurement from this session's benchmarks it is marked **[measured]**; where it is
an informed engineering estimate rather than a read fact it is marked **[estimate]**.
Everything else is **[verified]** by reading the cited code.

## 1. Scope this audit judges against

kaidb is positioned as a **single-node embedded relational engine for read-heavy,
mostly-in-RAM workloads** (SQL, MVCC, WAL-backed durability, optional primary+follower
replication). Its original narrower framing, the **control-plane / config store for the
Nova/Kyte orchestrator**, remains the primary proven deployment; the wider framing is
supported by this session's measurements (parity-to-2x of Postgres and ahead of
InnoDB on 1M rows through an identical client) and is adopted deliberately.

Three boundaries are explicit and non-negotiable rather than hand-waved:
1. **Clustered-storage cost** on large secondary-index fan-out (see 4.8): fine until a
   workload routinely ships thousands of rows via secondary indexes.
2. **Single node**: no horizontal scale/sharding.
3. **Operational surface** must be present for the wider claim (metrics/health/PITR
   /TLS-gated auth all landed this session, see 5.B; no required gate remains open).

This document judges fitness for that single-node relational role, and separately notes
what a distributed/general-purpose ambition would additionally require.

"Production-ready" here means: durable across crashes, safe under concurrency,
authenticated, backup/restorable, observable, and operable. Benchmark speed is
necessary but not part of the readiness bar.

## 2. Methodology and honesty note

An earlier one-line "is it prod-ready?" answer in this session was a fast triage from
memory and got two things wrong (it called auth "missing" and crash-recovery a "gap"
when both exist and are exercised). This document is the corrective: a subsystem walk
with citations. Treat the verdicts as reliable; treat effort sizes as estimates.

## 3. Verdict summary

| Subsystem | Status for scoped role | Evidence |
|---|---|---|
| Crash recovery / durability | READY | `durability/`, `pool.zig` WAL-before-page + doublewrite |
| Concurrency safety | READY (last known race fixed this session) | `btree.zig`, `root.zig` fuzzers |
| Authentication / authorization | READY (opt-in; enforce + TLS-gate via config) | `concurrency/security.zig`, `proto/session.zig` |
| Backup / restore | READY (cold + hot) | `main.zig` CLI, `BACKUP DATABASE TO` (live), snapshot+WAL |
| Replication / HA | USABLE, ops-thin | `schema/database.zig` fence + follower |
| Resource governance | PARTIAL | `query_executor.zig`, `tcp_server.zig` |
| Observability / metrics | PARTIAL (counters + query-latency histogram) | `main.zig` `/metrics`, `common/histogram.zig` |
| Point-in-time recovery | READY (LSN target) | WAL archiving + `restore --archive --target-lsn`; time-target follow-up |
| Scale (general-purpose) | NOT A GOAL for scoped role | `btree.zig` clustered design |

Bottom line: **fit for the scoped control-plane role** (observability, PITR, and TLS-gated
auth now landed; no required gate open); not intended as a general-purpose DB, and the
scale items are only required if that ambition changes.

## 4. Subsystem findings (verified)

### 4.1 Durability and crash recovery: READY

- Write-ahead logging with the standard WAL-before-page invariant is enforced at every
  page write, not just at checkpoint: `PagePool.walBeforePageLsn` (`storage/pool.zig:359`)
  is called on the eviction/flush paths (`storage/pool.zig:638`, `:721`) and
  `walBeforePage` up front on the whole-pool flushes (`:834`, `:923`, `:964`).
- WAL segments are `fsync`ed (`durability/write_ahead_log.zig:650` `sync`), and an
  atomic checkpoint marker (`CHECKPOINT`, written via temp-file + rename) records the
  replay boundary (`durability/checkpoint.zig:65`, `:99`).
- Recovery on open is a documented three-phase redo (catalog, user DML + index rebuild,
  then repair) (`schema/database.zig:54`), preceded by torn-write repair from the
  doublewrite buffer (`recoverDoublewriteBuffer`, `schema/database.zig:1213`) before any
  page is trusted. WAL replay entry points: `write_ahead_log.zig:1035` (`replay`),
  `:967` (`replayFile`).
- `synchronous_commit` is configurable (`common/config.zig`, applied at
  `main.zig` via `db.synchronous_commit`), trading latency for per-commit durability.
- **Known constraint [verified]:** the overflow cutoff is fixed at `PAGE_SIZE/8`
  (`storage/overflow.zig:94`); the comment at `:80`-`:90` states that raising it is
  blocked on an unaddressed ARIES crash-recovery gap for large overflow values. So very
  large inline values are deliberately capped rather than risk that path. Fine for the
  control-plane workload; a constraint to note for large-blob use.

Assessment: durable and crash-safe for its workload, with one self-documented cutoff.

### 4.2 Concurrency safety: READY

- Per-tree `structure_lock` (shared for in-place reads/writes, exclusive for
  restructures) plus per-frame latches; the buffer pool is 16-way sharded with
  per-shard `rw_lock` (`storage/pool.zig` header docs; `MIN_POOL_SIZE=64` at `:95`).
- A default-on concurrency fuzzer runs in the plain test gate (not opt-in):
  "OVERLAPPING-key writers on one tree" (`root.zig:4705`-`4708`), scaled up by
  `NOVADB_FUZZ`.
- **The one known race is fixed this session [measured]:** `PageStillPinned` on a
  delete-driven merge/collapse, root-caused to a page pinned across a `discardPage` of
  that same page in three sites (`mergePages`, `deleteExclusive` root collapse,
  `handleUnderflow`) in `storage/btree.zig`. Reproduced deterministically via a
  window-widening fault hook (`btree.fault_merge_yield_ms`, `storage/btree.zig`),
  which turned a ~0/15 natural rate into ~5/40; post-fix 0/50 with the window on, and
  an always-on regression test guards it ("REGRESSION: merge pin-lifetime", `root.zig`).

Assessment: safe for the concurrent access the role sees. Higher write concurrency on a
single hot tree is bounded by the exclusive `structure_lock` (see 5.6), which is not a
correctness issue.

### 4.3 Authentication and authorization: READY (opt-in)

- Full security surface exists: `CREATE USER ... WITH PASSWORD`, `CREATE ROLE`,
  `GRANT`/`REVOKE` in SQL (`sql/ast.zig:91`+, `sql/lexer.zig`), an Argon2id password
  verifier with per-user salt (`concurrency/security.zig:174` `User`, `authenticate` at
  `:516` with constant-time compare at `:559`), session tokens, and brute-force lockout
  with exponential backoff (`security.zig:333`-`339`).
- Startup challenge/response is wired: on an enabled manager with users the server
  issues a cleartext-password challenge and authenticates (`proto/session.zig:359`).
- **Enforcement was added this session [measured]:** `config.security.require_auth`
  (`common/config.zig:143`) is applied to the live manager at startup (`main.zig`), and
  the wire session fail-closes every data-plane frame on an unauthenticated connection
  with SQLSTATE 28000 (`proto/session.zig:361`). Verified: wrong password rejected,
  correct `admin` accepted.
- **TLS-gate landed this session [measured]:** `config.security.require_tls_for_auth`
  (implies `require_auth`) refuses the cleartext-password challenge on a plaintext link
  (SQLSTATE 28000, before any password is read) and only offers it over TLS. The data-plane
  `TcpServer` now honours `config.tls` (cert/key), building per-connection TLS options with
  a fresh `now`/CSPRNG (`query/tcp_server.zig` `enableTlsFiles`/`handleConnectionInner`); a
  `secure` flag threads from the connection handler to the session startup guard
  (`proto/session.zig`). The server refuses to start if `require_tls_for_auth` is on but TLS
  is not configured, so it can never silently lock every client out. Proven by
  "O4-TLS: require_tls_for_auth refuses cleartext password on a plaintext link" (`root.zig`):
  plaintext -> 28000 + unauthenticated; secure -> normal challenge succeeds.
- **Forced rotation landed this session [measured]:** a fresh database still bootstraps
  `admin`/`admin` (`schema/database.zig:506`), but `security.require_admin_password_change`
  makes the server *refuse to start* while `admin` still has the default password (checked
  with `SecurityManager.passwordMatches`, a constant-time compare that takes no
  lockout/side-effect path, unlike `authenticate`). The bootstrap paradox (rotating needs an
  admin login, but the gate blocks boot) is resolved by the offline
  `novadb passwd <base> admin '<newpw>'` command, which opens its own handle and rotates
  without the server running; the startup error names it. Proven by
  "ADMIN ROTATION: passwordMatches detects the default and clears after rotation"
  (`root.zig`) plus a CLI smoke test.

Assessment: the mechanism is real and enforceable; production use requires enabling
`require_auth` (or `require_tls_for_auth`), rotating the bootstrap credential, and running
over TLS.

### 4.4 Backup and restore: READY (cold + hot)

- Consistent physical snapshot primitives exist and are replication-tested:
  `Database.exportSnapshot` (`schema/database.zig:2450`) flushes WAL, flushes all pages,
  `fsync`s, copies every page to `snapshot.db`, and copies the WAL directory;
  `restoreSnapshot` (`:2504`) reconstructs the data file + WAL.
- **Operator CLI added this session [measured]:** `novadb backup <base> <dest>` and
  `novadb restore <snap> <dest>` (`main.zig`), verified end-to-end (load → backup →
  restore into a fresh dir → byte-identical query results across all benchmark queries).
- The `novadb backup` CLI opens its own handle, so it is a **cold** backup (run against a
  stopped server).
- **Hot (online) backup landed this session [measured]:** `BACKUP DATABASE TO 'dir'` was
  parsed but the executor's handler wrote a bare, WAL-less, unlocked single-file page image
  that neither the restore CLI nor recovery could consume. It now runs `exportSnapshot`
  in-process under the exclusive `rw_lock` that `executeStatement` already holds for the
  statement (so no checkpoint/vacuum reshuffles pages mid-copy; do NOT re-acquire the lock,
  it is not reentrant), producing the same `snapshot.db` + `wal/` layout as an offline
  backup. The result restores with the identical tooling (`novadb restore`, PITR).
  Proven by "HOT BACKUP: ... on a live server yields a restorable snapshot" (`root.zig`):
  a never-closed server takes the backup, keeps serving, and the restored copy has exactly
  the pre-backup rows (post-backup writes correctly excluded).

Assessment: reliable cold and hot backup/restore; PITR (LSN target) via WAL archiving +
`restore --archive --target-lsn` (see 5.B.3). All required backup gates closed.

### 4.5 Replication and HA: USABLE, operationally thin

- Primary ships WAL to a follower; a follower is entered via `becomeFollower`, and a
  fence epoch protects against a stale leader: `guardWrite` rejects a write from a
  fenced-off leader (`schema/database.zig:2396`), the epoch is persisted to a sidecar
  (`:2584`, `fenceDir` at `:2437`), and resync uses `exportSnapshotForResync` (`:2544`).
- A background writer + HA lease loop run under the `Database` (`schema/database.zig:522`
  spawns `runBgWriterTask`).

Assessment: the correctness primitives (fencing, snapshot resync) are present and
tested for the orchestrator's needs, but the **operational surface is thin**: no lag
metric, no first-class promote/failover command, no automated re-sync tooling (see 5.B.7).

### 4.6 Resource governance: PARTIAL

- Per-query result-memory cap: `result_bytes_limit` (`query_executor.zig:736`), set from
  `config.query_memory_limit_bytes` at `main.zig`; a runaway query fails with "Query
  Memory Limit Exceeded" rather than OOMing the server (`query_executor.zig:57`).
- Per-query deadline: `deadline_ms` + `checkDeadline` (`query_executor.zig:741`).
- Connection cap: `TcpServer.max_connections` enforced at accept time via an atomic
  counter (`query/tcp_server.zig:120`-`122`, `:36`-`38`); slow clients run as independent
  async tasks and cannot block the accept loop.

Gaps [verified/estimate]: no idle-connection timeout, no explicit backpressure signal
when the pool is saturated, and the memory cap is per-query not global. Adequate for a
low-connection control-plane client; needs the timeouts for a broader front door.

### 4.7 Observability: PARTIAL (basics landed this session)

- **Landed [measured]:** `GET /healthz` (liveness), `GET /readyz` (503 until the
  database is open/not-closed), and a Prometheus `GET /metrics` on the existing HTTP
  front door (`main.zig` `handleHttp`). `/metrics` exposes real counters: buffer-pool
  size + resident pages, `fetches_total`, `evictions_total`, mmap borrow vs pread
  serves, checksum failures, `next_tx_id`, and `current_lsn`. Verified live over HTTP.
- **Query-latency histogram landed this session [measured]:** `kaidb_query_duration_seconds`
  is a proper Prometheus histogram (cumulative `le` buckets from 0.5ms to 10s, plus `_sum`
  and `_count`). Every top-level `QueryExecutor.execute` records its end-to-end latency into
  a lock-free per-`Database` histogram (`common/histogram.zig`, atomic buckets), exported at
  `/metrics`. This gives p50/p90/p99 via `histogram_quantile` and a QPS rate via
  `rate(..._count[1m])`. Covered by a histogram unit test and an executor-fed integration
  test (`root.zig`).
- **Still missing [verified]:** a split hit/miss ratio (only combined `fetches_total`
  today), WAL-size and checkpoint-lag gauges, active-transaction and lock-wait gauges,
  replication-lag, and a structured (JSON) log option.

Assessment: the operability floor is now met (health, readiness, core engine
counters); the richer query/replication telemetry remains a follow-up.

### 4.8 Storage design and scale: intentional trade-off

- kaidb is **index-organised (clustered)**: the base table is a PK B+tree, and a
  secondary-index scan yields PKs, each of which costs a full base-tree descent to fetch
  the row. **[measured]** this session's QPROF put ~85% of warm row-return query time in
  that per-row `base-search+decode`.
- Mitigations already shipped: async base-leaf prefetch for the disk-bound regime
  (`storage/pager.zig:240` `prefetchPages`, batch `DEFAULT_PREFETCH_BATCH=256` at
  `query/iterator.zig:85`); auto buffer pool sized ~50% of RAM (`common/config.zig:257`,
  `:263`; `pool_size=0`=auto); footprint fixes (earlier sessions) that took a 1M load
  from ~15 GB to ~993 MB.
- **[measured]** against Postgres 18 and MySQL 8/InnoDB on 1M rows through an identical
  Kyte client: kaidb is parity-to-2x of Postgres on most row-return queries, ahead of
  InnoDB, and wins count/aggregate. The residual gap is the clustered double-lookup.

Assessment: correct and competitive for the scoped role; the clustered design's cost on
large secondary-index fan-out is a known trade-off, not a defect. The scale items in
section 5.A are only warranted if kaidb targets general-purpose use.

### 4.9 Test surface

- 69 `test` blocks in `root.zig` (`grep -c` verified), including default-on concurrency
  fuzzers (`:4705`), scaled by `NOVADB_FUZZ`; the full `zig build test` gate is green at
  `87a8221`.

## 5. Gap roadmap

### 5.A Scale architecture (only required if targeting general-purpose use)

1. **Physical row locator in secondary indexes** [estimate: large]. Store a validated
   base-leaf page-id hint in each index entry to skip the per-row base descent (the
   85%). Needs a per-page owner tag (on-disk format bump + reload) or a
   "base-tree-never-frees-to-global-pool" invariant; validate-and-fall-back-to-PK;
   backfill; recovery support. Highest-impact lever.
2. **Scan-not-seek planner** [medium]. Push `LIMIT` into index scans and prefer a seek
   when an index covers the predicate; small-`LIMIT` queries currently full-scan.
3. **Multi-index AND without per-index re-scans** [medium]. Index intersection.
4. **Large-value out-of-line store** [medium-large]. Replace overflow chains
   (`overflow.zig:94`) with a TOAST-style single-pointer store; also revisit the
   `PAGE_SIZE/8` cutoff once the ARIES gap (`overflow.zig:80`-`90`) is closed.
5. **Buffer-pool / memory beyond RAM** [medium]. Re-land mmap reads safely (previously
   reverted), improve the CLOCK evictor.
6. **Write-concurrency ceiling** [large]. The per-tree exclusive `structure_lock`
   serialises restructuring writers; finer-grained latching would raise write throughput.
7. **MVCC version-chain cost** [medium]. `rowVisible` (`query_executor.zig:883`) walks
   the chain per row; add in-place pruning; tune `vacuum` (`database.zig:1046`).
8. **Horizontal partitioning/sharding** [very large; out of scope for the role].

### 5.B Operational tooling (needed for the scoped role, in priority order)

1. **Observability `/metrics`** [DONE-partial]. Prometheus `/metrics` with core engine
   counters plus a query-latency histogram is live (4.7). Remaining follow-ups: hit/miss split,
   WAL-size + checkpoint-lag, active-txn/lock-wait, replication-lag gauges.
2. **Health / readiness probes** [DONE]. `/healthz` + `/readyz` live (4.7). JSON
   structured logging remains a small follow-up.
3. **Point-in-time recovery** [DONE (LSN target); time-target is a follow-up].
   WAL archiving landed this session: with `durability.wal_archive_dir` set, a checkpoint
   truncation and the age-based GC copy each retired segment into the archive before
   deleting it (`write_ahead_log.zig` `setArchive`/`archiveSegment`, hooked into
   `truncateActiveLogs` and `truncate`), so the full history past a base backup survives.
   `novadb restore <snap> <dest> --archive=<dir> --target-lsn=N` (`main.zig`) lays down the
   base snapshot, merges the archived segments (archive-wins on overlap), then opens with
   `openAt(target_lsn=N)` and closes, materialising the database exactly as of LSN N.
   Proven by a red->green test ("P7 PITR: archived WAL survives checkpoints and restores to
   a target LSN", `root.zig`) that checkpoints the target segments out of the live WAL and
   restores from snapshot + archive only. Remaining follow-up: a `--target-time` wall-clock
   selector (needs a timestamp index over archived segments; LSN targeting is the primitive).
4. **Hot (online) backup** [DONE]. `BACKUP DATABASE TO 'dir'` runs `exportSnapshot` live
   under the exclusive `rw_lock` the executor already holds, producing a restorable
   snapshot+WAL directory without stopping the server (4.4).
5. **TLS-gate the password path** [DONE]. `security.require_tls_for_auth` refuses the
   cleartext-password challenge on a non-TLS connection (SQLSTATE 28000) and the data-plane
   `TcpServer` now honours `config.tls` (4.3). Credential rotation is now possible via
   `ALTER USER name IDENTIFIED BY 'newpw'` (added this session; role preserved), and
   `security.require_admin_password_change` *forces* it: the server refuses to start while
   `admin` still has the default password, with `novadb passwd` as the offline rotation
   escape hatch (4.3). No follow-up remains here.
6. **Connection governance** [small-medium]. Idle timeout, backpressure; surface the
   existing per-query deadline + memory cap in config.
7. **Replication operations** [medium]. Lag monitoring, `promote`/failover command,
   automated re-sync, failover verification around the fence epoch (4.5).
8. **Online admin ops** [medium]. Online compact/index rebuild (`compact` is offline
   only), vacuum controls, a user/role admin CLI wrapper.
9. **On-disk format upgrade tooling** [medium]. A `migrate` path so a format bump (e.g.
   the 5.A.1 owner tag) does not require a manual dump+reload.

## 6. Recommended path for the scoped role

For the **single-node relational** role, the operability floor is now met: 5.B.1
(metrics), 5.B.2 (health/readiness), 5.B.3 (PITR, LSN target), 5.B.4 (hot backup), and
5.B.5 (TLS-gate the password path) all landed this session. No required gate remains open;
the richer telemetry that remains (hit/miss split, replication-lag gauges) is a quality
follow-up, not a gate; the query-latency histogram landed this session.
None of the section 5.A scale items are required for this role; the clustered-storage
cost (4.8) is the boundary that bounds it. If the ambition later widens to a
distributed/general-purpose database, 5.A.1 (physical row locator) is the first and
largest lever, followed by 5.A.8 (partitioning).

## 7. Limitations of this audit

Verdicts and the presence/absence of features are code-verified with citations.
Effort sizes are estimates. Performance figures are this session's single-machine
(8 GB) measurements, warm, 1M rows, and are directional rather than a formal
benchmark. This audit did not run a fault-injection or TSan sweep across every
subsystem; the concurrency confidence rests on the default-on fuzzers plus the specific
merge-race work done this session.
