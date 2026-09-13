# NovaDB Replication and HA design

Status: DESIGN (not started). Written 2026-08-01. This is the DB-provider side of the replicated
control-plane store described in the orchestrator repo
(`nova-orchestrator/docs/beta-config-store-design.md`). That doc is the consumer view (an etcd-shaped
config store on NovaDB); this doc is how NovaDB actually provides single-writer WAL-shipping
replication with fencing, bounded RPO, backup/restore, and a leader-lease primitive. Implementation is
gated and deliberate; language and compiler work outrank it (standing rule).

## 1. Where we are (honest, grounded in the code)

- SHIP SIDE EXISTS AND IS WIRED. `src/query/replication.zig` (662 lines):
  - `ReplWal` -- a segmented replication WAL: `append`/`flush`/`rotate`/`readFile`/`deleteFile`,
    plus `checkpoint(confirmed_seq)` and `loadCheckpoint`.
  - `ReplClient` -- the leader-to-peer streamer: `ship(frames)`, `ensureConnected`, `connect`,
    `authenticate`, `disconnect`, with exponential backoff reconnect (100ms..5s).
  - `CheckpointRecord` -- `{ file_seq, last_flushed_lsn }`, JSON load/save.
  - Wiring: `write_ahead_log.zig` has a `replication_manager` callback fired per WAL record
    (`callback(self.replication_manager, record)`); `schema/database.zig` holds a
    `replication_manager: ?*ReplicationManager`. So committed records already fan out to the ship side.
- MISSING (the whole HA half):
  - FOLLOWER / APPLY side: nothing receives shipped frames, applies them to the local db in seq order,
    persists a confirmed seq, and acks.
  - FENCING: no epoch on frames; a partitioned old leader could still ship/commit.
  - QUORUM-ACK: the leader does not wait for any follower before acking a client (async only -> RPO>0).
  - BACKUP / RESTORE: no consistent off-box snapshot or point-in-time restore.
  - LEADER-LEASE: no single-writer election primitive (a CAS row / compare-and-set).
- CONCURRENCY NOTE (out of scope here): the global `db.rw_lock` ~5-thread ceiling
  (`btree_readiness_plan.md` section 1) is IRRELEVANT to a config store (tiny data, low write rate) and
  is NOT touched by this design. It stays gated on throughput evidence.

## 2. Target shape

Single-writer, WAL-shipping, fenced replication. Deliberately NOT Raft/multi-master: config-plane volume
is low and a single leader with hot standbys is enough, and it matches what ship-side already assumes.

```
   LEADER (only writer)                         FOLLOWER (standby)
   commit -> local WAL (durable)                recv frame -> validate epoch+seq
     |  WAL callback                              -> apply to local db (seq order)
     v                                            -> persist confirmed seq (CheckpointRecord)
   ReplWal.append + ReplClient.ship  ---frames---> Follower.recv -> Follower.apply -> ack(seq,epoch)
     |                                            ^
     +-- (quorum-ack) wait N/2+1 acks cover LSN --+
```

Every frame carries a monotone FENCING EPOCH and a contiguous SEQ. A follower and the receiving path
reject any frame whose epoch is below the highest seen, and any seq that is not exactly next-in-order.

## 3. The follower / apply side (the core new piece)

New: `Follower` in `src/query/replication.zig` (peer of `ReplClient`), plus a receive handler wired
into the server accept loop for the replication stream.

- STREAM: reuse the existing binary wire protocol framing (`common/proto.zig` Packet). Add two message
  types to the set in `btree_network_design.md` section 2.3:
  - `ReplFrames { epoch: u64, base_seq: u64, frames: [][]u8 }` (leader -> follower)
  - `ReplAck { epoch: u64, confirmed_seq: u64 }` (follower -> leader)
- AUTH: the replication stream authenticates the peer as a replica (reuse `ReplClient.authenticate`);
  a non-replica peer is refused. See section 6 (security) for mutual TLS.
- APPLY, per received `ReplFrames`:
  1. FENCE: if `epoch < self.max_epoch_seen` -> reject (send `ReplAck{epoch=self.max_epoch, ...}`,
     do not apply). If `epoch > self.max_epoch_seen`, adopt it (a promotion happened) and persist it.
  2. ORDER: require `base_seq == self.confirmed_seq + 1`; a gap -> request resync from `confirmed_seq`
     (the leader re-ships from `ReplWal.readFile` at that seq). Never apply out of order.
  3. INTEGRITY: each frame is length-prefixed and CRC/checksummed (frames are the leader's committed WAL
     records); a checksum mismatch -> reject the batch, do not apply, log + alert. Corruption never
     silently applies.
  4. APPLY: reuse the crash-recovery per-record apply logic. DONE (2026-08-01, R1 step 1): recover()'s
     Phase 1/2 bodies are extracted into `Database.applyCatalogRecord` + `applyDmlRecord`, and a
     `Database.applyStream(records)` follower entry point drives them (single apply path).
     >>> FINDING + FIX (2026-08-01): "reuse the local recovery path" was NOT sufficient by itself.
     Local recovery works because the WAL and the db file are the SAME node's pages; applying shipped
     records to a FRESH follower failed (`InvalidChecksum ... beyond_eof`) because the catalog records
     carry the LEADER's root PAGE IDs, absent in the follower's independently-laid-out file. RESOLVED
     via LOGICAL apply (see follower-apply-fork.md, option A): `applyStream` intercepts table-defining
     records (`sys.tables`) and RE-CREATES the table locally via `Database.applyTableCreateFromLeader`
     -> `createTable`, which allocates a FRESH root and writes a self-consistent catalog; the leader's
     raw sys.objects/sys.columns/sys.users records are NOT applied verbatim. DML then applies to the
     follower's own tree. VERIFIED: the two-node harness (root.zig "replication: two-node apply
     consistency") is now GREEN -- leader writes -> shipped records -> follower builds its own tables
     -> identical query results (24/24 tests pass). Remaining: index re-creation on the follower and
     update/delete replication (currently insert-focused); tracked as follow-ons.
  5. PERSIST + ACK: advance `confirmed_seq`, `ReplWal.checkpoint(confirmed_seq)` (crash-safe resume
     point), send `ReplAck{ epoch, confirmed_seq }`.
- RESTART: on boot a follower `loadCheckpoint()` -> `{ file_seq, confirmed_seq }`, connects, and the
  leader re-ships from `confirmed_seq + 1`. So a follower restart resumes exactly, no full resync.
- READ-ONLY: a follower rejects client WRITES (it is not the leader); reads are served from its
  byte-consistent replica (bounded-stale, see RPO below).

## 4. Fencing (close split-brain)

- The leader-lease row (section 7) carries a monotone `epoch`. Winning/renewing the lease bumps it.
- The leader stamps `epoch` on every `ReplFrames` AND on every local commit record.
- Followers and the store persist `max_epoch_seen` and REJECT any lower-epoch frame or write.
- Result: a partitioned OLD leader (still thinks it is leader) has a STALE epoch; once a new leader has
  bumped the epoch and any follower has seen it, the old leader's frames are rejected -- independent of
  lease TTL or clock. This is the difference between "documented split-brain window" and "closed".
- Clocks: leases use time for LIVENESS only. SAFETY (no two committers) rests on the epoch, not clocks.
  Must hold under injected clock skew (test matrix, section 8).

## 5. Bounded RPO (quorum-ack, opt-in)

- Default stays ASYNC (ship without waiting) -- fine for most config writes; state the async RPO as
  "bounded by replication lag" and monitor lag = leader LSN minus min follower confirmed_seq.
- Add a per-commit DURABLE flag: when set, the leader does not ack the client until N/2+1 followers'
  `ReplAck.confirmed_seq` covers the commit's seq. That gives RPO=0 for those writes across a leader
  kill. The config store uses DURABLE for lease/CAS and workload-spec writes; bulk/unimportant writes
  stay async.
- Mechanism: the commit path registers a waiter keyed by target seq; incoming `ReplAck`s satisfy waiters
  when the quorum threshold is reached; a timeout returns an error the client can retry.

## 6. Security on the wire

- The replication stream (ship/apply) and client connections run over mutually-authenticated TLS using
  the pure-Nova TLS 1.3 stack. No plaintext WAL frames; a wrong-identity or unauthenticated peer is
  refused before any frame is applied.
- The replica auth (`ReplClient.authenticate`) proves the peer is an authorized replica; TLS proves the
  channel. Both are required for the DURABLE/quorum path to be trustworthy.

## 7. Leader-lease CAS primitive

- NovaDB exposes a compare-and-set on a well-known row: `casRow(key, expected_rev, new_value) ->
  bool`. Exactly-one-writer election is then an orchestrator policy on top (a lease row with a TTL and
  the fencing `epoch`); NovaDB only needs the atomic CAS + the epoch bump on successful acquire.
- The CAS write is a DURABLE (quorum-acked) write so a promoted leader cannot lose the lease it just won.

## 8. Backup and restore (PITR)

- BACKUP: a consistent snapshot = a checkpoint (the flushed page image at a known LSN) PLUS the WAL tail
  from that LSN. `ReplWal` already segments + checkpoints; extend with an `exportSnapshot(dir)` that
  copies the checkpoint image + WAL segments >= checkpoint LSN to an off-box target atomically.
- RESTORE: load the checkpoint image, then replay WAL to a target seq (`replayTo(seq)` -- the same apply
  path as section 3.4). Point-in-time = stop replay at the chosen seq.
- This doubles as disaster recovery for the single-node case, independent of replication.

## 9. Phased plan (maps to the orchestrator doc's P-phases)

- R0 (orchestrator P0): durability close-out already largely done; confirm undo-log rebuild on boot and
  make sync-commit the default for the config path. Gate: crash tests green (they are: sync/async
  200/200 per readiness plan).
  STATUS (2026-08-03): DONE. synchronous_commit promoted to a first-class yaml config
  (Config.durability.synchronous_commit; main.zig reads it as the default, SYNCHRONOUS_COMMIT env overrides);
  the orchestrator config-store deployment runs it true. Undo-rebuild-on-boot CONFIRMED by a new mid-write
  crash harness (tests/harness/crash_test_midwrite.sh): kill -9 WHILE inserts stream so a txn is in flight,
  recover() Phase 3 reverts the active txn and every observed-acked row survives with a clean recovery; the
  existing clean crash_test.sh also passes. Both harnesses had a stale SELECT COUNT(*) check (the SQL parser
  has no aggregates) -- now verified by point-lookup.
- R1 (P2): follower/apply + ack + checkpoint-resume; wire ship on leader and apply on follower into
  db open/close. Gate: two-node write -> appears on follower -> follower restart resumes from checkpoint,
  byte-consistent.
- R2 (P3): fencing epoch on lease + frames + local commits; reject-lower everywhere. Gate: kill leader,
  promote follower, artificially unpause old leader -> its frames/writes are REJECTED (proven).
- R3 (P5): quorum-ack durable write path + per-write flag; state RPO. Gate: kill leader right after a
  durable ack -> promoted follower HAS the write (RPO=0).
  STATUS (2026-08-03): DONE. DurableReplicator (replication.zig) is the leader-side durable path, live-wired
  into the executor's commit: it buffers every logged record via the WAL ship_callback (catching CATALOG
  writes, which never pass through the executor's logWalRecord -- an executor-level buffer shipped DML without
  its schema and the follower could not apply it), assigns a contiguous replication seq decoupled from the WAL
  lsn (open consumes lsn 1, so raw lsns start at 2 while a fresh follower expects base_seq 1), ships the
  closed txn via the tested shipAndRecord path feeding the QuorumTracker, and -- for a write flagged durable
  -- awaitQuorums the batch's last seq before the client is acked. Per-write flag: "SET DURABLE COMMIT ON|OFF"
  control command (executor), mirroring SET FENCE EPOCH; Database.becomeDurableLeader(host,port,N,epoch,tmo)
  registers it. Gate proven by two in-process real-socket tests (root.zig): "P5: executor durable commit is
  quorum-acked end to end (RPO=0)" -- the follower ALREADY holds the row when the leader returns (a promoted
  node queried directly has it), and "P5: durable commit FAILS the write when no quorum is reachable" -- a
  stopped follower makes the durable write return an error (client retries), never a false ack. Orchestrator
  opt-in: SqlConfigStore.setDurable(on). RPO stated: durable writes RPO=0; async writes bounded by lag.
- R4 (P6): mutual TLS + replica authz on the stream. Gate: unauthenticated/wrong-cert peer refused.
  STATUS (2026-08-03): REPLICA AUTHZ DONE; mutual TLS remaining. A challenge-response HMAC handshake now runs
  FIRST on the replication stream (replication.zig serverAuthenticate/clientAuthenticate): the ReplServer
  sends a fresh 32-byte nonce, the peer answers HMAC-SHA256(shared_key, nonce), the server constant-time
  verifies and sends a 1-byte ACK; a wrong/missing proof closes the connection so the client's connect fails
  with error.AuthFailed BEFORE any frame is read or applied. Wired through auth_key: ReplServer.init /
  DurableReplicator.init / Database.becomeFollower / becomeDurableLeader (empty key = disabled, back-compat);
  main.zig passes config.replica.key. Test "P6: replica auth handshake" proves matching-key accepted +
  wrong-key refused (35/35). The nonce defeats replay and the secret stays off the wire, so this holds even
  before TLS. MUTUAL TLS: DONE (2026-08-03). The replication stream is wrapped in TLS 1.3 with client-cert
  auth: ReplServer.handleReplConnInner does a TLS-vs-raw split (mirroring tcp_server's runLoop) --
  `tls.serverFromStream` with `auth = server cert`, `client_auth = { root_ca, .require }` -- so the follower
  presents its cert and REQUIRES + verifies the leader's cert against the shared CA before the HMAC handshake
  or any frame. The leader (ReplClient.upgradeTls) holds a PERSISTENT tls.Connection (a heap TlsClientHolder
  owns the raw stream reader/writer + buffers + the tls.Connection at stable addresses, since clientFromStream
  inlines its buffers into a transient frame); shipFrames/authenticateStream route through conn.writeAll (which
  auto-flushes via encryptWrite) / conn.readAll when TLS is active, else the raw stream. Threaded a TlsConfig
  (ca/cert/key + verify-host) through ReplServer / DurableReplicator / becomeFollower / becomeDurableLeader
  (empty ca = disabled, back-compat). Test "replication P6: mutual TLS" -- a valid cert chain ships over the
  encrypted channel + the follower has the row; a ROGUE client (cert chaining to a different CA) is REFUSED at
  the handshake so becomeDurableLeader errors and no replicator is set up. 37/37 tests, no new leaks. GOTCHA:
  Zig verifies an IP-literal host as a DNS name, so the client's verify-host is "localhost" (a DNS SAN on the
  server cert), not "127.0.0.1". Cert fixtures + regen steps in testdata/repl. --- (groundwork history below)
  De-risked the `tls` module API and shipped
  hermetic cert fixtures (testdata/repl: a test CA signing server + client certs, plus a rogue CA for the
  negative path). Confirmed the pieces: server opts `tls.config.Server{ .auth = server CertKeyPair,
  .client_auth = { root_ca, .require }, .rng, .now }`; client opts `tls.config.Client{ .host, .root_ca,
  .auth = client CertKeyPair, .rng, .now }`; certs via `CertKeyPair.fromFilePath` + `cert.fromFilePath`;
  `rng` = seeded `std.Random.DefaultCsprng`; `now` = `Io.Clock.real.now(io)`; the wrap pattern is
  `tcp_server.zig`'s runLoop split (`tls.serverFromStream` -> use `conn.reader()/writer()`). REMAINING
  integration: (1) ReplServer.handleReplConnInner picks TLS-vs-raw reader/writer like runLoop, running the
  HMAC handshake + frame loop over the TLS connection; (2) ReplClient holds a PERSISTENT tls.Connection --
  the current shipFrames makes a fresh reader/writer per call, which would bypass TLS, so the raw
  reader/writer + tls.Connection + its reader/writer must live at stable addresses across ships (a
  heap-owned holder); (3) thread a TlsConfig (ca/cert/key paths) through becomeFollower/becomeDurableLeader
  like auth_key; (4) a mutual-TLS replication test (matching certs ship; rogue-CA client refused). Regenerate
  the fixtures (P-256, 100y):
  ```
  openssl ecparam -name prime256v1 -genkey -noout -out ca.key
  openssl req -x509 -new -nodes -key ca.key -sha256 -days 36500 -subj "/CN=btree-repl-CA" -out ca.crt
  openssl ecparam -name prime256v1 -genkey -noout -out server.key
  openssl req -new -key server.key -subj "/CN=btree-follower" -out server.csr
  printf "subjectAltName=IP:127.0.0.1,DNS:localhost\n" > san.ext
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 36500 -sha256 -extfile san.ext -out server.crt
  openssl ecparam -name prime256v1 -genkey -noout -out client.key
  openssl req -new -key client.key -subj "/CN=btree-leader" -out client.csr
  openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial -days 36500 -sha256 -out client.crt
  ```
  CONFIG-API AUTHZ (P6, btree tier) ALSO DONE: the SQL RBAC model (users/roles/grant/revoke/login +
  per-object checkObjectPermission + session tokens) was already enforced in the executor for
  SELECT/INSERT/UPDATE/DELETE (no token when security on + users exist => "Authentication Required";
  read_only role => "Permission Denied" on writes, SELECT allowed). NEW: the control commands SET FENCE
  EPOCH / SET DURABLE COMMIT were intercepted BEFORE that block and bypassed auth -- an unauthenticated peer
  could fence the store (DoS) or downgrade durability; adminGate now requires an ADMIN session for both (test
  "P6 config-API authz: control commands require an ADMIN session"). REMAINING for the orchestrator to USE
  it: thread login/session-token through the Nova db.Connection seam (driver connect(dsn) carries no
  credentials today) so the config store authenticates as the orchestrator identity -- a driver-layer follow-on.
- R5 (P7): backup/restore (exportSnapshot + replayTo). Gate: backup/restore round-trips byte-for-byte;
  STATUS (2026-08-03): FULL BACKUP/RESTORE DONE; PITR-to-intermediate remaining. Database.exportSnapshot(dir)
  flushes the WAL buffer + all dirty pages + fsync (a coherent checkpoint) then copies the full page image ->
  dir/snapshot.db + the WAL segments -> dir/wal/. Database.restoreSnapshot(alloc, io, dir, db_path, wal_dir)
  is standalone (db not open): copies snapshot.db -> db_path (byte-for-byte) AND the bundled WAL -> wal_dir;
  opening db_path with that wal_dir runs recover(), which REBUILDS the committed-transaction set from the WAL
  commit records so the restored rows are MVCC-VISIBLE (their xmin is the original tx_id -- invisible without
  this, the same reason the follower logs commit records). Test "P7 backup/restore": 50 rows -> exportSnapshot
  -> restoreSnapshot into fresh paths -> the db file byte-equals the snapshot image (checked before open) AND
  every row is present after open. 38/38, no new leaks. PITR DONE (2026-08-03): recoverTo(target_lsn) is the
  bounded WAL replay (records with lsn > target are ignored, so a txn committing past the target is excluded)
  and Database.openAt(path, pool, wal_dir, target_lsn) opens replaying only up to the target; recover() =
  recoverTo(0) (unbounded, unchanged). Point-in-time restore = restoreSnapshot the EARLY checkpoint page image
  + bring the ARCHIVED (full) WAL, then openAt(target). Test "P7 PITR": insert rows 1..10 taking the base image
  after row 5, then openAt(row-7-commit-lsn) over the full WAL -> rows 1..7 present (rows 1..5 idempotently
  re-applied on the base, 6..7 rolled forward), rows 8..10 EXCLUDED (committed after the target). 40/40, no new
  leaks. REMAINING P7: rolling upgrade + membership + runbooks.
  PITR stops at target seq.
- LIVE SHIPPING (2026-08-03): a running LEADER process now ships committed writes to a running FOLLOWER
  process over the network. main.zig wires it: a follower (PRIMARY=false) listens on the replica port; a
  leader (PRIMARY=true + REPLICA_ENABLED) becomeDurableLeader-connects to the follower and every committed
  write ships via the executor's durableFinish (async by default; SET DURABLE COMMIT ON makes it
  quorum-blocking). Topology is set by ENV VARS (PRIMARY/REPLICA_ENABLED/REPLICA_PORT/REPLICA_ADDRESS/
  HTTP_PORT) because the yaml parser does not reliably map nested keys (replica.port leaked into the
  top-level port; replica.address came back empty). Async ship failures no longer fail the client write
  (committed locally; the follower resyncs) -- only durable writes fail on no-quorum. Harness
  tests/harness/live_repl_test.sh: start follower, start leader, write on the leader over HTTP, read the row
  back FROM THE FOLLOWER -> PASS. This unlocks the P8 live kill-follower / partition harnesses.
- RECONNECT + CATCH-UP (2026-08-03): the leader now survives a follower drop/restart. The replication seq
  advances for EVERY committed txn (a missed ship is a DETECTABLE gap, not a silent skip); each shipped batch
  is RETAINED in a bounded in-memory ring (MAX_RETAINED); a dead follower is detected fast via SO_RCVTIMEO/
  SO_SNDTIMEO (3s) instead of blocking the write path; and on the next write reconnectAndCatchUp reconnects,
  probes the follower's confirmed_seq (a heartbeat), and re-ships every retained batch beyond it, in order.
  An async ship failure no longer fails the client write (committed locally, backfilled later); a durable
  write still fails on no-quorum. If the follower fell farther behind than the retained ring, it logs that a
  full snapshot restore is required. VERIFIED: happy-path live shipping (live_repl_test.sh PASS) + unit suite
  40/40. NOT-YET-RELIABLY-VERIFIED: the gap-backfill end-to-end -- tests/harness/kill_follower_test.sh
  exercises it (kill follower, write during downtime, restart, verify catch-up) but is orchestration-flaky
  (slow multi-process timing + port reuse); an in-process ReplServer-restart test pollutes the shared test
  io and was dropped. FOLLOW-ON: a hardened, deterministic backfill harness. Remaining beyond that: a WAL/
  ReplWal-backed retained log (unbounded backfill) + snapshot-transfer resync when the ring is exceeded.
- R6 (P8): chaos + soak -- kill-mid-apply, partition old leader, corrupt frame, disk-full on commit,
  clock skew. Gate: zero committed-config loss beyond stated RPO, zero split-brain writes, recovery
  within stated RTO -- reported as numbers.
  STATUS (2026-08-03): CHAOS SUITE STARTED -- most scenarios proven, reported as numbers. tests/harness/
  chaos_suite.sh aggregates the SUBPROCESS kill scenarios (2/2 PASS): kill-leader-after-load (RPO=0, every
  sync-committed row survives kill -9) + kill-leader-mid-write (the active-txn/undo path, observed-acked rows
  survive). IN-PROCESS scenarios proven as Zig tests: NEW "P8 chaos: a corrupted shipped frame is detected
  and never applied" (flip a byte -> per-frame CRC32 mismatch -> deserialize rejects the batch before
  recvFrames, so a valid follower keeps only the good row); fenced old leader's writes rejected ("store-side
  write fencing"); follower re-ships from the last durable point after a mid-apply crash (applyStream
  durableFlush before confirmed_seq advances, P2). CLOCK-SKEW SAFETY proven (orchestrator test 188
  test_clock_skew_does_not_break_fencing): an old leader with a wildly skewed-behind clock -- so by its own
  clock the lease is not expired -- is STILL fenced, because renew rejects on epoch mismatch, not time.
  STATED: RPO=0 for sync/quorum-acked writes; RTO = lease TTL + one reconcile tick. REMAINING: subprocess
  kill-follower-MID-APPLY proof + partition-old-leader live-two-node harness (structurally covered, not yet a
  scripted kill); disk-full-on-commit injection; a multi-hour SOAK under a write workload published in CI.

## 10. Test matrix (each an automated case)

- two-node ship/apply consistency under a write workload.
- follower restart resumes from `CheckpointRecord` (no full resync, no gap).
- leader-kill no-loss for DURABLE writes (RPO=0); bounded lag reported for async.
- FENCED old-leader: a stale-epoch frame/write is rejected after promotion, under injected clock skew.
- corrupt shipped frame -> detected by checksum, rejected, NOT applied.
- backup -> restore byte-identical; PITR replay stops at target seq.
- disk-full on commit -> leader fails the write cleanly (no torn state), follower unaffected.

## 11. What exists vs what to build

| Piece | Exists | To build |
|---|---|---|
| Ship side | ReplWal, ReplClient.ship, auth, reconnect, CheckpointRecord, WAL callback hook | -- |
| Follower/apply | DONE: R1 applyStream (catalog+index+ins/upd/del); R2-a Follower.recvFrames validate+apply+checkpoint+ack; R2-b ReplServer+ReplClient over a socket; P2 LIFECYCLE Database.becomeFollower on open (main.zig starts it when !config.primary) + clean teardown; follower RESTART-DURABILITY fixed (applyStream logs a commit record per applied txn so recover() restores MVCC visibility of the durable rows) -- restart test asserts row survives; CRASH-SAFETY: applyStream durableFlush (header + pages) before confirmed_seq advances, so a crash re-ships from the last durable point | subprocess kill-mid-apply proof (P8 chaos); auth/mTLS is R4 |
| Fencing | R2-a: epoch on ReplFrames; Follower persists max_epoch_seen + rejects lower-epoch frames; ReplAck carries higher epoch so a fenced leader learns it lost (proto.zig ReplFrames/ReplAck, binary + CRC32). R2 store-side (done): Database.fencing_epoch + max_epoch_seen (persisted base_dir/fence_epoch.json); setWriteEpoch/observeEpoch/guardWrite; the executor rejects MUTATIONS below the high-water (reads/login/txn-boundaries pass), so a fenced node serves reads only | leader-side epoch lives with the lease policy (orchestrator LeaderLease, done); optional: stamp epoch INTO commit WAL records so a follower re-applying also enforces it |
| Quorum-ack (RPO=0) | DONE: QuorumTracker + awaitQuorum + R3-b shipAndRecord, now LIVE-WIRED as DurableReplicator into the executor commit (buffers via WAL ship_callback incl. catalog; contiguous repl seq decoupled from WAL lsn; "SET DURABLE COMMIT ON/OFF" per-write flag; becomeDurableLeader). Two real-socket tests: RPO=0 (follower has the durable row when the leader returns) + no-quorum-fails-the-write. SqlConfigStore.setDurable(on) opt-in | -- |
| Security | replica auth exists; pure-Nova TLS exists | mutual TLS on the replication + client streams |
| Leader-lease | nothing | casRow CAS primitive + epoch bump (policy in orchestrator) |
| Backup/restore | WAL segments + checkpoint | exportSnapshot(dir) + replayTo(seq) |
| Chaos/soak proof | nothing | the section 10 matrix, reported as numbers |
