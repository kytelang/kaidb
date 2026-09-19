# Concurrency ceiling: the global write lock (D5)

## The ceiling

NovaDB serialises writers with a single database-wide lock, `Database.rw_lock` (`src/schema/
database.zig`). Reads can proceed concurrently (MVCC gives each reader a consistent snapshot), but
every mutating path (insert, update, delete, DDL, checkpoint-affecting work) takes this one lock. In
practice that caps useful write concurrency at roughly **5 writer threads**: beyond that, threads spend
their time queued on the lock rather than doing work, so more threads do not buy more write throughput.

This is a deliberate, documented limit, not a bug. The single lock is **load-bearing for correctness**:
it is what makes the B+Tree structure modifications, the WAL append order, and the MVCC/undo bookkeeping
consistent with each other without a finer-grained latching protocol. Removing it is not a deletion; it
is a rewrite (see below).

## What it is fit for

- **A configuration store / control-plane store** (the orchestrator's use): small, read-mostly, low
  write-concurrency. The ceiling is irrelevant here because the workload never approaches it. This is
  the intended production shape today.
- **Read-heavy workloads generally**: reads are MVCC snapshots and do not contend on the write lock, so
  read scaling is not bounded by this ceiling.
- **Embedded / single-writer** use: a single application thread (or a handful) writing is entirely
  within budget.

## What it is NOT fit for (yet)

- **Write-heavy, highly-concurrent OLTP**: many independent writer threads hammering different keys.
  Because they all serialise on one lock, throughput plateaus around the ~5-thread mark regardless of
  core count. If your workload is dozens of concurrent writers sustaining high write rates, this engine
  will not scale to it today.
- **Fan-in ingestion** from many concurrent producers writing continuously.

## The path past it (measurement-gated, not started)

The way to lift the ceiling is **per-page latch coupling** (a.k.a. lock coupling / crabbing): a
writer latches only the B+Tree pages on its path, hand-over-hand, plus a WAL append serialisation that
is narrower than a whole-database lock. That is a real concurrency-control rewrite of the storage and
WAL layers, with its own correctness burden (deadlock avoidance ordering, latch upgrade/downgrade, WAL
LSN assignment under finer locks).

Per the production-readiness plan, this work is **only justified once a measured workload demands it**.
Adding fine-grained latching speculatively trades a large, bug-prone rewrite for throughput that a
config-store deployment never needs. The decision gate is a benchmark showing write throughput bounded
by lock contention on a target workload, not by disk or CPU. Until that measurement exists, the correct
engineering choice is this documented ceiling.

## If you hit it

- Confirm it is the lock, not I/O: if write throughput is flat as you add writer threads while CPU and
  disk are not saturated, you are on the ceiling.
- Shard at the application tier (multiple database instances, one per partition) as an interim scale-out
  that keeps each instance within its write-concurrency budget.
- Otherwise, this is the signal that funds the latch-coupling rewrite described above.
