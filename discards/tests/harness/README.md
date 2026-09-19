# NovaDB correctness harnesses

> **MANUAL ONLY -- NOT A MERGE GATE.** These are live, multi-process, timing-sensitive harnesses. They are
> run by hand to *confirm* behaviour against real processes; they do NOT gate a merge (their races would
> turn a green change red at random). Every guarantee they check has a deterministic in-process equivalent
> in `zig build test` -- see the audit in the monorepo `TEST-STRATEGY.md`. `gate.sh` does not run these.

Black-box harnesses that drive a running `btree` server over its HTTP/JSON endpoint
(`POST http://127.0.0.1:3008/query`) and verify engine correctness end-to-end. They
complement the in-process Zig unit tests (`zig build test`) by exercising durability
and concurrency the way real clients do.

Build the server first: `zig build` (produces `zig-out/bin/btree`).

## ⚠️ Build ReleaseFast for ANY performance measurement

```
zig build -Doptimize=ReleaseFast
```

Plain `zig build` is a **Debug** build, and `main.zig` then uses `std.heap.DebugAllocator`, which captures a
**full stack-trace unwind on every allocation** and serialises **every alloc/free behind one global mutex**
(release uses `c_allocator`). It is roughly **40x slower and destroys all concurrency scaling**, so debug
numbers are meaningless and actively misleading — an entire round of "the global lock is our bottleneck"
analysis turned out to be measuring the debug allocator. The 16x300 concurrency gate runs in ~116s debug vs
~1s release.

Debug builds are still the right choice for the *correctness* harnesses (allocator safety checks, leak
detection). Some bugs only appear in release, though — the query-cache use-after-free needed release speed
to reach its trigger — so run the harnesses **both** ways.

## crash_test.sh — durability under a hard crash
Loads N rows, `kill -9`s the server, restarts it, and checks that every committed
row survived recovery **and** that point-lookups work after the WAL-replay rebuild
(the recovery path resets user trees to a fresh root, replays the committed WAL, and
persists the final roots).

```
tests/harness/crash_test.sh              # SYNCHRONOUS_COMMIT=true, 200 rows
tests/harness/crash_test.sh true 1000    # 1000 rows
tests/harness/crash_test.sh false        # async-commit mode
```
Override the binary with `BTREE_BIN=/path/to/btree`. Manages its own temp dir + server.

## concurrency_stress.py — correctness under concurrent writers
Runs WORKERS parallel clients doing insert/update/delete over disjoint key ranges,
then verifies survivors == full-scan == COUNT(*) and that point-lookup matches the
scan for a sample (this is what catches internal-node routing corruption from unsafe
concurrent structure modifications).

Start the server yourself, then:
```
zig build && ./zig-out/bin/btree &           # in a scratch dir with db.json
python3 tests/harness/concurrency_stress.py 16 300
```

## throughput.py — read-vs-write scaling (perf, not correctness)

Measures how throughput scales with concurrency, using **keep-alive** connections and **process**-based
clients (a Python *threaded* client is GIL-bound past ~10k ops/s and flattens the curve no matter what the
server does). Start the server, then:

```
python3 tests/harness/throughput.py            # sweep 1,2,4,8,16
python3 tests/harness/throughput.py 1,8,32 --secs 5
```

The read-vs-write **split** is the diagnostic. Reads take `db.rw_lock` shared, writes take it exclusive:

* reads scale, writes flat (~1.0x) -> the global write lock IS the write ceiling
* reads and writes scale alike     -> the lock is NOT the ceiling; look elsewhere

As of 2026-07-15 (ReleaseFast, 8 cores): reads 2.3x / writes 1.9x at 16 workers — they track each other, so
the write lock is *not* the ceiling; both hit CPU saturation with the clients co-located on the same box.
Note the clients compete with the server for cores, so absolute numbers understate the server.

### Acceptance gate for locking changes
`concurrency_stress.py` MUST pass under the current global write-lock (baseline). It is
the guardrail for the P3 concurrency work (group-commit WAL, latch-safe SMOs, removing
the global lock): after any such change, this harness must still PASS. A regression here
means concurrent writes corrupted the B+tree. See `btree_readiness_plan.md` §7 / Phase 3.

Known baseline: 16 writers × 300 keys → exactly 3600 survivors, scan == count, all
point-lookups match.
