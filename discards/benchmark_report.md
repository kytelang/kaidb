# YCSB Benchmark Suite 10-Iteration Average Report

- **Record Count:** 10000
- **Operation Count:** 10000
- **Iterations:** 10

| Workload | Description | Mix | Throughput (ops/sec) | Avg Latency (us) | Min Latency (us) | Max Latency (us) | p95 (us) | p99 (us) |
|---|---|---|---|---|---|---|---|---|
| Workload A | Update Heavy | 50R/50U | 11016.4 | 93.0 | 28.6 | 60747.0 | 97.8 | 133.0 |
| Workload B | Read Mostly | 95R/5U | 10818.5 | 93.7 | 28.0 | 74568.2 | 78.2 | 126.2 |
| Workload C | Read Only | 100R | 10315.9 | 97.6 | 28.6 | 81549.6 | 73.6 | 111.0 |
| Workload D | Read Latest | 95R/5I | 9322.1 | 111.2 | 28.6 | 81538.4 | 104.4 | 261.6 |
| Workload E | Short Ranges | 95S/5I | 7762.4 | 136.6 | 39.6 | 213459.6 | 157.2 | 441.6 |
| Workload F | Read-Modify-Write | 50R/50RMW | 7306.5 | 137.8 | 29.0 | 66254.8 | 174.2 | 261.4 |

---

# SQL vs Document (NoSQL) mode, 2026-08-30

The same YCSB harness now drives NovaDB two ways over the one binary protocol on
port 3009:

- **SQL mode** (`workload-*`): each operation is an SQL statement (`INSERT` /
  `SELECT ... WHERE id = ...` / `UPDATE`) parsed and executed by the query
  engine against a relational table (`id` primary key plus `field0..field9`).
- **Document mode** (`document-*`): each operation is a binary `doc_op` frame
  (BSON document keyed by a 12-byte `_id`) against a collection, the same path
  the Nova document driver uses. Reads and updates are by `_id`; scans page the
  `_id`-ordered B+Tree with a cursor.

Both modes share the same storage engine (slotted-page B+Tree, buffer pool, WAL,
MVCC); only the request shape and the access path differ, so the comparison
isolates the cost of the SQL layer versus the document layer.

### Method

- 20,000 records, 20,000 operations, Zipfian key distribution, `scan_length=10`.
- One client, one connection, sequential operations over the loopback interface.
  These are single-client latency-bound figures (they characterise per-request
  cost and the access path), not the server's saturated multi-client ceiling.
- Server and harness both built `ReleaseFast`; same host; warm cache.
- YCSB integer keys map to deterministic ObjectIds (key in the low 8 bytes), so
  document reads/updates hit a real primary-key lookup and scans are ordered.

### Results

All six standard YCSB workloads (A-F), with **both optimisation passes applied**
(version-chain fast-path + projection pushdown on the engine, binary transport on
the SQL client). Each figure is a steady-state run of 200,000 operations against
a **freshly restarted server** with a 20,000-record store, so there is no
cross-workload degradation (see the `DROP TABLE` caveat below); this is the clean
apples-to-apples set and the numbers to trust:

| Workload | Mix | SQL ops/sec | Document ops/sec | Document / SQL |
|---|---|---|---|---|
| A | 50% read, 50% update | 11,010 | 30,488 | 2.77x |
| B | 95% read, 5% update | 29,373 | 44,277 | 1.51x |
| C | 100% read | 26,965 | 45,600 | 1.69x |
| D | 95% read, 5% insert (latest) | 28,798 | 45,704 | 1.59x |
| E | 95% scan, 5% insert | 5,860 | 28,118 | 4.80x |
| F | 50% read, 50% read-modify-write | 10,201 | 24,015 | 2.35x |

(The document update was aligned to rewrite all ten fields, matching the SQL
`UPDATE ... SET field0..field9`, so A and F compare like for like.)

**Why the ratio is not constant across workloads.** The read-heavy workloads
(B/C/D) cluster tightly at **1.5-1.7x**: that is the steady relational-read
overhead (parse, seek, decode typed columns, build a row value) over an
opaque-BSON keyed read. A, F and E sit higher because the relational *update* and
*scan* paths are proportionally heavier than their document counterparts, and
this is a real engine property, not a harness artifact (verified: both modes
rewrite all ten fields on update, and SQL update throughput is independent of
field size, which rules out SQL-text parsing as the cause):

- **Update cost, per operation:** a SQL update is about **3.7x a SQL read**
  (~37us -> ~137us): plan the `UPDATE`, seek, read and decode the current row,
  build the new row, then write a new MVCC version + undo record + WAL. A document
  update is about **2.3x a document read** (~22us -> ~50us): overwrite the doc
  bytes + WAL. So the 50%-update workloads (A, and F via read-modify-write) drag
  down more on the SQL side.
- **Scan (E):** SQL decodes and projects each row (`SELECT id, field0 ... WHERE id
  >= ? LIMIT 10`) and builds a row value per row, whereas the document cursor
  walks the `_id`-ordered tree and returns opaque BSON pages. Scanning is where
  the document model wins most, hence the ~4.8x.

In short: reads are proportionate; updates and scans are where a relational engine
inherently does more per operation than a document store. The next levers to
narrow A/F/E specifically are the SQL write path (lighter MVCC versioning on
update) and scan projection, both larger engine changes.

For reference, the pre-optimisation SQL numbers on the same fresh-server method
were roughly C 18,100, B ~18,000, D ~17,400, so the two passes lifted read-heavy
SQL by around 50-60%.

### Reading the numbers

- **Document mode is faster per request in every workload.** The dominant reason
  is that an SQL operation re-parses statement text and does relational
  type-coercion on each call, while a document operation ships pre-encoded BSON
  straight to a keyed B+Tree lookup. On the write/load path that parsing cost is
  largest, which is why document load is roughly 5x the SQL load rate.
- **Point reads/updates are primary-key lookups in both modes**, so the gap
  there (C, A) is the SQL-layer overhead, not an algorithmic difference.
- **Scans (E):** the document cursor walks the `_id`-ordered B+Tree directly; the
  SQL scan filters and projects columns per row. The 5x gap reflects that extra
  per-row relational work.

### A real fix this benchmark surfaced

The first document run was pathological: workload C fell from ~8,700 ops/sec at
200 records to **374 ops/sec at 20,000** (P50 1,293 us), i.e. latency grew with
collection size. Cause: a `find` / `find_one` whose filter pins `_id` to a
concrete ObjectId (`{_id: <oid>}`, the ordinary point read) was running a full
collection scan instead of a keyed lookup, even though the collection is
physically keyed by `_id`. A planning fast-path in `DocumentStore.candidateIds`
now recognises an `_id`-equality filter and restricts the candidate set to that
single B+Tree key (an `_id` bound to an operator document such as
`{_id: {$gt: ...}}` is not an equality and still falls through to the normal
path; `filter.matches` re-checks every predicate on the one candidate, so this
is a pure planning change). Point reads went from O(n) to O(log n): workload C at
20,000 records went **374 -> 33,222 ops/sec** (P50 1,293 us -> 19 us). Update and
delete by `_id` share the same `find` path and got the same speed-up.

### SQL read-path optimisation (2026-08-30)

Profiling the SQL point-read workload (macOS `sample` on the server during a long
read phase) showed the cost was in the storage/MVCC layer, not SQL parsing or
JSON: `reconstructVersionChain` allocating a `DecodedVersion` array plus two
~1KB `fixed`/`heap` copies on every read, and `getVisibleVersion` materialising
**all** columns of the row even though `SELECT id, field0` needs two. Two
changes, both verified against the full 159-test suite (159/159):

1. **Version-chain read fast-path** (`getVisibleVersion`): for the common case of
   a single committed version that predates every active transaction, decode the
   visible row straight from the page bytes with borrowed slices, skipping the
   per-read chain array and the two row-sized copies. Mirrors the existing guard
   in `reconstructVersionChain`; anything else falls through unchanged.
2. **Projection pushdown** (`collectNeededCols` + `scan_needed_cols`): for a
   simple single-table read, decode only the columns the plan actually reads
   (projection ∪ WHERE ∪ ORDER BY). Allocation-free (a fixed inline buffer, no
   per-query heap set, since a heap set per query costs more than it saves for a
   one-row point read), and it falls back to reading all columns for anything it
   cannot fully analyse (joins, aggregates, `*`, functions, subqueries), so
   correctness never depends on it.

Effect (isolated steady-state, 20,000 records, 300,000 ops for the read-only
case): workload C ~18,100 -> ~21,600 ops/sec (about +19%). The scan and
read-latest workloads gain most from decoding 2 of 11 columns per row: in a full
A-F suite run, E rose ~1,800 -> ~4,200 ops/sec and D ~11,500 -> ~19,400. Point-read
throughput on the loopback is noisy (±20% between runs), so treat the read-only
figure as "clearly improved, roughly a fifth" rather than an exact number. The
document path was unchanged.

The document mode is still faster in absolute terms: its storage model is an
opaque BSON blob keyed by `_id`, so a read is one keyed lookup and a byte
passthrough, whereas SQL must still decode typed columns and build a row value.
These two changes narrow the gap by removing wasted per-read work on the SQL side.

3. **Binary transport for the SQL client (fair comparison).** The SQL harness was
   using NovaDB's legacy JSON packet protocol (the request is a JSON document the
   server parses with `std.json`, the reply is JSON text it serialises), while the
   document harness used the binary wire protocol. Profiling put that JSON
   serialise/parse at roughly a fifth of the server's per-query cost, and it was
   an unfair thumb on the scale: the two modes were not speaking the same
   protocol. Switching the SQL client to the binary `query` frame (the same
   negotiated connection the document client uses; rows return as typed
   `data_row` frames, not JSON) is a client-only change, no engine risk.

   Effect: SQL read-only C went ~20,800 -> ~27,300 ops/sec (another ~+31%), so the
   cumulative gain over the original JSON-transport baseline is **18,100 ->
   27,300, about +50%**. The gap to document C (~33,000) narrowed from ~1.8x to
   ~1.2x, and it is now a like-for-like binary-vs-binary comparison. The residual
   ~1.2x is the genuine engine difference: SQL parses statement text and decodes
   typed columns into a row value; the document path ships opaque BSON.

**A measurement caveat worth recording.** All the reliable figures above are
*isolated* fresh-server runs. Running the workloads back to back degrades
throughput badly, and it does not recover within the process: repeated
`DROP TABLE` + `CREATE` + reload of the same table roughly halved read throughput
(~20,800 -> ~10,500 for C on the JSON build), which is why the in-suite numbers
sit well below the isolated ones. That points at page/space reclamation after
`DROP TABLE` (freed pages not being reused, so the file grows and the cache
thrashes) as a separate robustness issue, distinct from per-query speed. Treat
the isolated numbers as the real per-operation performance.

### How to reproduce

```bash
# 1. build + start the server (from novadb/)
zig build -Doptimize=ReleaseFast
rm -rf data nova.db && ./zig-out/bin/novadb &

# 2. build the harness (from novadb/benchmark/ycsb/)
zig build -Doptimize=ReleaseFast

# 3. run both modes at the same scale
./zig-out/bin/ycsb workload-all  --record_count=20000 --operation_count=20000  # SQL A-F
./zig-out/bin/ycsb document-all  --record_count=20000 --operation_count=20000  # document A-F
# or a single workload either way, e.g. document-b / workload-e
```
