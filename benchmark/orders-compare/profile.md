# NovaDB (kaidb) Q1..Q10 profiling: where the time goes

Dataset: 1,000,000 rows, ReleaseFast server, single kaidb instance, loopback TCP,
Kyte driver (`kyte-kaidb`, simple-query `'Q'` path, binary result cells). Warm pass
(second run of each query, full client round trip with every result row decoded into
typed `DbValue`s).

## How this was measured

Two instrumentation seams, both env-gated so they cost nothing when off:

- **Server, per query** (`src/proto/session.zig`, `NOVADB_QEXEC=1`): `[QWIRE]` splits
  the server's own time into `exec` (parse + plan + storage scan + row build) and
  `encode+send` (RowDescription + DataRow wire encode + socket write).
- **Server, SELECT stage histogram** (`src/query/query_executor.zig`, `NOVADB_QPROF=1`):
  `[QPROF]` breaks `exec` down into `scan` (`base-search+decode` = B+Tree walk plus
  per-row cell decode from pages, and `buildJson` = row image build), `project`, and
  `finalize` (sort/limit).
- **Client, per query**: the harness warm timing is the full round trip. Because the
  network is loopback (sub-100 microsecond), `client_total - server_total` is
  effectively the Kyte driver's frame-read plus per-cell typed decode and ResultSet
  materialisation.

So: `client_total = server_exec + server_encode/send + (network ~= 0) + client_decode`.

## Per-query breakdown (warm, ms)

| Query | rows | client total | server exec | server encode+send | **client decode** | scan: base-search+decode | scan: buildJson | project |
|---|--:|--:|--:|--:|--:|--:|--:|--:|
| Q1 emp=279 LIMIT 10k | 10000 | 36 | 13.3 | 9.7* | ~13 | 9.2 | 0.9 | 2.5 |
| Q2 emp=279 AND total_due>10k | 10000 | 39 | 14.6 | 2.7 | ~22 | 10.6 | 1.0 | 2.5 |
| Q3 Q2 ORDER BY total_due DESC | 10000 | 44 | 22.6 | 2.5 | ~19 | 20.9 | 1.0 | 2.6 |
| Q4 total_due>50k LIMIT 5k | 5000 | 22 | 11.0 | 1.4 | ~10 | 9.0 | 0.5 | 1.2 |
| Q5 Q4 ORDER BY DESC | 5000 | 23 | 11.9 | 1.3 | ~10 | 9.9 | 0.5 | 1.2 |
| Q6 customer_id=1045 | 4905 | 22 | 10.6 | 2.0 | ~9 | 8.4 | 0.5 | 1.3 |
| Q7 emp IN (279,281,283) | 10000 | 34 | 11.1 | 2.7 | ~20 | 6.9 | 1.0 | 2.7 |
| Q8 total_due 10k..50k | 10000 | 49 | 25.1 | 2.6 | ~21 | 20.9 | 1.0 | 2.6 |
| Q9 COUNT(*) emp=279 | 1 | 1 | 0.35 | 0.0 | ~0.6 | index-only count fast path | | |
| Q10 AVG BY emp top 5 | 5 | 35 | 34.3 | 0.0 | ~0.7 | grouped aggregate over 1M base rows | | |

`*` Q1's `encode+send` (9.7ms) is socket-send warmup on the first result-bearing query
of the connection; from Q4 onward it settles to 1.3 to 2.7ms. Treat the steady-state
`encode+send` as ~1 to 3ms.

## Where the time actually goes

Three cost centres, in order of size:

1. **Server B+Tree scan + per-row decode (`base-search+decode`)** is the single largest
   server cost and it dominates `exec`: 7 to 11ms for point/small-range queries, and
   **20 to 21ms for the wide-range predicates (Q3, Q8)**. This is the "scan, not seek"
   behaviour: for `total_due > 10000` style predicates the planner walks a large slice of
   the index and decodes every matching base row from its page. Q3 = Q2 + ORDER BY costs
   ~2x Q2 not because of sorting (the DESC order is satisfied by an index scan, so
   `finalize`/sort = 0.0ms) but because the ordered index path decodes more rows.

2. **Client-side decode in the Kyte driver** is the same order of magnitude as server
   exec: **~9 to 22ms** to turn a 10,000-row binary result into typed `DbValue`s. For
   Q2/Q7/Q8 the client spends *more* time decoding than the server spends executing. This
   is per-cell decode plus ResultSet/row-object materialisation in `kyte-kaidb`.

3. **Wire encode + socket send on the server** is small once warm (~1 to 3ms) and is
   *not* a bottleneck. The binary result cells keep this cheap.

Two things that are explicitly **no longer** costs, confirming the JSON removal landed:

- `buildJson` (the row-image build inside scan) is now **0.5 to 1.1ms**, i.e. noise. The
  read path no longer serialises a JSON object per row.
- `project` and `finalize` are 1 to 3ms and ~0ms respectively.

Fast paths are healthy: Q9 (index-only COUNT) is 0.35ms server; Q10 (GROUP BY AVG) is a
34ms server-bound full aggregate over all 1M base rows with a 5-row result, so its client
decode is ~0.

## What to optimise next, ranked by payoff

1. **Cut the wide-range base-row decode (Q3, Q8; helps Q2 too).** The 20ms is decoding
   every matched base row from pages. Options: a covering index that carries the projected
   columns so the base-row fetch is skipped, or decoding only the projected columns instead
   of the whole row during scan. This is the biggest single server win.
2. **Halve client decode for large result sets.** The driver spends ~1 to 2 microseconds
   per row. A tighter binary-cell decode loop (decode straight into the column vector
   without an intermediate per-cell allocation) would roughly halve Q2/Q7/Q8 client time.
3. **Leave wire encode/send alone.** It is already ~1 to 3ms warm; the binary path did its
   job.

## Note on the earlier concern

The JSON removal did not regress query timings: warm Q1..Q10 here (36/39/44/22/23/22/34/49/1/35 ms)
match the pre-change run within run-to-run noise, and correctness cross-checks with MySQL
are identical. The 55 to 58s **load** time is a separate, client-round-trip-bound cost
(500-row INSERT batches, ~25ms each on the driver), not a query-path issue and not caused
by this change.
