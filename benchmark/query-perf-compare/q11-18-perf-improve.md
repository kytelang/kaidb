# Q11-Q18 performance improvement plan

Grounded in a read of the kaidb SQL engine (`src/query/query_executor.zig`,
`src/query/iterator.zig`, `src/storage/btree.zig`, `src/storage/pool.zig`,
`src/storage/pager.zig`). Baseline is the `query-perf-compare` 1M, `--release`,
3-run median run (see `comparison.md`). All file:line references are from that read
and should be re-confirmed before editing.

## Scope

Already fast, leave alone: **Q11** (PK point seek), **Q13** (whole-table COUNT),
**Q14** (MIN/MAX), **Q15** (DISTINCT). These hit dedicated fast paths
(`tryIndexOnlyCount`, `tryIndexMinMax`, `tryLooseDistinct`, and the PK equality
`PrimaryKeyScanIterator`) and already win or tie.

Real targets: **Q12** (clustered PK range), the range-scan family **Q4/Q5/Q8/Q17**
(shared base-row fetch path), **Q16** (GROUP BY + HAVING), **Q18** (deep OFFSET).

## Baseline numbers (ms, median of 3, 1M rows, release)

| Query | kaidb | PostgreSQL | MySQL |
|---|--:|--:|--:|
| Q12 PK range `id 200000..210000` | 228 | 10 | 18 |
| Q18 deep OFFSET 50000 | 213 | 34 | 81 |
| Q16 GROUP BY customer_id HAVING top 10 | 262 | 45 | 114 |
| Q17 composite emp=279 AND total_due 20k..40k | 40 | 12 | 27 |
| Q4 total_due>50000 LIMIT 5000 | 17 | 5 | 13 |
| Q5 Q4 ORDER BY total_due DESC | 18 | 5 | 13 |
| Q8 total_due 10000..50000 LIMIT 10000 | 31 | 23 | 26 |

---

## STATUS

- **Fix 1 (Q12 clustered PK range): DONE** on branch `perf/q12-pk-range-scan`
  (commit `98d81c6`). Q12 **228 ms -> 10 ms** (matches PostgreSQL), 3-run stable,
  row count unchanged at 10001, all other query row counts unchanged, engine tests
  119/120 (pre-existing mutual-TLS failure). See the fix below for what shipped.
  Gotcha noted for the next session: build the server `zig build -Doptimize=ReleaseFast`
  (plain `zig build` is Debug = ~100x slower, load looks like it hangs), and start it
  on a fresh data dir (the leaked-table bloat makes `nova.db` balloon to GBs).
- Fixes 2-5: not started.

## Fix 1 (highest impact, ~20x): Q12 clustered PK range = full scan -> seek + stop  [DONE]

**Cause.** `buildIteratorTree` only range-scans *secondary* indexes; the base
(clustered PK) tree is never range-scanned. Every `rangeScan`/`iteratorAfter`/
`rangeScanDesc` call targets an `idx_tree`, never the base table tree
(`query_executor.zig:2600-2748`). The PK-specific path is equality only
(`equalityConjunctForCol` `:1281` -> `PrimaryKeyScanIterator` `:2494-2508`).

For a two-sided `id >= 200000 AND id <= 210000` the top node is `AND`.
`getStartKeyForCol` (`:1298`) matches only a single `col >= / > / = v` node, does not
recurse through `AND` (contrast `getRangeForCol` `:1404`), and never extracts an upper
bound. So it returns null (`:1302`), and the query falls to `TableScanIterator`
(`:2735-2748`) opened at the **leftmost leaf** (`iterator.zig:1103-1106`,
`btree.zig:1655`). Result: a **full clustered scan of all 1M rows** with a per-row
residual filter, to return 10k rows. No LIMIT, so no early stop. That is the 228ms.

A one-sided `id >= 200000` alone does get a lexical lower-bound seek
(`iteratorAfter`, `iterator.zig:1104`) but still scans to end-of-table (no upper
bound).

Extra: base PK keys are stored as raw decimal **text** and ordered byte-lexically
(`query_executor.zig:3432, 3449-3453, 810`), so bounds are lexical, not numeric.
Fine for equal-width ids; keep in mind when building the stop key.

**Fix.**
1. Extend PK-range detection: recurse through `AND` (mirror `getRangeForCol` `:1404`)
   to extract both a lower and an upper bound for the PK column.
2. Open the base clustered scan with a seek-to-lower **and a stop key**, so it walks
   only the ~10k-row slice and terminates at the upper bound instead of scanning to
   end-of-table. The `RangeIterator` machinery already exists for secondary indexes
   (`btree.zig:330-340`); reuse it on the base tree.
3. Also covers the one-sided case (add the missing upper/lower stop).

**Expected.** 228ms -> single-digit ms (PG territory). Self-contained, reuses
existing range-iterator code. Do this first.

## Fix 2 (broad): Q4/Q5/Q8/Q17 secondary-index base-row fetch

**Cause.** For each qualifying row the base row is fetched with a **fresh root-to-leaf
descent**: `fetchVisibleFilteredJson` -> `table_tree.search(pk)` (`query_executor.zig:6126`,
`btree.zig:585`). The leaf-reuse cursor that would amortise descents (`base_searcher` /
`LeafReuseSearcher`, `btree.zig:618`) is installed **only** by the equality
`IndexScanIterator` and **only** when env `NOVADB_BASE_CURSOR` is set
(`useBaseCursor`, `iterator.zig:100, 1345-1348`, default off); range/desc/in iterators
never set it. Worse, `refillBatch` (`iterator.zig:1545`) pulls 256 PKs in **index
(value) order, not PK order** (`:1556`), then readaheads their base leaves
(`prefetchBaseLeaves` `:1560`) — but value order is generally not PK order, so the
readahead touches scattered leaves.

**Fix.**
1. **Sort each 256-PK batch into PK order** before fetching, so base-leaf access is
   sequential and `prefetchBaseLeaves` (`pager.zig:240`) actually coalesces runs.
2. **Enable the leaf-reuse cursor by default** for the range/in/desc iterators (not
   just equality, not env-gated), so consecutive in-order PKs skip the root descent.

**Expected.** Cuts every secondary-index query (Q1/Q2/Q4/Q5/Q6/Q7/Q8/Q17), closing
most of the gap to PG's 5-23ms. Medium effort, moderate-to-good win.

## Fix 3 (small, targeted): Q16 GROUP BY + HAVING gate is over-conservative

**Cause.** The fast index-only group-aggregate path `tryIndexGroupAgg`
(`query_executor.zig:2099`) folds aggregates from covered-index key bytes in a single
ordered walk with **zero base-row fetches** (`:2162-2214`) — but it bails the moment a
`HAVING` is present: `if (... or sel.having_expr != null) return null;` (`:2100`).
So Q16 falls to **hash aggregation over a full table scan** (`:3666-3747`) with a
per-row `0x1f`-delimited text-key allocation and a hashmap probe. HAVING is applied
*after* grouping, so it does not actually prevent index-only grouping.

**Fix.** Let `having_expr` through the gate and apply the HAVING filter on the grouped
output the index-only path already produces (mirror the hash path's `evalHaving`
`:3736`). Keep the other guards (single group col, fixed-width group type, covering
`exact` index, sole in-flight txn).

**Expected.** Q16 262ms -> toward Q10's ~30ms. Small, contained change. Helps any
grouped query with HAVING.

## Fix 4: Q18 deep OFFSET = fetch-and-discard -> skip-by-count

**Cause.** When `OFFSET` is present the streaming early-break is disabled
(`query_executor.zig:4068`, and the stream-mode gate requires `sel.offset == null`
`:3887`). So the engine runs the iterator to exhaustion, decodes and buffers every
qualifying row (`:4060`), sorts (`:4083-4103`), then **skips and frees the first N**
(`:4112-4138`). For Q18 that is ~59,434 rows fetched and sorted to return 100. Even
when `total_due` is indexed and the scan order is already final (`order_by_scan_asc`
`:3835-3855`), the offset still forces buffering because the break is guarded on
`sel.offset == null`.

**Fix.** When the scan order already matches the `ORDER BY` (indexed case), push the
offset into the index walk: **advance the cursor N keys by counting, without fetching
or decoding base rows**, then materialise only the LIMIT rows after the offset. Fall
back to today's buffer-then-skip when a real sort is required (order not from an
index). Apply in the main loop and mirror in the two GROUP BY output paths
(`:2293-2312`, `:3792-3811`) if useful.

**Expected.** Deep pagination ~213ms -> tens of ms.

## Fix 5 (last, broad, riskier): per-row allocation churn

Every emitted row allocates two arrays (`names` + `cells`) in `buildRowJson`
(`query_executor.zig:5594, 5604, 5612`) and dups each TEXT cell (`readCellFromCol`
`:5670`), then frees the prior row's `TableRow` (`iterator.zig:1137-1140, 1611-1614`).
For a 10k-row scan that is 20k+ allocs plus dups plus frees. Reusing a row-image
buffer across rows would shave a slice off **every** scan (including Q12 after Fix 1).
Bigger and riskier because of ownership/ARC and the MVCC borrow paths
(`getVisibleVersion` `:5506`), so schedule it last, behind the wins above.

Note: `getVisibleVersion` also JSON-parses the whole MVCC blob per row when the stored
value begins with `{` (`:5518`); the common single-committed-version fast path
(`:5559-5573`) avoids it. Confirm the orders rows take the fast path (they should) so
this is not a hidden per-row cost.

---

## Suggested order of work

1. **Fix 1 (Q12 clustered PK range)** — biggest, self-contained, reuses range iterator.
2. **Fix 3 (Q16 HAVING gate)** — small, high value.
3. **Fix 2 (PK-sorted batch + default leaf cursor)** — helps the whole range family.
4. **Fix 4 (OFFSET skip-by-count)** — deep pagination.
5. **Fix 5 (row-image reuse)** — broad low-grade win, do last.

## Method (for every fix)

- Work on a branch; build `--release` (btree server and the harness).
- Gate on the built-in correctness cross-check: identical row counts and Q9/Q10/Q13/
  Q14 scalars must still match PostgreSQL and MySQL. A plan change that returns wrong
  rows is a regression, not a win.
- Re-run `query-perf-compare` (3 runs, medians) and record before/after in
  `comparison.md`.
- Watch for regressions on the queries a change touches: Fix 1 must not slow the PK
  point (Q11) or the existing table scans; Fix 2 must not slow equality scans; Fix 3
  must keep Q10 correct; Fix 4 must not change non-offset queries.
- `zig build test` for the btree engine must stay green (baseline 119/120; the one
  known failure is the pre-existing mutual-TLS replication test).
