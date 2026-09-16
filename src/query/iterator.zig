//! Volcano-style row iterators and the SQL expression evaluation engine.
//!
//! This module is the execution layer of NovaDB's SQL engine: the set of
//! pull-based operators the [`QueryExecutor`] wires into a tree to answer a
//! `SELECT`, plus the scalar/predicate evaluator that operators call to test a
//! `WHERE`, compute a projected value, or resolve a join key. Every operator
//! implements the same demand-driven ("Volcano") contract exposed through the
//! type-erased [`RowIterator`]: one `next()` call produces at most one [`Row`],
//! `null` signals end of stream, and operators compose by holding a child
//! `RowIterator`. This lets a plan such as `Filter(HashJoin(TableScan, TableScan))`
//! be built bottom-up and evaluated one row at a time without materialising the
//! whole intermediate result, except where an operator must buffer by nature
//! (the join build side, below).
//!
//! Row shape and multi-table naming. A [`Row`] is a parallel pair of slices:
//! `tables[i]` is the (aliased) table name and `data[i]` is that table's JSON
//! object contributing to the current combined row. A single-table scan yields
//! one entry; each join appends the right table, so a three-way join produces a
//! row with three `(name, object)` pairs. Column references are resolved against
//! this by [`getValCol`]: a qualified `t.col` matches the table whose name is
//! `t` and reads `col` from its object, while a bare `col` is searched across
//! every table's object left to right (first hit wins). This is why join
//! operators must carry the correct `tables` list, not just the data.
//!
//! MVCC visibility lives in the scan operators, not here in the evaluator. The
//! three leaf scans ([`TableScanIterator`], [`IndexScanIterator`],
//! [`PrimaryKeyScanIterator`]) walk B+Tree cells and, for each candidate,
//! call `exec.getVisibleVersion(...)` with the statement's `current_tx`; a cell
//! whose latest version is not visible to this transaction is skipped, so the
//! iterator only ever emits rows the snapshot may see. Each scan owns exactly
//! one decoded JSON row at a time in `current_row_json` and frees the PREVIOUS
//! one at the top of the next `next()` (and any pending one in `deinit`), so the
//! `Row` handed out borrows storage that stays valid only until the following
//! pull. Consumers that need a row to outlive the next `next()` must clone it,
//! which is exactly what the join build side does with [`cloneJson`].
//!
//! Join memory model. [`NestedLoopJoinIterator`] and [`HashJoinIterator`] both
//! DRAIN the right child fully at construction, deep-cloning each right row into
//! an owned `right_rows` array (the child's rows are transient, per the previous
//! paragraph), then stream the left side. LEFT/RIGHT/FULL outer semantics are
//! implemented with a two-phase state machine: `left_scan` emits matches plus
//! left-unmatched padding, then (for RIGHT/FULL) `right_unmatched_scan` walks a
//! `matched_right_set` and emits right rows that never matched, padded with
//! NULLs. Combined-row buffers (`current_tables`/`current_data`) are freed on
//! the NEXT pull via each operator's `clearCurrentCombined`, matching the
//! borrow-until-next-pull rule the scans follow. The hash join additionally
//! builds a `StringHashMap` over the right key column for O(1) probing, and
//! (unlike the nested-loop join) clones left data too so its combined rows are
//! fully owned.
//!
//! Three-valued logic. SQL predicates are evaluated in [`Bool3`] (true / false /
//! unknown) so that NULL propagates per the standard: comparisons against a NULL
//! operand yield `unknown`, `AND`/`OR`/`NOT` follow Kleene logic ([`and3`],
//! [`or3`], [`not3`]), and only a definite `true` passes a filter (see
//! [`evalExpr`]). The scalar evaluator [`evalScalar`] returns an optional
//! `Scalar` where `null` means SQL NULL (or "not computable"), and it is
//! parameterised over a resolver (`anytype` context with a `resolve(col)`
//! method) so the same code serves both row-shaped input ([`RowResolver`]) and a
//! single JSON object ([`JsonResolver`]), the latter used for row-at-a-time
//! re-evaluation outside a scan.
//!
//! Value comparison is type-tolerant by design because column data is stored as
//! loosely typed JSON. [`compareValues`] tries, in order, exact decimal-string
//! comparison ([`cmpDecimalStr`], which avoids float rounding for money/DECIMAL),
//! then integer, then float, then lexicographic string comparison, coercing
//! across representations (a numeric string compares numerically with an
//! integer). This ordering matters: it keeps `"1.10"` and `"1.1"` equal and
//! orders large exact decimals correctly where an `f64` path would not.

const std = @import("std");
const ast = @import("../sql/ast.zig");
const page = @import("../storage/page.zig");
const BPlusTree = @import("../storage/btree.zig").BPlusTree;
const BPlusTreeIterator = @import("../storage/btree.zig").Iterator;
const BPlusTreeRangeIterator = @import("../storage/btree.zig").RangeIterator;
const BPlusTreeRangeIteratorDesc = @import("../storage/btree.zig").RangeIteratorDesc;
const Database = @import("../schema.zig").Database;
const Table = @import("../schema.zig").Table;
const QueryExecutor = @import("query_executor.zig").QueryExecutor;

/// Default look-ahead / prefetch window shared by the index-scan operators. 256
/// gives the storage device a deep enough queue to overlap random base-page
/// reads (latency hiding on a disk-bound scan) while keeping latency-to-first-row
/// and memory small; measured as the knee on the 10M orders workload.
const DEFAULT_PREFETCH_BATCH: usize = 256;

/// Resolve the per-scan prefetch batch size, honouring `NOVADB_PREFETCH_BATCH`
/// (a value of `0`/`1` disables prefetch and streams one PK at a time).
fn resolvePrefetchBatch() usize {
    const raw = std.c.getenv("NOVADB_PREFETCH_BATCH") orelse return DEFAULT_PREFETCH_BATCH;
    const v = std.mem.span(raw);
    const n = std.fmt.parseInt(usize, v, 10) catch return DEFAULT_PREFETCH_BATCH;
    return if (n == 0) 1 else n;
}

/// Whether the equality index scan should reuse a base-table leaf cursor across
/// its (pk-ascending) row fetches. Opt-in via `NOVADB_BASE_CURSOR`; default off
/// because it holds the base tree's shared structure lock for the scan's
/// lifetime (fine for a single reader, but it delays writers).
fn useBaseCursor() bool {
    return std.c.getenv("NOVADB_BASE_CURSOR") != null;
}

/// Best-effort: resolve `pks` to their base-table leaf pages and hint the OS to
/// read them ahead concurrently, so the caller's subsequent per-row lookups find
/// the pages in flight / cache-resident. `leaf_ids` is reused scratch. Any error
/// inside `collectLeafPageIds` just yields fewer hints; correctness is unaffected
/// because the real reads still go through the normal search path.
/// Adaptive prefetch gate: return true (do the base-leaf prefetch) only while the
/// buffer pool is still missing enough that hiding disk latency is worth the extra
/// per-PK base-tree descent. Once the pool serves nearly everything from RAM, the
/// prefetch is pure overhead, so return false. Cumulative pool hit ratio is the
/// signal (`hit_count / fetch_count`); below a warm-up threshold of fetches we keep
/// prefetch on so a genuinely cold scan is never penalised.
fn shouldPrefetch(table_tree: *BPlusTree) bool {
    const pool = table_tree.pool;
    const fc = pool.fetch_count.load(.monotonic);
    if (fc < 20000) return true; // not enough signal yet: default to prefetch (cold-safe)
    const hc = pool.hit_count.load(.monotonic);
    // Prefetch while the hit ratio is under 98% (disk-bound); skip once resident.
    return (hc *% 100) < (fc *% 98);
}

fn prefetchBaseLeaves(table_tree: *BPlusTree, pks: []const []const u8, leaf_ids: *std.ArrayList(page.PageId), a: Allocator) void {
    leaf_ids.clearRetainingCapacity();
    table_tree.collectLeafPageIds(pks, leaf_ids, a);
    if (leaf_ids.items.len > 0) table_tree.pool.pager.prefetchPages(leaf_ids.items);
}
/// Convenience alias for the standard-library allocator interface used by every
/// operator here for its per-operator scratch and combined-row buffers.
const Allocator = std.mem.Allocator;

/// One combined result row flowing through the operator pipeline.
///
/// `tables` and `data` are parallel: `data[i]` is the JSON object contributed by
/// the table named `tables[i]`. A single-table scan has length 1; each join
/// appends its right table, so length grows with join arity. The slices (and the
/// JSON they point at) are borrowed from the producing operator and are only
/// valid until that operator's next `next()` call, so a consumer that must retain
/// a row past the next pull has to deep-copy it with [`cloneJson`]. Column lookup
/// against a `Row` is done by [`getValCol`], which understands the `table.column`
/// qualification encoded in the names.
pub const Row = struct {
    /// Table (or alias) name for each parallel entry in [`Row.data`]; used by
    /// [`getValCol`] to resolve a qualified `t.col` reference.
    tables: []const []const u8,
    /// The column set each table contributes to this combined row, one
    /// [`TableRow`] per entry in [`Row.tables`]. An outer-join non-match slot
    /// holds a [`TableRow`] with `is_null = true`.
    data: []const TableRow,
};

/// One table's contribution to a [`Row`]: a positional set of result cells with
/// their column names, replacing the former per-row `std.json` object. Cells are
/// the decoded TEXT form of each stored column (the representation the wire
/// ships), or `null` for a SQL NULL cell; `names` are borrowed column-name slices
/// (schema-stable, not duplicated per row). Lookups scan `names` (result sets
/// have a handful of columns, so a linear scan beats a per-row hashmap and costs
/// no allocation). `is_null` marks an outer-join non-match where the whole slot
/// reads as NULL.
/// The transient value the SQL expression evaluator works in: a native scalar
/// (no JSON). The variant names deliberately match the `Scalar` subset
/// the evaluator used (`null`/`bool`/`integer`/`float`/`string`) so the switch
/// arms and value literals in the evaluator are unchanged. Document/at-rest
/// values (objects, arrays) are NOT scalars and are handled separately by
/// `cloneJson`/`freeClonedJson`, which stay on `Scalar`.
pub const Scalar = union(enum) {
    null,
    bool: bool,
    integer: i64,
    float: f64,
    string: []const u8,

    /// Bridge to `std.json.Value` for the paths that still build a JSON row image
    /// (the join / non-catalog `SELECT *` blob, Group A in json-to-binary.md).
    /// Removed once those shapes emit positional cells.
    pub fn toJsonValue(self: Scalar) std.json.Value {
        return switch (self) {
            .null => .null,
            .bool => |b| .{ .bool = b },
            .integer => |n| .{ .integer = n },
            .float => |f| .{ .float = f },
            .string => |s| .{ .string = s },
        };
    }
};

/// One decoded result-cell value. Numeric variants carry the value inline (no
/// allocation, no decimal formatting), so a `SELECT` can ship them as either
/// binary wire bytes or on-demand text without a per-row `dtoa`/`itoa`. `.text`
/// is an owned string (TEXT/BLOB columns, dates); `.null` is SQL NULL.
pub const Cell = union(enum) {
    null,
    text: []const u8,
    int: i64,
    uint: u64,
    float: f64,
    float32: f32,
    boolean: bool,

    /// This cell as a transient evaluator scalar (numbers become `.integer`/
    /// `.float`, text becomes `.string`).
    pub fn scalar(self: Cell) Scalar {
        return switch (self) {
            .null => .null,
            .text => |t| .{ .string = t },
            .int => |v| .{ .integer = v },
            .uint => |v| .{ .integer = @intCast(v) },
            .float => |v| .{ .float = v },
            .float32 => |v| .{ .float = @floatCast(v) },
            .boolean => |b| .{ .bool = b },
        };
    }

    /// Owned text form of this cell, byte-identical to the former
    /// `RowReader.readToString` render (`{d}` for numerics on their own width,
    /// `true`/`false`, raw text), or null for SQL NULL. Caller owns the result.
    pub fn toTextAlloc(self: Cell, a: std.mem.Allocator) !?[]const u8 {
        return switch (self) {
            .null => null,
            .text => |t| try a.dupe(u8, t),
            .int => |v| try std.fmt.allocPrint(a, "{d}", .{v}),
            .uint => |v| try std.fmt.allocPrint(a, "{d}", .{v}),
            .float => |v| try std.fmt.allocPrint(a, "{d}", .{v}),
            .float32 => |v| try std.fmt.allocPrint(a, "{d}", .{v}),
            .boolean => |b| try a.dupe(u8, if (b) "true" else "false"),
        };
    }
};

pub const TableRow = struct {
    names: []const []const u8,
    cells: []const Cell,
    is_null: bool = false,

    pub const null_row: TableRow = .{ .names = &.{}, .cells = &.{}, .is_null = true };

    /// Index of `col` within this table row, or null if absent.
    pub fn find(self: TableRow, col: []const u8) ?usize {
        if (self.is_null) return null;
        for (self.names, 0..) |n, i| {
            if (std.mem.eql(u8, n, col)) return i;
        }
        return null;
    }

    /// The typed cell for `col`, or null if the column is absent.
    pub fn get(self: TableRow, col: []const u8) ?Cell {
        const i = self.find(col) orelse return null;
        return self.cells[i];
    }

    /// Owned text of `col`, or null if absent or SQL NULL. Caller owns the result.
    pub fn getTextAlloc(self: TableRow, a: std.mem.Allocator, col: []const u8) !?[]const u8 {
        const i = self.find(col) orelse return null;
        return self.cells[i].toTextAlloc(a);
    }

    /// The cell for `col` as a transient scalar for the expression evaluator, or
    /// `null` when the column is absent (treated as unresolved). This is the seam
    /// that keeps the evaluator on its existing scalar type while row storage is
    /// json-free and typed.
    pub fn getScalar(self: TableRow, col: []const u8) ?Scalar {
        const i = self.find(col) orelse return null;
        return self.cells[i].scalar();
    }
};

/// Frees a [`TableRow`] allocated by the executor's row builder: its owned cell
/// strings and the `cells`/`names` backing arrays. Borrowed column-name slices
/// inside `names` are NOT freed (they point at schema-stable storage). A
/// `null_row` / empty row is a no-op.
pub fn freeTableRow(a: std.mem.Allocator, tr: TableRow) void {
    for (tr.cells) |c| switch (c) {
        .text => |t| a.free(t),
        else => {},
    };
    if (tr.cells.len > 0) a.free(tr.cells);
    if (tr.names.len > 0) a.free(tr.names);
}

/// Deep-copies a [`TableRow`] so it can outlive the producer's page latch (used
/// by joins that buffer the right-hand row). Duplicates the cell strings and the
/// backing arrays; `names` entries remain borrowed (schema-stable).
pub fn cloneTableRow(a: std.mem.Allocator, tr: TableRow) !TableRow {
    if (tr.is_null) return TableRow.null_row;
    const names = try a.dupe([]const u8, tr.names);
    errdefer a.free(names);
    const cells = try a.alloc(Cell, tr.cells.len);
    errdefer a.free(cells);
    var done: usize = 0;
    errdefer for (cells[0..done]) |c| switch (c) {
        .text => |t| a.free(t),
        else => {},
    };
    for (tr.cells, 0..) |c, i| {
        cells[i] = switch (c) {
            .text => |t| Cell{ .text = try a.dupe(u8, t) },
            else => c,
        };
        done = i + 1;
    }
    return .{ .names = names, .cells = cells, .is_null = false };
}

/// Type-erased handle to any operator in the pull-based execution pipeline.
///
/// This is the uniform interface that lets operators compose without knowing
/// each other's concrete types: a parent holds a child `RowIterator` and pulls
/// from it. `ptr` is the erased pointer to the owning operator struct, and the
/// two function pointers are its vtable. Every concrete operator exposes an
/// `iterator()` method that fills this in with closures over its own state. The
/// `tables` field is published up front (before the first `next`) so a parent
/// operator can compute its own combined `tables` list at construction time.
pub const RowIterator = struct {
    /// Erased pointer to the concrete operator; `@ptrCast` back to the real type
    /// inside [`RowIterator.nextFn`] / [`RowIterator.deinitFn`].
    ptr: *anyopaque,
    /// The table-name layout of every [`Row`] this iterator will produce, known
    /// before iteration starts so parents can precompute their own layout.
    tables: []const []const u8,
    /// Pull one row: returns the next [`Row`], `null` at end of stream, or
    /// propagates an error (for example a deadline hit or an allocation failure).
    nextFn: *const fn (ctx: *anyopaque) anyerror!?Row,
    /// Tear down the operator and everything it owns (buffered rows, child
    /// iterators, the operator struct itself). Called exactly once.
    deinitFn: *const fn (ctx: *anyopaque) void,

    /// Pulls the next [`Row`], or `null` when the stream is exhausted.
    ///
    /// Thin dispatch to [`RowIterator.nextFn`]. The returned row borrows storage
    /// owned by the producer and is invalidated by the next call, per [`Row`].
    pub fn next(self: RowIterator) !?Row {
        return self.nextFn(self.ptr);
    }
    /// Releases the operator and all resources it owns.
    ///
    /// Dispatches to [`RowIterator.deinitFn`]. Composite operators forward
    /// `deinit` to their child iterators, so deinitialising the root of a plan
    /// tears down the whole tree.
    pub fn deinit(self: RowIterator) void {
        self.deinitFn(self.ptr);
    }
};


/// Resolves a column reference against a combined [`Row`], returning its value
/// or `null` if the column is absent.
///
/// Two forms are handled. A qualified `t.col` (a `.` in the name) is resolved by
/// finding the table entry whose name equals `t` and reading `col` from that
/// object. An unqualified `col` is searched across every table's object left to
/// right, returning the first match, which is how a bare column name works in a
/// join without an ambiguity check. Returns `null` if no table matches, the
/// matched value is not an object, or the key is missing.
pub fn getValCol(col_name: []const u8, row: Row) ?Scalar {
    if (std.mem.indexOfScalar(u8, col_name, '.')) |dot_idx| {
        const tbl = col_name[0..dot_idx];
        const col = col_name[dot_idx + 1 ..];
        for (row.tables, 0..) |t, i| {
            if (std.mem.eql(u8, t, tbl)) {
                return row.data[i].getScalar(col);
            }
        }
        return null;
    }
    for (row.data) |row_data| {
        if (row_data.getScalar(col_name)) |val| return val;
    }
    return null;
}

/// Evaluates a *simple* expression against a [`Row`] to a JSON value.
///
/// Handles only leaf forms: a column reference (delegated to [`getValCol`]) and
/// integer/float/text/NULL literals. Any compound expression (arithmetic,
/// functions, `CASE`) returns `null`; the full evaluator is [`evalScalar`]. This
/// narrow helper exists for the hash join, which only needs to pull a join-key
/// column value out of a row.
pub fn getVal(expr: *const ast.Expr, row: Row) ?Scalar {
    switch (expr.*) {
        .column_ref => |col_name| return getValCol(col_name, row),
        .literal_int => |val| return .{ .integer = val },
        .literal_float => |val| return .{ .float = val },
        .literal_text => |val| return .{ .string = val },
        .literal_null => return .null,
        else => return null,
    }
}

/// SQL three-valued logic (Kleene) truth value.
///
/// SQL predicates are not two-valued because NULL is neither true nor false: a
/// comparison with a NULL operand is `unknown`, and only a definite `true`
/// admits a row through a filter. The connectives are [`and3`], [`or3`],
/// [`not3`]; a definite Zig `bool` is lifted with [`boolToB3`].
pub const Bool3 = enum { false, true, unknown };

/// Kleene AND: `false` dominates, both-`true` is `true`, otherwise `unknown`.
///
/// `false AND unknown` is `false` (a false conjunct settles the result even when
/// the other is unknown), which is why the `false` check comes first.
fn and3(a: Bool3, b: Bool3) Bool3 {
    if (a == .false or b == .false) return .false;
    if (a == .true and b == .true) return .true;
    return .unknown;
}
/// Kleene OR: `true` dominates, both-`false` is `false`, otherwise `unknown`.
///
/// `true OR unknown` is `true`; only when neither disjunct is true and at least
/// one is unknown does the result become `unknown`.
fn or3(a: Bool3, b: Bool3) Bool3 {
    if (a == .true or b == .true) return .true;
    if (a == .false and b == .false) return .false;
    return .unknown;
}
/// Kleene NOT: swaps true/false and leaves `unknown` unchanged (`NOT NULL` is
/// still unknown).
fn not3(a: Bool3) Bool3 {
    return switch (a) {
        .true => .false,
        .false => .true,
        .unknown => .unknown,
    };
}
/// Lifts a definite Zig `bool` into [`Bool3`] (never produces `unknown`); used
/// after a comparison whose operands were both non-NULL.
fn boolToB3(b: bool) Bool3 {
    return if (b) .true else .false;
}

/// Column resolver backed by a combined [`Row`] (the multi-table scan/join case).
///
/// Satisfies the `resolve(col)` shape that [`evalScalar`]/[`eval3`] expect via
/// `anytype`, delegating to [`getValCol`] so qualified `t.col` names work.
const RowResolver = struct {
    /// The row whose column values expressions are evaluated against.
    row: Row,
    /// Resolves `col` (qualified or bare) against [`RowResolver.row`] using
    /// [`getValCol`].
    fn resolve(self: RowResolver, col: []const u8) ?Scalar {
        return getValCol(col, self.row);
    }
};
/// Column resolver backed by a single JSON object (row-at-a-time evaluation
/// outside a scan, e.g. re-checking a predicate against one materialised row).
///
/// Because there is only one object and no table dimension, a qualified `t.col`
/// is resolved by ignoring the table part and looking up the bare `col`.
pub const JsonResolver = struct {
    /// The single row whose columns back lookups.
    row: TableRow,
    /// Resolves `col` against [`JsonResolver.row`]; strips any `table.` prefix
    /// since there is a single unnamed row. Returns `null` if the column is absent.
    fn resolve(self: JsonResolver, col: []const u8) ?Scalar {
        if (std.mem.indexOfScalar(u8, col, '.')) |di| return self.row.getScalar(col[di + 1 ..]);
        return self.row.getScalar(col);
    }
};

/// Evaluates a scalar (value-producing) SQL expression against a resolver.
///
/// `ctx` is any type with a `resolve(col) ?Scalar` method, so this same
/// code drives both [`RowResolver`] and [`JsonResolver`] without duplication. A
/// returned `null` means SQL NULL *or* "not computable" (the two are deliberately
/// merged here); [`eval3`] maps that to `unknown` for predicates. Supports
/// column refs (a resolved NULL collapses to `null`), literals, the four
/// arithmetic operators via [`arith`], searched `CASE` (first `WHEN` whose
/// condition is definitely true, using [`eval3`]), and the scalar functions in
/// [`evalFunc`]. Placeholders (`?`) resolve to `null` because parameters are
/// expected to be substituted before execution. Any unsupported node returns
/// `null`.
fn evalScalar(expr: *const ast.Expr, ctx: anytype) ?Scalar {
    switch (expr.*) {
        .column_ref => |c| {
            const v = ctx.resolve(c) orelse return null;
            return if (v == .null) null else v;
        },
        .literal_int => |v| return .{ .integer = v },
        .literal_float => |v| return .{ .float = v },
        .literal_text => |v| return .{ .string = v },
        .literal_null => return null,
        .placeholder => return null,
        .binary_op => |op| switch (op.op) {
            .PLUS, .MINUS, .STAR, .SLASH => {
                const l = evalScalar(op.left, ctx) orelse return null;
                const r = evalScalar(op.right, ctx) orelse return null;
                return arith(l, op.op, r);
            },
            else => return null,
        },
        .case_expr => |ce| {
            for (ce.whens) |w| {
                if (eval3(w.cond, ctx) == .true) return evalScalar(w.result, ctx);
            }
            if (ce.else_result) |er| return evalScalar(er, ctx);
            return null;
        },
        .func_call => |fc| return evalFunc(fc, ctx),
        else => return null,
    }
}

/// One of two thread-local scratch buffers backing string functions
/// (`UPPER`/`LOWER`) so the result can be returned as a borrowed slice without
/// allocating. See [`fn_scratch_toggle`] for why there are two.
threadlocal var fn_scratch_a: [512]u8 = undefined;
/// The second thread-local string-function scratch buffer; paired with
/// [`fn_scratch_a`] and alternated via [`fn_scratch_toggle`].
threadlocal var fn_scratch_b: [512]u8 = undefined;
/// Alternates which of [`fn_scratch_a`]/[`fn_scratch_b`] the next string
/// function writes into, so two such results can be live at once (for example
/// both sides of `UPPER(a) = UPPER(b)`) without the second clobbering the first.
threadlocal var fn_scratch_toggle: bool = false;

/// Evaluates a scalar SQL function call against a resolver.
///
/// Recognised by name (case-sensitive, upper-case): `COALESCE` (first non-NULL
/// argument), `NULLIF` (NULL when the two arguments compare equal via
/// [`compareValues`], else the first), `ABS`, `LENGTH`/`CHAR_LENGTH`,
/// `UPPER`/`LOWER`/`TRIM`. Unknown functions and argument type mismatches return
/// `null`. Arguments are evaluated with [`evalScalar`], so nesting works.
///
/// The string-returning cases (`UPPER`/`LOWER`) write into a thread-local
/// scratch buffer ([`fn_scratch_a`]/[`fn_scratch_b`]) and return a borrowed
/// slice truncated to the buffer length, so the result is only valid until the
/// same-slot buffer is reused; `TRIM` instead returns a subslice of its input
/// and needs no buffer. `COALESCE`/`NULLIF` short-circuit before the shared
/// `arg0` is computed because they have argument-count semantics of their own.
fn evalFunc(fc: ast.FuncCall, ctx: anytype) ?Scalar {
    const name = fc.name;
    if (std.mem.eql(u8, name, "COALESCE")) {
        for (fc.args) |arg| {
            if (evalScalar(arg, ctx)) |v| return v;
        }
        return null;
    }
    if (std.mem.eql(u8, name, "NULLIF")) {
        if (fc.args.len != 2) return null;
        const a = evalScalar(fc.args[0], ctx) orelse return null;
        const b = evalScalar(fc.args[1], ctx) orelse return a;
        return if (compareValues(a, .EQ, b)) null else a;
    }
    if (fc.args.len == 0) return null;
    const arg0 = evalScalar(fc.args[0], ctx);
    if (std.mem.eql(u8, name, "ABS")) {
        const v = arg0 orelse return null;
        if (asI64(v)) |i| return .{ .integer = if (i < 0) -i else i };
        if (asF64(v)) |f| return .{ .float = if (f < 0) -f else f };
        return null;
    }
    if (std.mem.eql(u8, name, "LENGTH") or std.mem.eql(u8, name, "CHAR_LENGTH")) {
        const v = arg0 orelse return null;
        return switch (v) {
            .string => |s| .{ .integer = @as(i64, @intCast(s.len)) },
            else => null,
        };
    }
    if (std.mem.eql(u8, name, "UPPER") or std.mem.eql(u8, name, "LOWER") or std.mem.eql(u8, name, "TRIM")) {
        const v = arg0 orelse return null;
        const s = switch (v) {
            .string => |str| str,
            else => return null,
        };
        const buf: []u8 = if (fn_scratch_toggle) fn_scratch_b[0..] else fn_scratch_a[0..];
        fn_scratch_toggle = !fn_scratch_toggle;
        if (std.mem.eql(u8, name, "TRIM")) {
            return .{ .string = std.mem.trim(u8, s, " \t\r\n") };
        }
        const n = @min(s.len, buf.len);
        const up = std.mem.eql(u8, name, "UPPER");
        for (0..n) |i| buf[i] = if (up) std.ascii.toUpper(s[i]) else std.ascii.toLower(s[i]);
        return .{ .string = buf[0..n] };
    }
    return null;
}

/// Coerces a JSON value to an `i64`, or `null` if it cannot be one.
///
/// Accepts an integer directly and a string that parses as a base-10 integer;
/// everything else (float, bool, object, ...) yields `null`. The string path is
/// what lets numeric text stored as JSON participate in integer arithmetic and
/// comparison. Compare with [`asF64`], the floating-point counterpart.
fn asI64(v: Scalar) ?i64 {
    return switch (v) {
        .integer => |i| i,
        .string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
}

/// Coerces a JSON value to an `f64`, or `null` if it cannot be one.
///
/// Accepts a float directly, widens an integer, and parses a numeric string;
/// non-numeric values yield `null`. Used as the fallback numeric path in
/// [`arith`] and the function evaluator when the integer path ([`asI64`]) does
/// not apply.
fn asF64(v: Scalar) ?f64 {
    return switch (v) {
        .float => |f| f,
        .integer => |i| @as(f64, @floatFromInt(i)),
        .string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
}

/// Evaluates a binary arithmetic operator on two JSON operands.
///
/// Prefers integer arithmetic: if both operands coerce via [`asI64`] the result
/// is an integer, using wrapping `+%`/`-%`/`*%` (so overflow wraps rather than
/// traps) and truncating division; division by zero returns `null` (SQL NULL).
/// Otherwise it falls back to `f64` via [`asF64`], with float division by zero
/// also `null`. A non-arithmetic `op` or a non-numeric operand returns `null`.
fn arith(l: Scalar, op: ast.OpType, r: Scalar) ?Scalar {
    if (asI64(l)) |a| {
        if (asI64(r)) |b| {
            return switch (op) {
                .PLUS => .{ .integer = a +% b },
                .MINUS => .{ .integer = a -% b },
                .STAR => .{ .integer = a *% b },
                .SLASH => if (b == 0) null else .{ .integer = @divTrunc(a, b) },
                else => null,
            };
        }
    }
    const a = asF64(l) orelse return null;
    const b = asF64(r) orelse return null;
    return switch (op) {
        .PLUS => .{ .float = a + b },
        .MINUS => .{ .float = a - b },
        .STAR => .{ .float = a * b },
        .SLASH => if (b == 0.0) null else .{ .float = a / b },
        else => null,
    };
}

/// Renders a JSON value to a string slice for `LIKE` matching.
///
/// A string is returned as-is; an integer is formatted into the caller-supplied
/// `buf` (so the slice borrows `buf`); anything else returns `null`. Used by the
/// `LIKE` branch of [`eval3`] to obtain both the subject text and the pattern.
fn valStr(v: Scalar, buf: []u8) ?[]const u8 {
    return switch (v) {
        .string => |s| s,
        .integer => |i| std.fmt.bufPrint(buf, "{d}", .{i}) catch null,
        else => null,
    };
}

/// Matches `text` against a SQL `LIKE` pattern where `%` is any run (including
/// empty) and `_` is exactly one character.
///
/// Implements the classic linear-time backtracking wildcard match: it advances
/// through both strings, and on a mismatch after a `%` it rewinds to just past
/// that `%` and consumes one more subject character (`star_pi`/`star_ti` remember
/// the backtrack point). Trailing `%`s in the pattern are skipped at the end, so
/// the match succeeds iff the pattern is fully consumed. No escape handling and
/// no character-class support; comparison is byte-exact (case-sensitive).
fn likeMatch(text: []const u8, pat: []const u8) bool {
    var ti: usize = 0;
    var pi: usize = 0;
    var star_pi: ?usize = null;
    var star_ti: usize = 0;
    while (ti < text.len) {
        if (pi < pat.len and (pat[pi] == '_' or pat[pi] == text[ti])) {
            ti += 1;
            pi += 1;
        } else if (pi < pat.len and pat[pi] == '%') {
            star_pi = pi;
            star_ti = ti;
            pi += 1;
        } else if (star_pi) |sp| {
            pi = sp + 1;
            star_ti += 1;
            ti = star_ti;
        } else {
            return false;
        }
    }
    while (pi < pat.len and pat[pi] == '%') pi += 1;
    return pi == pat.len;
}

/// Evaluates a SQL predicate expression to a three-valued [`Bool3`].
///
/// This is the boolean counterpart of [`evalScalar`] and the heart of `WHERE`/
/// `ON`/`CASE`-condition evaluation. NULL propagation is uniform: whenever a
/// needed sub-value is `null` (SQL NULL), the result is `unknown` rather than
/// false, per the standard. Handles `AND`/`OR` (via [`and3`]/[`or3`], each side
/// recursively three-valued), the six comparison operators (both operands via
/// [`evalScalar`], compared by [`compareValues`]), `NOT` ([`not3`]),
/// `IS [NOT] NULL`, `IN` (a definite match wins immediately; if no match but a
/// list element was unknown the result is `unknown`), `LIKE` (via [`likeMatch`]),
/// and `BETWEEN` (two comparisons). `IN`/`LIKE`/`BETWEEN` honour their `negated`
/// flag. Any other node evaluates to `unknown`.
fn eval3(expr: *const ast.Expr, ctx: anytype) Bool3 {
    switch (expr.*) {
        .binary_op => |op| switch (op.op) {
            .AND => return and3(eval3(op.left, ctx), eval3(op.right, ctx)),
            .OR => return or3(eval3(op.left, ctx), eval3(op.right, ctx)),
            .EQ, .NE, .GT, .LT, .GTE, .LTE => {
                const l = evalScalar(op.left, ctx) orelse return .unknown;
                const r = evalScalar(op.right, ctx) orelse return .unknown;
                return boolToB3(compareValues(l, op.op, r));
            },
            else => return .unknown,
        },
        .unary_not => |inner| return not3(eval3(inner, ctx)),
        .is_null => |n| {
            const v = evalScalar(n.operand, ctx);
            const is_null = (v == null);
            return boolToB3(if (n.negated) !is_null else is_null);
        },
        .in_list => |in| {
            const l = evalScalar(in.operand, ctx) orelse return .unknown;
            var saw_unknown = false;
            for (in.items) |item| {
                const rv = evalScalar(item, ctx) orelse {
                    saw_unknown = true;
                    continue;
                };
                if (compareValues(l, .EQ, rv)) return boolToB3(!in.negated);
            }
            if (saw_unknown) return .unknown;
            return boolToB3(in.negated);
        },
        .like => |lk| {
            const l = evalScalar(lk.operand, ctx) orelse return .unknown;
            const p = evalScalar(lk.pattern, ctx) orelse return .unknown;
            var lbuf: [32]u8 = undefined;
            var pbuf: [128]u8 = undefined;
            const ls = valStr(l, &lbuf) orelse return .unknown;
            const ps = valStr(p, &pbuf) orelse return .unknown;
            const m = likeMatch(ls, ps);
            return boolToB3(if (lk.negated) !m else m);
        },
        .between => |bt| {
            const l = evalScalar(bt.operand, ctx) orelse return .unknown;
            const lo = evalScalar(bt.lo, ctx) orelse return .unknown;
            const hi = evalScalar(bt.hi, ctx) orelse return .unknown;
            const in_range = compareValues(l, .GTE, lo) and compareValues(l, .LTE, hi);
            return boolToB3(if (bt.negated) !in_range else in_range);
        },
        else => return .unknown,
    }
}

/// A normalised fixed-point decimal parsed from a string, held as digit slices so
/// exact (non-float) comparison is possible.
///
/// Normalisation strips the sign, leading integer zeros, and trailing fractional
/// zeros, and records a distinguished `zero` so `-0`, `0.0` and `0` all compare
/// equal with no sign. The `int`/`frac` slices borrow the parsed input. Produced
/// by [`parseDec`] and ordered by [`cmpMag`]/[`cmpDecimalStr`].
const Dec = struct {
    /// True if the value is negative; forced false when [`Dec.zero`] is set so
    /// zero has no sign.
    neg: bool,
    /// Integer-part digits with leading zeros removed (empty means the integer
    /// part is zero).
    int: []const u8,
    /// Fractional-part digits with trailing zeros removed (empty means no
    /// fraction).
    frac: []const u8,
    /// True when the whole magnitude is zero (integer and fraction both empty).
    zero: bool,
};

/// Parses a decimal literal string into a normalised [`Dec`], or `null` if it is
/// not a valid plain decimal.
///
/// Accepts an optional leading `+`/`-`, then digits with at most one `.`. Rejects
/// empty input, a second decimal point, or any non-digit character (so no
/// exponent notation). Leading integer zeros and trailing fractional zeros are
/// trimmed to canonical form, and an all-zero magnitude sets `zero` and clears
/// the sign so `-0` == `0`. Underlies exact money/DECIMAL comparison in
/// [`cmpDecimalStr`].
fn parseDec(s_in: []const u8) ?Dec {
    const s = std.mem.trim(u8, s_in, " \t");
    if (s.len == 0) return null;
    var i: usize = 0;
    var neg = false;
    if (s[0] == '+' or s[0] == '-') {
        neg = s[0] == '-';
        i = 1;
    }
    const rest = s[i..];
    if (rest.len == 0) return null;
    var dot: ?usize = null;
    for (rest, 0..) |c, k| {
        if (c == '.') {
            if (dot != null) return null;
            dot = k;
        } else if (c < '0' or c > '9') {
            return null;
        }
    }
    var int_part = if (dot) |d| rest[0..d] else rest;
    var frac_part = if (dot) |d| rest[d + 1 ..] else rest[rest.len..];
    while (int_part.len > 0 and int_part[0] == '0') int_part = int_part[1..];
    while (frac_part.len > 0 and frac_part[frac_part.len - 1] == '0') frac_part = frac_part[0 .. frac_part.len - 1];
    const zero = int_part.len == 0 and frac_part.len == 0;
    return Dec{ .neg = neg and !zero, .int = int_part, .frac = frac_part, .zero = zero };
}

/// Compares the magnitudes (ignoring sign) of two normalised decimals, returning
/// -1, 0, or 1.
///
/// Because both are normalised, the integer part with more digits is the larger
/// magnitude; on equal integer length it compares integer digits lexically (which
/// equals numeric order for equal-length digit strings), then compares fractional
/// digits position by position, treating a missing digit as `'0'`. Sign is
/// applied by the caller [`cmpDecimalStr`].
fn cmpMag(a: Dec, b: Dec) i32 {
    if (a.int.len != b.int.len) return if (a.int.len < b.int.len) -1 else 1;
    switch (std.mem.order(u8, a.int, b.int)) {
        .lt => return -1,
        .gt => return 1,
        .eq => {},
    }
    var k: usize = 0;
    while (k < a.frac.len or k < b.frac.len) : (k += 1) {
        const da: u8 = if (k < a.frac.len) a.frac[k] else '0';
        const dbb: u8 = if (k < b.frac.len) b.frac[k] else '0';
        if (da != dbb) return if (da < dbb) -1 else 1;
    }
    return 0;
}

/// Exactly compares two decimal strings, returning -1/0/1, or `null` if either is
/// not a plain decimal.
///
/// This is the money/DECIMAL comparison path used first by [`compareValues`], so
/// that values like `"1.10"` and `"1.1"` compare equal and large exact decimals
/// order correctly, which an `f64` round-trip would get wrong. Both sides are
/// normalised with [`parseDec`]; two zeros are equal, differing signs decide
/// directly, and otherwise the sign is applied to the [`cmpMag`] magnitude order.
pub fn cmpDecimalStr(a_in: []const u8, b_in: []const u8) ?i32 {
    const a = parseDec(a_in) orelse return null;
    const b = parseDec(b_in) orelse return null;
    if (a.zero and b.zero) return 0;
    if (a.neg != b.neg) return if (a.neg) -1 else 1;
    const mag = cmpMag(a, b);
    return if (a.neg) -mag else mag;
}

/// Compares two JSON values under a SQL comparison operator, coercing across
/// representations.
///
/// Because column data is loosely typed JSON, this tries progressively weaker
/// interpretations and uses the first that applies to BOTH sides:
///   1. If both are strings that parse as decimals, exact decimal comparison via
///      [`cmpDecimalStr`] (keeps money/DECIMAL precise).
///   2. Integer comparison (`.integer`, or a string that parses as an integer).
///   3. Float comparison (`.float`, a widened integer, or a numeric string).
///   4. Lexicographic string comparison, formatting integers/floats to text as a
///      last resort (this path allocates two temporaries from
///      `std.heap.page_allocator` and frees them on the way out).
///
/// Returns a plain `bool` (not [`Bool3`]); NULL handling lives in the callers
/// ([`eval3`] treats a missing operand as `unknown` before ever calling here).
/// A non-comparison `op` or a value that fits no path returns `false`. An
/// allocation failure in the string fallback also returns `false`.
pub fn compareValues(left: Scalar, op: ast.OpType, right: Scalar) bool {
    {
        const ls: ?[]const u8 = switch (left) {
            .string => |s| s,
            else => null,
        };
        const rs: ?[]const u8 = switch (right) {
            .string => |s| s,
            else => null,
        };
        if (ls != null and rs != null) {
            if (cmpDecimalStr(ls.?, rs.?)) |c| {
                return switch (op) {
                    .EQ => c == 0,
                    .NE => c != 0,
                    .GT => c > 0,
                    .LT => c < 0,
                    .GTE => c >= 0,
                    .LTE => c <= 0,
                    else => false,
                };
            }
        }
    }
    const l_num: ?i64 = switch (left) {
        .integer => |i| i,
        .string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
    const r_num: ?i64 = switch (right) {
        .integer => |i| i,
        .string => |s| std.fmt.parseInt(i64, s, 10) catch null,
        else => null,
    };
    if (l_num) |a| {
        if (r_num) |b| {
            return switch (op) {
                .EQ => a == b,
                .NE => a != b,
                .GT => a > b,
                .LT => a < b,
                .GTE => a >= b,
                .LTE => a <= b,
                else => false,
            };
        }
    }

    const l_f: ?f64 = switch (left) {
        .float => |f| f,
        .integer => |i| @as(f64, @floatFromInt(i)),
        .string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
    const r_f: ?f64 = switch (right) {
        .float => |f| f,
        .integer => |i| @as(f64, @floatFromInt(i)),
        .string => |s| std.fmt.parseFloat(f64, s) catch null,
        else => null,
    };
    if (l_f) |a| {
        if (r_f) |b| {
            return switch (op) {
                .EQ => a == b,
                .NE => a != b,
                .GT => a > b,
                .LT => a < b,
                .GTE => a >= b,
                .LTE => a <= b,
                else => false,
            };
        }
    }

    const l_str = switch (left) {
        .string => |s| s,
        .integer => |i| std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{i}) catch return false,
        .float => |f| std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{f}) catch return false,
        else => return false,
    };
    defer if (left == .integer or left == .float) std.heap.page_allocator.free(l_str);

    const r_str = switch (right) {
        .string => |s| s,
        .integer => |i| std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{i}) catch return false,
        .float => |f| std.fmt.allocPrint(std.heap.page_allocator, "{d}", .{f}) catch return false,
        else => return false,
    };
    defer if (right == .integer or right == .float) std.heap.page_allocator.free(r_str);

    return switch (op) {
        .EQ => std.mem.eql(u8, l_str, r_str),
        .NE => !std.mem.eql(u8, l_str, r_str),
        .GT => std.mem.order(u8, l_str, r_str) == .gt,
        .LT => std.mem.order(u8, l_str, r_str) == .lt,
        .GTE => std.mem.order(u8, l_str, r_str) != .lt,
        .LTE => std.mem.order(u8, l_str, r_str) != .gt,
        else => false,
    };
}

/// Evaluates a `WHERE`/`ON` predicate against a combined [`Row`], returning true
/// only for a definite match.
///
/// Wraps the row in a [`RowResolver`] and passes it to [`eval3`]; `unknown`
/// (NULL) is treated as NOT matching, which is the correct SQL filter semantics.
/// This is the predicate hook the [`FilterIterator`] and join `ON` checks call.
pub fn evalExpr(expr: *const ast.Expr, row: Row) bool {
    return eval3(expr, RowResolver{ .row = row }) == .true;
}

/// Like [`evalExpr`] but evaluates the predicate against a single JSON object via
/// [`JsonResolver`], for row-at-a-time checks that are not shaped as a [`Row`].
pub fn evalExprJson(expr: *const ast.Expr, row: TableRow) bool {
    return eval3(expr, JsonResolver{ .row = row }) == .true;
}

/// Evaluates a scalar expression against a single JSON object, returning its
/// value or `null`.
///
/// The [`JsonResolver`] counterpart of [`evalScalar`]; used to compute a
/// projected/derived value from one materialised row outside the operator
/// pipeline.
pub fn evalScalarJson(expr: *const ast.Expr, row: TableRow) ?Scalar {
    return evalScalar(expr, JsonResolver{ .row = row });
}

/// Deep-copies a `Scalar` so it can outlive the storage it was decoded
/// from.
///
/// Scans hand out rows that borrow scan-owned storage valid only until the next
/// pull (see [`Row`]); the join operators must retain right-side (and, for the
/// hash join, left-side) rows across many pulls, so they clone with this. Scalars
/// are copied by value; strings, `number_string`, arrays and objects are
/// recursively duplicated into `allocator`, with `errdefer` cleanup on partial
/// failure. Everything it allocates must later be released by [`freeClonedJson`]
/// with the same allocator. Returns an error only on allocation failure.
pub fn cloneJson(allocator: Allocator, val: std.json.Value) anyerror!std.json.Value {
    switch (val) {
        .null => return .null,
        .bool => |b| return .{ .bool = b },
        .integer => |i| return .{ .integer = i },
        .float => |f| return .{ .float = f },
        .number_string => |s| return .{ .number_string = try allocator.dupe(u8, s) },
        .string => |s| return .{ .string = try allocator.dupe(u8, s) },
        .array => |arr| {
            var new_arr = std.json.Array.init(allocator);
            errdefer new_arr.deinit();
            for (arr.items) |item| {
                try new_arr.append(try cloneJson(allocator, item));
            }
            return .{ .array = new_arr };
        },
        .object => |obj| {
            var new_obj = std.json.ObjectMap.empty;
            errdefer new_obj.deinit(allocator);
            var it = obj.iterator();
            while (it.next()) |entry| {
                const cloned_key = try allocator.dupe(u8, entry.key_ptr.*);
                const cloned_val = try cloneJson(allocator, entry.value_ptr.*);
                try new_obj.put(allocator, cloned_key, cloned_val);
            }
            return .{ .object = new_obj };
        },
    }
}

/// Frees a value previously produced by [`cloneJson`], recursively.
///
/// Must be paired with the SAME allocator that [`cloneJson`] used. Frees owned
/// strings/`number_string`, then array elements and the array, and for objects
/// both the duplicated keys and the recursively cloned values before deinit-ing
/// the map. Scalars own nothing and are a no-op. Do NOT call this on a value that
/// was not produced by [`cloneJson`] (for example a scan's borrowed row).
pub fn freeClonedJson(allocator: Allocator, val: std.json.Value) void {
    switch (val) {
        .number_string => |s| allocator.free(s),
        .string => |s| allocator.free(s),
        .array => |arr| {
            for (arr.items) |item| freeClonedJson(allocator, item);
            var mut_arr = arr;
            mut_arr.deinit();
        },
        .object => |obj| {
            var mut_obj = obj;
            var it = mut_obj.iterator();
            while (it.next()) |entry| {
                allocator.free(entry.key_ptr.*);
                freeClonedJson(allocator, entry.value_ptr.*);
            }
            mut_obj.deinit(allocator);
        },
        else => {},
    }
}


/// Full-table (or range) scan leaf operator: walks every cell of a table's
/// B+Tree and emits the rows visible to the scanning transaction.
///
/// This is the base-case producer for a single table. It holds a live B+Tree
/// [`Iterator`] and, per cell, asks the executor for the MVCC-visible version
/// (`getVisibleVersion` with `current_tx`), skipping cells with no visible
/// version. Only one decoded row is held at a time in `current_row_json`, freed
/// at the start of the next pull, so the emitted [`Row`] is borrowed until then.
/// `tables_slice`/`data_slice` are one-element inline arrays reused across pulls
/// (this operator always yields a width-1 row). An optional start key turns a
/// full scan into a range scan by seeking with `iteratorAfter`. Construct with
/// [`TableScanIterator.init`] and expose via [`TableScanIterator.iterator`].
pub const TableScanIterator = struct {
    /// Allocator owning this operator struct and its decoded rows.
    allocator: Allocator,
    /// Executor supplying MVCC visibility, deadline checks, and JSON freeing.
    exec: *QueryExecutor,
    /// The catalog table being scanned (carries name and column metadata).
    table: Table,
    /// The table's data B+Tree; `deinit` drops the reference taken on it.
    table_tree: *BPlusTree,
    /// Live cursor over `table_tree` cells, positioned by [`TableScanIterator.init`].
    btree_iter: BPlusTreeIterator,
    /// The transaction id whose snapshot decides row visibility.
    current_tx: u64,
    /// The one decoded row currently borrowed by the last-returned [`Row`]; freed
    /// before decoding the next and in `deinit`.
    current_row_json: ?TableRow = null,
    /// Reusable one-element table-name array (always this table's name).
    tables_slice: [1][]const u8,
    /// Reusable one-element data array holding the current row's object.
    data_slice: [1]TableRow = undefined,

    /// Allocates and initialises a table scan, optionally seeking to a start key.
    ///
    /// With `opt_start_key` the underlying cursor is positioned with
    /// `iteratorAfter(start_key)` for a range scan; without it a full-table
    /// `iterator()` is used. Returns a heap-allocated operator owned by
    /// `allocator`; the caller drives it through [`TableScanIterator.iterator`]
    /// and must eventually `deinit` the resulting [`RowIterator`].
    pub fn init(allocator: Allocator, exec: *QueryExecutor, table: Table, table_tree: *BPlusTree, current_tx: u64, opt_start_key: ?[]const u8) anyerror!*TableScanIterator {
        const self = try allocator.create(TableScanIterator);
        const btree_iter = if (opt_start_key) |start_key|
            try table_tree.iteratorAfter(start_key)
        else
            try table_tree.iterator();

        self.* = .{
            .allocator = allocator,
            .exec = exec,
            .table = table,
            .table_tree = table_tree,
            .btree_iter = btree_iter,
            .current_tx = current_tx,
            .tables_slice = .{table.name},
        };
        return self;
    }

    /// Returns the type-erased [`RowIterator`] view of this scan.
    ///
    /// The `next` closure checks the query deadline, frees the previous decoded
    /// row, then advances the B+Tree cursor until it finds a cell with an
    /// MVCC-visible version for `current_tx` (passing the overflow flag through so
    /// large values are reassembled), emitting that as a width-1 [`Row`]; it
    /// returns `null` at end of tree. The `deinit` closure frees any held row,
    /// releases the B+Tree cursor and the tree reference, and destroys the
    /// operator.
    pub fn iterator(self: *TableScanIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *TableScanIterator = @alignCast(@ptrCast(ctx));
                    try s.exec.checkDeadline();
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                        s.current_row_json = null;
                    }

                    while (try s.btree_iter.next()) |cell| {
                        if (try s.exec.getVisibleVersion(s.table, cell.value, cell.flags.value_overflow, s.current_tx)) |visible_row| {
                            s.current_row_json = visible_row;
                            s.data_slice[0] = visible_row;
                            return Row{
                                .tables = &s.tables_slice,
                                .data = &s.data_slice,
                            };
                        }
                    }
                    return null;
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *TableScanIterator = @alignCast(@ptrCast(ctx));
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                    }
                    s.btree_iter.deinit();
                    s.table_tree.deinit();
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};


/// In-memory leaf operator over a pre-built slice of rows.
///
/// Unlike [`TableScanIterator`], which decodes rows from a B+Tree on demand, this
/// operator is handed a fully materialised `[]TableRow` at construction and simply
/// replays it. It is used for the `sys.*` catalog tables, whose rows the executor
/// synthesises from the in-memory catalog rather than reading raw metadata blobs
/// out of storage (those blobs are not in the MVCC row layout the scan decoder
/// expects, so a normal table scan cannot read them). The rest of the pipeline
/// (filter, projection, sort, limit) then applies unchanged, so a query like
/// `SELECT name, data_type FROM sys.columns WHERE table_name = 't' ORDER BY
/// ordinal` works exactly as it would over a real table.
///
/// It owns every row in `rows` and frees them all in `deinit` (via
/// [`freeTableRow`]). Because it keeps all rows alive for its whole lifetime it
/// more than satisfies the [`Row`] borrow contract (a returned row stays valid
/// until `deinit`, not merely until the next `next`).
pub const MaterializedScanIterator = struct {
    allocator: Allocator,
    /// The synthesised rows, owned by this operator.
    rows: []TableRow,
    /// Cursor into `rows`.
    idx: usize = 0,
    /// Reusable one-element table-name array (the catalog table's name).
    tables_slice: [1][]const u8,
    /// Reusable one-element data array pointing at the current row.
    data_slice: [1]TableRow = undefined,

    /// Builds the operator. Takes ownership of `rows` (and every `TableRow` in it);
    /// `table_name` must outlive the operator (catalog names are schema-stable).
    pub fn init(allocator: Allocator, table_name: []const u8, rows: []TableRow) !*MaterializedScanIterator {
        const self = try allocator.create(MaterializedScanIterator);
        self.* = .{
            .allocator = allocator,
            .rows = rows,
            .tables_slice = .{table_name},
        };
        return self;
    }

    /// Returns the type-erased [`RowIterator`] view. The `next` closure emits each
    /// row in turn as a width-1 [`Row`], returning `null` once the slice is
    /// exhausted; `deinit` frees every row and the operator.
    pub fn iterator(self: *MaterializedScanIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *MaterializedScanIterator = @alignCast(@ptrCast(ctx));
                    try s.exec_checkDeadline();
                    if (s.idx >= s.rows.len) return null;
                    s.data_slice[0] = s.rows[s.idx];
                    s.idx += 1;
                    return Row{
                        .tables = &s.tables_slice,
                        .data = &s.data_slice,
                    };
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *MaterializedScanIterator = @alignCast(@ptrCast(ctx));
                    for (s.rows) |r| freeTableRow(s.allocator, r);
                    s.allocator.free(s.rows);
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }

    /// A materialised scan has no deadline-sensitive I/O, so this is a no-op kept
    /// only so the `next` closure reads like the storage scans.
    fn exec_checkDeadline(self: *MaterializedScanIterator) !void {
        _ = self;
    }
};


/// Secondary-index scan leaf operator: walks an index B+Tree over a value prefix,
/// then fetches and MVCC-filters the matching base-table rows.
///
/// Index entries are keyed `"<indexed-value>:<primary-key>"`. This operator seeks
/// the index cursor to `index_prefix` and, for each entry that still starts with
/// `prefix_colon` (the prefix plus `:`), extracts the primary key suffix, looks
/// it up in the base table, and yields the row if it has a version visible to
/// `current_tx`. The scan stops at the first index key that no longer matches the
/// prefix (index order guarantees matches are contiguous). Like the other scans
/// it holds one decoded row at a time; the base-table `search` result is a
/// short-lived buffer freed within the loop. Build with
/// [`IndexScanIterator.init`], drive via [`IndexScanIterator.iterator`].
pub const IndexScanIterator = struct {
    /// Allocator owning this operator, its decoded rows, and `prefix_colon`.
    allocator: Allocator,
    /// Executor supplying MVCC visibility, deadline checks, and JSON freeing.
    exec: *QueryExecutor,
    /// The base table whose rows are ultimately produced.
    table: Table,
    /// The base-table data B+Tree, searched by primary key per index hit.
    table_tree: *BPlusTree,
    /// The secondary-index B+Tree being scanned.
    idx_tree: *BPlusTree,
    /// Live cursor over `idx_tree`, seeked to `index_prefix`.
    idx_iter: BPlusTreeIterator,
    /// The match prefix with a trailing `:` appended, used both to test whether
    /// an index key still matches and to locate where the primary key begins.
    prefix_colon: []const u8,
    /// The transaction id whose snapshot decides row visibility.
    current_tx: u64,
    /// The one decoded row currently borrowed by the last-returned [`Row`].
    current_row_json: ?TableRow = null,
    /// Reusable one-element table-name array (always the base table's name).
    tables_slice: [1][]const u8,
    /// Reusable one-element data array holding the current row's object.
    data_slice: [1]TableRow = undefined,
    /// Optional residual WHERE pushed down by the planner: when set, the base
    /// fetch rejects non-matching rows on the raw image before building their
    /// JSON (see `QueryExecutor.fetchVisibleFilteredJson`). The downstream
    /// FilterIterator still enforces correctness; this only skips wasted builds.
    residual: ?*const ast.Expr = null,
    residual_cols: ?[]const []const u8 = null,

    /// Look-ahead buffer of matching primary keys pulled from the index
    /// (index-only, cheap), kept in index order. Owned pk copies, freed on
    /// refill/deinit. Before the batch is served, the base-table leaf pages that
    /// these PKs map to are prefetched (see [`refillBatch`]) so the subsequent
    /// per-row lookups find their pages already in flight / cache-resident.
    pk_batch: std.ArrayList([]const u8) = .empty,
    /// Cursor into `pk_batch` for the current serve pass.
    pk_cursor: usize = 0,
    /// True once the index cursor has walked past the match prefix.
    index_done: bool = false,
    /// Look-ahead batch size = how many matching PKs to gather and prefetch at a
    /// time. Larger batches give the storage device more concurrent readahead
    /// (better latency hiding on a disk-bound random-fetch scan) at the cost of
    /// a bounded upfront index walk and memory. A downstream LIMIT still stops
    /// early: the batch is a look-ahead window, not a materialise-everything.
    ///
    /// `1` disables prefetch and streams one PK at a time (the old plain
    /// index-order fetch). Overridable with `NOVADB_PREFETCH_BATCH`.
    prefetch_batch: usize = DEFAULT_PREFETCH_BATCH,
    /// Reusable scratch for the base-leaf page ids of the current batch, so a
    /// scan does not allocate a fresh list per refill.
    leaf_ids: std.ArrayList(page.PageId) = .empty,
    /// Optional base-table leaf-reuse cursor (env `NOVADB_BASE_CURSOR`). The
    /// equality index yields PKs in ascending pk order, so a persistent leaf
    /// cursor answers most base fetches without re-descending. Held for the scan;
    /// closed in `deinit`. `prev_base_searcher` restores the executor's prior
    /// value (nested scans).
    base_cursor: ?BPlusTree.LeafReuseSearcher = null,
    prev_base_searcher: ?*BPlusTree.LeafReuseSearcher = null,

    /// Allocates and initialises an index scan seeked to `index_prefix`.
    ///
    /// Builds `prefix_colon` (the owned `"<index_prefix>:"` string) and positions
    /// the index cursor with `iteratorAfter(index_prefix)`. Returns a
    /// heap-allocated operator owned by `allocator`; drive it via
    /// [`IndexScanIterator.iterator`] and `deinit` the resulting [`RowIterator`].
    pub fn init(allocator: Allocator, exec: *QueryExecutor, table: Table, table_tree: *BPlusTree, idx_tree: *BPlusTree, index_prefix: []const u8, current_tx: u64) anyerror!*IndexScanIterator {
        const self = try allocator.create(IndexScanIterator);
        const prefix_colon = try std.fmt.allocPrint(allocator, "{s}:", .{index_prefix});
        self.* = .{
            .allocator = allocator,
            .exec = exec,
            .table = table,
            .table_tree = table_tree,
            .idx_tree = idx_tree,
            .idx_iter = try idx_tree.iteratorAfter(index_prefix),
            .prefix_colon = prefix_colon,
            .current_tx = current_tx,
            .tables_slice = .{table.name},
            .prefetch_batch = resolvePrefetchBatch(),
        };
        // Base-table leaf-reuse cursor (opt-in). Only for the equality scan,
        // whose PKs are pk-ascending, so leaf reuse actually hits.
        if (useBaseCursor()) {
            self.base_cursor = table_tree.leafReuseSearcher();
            self.prev_base_searcher = exec.base_searcher;
            exec.base_searcher = &self.base_cursor.?;
        }
        return self;
    }

    /// Refill `pk_batch` with up to `prefetch_batch` matching primary keys from
    /// the index (owned copies, index order), then issue OS readahead for the
    /// base-table leaf pages those PKs live on so the serve pass finds them warm.
    /// Returns false when the index is exhausted and no new keys were gathered.
    fn refillBatch(self: *IndexScanIterator) !bool {
        for (self.pk_batch.items) |pk| self.allocator.free(pk);
        self.pk_batch.clearRetainingCapacity();
        self.pk_cursor = 0;
        if (self.index_done) return false;
        while (self.pk_batch.items.len < self.prefetch_batch) {
            const idx_cell = (try self.idx_iter.next()) orelse {
                self.index_done = true;
                break;
            };
            if (!std.mem.startsWith(u8, idx_cell.key, self.prefix_colon)) {
                self.index_done = true;
                break;
            }
            const pk = try self.allocator.dupe(u8, idx_cell.key[self.prefix_colon.len..]);
            try self.pk_batch.append(self.allocator, pk);
        }
        if (self.pk_batch.items.len == 0) return false;

        // Prefetch pass: resolve this batch's PKs to base-table leaf pages and
        // hint the OS to read them ahead concurrently. Best-effort throughout,
        // so a batch of 1 (prefetch disabled) or any failure just skips it.
        //
        // ADAPTIVE: the prefetch only earns its keep when base pages are on disk
        // (it overlaps random-read latency). Once the working set is resident in
        // the buffer pool, the OS readahead hint is a no-op but `collectLeafPageIds`
        // still pays a full base-tree descent per PK - pure overhead that measurably
        // slowed warm wide-range scans (Q3/Q8). So gate it on the pool hit ratio:
        // run prefetch only while the pool is actually MISSING (cold/disk), and
        // skip it once the pool is serving from RAM. This keeps the cold-scan
        // speedup and removes the warm-scan tax, with no config knob.
        if (self.prefetch_batch > 1 and self.pk_batch.items.len > 1 and shouldPrefetch(self.table_tree)) {
            prefetchBaseLeaves(self.table_tree, self.pk_batch.items, &self.leaf_ids, self.allocator);
        }
        return true;
    }

    /// Returns the type-erased [`RowIterator`] view of this index scan.
    ///
    /// The `next` closure checks the deadline, frees the previous row, then walks
    /// index entries: it stops as soon as a key no longer starts with
    /// `prefix_colon`, otherwise it takes the primary-key suffix, `search`es the
    /// base table for it (freeing that temporary buffer via `defer`), and emits
    /// the row if a version is visible to `current_tx`. `deinit` frees any held
    /// row, both B+Tree cursors/references, the `prefix_colon` string, and the
    /// operator.
    pub fn iterator(self: *IndexScanIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *IndexScanIterator = @alignCast(@ptrCast(ctx));
                    try s.exec.checkDeadline();
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                        s.current_row_json = null;
                    }

                    // Bitmap scan: serve primary keys from the sorted batch so the
                    // base-table fetches walk the heap in physical order; refill
                    // from the index when the batch drains.
                    while (true) {
                        if (s.pk_cursor >= s.pk_batch.items.len) {
                            if (!try s.refillBatch()) return null;
                        }
                        const pk_val = s.pk_batch.items[s.pk_cursor];
                        s.pk_cursor += 1;

                        if (try s.exec.fetchVisibleFilteredJson(s.table, s.table_tree, pk_val, s.current_tx, s.residual, s.residual_cols)) |visible_row| {
                            s.current_row_json = visible_row;
                            s.data_slice[0] = visible_row;
                            return Row{
                                .tables = &s.tables_slice,
                                .data = &s.data_slice,
                            };
                        }
                    }
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *IndexScanIterator = @alignCast(@ptrCast(ctx));
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                    }
                    for (s.pk_batch.items) |pk| s.allocator.free(pk);
                    s.pk_batch.deinit(s.allocator);
                    s.leaf_ids.deinit(s.allocator);
                    if (s.base_cursor) |*c| {
                        s.exec.base_searcher = s.prev_base_searcher;
                        c.deinit();
                    }
                    s.idx_iter.deinit();
                    s.idx_tree.deinit();
                    s.table_tree.deinit();
                    s.allocator.free(s.prefix_colon);
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};

/// A bounded secondary-index range scan: walks the index over the encoded value
/// range `[start_key, end_key]` and emits each base-table row.
///
/// Unlike [`IndexScanIterator`] (which walks a single equality prefix), this
/// operator seeks to `start_key` and streams index entries whose key is
/// `<= end_key` (a `null` `end_key` scans to the end of the index), stopping
/// early as the consumer stops pulling. Because the leading index-column value
/// is stored in the order-preserving, colon-free encoding
/// ([`schema.types.encodeIndexValueAlloc`]), lexical index order is the column's
/// natural order, so the caller can encode a `col BETWEEN lo AND hi` / `col > x`
/// predicate directly into `start_key`/`end_key` (inclusive vs exclusive bounds
/// are expressed by appending `":\xFF"` to include a value's whole `pk` fan-out,
/// or omitting it to exclude it). The pk is the suffix after the first `:` in the
/// index key. Rows are still MVCC-filtered against `current_tx`. This is the SQL
/// analogue of the document surface's index range scan; combined with the
/// executor's streaming `LIMIT` break it turns a broad `LIMIT k` range query
/// from a full table scan into an O(k) index walk.
///
/// Drive via [`IndexRangeScanIterator.iterator`]; `deinit` the resulting
/// [`RowIterator`].
/// Returns the primary-key suffix of a composite index key: everything after
/// the `n`-th `:`. For a single-column index the key is `enc:pk` (n = 1); for a
/// K-column composite index it is `enc0:enc1:...:pk` and the pk begins after the
/// K-th colon (n = K). The leading encoded fields are colon-free, so the first
/// `n` colons are exactly the field separators; the pk itself may contain
/// colons (it is the whole remaining suffix). Returns null if there are fewer
/// than `n` colons.
fn pkAfterNthColon(key: []const u8, n: usize) ?[]const u8 {
    var pos: usize = 0;
    var found: usize = 0;
    while (found < n) : (found += 1) {
        const c = std.mem.indexOfScalarPos(u8, key, pos, ':') orelse return null;
        pos = c + 1;
    }
    return key[pos..];
}

pub const IndexRangeScanIterator = struct {
    /// Allocator owning this operator, its decoded rows, and the bound keys.
    allocator: Allocator,
    /// Executor supplying MVCC visibility, deadline checks, and JSON freeing.
    exec: *QueryExecutor,
    /// The base table whose rows are ultimately produced.
    table: Table,
    /// The base-table data B+Tree, searched by primary key per index hit.
    table_tree: *BPlusTree,
    /// The secondary-index B+Tree being range-scanned.
    idx_tree: *BPlusTree,
    /// Live bounded cursor over `idx_tree`, `[start_key, end_key]`.
    range_iter: BPlusTreeRangeIterator,
    /// Owned copy of the inclusive lower-bound seek key (may be empty).
    start_key: []const u8,
    /// Owned copy of the inclusive upper-bound key, or null for unbounded.
    end_key: ?[]const u8,
    /// The transaction id whose snapshot decides row visibility.
    current_tx: u64,
    /// The one decoded row currently borrowed by the last-returned [`Row`].
    current_row_json: ?TableRow = null,
    /// Reusable one-element table-name array (always the base table's name).
    tables_slice: [1][]const u8,
    /// Reusable one-element data array holding the current row's object.
    data_slice: [1]TableRow = undefined,
    /// Optional residual WHERE pushed down by the planner (see IndexScanIterator).
    residual: ?*const ast.Expr = null,
    residual_cols: ?[]const []const u8 = null,
    /// Which `:` in the index key delimits the primary key (1 = single-column
    /// index `enc:pk`; 2 = composite `enc0:enc1:pk`, i.e. a prefix scan over a
    /// composite index). See [`pkAfterNthColon`].
    pk_after_colon: usize = 1,
    /// Look-ahead buffer of matching PKs (owned copies, range order) whose base
    /// leaves are prefetched before the batch is served; see [`refillBatch`].
    pk_batch: std.ArrayList([]const u8) = .empty,
    /// Cursor into `pk_batch` for the current serve pass.
    pk_cursor: usize = 0,
    /// True once the bounded range cursor is exhausted.
    range_done: bool = false,
    /// Look-ahead / prefetch batch size (see [`resolvePrefetchBatch`]).
    prefetch_batch: usize = DEFAULT_PREFETCH_BATCH,
    /// Reusable scratch for the batch's base-leaf page ids.
    leaf_ids: std.ArrayList(page.PageId) = .empty,

    /// Refill `pk_batch` with up to `prefetch_batch` PKs from the bounded range
    /// cursor (range order preserved), then prefetch their base leaves. Returns
    /// false when the range is exhausted and no new keys were gathered.
    fn refillBatch(self: *IndexRangeScanIterator) !bool {
        for (self.pk_batch.items) |pk| self.allocator.free(pk);
        self.pk_batch.clearRetainingCapacity();
        self.pk_cursor = 0;
        if (self.range_done) return false;
        while (self.pk_batch.items.len < self.prefetch_batch) {
            const idx_cell = (try self.range_iter.next()) orelse {
                self.range_done = true;
                break;
            };
            const pk_val = pkAfterNthColon(idx_cell.key, self.pk_after_colon) orelse continue;
            const pk = try self.allocator.dupe(u8, pk_val);
            try self.pk_batch.append(self.allocator, pk);
        }
        if (self.pk_batch.items.len == 0) return false;
        if (self.prefetch_batch > 1 and self.pk_batch.items.len > 1) {
            prefetchBaseLeaves(self.table_tree, self.pk_batch.items, &self.leaf_ids, self.allocator);
        }
        return true;
    }

    /// Allocates and initialises a range scan over `[start_key, end_key]`.
    ///
    /// Duplicates the bound keys (so the caller may free its copies) and opens a
    /// [`BPlusTree.rangeScan`] positioned at the first index key `>= start_key`.
    /// Returns a heap-allocated operator owned by `allocator`; drive it via
    /// [`IndexRangeScanIterator.iterator`] and `deinit` the resulting
    /// [`RowIterator`].
    pub fn init(allocator: Allocator, exec: *QueryExecutor, table: Table, table_tree: *BPlusTree, idx_tree: *BPlusTree, start_key: []const u8, end_key: ?[]const u8, current_tx: u64) anyerror!*IndexRangeScanIterator {
        const self = try allocator.create(IndexRangeScanIterator);
        const start_owned = try allocator.dupe(u8, start_key);
        errdefer allocator.free(start_owned);
        const end_owned: ?[]const u8 = if (end_key) |ek| try allocator.dupe(u8, ek) else null;
        errdefer if (end_owned) |eo| allocator.free(eo);
        self.* = .{
            .allocator = allocator,
            .exec = exec,
            .table = table,
            .table_tree = table_tree,
            .idx_tree = idx_tree,
            .range_iter = try idx_tree.rangeScan(start_owned, end_owned),
            .start_key = start_owned,
            .end_key = end_owned,
            .current_tx = current_tx,
            .tables_slice = .{table.name},
            .prefetch_batch = resolvePrefetchBatch(),
        };
        return self;
    }

    /// Returns the type-erased [`RowIterator`] view of this range scan.
    ///
    /// The `next` closure checks the deadline, frees the previous row, then walks
    /// bounded index cells: for each it takes the primary-key suffix (after the
    /// first `:`), `search`es the base table for it (freeing that temporary
    /// buffer via `defer`), and emits the row if a version is visible to
    /// `current_tx`. `deinit` frees any held row, the range cursor, both B+Tree
    /// references, the bound keys, and the operator.
    pub fn iterator(self: *IndexRangeScanIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *IndexRangeScanIterator = @alignCast(@ptrCast(ctx));
                    try s.exec.checkDeadline();
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                        s.current_row_json = null;
                    }

                    while (true) {
                        if (s.pk_cursor >= s.pk_batch.items.len) {
                            if (!try s.refillBatch()) return null;
                        }
                        const pk_val = s.pk_batch.items[s.pk_cursor];
                        s.pk_cursor += 1;

                        if (try s.exec.fetchVisibleFilteredJson(s.table, s.table_tree, pk_val, s.current_tx, s.residual, s.residual_cols)) |visible_row| {
                            s.current_row_json = visible_row;
                            s.data_slice[0] = visible_row;
                            return Row{
                                .tables = &s.tables_slice,
                                .data = &s.data_slice,
                            };
                        }
                    }
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *IndexRangeScanIterator = @alignCast(@ptrCast(ctx));
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                    }
                    for (s.pk_batch.items) |pk| s.allocator.free(pk);
                    s.pk_batch.deinit(s.allocator);
                    s.leaf_ids.deinit(s.allocator);
                    s.range_iter.deinit();
                    s.idx_tree.deinit();
                    s.table_tree.deinit();
                    s.allocator.free(s.start_key);
                    if (s.end_key) |ek| s.allocator.free(ek);
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};

/// A DESCENDING bounded secondary-index range scan: the mirror of
/// [`IndexRangeScanIterator`], walking the index over `[start_key, end_key]` from
/// the HIGHEST key down and emitting each base-table row. Because the leading
/// index-column value is stored order-preservingly, descending index order is the
/// column's descending order, so `ORDER BY col DESC LIMIT k` is served directly
/// from the scan (the executor's streaming LIMIT break stops it after k rows)
/// instead of scanning the whole range, materialising, and sorting. Uses the
/// btree [`RangeIteratorDesc`] backward cursor. Rows are MVCC-filtered against
/// `current_tx`, and a pushed residual is pre-filtered on the raw image exactly
/// as in the ascending scan. Drive via [`IndexRangeScanDescIterator.iterator`];
/// `deinit` the resulting [`RowIterator`].
pub const IndexRangeScanDescIterator = struct {
    allocator: Allocator,
    exec: *QueryExecutor,
    table: Table,
    table_tree: *BPlusTree,
    idx_tree: *BPlusTree,
    range_iter: BPlusTreeRangeIteratorDesc,
    start_key: ?[]const u8,
    end_key: ?[]const u8,
    current_tx: u64,
    current_row_json: ?TableRow = null,
    tables_slice: [1][]const u8,
    data_slice: [1]TableRow = undefined,
    residual: ?*const ast.Expr = null,
    residual_cols: ?[]const []const u8 = null,
    /// Which `:` delimits the pk (1 = single-column, 2 = composite prefix scan).
    pk_after_colon: usize = 1,
    /// Look-ahead buffer of matching PKs (owned copies, DESC order preserved)
    /// whose base leaves are prefetched before the batch is served.
    pk_batch: std.ArrayList([]const u8) = .empty,
    pk_cursor: usize = 0,
    range_done: bool = false,
    prefetch_batch: usize = DEFAULT_PREFETCH_BATCH,
    leaf_ids: std.ArrayList(page.PageId) = .empty,

    /// Refill `pk_batch` from the descending range cursor (DESC order preserved),
    /// then prefetch the batch's base leaves. False when the range is exhausted.
    fn refillBatch(self: *IndexRangeScanDescIterator) !bool {
        for (self.pk_batch.items) |pk| self.allocator.free(pk);
        self.pk_batch.clearRetainingCapacity();
        self.pk_cursor = 0;
        if (self.range_done) return false;
        while (self.pk_batch.items.len < self.prefetch_batch) {
            const idx_cell = (try self.range_iter.next()) orelse {
                self.range_done = true;
                break;
            };
            const pk_val = pkAfterNthColon(idx_cell.key, self.pk_after_colon) orelse continue;
            const pk = try self.allocator.dupe(u8, pk_val);
            try self.pk_batch.append(self.allocator, pk);
        }
        if (self.pk_batch.items.len == 0) return false;
        if (self.prefetch_batch > 1 and self.pk_batch.items.len > 1) {
            prefetchBaseLeaves(self.table_tree, self.pk_batch.items, &self.leaf_ids, self.allocator);
        }
        return true;
    }

    /// `start_key`/`end_key` are the inclusive encoded bounds (either may be null
    /// for an open side); duplicated so the caller may free its copies.
    pub fn init(allocator: Allocator, exec: *QueryExecutor, table: Table, table_tree: *BPlusTree, idx_tree: *BPlusTree, start_key: ?[]const u8, end_key: ?[]const u8, current_tx: u64) anyerror!*IndexRangeScanDescIterator {
        const self = try allocator.create(IndexRangeScanDescIterator);
        const start_owned: ?[]const u8 = if (start_key) |sk| try allocator.dupe(u8, sk) else null;
        errdefer if (start_owned) |so| allocator.free(so);
        const end_owned: ?[]const u8 = if (end_key) |ek| try allocator.dupe(u8, ek) else null;
        errdefer if (end_owned) |eo| allocator.free(eo);
        self.* = .{
            .allocator = allocator,
            .exec = exec,
            .table = table,
            .table_tree = table_tree,
            .idx_tree = idx_tree,
            .range_iter = try idx_tree.rangeScanDesc(start_owned, end_owned),
            .start_key = start_owned,
            .end_key = end_owned,
            .current_tx = current_tx,
            .tables_slice = .{table.name},
            .prefetch_batch = resolvePrefetchBatch(),
        };
        return self;
    }

    pub fn iterator(self: *IndexRangeScanDescIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *IndexRangeScanDescIterator = @alignCast(@ptrCast(ctx));
                    try s.exec.checkDeadline();
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                        s.current_row_json = null;
                    }

                    while (true) {
                        if (s.pk_cursor >= s.pk_batch.items.len) {
                            if (!try s.refillBatch()) return null;
                        }
                        const pk_val = s.pk_batch.items[s.pk_cursor];
                        s.pk_cursor += 1;

                        if (try s.exec.fetchVisibleFilteredJson(s.table, s.table_tree, pk_val, s.current_tx, s.residual, s.residual_cols)) |visible_row| {
                            s.current_row_json = visible_row;
                            s.data_slice[0] = visible_row;
                            return Row{
                                .tables = &s.tables_slice,
                                .data = &s.data_slice,
                            };
                        }
                    }
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *IndexRangeScanDescIterator = @alignCast(@ptrCast(ctx));
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                    }
                    for (s.pk_batch.items) |pk| s.allocator.free(pk);
                    s.pk_batch.deinit(s.allocator);
                    s.leaf_ids.deinit(s.allocator);
                    s.range_iter.deinit();
                    s.idx_tree.deinit();
                    s.table_tree.deinit();
                    if (s.start_key) |sk| s.allocator.free(sk);
                    if (s.end_key) |ek| s.allocator.free(ek);
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};

/// A secondary-index UNION scan for `col IN (v1, v2, ...)`: one equality-prefix
/// seek per value, concatenated, each yielding the matching base-table rows.
///
/// The planner builds this instead of a full table scan when `IN` targets an
/// indexed column (`getInListForCol`). It walks the values in list order; for
/// each it seeks `iteratorAfter(enc(v))` and emits base rows while the index key
/// still starts with `enc(v):`, then advances to the next value. The pk is the
/// suffix after that prefix. Rows are MVCC-filtered against `current_tx`, and the
/// residual `WHERE` FILTER still wraps this, so duplicate/overlapping list values
/// (or an unindexed extra predicate) remain correct; the streaming `LIMIT` break
/// stops it early. Drive via [`IndexInScanIterator.iterator`]; `deinit` the
/// resulting [`RowIterator`].
pub const IndexInScanIterator = struct {
    allocator: Allocator,
    exec: *QueryExecutor,
    table: Table,
    table_tree: *BPlusTree,
    idx_tree: *BPlusTree,
    /// Encoded seek keys (one per IN value), owned.
    seeks: [][]const u8,
    /// Match prefixes `enc(v) ++ ":"` (one per value), owned; used to test whether
    /// an index key still belongs to the current value and to locate the pk.
    matches: [][]const u8,
    /// Index of the value currently being scanned.
    cur: usize,
    /// Live cursor over `idx_tree` for value `cur`; valid until `done`.
    idx_iter: BPlusTreeIterator,
    /// True once every value has been scanned (guards `idx_iter` re-use/deinit).
    done: bool,
    /// Transaction id whose snapshot decides row visibility.
    current_tx: u64,
    current_row_json: ?TableRow = null,
    tables_slice: [1][]const u8,
    data_slice: [1]TableRow = undefined,
    /// Optional residual WHERE pushed down by the planner (see IndexScanIterator).
    residual: ?*const ast.Expr = null,
    residual_cols: ?[]const []const u8 = null,

    /// `encoded_values` are index-encoded membership values (caller keeps
    /// ownership of the slice and entries; this dups what it needs). Requires at
    /// least one value.
    pub fn init(allocator: Allocator, exec: *QueryExecutor, table: Table, table_tree: *BPlusTree, idx_tree: *BPlusTree, encoded_values: []const []const u8, current_tx: u64) anyerror!*IndexInScanIterator {
        std.debug.assert(encoded_values.len > 0);
        const self = try allocator.create(IndexInScanIterator);
        const seeks = try allocator.alloc([]const u8, encoded_values.len);
        const matches = try allocator.alloc([]const u8, encoded_values.len);
        for (encoded_values, 0..) |v, i| {
            seeks[i] = try allocator.dupe(u8, v);
            matches[i] = try std.fmt.allocPrint(allocator, "{s}:", .{v});
        }
        self.* = .{
            .allocator = allocator,
            .exec = exec,
            .table = table,
            .table_tree = table_tree,
            .idx_tree = idx_tree,
            .seeks = seeks,
            .matches = matches,
            .cur = 0,
            .idx_iter = try idx_tree.iteratorAfter(seeks[0]),
            .done = false,
            .current_tx = current_tx,
            .tables_slice = .{table.name},
        };
        return self;
    }

    pub fn iterator(self: *IndexInScanIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *IndexInScanIterator = @alignCast(@ptrCast(ctx));
                    try s.exec.checkDeadline();
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                        s.current_row_json = null;
                    }
                    if (s.done) return null;
                    while (true) {
                        while (try s.idx_iter.next()) |idx_cell| {
                            if (!std.mem.startsWith(u8, idx_cell.key, s.matches[s.cur])) break;
                            const pk_val = idx_cell.key[s.matches[s.cur].len..];
                            if (try s.exec.fetchVisibleFilteredJson(s.table, s.table_tree, pk_val, s.current_tx, s.residual, s.residual_cols)) |visible_row| {
                                s.current_row_json = visible_row;
                                s.data_slice[0] = visible_row;
                                return Row{ .tables = &s.tables_slice, .data = &s.data_slice };
                            }
                        }
                        // Current value exhausted; move to the next.
                        s.idx_iter.deinit();
                        s.cur += 1;
                        if (s.cur >= s.seeks.len) {
                            s.done = true;
                            return null;
                        }
                        s.idx_iter = try s.idx_tree.iteratorAfter(s.seeks[s.cur]);
                    }
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *IndexInScanIterator = @alignCast(@ptrCast(ctx));
                    if (s.current_row_json) |row_json| s.exec.freeTableRow(row_json);
                    if (!s.done) s.idx_iter.deinit();
                    s.idx_tree.deinit();
                    s.table_tree.deinit();
                    for (s.seeks) |k| s.allocator.free(k);
                    for (s.matches) |k| s.allocator.free(k);
                    s.allocator.free(s.seeks);
                    s.allocator.free(s.matches);
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};


/// `WHERE`-clause filter operator: passes through only child rows for which the
/// predicate is definitely true.
///
/// A pure streaming operator with no buffering. It pulls from `child` and returns
/// the first row that satisfies `where_expr` (evaluated by [`evalExpr`], so an
/// `unknown`/NULL result is rejected, matching SQL). It does not own the child's
/// row storage, so the returned [`Row`] carries the child's borrow lifetime
/// unchanged. Build with [`FilterIterator.init`].
pub const FilterIterator = struct {
    /// Allocator owning this operator struct.
    allocator: Allocator,
    /// Upstream operator supplying candidate rows.
    child: RowIterator,
    /// The predicate every emitted row must satisfy (see [`evalExpr`]).
    where_expr: *const ast.Expr,

    /// Allocates a filter wrapping `child` with predicate `where_expr`.
    ///
    /// Takes ownership of `child` (its `deinit` is called when this operator is
    /// torn down). Returns a heap-allocated operator owned by `allocator`.
    pub fn init(allocator: Allocator, child: RowIterator, where_expr: *const ast.Expr) anyerror!*FilterIterator {
        const self = try allocator.create(FilterIterator);
        self.* = .{
            .allocator = allocator,
            .child = child,
            .where_expr = where_expr,
        };
        return self;
    }

    /// Returns the type-erased [`RowIterator`] view of this filter.
    ///
    /// Publishes the child's `tables` layout unchanged (a filter never alters row
    /// shape). The `next` closure loops pulling child rows until one satisfies the
    /// predicate; `deinit` forwards to the child and destroys the operator.
    pub fn iterator(self: *FilterIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = self.child.tables,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *FilterIterator = @alignCast(@ptrCast(ctx));
                    while (try s.child.next()) |row| {
                        if (evalExpr(s.where_expr, row)) {
                            return row;
                        }
                    }
                    return null;
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *FilterIterator = @alignCast(@ptrCast(ctx));
                    s.child.deinit();
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};


/// Nested-loop join operator supporting INNER/LEFT/RIGHT/FULL joins on an
/// arbitrary `ON` predicate.
///
/// The right side is drained and deep-cloned once at construction into
/// `right_rows` (child rows are transient, so they must be owned); the left side
/// is then streamed, and for each left row every right row is tested with
/// [`evalExpr`] on `on_expr`. This is the general O(left * right) join used when
/// the predicate is not a simple equi-join (which the [`HashJoinIterator`]
/// handles).
///
/// Outer semantics use a two-phase state machine. In `left_scan` it emits every
/// match and, when a left row finishes with no match under LEFT/FULL, one row
/// padded with a NULL right side. For RIGHT/FULL it records matched right indices
/// in `matched_right_set`, then in `right_unmatched_scan` emits each never-matched
/// right row padded with NULL left columns ([`NestedLoopJoinIterator.nextUnmatchedRightRow`]).
/// Each emitted combined row's `tables`/`data` arrays are freshly allocated and
/// tracked in `current_tables`/`current_data`, freed on the next pull by
/// [`NestedLoopJoinIterator.clearCurrentCombined`]. Note the combined `data`
/// slices point at the (owned) right rows and the (borrowed) left row without
/// re-cloning, so a combined row is valid only until the next pull.
pub const NestedLoopJoinIterator = struct {
    /// Allocator owning this operator, the buffered right rows, and per-row
    /// combined arrays.
    allocator: Allocator,
    /// Left (streamed) input.
    left_iter: RowIterator,
    /// All right rows, deep-cloned and owned (the right child is drained at init).
    right_rows: []const TableRow,
    /// Name to label the right table's column group with in produced rows.
    right_table_name: []const u8,
    /// The join predicate applied to each left*right combination.
    on_expr: *const ast.Expr,
    /// INNER/LEFT/RIGHT/FULL selector governing the outer-padding behaviour.
    join_type: ast.JoinType,
    /// The combined table-name layout (left tables plus the right table).
    tables_slice: []const []const u8,

    /// The left row currently being matched, or `null` between left rows.
    left_row: ?Row = null,
    /// Cursor into `right_rows` for the current left row's inner loop.
    right_idx: usize = 0,
    /// Whether the current left row matched at least one right row (drives
    /// LEFT/FULL padding).
    left_matched: bool = false,
    /// Set of right-row indices that matched some left row, for RIGHT/FULL
    /// unmatched emission.
    matched_right_set: std.AutoHashMap(usize, bool),
    /// Which phase of the state machine is active: matching left rows, or
    /// emitting never-matched right rows.
    phase: enum { left_scan, right_unmatched_scan } = .left_scan,
    /// Cursor into `right_rows` during `right_unmatched_scan`.
    unmatched_right_idx: usize = 0,

    /// The most recently emitted row's owned `tables` array, freed on next pull.
    current_tables: ?[]const []const u8 = null,
    /// The most recently emitted row's owned `data` array, freed on next pull.
    current_data: ?[]TableRow = null,

    /// Allocates the join, draining and cloning the entire right input up front.
    ///
    /// Builds `tables_slice` as left tables + `right_table_name`, deep-clones each
    /// right row with [`cloneJson`] into owned `right_rows` (with `errdefer`
    /// cleanup), then deinit-s the right child. Takes ownership of `left_iter`.
    /// Returns a heap-allocated operator owned by `allocator`.
    pub fn init(allocator: Allocator, left_iter: RowIterator, right_iter: RowIterator, right_table_name: []const u8, on_expr: *const ast.Expr, join_type: ast.JoinType) anyerror!*NestedLoopJoinIterator {
        var tables = std.ArrayList([]const u8).empty;
        defer tables.deinit(allocator);
        try tables.appendSlice(allocator, left_iter.tables);
        try tables.append(allocator, right_table_name);

        var right_rows = std.ArrayList(TableRow).empty;
        errdefer {
            for (right_rows.items) |r| freeTableRow(allocator, r);
            right_rows.deinit(allocator);
        }

        while (try right_iter.next()) |r_row| {
            const cloned = try cloneTableRow(allocator, r_row.data[0]);
            try right_rows.append(allocator, cloned);
        }
        right_iter.deinit();

        const self = try allocator.create(NestedLoopJoinIterator);
        self.* = .{
            .allocator = allocator,
            .left_iter = left_iter,
            .right_rows = try right_rows.toOwnedSlice(allocator),
            .right_table_name = right_table_name,
            .on_expr = on_expr,
            .join_type = join_type,
            .tables_slice = try tables.toOwnedSlice(allocator),
            .matched_right_set = std.AutoHashMap(usize, bool).init(allocator),
        };
        return self;
    }

    /// Frees the previously emitted combined row's `tables`/`data` arrays.
    ///
    /// Only the arrays are freed, not their elements: the JSON values they point
    /// at are either owned `right_rows` (freed at `deinit`) or the borrowed left
    /// row, so freeing them here would double-free. Called at the top of each new
    /// emission and at teardown.
    fn clearCurrentCombined(self: *NestedLoopJoinIterator) void {
        if (self.current_tables) |t| self.allocator.free(t);
        if (self.current_data) |d| self.allocator.free(d);
        self.current_tables = null;
        self.current_data = null;
    }

    /// Returns the type-erased [`RowIterator`] view of this nested-loop join.
    ///
    /// The `next` closure runs the two-phase state machine described on
    /// [`NestedLoopJoinIterator`]: fetch a left row (advancing to the unmatched-
    /// right phase for RIGHT/FULL when the left side ends), inner-loop the buffered
    /// right rows building and testing a combined row (freeing non-matching
    /// combined arrays immediately), then emit LEFT/FULL padding for an unmatched
    /// left row. `deinit` releases the left child, all cloned right rows, the
    /// tables layout, the matched-set, and the operator.
    pub fn iterator(self: *NestedLoopJoinIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *NestedLoopJoinIterator = @alignCast(@ptrCast(ctx));
                    const allocator = s.allocator;

                    if (s.phase == .left_scan) {
                        while (true) {
                            if (s.left_row == null) {
                                s.left_row = try s.left_iter.next();
                                if (s.left_row == null) {
                                    if (s.join_type == .right or s.join_type == .full) {
                                        s.phase = .right_unmatched_scan;
                                        s.unmatched_right_idx = 0;
                                        return try s.nextUnmatchedRightRow();
                                    }
                                    return null;
                                }
                                s.right_idx = 0;
                                s.left_matched = false;
                            }

                            while (s.right_idx < s.right_rows.len) {
                                const r_row_json = s.right_rows[s.right_idx];
                                s.right_idx += 1;

                                var tables = try allocator.alloc([]const u8, s.left_row.?.tables.len + 1);
                                @memcpy(tables[0..s.left_row.?.tables.len], s.left_row.?.tables);
                                tables[s.left_row.?.tables.len] = s.right_table_name;

                                var data = try allocator.alloc(TableRow, s.left_row.?.data.len + 1);
                                @memcpy(data[0..s.left_row.?.data.len], s.left_row.?.data);
                                data[s.left_row.?.data.len] = r_row_json;

                                const combined_row = Row{ .tables = tables, .data = data };

                                if (evalExpr(s.on_expr, combined_row)) {
                                    s.left_matched = true;
                                    if (s.join_type == .right or s.join_type == .full) {
                                        try s.matched_right_set.put(s.right_idx - 1, true);
                                    }
                                    s.clearCurrentCombined();
                                    s.current_tables = tables;
                                    s.current_data = data;
                                    return combined_row;
                                } else {
                                    allocator.free(tables);
                                    allocator.free(data);
                                }
                            }

                            const finished_left = s.left_row.?;
                            s.left_row = null;

                            if ((s.join_type == .left or s.join_type == .full) and !s.left_matched) {
                                var tables = try allocator.alloc([]const u8, finished_left.tables.len + 1);
                                @memcpy(tables[0..finished_left.tables.len], finished_left.tables);
                                tables[finished_left.tables.len] = s.right_table_name;

                                var data = try allocator.alloc(TableRow, finished_left.data.len + 1);
                                @memcpy(data[0..finished_left.data.len], finished_left.data);
                                data[finished_left.data.len] = TableRow.null_row;

                                const combined_row = Row{ .tables = tables, .data = data };
                                s.clearCurrentCombined();
                                s.current_tables = tables;
                                s.current_data = data;
                                return combined_row;
                            }
                        }
                    } else {
                        return try s.nextUnmatchedRightRow();
                    }
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *NestedLoopJoinIterator = @alignCast(@ptrCast(ctx));
                    s.left_iter.deinit();
                    for (s.right_rows) |r| freeTableRow(s.allocator, r);
                    s.allocator.free(s.right_rows);
                    s.allocator.free(s.tables_slice);
                    s.clearCurrentCombined();
                    s.matched_right_set.deinit();
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }

    /// Emits the next right row that never matched any left row, padded with NULL
    /// left columns (RIGHT/FULL join phase), or `null` when all are emitted.
    ///
    /// Advances `unmatched_right_idx` past indices present in `matched_right_set`.
    /// The left side of the produced row is `left_iter.tables.len` NULL slots (the
    /// left stream is exhausted by now), and the last slot is the right row. The
    /// combined arrays are tracked in `current_*` and freed on the next pull.
    fn nextUnmatchedRightRow(self: *NestedLoopJoinIterator) !?Row {
        const allocator = self.allocator;
        while (self.unmatched_right_idx < self.right_rows.len) {
            const idx = self.unmatched_right_idx;
            self.unmatched_right_idx += 1;

            if (!self.matched_right_set.contains(idx)) {
                var tables = try allocator.alloc([]const u8, self.left_iter.tables.len + 1);
                @memcpy(tables[0..self.left_iter.tables.len], self.left_iter.tables);
                tables[self.left_iter.tables.len] = self.right_table_name;

                var data = try allocator.alloc(TableRow, self.left_iter.tables.len + 1);
                for (0..self.left_iter.tables.len) |i| {
                    data[i] = TableRow.null_row;
                }
                data[self.left_iter.tables.len] = self.right_rows[idx];

                const combined_row = Row{ .tables = tables, .data = data };
                self.clearCurrentCombined();
                self.current_tables = tables;
                self.current_data = data;
                return combined_row;
            }
        }
        return null;
    }
};


/// Hash join operator for equi-joins (`left.col = right.col`), supporting
/// INNER/LEFT/RIGHT/FULL.
///
/// At construction the right side is drained, deep-cloned into owned `right_rows`,
/// and a `StringHashMap` from the right key column's string value to the list of
/// matching right-row indices is built (`hash_map`). The left side is then
/// streamed and each left key is probed in O(1), replacing the nested loop's
/// inner scan. Only string-valued join keys are indexed/probed (non-string keys
/// never match).
///
/// Memory differs from the nested-loop join: this operator deep-clones the LEFT
/// row too and clones each value into its combined `data`, so combined rows are
/// fully owned and are released (values included) by
/// [`HashJoinIterator.clearCurrentCombined`] on the next pull. Outer semantics use
/// the same two-phase machine (`left_scan` then `right_unmatched_scan` via
/// [`HashJoinIterator.nextUnmatchedRightRow`]) and `matched_right_set`. The key
/// column names are stored `.dupe`d so they outlive the caller's AST. Build with
/// [`HashJoinIterator.init`].
pub const HashJoinIterator = struct {
    /// Allocator owning this operator and everything it clones.
    allocator: Allocator,
    /// Left (streamed, probing) input.
    left_iter: RowIterator,
    /// All right rows, deep-cloned and owned; indexed by `hash_map`.
    right_rows: []const TableRow,
    /// Owned copy of the right table's label for produced rows.
    right_table_name: []const u8,
    /// Owned name of the left join-key column (may be `table.col` qualified).
    left_key_col: []const u8,
    /// Owned name of the right join-key column, reduced to its base name when
    /// building the hash index (see [`HashJoinIterator.getBaseColName`]).
    right_key_col: []const u8,
    /// INNER/LEFT/RIGHT/FULL selector governing outer padding.
    join_type: ast.JoinType,
    /// The combined table-name layout (left tables plus the right table).
    tables_slice: []const []const u8,

    /// Build-side index: right key string -> list of right-row indices with that
    /// key (a list, since keys need not be unique).
    hash_map: std.StringHashMap(std.ArrayList(usize)),

    /// The current (owned) left row being probed, or `null` between left rows.
    left_row: ?Row = null,
    /// The list of right-row indices matching the current left key, being emitted.
    matching_right_indices: ?[]const usize = null,
    /// Cursor into `matching_right_indices`.
    current_matching_idx: usize = 0,
    /// Whether the current left row matched (drives LEFT/FULL padding).
    left_matched: bool = false,
    /// Right indices that matched some left row, for RIGHT/FULL unmatched emission.
    matched_right_set: std.AutoHashMap(usize, bool),
    /// Active phase: probing left rows, or emitting never-matched right rows.
    phase: enum { left_scan, right_unmatched_scan } = .left_scan,
    /// Cursor into `right_rows` during `right_unmatched_scan`.
    unmatched_right_idx: usize = 0,

    /// The most recently emitted row's owned `tables` array, freed on next pull.
    current_tables: ?[]const []const u8 = null,
    /// The most recently emitted row's owned `data` array (values owned too),
    /// freed on next pull.
    current_data: ?[]TableRow = null,

    /// Allocates the hash join, draining/cloning the right input and building the
    /// probe index.
    ///
    /// Deep-clones every right row, `.dupe`s the table/key-column names, then
    /// populates `hash_map` keyed by the right rows' base key column (only string
    /// keys are indexed). Takes ownership of `left_iter`; deinit-s the right child.
    /// Returns a heap-allocated operator owned by `allocator`.
    pub fn init(
        allocator: Allocator,
        left_iter: RowIterator,
        right_iter: RowIterator,
        right_table_name: []const u8,
        left_key_col: []const u8,
        right_key_col: []const u8,
        join_type: ast.JoinType,
    ) anyerror!*HashJoinIterator {
        var tables = std.ArrayList([]const u8).empty;
        defer tables.deinit(allocator);
        try tables.appendSlice(allocator, left_iter.tables);
        try tables.append(allocator, right_table_name);

        var right_rows = std.ArrayList(TableRow).empty;
        errdefer {
            for (right_rows.items) |r| freeTableRow(allocator, r);
            right_rows.deinit(allocator);
        }

        while (try right_iter.next()) |r_row| {
            const cloned = try cloneTableRow(allocator, r_row.data[0]);
            try right_rows.append(allocator, cloned);
        }
        right_iter.deinit();

        const self = try allocator.create(HashJoinIterator);
        self.* = .{
            .allocator = allocator,
            .left_iter = left_iter,
            .right_rows = try right_rows.toOwnedSlice(allocator),
            .right_table_name = try allocator.dupe(u8, right_table_name),
            .left_key_col = try allocator.dupe(u8, left_key_col),
            .right_key_col = try allocator.dupe(u8, right_key_col),
            .join_type = join_type,
            .tables_slice = try tables.toOwnedSlice(allocator),
            .hash_map = std.StringHashMap(std.ArrayList(usize)).init(allocator),
            .matched_right_set = std.AutoHashMap(usize, bool).init(allocator),
        };

        const right_col_base = getBaseColName(self.right_key_col);
        for (self.right_rows, 0..) |r_val, idx| {
            if (try r_val.getTextAlloc(allocator, right_col_base)) |key_str| {
                var res = try self.hash_map.getOrPut(key_str);
                if (res.found_existing) {
                    allocator.free(key_str); // map already owns an equal key
                } else {
                    res.value_ptr.* = std.ArrayList(usize).empty;
                }
                try res.value_ptr.append(allocator, idx);
            }
        }

        return self;
    }

    /// Strips a `table.` qualifier from a column name, returning the bare column.
    ///
    /// The build side indexes right rows by their unqualified field name (JSON
    /// objects store bare column keys), so a qualified `right_key_col` must be
    /// reduced before lookup. Returns the input unchanged if there is no `.`.
    fn getBaseColName(col_name: []const u8) []const u8 {
        if (std.mem.indexOfScalar(u8, col_name, '.')) |dot_idx| {
            return col_name[dot_idx + 1 ..];
        }
        return col_name;
    }

    /// Frees the previously emitted combined row's `tables` and `data`, INCLUDING
    /// each data value.
    ///
    /// Unlike the nested-loop join, the hash join clones every value into its
    /// combined `data`, so it must free those clones with [`freeClonedJson`] as
    /// well as the arrays. Called before each new emission and at teardown.
    fn clearCurrentCombined(self: *HashJoinIterator) void {
        const allocator = self.allocator;
        if (self.current_tables) |t| {
            allocator.free(t);
            self.current_tables = null;
        }
        if (self.current_data) |d| {
            for (d) |v| freeTableRow(allocator, v);
            allocator.free(d);
            self.current_data = null;
        }
    }

    /// Returns the type-erased [`RowIterator`] view of this hash join.
    ///
    /// The `next` closure: while emitting a left row's matched right indices,
    /// records each in `matched_right_set` and yields a fully cloned combined row;
    /// when a match list is exhausted it frees the owned left row and pulls the
    /// next, cloning it, probing `hash_map` by its string key, and either queueing
    /// the matches or (for LEFT/FULL, no match) emitting a NULL-padded row; when
    /// the left stream ends it switches to `right_unmatched_scan` for RIGHT/FULL.
    /// `deinit` releases the left child, all right rows, the duped name strings,
    /// the tables layout, any in-flight left row, every hash-map bucket list, the
    /// current combined row, and the operator.
    pub fn iterator(self: *HashJoinIterator) RowIterator {
        return RowIterator{
            .ptr = self,
            .tables = self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *HashJoinIterator = @alignCast(@ptrCast(ctx));
                    const allocator = s.allocator;

                    while (true) {
                        if (s.phase == .right_unmatched_scan) {
                            return try s.nextUnmatchedRightRow();
                        }

                        if (s.matching_right_indices) |indices| {
                            if (s.current_matching_idx < indices.len) {
                                const idx = indices[s.current_matching_idx];
                                s.current_matching_idx += 1;

                                try s.matched_right_set.put(idx, true);

                                var tables = try allocator.alloc([]const u8, s.left_row.?.tables.len + 1);
                                @memcpy(tables[0..s.left_row.?.tables.len], s.left_row.?.tables);
                                tables[s.left_row.?.tables.len] = s.right_table_name;

                                var data = try allocator.alloc(TableRow, s.left_row.?.data.len + 1);
                                for (s.left_row.?.data, 0..) |val, i| {
                                    data[i] = try cloneTableRow(allocator, val);
                                }
                                data[s.left_row.?.data.len] = try cloneTableRow(allocator, s.right_rows[idx]);

                                const combined_row = Row{ .tables = tables, .data = data };
                                s.clearCurrentCombined();
                                s.current_tables = tables;
                                s.current_data = data;
                                return combined_row;
                            } else {
                                if (s.left_row) |*lr| {
                                    for (lr.data) |v| freeTableRow(allocator, v);
                                    allocator.free(lr.tables);
                                    allocator.free(lr.data);
                                }
                                s.left_row = null;
                                s.matching_right_indices = null;
                            }
                        }

                        const next_left = try s.left_iter.next() orelse {
                            if (s.join_type == .right or s.join_type == .full) {
                                s.phase = .right_unmatched_scan;
                                continue;
                            }
                            return null;
                        };

                        const cloned_tables = try allocator.alloc([]const u8, next_left.tables.len);
                        @memcpy(cloned_tables, next_left.tables);
                        var cloned_data = try allocator.alloc(TableRow, next_left.data.len);
                        for (next_left.data, 0..) |v, i| {
                            cloned_data[i] = try cloneTableRow(allocator, v);
                        }
                        s.left_row = Row{ .tables = cloned_tables, .data = cloned_data };
                        s.left_matched = false;

                        const expr = ast.Expr{ .column_ref = s.left_key_col };
                        if (getVal(&expr, s.left_row.?)) |left_val| {
                            // Key by the text form so a numeric join column matches
                            // the text keys built from the right side.
                            const key_txt: ?[]const u8 = switch (left_val) {
                                .null => null,
                                .string => |str| try allocator.dupe(u8, str),
                                .integer => |n| try std.fmt.allocPrint(allocator, "{d}", .{n}),
                                .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
                                .bool => |b| try allocator.dupe(u8, if (b) "true" else "false"),
                            };
                            if (key_txt) |kt| {
                                defer allocator.free(kt);
                                if (s.hash_map.get(kt)) |list| {
                                    s.matching_right_indices = list.items;
                                    s.current_matching_idx = 0;
                                    s.left_matched = true;
                                    continue;
                                }
                            }
                        }

                        if (s.join_type == .left or s.join_type == .full) {
                            var tables = try allocator.alloc([]const u8, s.left_row.?.tables.len + 1);
                            @memcpy(tables[0..s.left_row.?.tables.len], s.left_row.?.tables);
                            tables[s.left_row.?.tables.len] = s.right_table_name;

                            var data = try allocator.alloc(TableRow, s.left_row.?.data.len + 1);
                            for (s.left_row.?.data, 0..) |val, i| {
                                data[i] = try cloneTableRow(allocator, val);
                            }
                            data[s.left_row.?.data.len] = TableRow.null_row;

                            const combined_row = Row{ .tables = tables, .data = data };
                            s.clearCurrentCombined();
                            s.current_tables = tables;
                            s.current_data = data;

                            for (s.left_row.?.data) |v| freeTableRow(allocator, v);
                            allocator.free(s.left_row.?.tables);
                            allocator.free(s.left_row.?.data);
                            s.left_row = null;

                            return combined_row;
                        } else {
                            for (s.left_row.?.data) |v| freeTableRow(allocator, v);
                            allocator.free(s.left_row.?.tables);
                            allocator.free(s.left_row.?.data);
                            s.left_row = null;
                        }
                    }
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *HashJoinIterator = @alignCast(@ptrCast(ctx));
                    const allocator = s.allocator;

                    s.left_iter.deinit();
                    for (s.right_rows) |r| freeTableRow(allocator, r);
                    allocator.free(s.right_rows);

                    allocator.free(s.right_table_name);
                    allocator.free(s.left_key_col);
                    allocator.free(s.right_key_col);
                    allocator.free(s.tables_slice);

                    if (s.left_row) |*lr| {
                        for (lr.data) |v| freeTableRow(allocator, v);
                        allocator.free(lr.tables);
                        allocator.free(lr.data);
                    }

                    var it = s.hash_map.iterator();
                    while (it.next()) |entry| {
                        allocator.free(entry.key_ptr.*); // owned text key
                        entry.value_ptr.deinit(allocator);
                    }
                    s.hash_map.deinit();

                    s.clearCurrentCombined();
                    s.matched_right_set.deinit();
                    allocator.destroy(s);
                }
            }.deinit,
        };
    }

    /// Emits the next never-matched right row padded with NULL left columns
    /// (RIGHT/FULL phase), or `null` when done.
    ///
    /// Skips indices in `matched_right_set`. The left columns are filled with
    /// `left_iter.tables.len` NULL slots and the last slot is a fresh
    /// [`cloneJson`] of the right row (so the combined `data` is fully owned, as
    /// this operator's `clearCurrentCombined` expects). Tracked in `current_*` and
    /// freed on the next pull.
    fn nextUnmatchedRightRow(self: *HashJoinIterator) !?Row {
        const allocator = self.allocator;
        while (self.unmatched_right_idx < self.right_rows.len) {
            const idx = self.unmatched_right_idx;
            self.unmatched_right_idx += 1;

            if (!self.matched_right_set.contains(idx)) {
                var tables = try allocator.alloc([]const u8, self.left_iter.tables.len + 1);
                @memcpy(tables[0..self.left_iter.tables.len], self.left_iter.tables);
                tables[self.left_iter.tables.len] = self.right_table_name;

                var data = try allocator.alloc(TableRow, self.left_iter.tables.len + 1);
                for (0..self.left_iter.tables.len) |i| {
                    data[i] = TableRow.null_row;
                }
                data[self.left_iter.tables.len] = try cloneTableRow(allocator, self.right_rows[idx]);

                const combined_row = Row{ .tables = tables, .data = data };
                self.clearCurrentCombined();
                self.current_tables = tables;
                self.current_data = data;
                return combined_row;
            }
        }
        return null;
    }
};


/// Point-lookup leaf operator: yields at most the single row matching a given
/// primary key.
///
/// The most selective access path, chosen when a query constrains the primary
/// key to one value. It performs one B+Tree `search` for `pk_val`, checks MVCC
/// visibility for `current_tx`, and emits that row (if visible), then is
/// exhausted. `done` makes the second `next()` return `null` without a second
/// lookup. `pk_val` is `.dupe`d at init so it outlives the caller. Build with
/// [`PrimaryKeyScanIterator.init`].
pub const PrimaryKeyScanIterator = struct {
    /// Allocator owning this operator, the decoded row, and the duped key.
    allocator: Allocator,
    /// Executor supplying MVCC visibility, deadline checks, and JSON freeing.
    exec: *QueryExecutor,
    /// The table being point-queried.
    table: Table,
    /// The table's data B+Tree.
    table_tree: *BPlusTree,
    /// Owned copy of the primary-key value to look up.
    pk_val: []const u8,
    /// The transaction id whose snapshot decides row visibility.
    current_tx: u64,
    /// The one decoded row currently borrowed by the last-returned [`Row`].
    current_row_json: ?TableRow = null,
    /// Set after the single lookup so subsequent pulls short-circuit to `null`.
    done: bool = false,
    /// Reusable one-element table-name array.
    tables_slice: [1][]const u8,
    /// Reusable one-element data array holding the found row's object.
    data_slice: [1]TableRow = undefined,

    /// Allocates a primary-key point scan for `pk_val` (which is duplicated).
    ///
    /// Returns a heap-allocated operator owned by `allocator`; drive it via
    /// [`PrimaryKeyScanIterator.iterator`] and `deinit` the resulting
    /// [`RowIterator`].
    pub fn init(allocator: Allocator, exec: *QueryExecutor, table: Table, table_tree: *BPlusTree, pk_val: []const u8, current_tx: u64) !*PrimaryKeyScanIterator {
        const self = try allocator.create(PrimaryKeyScanIterator);
        self.* = .{
            .allocator = allocator,
            .exec = exec,
            .table = table,
            .table_tree = table_tree,
            .pk_val = try allocator.dupe(u8, pk_val),
            .current_tx = current_tx,
            .tables_slice = .{table.name},
        };
        return self;
    }

    /// Returns the type-erased [`RowIterator`] view of this point lookup.
    ///
    /// The `next` closure checks the deadline, frees any previous row, returns
    /// `null` if already `done`, otherwise marks `done`, `search`es the tree once
    /// (freeing that buffer via `defer`), and emits the row if a version is
    /// visible to `current_tx`. `deinit` frees any held row, the tree reference,
    /// the duped key, and the operator.
    pub fn iterator(self: *PrimaryKeyScanIterator) RowIterator {
        return .{
            .ptr = self,
            .tables = &self.tables_slice,
            .nextFn = struct {
                fn next(ctx: *anyopaque) anyerror!?Row {
                    const s: *PrimaryKeyScanIterator = @alignCast(@ptrCast(ctx));
                    try s.exec.checkDeadline();
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                        s.current_row_json = null;
                    }
                    if (s.done) return null;
                    s.done = true;

                    const opt_val = try s.table_tree.search(s.pk_val, s.allocator);
                    if (opt_val) |val| {
                        defer s.allocator.free(val);
                        if (try s.exec.getVisibleVersion(s.table, val, false, s.current_tx)) |visible_row| {
                            s.current_row_json = visible_row;
                            s.data_slice[0] = visible_row;
                            return Row{
                                .tables = &s.tables_slice,
                                .data = &s.data_slice,
                            };
                        }
                    }
                    return null;
                }
            }.next,
            .deinitFn = struct {
                fn deinit(ctx: *anyopaque) void {
                    const s: *PrimaryKeyScanIterator = @alignCast(@ptrCast(ctx));
                    if (s.current_row_json) |row_json| {
                        s.exec.freeTableRow(row_json);
                    }
                    s.table_tree.deinit();
                    s.allocator.free(s.pk_val);
                    s.allocator.destroy(s);
                }
            }.deinit,
        };
    }
};
