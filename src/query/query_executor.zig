//! SQL query executor: the layer that turns a parsed SQL statement into
//! reads and writes against the B+Tree storage engine, under MVCC and the
//! session's isolation level.
//!
//! This file is the "brain" that sits between the SQL frontend (lexer,
//! parser, [`ast`]) and the storage engine ([`BPlusTree`], the pager, the
//! WAL). A client sends a [`QueryRequest`] (raw SQL plus optional bound
//! parameters, a session token, a timeout and a memory cap); [`QueryExecutor`]
//! parses it (with a shared statement cache), authenticates and authorises it,
//! wraps it in a transaction if the client did not open one explicitly, runs
//! it, and returns a [`QueryResponse`] (columns, rows and an error string).
//!
//! Key design points and invariants a reader should keep in mind:
//!
//! - **MVCC visibility.** Every physical row is a chain of versions, each
//!   tagged with `xmin` (the transaction that created it) and `xmax` (the
//!   transaction that deleted/superseded it, or 0 if still live).
//!   [`QueryExecutor.rowVisible`] is the single arbiter of which version a
//!   given transaction may see, and [`QueryExecutor.getVisibleVersion`]
//!   decodes a stored cell into the one visible row. Writes never mutate a row
//!   in place; they append a new version and set the old version's `xmax`
//!   (see [`QueryExecutor.writeNewVersion`] / [`QueryExecutor.deleteRowVersion`]).
//!
//! - **Isolation levels.** READ COMMITTED (the default) re-checks commit
//!   status per statement; REPEATABLE READ / SNAPSHOT capture a
//!   [`txn_mod.Snapshot`] at BEGIN so all statements see one frozen commit
//!   set; SERIALIZABLE additionally tracks per-table SIREAD/write footprints
//!   (SSI, Cahill) and aborts a pivot transaction at COMMIT to prevent write
//!   skew. The tracking hooks are [`QueryExecutor.ssiTrackRead`] /
//!   [`QueryExecutor.ssiTrackWrite`].
//!
//! - **Locking protocol.** [`QueryExecutor.executeStatement`] takes the
//!   db-wide `rw_lock` shared for reads and exclusive for writes, but when a
//!   statement touches exactly one user table and there are no foreign keys in
//!   play it downgrades to the per-table `GroupLock` (read for SELECT, write
//!   for INSERT so concurrent inserters proceed, exclusive for UPDATE/DELETE
//!   which scan). DDL and any FK/join statement stay on the coarse lock.
//!
//! - **WAL ordering.** Autocommit statements emit `begin`, then the mutation
//!   records, then `commit` (or `rollback` on failure) through
//!   [`QueryExecutor.logWalRecord`]. The commit record is what makes a
//!   transaction durable: if writing it fails the transaction is aborted, and
//!   for `synchronous_commit` the commit record is fsynced before returning.
//!   Durable replication (`durable_repl`) ships the pending records only after
//!   the local commit succeeds (see [`QueryExecutor.durableFinish`]).
//!
//! - **Subtransactions.** SAVEPOINT is implemented by beginning a fresh child
//!   transaction id and remembering it in [`QueryExecutor.savepoints`] plus the
//!   `live_sub` set; ROLLBACK TO aborts the child ids above the savepoint and
//!   starts a new one, RELEASE just forgets them. Rows written under a live
//!   subtransaction are visible to the parent via
//!   [`QueryExecutor.isOwnLiveWrite`], and [`QueryExecutor.currentWriteXid`]
//!   stamps new versions with the innermost live id.
//!
//! - **Resource limits.** A per-query [`MemoryLimitAllocator`] enforces
//!   `memory_limit_bytes` (returning `error.OutOfMemory`, surfaced as "Query
//!   Memory Limit Exceeded"), and [`QueryExecutor.checkDeadline`] enforces
//!   `timeout_ms` at every row of a scan.
//!
//! The executor is deliberately string-oriented at its edges: values move
//! through it as JSON objects / text cells rather than typed columns, which
//! keeps the SQL surface flexible at the cost of per-row allocation. Ownership
//! is manual throughout, so most functions here are dense with `defer`/
//! `errdefer` cleanup; the recurring pattern is "dupe into an owned buffer,
//! free on the way out, and hand ownership to the [`QueryResponse`] slices at
//! the very end via `toOwnedSlice`".

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const yaml_pkg = @import("yaml");
const Yaml = yaml_pkg.Yaml;

const schema = @import("../schema.zig");
const Database = schema.Database;
const Column = schema.Column;
const ColumnType = schema.ColumnType;
const Table = schema.Table;
const Index = schema.Index;
const RowBuilder = schema.RowBuilder;
const RowReader = schema.RowReader;
const BPlusTree = @import("../storage/btree.zig").BPlusTree;
const LogRecord = @import("../common/common.zig").LogRecord;
const ast = @import("../sql/ast.zig");
const Parser = @import("../sql/parser.zig").Parser;
const Lexer = @import("../sql/lexer.zig").Lexer;
const OpKind = @import("../common/common.zig").OpKind;
const hexEncode = @import("../concurrency/security.zig").hexEncode;
const query_iter = @import("iterator.zig");
const oidmap = @import("../proto/oidmap.zig");

/// Owned text render of an evaluator scalar, matching the former all-text row
/// representation (`{d}` numerics, `true`/`false`, raw string, `NULL`). Used by
/// the projection / GROUP BY / aggregate paths that consumed row cells as text
/// before result cells became typed.
fn scalarTextInto(a: std.mem.Allocator, v: query_iter.Scalar) ![]const u8 {
    return switch (v) {
        .null => try a.dupe(u8, "NULL"),
        .string => |s| try a.dupe(u8, s),
        .integer => |n| try std.fmt.allocPrint(a, "{d}", .{n}),
        .float => |f| try std.fmt.allocPrint(a, "{d}", .{f}),
        .bool => |b| try a.dupe(u8, if (b) "true" else "false"),
    };
}
const StopWatch = @import("utils").StopWatch;
const txn_mod = @import("../concurrency/transaction.zig");
const command_mod = @import("../proto/command.zig");

/// Transaction isolation level for a session, selected by
/// `SET TRANSACTION ISOLATION LEVEL`.
///
/// - `read_committed`: the default; each statement re-checks commit status, so
///   a transaction sees other transactions' commits as they land. `READ
///   UNCOMMITTED` is also mapped here (kaidb does not expose dirty reads).
/// - `repeatable_read`: captures a [`txn_mod.Snapshot`] at BEGIN and reuses it
///   for every statement, so the visible commit set is frozen. `SNAPSHOT` is an
///   alias for this level.
/// - `serializable`: repeatable-read plus SSI (Cahill's serialisable snapshot
///   isolation) read/write tracking, which can abort a transaction at COMMIT to
///   prevent write skew.
pub const IsolationLevel = enum { read_committed, repeatable_read, serializable };

/// A named subtransaction created by SAVEPOINT.
///
/// Each savepoint owns a distinct MVCC transaction id (`xid`, obtained from a
/// fresh `txn_manager.begin`) so its writes can be rolled back independently by
/// aborting that id. `name` is heap-owned by the executor and freed when the
/// savepoint is released, rolled back over, or the outer transaction ends.
const Savepoint = struct {
    /// Client-supplied savepoint name, owned by the executor's allocator.
    name: []u8,
    /// The MVCC transaction id backing this subtransaction; writes made while
    /// it is the innermost savepoint are stamped with this id.
    xid: u64,
};

/// Cost estimates used to choose between a nested-loop and a hash join.
///
/// The numbers are deliberately crude relative costs (not calibrated times):
/// nested loop is quadratic in the two inputs, hash join is roughly linear
/// (build the right side, probe once per left row). [`buildIteratorTree`]
/// consults [`CostModel.preferHashJoin`] only for small right sides where the
/// hash table is cheap to hold in memory.
pub const CostModel = struct {
    /// Relative cost of a nested-loop join: every left row scans every right
    /// row, hence `left * right` (saturating so a huge product cannot wrap).
    pub fn nestedLoopCost(left_rows: u64, right_rows: u64) u64 {
        return left_rows *| right_rows;
    }
    /// Relative cost of a hash join: build the hash table from the right side
    /// (weighted x2 for hashing + storage) then probe once per left row, hence
    /// `left + 2*right` (saturating).
    pub fn hashJoinCost(left_rows: u64, right_rows: u64) u64 {
        return left_rows +| (right_rows *| 2);
    }
    /// Returns true when the hash join is estimated cheaper than the nested
    /// loop for the given cardinalities.
    pub fn preferHashJoin(left_rows: u64, right_rows: u64) bool {
        return hashJoinCost(left_rows, right_rows) < nestedLoopCost(left_rows, right_rows);
    }
};

/// Scoped logger for this subsystem (`query_api`).
const log = std.log.scoped(.query_api);

/// Alias for `std.mem.Alignment`, used by the [`MemoryLimitAllocator`] vtable.
const Alignment = std.mem.Alignment;

/// An allocator wrapper that caps total live bytes for a single query.
///
/// It forwards every request to `parent_allocator` but first checks that the
/// running total would stay within `limit_bytes`; an over-limit alloc/resize
/// returns null/false, which surfaces to the caller as `error.OutOfMemory` and
/// ultimately the "Query Memory Limit Exceeded" response. [`execute`] installs
/// one of these as `self.allocator` for the duration of a query when the
/// request or session sets a memory limit, then restores the real allocator.
///
/// `allocated_bytes` is a simple running sum (no per-allocation bookkeeping
/// beyond the length the allocator interface already passes back on free), so
/// it tracks payload bytes, not allocator overhead.
const MemoryLimitAllocator = struct {
    /// The real allocator that actually owns the memory.
    parent_allocator: Allocator,
    /// Running total of live bytes handed out through this wrapper.
    allocated_bytes: usize = 0,
    /// Hard cap: an allocation that would push `allocated_bytes` past this
    /// value is refused.
    limit_bytes: usize,

    /// Returns the `std.mem.Allocator` interface bound to this limiter.
    pub fn allocator(self: *MemoryLimitAllocator) Allocator {
        return .{
            .ptr = self,
            .vtable = &.{
                .alloc = alloc,
                .resize = resize,
                .remap = remap,
                .free = free,
            },
        };
    }

    /// Allocator vtable `alloc`: refuses (returns null) if `len` would exceed
    /// the cap, otherwise forwards to the parent and adds `len` to the running
    /// total.
    fn alloc(ctx: *anyopaque, len: usize, ptr_align: Alignment, ret_addr: usize) ?[*]u8 {
        const self: *MemoryLimitAllocator = @alignCast(@ptrCast(ctx));
        if (self.allocated_bytes + len > self.limit_bytes) {
            return null;
        }
        const result = self.parent_allocator.rawAlloc(len, ptr_align, ret_addr) orelse return null;
        self.allocated_bytes += len;
        return result;
    }

    /// Allocator vtable `resize`: adjusts the running total by the delta.
    ///
    /// Growth is refused if the extra bytes would exceed the cap; a shrink
    /// always succeeds if the parent accepts it and credits the freed bytes
    /// back. A same-size resize is a no-op that reports success.
    fn resize(ctx: *anyopaque, buf: []u8, buf_align: Alignment, new_len: usize, ret_addr: usize) bool {
        const self: *MemoryLimitAllocator = @alignCast(@ptrCast(ctx));
        if (new_len > buf.len) {
            const extra = new_len - buf.len;
            if (self.allocated_bytes + extra > self.limit_bytes) {
                return false;
            }
            if (self.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr)) {
                self.allocated_bytes += extra;
                return true;
            }
            return false;
        } else if (new_len < buf.len) {
            const reduction = buf.len - new_len;
            if (self.parent_allocator.rawResize(buf, buf_align, new_len, ret_addr)) {
                self.allocated_bytes -|= reduction;
                return true;
            }
            return false;
        }
        return true;
    }

    /// Allocator vtable `remap`: like [`MemoryLimitAllocator.resize`] but may
    /// move the allocation, returning the (possibly new) base pointer.
    ///
    /// Growth beyond the cap returns null; otherwise the running total is
    /// adjusted by the same delta accounting as `resize`.
    fn remap(ctx: *anyopaque, buf: []u8, buf_align: Alignment, new_len: usize, ret_addr: usize) ?[*]u8 {
        const self: *MemoryLimitAllocator = @alignCast(@ptrCast(ctx));
        if (new_len > buf.len) {
            const extra = new_len - buf.len;
            if (self.allocated_bytes + extra > self.limit_bytes) {
                return null;
            }
            if (self.parent_allocator.rawRemap(buf, buf_align, new_len, ret_addr)) |result| {
                self.allocated_bytes += extra;
                return result;
            }
            return null;
        } else if (new_len < buf.len) {
            const reduction = buf.len - new_len;
            if (self.parent_allocator.rawRemap(buf, buf_align, new_len, ret_addr)) |result| {
                self.allocated_bytes -|= reduction;
                return result;
            }
            return null;
        }
        return buf.ptr;
    }

    /// Allocator vtable `free`: credits the freed length back to the running
    /// total and forwards to the parent.
    ///
    /// The subtraction saturates at zero (`-|=`). A single query's wrapper is a
    /// fresh instance whose running total starts at zero, but the executor's
    /// `self.allocator` also backs memory that outlives one query (the parsed
    /// statement cache, catalog and tree caches). When a later query frees such
    /// memory through its own fresh wrapper, `buf.len` legitimately exceeds this
    /// wrapper's running total; clamping keeps that from underflowing (which
    /// previously panicked once the cap was enabled by default). This can only
    /// ever under-count freed bytes, never over-count live bytes, so it cannot
    /// falsely trip the cap on a query that is genuinely within budget.
    fn free(ctx: *anyopaque, buf: []u8, buf_align: Alignment, ret_addr: usize) void {
        const self: *MemoryLimitAllocator = @alignCast(@ptrCast(ctx));
        self.allocated_bytes -|= buf.len;
        self.parent_allocator.rawFree(buf, buf_align, ret_addr);
    }
};

/// One unit of work handed to [`QueryExecutor.execute`].
///
/// All slice fields are borrowed for the duration of the call; the executor
/// does not take ownership of them. `params`/`param_classes` carry positional
/// bind values for `$N` placeholders which [`execute`] substitutes into the SQL
/// text before parsing (see `command_mod.substituteParams`).
pub const QueryRequest = struct {
    /// The raw SQL text to run (a single statement, optionally with `$N`
    /// placeholders when `params` is non-empty).
    sql: []const u8,
    /// Hex-encoded session token for authentication, or null when security is
    /// disabled or the statement is a `login`.
    session_token: ?[]const u8 = null,
    /// Per-query wall-clock timeout in milliseconds; overrides the executor's
    /// session-level `timeout_ms` when present.
    timeout_ms: ?u64 = null,
    /// Per-query memory cap in bytes; installs a [`MemoryLimitAllocator`] for
    /// the call. Overrides the session-level `memory_limit_bytes`.
    memory_limit_bytes: ?usize = null,
    /// Positional bind values for `$1..$N`; null entries bind SQL NULL.
    params: []const ?[]const u8 = &.{},
    /// Per-parameter class (numeric vs text vs ...) controlling how each bind
    /// value is quoted/validated during substitution.
    param_classes: []const command_mod.ParamClass = &.{},
};

/// The result of running one [`QueryRequest`].
///
/// On success the caller owns `columns`, `column_types` and `rows` (freed via
/// the response's own slices). On failure `error_message` is set and the row
/// data is empty. Note the mutually exclusive convention: a non-null
/// `error_message` means the statement failed logically even though
/// [`QueryExecutor.execute`] returned a value rather than a Zig error.
pub const QueryResponse = struct {
    /// Output column headers (one per projection), owned by the response.
    columns: []const []const u8 = &.{},
    /// Declared type of each output column, parallel to `columns`.
    column_types: []const ColumnType = &.{},
    /// Result rows; each row is a slice of text cells parallel to `columns`.
    /// SQL NULL is rendered as the literal text "NULL".
    rows: []const []const []const u8 = &.{},
    /// Number of rows changed for a DML/DDL statement (0 for a pure SELECT).
    rows_affected: u64 = 0,
    /// When true, numeric output columns carry big-endian binary cell bytes
    /// (not decimal text); the wire layer marks those columns binary in the
    /// RowDescription. Set only for the opt-in single-table `SELECT *` path.
    result_binary: bool = false,
    /// Human-readable failure message, or null on success.
    error_message: ?[]const u8 = null,
    /// True when the rows were already streamed to the client via a [`RowSink`]
    /// during the scan (see [`QueryExecutor.row_sink`]); `rows` is then empty and
    /// the caller must NOT re-send them, only the final command tag. `rows_affected`
    /// carries the streamed row count.
    streamed: bool = false,
};

/// A callback seam that lets the connection layer receive SELECT rows AS the scan
/// produces them, instead of the executor buffering the whole result set and the
/// connection sending it afterwards. Streaming overlaps the client's row decode
/// with the server's scan (the two run concurrently over one connection), which is
/// how a mature engine hides its scan behind the client's materialisation. Only the
/// scan-order (non-sorted) SELECT path uses it; a query that must sort still buffers.
///
///   begin: called once, after the column list is known, before any row — the sink
///          sends the RowDescription here.
///   row:   called per row with the row's text/binary cells (parallel to columns);
///          the sink encodes and sends one DataRow. The cells are owned by the
///          executor and freed right after this returns.
pub const RowSink = struct {
    ctx: *anyopaque,
    begin: *const fn (ctx: *anyopaque, names: []const []const u8, types: []const ColumnType, binary: bool) anyerror!void,
    row: *const fn (ctx: *anyopaque, cells: []const []const u8) anyerror!void,
};

/// Sentinel string used as the sort key for SQL NULL in ORDER BY.
///
/// It embeds control bytes so it cannot collide with a real value, and
/// [`orderCompareKey`] special-cases it to sort NULLs last (ascending).
const ORDER_NULL_SENTINEL = "\x00\x01NULL\x01\x00";

/// Renders an ORDER BY key value into a comparable string.
///
/// Numbers and bools are stringified (so [`orderCompareKey`] can later parse
/// them back for numeric ordering) and strings pass through; a missing value or
/// JSON null becomes [`ORDER_NULL_SENTINEL`]. The returned slice is owned by
/// `allocator`.
fn orderKeyString(allocator: std.mem.Allocator, v: ?query_iter.Scalar) ![]const u8 {
    if (v) |val| {
        return switch (val) {
            .null => try allocator.dupe(u8, ORDER_NULL_SENTINEL),
            .string => |s| try allocator.dupe(u8, s),
            .integer => |i| try std.fmt.allocPrint(allocator, "{d}", .{i}),
            .float => |f| try std.fmt.allocPrint(allocator, "{d}", .{f}),
            .bool => |b| try allocator.dupe(u8, if (b) "1" else "0"),
        };
    }
    return try allocator.dupe(u8, ORDER_NULL_SENTINEL);
}

/// Three-way compares two ORDER BY key strings produced by [`orderKeyString`].
///
/// The comparison is type-aware and tries progressively weaker forms so that
/// mixed data still orders sensibly: NULL sentinels sort last, then integers
/// numerically, then decimal strings (via `query_iter.cmpDecimalStr`), then
/// floats, and finally a plain byte-wise comparison as the fallback. Returns
/// -1/0/1.
/// Resolves an `ORDER BY` key against a result's output column names: a bare
/// integer is a 1-based ordinal (`ORDER BY 2`), otherwise it matches a column /
/// alias name. Returns the 0-based output-column index, or null if unresolved.
fn resolveOrderColIndex(cols: []const []const u8, name: []const u8) ?usize {
    if (std.fmt.parseInt(usize, name, 10)) |n| {
        if (n >= 1 and n <= cols.len) return n - 1;
    } else |_| {}
    for (cols, 0..) |c, i| if (std.mem.eql(u8, c, name)) return i;
    return null;
}

fn orderCompareKey(a: []const u8, b: []const u8) i32 {
    const a_null = std.mem.eql(u8, a, ORDER_NULL_SENTINEL);
    const b_null = std.mem.eql(u8, b, ORDER_NULL_SENTINEL);
    if (a_null and b_null) return 0;
    if (a_null) return 1;
    if (b_null) return -1;
    const ai: ?i64 = std.fmt.parseInt(i64, a, 10) catch null;
    const bi: ?i64 = std.fmt.parseInt(i64, b, 10) catch null;
    if (ai != null and bi != null) {
        if (ai.? < bi.?) return -1;
        if (ai.? > bi.?) return 1;
        return 0;
    }
    if (query_iter.cmpDecimalStr(a, b)) |c| return c;
    const af: ?f64 = std.fmt.parseFloat(f64, a) catch null;
    const bf: ?f64 = std.fmt.parseFloat(f64, b) catch null;
    if (af != null and bf != null) {
        if (af.? < bf.?) return -1;
        if (af.? > bf.?) return 1;
        return 0;
    }
    return switch (std.mem.order(u8, a, b)) { .lt => -1, .eq => 0, .gt => 1 };
}

/// Fixed-image byte width for a column of the given [`ColumnType`], matching
/// [`table.TableBuilder`]'s sizing: fixed-width scalars occupy their natural
/// width, and `TEXT`/`BLOB` occupy a 4-byte heap handle (the payload lives in the
/// row's heap area, not the fixed image). This MUST be used when a `CREATE TABLE`
/// column is built; a wrong (e.g. constant 255) size inflates every row's
/// fixed image with zero padding, which is stored verbatim on disk.
fn fixedSizeForType(t: ColumnType) u16 {
    return switch (t) {
        .BOOL => 1,
        .UINT32, .INT32, .FLOAT32 => 4,
        .UINT64, .INT64, .FLOAT64, .TIMESTAMP => 8,
        .TEXT, .BLOB => 4,
    };
}

/// Maps a SQL type name (as written in DDL) to the engine's [`ColumnType`].
///
/// Case-insensitive, and folds the common integer aliases (INT/BIGINT/SERIAL/...)
/// to `INT64` and the floating aliases to `FLOAT64`; BOOL/BOOLEAN to `BOOL`.
/// Anything unrecognised (including VARCHAR/TEXT/DATE/DECIMAL) defaults to
/// `TEXT`, which matches the executor's string-oriented storage of values.
fn mapSqlType(type_name: []const u8) ColumnType {
    const eq = std.ascii.eqlIgnoreCase;
    if (eq(type_name, "INT") or eq(type_name, "INTEGER") or eq(type_name, "INT64") or
        eq(type_name, "BIGINT") or eq(type_name, "INT8") or eq(type_name, "INT4") or
        eq(type_name, "INT2") or eq(type_name, "SMALLINT") or eq(type_name, "TINYINT") or
        eq(type_name, "SERIAL") or eq(type_name, "BIGSERIAL"))
    {
        return .INT64;
    }
    if (eq(type_name, "REAL") or eq(type_name, "FLOAT") or eq(type_name, "FLOAT4") or
        eq(type_name, "FLOAT8") or eq(type_name, "DOUBLE") or eq(type_name, "DOUBLE PRECISION"))
    {
        return .FLOAT64;
    }
    if (eq(type_name, "BOOL") or eq(type_name, "BOOLEAN")) return .BOOL;
    return .TEXT;
}

/// Returns true if the statement mutates data or schema.
///
/// Used by [`executeWrapped`] to decide whether the fencing epoch must be
/// checked (`guardWrite`) before running, so a stale replica cannot apply
/// writes. Read/transaction-control statements return false.
fn isWriteStmt(stmt: ast.Statement) bool {
    return switch (stmt) {
        .insert, .update, .delete, .create_table, .create_index, .create_sequence, .create_foreign_key, .drop_table, .drop_index, .drop_sequence, .drop_foreign_key, .import_stmt, .create_user, .drop_user, .alter_user, .alter_table, .create_role, .grant, .revoke => true,
        else => false,
    };
}

/// Running accumulator for a single aggregate over one group.
///
/// One [`AggAcc`] is folded per projection (and per HAVING aggregate) as rows
/// stream by in [`foldOneAgg`], then read out by [`formatAggregate`] /
/// [`aggNumericValue`]. It carries both a numeric and a string track because
/// MIN/MAX must work on non-numeric columns too, and it lazily allocates
/// `distinct_seen` only for COUNT(DISTINCT ...).
const AggAcc = struct {
    /// Number of rows counted (COUNT, or DISTINCT count when deduped).
    count: i64 = 0,
    /// Running numeric sum for SUM/AVG. Float so `DOUBLE`/decimal columns (and
    /// integers, which are exact in f64 up to 2^53) sum correctly - the old i64
    /// accumulator dropped every non-integer value (`SUM`/`AVG` on a `DOUBLE`
    /// column returned 0/NULL).
    sum: f64 = 0,
    /// Number of numerically-parseable values seen (the AVG denominator).
    num_seen: i64 = 0,
    /// Smallest numeric value seen so far (valid once `have_num`).
    min_num: f64 = 0,
    /// Largest numeric value seen so far (valid once `have_num`).
    max_num: f64 = 0,
    /// True while every numeric value folded has been integral, so SUM/MIN/MAX
    /// over an integer column still print as integers, not `123.0`.
    all_int: bool = true,
    /// Whether any numeric value has been folded yet.
    have_num: bool = false,
    /// Lexicographically smallest string value seen (for MIN on text).
    min_str: ?[]const u8 = null,
    /// Lexicographically largest string value seen (for MAX on text).
    max_str: ?[]const u8 = null,
    /// Cleared to false the first time a non-numeric value is seen; when false,
    /// MIN/MAX fall back to the string track.
    all_numeric: bool = true,
    /// Whether any non-NULL value was folded at all (distinguishes empty
    /// aggregate from a genuine 0/NULL result).
    saw_value: bool = false,
    /// Set of already-seen values for COUNT(DISTINCT ...); allocated lazily.
    distinct_seen: ?std.StringHashMap(void) = null,
};

/// The per-group state for a GROUP BY query.
///
/// Keyed in a hash map by the concatenated group-by column values; `col_vals`
/// holds the group's representative value for each non-aggregate projection,
/// `aggs` is parallel to the projections, and `hv` holds extra accumulators for
/// aggregates that appear only in HAVING (see [`collectHavingAggs`]).
const GroupAcc = struct {
    /// Representative cell value per projection (used for GROUP BY columns).
    col_vals: [][]const u8,
    /// Accumulator per projection, parallel to `sel.projections`.
    aggs: []AggAcc,
    /// Accumulators for HAVING-only aggregates, parallel to the collected
    /// HAVING specs.
    hv: []AggAcc = &.{},
};

/// Collects the aggregate calls referenced by a HAVING expression that are not
/// already computed by the SELECT list.
///
/// Walks the HAVING expression tree and, for each COUNT/SUM/MIN/MAX/AVG that
/// does not match an existing projection aggregate (same kind and argument),
/// appends an [`ast.AggregateCall`] to `out`. The result tells the GROUP BY
/// path which extra accumulators ([`GroupAcc.hv`]) to fold so HAVING can be
/// evaluated. `ga` is the group arena.
fn collectHavingAggs(sel: ast.SelectStmt, e: *const ast.Expr, out: *std.ArrayList(ast.AggregateCall), ga: std.mem.Allocator) !void {
    switch (e.*) {
        .binary_op => |op| {
            try collectHavingAggs(sel, op.left, out, ga);
            try collectHavingAggs(sel, op.right, out, ga);
        },
        .unary_not => |inner| try collectHavingAggs(sel, inner, out, ga),
        .func_call => |fc| {
            const kind: ?ast.AggregateKind =
                if (std.mem.eql(u8, fc.name, "COUNT")) .COUNT else if (std.mem.eql(u8, fc.name, "SUM")) .SUM else if (std.mem.eql(u8, fc.name, "MIN")) .MIN else if (std.mem.eql(u8, fc.name, "MAX")) .MAX else if (std.mem.eql(u8, fc.name, "AVG")) .AVG else null;
            if (kind) |k| {
                const arg: ast.AggregateArg = if (fc.args.len == 0) .star else if (fc.args[0].* == .column_ref) .{ .column = fc.args[0].column_ref } else .star;
                for (sel.projections) |proj| {
                    if (proj.expr == .aggregate and proj.expr.aggregate.kind == k) {
                        const p = proj.expr.aggregate;
                        const same = switch (p.argument) {
                            .star => arg == .star,
                            .column => |pc| (arg == .column and std.mem.eql(u8, pc, arg.column)),
                            .expression => false,
                        };
                        if (same) return;
                    }
                }
                try out.append(ga, ast.AggregateCall{ .kind = k, .argument = arg });
            }
            for (fc.args) |a| try collectHavingAggs(sel, a, out, ga);
        },
        else => {},
    }
}

/// Reads a finished aggregate accumulator out as an `f64`, for HAVING
/// comparisons.
///
/// AVG divides sum by count (guarding division by zero); MIN/MAX return the
/// numeric track. This is the float-domain sibling of [`formatAggregate`],
/// which produces the display string.
fn aggNumericValue(kind: ast.AggregateKind, acc: AggAcc) f64 {
    return switch (kind) {
        .COUNT => @floatFromInt(acc.count),
        .SUM => acc.sum,
        .AVG => if (acc.num_seen == 0) 0 else acc.sum / @as(f64, @floatFromInt(acc.num_seen)),
        .MIN => acc.min_num,
        .MAX => acc.max_num,
    };
}

/// Evaluates a scalar sub-expression of a HAVING clause to an `f64`.
///
/// Resolves literals, references to grouped columns (parsed from the group's
/// representative value), aggregate calls (matched first against the SELECT
/// projections, then against the HAVING-only specs in `hv_specs`), and the four
/// arithmetic operators. Returns null when the expression cannot be resolved to
/// a number (unknown column, non-arithmetic operator, division by zero), which
/// [`evalHaving`] treats as the row failing the predicate.
fn havingScalar(sel: ast.SelectStmt, grp: GroupAcc, hv_specs: []const ast.AggregateCall, e: *const ast.Expr) ?f64 {
    switch (e.*) {
        .literal_int => |v| return @floatFromInt(v),
        .literal_float => |v| return v,
        .column_ref => |c| {
            for (sel.projections, 0..) |proj, i| {
                if (proj.expr == .column and std.mem.eql(u8, proj.expr.column, c)) {
                    return std.fmt.parseFloat(f64, grp.col_vals[i]) catch null;
                }
            }
            return null;
        },
        .func_call => |fc| {
            const kind: ?ast.AggregateKind =
                if (std.mem.eql(u8, fc.name, "COUNT")) .COUNT else if (std.mem.eql(u8, fc.name, "SUM")) .SUM else if (std.mem.eql(u8, fc.name, "MIN")) .MIN else if (std.mem.eql(u8, fc.name, "MAX")) .MAX else if (std.mem.eql(u8, fc.name, "AVG")) .AVG else null;
            const k = kind orelse return null;
            const want_star = fc.args.len == 0;
            const want_col: ?[]const u8 = if (!want_star and fc.args[0].* == .column_ref) fc.args[0].column_ref else null;
            for (sel.projections, 0..) |proj, i| {
                if (proj.expr == .aggregate) {
                    const p = proj.expr.aggregate;
                    if (p.kind != k) continue;
                    const arg_ok = switch (p.argument) {
                        .star => want_star,
                        .column => |pc| (want_col != null and std.mem.eql(u8, pc, want_col.?)),
                        .expression => false,
                    };
                    if (arg_ok) return aggNumericValue(k, grp.aggs[i]);
                }
            }
            for (hv_specs, 0..) |spec, j| {
                if (spec.kind != k) continue;
                const arg_ok = switch (spec.argument) {
                    .star => want_star,
                    .column => |pc| (want_col != null and std.mem.eql(u8, pc, want_col.?)),
                    .expression => false,
                };
                if (arg_ok) return aggNumericValue(k, grp.hv[j]);
            }
            return null;
        },
        .binary_op => |op| switch (op.op) {
            .PLUS, .MINUS, .STAR, .SLASH => {
                const a = havingScalar(sel, grp, hv_specs, op.left) orelse return null;
                const b = havingScalar(sel, grp, hv_specs, op.right) orelse return null;
                return switch (op.op) {
                    .PLUS => a + b,
                    .MINUS => a - b,
                    .STAR => a * b,
                    .SLASH => if (b == 0) null else a / b,
                    else => null,
                };
            },
            else => return null,
        },
        else => return null,
    }
}

/// Evaluates a HAVING predicate for one group, returning whether it is kept.
///
/// Handles the boolean connectives (AND/OR/NOT) and the six comparison
/// operators, deferring each operand to [`havingScalar`]. A comparison whose
/// operand does not resolve to a number returns false (the group is dropped),
/// and any expression shape that is not a comparison/connective also returns
/// false.
fn evalHaving(sel: ast.SelectStmt, grp: GroupAcc, hv_specs: []const ast.AggregateCall, e: *const ast.Expr) bool {
    switch (e.*) {
        .binary_op => |op| switch (op.op) {
            .AND => return evalHaving(sel, grp, hv_specs, op.left) and evalHaving(sel, grp, hv_specs, op.right),
            .OR => return evalHaving(sel, grp, hv_specs, op.left) or evalHaving(sel, grp, hv_specs, op.right),
            .EQ, .NE, .GT, .LT, .GTE, .LTE => {
                const a = havingScalar(sel, grp, hv_specs, op.left) orelse return false;
                const b = havingScalar(sel, grp, hv_specs, op.right) orelse return false;
                return switch (op.op) {
                    .EQ => a == b,
                    .NE => a != b,
                    .GT => a > b,
                    .LT => a < b,
                    .GTE => a >= b,
                    .LTE => a <= b,
                    else => false,
                };
            },
            else => return false,
        },
        .unary_not => |inner| return !evalHaving(sel, grp, hv_specs, inner),
        else => return false,
    }
}

/// The per-session SQL executor.
///
/// One [`QueryExecutor`] represents a client session: it holds the current
/// transaction id, isolation level, snapshot, open savepoints and per-session
/// limits, and drives every statement against the shared [`Database`]. It is
/// not thread-safe on its own; concurrency between sessions is mediated by the
/// database's `rw_lock` and per-table `GroupLock`s that [`executeStatement`]
/// takes. The lifecycle is [`init`] → many [`execute`] calls → [`deinit`].
pub const QueryExecutor = struct {
    /// Working allocator for this session. Temporarily swapped for a
    /// [`MemoryLimitAllocator`] inside [`execute`] when a memory cap applies.
    allocator: Allocator,
    /// The shared database this session operates on (catalog, pool, WAL,
    /// transaction manager, security manager).
    db: *Database,
    /// The active MVCC transaction id, or null when no transaction is open
    /// (autocommit). [`executeWrapped`] opens an implicit transaction around a
    /// single statement when this is null.
    current_tx_id: ?u64 = null,
    /// Projection pushdown: when non-null, [`getVisibleVersion`] materialises
    /// ONLY these columns of a scanned row instead of every column. Set (and
    /// restored) by the SELECT drain for simple single-table reads where the
    /// full set of referenced columns (projection + WHERE + ORDER BY) is known,
    /// so a `SELECT id, field0 FROM t WHERE id = ?` decodes 2 columns rather than
    /// all of them. Null everywhere else (joins, aggregates, UPDATE/DELETE read
    /// paths), which keeps the original full-row behaviour. The slices are
    /// borrowed and stay valid for the duration of the drain that sets them.
    /// When set (by an equality index scan whose PKs arrive pk-ascending), base
    /// row lookups reuse this leaf cursor instead of re-descending per row. Only
    /// consulted for the tree it wraps; saved/restored around the scan like
    /// [`scan_needed_cols`].
    base_searcher: ?*BPlusTree.LeafReuseSearcher = null,
    scan_needed_cols: ?[]const []const u8 = null,
    /// Backing storage for [`scan_needed_cols`] (see [`collectNeededCols`]); its
    /// contents are only valid while `scan_needed_cols` points into it.
    needed_buf: [32][]const u8 = undefined,
    /// True when this session is an authenticated replication peer; reserved
    /// for replica-specific write paths.
    is_authenticated_replica: bool = false,

    /// The session's isolation level; see [`IsolationLevel`].
    isolation_level: IsolationLevel = .read_committed,
    /// The frozen commit-set snapshot captured at BEGIN for REPEATABLE READ /
    /// SNAPSHOT / SERIALIZABLE, or null under READ COMMITTED.
    snapshot: ?txn_mod.Snapshot = null,

    /// Stack of open SAVEPOINTs (innermost last); each owns a child
    /// transaction id. See [`Savepoint`].
    savepoints: std.ArrayList(Savepoint) = .empty,
    /// Set of subtransaction ids that are live (not yet committed/aborted), so
    /// their writes are visible to the parent. Initialised lazily.
    live_sub: std.AutoHashMap(u64, void) = undefined,
    /// Whether `live_sub` has been initialised (it is `undefined` until then).
    live_sub_init: bool = false,

    /// Session-default memory cap in bytes, overridable per request.
    memory_limit_bytes: ?usize = null,
    /// Cap on the bytes this executor may accumulate while materialising a single
    /// query's result set; `null` means unlimited. Seeded from
    /// [`result_bytes_limit_default`] in [`init`] so every server-created
    /// executor inherits the process-wide protection, and settable per instance
    /// (tests lower it on their own executor without touching the global). See
    /// [`result_bytes_limit_default`] for why the guard lives at materialisation
    /// rather than in a general allocator wrapper.
    result_bytes_limit: ?usize = null,
    /// Session-default query timeout in milliseconds, overridable per request.
    timeout_ms: ?u64 = null,
    /// Absolute deadline (epoch ms) computed from the timeout for the current
    /// query; checked by [`checkDeadline`]. Null means no deadline active.
    deadline_ms: ?i64 = null,

    /// When true, a commit waits for durable replication acknowledgement (see
    /// [`durableFinish`]); toggled by `SET DURABLE COMMIT ON/OFF`.
    durable_commit: bool = false,

    /// Diagnostic: number of bounded secondary-index range scans the planner has
    /// chosen this session (see the range branch in [`buildIteratorTree`]).
    /// Lets tests assert the range accelerator is actually used rather than the
    /// query merely being correct via the residual full-scan filter.
    range_scans_built: u64 = 0,

    /// Diagnostic: number of DESCENDING (backward) index range scans chosen for
    /// `ORDER BY <indexed col> DESC` (a subset of `range_scans_built`). Lets tests
    /// assert the backward cursor was used rather than an ascending scan reversed.
    desc_range_scans_built: u64 = 0,

    /// Diagnostic: number of COUNT(*) queries answered index-only (entries counted
    /// without any base-row fetch). Lets tests assert the fast path fired.
    index_only_counts: u64 = 0,

    /// Diagnostic: number of MIN/MAX queries answered from an index endpoint
    /// (first/last entry) instead of a full scan.
    index_minmax_used: u64 = 0,

    /// Diagnostic: number of `col IN (...)` / `col = a OR col = b [OR ...]`
    /// predicates served by an index union (one seek per value) rather than a
    /// full scan. Lets tests assert the OR-to-union rewrite actually fired.
    index_in_unions_built: u64 = 0,

    /// Diagnostic: number of `SELECT DISTINCT col` / `COUNT(DISTINCT col)`
    /// queries answered by a loose (skip) index scan that visits one entry per
    /// distinct value instead of scanning every row and hash-deduping.
    loose_distinct_used: u64 = 0,

    /// Diagnostic: number of `SELECT g, AGG(v) ... GROUP BY g` queries answered
    /// index-only from a covering `(g, v)` index (values decoded from the index
    /// key, no base-row fetch) instead of scanning every row.
    index_group_aggs: u64 = 0,

    /// Diagnostic: number of `WHERE lead = const ORDER BY next_col` queries served
    /// by a composite-index PREFIX scan (equality on the leading column, ordered
    /// by the second column) so the ORDER BY sort is skipped and LIMIT streams.
    composite_ordered_scans: u64 = 0,

    /// Set by [`buildIteratorTree`] to the column the chosen base access already
    /// yields in ascending, order-preserving (numeric for encoded types) order:
    /// the leading column of a secondary-index equality or range scan. Null for a
    /// table scan (lexical primary-key text order), an IN union (per-value blocks),
    /// or when no index applies. The SELECT executor uses it to SATISFY an
    /// `ORDER BY` on that column from the scan itself - streaming ascending (with
    /// an early LIMIT), or reversing for descending - instead of a comparison sort.
    scan_ordered_col: ?[]const u8 = null,

    /// Whether the base access yields [`scan_ordered_col`] in DESCENDING order
    /// (a backward index range scan) rather than ascending. Set with
    /// `scan_ordered_col` by [`buildIteratorTree`]; the executor compares it to
    /// the ORDER BY direction to decide whether the scan order is already final
    /// (stream + early LIMIT) or needs the collected rows reversed.
    scan_order_is_desc: bool = false,

    /// Per-query residual-predicate pushdown into the base scan (set by
    /// [`buildIteratorTree`], borrowed by the index scan iterators via
    /// [`fetchVisibleFilteredJson`]). When the query is a single-table read (no
    /// joins) whose WHERE references only enumerable columns, `residual_expr` is
    /// that WHERE and `residual_cols` (backed by `residual_cols_buf`, name slices
    /// borrowed from the AST) is every column it references. The scan then rejects
    /// non-matching rows on the RAW image before building their full JSON. Left
    /// null when a join is present or the column set is not fully knowable, in
    /// which case only the downstream `FilterIterator` filters (unchanged).
    residual_expr: ?*const ast.Expr = null,
    residual_cols_buf: [32][]const u8 = undefined,
    residual_cols: ?[]const []const u8 = null,
    /// True when the base scan has consumed the query's OFFSET itself (an ordered
    /// index scan that skipped the leading rows via `skip_remaining`), so the
    /// executor must NOT re-apply the offset and may stream + break at LIMIT.
    scan_offset_pushed: bool = false,

    /// Profiling (gated by env KAIDB_QPROF): when true, the SELECT path prints a
    /// per-query phase breakdown and `buildRowJson` accumulates into `qp_json`.
    qprof: bool = false,
    /// Optional row sink: when set by the connection layer, the scan-order SELECT
    /// path streams each row to it during the scan instead of buffering the whole
    /// result set, so the client decodes rows concurrently with the server scan.
    /// Null (the default) keeps the buffer-then-return behaviour. Cleared after use.
    row_sink: ?RowSink = null,
    /// Gated by env `KAIDB_BINARY_RESULTS`: when set, a single-table `SELECT *`
    /// ships numeric columns as binary (big-endian fixed-width) cells and marks
    /// them binary in the RowDescription, instead of decimal text. Default off,
    /// so the wire stays text for existing clients.
    binary_results: bool = false,
    /// Time spent materialising rows into JSON objects (`buildRowJson`), a subset
    /// of the scan phase. Reset per SELECT when profiling.
    qp_json: StopWatch = .{},
    /// Time spent in the per-PK base-table B+Tree seek (`table_tree.search`/leaf
    /// cursor `get`) inside `fetchVisibleFilteredJson`, a subset of scan. Isolates
    /// base-table descent cost from secondary-index walk cost. QPROF only.
    qp_seek: StopWatch = .{},

    /// Constructs a fresh session executor bound to `db`.
    ///
    /// Starts in autocommit (no transaction), READ COMMITTED, with no limits.
    /// Must be paired with [`deinit`] to release savepoints/snapshot/live_sub.
    /// Process-wide cap on the bytes a single query may accumulate while
    /// MATERIALISING its result set (the sum of every projected cell it buffers
    /// before the rows are handed back). `null` means no cap.
    ///
    /// This is deliberately NOT a general allocator cap: wrapping the executor's
    /// allocator for every query is unsafe, because query-scoped allocations such
    /// as cached parsed statements and DDL-created catalog column names outlive a
    /// single `execute()` call, so a per-call allocator wrapper corrupts their
    /// ownership. Instead the guard is applied at exactly the one place a query's
    /// footprint can grow without bound: result materialisation. A large
    /// unindexed `ORDER BY` (or any non-streamable scan) collects every matching
    /// row before it can sort/limit, so at 10M rows it grows the process until
    /// the OS OOM-kills the whole server. With this cap that query instead fails
    /// cleanly with "Query Memory Limit Exceeded" and the server keeps serving.
    ///
    /// Starts at 512 MiB, which fits any LIMIT-bounded result comfortably.
    /// `main` may raise or lift it from `config.query_memory_limit_bytes` at
    /// startup (0 in config means unlimited and sets this to `null`). It is
    /// written once before connections are accepted and only read thereafter, so
    /// the plain `var` needs no synchronisation.
    pub var result_bytes_limit_default: ?usize = 512 * 1024 * 1024;

    pub fn init(allocator: Allocator, db: *Database) QueryExecutor {
        return .{
            .allocator = allocator,
            .db = db,
            .current_tx_id = null,
            .is_authenticated_replica = false,
            .memory_limit_bytes = null,
            .result_bytes_limit = result_bytes_limit_default,
            .timeout_ms = null,
            .deadline_ms = null,
        };
    }

    /// The MVCC visibility test: may `current_tx` see the version with these
    /// `xmin`/`xmax` stamps?
    ///
    /// This is the heart of isolation. A version is visible when its creator
    /// (`xmin`) is visible to us and its deleter (`xmax`) is not:
    /// - `xmin` counts as valid if it is 0 (bootstrap), is our own transaction
    ///   or one of our live subtransactions ([`isOwnLiveWrite`]), or is
    ///   committed. Under a snapshot "committed" means "in the snapshot's
    ///   committed set and below its xmax"; otherwise it is a live commit-status
    ///   check via `txn_manager.isCommitted`.
    /// - a non-zero `xmax` hides the row when it is our own live write or a
    ///   committed transaction (again snapshot-relative when a snapshot is held);
    ///   an in-flight foreign `xmax` leaves the row visible.
    ///
    /// The subtlety is that the same predicate must behave differently per
    /// isolation level purely by whether `self.snapshot` is set, so both
    /// branches are threaded through here rather than duplicated at call sites.
    fn rowVisible(self: *QueryExecutor, current_tx: u64, xmin: u64, xmax: u64) bool {
        const io = self.db.pool.pager.io;
        const own = struct {
            fn f(e: *QueryExecutor, ct: u64, x: u64) bool {
                return x == ct or e.isOwnLiveWrite(x);
            }
        }.f;

        var xmin_valid = (xmin == 0) or own(self, current_tx, xmin);
        if (!xmin_valid) {
            if (self.snapshot) |*s| {
                xmin_valid = (xmin < s.xmax) and s.committed.contains(xmin);
            } else {
                xmin_valid = self.db.txn_manager.isCommitted(io, xmin);
            }
        }
        if (!xmin_valid) return false;

        if (xmax == 0) return true;
        if (self.isOwnLiveWrite(xmax)) return false;
        const xmax_committed = if (self.snapshot) |*s|
            ((xmax < s.xmax) and s.committed.contains(xmax))
        else
            self.db.txn_manager.isCommitted(io, xmax);
        if (xmax_committed) return false;
        return true;
    }

    /// Recursively tests whether an expression tree contains a subquery.
    ///
    /// Used by [`materializeSubqueries`] as a cheap pre-check so an expression
    /// with no subqueries is returned untouched instead of being deep-cloned.
    fn exprHasSubquery(e: *const ast.Expr) bool {
        return switch (e.*) {
            .subquery, .in_subquery => true,
            .binary_op => |b| exprHasSubquery(b.left) or exprHasSubquery(b.right),
            .unary_not => |u| exprHasSubquery(u),
            .is_null => |x| exprHasSubquery(x.operand),
            .in_list => |x| blk: {
                if (exprHasSubquery(x.operand)) break :blk true;
                for (x.items) |it| if (exprHasSubquery(it)) break :blk true;
                break :blk false;
            },
            .like => |x| exprHasSubquery(x.operand) or exprHasSubquery(x.pattern),
            .between => |x| exprHasSubquery(x.operand) or exprHasSubquery(x.lo) or exprHasSubquery(x.hi),
            .func_call => |f| blk: {
                for (f.args) |a| if (exprHasSubquery(a)) break :blk true;
                break :blk false;
            },
            else => false,
        };
    }

    /// Runs a scalar/IN subquery and returns its first column as a list of
    /// text values, allocated in `arena`.
    ///
    /// Executes the nested SELECT through [`executeStatementInternal`], frees
    /// the temporary [`QueryResponse`] it produced, and projects out column 0 of
    /// each row (skipping empty rows). Returns `error.SubqueryFailed` if the
    /// nested statement reported an error. The caller (materialisation) uses the
    /// result to rewrite the outer expression.
    fn evalSubqueryColumn(self: *QueryExecutor, sub: *const ast.SelectStmt, arena: std.mem.Allocator) ![]const []const u8 {
        const res = try self.executeStatementInternal(.{ .select = sub.* });
        defer {
            for (res.columns) |c| self.allocator.free(c);
            if (res.columns.len > 0) self.allocator.free(res.columns);
            for (res.rows) |row| {
                for (row) |cell| self.allocator.free(cell);
                self.allocator.free(row);
            }
            if (res.rows.len > 0) self.allocator.free(res.rows);
            if (res.column_types.len > 0) self.allocator.free(res.column_types);
            if (res.error_message) |m| self.allocator.free(m);
        }
        if (res.error_message != null) return error.SubqueryFailed;
        var out = std.ArrayList([]const u8).empty;
        for (res.rows) |row| {
            if (row.len == 0) continue;
            try out.append(arena, try arena.dupe(u8, row[0]));
        }
        return out.toOwnedSlice(arena);
    }

    /// Rewrites an expression tree, replacing every subquery with the literal
    /// value(s) it evaluates to.
    ///
    /// This is kaidb's subquery strategy: rather than correlate at scan time it
    /// eagerly evaluates uncorrelated subqueries once and splices the results
    /// in. A scalar `(SELECT ...)` becomes a text literal (or NULL when empty);
    /// an `x IN (SELECT ...)` becomes an `IN (list of literals)`. All other node
    /// shapes are cloned into `arena` with their children recursively
    /// materialised, so the returned tree is arena-owned and safe to run.
    /// Returns null only when the input is null. Called from the SELECT/UPDATE/
    /// DELETE paths before building the iterator tree.
    fn materializeSubqueries(self: *QueryExecutor, e: ?*ast.Expr, arena: std.mem.Allocator) anyerror!?*ast.Expr {
        const expr = e orelse return null;
        if (!exprHasSubquery(expr)) return expr;
        const out = try arena.create(ast.Expr);
        switch (expr.*) {
            .subquery => |sq| {
                const vals = try self.evalSubqueryColumn(sq, arena);
                if (vals.len == 0) {
                    out.* = .literal_null;
                } else {
                    out.* = .{ .literal_text = vals[0] };
                }
            },
            .in_subquery => |isq| {
                const vals = try self.evalSubqueryColumn(isq.subquery, arena);
                const items = try arena.alloc(*ast.Expr, vals.len);
                for (vals, 0..) |v, i| {
                    const it = try arena.create(ast.Expr);
                    it.* = .{ .literal_text = v };
                    items[i] = it;
                }
                const op = (try self.materializeSubqueries(isq.operand, arena)).?;
                out.* = .{ .in_list = .{ .operand = op, .items = items, .negated = isq.negated } };
            },
            .binary_op => |b| out.* = .{ .binary_op = .{
                .left = (try self.materializeSubqueries(b.left, arena)).?,
                .op = b.op,
                .right = (try self.materializeSubqueries(b.right, arena)).?,
            } },
            .unary_not => |u| out.* = .{ .unary_not = (try self.materializeSubqueries(u, arena)).? },
            .is_null => |x| out.* = .{ .is_null = .{ .operand = (try self.materializeSubqueries(x.operand, arena)).?, .negated = x.negated } },
            .in_list => |x| blk: {
                const items = try arena.alloc(*ast.Expr, x.items.len);
                for (x.items, 0..) |it, i| items[i] = (try self.materializeSubqueries(it, arena)).?;
                out.* = .{ .in_list = .{ .operand = (try self.materializeSubqueries(x.operand, arena)).?, .items = items, .negated = x.negated } };
                break :blk;
            },
            .like => |x| out.* = .{ .like = .{ .operand = (try self.materializeSubqueries(x.operand, arena)).?, .pattern = (try self.materializeSubqueries(x.pattern, arena)).?, .negated = x.negated } },
            .between => |x| out.* = .{ .between = .{ .operand = (try self.materializeSubqueries(x.operand, arena)).?, .lo = (try self.materializeSubqueries(x.lo, arena)).?, .hi = (try self.materializeSubqueries(x.hi, arena)).?, .negated = x.negated } },
            .func_call => |f| blk: {
                const args = try arena.alloc(*ast.Expr, f.args.len);
                for (f.args, 0..) |a, i| args[i] = (try self.materializeSubqueries(a, arena)).?;
                out.* = .{ .func_call = .{ .name = f.name, .args = args } };
                break :blk;
            },
            else => out.* = expr.*,
        }
        return out;
    }

    /// Records a table-granularity SIREAD for SSI, if the session is
    /// SERIALIZABLE.
    ///
    /// A no-op at weaker isolation levels or outside a transaction. The read
    /// footprint lets the transaction manager detect rw-antidependencies at
    /// commit time and abort a pivot ([`executeStatementInternal`]'s commit
    /// path). Errors from the manager are swallowed: tracking is best-effort and
    /// must not fail a read.
    fn ssiTrackRead(self: *QueryExecutor, table: []const u8) void {
        if (self.isolation_level != .serializable) return;
        const tx = self.current_tx_id orelse return;
        self.db.txn_manager.ssiRead(self.db.pool.pager.io, tx, table) catch {};
    }
    /// Records a table-granularity write for SSI, if the session is
    /// SERIALIZABLE.
    ///
    /// Companion to [`ssiTrackRead`]; combined they form the in/out
    /// antidependency edges Cahill's algorithm uses to catch write skew.
    /// Best-effort (errors ignored) and a no-op below SERIALIZABLE.
    fn ssiTrackWrite(self: *QueryExecutor, table: []const u8) void {
        if (self.isolation_level != .serializable) return;
        const tx = self.current_tx_id orelse return;
        self.db.txn_manager.ssiWrite(self.db.pool.pager.io, tx, table) catch {};
    }

    /// Lazily initialises the `live_sub` map on first use.
    ///
    /// The set of live subtransaction ids is only needed once a session uses
    /// SAVEPOINT or opens an explicit transaction, so it is left `undefined`
    /// until this is called (guarded by `live_sub_init`).
    fn ensureLiveSub(self: *QueryExecutor) void {
        if (!self.live_sub_init) {
            self.live_sub = std.AutoHashMap(u64, void).init(self.allocator);
            self.live_sub_init = true;
        }
    }

    /// Returns the transaction id that new row versions should be stamped with.
    ///
    /// Inside a SAVEPOINT this is the innermost subtransaction's id (so its
    /// writes can be rolled back independently); otherwise the outer
    /// transaction id, falling back to 1 when somehow called without one.
    fn currentWriteXid(self: *QueryExecutor) u64 {
        if (self.savepoints.items.len > 0) return self.savepoints.items[self.savepoints.items.len - 1].xid;
        return self.current_tx_id orelse 1;
    }

    /// Returns whether `xid` is this session's own transaction or one of its
    /// live subtransactions.
    ///
    /// [`rowVisible`] uses this so a session sees its own uncommitted writes
    /// (including those made under an inner savepoint) even though they are not
    /// yet committed to anyone else.
    fn isOwnLiveWrite(self: *QueryExecutor, xid: u64) bool {
        if (self.current_tx_id) |t| {
            if (xid == t) return true;
        }
        if (self.live_sub_init) return self.live_sub.contains(xid);
        return false;
    }

    /// Discards all open savepoints, optionally aborting their subtransactions.
    ///
    /// When `abort_subs` is true (used on ROLLBACK), every live subtransaction
    /// id is aborted in the transaction manager first; on COMMIT the caller
    /// passes false because those ids are committed separately. In both cases
    /// the savepoint names are freed and the `savepoints`/`live_sub` containers
    /// are cleared (retaining capacity). Manager abort errors are ignored.
    fn clearSavepoints(self: *QueryExecutor, abort_subs: bool) void {
        const io = self.db.pool.pager.io;
        if (self.live_sub_init and abort_subs) {
            var it = self.live_sub.keyIterator();
            while (it.next()) |k| self.db.txn_manager.abort(io, k.*) catch {};
        }
        for (self.savepoints.items) |sp| self.allocator.free(sp.name);
        self.savepoints.clearRetainingCapacity();
        if (self.live_sub_init) self.live_sub.clearRetainingCapacity();
    }

    /// Releases and clears the session's captured snapshot, if any.
    ///
    /// Called at the end of every transaction so a REPEATABLE READ / SNAPSHOT /
    /// SERIALIZABLE snapshot does not leak into the next one.
    fn clearSnapshot(self: *QueryExecutor) void {
        if (self.snapshot) |*s| {
            s.deinit();
            self.snapshot = null;
        }
    }

    /// Returns `error.QueryTimeoutExceeded` if the current query's deadline has
    /// passed.
    ///
    /// A no-op when no deadline is set. It is called once per row inside the
    /// scan loops so a long-running query is interrupted between rows rather
    /// than only at statement boundaries. Reads the real clock each call.
    pub fn checkDeadline(self: *QueryExecutor) !void {
        if (self.deadline_ms) |dl| {
            const now = Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds();
            if (now >= dl) {
                return error.QueryTimeoutExceeded;
            }
        }
    }

    /// Releases all session-owned resources.
    ///
    /// Frees savepoint names and the savepoint list, tears down the `live_sub`
    /// map if it was initialised, and clears any snapshot. Does not touch the
    /// shared [`Database`], which outlives the session.
    pub fn deinit(self: *QueryExecutor) void {
        for (self.savepoints.items) |sp| self.allocator.free(sp.name);
        self.savepoints.deinit(self.allocator);
        if (self.live_sub_init) self.live_sub.deinit();
        self.clearSnapshot();
    }

    /// Authorises an admin-only meta-command, returning a deny response or null
    /// to allow.
    ///
    /// Returns null (allow) when security is disabled or no users exist.
    /// Otherwise it requires a valid session token with the `admin` permission
    /// and returns a populated [`QueryResponse`] carrying the specific denial
    /// message ("missing session token", "Invalid session token format",
    /// "Authentication Error", "Permission Denied") when any check fails. Used
    /// to gate the `SET FENCE EPOCH` / `SET DURABLE COMMIT` commands.
    fn adminGate(self: *QueryExecutor, req: QueryRequest) !?QueryResponse {
        const sec = self.db.security_manager;
        if (!sec.enabled or sec.users.count() == 0) return null;
        const tok_hex = req.session_token orelse
            return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Authentication Required: missing session token") };
        const token = @import("../concurrency/security.zig").parseTokenHex(tok_hex) catch
            return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Invalid session token format") };
        const session = sec.validateSession(token) catch
            return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Authentication Error") };
        sec.checkPermission(&session, .admin) catch
            return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Permission Denied") };
        return null;
    }

    /// Finalises durable replication for a just-ended transaction.
    ///
    /// A no-op when durable replication is not configured. On abort
    /// (`committed == false`) it discards the pending records; on commit it
    /// ships them, waiting for the durability guarantee implied by
    /// `self.durable_commit` (quorum ack). A failure here surfaces to the caller
    /// as the "replication quorum not reached" error even though the local
    /// commit already succeeded.
    fn durableFinish(self: *QueryExecutor, committed: bool) !void {
        const dr = self.db.durable_repl orelse return;
        if (!committed) {
            dr.discardPending();
            return;
        }
        try dr.shipPending(self.durable_commit);
    }

    /// Returns the declared [`schema.ColumnType`] of `name` in `table_meta`, or
    /// `.TEXT` if the column is not found. Used to pick the order-preserving
    /// index-key encoding for a column value ([`schema.types.encodeIndexValueAlloc`]).
    fn colTypeByName(table_meta: Table, name: []const u8) schema.ColumnType {
        for (table_meta.columns) |c| {
            if (std.mem.eql(u8, c.name, name)) return c.type;
        }
        return .TEXT;
    }

    /// Extracts the constant a column is being tested for equality against in a
    /// WHERE clause.
    ///
    /// Looks for a top-level `col = literal` (either operand order) binary op
    /// where the column matches `target_col`, and returns the literal as an
    /// owned string (integers are formatted, text is duped). Returns null if the
    /// predicate is not a simple equality on this column. This is the hook that
    /// lets [`buildIteratorTree`] turn `WHERE pk = 5` into a point lookup rather
    /// than a full scan; the caller owns and frees the returned string.
    fn getEqualityValueForCol(self: *QueryExecutor, where_expr: ?*const ast.Expr, target_col: []const u8) !?[]const u8 {
        const expr = where_expr orelse return null;
        switch (expr.*) {
            .binary_op => |bin| {
                if (bin.op != .EQ) return null;

                switch (bin.left.*) {
                    .column_ref => |col| {
                        if (std.mem.eql(u8, col, target_col)) {
                            return switch (bin.right.*) {
                                .literal_int => |val| try std.fmt.allocPrint(self.allocator, "{d}", .{val}),
                                .literal_text => |val| try self.allocator.dupe(u8, val),
                                else => null,
                            };
                        }
                    },
                    else => {},
                }

                switch (bin.right.*) {
                    .column_ref => |col| {
                        if (std.mem.eql(u8, col, target_col)) {
                            return switch (bin.left.*) {
                                .literal_int => |val| try std.fmt.allocPrint(self.allocator, "{d}", .{val}),
                                .literal_text => |val| try self.allocator.dupe(u8, val),
                                else => null,
                            };
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
        return null;
    }

    /// Like [`getEqualityValueForCol`] but also descends `AND` conjunctions, so a
    /// `col = literal` buried in `... AND ...` is found.
    ///
    /// Used ONLY by the SELECT planner ([`buildIteratorTree`]) to pick an equality
    /// index for a multi-predicate query (e.g. `employee_id = 279 AND total_due >
    /// 10000` should seek the selective `employee_id` index rather than range-scan
    /// `total_due` or table-scan). Safe there because the base scan is always
    /// wrapped in a FILTER carrying the full WHERE, so the residual predicate is
    /// still enforced. NOT used by the UPDATE/DELETE direct-primary-key fast path,
    /// which applies to the matched key WITHOUT re-checking a residual predicate
    /// and so must keep the top-level-only [`getEqualityValueForCol`]. Caller frees.
    fn equalityConjunctForCol(self: *QueryExecutor, where_expr: ?*const ast.Expr, target_col: []const u8) !?[]const u8 {
        const expr = where_expr orelse return null;
        if (expr.* == .binary_op and expr.binary_op.op == .AND) {
            if (try self.equalityConjunctForCol(expr.binary_op.left, target_col)) |v| return v;
            return self.equalityConjunctForCol(expr.binary_op.right, target_col);
        }
        return self.getEqualityValueForCol(where_expr, target_col);
    }

    /// Extracts a range start key for a column from a WHERE clause.
    ///
    /// Recognises `col >= v`, `col > v`, or `col = v` (and the reflected form
    /// `v <op> col` only for equality) and returns `v` as an owned string, so a
    /// primary-key range scan can begin at that key instead of the first row.
    /// Returns null when no such lower bound is present. The caller owns and
    /// frees the returned string. Used by [`buildIteratorTree`] for the
    /// non-indexed table-scan path.
    fn getStartKeyForCol(self: *QueryExecutor, where_expr: ?*const ast.Expr, target_col: []const u8) !?[]const u8 {
        const expr = where_expr orelse return null;
        switch (expr.*) {
            .binary_op => |bin| {
                if (bin.op != .GTE and bin.op != .GT and bin.op != .EQ) return null;

                switch (bin.left.*) {
                    .column_ref => |col| {
                        if (std.mem.eql(u8, col, target_col)) {
                            return switch (bin.right.*) {
                                .literal_int => |val| try std.fmt.allocPrint(self.allocator, "{d}", .{val}),
                                .literal_text => |val| try self.allocator.dupe(u8, val),
                                else => null,
                            };
                        }
                    },
                    else => {},
                }

                switch (bin.right.*) {
                    .column_ref => |col| {
                        if (std.mem.eql(u8, col, target_col)) {
                            if (bin.op == .EQ) {
                                return switch (bin.left.*) {
                                    .literal_int => |val| try std.fmt.allocPrint(self.allocator, "{d}", .{val}),
                                    .literal_text => |val| try self.allocator.dupe(u8, val),
                                    else => null,
                                };
                            }
                        }
                    },
                    else => {},
                }
            },
            else => {},
        }
        return null;
    }

    /// A clustered (primary-key) scan window: an inclusive lower seek key and an
    /// inclusive upper stop key, either of which may be null. Both are owned by
    /// the caller.
    const PkWindow = struct { start: ?[]const u8, stop: ?[]const u8 };

    /// Chooses a bounded clustered-scan window for a primary-key predicate.
    ///
    /// The base table is keyed by the primary key stored as raw decimal/text and
    /// ordered byte-lexically. A numeric range like `id >= a AND id <= b` can be
    /// turned into a seek-to-`a` + stop-at-`b` window ONLY when the lexical order
    /// of the keys in that window coincides with numeric order. That holds when
    /// both bounds are non-negative decimals of the SAME digit width (equal length
    /// + no sign): every id numerically in `[a, b]` is then also lexically in
    /// `[a, b]`, so the window is a correct superset. Other-width strings (e.g.
    /// "21" or "2000000") may also fall inside the lexical window, but the
    /// downstream `FilterIterator` re-checks the full predicate and drops them, so
    /// the result stays exact. Mixed-width or signed bounds fall back to the
    /// existing one-sided lower-bound seek (no stop key), and everything else to a
    /// full scan. Both returned strings are owned by the caller.
    ///
    /// This is deliberately conservative: it never returns an unsafe stop key, so
    /// correctness never depends on the residual. A fully general clustered range
    /// (across digit widths, or negative ids) needs an order-preserving key
    /// encoding for the base tree, which is a separate, larger change.
    fn pkClusteredWindow(self: *QueryExecutor, where_expr: ?*const ast.Expr, pk_col: []const u8) !PkWindow {
        if (where_expr) |we| {
            var lo: ?CmpBound = null;
            var hi: ?CmpBound = null;
            defer {
                if (lo) |l| self.allocator.free(l.txt);
                if (hi) |h| self.allocator.free(h.txt);
            }
            try self.collectColBounds(we, pk_col, &lo, &hi);
            if (lo != null and hi != null) {
                const l = lo.?;
                const h = hi.?;
                if (l.txt.len > 0 and l.txt.len == h.txt.len and l.txt[0] != '-' and h.txt[0] != '-') {
                    const start = try self.allocator.dupe(u8, l.txt);
                    errdefer self.allocator.free(start);
                    const stop = try self.allocator.dupe(u8, h.txt);
                    return .{ .start = start, .stop = stop };
                }
            }
        }
        // No safe two-sided window: keep the existing one-sided lower-bound seek.
        return .{ .start = try self.getStartKeyForCol(where_expr, pk_col), .stop = null };
    }

    /// Whether `e` is a literal (int/float/text) value node.
    fn isLiteralExpr(e: *const ast.Expr) bool {
        return switch (e.*) {
            .literal_int, .literal_float, .literal_text => true,
            else => false,
        };
    }

    /// Whether a composite `(lead, second)` index scan seeked to `lead = const`
    /// with an encoded range on `second` FULLY captures `e` - i.e. every index
    /// entry in that key window provably satisfies the whole predicate, so the
    /// residual is redundant. True only for an AND-tree whose leaves are the
    /// `lead = literal` equality and range comparisons (`< <= > >=` / BETWEEN) on
    /// `second`; any other column, operator (OR, `<>`, NOT), or function makes it
    /// false (then the caller must verify each row instead of trusting the range).
    fn whereCapturedByCompositeKey(self: *QueryExecutor, e: *const ast.Expr, lead: []const u8, second: []const u8) bool {
        switch (e.*) {
            .binary_op => |b| switch (b.op) {
                .AND => return self.whereCapturedByCompositeKey(b.left, lead, second) and
                    self.whereCapturedByCompositeKey(b.right, lead, second),
                .EQ => {
                    if (b.left.* == .column_ref and std.mem.eql(u8, b.left.column_ref, lead) and isLiteralExpr(b.right)) return true;
                    if (b.right.* == .column_ref and std.mem.eql(u8, b.right.column_ref, lead) and isLiteralExpr(b.left)) return true;
                    return false;
                },
                .GT, .GTE, .LT, .LTE => {
                    if (b.left.* == .column_ref and std.mem.eql(u8, b.left.column_ref, second) and isLiteralExpr(b.right)) return true;
                    if (b.right.* == .column_ref and std.mem.eql(u8, b.right.column_ref, second) and isLiteralExpr(b.left)) return true;
                    return false;
                },
                else => return false,
            },
            .between => |bt| return !bt.negated and bt.operand.* == .column_ref and
                std.mem.eql(u8, bt.operand.column_ref, second),
            else => return false,
        }
    }

    /// Renders a literal expression as owned decimal/text, matching how INSERT
    /// stores column values. Returns null for non-literal operands. Caller frees.
    fn literalText(self: *QueryExecutor, e: *const ast.Expr) !?[]const u8 {
        return switch (e.*) {
            .literal_int => |v| try std.fmt.allocPrint(self.allocator, "{d}", .{v}),
            .literal_text => |v| try self.allocator.dupe(u8, v),
            else => null,
        };
    }

    /// One extracted comparison bound: whether it is a lower or upper bound,
    /// whether it is inclusive, and the owned literal text it compares against.
    const CmpBound = struct { is_lo: bool, incl: bool, txt: []const u8 };

    /// Parses a single `col <cmp> literal` (or reflected `literal <cmp> col`)
    /// comparison on `target_col` into a [`CmpBound`], or null if `e` is not such
    /// a comparison. The reflected form flips the sense (`5 < col` is `col > 5`).
    /// The returned `txt` is owned by the caller.
    fn parseComparison(self: *QueryExecutor, e: *const ast.Expr, target_col: []const u8) !?CmpBound {
        if (e.* != .binary_op) return null;
        const b = e.binary_op;
        if (b.left.* == .column_ref and std.mem.eql(u8, b.left.column_ref, target_col)) {
            const t = (try self.literalText(b.right)) orelse return null;
            return switch (b.op) {
                .GT => .{ .is_lo = true, .incl = false, .txt = t },
                .GTE => .{ .is_lo = true, .incl = true, .txt = t },
                .LT => .{ .is_lo = false, .incl = false, .txt = t },
                .LTE => .{ .is_lo = false, .incl = true, .txt = t },
                else => {
                    self.allocator.free(t);
                    return null;
                },
            };
        }
        if (b.right.* == .column_ref and std.mem.eql(u8, b.right.column_ref, target_col)) {
            const t = (try self.literalText(b.left)) orelse return null;
            return switch (b.op) {
                .GT => .{ .is_lo = false, .incl = false, .txt = t }, // 5 > col  => col < 5
                .GTE => .{ .is_lo = false, .incl = true, .txt = t },
                .LT => .{ .is_lo = true, .incl = false, .txt = t }, // 5 < col  => col > 5
                .LTE => .{ .is_lo = true, .incl = true, .txt = t },
                else => {
                    self.allocator.free(t);
                    return null;
                },
            };
        }
        return null;
    }

    /// An encoded index-key range for a single indexed column: `start_key` is the
    /// inclusive seek key (possibly empty for no lower bound), `end_key` bounds
    /// the walk (`<= end_key`, null for no upper bound). Both are owned.
    const RangeBounds = struct { start_key: []const u8, end_key: ?[]const u8 };

    /// Turns a `col BETWEEN lo AND hi`, `col <cmp> v`, or `col > x AND col < y`
    /// predicate on `target_col` into encoded [`RangeBounds`] suitable for an
    /// [`query_iter.IndexRangeScanIterator`] over the column's index, or null if
    /// the WHERE has no usable single-column range on it.
    ///
    /// Each bound value is encoded order-preservingly
    /// ([`schema.types.encodeIndexValueAlloc`], the same encoding as the stored
    /// keys). Inclusivity is expressed in the encoded key: an inclusive upper
    /// bound and an exclusive lower bound append `":\xFF"` (0xFF sorts after any
    /// pk byte, so the whole `pk` fan-out of that value is respectively included
    /// or skipped); the opposite ends use the bare encoded value. Caller frees
    /// `start_key` and `end_key`.
    fn getRangeForCol(self: *QueryExecutor, where_expr: ?*const ast.Expr, target_col: []const u8, col_type: schema.ColumnType) !?RangeBounds {
        const expr = where_expr orelse return null;
        var lo: ?CmpBound = null;
        var hi: ?CmpBound = null;
        defer {
            if (lo) |l| self.allocator.free(l.txt);
            if (hi) |h| self.allocator.free(h.txt);
        }

        // Walk the whole predicate (recursing through nested ANDs) so BOTH bounds
        // of a range on `target_col` are found even when the WHERE has more than
        // two conjuncts. A 3-way `a AND b AND c` parses left-associatively as
        // `AND(AND(a, b), c)`; inspecting only the top-level operands would miss
        // the bound buried in the left AND subtree, which silently dropped e.g.
        // the `total_due > 10000` half of `emp=279 AND td>10000 AND td<50000` and
        // made an ASC ordered scan fall back to scanning the whole prefix.
        try self.collectColBounds(expr, target_col, &lo, &hi);

        if (lo == null and hi == null) return null;

        const start_key = if (lo) |l| blk: {
            const enc = try schema.types.encodeIndexValueAlloc(self.allocator, col_type, l.txt);
            defer self.allocator.free(enc);
            break :blk if (l.incl)
                try self.allocator.dupe(u8, enc)
            else
                try std.fmt.allocPrint(self.allocator, "{s}:\xFF", .{enc});
        } else try self.allocator.dupe(u8, "");
        errdefer self.allocator.free(start_key);

        const end_key: ?[]const u8 = if (hi) |h| blk: {
            const enc = try schema.types.encodeIndexValueAlloc(self.allocator, col_type, h.txt);
            defer self.allocator.free(enc);
            break :blk if (h.incl)
                try std.fmt.allocPrint(self.allocator, "{s}:\xFF", .{enc})
            else
                try self.allocator.dupe(u8, enc);
        } else null;

        return RangeBounds{ .start_key = start_key, .end_key = end_key };
    }

    /// Recursively collects every range bound on `target_col` from `expr` into
    /// `lo`/`hi`, descending through nested `AND`s so a bound in either subtree is
    /// found (see [`getRangeForCol`]). Handles a `BETWEEN` on the column and any
    /// `<`/`<=`/`>`/`>=` comparison; ignores everything else. Later bounds on the
    /// same side replace earlier ones via [`assignBound`] (which frees the old).
    fn collectColBounds(self: *QueryExecutor, expr: *const ast.Expr, target_col: []const u8, lo: *?CmpBound, hi: *?CmpBound) anyerror!void {
        switch (expr.*) {
            .between => |bt| {
                if (bt.negated) return;
                if (bt.operand.* != .column_ref or !std.mem.eql(u8, bt.operand.column_ref, target_col)) return;
                const lot = (try self.literalText(bt.lo)) orelse return;
                const hit = (try self.literalText(bt.hi)) orelse {
                    self.allocator.free(lot);
                    return;
                };
                assignBound(lo, hi, .{ .is_lo = true, .incl = true, .txt = lot }, self.allocator);
                assignBound(lo, hi, .{ .is_lo = false, .incl = true, .txt = hit }, self.allocator);
            },
            .binary_op => |b| {
                if (b.op == .AND) {
                    try self.collectColBounds(b.left, target_col, lo, hi);
                    try self.collectColBounds(b.right, target_col, lo, hi);
                    return;
                }
                if (try self.parseComparison(expr, target_col)) |c| assignBound(lo, hi, c, self.allocator);
            },
            else => {},
        }
    }

    /// Assigns an extracted [`CmpBound`] into the lower/upper slot, freeing any
    /// previously-held text if the same side is set twice (e.g. a malformed
    /// `col > 1 AND col > 2`).
    fn assignBound(lo: *?CmpBound, hi: *?CmpBound, c: CmpBound, alloc: std.mem.Allocator) void {
        if (c.is_lo) {
            if (lo.*) |old| alloc.free(old.txt);
            lo.* = c;
        } else {
            if (hi.*) |old| alloc.free(old.txt);
            hi.* = c;
        }
    }

    /// Finds a `target_col IN (literal, ...)` predicate (top level or inside an
    /// AND) and returns the membership values ENCODED as index prefixes
    /// (`schema.types.encodeIndexValueAlloc`, same as the stored keys), so the
    /// planner can drive an index union - one equality seek per value - instead of
    /// a full table scan. Returns null unless the operand is exactly `target_col`,
    /// the `IN` is not negated, and every item is a literal. Caller owns the slice
    /// and each entry.
    fn getInListForCol(self: *QueryExecutor, where_expr: ?*const ast.Expr, target_col: []const u8, col_type: schema.ColumnType) !?[][]const u8 {
        const expr = where_expr orelse return null;
        switch (expr.*) {
            .in_list => |il| {
                if (il.negated) return null;
                if (il.operand.* != .column_ref or !std.mem.eql(u8, il.operand.column_ref, target_col)) return null;
                if (il.items.len == 0) return null;
                var out = std.ArrayList([]const u8).empty;
                errdefer {
                    for (out.items) |v| self.allocator.free(v);
                    out.deinit(self.allocator);
                }
                for (il.items) |it| {
                    const txt = (try self.literalText(it)) orelse return null; // non-literal -> bail
                    defer self.allocator.free(txt);
                    try out.append(self.allocator, try schema.types.encodeIndexValueAlloc(self.allocator, col_type, txt));
                }
                return try out.toOwnedSlice(self.allocator);
            },
            .binary_op => |b| {
                if (b.op != .AND) return null;
                if (try self.getInListForCol(b.left, target_col, col_type)) |v| return v;
                return self.getInListForCol(b.right, target_col, col_type);
            },
            else => return null,
        }
    }

    /// If `where_expr` is a pure `OR`-tree whose every leaf is `target_col =
    /// <literal>` (e.g. `emp=279 OR emp=281 OR emp=283`), returns the encoded
    /// value list so the planner can run it as an index UNION (one seek per
    /// value) exactly like `IN (...)`, instead of a full table scan. Returns null
    /// if any leaf is not an equality on `target_col` against a literal, or if
    /// there are fewer than two values (a lone equality is the eq-scan's job).
    /// Caller frees the slice and its entries.
    fn getOrEqualsForCol(self: *QueryExecutor, where_expr: ?*const ast.Expr, target_col: []const u8, col_type: schema.ColumnType) !?[][]const u8 {
        const expr = where_expr orelse return null;
        var out = std.ArrayList([]const u8).empty;
        errdefer {
            for (out.items) |v| self.allocator.free(v);
            out.deinit(self.allocator);
        }
        if (!(try self.collectOrEquals(expr, target_col, col_type, &out)) or out.items.len < 2) {
            for (out.items) |v| self.allocator.free(v);
            out.deinit(self.allocator);
            return null;
        }
        return try out.toOwnedSlice(self.allocator);
    }

    fn collectOrEquals(self: *QueryExecutor, expr: *const ast.Expr, col: []const u8, col_type: schema.ColumnType, out: *std.ArrayList([]const u8)) !bool {
        switch (expr.*) {
            .binary_op => |b| {
                if (b.op == .OR) {
                    const l = try self.collectOrEquals(b.left, col, col_type, out);
                    if (!l) return false;
                    return self.collectOrEquals(b.right, col, col_type, out);
                }
                if (b.op == .EQ) {
                    const lit: *const ast.Expr = if (b.left.* == .column_ref and std.mem.eql(u8, b.left.column_ref, col))
                        b.right
                    else if (b.right.* == .column_ref and std.mem.eql(u8, b.right.column_ref, col))
                        b.left
                    else
                        return false;
                    const txt = (try self.literalText(lit)) orelse return false;
                    defer self.allocator.free(txt);
                    try out.append(self.allocator, try schema.types.encodeIndexValueAlloc(self.allocator, col_type, txt));
                    return true;
                }
                return false;
            },
            else => return false,
        }
    }

    /// Extracts the two join columns from an equi-join `ON` expression.
    ///
    /// Returns `{left, right}` (borrowed column names) when the `ON` clause is a
    /// single `left_col = right_col`, otherwise null. A hash join is only
    /// possible for such equi-joins; a null result forces the nested-loop path
    /// in [`buildIteratorTree`].
    fn getJoinKeys(on_expr: *const ast.Expr) ?struct { left: []const u8, right: []const u8 } {
        switch (on_expr.*) {
            .binary_op => |bin| {
                if (bin.op == .EQ) {
                    switch (bin.left.*) {
                        .column_ref => |l_col| {
                            switch (bin.right.*) {
                                .column_ref => |r_col| {
                                    return .{ .left = l_col, .right = r_col };
                                },
                                else => {},
                            }
                        },
                        else => {},
                    }
                }
            },
            else => {},
        }
        return null;
    }

    /// Returns whether a table has a column named `col`.
    ///
    /// Tolerates a qualified name (`table.col`) by comparing only the part after
    /// the dot. Used when deciding which side of a join key belongs to the right
    /// table so the hash join builds its table on the correct column.
    fn hasColumn(meta: schema.Table, col: []const u8) bool {
        const base_col = if (std.mem.indexOfScalar(u8, col, '.')) |dot_idx| col[dot_idx + 1 ..] else col;
        for (meta.columns) |c| {
            if (std.mem.eql(u8, c.name, base_col)) return true;
        }
        return false;
    }

    /// Reads the persisted row/page statistics for a table, or null if none.
    ///
    /// Statistics live as ordinary MVCC rows in the `sys.table_stats` system
    /// table (written by ANALYZE), so this walks that table's version chain and
    /// returns the version visible to the current transaction, parsing
    /// `row_count`/`page_count` out of it. Any decode/parse failure yields null
    /// (treated as "no stats"), which the planner reads as "assume nothing" and
    /// falls back to a full scan / nested-loop join.
    pub fn getTableStats(self: *QueryExecutor, table_name: []const u8) ?@import("stats.zig").TableStats {
        const stats_table = for (self.db.catalog.tables.items) |tbl| {
            if (std.mem.eql(u8, tbl.name, "sys.table_stats")) break tbl;
        } else return null;

        const stats_tree = self.db.getTableTree("sys.table_stats") catch return null;
        defer stats_tree.deinit();

        const opt_val = stats_tree.search(table_name, self.allocator) catch return null;
        const val_bytes = opt_val orelse return null;
        defer self.allocator.free(val_bytes);

        const versions = self.db.reconstructVersionChain(val_bytes, self.allocator) catch return null;
        defer {
            for (versions) |*v| v.deinit(self.allocator);
            self.allocator.free(versions);
        }

        const current_tx = self.current_tx_id orelse 1;
        for (versions) |v| {
            if (v.xmin <= current_tx) {
                if (v.xmax == 0 or v.xmax > current_tx) {
                    const reader = RowReader.init(stats_table, v.fixed, v.heap);
                    const row_count_str = reader.readToString(self.allocator, "row_count") catch return null;
                    defer self.allocator.free(row_count_str);
                    const page_count_str = reader.readToString(self.allocator, "page_count") catch return null;
                    defer self.allocator.free(page_count_str);

                    const rc = std.fmt.parseInt(u64, row_count_str, 10) catch return null;
                    const pc = std.fmt.parseInt(u64, page_count_str, 10) catch return null;
                    return .{ .row_count = rc, .page_count = pc };
                }
            }
        }
        return null;
    }

    /// Makes a physical, page-by-page copy of the database file to
    /// `backup_path`.
    ///
    /// Flushes the buffer pool and fsyncs the live file first so the on-disk
    /// image is consistent, then copies all `num_pages` pages one PAGE_SIZE
    /// block at a time into a freshly created (truncated) file and fsyncs it.
    /// This is a cold physical backup, not a logical export; it captures the WAL
    /// state exactly as flushed. Backs the SQL `BACKUP` statement.
    /// Hot (online) backup of a running server: writes a consistent snapshot
    /// directory (`<backup_path>/snapshot.db` + `<backup_path>/wal/`) using the
    /// same primitive the replication path uses, so `BACKUP DATABASE TO 'dir'`
    /// does not require a stopped server and the result is restorable with the
    /// exact same tooling as an offline backup (`kaidb restore`, PITR).
    ///
    /// Runs under the db `rw_lock` held exclusively by [`executeStatement`] (a
    /// `.backup` statement has no single table, so it takes the coarse exclusive
    /// lock), which is exactly what we want: no checkpoint or vacuum can reshuffle
    /// pages during the page copy. Concurrent DML on other connections is blocked
    /// for the copy's duration; the WAL is copied alongside the pages so a restore
    /// replays it to a crash-consistent state as of the backup point. Must NOT
    /// re-acquire `rw_lock` here (it is not reentrant) or it self-deadlocks. (The
    /// previous form wrote a bare, WAL-less, unlocked single-file page image that
    /// neither the restore CLI nor the recovery path could consume.)
    fn backupDatabase(self: *QueryExecutor, backup_path: []const u8) !void {
        try self.db.exportSnapshot(backup_path);
    }

    /// Resolves the declared [`ColumnType`] of a column referenced by a SELECT.
    ///
    /// Strips any `table.` qualifier, then prefers a match among the tables the
    /// query actually involves (the base table and any joined right tables)
    /// before falling back to any table in the catalog with that column name.
    /// Returns null when unknown, in which case [`projectionType`] defaults to
    /// `TEXT`.
    /// Strips a leading `qual.` from `name` when `qual` matches the table name or
    /// its alias (case-insensitive), e.g. `a.id` -> `id` given alias `a`. Returns a
    /// subslice of `name` (no allocation); unqualified or non-matching names are
    /// returned unchanged, so `id` stays `id` and a stray `x.id` is left for the
    /// normal (null -> NULL) resolution rather than silently rebound.
    fn stripMatchingQualifier(name: []const u8, table: []const u8, alias: ?[]const u8) []const u8 {
        const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return name;
        const qual = name[0..dot];
        if (std.ascii.eqlIgnoreCase(qual, table)) return name[dot + 1 ..];
        if (alias) |a| if (std.ascii.eqlIgnoreCase(qual, a)) return name[dot + 1 ..];
        return name;
    }

    /// Recursively strips matching table qualifiers from every `column_ref` in an
    /// expression tree (WHERE/HAVING). Does not descend into subqueries: those have
    /// their own FROM scope and are normalised when executed in their own right.
    fn stripExprQualifiers(e: *ast.Expr, table: []const u8, alias: ?[]const u8) void {
        switch (e.*) {
            .column_ref => |c| e.* = .{ .column_ref = stripMatchingQualifier(c, table, alias) },
            .binary_op => |b| {
                stripExprQualifiers(b.left, table, alias);
                stripExprQualifiers(b.right, table, alias);
            },
            .unary_not => |u| stripExprQualifiers(u, table, alias),
            .is_null => |n| stripExprQualifiers(n.operand, table, alias),
            .in_list => |il| {
                stripExprQualifiers(il.operand, table, alias);
                for (il.items) |it| stripExprQualifiers(it, table, alias);
            },
            .like => |l| {
                stripExprQualifiers(l.operand, table, alias);
                stripExprQualifiers(l.pattern, table, alias);
            },
            .between => |bt| {
                stripExprQualifiers(bt.operand, table, alias);
                stripExprQualifiers(bt.lo, table, alias);
                stripExprQualifiers(bt.hi, table, alias);
            },
            .func_call => |f| for (f.args) |arg| stripExprQualifiers(arg, table, alias),
            .case_expr => |ce| {
                for (ce.whens) |w| {
                    stripExprQualifiers(w.cond, table, alias);
                    stripExprQualifiers(w.result, table, alias);
                }
                if (ce.else_result) |er| stripExprQualifiers(er, table, alias);
            },
            .in_subquery => |isq| stripExprQualifiers(isq.operand, table, alias),
            else => {}, // literals, placeholder, subquery
        }
    }

    /// Single-table qualifier normalisation (see the call site). No-op when the
    /// query has joins; otherwise strips a `table.`/`alias.` prefix from every
    /// column reference in the projections, WHERE, HAVING and ORDER BY so they
    /// resolve against the row's bare column names.
    fn normalizeSingleTableQualifiers(sel: ast.SelectStmt) void {
        if (sel.joins.len != 0) return;
        const t = sel.table_name;
        const a = sel.table_alias;
        for (sel.projections) |*p| {
            switch (p.expr) {
                .column => |c| p.expr = .{ .column = stripMatchingQualifier(c, t, a) },
                .aggregate => |agg| switch (agg.argument) {
                    .column => |c| {
                        var na = agg;
                        na.argument = .{ .column = stripMatchingQualifier(c, t, a) };
                        p.expr = .{ .aggregate = na };
                    },
                    else => {},
                },
                else => {},
            }
        }
        if (sel.where_expr) |w| stripExprQualifiers(w, t, a);
        if (sel.having_expr) |h| stripExprQualifiers(h, t, a);
        if (sel.order_by) |obs| {
            for (@constCast(obs)) |*o| o.column = stripMatchingQualifier(o.column, t, a);
        }
        if (sel.group_by) |gbs| {
            for (@constCast(gbs)) |*g| g.* = stripMatchingQualifier(g.*, t, a);
        }
    }

    /// Rewrites an `alias.col` qualifier to `realtable.col` when `alias` is one of
    /// this query's table aliases. The join executor resolves qualified columns in
    /// a combined row by REAL table name (verified: `emp.name` works, `e.name` did
    /// not), so for joins the qualifier must be preserved and mapped, not stripped.
    /// Real table names and unqualified names pass through unchanged. Allocates the
    /// rewritten name in `alloc`; on OOM it returns the original (which then simply
    /// fails to resolve, i.e. the prior behaviour, never a crash).
    fn rewriteAliasQualifier(alloc: Allocator, name: []const u8, aliases: []const ?[]const u8, tables: []const []const u8) []const u8 {
        const dot = std.mem.lastIndexOfScalar(u8, name, '.') orelse return name;
        const qual = name[0..dot];
        for (aliases, tables) |al, tbl| {
            if (al) |a| {
                if (std.ascii.eqlIgnoreCase(qual, a))
                    return std.fmt.allocPrint(alloc, "{s}.{s}", .{ tbl, name[dot + 1 ..] }) catch name;
            }
        }
        return name;
    }

    /// Recursively rewrites alias qualifiers in an expression tree (ON/WHERE/HAVING).
    fn rewriteExprAliases(alloc: Allocator, e: *ast.Expr, aliases: []const ?[]const u8, tables: []const []const u8) void {
        switch (e.*) {
            .column_ref => |c| e.* = .{ .column_ref = rewriteAliasQualifier(alloc, c, aliases, tables) },
            .binary_op => |b| {
                rewriteExprAliases(alloc, b.left, aliases, tables);
                rewriteExprAliases(alloc, b.right, aliases, tables);
            },
            .unary_not => |u| rewriteExprAliases(alloc, u, aliases, tables),
            .is_null => |n| rewriteExprAliases(alloc, n.operand, aliases, tables),
            .in_list => |il| {
                rewriteExprAliases(alloc, il.operand, aliases, tables);
                for (il.items) |it| rewriteExprAliases(alloc, it, aliases, tables);
            },
            .like => |l| {
                rewriteExprAliases(alloc, l.operand, aliases, tables);
                rewriteExprAliases(alloc, l.pattern, aliases, tables);
            },
            .between => |bt| {
                rewriteExprAliases(alloc, bt.operand, aliases, tables);
                rewriteExprAliases(alloc, bt.lo, aliases, tables);
                rewriteExprAliases(alloc, bt.hi, aliases, tables);
            },
            .func_call => |f| for (f.args) |arg| rewriteExprAliases(alloc, arg, aliases, tables),
            .case_expr => |ce| {
                for (ce.whens) |w| {
                    rewriteExprAliases(alloc, w.cond, aliases, tables);
                    rewriteExprAliases(alloc, w.result, aliases, tables);
                }
                if (ce.else_result) |er| rewriteExprAliases(alloc, er, aliases, tables);
            },
            .in_subquery => |isq| rewriteExprAliases(alloc, isq.operand, aliases, tables),
            else => {},
        }
    }

    /// Join-query alias normalisation: maps `alias.col` to `realtable.col` across
    /// projections, ON, WHERE, HAVING, ORDER BY and GROUP BY so aliased joins
    /// resolve like their real-table-name equivalents. No-op for single-table
    /// queries (handled by `normalizeSingleTableQualifiers`) and when no alias is
    /// present. Bounded to 16 relations; extra joins simply keep prior behaviour.
    fn normalizeJoinAliases(sel: ast.SelectStmt, alloc: Allocator) void {
        if (sel.joins.len == 0) return;
        var al_buf: [16]?[]const u8 = undefined;
        var tb_buf: [16][]const u8 = undefined;
        var n: usize = 0;
        al_buf[n] = sel.table_alias;
        tb_buf[n] = sel.table_name;
        n += 1;
        for (sel.joins) |j| {
            if (n >= al_buf.len) break;
            al_buf[n] = j.right_alias;
            tb_buf[n] = j.right_table;
            n += 1;
        }
        const aliases = al_buf[0..n];
        const tables = tb_buf[0..n];
        var any = false;
        for (aliases) |a| {
            if (a != null) any = true;
        }
        if (!any) return;
        for (sel.projections) |*p| {
            switch (p.expr) {
                .column => |c| p.expr = .{ .column = rewriteAliasQualifier(alloc, c, aliases, tables) },
                .aggregate => |agg| switch (agg.argument) {
                    .column => |c| {
                        var na = agg;
                        na.argument = .{ .column = rewriteAliasQualifier(alloc, c, aliases, tables) };
                        p.expr = .{ .aggregate = na };
                    },
                    else => {},
                },
                else => {},
            }
        }
        if (sel.where_expr) |w| rewriteExprAliases(alloc, w, aliases, tables);
        if (sel.having_expr) |h| rewriteExprAliases(alloc, h, aliases, tables);
        for (sel.joins) |j| rewriteExprAliases(alloc, j.on_expr, aliases, tables);
        if (sel.order_by) |obs| {
            for (@constCast(obs)) |*o| o.column = rewriteAliasQualifier(alloc, o.column, aliases, tables);
        }
        if (sel.group_by) |gbs| {
            for (@constCast(gbs)) |*g| g.* = rewriteAliasQualifier(alloc, g.*, aliases, tables);
        }
    }

    fn lookupColumnType(self: *QueryExecutor, sel: ast.SelectStmt, col_name: []const u8) ?ColumnType {
        const bare = if (std.mem.lastIndexOfScalar(u8, col_name, '.')) |dot| col_name[dot + 1 ..] else col_name;
        for (self.db.catalog.tables.items) |tbl| {
            const involved = std.mem.eql(u8, tbl.name, sel.table_name) or blk: {
                for (sel.joins) |j| if (std.mem.eql(u8, tbl.name, j.right_table)) break :blk true;
                break :blk false;
            };
            if (!involved) continue;
            for (tbl.columns) |c| if (std.mem.eql(u8, c.name, bare)) return c.type;
        }
        for (self.db.catalog.tables.items) |tbl| {
            for (tbl.columns) |c| if (std.mem.eql(u8, c.name, bare)) return c.type;
        }
        return null;
    }

    /// Formats a finished aggregate accumulator into its display string.
    ///
    /// Encodes the SQL result conventions: COUNT is always an integer; SUM over
    /// no numeric values is "0" if any value was seen else "NULL"; AVG is "NULL"
    /// on an empty group, otherwise printed as an integer when whole or trimmed
    /// to at most 4 decimals; MIN/MAX return the numeric extreme when the column
    /// was all-numeric and a value was seen, otherwise the string extreme, and
    /// "NULL" when the group was empty. Returned string is owned by the caller.
    /// The float-domain counterpart used for HAVING is [`aggNumericValue`].
    /// Formats a numeric aggregate result: an integral value prints as an integer
    /// (so integer-column SUM/MIN/MAX stay `123`, not `123.0`), a fractional value
    /// prints with up to 4 decimals, trailing zeros trimmed. Matches the
    /// benchmark's round-4 data and keeps AVG output stable.
    fn fmtNumeric(self: *QueryExecutor, x: f64, force_int: bool) ![]const u8 {
        if ((force_int or x == @trunc(x)) and @abs(x) < 9.007199254740992e15) {
            return std.fmt.allocPrint(self.allocator, "{d}", .{@as(i64, @intFromFloat(@trunc(x)))});
        }
        var buf: [64]u8 = undefined;
        const raw = try std.fmt.bufPrint(&buf, "{d:.4}", .{x});
        var end = raw.len;
        while (end > 0 and raw[end - 1] == '0') end -= 1;
        if (end > 0 and raw[end - 1] == '.') end -= 1;
        return self.allocator.dupe(u8, raw[0..end]);
    }

    fn formatAggregate(self: *QueryExecutor, agg: ast.AggregateCall, acc: AggAcc) ![]const u8 {
        return switch (agg.kind) {
            .COUNT => try std.fmt.allocPrint(self.allocator, "{d}", .{acc.count}),
            .SUM => if (acc.num_seen == 0)
                try self.allocator.dupe(u8, if (acc.saw_value) "0" else "NULL")
            else
                try self.fmtNumeric(acc.sum, acc.all_int),
            .AVG => if (acc.num_seen == 0)
                try self.allocator.dupe(u8, "NULL")
            else
                try self.fmtNumeric(acc.sum / @as(f64, @floatFromInt(acc.num_seen)), false),
            .MIN => if (!acc.saw_value)
                try self.allocator.dupe(u8, "NULL")
            else if (acc.all_numeric and acc.have_num)
                try self.fmtNumeric(acc.min_num, acc.all_int)
            else
                try self.allocator.dupe(u8, acc.min_str orelse "NULL"),
            .MAX => if (!acc.saw_value)
                try self.allocator.dupe(u8, "NULL")
            else if (acc.all_numeric and acc.have_num)
                try self.fmtNumeric(acc.max_num, acc.all_int)
            else
                try self.allocator.dupe(u8, acc.max_str orelse "NULL"),
        };
    }

    /// Determines the output [`ColumnType`] of a single projection.
    ///
    /// A plain column reference is resolved via [`lookupColumnType`] (defaulting
    /// to `TEXT`). COUNT reports `INT64`; SUM/AVG report `FLOAT64` (a SUM over a
    /// DOUBLE column is fractional, so `INT64` would mislabel it); MIN/MAX inherit
    /// the argument column's type when known (numeric columns then decode
    /// correctly), else `TEXT`. Fills `column_types` in the response header.
    fn projectionType(self: *QueryExecutor, sel: ast.SelectStmt, proj: ast.Projection) ColumnType {
        return switch (proj.expr) {
            .column => |col| self.lookupColumnType(sel, col) orelse .TEXT,
            .aggregate => |agg| switch (agg.kind) {
                .COUNT => .INT64,
                .SUM, .AVG => .FLOAT64,
                .MIN, .MAX => switch (agg.argument) {
                    .column => |c| self.lookupColumnType(sel, c) orelse .TEXT,
                    else => .TEXT,
                },
            },
            else => .TEXT,
        };
    }

    /// Builds the physical execution plan for a SELECT as a composed iterator
    /// tree.
    ///
    /// This is the query planner. It chooses the base access path in priority
    /// order: a primary-key point lookup if the WHERE pins the PK to a constant;
    /// else a secondary index scan if a non-PK index column is pinned (unless
    /// stats say the table is tiny, `page_count < 5`, where a scan is cheaper);
    /// else a full table scan, seeded with a range start key when the WHERE
    /// gives a PK lower bound. It then layers each JOIN on top, choosing a hash
    /// join over a nested-loop only for small right sides (`row_count < 100`)
    /// where [`CostModel.preferHashJoin`] agrees, and orienting the build/probe
    /// keys to the correct table; finally it wraps a FILTER node for the
    /// residual WHERE. The returned [`query_iter.RowIterator`] owns its
    /// sub-iterators and must be `deinit`'d by the caller; an error mid-build
    /// tears down what was built so far via `errdefer`.
    /// Returns the first VISIBLE, non-NULL value of `col` scanning its index from
    /// one end: ascending (for MIN) or descending (for MAX). Because the index is
    /// order-preserving, the first such entry is the extreme value, so MIN/MAX
    /// answer in O(1) amortised (one base-row fetch) instead of a full scan.
    /// Fetches the base row to (a) apply MVCC visibility and (b) read the true
    /// column value, and skips NULLs (SQL MIN/MAX ignore them). Returns null when
    /// the column has no live non-NULL value. Caller owns the returned string.
    fn indexEndpointValue(self: *QueryExecutor, table: Table, idx_tree: *BPlusTree, col: []const u8, want_max: bool, current_tx: u64) !?[]const u8 {
        const table_tree = try self.db.getTableTree(table.name);
        defer table_tree.deinit();

        const Step = struct {
            fn take(s: *QueryExecutor, tt: *BPlusTree, tbl: Table, c: []const u8, ct: u64, key: []const u8) !?[]const u8 {
                const colon = std.mem.indexOfScalar(u8, key, ':') orelse return null;
                const pk = key[colon + 1 ..];
                const val = (try tt.search(pk, s.allocator)) orelse return null;
                defer s.allocator.free(val);
                const vis = (try s.getVisibleVersion(tbl, val, false, ct)) orelse return null;
                defer s.freeTableRow(vis);
                if (try vis.getTextAlloc(s.allocator, c)) |cv| {
                    if (!std.mem.eql(u8, cv, "NULL")) return cv; // owned, handed to caller
                    s.allocator.free(cv);
                }
                return null; // NULL or not visible: caller advances
            }
        };

        if (want_max) {
            var it = try idx_tree.rangeScanDesc(null, null);
            defer it.deinit();
            while (try it.next()) |cell| {
                if (try Step.take(self, table_tree, table, col, current_tx, cell.key)) |v| return v;
            }
        } else {
            var it = try idx_tree.iterator();
            defer it.deinit();
            while (try it.next()) |cell| {
                if (try Step.take(self, table_tree, table, col, current_tx, cell.key)) |v| return v;
            }
        }
        return null;
    }

    /// Fast path for `SELECT MIN(col)|MAX(col)[, ...] FROM t` (no WHERE / GROUP BY
    /// / joins): each MIN/MAX over an indexed column is answered from that index's
    /// first/last entry (see [`indexEndpointValue`]) instead of scanning the whole
    /// table. Only when every projection is a MIN/MAX over an indexed column and
    /// this is the sole in-flight transaction (so the endpoint reflects a stable
    /// committed state). Returns null (fall back to the scanning aggregate)
    /// otherwise.
    /// Numeric value of an integer/float literal expression, else null.
    fn litValF64(e: *const ast.Expr) ?f64 {
        return switch (e.*) {
            .literal_int => |i| @floatFromInt(i),
            .literal_float => |f| f,
            else => null,
        };
    }

    fn isColRefNamed(e: *const ast.Expr, col: []const u8) bool {
        return e.* == .column_ref and std.mem.eql(u8, e.column_ref, col);
    }

    /// True when `e` is a conjunction (AND-tree) of plain range/equality
    /// comparisons on `col` alone (`col BETWEEN a AND b`, `col < k`, `col = k`,
    /// etc.), referencing no other column and no `<>`/OR/NOT/IN/LIKE/function. When
    /// this holds, the predicate is entirely decidable from `col`'s value, so an
    /// aggregate over `col` filtered by it can be answered from the index alone.
    fn whereIsPlainRangeOn(e: *const ast.Expr, col: []const u8) bool {
        switch (e.*) {
            .between => |b| return !b.negated and isColRefNamed(b.operand, col) and
                litValF64(b.lo) != null and litValF64(b.hi) != null,
            .binary_op => |b| switch (b.op) {
                .AND => return whereIsPlainRangeOn(b.left, col) and whereIsPlainRangeOn(b.right, col),
                .EQ, .LT, .LTE, .GT, .GTE => return (isColRefNamed(b.left, col) and litValF64(b.right) != null) or
                    (litValF64(b.left) != null and isColRefNamed(b.right, col)),
                else => return false,
            },
            else => return false,
        }
    }

    /// Extract the numeric lower/upper bounds a `whereIsPlainRangeOn(col)`
    /// predicate places on `col` (ignoring inclusivity, which does not matter for
    /// a selectivity estimate). Recurses through ANDs; leaves a bound null when
    /// the predicate is open on that side.
    fn rangeNumericBounds(e: *const ast.Expr, col: []const u8, lo: *?f64, hi: *?f64) void {
        switch (e.*) {
            .between => |b| if (isColRefNamed(b.operand, col)) {
                if (litValF64(b.lo)) |v| lo.* = v;
                if (litValF64(b.hi)) |v| hi.* = v;
            },
            .binary_op => |b| switch (b.op) {
                .AND => {
                    rangeNumericBounds(b.left, col, lo, hi);
                    rangeNumericBounds(b.right, col, lo, hi);
                },
                .GT, .GTE => {
                    if (isColRefNamed(b.left, col)) {
                        if (litValF64(b.right)) |v| lo.* = v;
                    } else if (isColRefNamed(b.right, col)) {
                        if (litValF64(b.left)) |v| hi.* = v;
                    }
                },
                .LT, .LTE => {
                    if (isColRefNamed(b.left, col)) {
                        if (litValF64(b.right)) |v| hi.* = v;
                    } else if (isColRefNamed(b.right, col)) {
                        if (litValF64(b.left)) |v| lo.* = v;
                    }
                },
                .EQ => {
                    const v = if (isColRefNamed(b.left, col)) litValF64(b.right) else if (isColRefNamed(b.right, col)) litValF64(b.left) else null;
                    if (v) |x| {
                        lo.* = x;
                        hi.* = x;
                    }
                },
                else => {},
            },
            else => {},
        }
    }

    /// Estimate the fraction of rows a plain range on `col` matches, from the
    /// index's value span (its first/last encoded key) under a uniform-distribution
    /// assumption (the same estimate PostgreSQL falls back to without a histogram).
    /// Decodes min/max straight from the index key endpoints (no base fetch), so it
    /// is O(one descent each). Returns null when it cannot estimate (non-fixed-width
    /// column, empty index, degenerate span), leaving the planner's default choice.
    fn estimateRangeFraction(we: *const ast.Expr, col: []const u8, ctype: ColumnType, idx_tree: *BPlusTree) !?f64 {
        if (!isFixedWidthIndexEnc(ctype)) return null;

        var it_lo = try idx_tree.rangeScan("", null);
        defer it_lo.deinit();
        const cmin = while (try it_lo.next()) |cell| {
            const colon = std.mem.indexOfScalar(u8, cell.key, ':') orelse continue;
            break decodeFixedEncF64(cell.key[0..colon], ctype) orelse continue;
        } else return null;

        var it_hi = try idx_tree.rangeScanDesc(null, null);
        defer it_hi.deinit();
        const cmax = while (try it_hi.next()) |cell| {
            const colon = std.mem.indexOfScalar(u8, cell.key, ':') orelse continue;
            break decodeFixedEncF64(cell.key[0..colon], ctype) orelse continue;
        } else return null;

        if (cmax <= cmin) return null; // single-valued or degenerate: no useful estimate

        var lo: ?f64 = null;
        var hi: ?f64 = null;
        rangeNumericBounds(we, col, &lo, &hi);
        const rlo = @max(lo orelse cmin, cmin);
        const rhi = @min(hi orelse cmax, cmax);
        if (rhi <= rlo) return 0;
        return (rhi - rlo) / (cmax - cmin);
    }

    /// Evaluate a `whereIsPlainRangeOn(col)` predicate against a single numeric
    /// value `v` of `col`. Precise (handles inclusive/exclusive bounds), so the
    /// index-only scan can filter each decoded value exactly.
    fn evalRangeOnValue(e: *const ast.Expr, col: []const u8, v: f64) bool {
        switch (e.*) {
            .between => |b| {
                const lo = litValF64(b.lo) orelse return false;
                const hi = litValF64(b.hi) orelse return false;
                return v >= lo and v <= hi;
            },
            .binary_op => |b| {
                if (b.op == .AND) return evalRangeOnValue(b.left, col, v) and evalRangeOnValue(b.right, col, v);
                // Normalise to `v OP lit` (flip when the literal is on the left).
                var op = b.op;
                var lit: f64 = undefined;
                if (isColRefNamed(b.left, col)) {
                    lit = litValF64(b.right) orelse return false;
                } else {
                    lit = litValF64(b.left) orelse return false;
                    op = switch (b.op) { .LT => .GT, .GT => .LT, .LTE => .GTE, .GTE => .LTE, else => b.op };
                }
                return switch (op) {
                    .EQ => v == lit,
                    .LT => v < lit,
                    .LTE => v <= lit,
                    .GT => v > lit,
                    .GTE => v >= lit,
                    else => false,
                };
            },
            else => return false,
        }
    }

    /// Index-only scalar aggregate: `SELECT agg(col), ... FROM t WHERE <range on col>`
    /// with no GROUP BY / join / DISTINCT / HAVING, where every aggregate is over the
    /// SAME column `col`, `col` leads a fixed-width index, and the WHERE is a plain
    /// range/equality on `col` only. The aggregated value sits in the index key, so
    /// the whole query is answered by an index-only range scan with NO base-row
    /// fetch. This is PostgreSQL's index-only aggregate; it closes a large gap on
    /// `SELECT avg(total_due) WHERE total_due BETWEEN ...` (measured ~197x on a
    /// disk-bound VM: the base-row descents dominated an otherwise index-answerable
    /// query). Aggregates needing a non-indexed column still fall through to the scan.
    fn tryIndexOnlyScalarAgg(self: *QueryExecutor, sel: ast.SelectStmt) !?QueryResponse {
        if (sel.joins.len != 0 or sel.group_by != null or sel.distinct or sel.having_expr != null) return null;
        if (sel.projections.len == 0 or sel.limit != null or sel.offset != null) return null;
        if (self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) != 1) return null;
        const we = sel.where_expr orelse return null;

        // Every projection must be a non-distinct aggregate over the same column
        // `col` (COUNT(*) is allowed alongside). Require at least one column
        // aggregate; pure COUNT(*) is already served by `tryIndexOnlyCount`.
        var vcol: ?[]const u8 = null;
        for (sel.projections) |p| {
            if (p.expr != .aggregate) return null;
            const agg = p.expr.aggregate;
            if (agg.distinct) return null;
            switch (agg.argument) {
                .star => {},
                .column => |c| {
                    if (vcol) |vc| {
                        if (!std.mem.eql(u8, vc, c)) return null;
                    } else vcol = c;
                },
                else => return null,
            }
        }
        const col = vcol orelse return null;

        const table_meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;
        const ctype = colTypeByName(table_meta, col);
        if (!isFixedWidthIndexEnc(ctype)) return null;
        if (!whereIsPlainRangeOn(we, col)) return null;

        const idx = for (self.db.catalog.indexes.items) |ix| {
            if (ix.table_id == table_meta.id and ix.exact and ix.key_columns.len > 0 and
                std.mem.eql(u8, ix.key_columns[0].name, col)) break ix;
        } else return null;

        const rb = (try self.getRangeForCol(we, col, ctype)) orelse return null;
        defer self.allocator.free(rb.start_key);
        defer if (rb.end_key) |e| self.allocator.free(e);

        // Upper bound: index keys are `enc:pk`, and `enc_hi:pk` sorts AFTER the bare
        // `enc_hi`, so extend the walk bound by the byte just above ':' (0x3b, ';')
        // to include every entry whose encoded value equals `enc_hi`. The precise
        // filter (`evalRangeOnValue`) then drops anything actually out of range.
        var upper: ?[]u8 = null;
        defer if (upper) |u| self.allocator.free(u);
        if (rb.end_key) |e| {
            const u = try self.allocator.alloc(u8, e.len + 1);
            @memcpy(u[0..e.len], e);
            u[e.len] = 0x3b;
            upper = u;
        }

        const idx_tree = try self.db.getIndexTree(idx.name);
        defer idx_tree.deinit();

        var accs = try self.allocator.alloc(AggAcc, sel.projections.len);
        defer self.allocator.free(accs);
        for (accs) |*a| a.* = .{};

        var it = try idx_tree.rangeScan(rb.start_key, if (upper) |u| @as([]const u8, u) else null);
        defer it.deinit();
        while (try it.next()) |cell| {
            const c0 = std.mem.indexOfScalar(u8, cell.key, ':') orelse continue;
            const v = decodeFixedEncF64(cell.key[0..c0], ctype) orelse continue;
            if (!evalRangeOnValue(we, col, v)) continue;
            for (sel.projections, 0..) |p, j| {
                const agg = p.expr.aggregate;
                const acc = &accs[j];
                if (agg.kind == .COUNT) {
                    acc.count += 1; // COUNT(*) and COUNT(col): col is the non-null indexed value
                } else {
                    acc.saw_value = true;
                    acc.sum += v;
                    acc.num_seen += 1;
                    if (v != @trunc(v)) acc.all_int = false;
                    if (!acc.have_num) {
                        acc.min_num = v;
                        acc.max_num = v;
                        acc.have_num = true;
                    } else {
                        if (v < acc.min_num) acc.min_num = v;
                        if (v > acc.max_num) acc.max_num = v;
                    }
                }
            }
        }

        var columns = try self.allocator.alloc([]const u8, sel.projections.len);
        var col_types = try self.allocator.alloc(ColumnType, sel.projections.len);
        var cells = try self.allocator.alloc([]const u8, sel.projections.len);
        for (sel.projections, 0..) |p, i| {
            const agg = p.expr.aggregate;
            columns[i] = try self.allocator.dupe(u8, p.alias orelse switch (agg.kind) {
                .COUNT => "COUNT",
                .SUM => "SUM",
                .AVG => "AVG",
                .MIN => "MIN",
                .MAX => "MAX",
            });
            col_types[i] = self.projectionType(sel, p);
            cells[i] = try self.formatAggregate(agg, accs[i]);
        }
        var rows = try self.allocator.alloc([]const []const u8, 1);
        rows[0] = cells;
        self.index_only_counts += 1;
        return QueryResponse{ .columns = columns, .column_types = col_types, .rows = rows, .rows_affected = 0 };
    }

    /// Index-only scalar aggregate over a COVERING composite index: serves
    /// `SELECT agg(v) [, COUNT(*)] FROM t WHERE f BETWEEN a AND b` (a plain range
    /// on `f`, aggregates over a different column `v`) entirely from an EXACT
    /// composite index that leads with the FILTER column `f` and carries the
    /// aggregated column `v` as its second key column. Both are read straight from
    /// the index key (`enc(f):enc(v):pk`), so the clustered base tree is never
    /// touched - this is the covering-index answer to the wide secondary-index
    /// base fetch (Q3), the same trick a DBA adds a covering index for on any
    /// clustered engine. Falls back (returns null) when no such index exists or
    /// the shape is unsupported; `tryIndexOnlyScalarAgg` still handles the case
    /// where the filter and aggregate are the same column.
    ///
    /// Soundness gate matches the sibling fast paths: sole in-flight transaction,
    /// no join/group/distinct/having/limit/offset. Correct because a plain range
    /// on the lead is one contiguous ascending run in the index, so the scan can
    /// stop the moment it leaves the range.
    fn tryIndexOnlyScalarAggComposite(self: *QueryExecutor, sel: ast.SelectStmt) !?QueryResponse {
        if (sel.joins.len != 0 or sel.group_by != null or sel.distinct or sel.having_expr != null) return null;
        if (sel.projections.len == 0 or sel.limit != null or sel.offset != null) return null;
        if (self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) != 1) return null;
        const we = sel.where_expr orelse return null;

        // All projections must be non-distinct aggregates sharing one value column
        // `vc` (COUNT(*) allowed alongside); require at least one column aggregate.
        var vcol: ?[]const u8 = null;
        for (sel.projections) |p| {
            if (p.expr != .aggregate) return null;
            const agg = p.expr.aggregate;
            if (agg.distinct) return null;
            switch (agg.argument) {
                .star => {},
                .column => |c| {
                    if (vcol) |v| {
                        if (!std.mem.eql(u8, v, c)) return null;
                    } else vcol = c;
                },
                else => return null,
            }
        }
        const vc = vcol orelse return null;

        const table_meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;
        const vtype = colTypeByName(table_meta, vc);
        if (!isFixedWidthIndexEnc(vtype)) return null;

        // An EXACT composite index (f, v) whose LEAD f (!= v) the WHERE ranges on.
        const idx = for (self.db.catalog.indexes.items) |ix| {
            if (ix.table_id != table_meta.id) continue;
            if (!ix.exact) continue;
            if (ix.key_columns.len < 2) continue;
            if (!std.mem.eql(u8, ix.key_columns[1].name, vc)) continue;
            if (std.mem.eql(u8, ix.key_columns[0].name, vc)) continue;
            if (!whereIsPlainRangeOn(we, ix.key_columns[0].name)) continue;
            break ix;
        } else return null;
        const fcol = idx.key_columns[0].name;
        const ftype = colTypeByName(table_meta, fcol);
        if (!isFixedWidthIndexEnc(ftype)) return null;

        const rb = (try self.getRangeForCol(we, fcol, ftype)) orelse return null;
        defer self.allocator.free(rb.start_key);
        defer if (rb.end_key) |e| self.allocator.free(e);

        const idx_tree = try self.db.getIndexTree(idx.name);
        defer idx_tree.deinit();

        var accs = try self.allocator.alloc(AggAcc, sel.projections.len);
        defer self.allocator.free(accs);
        for (accs) |*a| a.* = .{};

        // Scan from the lower bound; a plain range is one contiguous ascending run
        // on the lead, so stop as soon as we leave it (after having entered it).
        var it = try idx_tree.rangeScan(rb.start_key, null);
        defer it.deinit();
        var seen_in_range = false;
        while (try it.next()) |cell| {
            const key = cell.key;
            const c0 = std.mem.indexOfScalar(u8, key, ':') orelse continue;
            const fval = decodeFixedEncF64(key[0..c0], ftype) orelse continue;
            if (!evalRangeOnValue(we, fcol, fval)) {
                if (seen_in_range) break; // passed the upper end of the range
                continue; // still below an exclusive lower bound
            }
            seen_in_range = true;
            // Decode the aggregated value from the SECOND key field.
            const rest = key[c0 + 1 ..];
            const c1 = std.mem.indexOfScalar(u8, rest, ':') orelse continue;
            const v = decodeFixedEncF64(rest[0..c1], vtype) orelse continue;
            for (sel.projections, 0..) |p, j| {
                const agg = p.expr.aggregate;
                const acc = &accs[j];
                if (agg.kind == .COUNT) {
                    acc.count += 1;
                } else {
                    acc.saw_value = true;
                    acc.sum += v;
                    acc.num_seen += 1;
                    if (v != @trunc(v)) acc.all_int = false;
                    if (!acc.have_num) {
                        acc.min_num = v;
                        acc.max_num = v;
                        acc.have_num = true;
                    } else {
                        if (v < acc.min_num) acc.min_num = v;
                        if (v > acc.max_num) acc.max_num = v;
                    }
                }
            }
        }

        var columns = try self.allocator.alloc([]const u8, sel.projections.len);
        var col_types = try self.allocator.alloc(ColumnType, sel.projections.len);
        var cells = try self.allocator.alloc([]const u8, sel.projections.len);
        for (sel.projections, 0..) |p, i| {
            const agg = p.expr.aggregate;
            columns[i] = try self.allocator.dupe(u8, p.alias orelse switch (agg.kind) {
                .COUNT => "COUNT",
                .SUM => "SUM",
                .AVG => "AVG",
                .MIN => "MIN",
                .MAX => "MAX",
            });
            col_types[i] = self.projectionType(sel, p);
            cells[i] = try self.formatAggregate(agg, accs[i]);
        }
        var rows = try self.allocator.alloc([]const []const u8, 1);
        rows[0] = cells;
        self.index_only_counts += 1;
        return QueryResponse{ .columns = columns, .column_types = col_types, .rows = rows, .rows_affected = 0 };
    }

    fn tryIndexMinMax(self: *QueryExecutor, sel: ast.SelectStmt) !?QueryResponse {
        if (sel.joins.len != 0 or sel.group_by != null or sel.having_expr != null or sel.distinct) return null;
        if (sel.where_expr != null or sel.projections.len == 0) return null;
        if (self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) != 1) return null;
        const current_tx = self.current_tx_id orelse return null;

        const table_meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;

        // Validate: every projection is MIN/MAX over a column that leads an index.
        for (sel.projections) |p| {
            if (p.expr != .aggregate) return null;
            const agg = p.expr.aggregate;
            if (agg.kind != .MIN and agg.kind != .MAX) return null;
            const col = switch (agg.argument) {
                .column => |c| c,
                else => return null,
            };
            const has_idx = for (self.db.catalog.indexes.items) |idx| {
                if (idx.table_id == table_meta.id and idx.key_columns.len > 0 and
                    std.mem.eql(u8, idx.key_columns[0].name, col)) break true;
            } else false;
            if (!has_idx) return null;
        }

        var columns = try self.allocator.alloc([]const u8, sel.projections.len);
        var col_types = try self.allocator.alloc(ColumnType, sel.projections.len);
        var row = try self.allocator.alloc([]const u8, sel.projections.len);
        for (sel.projections, 0..) |p, i| {
            const agg = p.expr.aggregate;
            const col = agg.argument.column;
            columns[i] = try self.allocator.dupe(u8, p.alias orelse if (agg.kind == .MAX) "MAX" else "MIN");
            col_types[i] = colTypeByName(table_meta, col);
            const idx = for (self.db.catalog.indexes.items) |ix| {
                if (ix.table_id == table_meta.id and ix.key_columns.len > 0 and
                    std.mem.eql(u8, ix.key_columns[0].name, col)) break ix;
            } else unreachable;
            const idx_tree = try self.db.getIndexTree(idx.name);
            defer idx_tree.deinit();
            row[i] = (try self.indexEndpointValue(table_meta, idx_tree, col, agg.kind == .MAX, current_tx)) orelse
                try self.allocator.dupe(u8, "NULL");
        }
        self.index_minmax_used += 1;
        var rows = try self.allocator.alloc([]const []const u8, 1);
        rows[0] = row;
        return QueryResponse{ .columns = columns, .column_types = col_types, .rows = rows, .rows_affected = 0 };
    }

    /// True for column types whose index-value encoding is FIXED WIDTH (a 16-hex
    /// string; see [`types.encodeIndexValueAlloc`]). Loose-distinct's skip step
    /// (jump past every entry sharing an encoded prefix) is only sound when no
    /// encoded value is a prefix of another, which fixed-width guarantees;
    /// variable-length TEXT/BLOB/DECIMAL do not qualify.
    fn isFixedWidthIndexEnc(ct: ColumnType) bool {
        return switch (ct) {
            .UINT32, .UINT64, .INT32, .INT64, .TIMESTAMP, .FLOAT32, .FLOAT64 => true,
            else => false,
        };
    }

    /// Fast path for `SELECT DISTINCT col FROM t` and `SELECT COUNT(DISTINCT col)
    /// FROM t` (no WHERE / GROUP BY / HAVING / join / ORDER BY / LIMIT / OFFSET):
    /// a LOOSE (skip) index scan visits ONE entry per distinct value instead of
    /// scanning every row and hash-deduping.
    ///
    /// Index keys are `enc:pk` (an order-preserving encoded column value, a
    /// colon, then the primary key; the encoding is colon-free). After reading
    /// the first entry of a distinct value `enc`, the scan seeks to `enc ++ 0x3b`
    /// (`;`, the byte just above the `:` separator), which sorts strictly after
    /// every `enc:...` key and at-or-before the next distinct value's keys, so
    /// the next `rangeScan` lands on the next distinct value. This is only sound
    /// when no encoded value is a prefix of another, so it is gated on a
    /// FIXED-WIDTH encoded column (see [`isFixedWidthIndexEnc`]).
    ///
    /// NULLs are handled, not excluded by gate: a NULL column value indexes as
    /// the literal "NULL" (its 4-byte encoding is distinguishable from any real
    /// 16-hex value), so the scan counts it out of `COUNT(DISTINCT c)` and keeps
    /// exactly one NULL group in `SELECT DISTINCT c`, matching SQL.
    ///
    /// Soundness gate (mirrors [`tryIndexOnlyCount`]): the column must lead an
    /// EXACT index (no stale entries linger) and this must be the sole in-flight
    /// transaction (so every index entry maps to a visible row). Returns null
    /// (fall back to the correct scan-and-dedupe path) whenever a condition fails.
    fn tryLooseDistinct(self: *QueryExecutor, sel: ast.SelectStmt) !?QueryResponse {
        if (sel.joins.len != 0 or sel.group_by != null or sel.having_expr != null) return null;
        if (sel.where_expr != null or sel.order_by != null or sel.limit != null or sel.offset != null) return null;
        if (sel.projections.len != 1) return null;
        if (self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) != 1) return null;
        const current_tx = self.current_tx_id orelse return null;

        // Two accepted shapes: `SELECT DISTINCT col` (plain-column projection with
        // the DISTINCT flag) and `SELECT COUNT(DISTINCT col)` (a COUNT aggregate
        // marked distinct). Anything else is not this fast path.
        var target_col: []const u8 = undefined;
        var is_count = false;
        switch (sel.projections[0].expr) {
            .column => |c| {
                if (!sel.distinct) return null;
                target_col = c;
            },
            .aggregate => |agg| {
                if (sel.distinct) return null;
                if (agg.kind != .COUNT or !agg.distinct) return null;
                target_col = switch (agg.argument) {
                    .column => |c| c,
                    else => return null,
                };
                is_count = true;
            },
            else => return null,
        }

        const table_meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;

        const col = for (table_meta.columns) |c| {
            if (std.mem.eql(u8, c.name, target_col)) break c;
        } else return null;
        if (!isFixedWidthIndexEnc(col.type)) return null;

        const idx = for (self.db.catalog.indexes.items) |ix| {
            if (ix.table_id == table_meta.id and ix.key_columns.len > 0 and
                std.mem.eql(u8, ix.key_columns[0].name, target_col)) break ix;
        } else return null;
        if (!idx.exact) return null;

        const idx_tree = try self.db.getIndexTree(idx.name);
        defer idx_tree.deinit();
        const table_tree = try self.db.getTableTree(table_meta.name);
        defer table_tree.deinit();

        var values = std.ArrayList([]const u8).empty;
        errdefer {
            for (values.items) |v| self.allocator.free(v);
            values.deinit(self.allocator);
        }
        var n: u64 = 0;

        // Skip-scan: seek to the first key >= cur_start, take its distinct value,
        // then advance cur_start past every key sharing that encoded prefix.
        var cur_start = try self.allocator.dupe(u8, "");
        defer self.allocator.free(cur_start);
        while (true) {
            var it = try idx_tree.rangeScan(cur_start, null);
            const cell = (try it.next()) orelse {
                it.deinit();
                break;
            };
            const colon = std.mem.indexOfScalar(u8, cell.key, ':') orelse {
                it.deinit();
                return null; // unexpected key shape: bail to the safe path
            };
            const enc = try self.allocator.dupe(u8, cell.key[0..colon]);
            defer self.allocator.free(enc);
            const pk = try self.allocator.dupe(u8, cell.key[colon + 1 ..]);
            defer self.allocator.free(pk);
            it.deinit();

            // A NULL column value indexes as the literal "NULL" (parseInt/parseFloat
            // fail, so `encodeIndexValueAlloc` dupes the raw string); a real value
            // is always an `index_key.ENC_WIDTH`-byte token for these fixed-width
            // types. So a non-token width is an unambiguous NULL marker here. SQL
            // `COUNT(DISTINCT c)` excludes NULLs; `SELECT DISTINCT c` keeps exactly
            // one NULL group (all NULLs share the single "NULL" entry).
            const is_null = enc.len != schema.index_key.ENC_WIDTH;

            if (is_count) {
                if (!is_null) n += 1;
            } else if (is_null) {
                try values.append(self.allocator, try self.allocator.dupe(u8, "NULL"));
                n += 1;
            } else {
                // Fetch the one base row to read the true, display-formatted value
                // (and confirm visibility). Under the gate every entry is visible.
                const raw = (try table_tree.search(pk, self.allocator)) orelse return null;
                defer self.allocator.free(raw);
                const vis = (try self.getVisibleVersion(table_meta, raw, false, current_tx)) orelse return null;
                defer self.freeTableRow(vis);
                if (try vis.getTextAlloc(self.allocator, target_col)) |cv| {
                    try values.append(self.allocator, cv);
                } else return null;
                n += 1;
            }

            // Advance to the next distinct value: enc ++ 0x3b (see the doc comment).
            const next = try self.allocator.alloc(u8, enc.len + 1);
            @memcpy(next[0..enc.len], enc);
            next[enc.len] = 0x3b;
            self.allocator.free(cur_start);
            cur_start = next;
        }

        self.loose_distinct_used += 1;

        if (is_count) {
            for (values.items) |v| self.allocator.free(v);
            values.deinit(self.allocator);
            const columns = try self.allocator.alloc([]const u8, 1);
            columns[0] = try self.allocator.dupe(u8, sel.projections[0].alias orelse "COUNT");
            const col_types = try self.allocator.alloc(ColumnType, 1);
            col_types[0] = .INT64;
            const rows = try self.allocator.alloc([]const []const u8, 1);
            const row = try self.allocator.alloc([]const u8, 1);
            row[0] = try std.fmt.allocPrint(self.allocator, "{d}", .{n});
            rows[0] = row;
            return QueryResponse{ .columns = columns, .column_types = col_types, .rows = rows, .rows_affected = 0 };
        }

        const columns = try self.allocator.alloc([]const u8, 1);
        columns[0] = try self.allocator.dupe(u8, sel.projections[0].alias orelse target_col);
        const col_types = try self.allocator.alloc(ColumnType, 1);
        col_types[0] = col.type;
        const rows = try self.allocator.alloc([]const []const u8, values.items.len);
        for (values.items, 0..) |v, i| {
            const row = try self.allocator.alloc([]const u8, 1);
            row[0] = v;
            rows[i] = row;
        }
        values.deinit(self.allocator); // rows now own the value strings
        return QueryResponse{ .columns = columns, .column_types = col_types, .rows = rows, .rows_affected = 0 };
    }

    /// Decodes a fixed-width index-value token (a 10-byte order-preserving
    /// base-128 token, see [`schema.index_key`]) back to an f64. Returns null
    /// when `enc` is not a valid token (the NULL sentinel "NULL", or any other
    /// shape). The sign-flip / IEEE-754 total-order transform is inverted inside
    /// the codec.
    fn decodeFixedEncF64(enc: []const u8, ct: ColumnType) ?f64 {
        return schema.index_key.decodeF64(enc, ct);
    }

    /// Index-only GROUP BY aggregation for `SELECT g, AGG(v)... FROM t GROUP BY g`
    /// (no WHERE / HAVING / join). When an index leads with the group column `g`
    /// and (for aggregates over a value column `v`) carries `v` as its second key
    /// column, the whole query is answered by ONE ordered walk of that index:
    /// entries arrive grouped by `g` (order-preserving leading field), each
    /// group's `g` and every `v` are decoded straight from the composite key
    /// (`enc(g):enc(v):pk`), and the aggregates are folded WITHOUT fetching a
    /// single base row. `COUNT(*)`-only queries need just a `g` index.
    ///
    /// Values fold exactly as the scanning path (`foldOneAgg`): decoded f64 sum /
    /// count / min / max, `all_int` cleared on any fractional value, so output
    /// formatting via [`formatAggregate`] agrees with the non-fast path. A NULL
    /// `g` forms its own group; a NULL `v` (decoded as null) is skipped by the
    /// numeric aggregates and by `COUNT(v)`, but still counted by `COUNT(*)`,
    /// matching SQL. ORDER BY / OFFSET / LIMIT over the grouped result are applied
    /// in memory (there are as many rows as distinct `g`).
    ///
    /// Soundness gate (as [`tryIndexOnlyCount`]): EXACT index + sole in-flight
    /// transaction. Returns null (fall back to the hash GROUP BY) otherwise, or
    /// on any unsupported projection / type shape.
    fn tryIndexGroupAgg(self: *QueryExecutor, sel: ast.SelectStmt) !?QueryResponse {
        if (sel.joins.len != 0) return null;
        const gb = sel.group_by orelse return null;
        if (gb.len != 1 or sel.projections.len == 0) return null;
        // WHERE is allowed only when it is a plain range on the group column (the
        // index lead), so the whole predicate is captured by the scanned index
        // range and this fast path stays index-only (e.g.
        // `WHERE g BETWEEN a AND b GROUP BY g`). Any other WHERE - a different
        // column, an OR, a non-range - would need a base-row check, so fall back
        // to the hash GROUP BY over the base scan.
        if (sel.where_expr) |we| {
            if (!whereIsPlainRangeOn(we, gb[0])) return null;
        }
        if (self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) != 1) return null;

        // HAVING is allowed on this fast path only when every aggregate it
        // references is already one of the SELECT projections (so it can be read
        // from the group's `accs` with no extra accumulators). If HAVING needs a
        // HAVING-only aggregate, we would have to fold it from the index too;
        // rather than wire that here, fall back to the hash GROUP BY, which
        // already handles it. `collectHavingAggs` returns exactly those extras.
        if (sel.having_expr) |he| {
            var hv_probe = std.ArrayList(ast.AggregateCall).empty;
            defer hv_probe.deinit(self.allocator);
            try collectHavingAggs(sel, he, &hv_probe, self.allocator);
            if (hv_probe.items.len != 0) return null;
        }

        const gcol = gb[0];

        // Validate projections: only the group column bare, or aggregates whose
        // argument is `*` or a SINGLE shared value column `vcol`.
        var vcol: ?[]const u8 = null;
        for (sel.projections) |p| {
            switch (p.expr) {
                .column => |c| if (!std.mem.eql(u8, c, gcol)) return null,
                .aggregate => |agg| switch (agg.argument) {
                    .star => {},
                    .column => |c| {
                        if (agg.distinct) return null; // COUNT(DISTINCT v) not handled here
                        if (vcol) |vc| {
                            if (!std.mem.eql(u8, vc, c)) return null;
                        } else vcol = c;
                    },
                    else => return null,
                },
                else => return null,
            }
        }

        const table_meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;

        const gtype = colTypeByName(table_meta, gcol);
        if (!isFixedWidthIndexEnc(gtype)) return null; // need to decode the group value
        const vtype: ColumnType = if (vcol) |v| colTypeByName(table_meta, v) else .INT64;
        if (vcol != null and !isFixedWidthIndexEnc(vtype)) return null;

        // Find an EXACT index that leads with gcol, and (when a value column is
        // aggregated) carries it as the second key column so it can be decoded.
        const idx = for (self.db.catalog.indexes.items) |ix| {
            if (ix.table_id != table_meta.id) continue;
            if (ix.key_columns.len == 0) continue;
            if (!std.mem.eql(u8, ix.key_columns[0].name, gcol)) continue;
            if (vcol) |v| {
                if (ix.key_columns.len < 2 or !std.mem.eql(u8, ix.key_columns[1].name, v)) continue;
            }
            if (!ix.exact) continue;
            break ix;
        } else return null;

        const idx_tree = try self.db.getIndexTree(idx.name);
        defer idx_tree.deinit();

        const Group = struct { key_enc: []u8, accs: []AggAcc };
        var groups = std.ArrayList(Group).empty;
        defer {
            for (groups.items) |g| {
                self.allocator.free(g.key_enc);
                self.allocator.free(g.accs);
            }
            groups.deinit(self.allocator);
        }

        // Scan only the index range the WHERE selects (a plain range on the group
        // column, validated above); with no WHERE this is the whole index
        // (`""`..null). Either way the walk stays index-only.
        var scan_start: []const u8 = "";
        var scan_end: ?[]const u8 = null;
        var scan_bounds_owned = false;
        if (sel.where_expr) |we| {
            const rb = (try self.getRangeForCol(we, gcol, gtype)) orelse return null;
            scan_start = rb.start_key;
            scan_end = rb.end_key;
            scan_bounds_owned = true;
        }
        defer if (scan_bounds_owned) {
            self.allocator.free(scan_start);
            if (scan_end) |e| self.allocator.free(e);
        };
        var it = try idx_tree.rangeScan(scan_start, scan_end);
        defer it.deinit();
        var cur: ?*Group = null;
        while (try it.next()) |cell| {
            const key = cell.key;
            const c0 = std.mem.indexOfScalar(u8, key, ':') orelse return null;
            const genc = key[0..c0];

            // New group when the leading encoded value changes (the index is
            // ordered, so equal-g entries are contiguous).
            if (cur == null or !std.mem.eql(u8, cur.?.key_enc, genc)) {
                const accs = try self.allocator.alloc(AggAcc, sel.projections.len);
                for (accs) |*a| a.* = .{};
                try groups.append(self.allocator, .{ .key_enc = try self.allocator.dupe(u8, genc), .accs = accs });
                cur = &groups.items[groups.items.len - 1];
            }

            // Decode the value column from the second key field (only if needed).
            var vnum: ?f64 = null;
            if (vcol != null) {
                const rest = key[c0 + 1 ..];
                const c1 = std.mem.indexOfScalar(u8, rest, ':') orelse return null;
                vnum = decodeFixedEncF64(rest[0..c1], vtype);
            }

            for (sel.projections, 0..) |p, j| {
                if (p.expr != .aggregate) continue;
                const agg = p.expr.aggregate;
                const acc = &cur.?.accs[j];
                if (agg.kind == .COUNT) {
                    switch (agg.argument) {
                        .star => acc.count += 1,
                        .column => if (vnum != null) {
                            acc.count += 1;
                        },
                        else => {},
                    }
                } else if (vnum) |n| {
                    acc.saw_value = true;
                    acc.sum += n;
                    acc.num_seen += 1;
                    if (n != @trunc(n)) acc.all_int = false;
                    if (!acc.have_num) {
                        acc.min_num = n;
                        acc.max_num = n;
                        acc.have_num = true;
                    } else {
                        if (n < acc.min_num) acc.min_num = n;
                        if (n > acc.max_num) acc.max_num = n;
                    }
                }
            }
        }

        // Build output columns + a row per group.
        var columns = try self.allocator.alloc([]const u8, sel.projections.len);
        var col_types = try self.allocator.alloc(ColumnType, sel.projections.len);
        for (sel.projections, 0..) |p, i| {
            columns[i] = try self.allocator.dupe(u8, p.alias orelse switch (p.expr) {
                .column => |c| c,
                .aggregate => |agg| switch (agg.kind) {
                    .COUNT => "COUNT",
                    .SUM => "SUM",
                    .AVG => "AVG",
                    .MIN => "MIN",
                    .MAX => "MAX",
                },
                else => "?",
            });
            col_types[i] = self.projectionType(sel, p);
        }

        var rows = std.ArrayList([]const []const u8).empty;
        defer rows.deinit(self.allocator);
        for (groups.items) |g| {
            const cells = try self.allocator.alloc([]const u8, sel.projections.len);
            for (sel.projections, 0..) |p, i| {
                cells[i] = switch (p.expr) {
                    .aggregate => |agg| try self.formatAggregate(agg, g.accs[i]),
                    .column => if (decodeFixedEncF64(g.key_enc, gtype)) |gv|
                        try self.fmtNumeric(gv, switch (gtype) {
                            .FLOAT32, .FLOAT64 => false,
                            else => true,
                        })
                    else
                        try self.allocator.dupe(u8, "NULL"),
                    else => try self.allocator.dupe(u8, ""),
                };
            }
            // HAVING: drop groups that fail the predicate. Every aggregate it can
            // reference is a projection aggregate (guarded above), so the empty
            // `hv` specs suffice; `col_vals = cells` lets it read the group column.
            if (sel.having_expr) |he| {
                const grp = GroupAcc{ .col_vals = cells, .aggs = g.accs, .hv = &.{} };
                if (!evalHaving(sel, grp, &.{}, he)) {
                    for (cells) |c| self.allocator.free(c);
                    self.allocator.free(cells);
                    continue;
                }
            }
            try rows.append(self.allocator, cells);
        }

        // ORDER BY over the grouped result (keys reference output columns), then
        // OFFSET / LIMIT. Same shape as the hash GROUP BY path.
        if (sel.order_by) |ob| {
            if (rows.items.len > 1 and ob.len > 0) {
                var okeys = try self.allocator.alloc(usize, ob.len);
                defer self.allocator.free(okeys);
                var resolvable = true;
                for (ob, 0..) |k, ki| {
                    okeys[ki] = resolveOrderColIndex(columns, k.column) orelse {
                        resolvable = false;
                        break;
                    };
                }
                if (resolvable) {
                    const n = rows.items.len;
                    const order = try self.allocator.alloc(usize, n);
                    defer self.allocator.free(order);
                    for (order, 0..) |*x, i| x.* = i;
                    const SortCtx = struct {
                        rows: []const []const []const u8,
                        okeys: []const usize,
                        ob: []const ast.OrderKey,
                        fn lessThan(ctx: @This(), ia: usize, ib: usize) bool {
                            for (ctx.ob, 0..) |ok, ki| {
                                const col = ctx.okeys[ki];
                                const c = orderCompareKey(ctx.rows[ia][col], ctx.rows[ib][col]);
                                if (c != 0) return if (ok.desc) c > 0 else c < 0;
                            }
                            return false;
                        }
                    };
                    std.sort.pdq(usize, order, SortCtx{ .rows = rows.items, .okeys = okeys, .ob = ob }, SortCtx.lessThan);
                    const original = try self.allocator.dupe([]const []const u8, rows.items);
                    defer self.allocator.free(original);
                    for (order, 0..) |src, dst| rows.items[dst] = original[src];
                }
            }
        }

        if (sel.limit != null or sel.offset != null) {
            const off: usize = if (sel.offset) |o| o else 0;
            var w: usize = 0;
            for (rows.items, 0..) |row, pos| {
                var keep = true;
                if (pos < off) {
                    keep = false;
                } else if (sel.limit) |lim| {
                    if (w >= lim) keep = false;
                }
                if (keep) {
                    rows.items[w] = row;
                    w += 1;
                } else {
                    for (row) |c| self.allocator.free(c);
                    self.allocator.free(row);
                }
            }
            while (rows.items.len > w) _ = rows.pop();
        }

        self.index_group_aggs += 1;
        return QueryResponse{
            .columns = columns,
            .column_types = col_types,
            .rows = try rows.toOwnedSlice(self.allocator),
            .rows_affected = 0,
        };
    }

    /// Fast path for `SELECT COUNT(*) FROM t [WHERE <indexed col> = / range]`:
    /// answers the count by walking only the index (counting entries in the
    /// matched key range) WITHOUT fetching a single base row, returning a ready
    /// `QueryResponse`, or `null` when the query is not eligible (in which case
    /// the caller runs the normal scan-and-count).
    ///
    /// Sound because it is used only when:
    ///   - the projection is exactly `COUNT(*)` (no other columns/aggregates,
    ///     no GROUP BY / HAVING / DISTINCT / joins),
    ///   - the WHERE is either absent or a single equality/range predicate on the
    ///     leading column of some index AND references no other column (so the
    ///     index range's entries are *exactly* the matching rows - no residual),
    ///   - that index is `exact` (every entry maps 1:1 to a live row - see
    ///     [`types.IndexMetadata.exact`]; a delete/update clears this), and
    ///   - this is the only in-flight transaction, so no concurrent writer can
    ///     have added an index entry a snapshot should not count.
    /// Under those conditions `entries in range == visible rows in range`, so the
    /// entry count is the exact answer. Any condition unmet returns `null`.
    fn tryIndexOnlyCount(self: *QueryExecutor, sel: ast.SelectStmt) !?QueryResponse {
        if (sel.joins.len != 0 or sel.group_by != null or sel.having_expr != null or sel.distinct) return null;
        if (sel.projections.len != 1) return null;
        const proj = sel.projections[0];
        if (proj.expr != .aggregate) return null;
        const agg = proj.expr.aggregate;
        if (agg.kind != .COUNT or agg.argument != .star or agg.distinct) return null;

        // Only safe with no other in-flight transaction (see doc comment).
        if (self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) != 1) return null;

        const table_meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;

        // Which columns does the WHERE constrain? Must be none (count all) or
        // exactly one, and that one must be an exact index's leading column.
        var where_cols = NeededSet{};
        if (sel.where_expr) |we| {
            collectExprCols(we, &where_cols) catch return null;
            if (!where_cols.ok or where_cols.n > 1) return null;
        }

        for (self.db.catalog.indexes.items) |idx| {
            if (idx.table_id != table_meta.id or !idx.exact or idx.key_columns.len == 0) continue;
            const lead = idx.key_columns[0].name;

            // WHERE (if any) must constrain exactly this one column.
            if (where_cols.n == 1 and !std.mem.eql(u8, where_cols.buf[0], lead)) continue;

            const idx_tree = try self.db.getIndexTree(idx.name);
            defer idx_tree.deinit();
            const col_type = colTypeByName(table_meta, lead);
            var count: u64 = 0;

            if (sel.where_expr == null) {
                // COUNT(*) with no predicate: every index entry is one live row.
                var it = try idx_tree.iterator();
                defer it.deinit();
                while (try it.next()) |_| count += 1;
            } else if (try self.equalityConjunctForCol(sel.where_expr, lead)) |val| {
                defer self.allocator.free(val);
                const enc = try schema.types.encodeIndexValueAlloc(self.allocator, col_type, val);
                defer self.allocator.free(enc);
                const prefix = try std.fmt.allocPrint(self.allocator, "{s}:", .{enc});
                defer self.allocator.free(prefix);
                var it = try idx_tree.iteratorAfter(enc);
                defer it.deinit();
                while (try it.next()) |cell| {
                    if (!std.mem.startsWith(u8, cell.key, prefix)) break;
                    count += 1;
                }
            } else if (try self.getRangeForCol(sel.where_expr, lead, col_type)) |rb| {
                defer self.allocator.free(rb.start_key);
                defer if (rb.end_key) |ek| self.allocator.free(ek);
                var it = try idx_tree.rangeScan(rb.start_key, rb.end_key);
                defer it.deinit();
                while (try it.next()) |_| count += 1;
            } else {
                // WHERE references this column but not in an index-coverable form
                // (e.g. a function of it); let the normal path handle it.
                return null;
            }

            self.index_only_counts += 1;

            var columns = try self.allocator.alloc([]const u8, 1);
            columns[0] = try self.allocator.dupe(u8, proj.alias orelse "COUNT");
            var col_types = try self.allocator.alloc(ColumnType, 1);
            col_types[0] = .INT64;
            var rows = try self.allocator.alloc([]const []const u8, 1);
            const cell = try std.fmt.allocPrint(self.allocator, "{d}", .{count});
            const row = try self.allocator.alloc([]const u8, 1);
            row[0] = cell;
            rows[0] = row;
            return QueryResponse{ .columns = columns, .column_types = col_types, .rows = rows, .rows_affected = 0 };
        }
        return null;
    }

    fn buildIteratorTree(self: *QueryExecutor, sel: ast.SelectStmt) anyerror!query_iter.RowIterator {
        const current_tx = self.current_tx_id orelse return error.NoActiveTransaction;

        // Reset the scan-order hint; set only when a secondary-index equality or
        // range scan is chosen (those yield the leading column in encoded, i.e.
        // numeric, ascending order). Left null otherwise so the executor won't
        // wrongly skip a sort for a lexically-ordered table scan or an IN union.
        self.scan_ordered_col = null;
        self.scan_order_is_desc = false;

        // Compute the residual-predicate pushdown for a single-table read: the
        // WHERE and the exact set of columns it references, so the base scan can
        // reject non-matching rows on the raw image before building their JSON.
        // Only when there are no joins (a join's residual may reference the other
        // table, which the base scan cannot see) and the column set is fully
        // enumerable (collectExprCols bails on functions/CASE/subqueries).
        self.residual_expr = null;
        self.residual_cols = null;
        self.scan_offset_pushed = false;
        if (sel.joins.len == 0) {
            if (sel.where_expr) |we| {
                var ns = NeededSet{};
                collectExprCols(we, &ns) catch {
                    ns.ok = false;
                };
                if (ns.ok and ns.n > 0) {
                    @memcpy(self.residual_cols_buf[0..ns.n], ns.buf[0..ns.n]);
                    self.residual_cols = self.residual_cols_buf[0..ns.n];
                    self.residual_expr = we;
                }
            }
        }

        const base_table_meta = for (self.db.catalog.tables.items) |tbl| {
            if (std.mem.eql(u8, tbl.name, sel.table_name)) break tbl;
        } else return error.TableNotFound;

        // System catalog tables (sys.tables, sys.indexes, sys.columns) are served
        // from the in-memory catalog, not scanned from storage: their on-disk rows
        // are raw metadata blobs (or, for sys.columns, do not exist at all), not the
        // MVCC row layout the scan decoder expects. We synthesise their rows below
        // and skip all storage-scan setup; there is no B+Tree to open for them.
        const is_catalog_synth = isCatalogSynthTable(sel.table_name);
        const base_table_tree = blk: {
            if (is_catalog_synth) break :blk undefined;
            break :blk try self.db.getTableTree(sel.table_name);
        };

        var skip_index_scan = false;
        if (!is_catalog_synth) {
            if (self.getTableStats(sel.table_name)) |stats| {
                if (stats.page_count < 5) {
                    skip_index_scan = true;
                }
            }
        }

        var base_iter: query_iter.RowIterator = undefined;
        var used_index = false;

        if (is_catalog_synth) {
            const rows = try self.buildCatalogRows(base_table_meta, sel.table_name);
            const mat = try query_iter.MaterializedScanIterator.init(self.allocator, base_table_meta.name, rows);
            base_iter = mat.iterator();
            used_index = true;
        }

        var pk_col_name: ?[]const u8 = null;
        for (base_table_meta.columns) |col| {
            if (col.is_primary_key) {
                pk_col_name = col.name;
                break;
            }
        }
        if (pk_col_name) |pk_name| {
            if (try self.equalityConjunctForCol(sel.where_expr, pk_name)) |val| {
                const pk_scan = try query_iter.PrimaryKeyScanIterator.init(
                    self.allocator,
                    self,
                    base_table_meta,
                    base_table_tree,
                    val,
                    current_tx,
                );
                base_iter = pk_scan.iterator();
                used_index = true;
                self.allocator.free(val);
            }
        }

        // Composite-index PREFIX ordered scan: `WHERE lead = const ORDER BY
        // second_col [DESC]` on a composite index `(lead, second_col)`. Within the
        // `lead = const` prefix the index is already ordered by `second_col`, so a
        // forward (ASC) or backward (DESC) scan of that prefix yields the ORDER BY
        // order directly - no sort, and the streaming LIMIT stops early. The
        // residual WHERE (the full predicate, incl. any range on second_col) still
        // filters each row. This is what lets `emp=279 AND td>10000 ORDER BY td
        // DESC LIMIT k` cost O(k) instead of scanning + sorting the whole prefix.
        if (!used_index and !skip_index_scan and sel.joins.len == 0) {
            if (sel.order_by) |ob| if (ob.len == 1) {
                for (self.db.catalog.indexes.items) |idx| {
                    if (idx.table_id != base_table_meta.id or idx.key_columns.len < 2) continue;
                    if (!std.mem.eql(u8, idx.key_columns[1].name, ob[0].column)) continue;
                    const lead = idx.key_columns[0].name;
                    const lead_val = (try self.equalityConjunctForCol(sel.where_expr, lead)) orelse continue;
                    defer self.allocator.free(lead_val);

                    const enc = try schema.types.encodeIndexValueAlloc(self.allocator, colTypeByName(base_table_meta, lead), lead_val);
                    defer self.allocator.free(enc);

                    // Tighten the prefix seek with any range predicate on the
                    // ORDER BY (second) column. Without this the scan covers the
                    // whole `lead = const` prefix `[enc:, enc;)` and leans on the
                    // residual to drop out-of-range rows - fine for a DESC scan
                    // whose top rows all qualify, but pathological for e.g.
                    // `lead=const AND second > X ORDER BY second ASC`: it fetches
                    // and discards every `second <= X` row (207k of them for
                    // employee 279) before reaching the first result. Encoding the
                    // bound into the key (keys are `enc(lead):enc(second):pk`) makes
                    // the scan start at `(lead, X)` / stop at `(lead, hi)` instead.
                    // The residual still runs, so boundary exactness is unchanged.
                    const second_col = idx.key_columns[1].name;
                    const rb = try self.getRangeForCol(sel.where_expr, second_col, colTypeByName(base_table_meta, second_col));
                    defer if (rb) |r| {
                        self.allocator.free(r.start_key);
                        if (r.end_key) |ek| self.allocator.free(ek);
                    };
                    const start_key = if (rb != null and rb.?.start_key.len > 0)
                        try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ enc, rb.?.start_key })
                    else
                        try std.fmt.allocPrint(self.allocator, "{s}:", .{enc});
                    defer self.allocator.free(start_key);
                    const end_key = if (rb != null and rb.?.end_key != null)
                        try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ enc, rb.?.end_key.? })
                    else
                        try std.fmt.allocPrint(self.allocator, "{s};", .{enc});
                    defer self.allocator.free(end_key);

                    const idx_tree = try self.db.getIndexTree(idx.name);
                    if (ob[0].desc) {
                        const desc_scan = try query_iter.IndexRangeScanDescIterator.init(
                            self.allocator,
                            self,
                            base_table_meta,
                            base_table_tree,
                            idx_tree,
                            start_key,
                            end_key,
                            current_tx,
                        );
                        desc_scan.residual = self.residual_expr;
                        desc_scan.residual_cols = self.residual_cols;
                        desc_scan.pk_after_colon = 2;
                        base_iter = desc_scan.iterator();
                        self.scan_order_is_desc = true;
                    } else {
                        const asc_scan = try query_iter.IndexRangeScanIterator.init(
                            self.allocator,
                            self,
                            base_table_meta,
                            base_table_tree,
                            idx_tree,
                            start_key,
                            end_key,
                            current_tx,
                        );
                        asc_scan.residual = self.residual_expr;
                        asc_scan.residual_cols = self.residual_cols;
                        asc_scan.pk_after_colon = 2;
                        // Push an OFFSET into the scan: this scan yields the final
                        // ORDER BY order, so the leading `offset` qualifying rows can
                        // be counted past (rowQualifies, no materialise) instead of
                        // being built, buffered and discarded by the executor.
                        if (sel.offset) |off| if (off > 0) {
                            asc_scan.skip_remaining = off;
                            self.scan_offset_pushed = true;
                            // Fetch-free skip when the key range captures the whole
                            // WHERE and a single txn is in flight (every in-range
                            // entry is a visible, qualifying row). Otherwise the skip
                            // verifies each row with rowQualifies.
                            const single_txn = self.db.txn_manager.activeTxnCount(self.db.pool.pager.io) == 1;
                            if (single_txn) {
                                if (sel.where_expr) |we| {
                                    if (self.whereCapturedByCompositeKey(we, lead, second_col)) asc_scan.skip_no_fetch = true;
                                } else asc_scan.skip_no_fetch = true;
                            }
                        };
                        base_iter = asc_scan.iterator();
                    }
                    used_index = true;
                    self.scan_ordered_col = idx.key_columns[1].name;
                    self.composite_ordered_scans += 1;
                    break;
                }
            };
        }

        if (!used_index and !skip_index_scan) {
            for (self.db.catalog.indexes.items) |idx| {
                if (idx.table_id == base_table_meta.id and idx.key_columns.len > 0) {
                    if (try self.equalityConjunctForCol(sel.where_expr, idx.key_columns[0].name)) |val| {
                        defer self.allocator.free(val);
                        const idx_tree = try self.db.getIndexTree(idx.name);

                        // The stored index keys encode the leading column value in
                        // order-preserving form, so the equality prefix must be
                        // encoded the same way (see schema.types.encodeIndexValueAlloc).
                        // The column type comes from the table (index key_columns do
                        // not reliably carry it), matching every write site.
                        const enc = try schema.types.encodeIndexValueAlloc(self.allocator, colTypeByName(base_table_meta, idx.key_columns[0].name), val);
                        defer self.allocator.free(enc);

                        const index_scan = try query_iter.IndexScanIterator.init(
                            self.allocator,
                            self,
                            base_table_meta,
                            base_table_tree,
                            idx_tree,
                            enc,
                            current_tx,
                        );
                        index_scan.residual = self.residual_expr;
                        index_scan.residual_cols = self.residual_cols;
                        base_iter = index_scan.iterator();
                        used_index = true;
                        self.scan_ordered_col = idx.key_columns[0].name;
                        break;
                    }
                }
            }
        }

        // No equality index applied: try a bounded index range scan when the
        // WHERE has a single-column range predicate (BETWEEN / < / > / a
        // conjunction of them) on an indexed column. This is the SQL analogue of
        // the document surface's index range scan; the executor's streaming LIMIT
        // break then makes an unordered `LIMIT k` cost O(k) instead of a full scan.
        if (!used_index and !skip_index_scan) {
            for (self.db.catalog.indexes.items) |idx| {
                if (idx.table_id == base_table_meta.id and idx.key_columns.len > 0) {
                    if (try self.getRangeForCol(sel.where_expr, idx.key_columns[0].name, colTypeByName(base_table_meta, idx.key_columns[0].name))) |rb| {
                        defer self.allocator.free(rb.start_key);
                        defer if (rb.end_key) |ek| self.allocator.free(ek);
                        const idx_tree = try self.db.getIndexTree(idx.name);

                        // Selectivity-aware plan choice: for an UNORDERED, UNLIMITED
                        // read (the aggregate / full-materialise shape), when the
                        // range matches a large fraction of the table the index +
                        // clustered base fetch reads most base leaves ANYWAY, plus
                        // the index walk and a per-PK descent. A full clustered scan
                        // reads those leaves once, sequentially, with none of that
                        // overhead - the switch PostgreSQL makes above ~a quarter
                        // selectivity. Skip the index and fall through to the full
                        // table scan (the always-on FilterIterator re-applies the
                        // WHERE, so the result is identical). Ordered/limited queries
                        // keep the index (it supplies order / an early LIMIT break),
                        // and a non-plain range keeps it too.
                        if (sel.order_by == null and sel.limit == null and
                            whereIsPlainRangeOn(sel.where_expr.?, idx.key_columns[0].name))
                        {
                            const est = try estimateRangeFraction(sel.where_expr.?, idx.key_columns[0].name, colTypeByName(base_table_meta, idx.key_columns[0].name), idx_tree);
                            if (est) |frac| if (frac > 0.30) {
                                idx_tree.deinit();
                                continue;
                            };
                        }

                        // A single-key `ORDER BY <this column> DESC` is served by a
                        // BACKWARD range scan: the top of the range is emitted first,
                        // so the streaming LIMIT break stops after k rows instead of
                        // scanning the whole range, materialising, and reversing.
                        const desc_order = sel.joins.len == 0 and if (sel.order_by) |ob|
                            (ob.len == 1 and ob[0].desc and std.mem.eql(u8, ob[0].column, idx.key_columns[0].name))
                        else
                            false;

                        if (desc_order) {
                            const desc_scan = try query_iter.IndexRangeScanDescIterator.init(
                                self.allocator,
                                self,
                                base_table_meta,
                                base_table_tree,
                                idx_tree,
                                rb.start_key,
                                rb.end_key,
                                current_tx,
                            );
                            desc_scan.residual = self.residual_expr;
                            desc_scan.residual_cols = self.residual_cols;
                            base_iter = desc_scan.iterator();
                            self.scan_order_is_desc = true;
                            self.desc_range_scans_built += 1;
                        } else {
                            const range_scan = try query_iter.IndexRangeScanIterator.init(
                                self.allocator,
                                self,
                                base_table_meta,
                                base_table_tree,
                                idx_tree,
                                rb.start_key,
                                rb.end_key,
                                current_tx,
                            );
                            range_scan.residual = self.residual_expr;
                            range_scan.residual_cols = self.residual_cols;
                            // This scan's ascending value order is relied upon only
                            // when it directly satisfies `ORDER BY <this col> ASC`
                            // (single key, ascending). In every other case (no
                            // ORDER BY, or one on another column that the executor
                            // sorts downstream) the row order is free, so each
                            // look-ahead batch may be reordered into primary-key
                            // order for a near-sequential base-row fetch.
                            const served_asc = sel.joins.len == 0 and if (sel.order_by) |ob|
                                (ob.len == 1 and !ob[0].desc and std.mem.eql(u8, ob[0].column, idx.key_columns[0].name))
                            else
                                false;
                            range_scan.may_reorder = !served_asc;
                            // MRR-style sort window: when the row order is free
                            // (`may_reorder`), the iterator sorts each PK batch into
                            // primary-key order so consecutive base-row fetches land on
                            // the same clustered leaf and the leaf-reuse cursor hits.
                            // That only pays off if the window is large enough for
                            // sorted PKs to actually share leaves, so size it from the
                            // LIMIT: an unbounded scan (aggregate / wide read) sorts a
                            // large window; a tight LIMIT keeps the small default so it
                            // never buffers/sorts far more PKs than it returns. Measured
                            // on a disk-bound VM this took `avg(col) WHERE idxcol
                            // BETWEEN ...` (secondary-index base fetch) from ~4.3 s toward
                            // InnoDB's range, the clustered-engine reference.
                            if (range_scan.may_reorder) {
                                const MRR_WINDOW: usize = 262144;
                                if (sel.offset == null) {
                                    if (sel.limit) |lim| {
                                        const want = @as(usize, lim) + 64;
                                        if (want > range_scan.prefetch_batch) range_scan.prefetch_batch = @min(want, MRR_WINDOW);
                                    } else {
                                        range_scan.prefetch_batch = MRR_WINDOW;
                                    }
                                }
                            }
                            // Push an OFFSET into the scan when it directly yields the
                            // final ORDER BY order (served_asc == this scan's ascending
                            // value order IS the requested order): the leading `offset`
                            // rows are counted past with a per-row visibility+residual
                            // check (rowQualifies, no JSON build/materialise) instead of
                            // being built, buffered and discarded downstream. Mirrors the
                            // composite ordered-scan site.
                            //
                            // The fetch-free variant (skip_no_fetch) is deliberately NOT
                            // enabled here: unlike the composite site, this single-column
                            // range can over-select (a residual like `col <> k` or `OR`
                            // still on the same column is not expressed by the range
                            // bounds), and `residual_expr` is always the full WHERE, so a
                            // fetch-free skip would need a single-column analogue of
                            // `whereCapturedByCompositeKey` to prove every in-range entry
                            // qualifies. Until that check exists, the skip stays
                            // verified-per-row (correct, avoids materialisation) rather
                            // than fetch-free. No query in the Q1..Q18 suite exercises the
                            // fetch-free single-column deep-OFFSET shape (Q18 is composite).
                            if (served_asc) {
                                if (sel.offset) |off| if (off > 0) {
                                    range_scan.skip_remaining = off;
                                    self.scan_offset_pushed = true;
                                };
                            }
                            base_iter = range_scan.iterator();
                        }
                        used_index = true;
                        self.range_scans_built += 1;
                        self.scan_ordered_col = idx.key_columns[0].name;
                        break;
                    }
                }
            }
        }

        // No equality/range index applied: try an index UNION for `col IN (...)`
        // on an indexed column (one equality seek per value) instead of a full
        // table scan. The residual WHERE FILTER wraps it, so overlapping values or
        // extra predicates stay correct; the streaming LIMIT break stops early.
        if (!used_index and !skip_index_scan) {
            for (self.db.catalog.indexes.items) |idx| {
                if (idx.table_id == base_table_meta.id and idx.key_columns.len > 0) {
                    const ct = colTypeByName(base_table_meta, idx.key_columns[0].name);
                    // `col IN (...)` OR a pure `col = a OR col = b [OR ...]` tree on
                    // this indexed column: both become an index union (one seek per
                    // value) instead of a full scan; the residual FILTER still gates.
                    const in_vals = (try self.getInListForCol(sel.where_expr, idx.key_columns[0].name, ct)) orelse
                        (try self.getOrEqualsForCol(sel.where_expr, idx.key_columns[0].name, ct));
                    if (in_vals) |vals| {
                        defer {
                            for (vals) |v| self.allocator.free(v);
                            self.allocator.free(vals);
                        }
                        const idx_tree = try self.db.getIndexTree(idx.name);
                        const in_scan = try query_iter.IndexInScanIterator.init(
                            self.allocator,
                            self,
                            base_table_meta,
                            base_table_tree,
                            idx_tree,
                            vals,
                            current_tx,
                        );
                        in_scan.residual = self.residual_expr;
                        in_scan.residual_cols = self.residual_cols;
                        base_iter = in_scan.iterator();
                        used_index = true;
                        self.index_in_unions_built += 1;
                        break;
                    }
                }
            }
        }

        // Ordering-only index access: no WHERE predicate selected an index, but a
        // single-key `ORDER BY <indexed col>` can be served by a FULL scan of that
        // index in the ORDER BY direction. The index yields rows already ordered by
        // the column (order-preserving keys), so downstream sees the scan as the
        // final order (`scan_ordered_col` + `scan_order_is_desc`), skips the sort,
        // and the streaming LIMIT break serves `ORDER BY col [DESC] LIMIT k` in O(k)
        // base fetches instead of a full table scan plus a sort of every row. Only
        // with no joins and a single ORDER key on this table's indexed column.
        if (!used_index and !skip_index_scan and sel.joins.len == 0) {
            if (sel.order_by) |ob| if (ob.len == 1) {
                for (self.db.catalog.indexes.items) |idx| {
                    if (idx.table_id == base_table_meta.id and idx.key_columns.len > 0 and
                        std.mem.eql(u8, ob[0].column, idx.key_columns[0].name))
                    {
                        const idx_tree = try self.db.getIndexTree(idx.name);
                        if (ob[0].desc) {
                            const desc_scan = try query_iter.IndexRangeScanDescIterator.init(
                                self.allocator, self, base_table_meta, base_table_tree, idx_tree, null, null, current_tx);
                            desc_scan.residual = self.residual_expr;
                            desc_scan.residual_cols = self.residual_cols;
                            base_iter = desc_scan.iterator();
                            self.scan_order_is_desc = true;
                            self.desc_range_scans_built += 1;
                        } else {
                            // Unbounded ascending scan: "" is the lowest key, so the
                            // cursor starts at the first index entry; no end bound.
                            const asc_scan = try query_iter.IndexRangeScanIterator.init(
                                self.allocator, self, base_table_meta, base_table_tree, idx_tree, "", null, current_tx);
                            asc_scan.residual = self.residual_expr;
                            asc_scan.residual_cols = self.residual_cols;
                            // The ascending index order IS the requested final order, so
                            // it must NOT be reordered for the clustered-fetch heuristic.
                            asc_scan.may_reorder = false;
                            if (sel.offset) |off| if (off > 0) {
                                asc_scan.skip_remaining = off;
                                self.scan_offset_pushed = true;
                            };
                            base_iter = asc_scan.iterator();
                        }
                        used_index = true;
                        self.range_scans_built += 1;
                        self.scan_ordered_col = idx.key_columns[0].name;
                        break;
                    }
                }
            };
        }

        if (!used_index) {
            var start_key: ?[]const u8 = null;
            var stop_key: ?[]const u8 = null;
            if (pk_col_name) |pk_name| {
                const pk_range = try self.pkClusteredWindow(sel.where_expr, pk_name);
                start_key = pk_range.start;
                stop_key = pk_range.stop;
            }
            defer if (start_key) |val| self.allocator.free(val);
            defer if (stop_key) |val| self.allocator.free(val);
            const table_scan = try query_iter.TableScanIterator.init(
                self.allocator,
                self,
                base_table_meta,
                base_table_tree,
                current_tx,
                start_key,
                stop_key,
            );
            base_iter = table_scan.iterator();
        }

        var current_iter = base_iter;
        errdefer current_iter.deinit();

        for (sel.joins) |join| {
            const right_table_meta = for (self.db.catalog.tables.items) |tbl| {
                if (std.mem.eql(u8, tbl.name, join.right_table)) break tbl;
            } else return error.TableNotFound;

            const right_table_tree = try self.db.getTableTree(join.right_table);

            const right_scan = try query_iter.TableScanIterator.init(
                self.allocator,
                self,
                right_table_meta,
                right_table_tree,
                current_tx,
                null,
                null,
            );
            const right_iter = right_scan.iterator();

            var use_hash_join = false;
            var left_key: []const u8 = undefined;
            var right_key: []const u8 = undefined;

            if (getJoinKeys(join.on_expr)) |keys| {
                if (self.getTableStats(join.right_table)) |right_stats| {
                    const left_rows: u64 = if (self.getTableStats(sel.table_name)) |s| s.row_count else right_stats.row_count;
                    const right_rows: u64 = right_stats.row_count;
                    if (right_rows < 100 and CostModel.preferHashJoin(left_rows, right_rows)) {
                    use_hash_join = true;
                    if (std.mem.startsWith(u8, keys.left, join.right_table) or
                        (std.mem.indexOfScalar(u8, keys.left, '.') == null and hasColumn(right_table_meta, keys.left)))
                    {
                        left_key = keys.right;
                        right_key = keys.left;
                    } else {
                        left_key = keys.left;
                        right_key = keys.right;
                    }
                    }
                }
            }

            if (use_hash_join) {
                const join_node = try query_iter.HashJoinIterator.init(
                    self.allocator,
                    current_iter,
                    right_iter,
                    join.right_table,
                    left_key,
                    right_key,
                    join.join_type,
                );
                current_iter = join_node.iterator();
            } else {
                const join_node = try query_iter.NestedLoopJoinIterator.init(
                    self.allocator,
                    current_iter,
                    right_iter,
                    join.right_table,
                    join.on_expr,
                    join.join_type,
                );
                current_iter = join_node.iterator();
            }
        }

        if (sel.where_expr) |we| {
            const filter_node = try query_iter.FilterIterator.init(
                self.allocator,
                current_iter,
                we,
            );
            current_iter = filter_node.iterator();
        }

        return current_iter;
    }

    /// The public entry point: runs one request and returns a response.
    ///
    /// This is the outermost wrapper that sets up cross-cutting concerns before
    /// delegating to [`executeWrapped`]:
    /// - installs a [`MemoryLimitAllocator`] as `self.allocator` for the call
    ///   when a memory cap is set (request cap wins over session cap), restored
    ///   on return;
    /// - computes an absolute `deadline_ms` from the timeout;
    /// - substitutes `$N` bound parameters into the SQL text (a bind failure is
    ///   returned as an error response, not a Zig error);
    /// - catches any Zig error from execution and converts it to an
    ///   `error_message` response, mapping `OutOfMemory`/`QueryTimeoutExceeded`
    ///   to the user-facing limit messages.
    /// It therefore (almost) never propagates a Zig error to the caller: logical
    /// failures come back inside [`QueryResponse.error_message`].
    pub fn execute(self: *QueryExecutor, req: QueryRequest) !QueryResponse {
        // Point the scalar-eval wasm-UDF hook at this database's registry (embed-wasm.md M1).
        // The registry is a stable field of the heap-allocated Database, so the pointer is
        // valid for the query; setting it every execute is a cheap idempotent store.
        query_iter.active_wasm_registry = &self.db.wasm_functions;

        // Record end-to-end latency of every top-level query into the shared
        // histogram, on all return/error paths. Monotonic clock; nanoseconds.
        const lat_io = self.db.pool.pager.io;
        const lat_start: i128 = Io.Clock.now(.awake, lat_io).toNanoseconds();
        defer {
            const elapsed = Io.Clock.now(.awake, lat_io).toNanoseconds() - lat_start;
            if (elapsed >= 0) self.db.query_latency.observeNs(@intCast(elapsed));
        }

        var limit_allocator_state = if (req.memory_limit_bytes) |limit|
            MemoryLimitAllocator{ .parent_allocator = self.allocator, .limit_bytes = limit }
        else if (self.memory_limit_bytes) |limit|
            MemoryLimitAllocator{ .parent_allocator = self.allocator, .limit_bytes = limit }
        else
            null;
        
        const orig_allocator = self.allocator;
        if (limit_allocator_state) |*state| {
            self.allocator = state.allocator();
        }
        defer self.allocator = orig_allocator;

        const timeout = req.timeout_ms orelse self.timeout_ms;
        if (timeout) |t| {
            const start = Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds();
            self.deadline_ms = start + @as(i64, @intCast(t));
        } else {
            self.deadline_ms = null;
        }
        defer self.deadline_ms = null;

        var req2 = req;
        var substituted: ?[]u8 = null;
        defer if (substituted) |s| orig_allocator.free(s);
        if (req.params.len > 0) {
            substituted = command_mod.substituteParams(orig_allocator, req.sql, req.params, req.param_classes) catch |e| {
                const msg = switch (e) {
                    error.InvalidNumericParam => "Bind failed: a $N numeric parameter is not a valid numeric literal",
                    else => "Bind failed: could not substitute parameters",
                };
                return QueryResponse{ .error_message = try orig_allocator.dupe(u8, msg) };
            };
            req2.sql = substituted.?;
            req2.params = &.{};
        }

        return self.executeWrapped(req2) catch |err| {
            const err_msg = switch (err) {
                error.OutOfMemory => try orig_allocator.dupe(u8, "Query Memory Limit Exceeded"),
                error.QueryTimeoutExceeded => try orig_allocator.dupe(u8, "Query Timeout Exceeded"),
                else => try std.fmt.allocPrint(orig_allocator, "Execution Error: {any}", .{err}),
            };
            return QueryResponse{ .error_message = err_msg };
        };
    }

    /// Parses, authorises and runs a single statement, wrapping it in an
    /// implicit transaction when needed.
    ///
    /// Called by [`execute`] after limits/params are set up. Its responsibilities
    /// in order:
    /// 1. Handle the textual meta-commands that are not real SQL statements
    ///    (`SET FENCE EPOCH`, `SET DURABLE COMMIT`, `SET TRANSACTION ISOLATION
    ///    LEVEL`), gating the first two behind [`adminGate`].
    /// 2. Parse the SQL, using and populating the database's shared statement
    ///    cache (bounded at `max_cached_statements`); a parse failure becomes an
    ///    error response. Ownership of the parser/SQL is handed to the cache, or
    ///    kept local and freed on the way out when the cache is full.
    /// 3. Enforce per-object/per-action permissions when security is enabled
    ///    (login is exempt).
    /// 4. Guard writes against a stale fencing epoch (`guardWrite`).
    /// 5. If the client is in an explicit transaction, just run the statement;
    ///    otherwise open an implicit transaction, run it, and commit
    ///    (WAL begin/commit) or roll back on error, including durable-commit
    ///    finalisation.
    /// Transaction-boundary statements (BEGIN/COMMIT/...) bypass the autocommit
    /// wrapper and go straight to [`executeStatement`].
    fn executeWrapped(self: *QueryExecutor, req: QueryRequest) anyerror!QueryResponse {
        if (std.mem.startsWith(u8, req.sql, "SET FENCE EPOCH ")) {
            if (try self.adminGate(req)) |deny| return deny;
            const rest = std.mem.trim(u8, req.sql["SET FENCE EPOCH ".len..], " \t\r\n;");
            const epoch = std.fmt.parseUnsigned(u64, rest, 10) catch {
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "SET FENCE EPOCH: invalid epoch") };
            };
            try self.db.setWriteEpoch(epoch);
            return QueryResponse{ .rows_affected = 0 };
        }

        // Runtime role transition (admin-gated), mechanism for orchestrated failover.
        // `PROMOTE` makes this node a writable primary at a fresh, strictly-greater
        // fence epoch (split-brain-safe; see Database.promote). Idempotent-ish:
        // re-promoting just bumps the epoch.
        if (std.ascii.eqlIgnoreCase(std.mem.trim(u8, req.sql, " \t\r\n;"), "PROMOTE")) {
            if (try self.adminGate(req)) |deny| return deny;
            const new_epoch = self.db.promote() catch |err| {
                return QueryResponse{ .error_message = try std.fmt.allocPrint(self.allocator, "PROMOTE failed: {s}", .{@errorName(err)}) };
            };
            var cols = try self.allocator.alloc([]const u8, 1);
            cols[0] = try self.allocator.dupe(u8, "epoch");
            var row_cells = try self.allocator.alloc([]const u8, 1);
            row_cells[0] = try std.fmt.allocPrint(self.allocator, "{d}", .{new_epoch});
            var rows = try self.allocator.alloc([]const []const u8, 1);
            rows[0] = row_cells;
            return QueryResponse{ .columns = cols, .rows = rows, .rows_affected = 0 };
        }

        // `DEMOTE TO FOLLOWER ON 'host:port'` fences local writes, stops shipping,
        // and starts following on the given LISTEN address (push-model: the new
        // primary dials this follower). v1 uses no replication TLS/auth on the SQL
        // path; a TLS-secured replication link is configured the file-config way.
        if (std.ascii.startsWithIgnoreCase(req.sql, "DEMOTE TO FOLLOWER ON ")) {
            if (try self.adminGate(req)) |deny| return deny;
            const rest = std.mem.trim(u8, req.sql["DEMOTE TO FOLLOWER ON ".len..], " \t\r\n;'\"");
            const colon = std.mem.lastIndexOfScalar(u8, rest, ':') orelse
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "DEMOTE: expected 'host:port'") };
            const host = rest[0..colon];
            const port = std.fmt.parseUnsigned(u16, rest[colon + 1 ..], 10) catch
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "DEMOTE: invalid port") };
            if (host.len == 0) return QueryResponse{ .error_message = try self.allocator.dupe(u8, "DEMOTE: empty host") };
            self.db.demote(host, port, "", .{}) catch |err| {
                return QueryResponse{ .error_message = try std.fmt.allocPrint(self.allocator, "DEMOTE failed: {s}", .{@errorName(err)}) };
            };
            return QueryResponse{ .rows_affected = 0 };
        }

        if (std.mem.startsWith(u8, req.sql, "SET DURABLE COMMIT ")) {
            if (try self.adminGate(req)) |deny| return deny;
            const rest = std.mem.trim(u8, req.sql["SET DURABLE COMMIT ".len..], " \t\r\n;");
            if (std.ascii.eqlIgnoreCase(rest, "on") or std.ascii.eqlIgnoreCase(rest, "true")) {
                self.durable_commit = true;
            } else if (std.ascii.eqlIgnoreCase(rest, "off") or std.ascii.eqlIgnoreCase(rest, "false")) {
                self.durable_commit = false;
            } else {
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "SET DURABLE COMMIT: expected ON or OFF") };
            }
            return QueryResponse{ .rows_affected = 0 };
        }

        {
            const up = std.ascii.allocUpperString(self.allocator, std.mem.trim(u8, req.sql, " \t\r\n;")) catch null;
            if (up) |u| {
                defer self.allocator.free(u);
                if (std.mem.startsWith(u8, u, "SET TRANSACTION ISOLATION LEVEL")) {
                    const rest = std.mem.trim(u8, u["SET TRANSACTION ISOLATION LEVEL".len..], " \t");
                    if (std.mem.eql(u8, rest, "READ COMMITTED")) {
                        self.isolation_level = .read_committed;
                    } else if (std.mem.eql(u8, rest, "REPEATABLE READ") or std.mem.eql(u8, rest, "SNAPSHOT")) {
                        self.isolation_level = .repeatable_read;
                    } else if (std.mem.eql(u8, rest, "READ UNCOMMITTED")) {
                        self.isolation_level = .read_committed;
                    } else if (std.mem.eql(u8, rest, "SERIALIZABLE")) {
                        self.isolation_level = .serializable;
                    } else {
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "SET TRANSACTION ISOLATION LEVEL: expected READ COMMITTED | REPEATABLE READ | SNAPSHOT") };
                    }
                    return QueryResponse{ .rows_affected = 0 };
                }
            }
        }

        const max_cached_statements: usize = 10000;
        var owned_parser: ?*Parser = null;
        var owned_sql: ?[]const u8 = null;
        defer {
            if (owned_parser) |p| {
                p.deinit();
                self.db.allocator.destroy(p);
            }
            if (owned_sql) |s| self.db.allocator.free(s);
        }

        var cached_stmt: ?ast.Statement = null;
        self.db.query_cache_mutex.lockUncancelable(self.db.pool.pager.io);
        if (self.db.query_cache.get(req.sql)) |cached| {
            cached_stmt = cached.stmt;
        }
        self.db.query_cache_mutex.unlock(self.db.pool.pager.io);

        const stmt = if (cached_stmt) |s| s else stmt_blk: {
            const sql_copy = try self.db.allocator.dupe(u8, req.sql);

            const prs = self.db.allocator.create(Parser) catch |err| {
                self.db.allocator.free(sql_copy);
                return err;
            };

            prs.* = Parser.init(self.db.allocator, sql_copy) catch |err| {
                self.db.allocator.destroy(prs);
                self.db.allocator.free(sql_copy);
                if (err == error.OutOfMemory or err == error.QueryTimeoutExceeded) return err;
                var buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&buf, "SQL Parser Init Error: {any}", .{err});
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, msg) };
            };

            const parsed = prs.parseStatement() catch |err| {
                prs.deinit();
                self.db.allocator.destroy(prs);
                self.db.allocator.free(sql_copy);
                if (err == error.OutOfMemory or err == error.QueryTimeoutExceeded) return err;
                var buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&buf, "SQL Parser Error: {any}", .{err});
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, msg) };
            };

            self.db.query_cache_mutex.lockUncancelable(self.db.pool.pager.io);
            defer self.db.query_cache_mutex.unlock(self.db.pool.pager.io);

            if (self.db.query_cache.count() < max_cached_statements) {
                self.db.query_cache.put(sql_copy, .{ .parser = prs, .stmt = parsed }) catch {
                    owned_parser = prs;
                    owned_sql = sql_copy;
                    break :stmt_blk parsed;
                };
                break :stmt_blk parsed;
            } else {
                owned_parser = prs;
                owned_sql = sql_copy;
                break :stmt_blk parsed;
            }
        };

        if (self.db.security_manager.enabled) {
            const is_login = switch (stmt) {
                .login => true,
                else => false,
            };
            if (!is_login and self.db.security_manager.users.count() > 0) {
                if (req.session_token) |tok_hex| {
                    const token = @import("../concurrency/security.zig").parseTokenHex(tok_hex) catch {
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Invalid session token format") };
                    };
                    const session = self.db.security_manager.validateSession(token) catch |err| {
                        return QueryResponse{ .error_message = try std.fmt.allocPrint(self.allocator, "Authentication Error: {any}", .{err}) };
                    };

                    switch (stmt) {
                        .select => |sel| {
                            self.db.security_manager.checkObjectPermission(&session, sel.table_name, "SELECT") catch {
                                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Permission Denied") };
                            };
                        },
                        .insert => |ins| {
                            self.db.security_manager.checkObjectPermission(&session, ins.table_name, "INSERT") catch {
                                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Permission Denied") };
                            };
                        },
                        .update => |upd| {
                            self.db.security_manager.checkObjectPermission(&session, upd.table_name, "UPDATE") catch {
                                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Permission Denied") };
                            };
                        },
                        .delete => |del| {
                            self.db.security_manager.checkObjectPermission(&session, del.table_name, "DELETE") catch {
                                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Permission Denied") };
                            };
                        },
                        else => {
                            const perm_type: @import("../concurrency/security.zig").PermissionType = switch (stmt) {
                                .export_stmt => .read,
                                .import_stmt => .write,
                                else => .admin,
                            };
                            self.db.security_manager.checkPermission(&session, perm_type) catch {
                                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Permission Denied") };
                            };
                        },
                    }
                } else {
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Authentication Required: missing session token") };
                }
            }
        }

        const is_txn_boundary = switch (stmt) {
            .begin, .commit, .rollback, .savepoint, .release_savepoint => true,
            else => false,
        };

        if (is_txn_boundary) {
            return self.executeStatement(stmt) catch |err| {
                if (err == error.OutOfMemory or err == error.QueryTimeoutExceeded) return err;
                var buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&buf, "Execution Error: {any}", .{err});
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, msg) };
            };
        }

        if (isWriteStmt(stmt)) {
            self.db.guardWrite() catch {
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Fenced: write rejected (stale fencing epoch)") };
            };
        }

        if (self.current_tx_id == null) {
            const tx = try self.db.txn_manager.begin(self.db.pool.pager.io);
            self.current_tx_id = tx;
            try self.logWalRecord(.begin, "", "", "");

            const res = self.executeStatement(stmt) catch |err| {
                if (err == error.OutOfMemory or err == error.QueryTimeoutExceeded) return err;
                try self.logWalRecord(.rollback, "", "", "");
                try self.db.txn_manager.abort(self.db.pool.pager.io, tx);
                self.durableFinish(false) catch {};
                self.current_tx_id = null;
                var buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&buf, "Execution Error: {any}", .{err});
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, msg) };
            };

            if (res.error_message != null) {
                try self.logWalRecord(.rollback, "", "", "");
                try self.db.txn_manager.abort(self.db.pool.pager.io, tx);
                self.durableFinish(false) catch {};
            } else {
                self.logWalRecord(.commit, "", "", "") catch {
                    self.db.txn_manager.abort(self.db.pool.pager.io, tx) catch {};
                    self.current_tx_id = null;
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Commit failed: could not write the WAL commit record; transaction aborted") };
                };
                try self.db.txn_manager.commit(self.db.pool.pager.io, tx);
                self.durableFinish(true) catch {
                    self.current_tx_id = null;
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Durable commit failed: replication quorum not reached within timeout") };
                };
            }
            self.current_tx_id = null;
            return res;
        } else {
            return self.executeStatement(stmt) catch |err| {
                if (err == error.OutOfMemory or err == error.QueryTimeoutExceeded) return err;
                var buf: [256]u8 = undefined;
                const msg = try std.fmt.bufPrint(&buf, "Execution Error: {any}", .{err});
                return QueryResponse{ .error_message = try self.allocator.dupe(u8, msg) };
            };
        }
    }

    /// Acquires the correct locks for a statement, then dispatches to
    /// [`executeStatementInternal`].
    ///
    /// This implements the locking protocol described in the file header. When
    /// the statement touches exactly one user table and there are no foreign
    /// keys defined (so no cascading table access), it takes the db `rw_lock`
    /// shared and the per-table [`GroupLock`] at the granularity the operation
    /// needs: read for SELECT, write for INSERT (concurrent inserters allowed),
    /// exclusive for UPDATE/DELETE (they scan). If the per-table lock cannot be
    /// obtained it falls back to the coarse exclusive db lock. Multi-table reads
    /// take the db lock shared; multi-table writes and DDL take it exclusive.
    fn executeStatement(self: *QueryExecutor, stmt: ast.Statement) !QueryResponse {
        const io = self.db.pool.pager.io;
        const is_read = switch (stmt) {
            .select, .export_stmt, .backup => true,
            else => false,
        };

        const single_table: ?[]const u8 = if (self.db.catalog.foreign_keys.items.len == 0)
            switch (stmt) {
                .select => |s| if (s.joins.len == 0) s.table_name else null,
                .insert => |s| s.table_name,
                .update => |s| s.table_name,
                .delete => |s| s.table_name,
                else => null,
            }
        else
            null;

        if (single_table) |tname| {
            const tl = self.db.tableLock(tname) catch {
                self.db.rw_lock.lock(io);
                defer self.db.rw_lock.unlock(io);
                return try self.executeStatementInternal(stmt);
            };
            self.db.rw_lock.lockShared(io);
            defer self.db.rw_lock.unlockShared(io);
            switch (stmt) {
                .insert => {
                    tl.lockWrite(io);
                    defer tl.unlockWrite(io);
                    return try self.executeStatementInternal(stmt);
                },
                .update, .delete => {
                    tl.lockExclusive(io);
                    defer tl.unlockExclusive(io);
                    return try self.executeStatementInternal(stmt);
                },
                else => {
                    tl.lockRead(io);
                    defer tl.unlockRead(io);
                    return try self.executeStatementInternal(stmt);
                },
            }
        }

        if (is_read) {
            self.db.rw_lock.lockShared(io);
            defer self.db.rw_lock.unlockShared(io);
            return try self.executeStatementInternal(stmt);
        } else {
            self.db.rw_lock.lock(io);
            defer self.db.rw_lock.unlock(io);
            return try self.executeStatementInternal(stmt);
        }
    }

    /// The big statement dispatcher: executes one parsed statement, assuming
    /// locks are already held.
    ///
    /// This is the core of the executor. It switches on the [`ast.Statement`]
    /// union and carries out each kind directly against the storage engine:
    /// DDL (CREATE/DROP/ALTER TABLE, indexes) mutates the catalog and system
    /// tables and logs WAL records; DML (INSERT/UPDATE/DELETE) appends MVCC
    /// versions, maintains secondary indexes, and enforces FK/UNIQUE
    /// constraints; SELECT builds the iterator tree ([`buildIteratorTree`]),
    /// materialises subqueries, then does grouping/aggregation, ORDER BY,
    /// DISTINCT and LIMIT/OFFSET; UNION merges child SELECTs with optional
    /// dedup; transaction-control statements (BEGIN/COMMIT/ROLLBACK/SAVEPOINT/
    /// RELEASE) drive the transaction manager and snapshot/SSI lifecycle;
    /// and IMPORT/EXPORT/BACKUP/ANALYZE/user-and-role statements round it out.
    /// It is also called recursively for subqueries, UNION branches, and
    /// manifest-driven import/export. Unhandled statements return
    /// `error.UnsupportedStatement`.
    fn executeStatementInternal(self: *QueryExecutor, stmt: ast.Statement) !QueryResponse {
        switch (stmt) {
            .create_table => |ct| {
                if (ct.if_not_exists and self.db.catalog.getTable(ct.table_name) != null) {
                    return QueryResponse{ .rows_affected = 0 };
                }

                var cols = try self.allocator.alloc(Column, ct.columns.len);
                errdefer self.allocator.free(cols);
                for (ct.columns, 0..) |col_ast, i| {
                    const c_type = mapSqlType(col_ast.type_name);
                    cols[i] = Column{
                        .name = try self.allocator.dupe(u8, col_ast.name),
                        .type = c_type,
                        .size = fixedSizeForType(c_type),
                        .offset = 0,
                        .is_primary_key = col_ast.is_primary_key,
                        .is_auto_increment = false,
                        .is_nullable = col_ast.is_nullable,
                        .default_value = if (col_ast.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                    };
                }

                _ = try self.db.createTable(ct.table_name, cols, self.current_tx_id.?);

                for (ct.columns) |col_ast| {
                    if (col_ast.is_unique and !col_ast.is_primary_key) {
                        const uq_name = try std.fmt.allocPrint(self.allocator, "uq_{s}_{s}", .{ ct.table_name, col_ast.name });
                        defer self.allocator.free(uq_name);
                        var ucols = try self.allocator.alloc(Column, 1);
                        ucols[0] = Column{
                            .name = try self.allocator.dupe(u8, col_ast.name),
                            .type = mapSqlType(col_ast.type_name),
                            .size = fixedSizeForType(mapSqlType(col_ast.type_name)),
                            .offset = 0,
                        };
                        _ = try self.db.createIndex(uq_name, ct.table_name, schema.IndexKind.UNIQUE, ucols, null, self.current_tx_id.?);
                    }
                }

                for (ct.columns) |col_ast| {
                    if (col_ast.foreign_key_table) |ref_tbl| {
                        const ref_col = col_ast.foreign_key_column.?;
                        const fk_name = try std.fmt.allocPrint(self.allocator, "fk_{s}_{s}", .{ ct.table_name, col_ast.name });
                        defer self.allocator.free(fk_name);

                        try self.db.createForeignKey(fk_name, ct.table_name, ref_tbl, col_ast.name, ref_col, self.current_tx_id.?);
                    }
                }

                return QueryResponse{ .rows_affected = 1 };
            },
            .create_index => |ci| {
                var cols = try self.allocator.alloc(Column, ci.columns.len);
                errdefer self.allocator.free(cols);
                for (ci.columns, 0..) |col_name, i| {
                    cols[i] = Column{
                        .name = try self.allocator.dupe(u8, col_name),
                        .type = ColumnType.TEXT,
                        .size = 255,
                        .offset = 0,
                    };
                }
                const kind = if (ci.is_unique) schema.IndexKind.UNIQUE else schema.IndexKind.COMPOSITE;
                _ = try self.db.createIndex(ci.index_name, ci.table_name, kind, cols, null, self.current_tx_id.?);

                const table_meta = for (self.db.catalog.tables.items) |tbl| {
                    if (std.mem.eql(u8, tbl.name, ci.table_name)) break tbl;
                } else return error.TableNotFound;

                const table_tree = try self.db.getTableTree(ci.table_name);
                const idx_tree = try self.db.getIndexTree(ci.index_name);

                const current_tx = self.current_tx_id orelse return error.NoActiveTransaction;

                // Sorted bulk build (the SQLite CREATE INDEX strategy, mirroring the
                // document side's backfillIndex). A single SEQUENTIAL table scan
                // extracts every encoded index key (`enc(col):...:pk`) into one shared
                // blob; the keys are then SORTED and inserted in index order. Sorted
                // insertion keeps every insert on the tree's hot right-hand edge
                // instead of descending to a random leaf per key, so the index's
                // working set stays in the buffer pool even when the table is far
                // larger than it. The previous per-row insert in pk order landed each
                // key at a random leaf, turning the build into O(n log n) random I/O
                // once the index exceeded the pool (≈12.5 min for 1M rows on three
                // indexes); the sorted build is effectively sequential. The root page
                // id is persisted once at the end rather than per row.
                const KeySpan = struct { off: u32, len: u32 };
                var blob = std.ArrayList(u8).empty;
                defer blob.deinit(self.allocator);
                var spans = std.ArrayList(KeySpan).empty;
                defer spans.deinit(self.allocator);

                var it = try table_tree.iterator();
                defer it.deinit();

                while (try it.next()) |cell| {
                    if (try self.getVisibleVersion(table_meta, cell.value, cell.flags.value_overflow, current_tx)) |visible_row| {
                        defer self.freeTableRow(visible_row);

                        const off: u32 = @intCast(blob.items.len);
                        for (ci.columns, 0..) |col_name, col_i| {
                            if (col_i > 0) try blob.append(self.allocator, ':');
                            const rv = try visible_row.getTextAlloc(self.allocator, col_name);
                            defer if (rv) |r| self.allocator.free(r);
                            const raw_val = rv orelse "NULL";
                            const enc = try schema.types.encodeIndexValueAlloc(self.allocator, colTypeByName(table_meta, col_name), raw_val);
                            defer self.allocator.free(enc);
                            try blob.appendSlice(self.allocator, enc);
                        }
                        try blob.append(self.allocator, ':');
                        try blob.appendSlice(self.allocator, cell.key);
                        try spans.append(self.allocator, .{ .off = off, .len = @intCast(blob.items.len - off) });
                    }
                }

                const base = blob.items;
                std.mem.sort(KeySpan, spans.items, base, struct {
                    fn lt(b: []const u8, a: KeySpan, c: KeySpan) bool {
                        return std.mem.order(u8, b[a.off .. a.off + a.len], b[c.off .. c.off + c.len]) == .lt;
                    }
                }.lt);

                for (spans.items) |s| {
                    idx_tree.insert(base[s.off .. s.off + s.len], "") catch |err| switch (err) {
                        error.KeyAlreadyExists => {},
                        else => return err,
                    };
                }
                try self.db.updateIndexRootPageId(ci.index_name, idx_tree.root_page_id, current_tx);

                return QueryResponse{ .rows_affected = 1 };
            },
            .insert => |ins| {
                self.ssiTrackWrite(ins.table_name);
                const table_tree = try self.db.getTableTree(ins.table_name);

                const table_meta = for (self.db.catalog.tables.items) |tbl| {
                    if (std.mem.eql(u8, tbl.name, ins.table_name)) break tbl;
                } else return error.TableNotFound;

                var pk_col_index: ?usize = null;
                for (table_meta.columns, 0..) |col, i| {
                    if (col.is_primary_key) {
                        pk_col_index = i;
                        break;
                    }
                }

                var inserted: u64 = 0;
                for (ins.rows) |row_values| {
                var row_obj: CatalogCellMap = .empty;
                defer {
                    var i: usize = 0;
                    while (i < row_obj.entries.len) : (i += 1) {
                        self.allocator.free(row_obj.entries.items(.value)[i].text);
                    }
                    row_obj.deinit(self.allocator);
                }

                var pk_val: ?[]const u8 = null;
                for (ins.columns, 0..) |col_name, i| {
                    const expr_val = row_values[i];
                    const val_str = switch (expr_val) {
                        .literal_int => |val| try std.fmt.allocPrint(self.allocator, "{d}", .{val}),
                        .literal_float => |val| try std.fmt.allocPrint(self.allocator, "{d}", .{val}),
                        .literal_text => |val| try self.allocator.dupe(u8, val),
                        else => return error.UnsupportedInsertValue,
                    };
                    errdefer self.allocator.free(val_str);

                    try row_obj.put(self.allocator, col_name, query_iter.Cell{ .text = val_str });

                    if (pk_col_index) |idx| {
                        if (std.mem.eql(u8, table_meta.columns[idx].name, col_name)) {
                            pk_val = val_str;
                        }
                    }
                }

                var owned_pk = false;
                const final_pk = if (pk_val) |pk| pk else blk: {
                    const row_id = Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds();
                    owned_pk = true;
                    break :blk try std.fmt.allocPrint(self.allocator, "{d}", .{row_id});
                };
                defer if (owned_pk) self.allocator.free(final_pk);

                try self.validateForeignKeyConstraintsForInsertOrUpdate(table_meta.id, row_obj);
                self.validateUniqueConstraints(table_meta.id, row_obj, null) catch |err| {
                    if (err == error.UniqueConstraintViolation) {
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "UNIQUE constraint violation") };
                    }
                    return err;
                };

                try self.writeNewVersion(table_tree, ins.table_name, final_pk, row_obj, false);

                for (self.db.catalog.indexes.items) |idx| {
                    if (idx.table_id == table_meta.id) {
                        const idx_tree = try self.db.getIndexTree(idx.name);

                        var list = std.ArrayList([]const u8).empty;
                        defer {
                            for (list.items) |item| self.allocator.free(item);
                            list.deinit(self.allocator);
                        }

                        for (idx.key_columns) |col| {
                            const cell_val = row_obj.get(col.name);
                            const raw_val = if (cell_val) |v| v.text else "NULL";
                            const str_val = try schema.types.encodeIndexValueAlloc(self.allocator, colTypeByName(table_meta, col.name), raw_val);
                            try list.append(self.allocator, str_val);
                        }

                        try list.append(self.allocator, try self.allocator.dupe(u8, final_pk));

                        var key_parts = try self.allocator.alloc([]const u8, list.items.len);
                        defer self.allocator.free(key_parts);
                        for (list.items, 0..) |part, idx_j| key_parts[idx_j] = part;

                        const index_key = try std.mem.join(self.allocator, ":", key_parts);
                        defer self.allocator.free(index_key);

                        try idx_tree.insert(index_key, "");
                        try self.db.updateIndexRootPageId(idx.name, idx_tree.root_page_id, self.current_tx_id.?);
                    }
                }

                inserted += 1;
                }

                return QueryResponse{ .rows_affected = inserted };
            },
            .union_query => |u| {
                var any_dedup = false;
                for (u.all) |a| {
                    if (!a) any_dedup = true;
                }

                var out_rows = std.ArrayList([]const []const u8).empty;
                var out_cols: []const []const u8 = &.{};
                var out_types: []const ColumnType = &.{};
                var have_cols = false;

                var seen = std.StringHashMap(void).init(self.allocator);
                defer {
                    var it = seen.keyIterator();
                    while (it.next()) |k| self.allocator.free(k.*);
                    seen.deinit();
                }

                for (u.selects) |sel| {
                    const res = try self.executeStatementInternal(.{ .select = sel });
                    if (res.error_message != null) {
                        for (out_rows.items) |row| {
                            for (row) |cell| self.allocator.free(cell);
                            self.allocator.free(row);
                        }
                        out_rows.deinit(self.allocator);
                        for (out_cols) |c| self.allocator.free(c);
                        if (out_cols.len > 0) self.allocator.free(out_cols);
                        if (out_types.len > 0) self.allocator.free(out_types);
                        return res;
                    }
                    if (!have_cols) {
                        const cols = try self.allocator.alloc([]const u8, res.columns.len);
                        for (res.columns, 0..) |c, i| cols[i] = try self.allocator.dupe(u8, c);
                        out_cols = cols;
                        const types = try self.allocator.alloc(ColumnType, res.column_types.len);
                        for (res.column_types, 0..) |t, i| types[i] = t;
                        out_types = types;
                        have_cols = true;
                    }
                    for (res.rows) |row| {
                        if (any_dedup) {
                            const key = try std.mem.join(self.allocator, "\x00", row);
                            if (seen.contains(key)) {
                                self.allocator.free(key);
                                continue;
                            }
                            try seen.put(key, {});
                        }
                        const newrow = try self.allocator.alloc([]const u8, row.len);
                        for (row, 0..) |cell, i| newrow[i] = try self.allocator.dupe(u8, cell);
                        try out_rows.append(self.allocator, newrow);
                    }
                    for (res.columns) |c| self.allocator.free(c);
                    if (res.columns.len > 0) self.allocator.free(res.columns);
                    for (res.rows) |row| {
                        for (row) |cell| self.allocator.free(cell);
                        self.allocator.free(row);
                    }
                    if (res.rows.len > 0) self.allocator.free(res.rows);
                    if (res.column_types.len > 0) self.allocator.free(res.column_types);
                }

                return QueryResponse{
                    .columns = out_cols,
                    .column_types = out_types,
                    .rows = try out_rows.toOwnedSlice(self.allocator),
                };
            },
            .select => |sel_in| {
                self.ssiTrackRead(sel_in.table_name);
                for (sel_in.joins) |j| self.ssiTrackRead(j.right_table);
                var sq_arena = std.heap.ArenaAllocator.init(self.allocator);
                defer sq_arena.deinit();
                var sel = sel_in;
                sel.where_expr = try self.materializeSubqueries(sel_in.where_expr, sq_arena.allocator());
                sel.having_expr = try self.materializeSubqueries(sel_in.having_expr, sq_arena.allocator());
                // Strip a leading table qualifier that matches the driving table's
                // name or its alias from every column reference, so `SELECT a.id
                // FROM t a WHERE a.v = 25` resolves `a.id`/`a.v` to the bare `id`/`v`
                // the row is keyed by (otherwise the value/WHERE paths match the full
                // `a.id` against bare field names and yield NULL / drop the predicate).
                // Gated to single-table selects; join-side qualifier resolution across
                // a combined row is a separate path and is left untouched.
                normalizeSingleTableQualifiers(sel);
                // Joins: map alias qualifiers (`e.col`) to the real table name
                // (`emp.col`) the combined-row resolver understands. No-op without
                // joins or aliases. Uses the subquery arena for the rewritten names.
                normalizeJoinAliases(sel, sq_arena.allocator());

                // Index-only COUNT(*): answer from the index without scanning base
                // rows when sound (see tryIndexOnlyCount). Falls through otherwise.
                if (try self.tryIndexOnlyCount(sel)) |resp| return resp;
                // MIN/MAX from index endpoints instead of a full scan.
                if (try self.tryIndexMinMax(sel)) |resp| return resp;
                // DISTINCT / COUNT(DISTINCT) via a loose (skip) index scan.
                if (try self.tryLooseDistinct(sel)) |resp| return resp;
                // GROUP BY g, AGG(v) answered index-only from a covering (g,v) index.
                if (try self.tryIndexGroupAgg(sel)) |resp| return resp;
                // Scalar AGG(col) WHERE <range on col> answered index-only (col is in
                // the index key), no base-row descent. Closes the ~197x wide-range
                // aggregate gap vs PostgreSQL measured on the disk-bound VM.
                if (try self.tryIndexOnlyScalarAgg(sel)) |resp| return resp;
                // Scalar AGG(v) WHERE <range on f> answered index-only from a
                // COVERING composite (f, v) index (v decoded from the second key
                // field), no clustered base-row descent - the covering-index answer
                // to the wide secondary-index base fetch.
                if (try self.tryIndexOnlyScalarAggComposite(sel)) |resp| return resp;

                // Projection pushdown: for a simple single-table read, decode only
                // the columns the plan actually reads. Saved/restored so nested or
                // subsequent statements are unaffected (the drain below is
                // synchronous and non-reentrant: subqueries were materialised above).
                const prev_needed = self.scan_needed_cols;
                self.scan_needed_cols = self.collectNeededCols(sel);
                defer self.scan_needed_cols = prev_needed;

                var iter = try self.buildIteratorTree(sel);
                defer iter.deinit();

                var columns = std.ArrayList([]const u8).empty;
                defer columns.deinit(self.allocator);
                errdefer {
                    for (columns.items) |c| self.allocator.free(c);
                }

                var col_types = std.ArrayList(ColumnType).empty;
                defer col_types.deinit(self.allocator);

                // Binary result path: a single-table `SELECT *` is expanded into one
                // typed cell PER COLUMN, rather than one JSON-serialised blob cell.
                // This removes JSON from the wire (row_description lists the real
                // columns; data_row carries a value per column) and removes the
                // per-row JSON re-serialisation. Restricted to no-join so column
                // identity/order is unambiguous.
                const star_expand = sel.projections.len == 1 and
                    sel.projections[0].expr == .star and sel.joins.len == 0;
                const star_meta: ?Table = if (star_expand)
                    self.db.catalog.getTable(sel.table_name)
                else
                    null;

                if (star_expand and star_meta != null) {
                    for (star_meta.?.columns) |col| {
                        try columns.append(self.allocator, try self.allocator.dupe(u8, col.name));
                        try col_types.append(self.allocator, col.type);
                    }
                } else for (sel.projections) |proj| {
                    const header = switch (proj.expr) {
                        .star => "*",
                        .column => |col| col,
                        .aggregate => |agg| switch (agg.kind) {
                            .COUNT => "COUNT",
                            .SUM => "SUM",
                            .MIN => "MIN",
                            .MAX => "MAX",
                            .AVG => "AVG",
                        },
                    };
                    if (proj.alias) |alias| {
                        try columns.append(self.allocator, try self.allocator.dupe(u8, alias));
                    } else {
                        try columns.append(self.allocator, try self.allocator.dupe(u8, header));
                    }
                    try col_types.append(self.allocator, self.projectionType(sel, proj));
                }

                var rows = std.ArrayList([]const []const u8).empty;
                defer rows.deinit(self.allocator);
                errdefer {
                    for (rows.items) |row| {
                        for (row) |c| self.allocator.free(c);
                        self.allocator.free(row);
                    }
                }

                // Set when the star-expand path ships numeric columns as binary
                // cells, so the response marks them binary in the RowDescription.
                var result_binary = false;

                const is_agg_query = blk: {
                    if (sel.group_by != null) break :blk true;
                    for (sel.projections) |p| if (p.expr == .aggregate) break :blk true;
                    break :blk false;
                };
                if (is_agg_query) {
                    var g_arena = std.heap.ArenaAllocator.init(self.allocator);
                    defer g_arena.deinit();
                    const ga = g_arena.allocator();

                    var groups = std.StringHashMap(GroupAcc).init(ga);
                    var group_order = std.ArrayList([]const u8).empty;

                    var hv_specs = std.ArrayList(ast.AggregateCall).empty;
                    if (sel.having_expr) |he| try collectHavingAggs(sel, he, &hv_specs, ga);

                    while (try iter.next()) |row| {
                        try self.checkDeadline();
                        var key_buf = std.ArrayList(u8).empty;
                        if (sel.group_by) |gcols| {
                            for (gcols) |gc| {
                                const v = query_iter.getVal(&ast.Expr{ .column_ref = gc }, row);
                                const s = if (v) |vv| try scalarTextInto(ga, vv) else "NULL";
                                try key_buf.appendSlice(ga, s);
                                try key_buf.append(ga, 0x1f);
                            }
                        }
                        const key = try key_buf.toOwnedSlice(ga);

                        const gop = try groups.getOrPut(key);
                        if (!gop.found_existing) {
                            const cv = try ga.alloc([]const u8, sel.projections.len);
                            const ac = try ga.alloc(AggAcc, sel.projections.len);
                            for (sel.projections, 0..) |proj, i| {
                                ac[i] = .{};
                                cv[i] = switch (proj.expr) {
                                    .column => |col| dc: {
                                        const v = query_iter.getVal(&ast.Expr{ .column_ref = col }, row);
                                        break :dc if (v) |vv| try scalarTextInto(ga, vv) else try ga.dupe(u8, "NULL");
                                    },
                                    else => "",
                                };
                            }
                            const hv = try ga.alloc(AggAcc, hv_specs.items.len);
                            for (hv) |*h| h.* = .{};
                            gop.value_ptr.* = .{ .col_vals = cv, .aggs = ac, .hv = hv };
                            try group_order.append(ga, key);
                        }

                        for (sel.projections, 0..) |proj, i| {
                            if (proj.expr != .aggregate) continue;
                            try self.foldOneAgg(&gop.value_ptr.aggs[i], proj.expr.aggregate, row, ga);
                        }
                        for (hv_specs.items, 0..) |hspec, j| {
                            try self.foldOneAgg(&gop.value_ptr.hv[j], hspec, row, ga);
                        }
                    }

                    if (groups.count() == 0 and sel.group_by == null) {
                        const ekey = try ga.dupe(u8, "");
                        const cv = try ga.alloc([]const u8, sel.projections.len);
                        const ac = try ga.alloc(AggAcc, sel.projections.len);
                        for (sel.projections, 0..) |_, i| {
                            ac[i] = .{};
                            cv[i] = "";
                        }
                        const hv0 = try ga.alloc(AggAcc, hv_specs.items.len);
                        for (hv0) |*h| h.* = .{};
                        try groups.put(ekey, .{ .col_vals = cv, .aggs = ac, .hv = hv0 });
                        try group_order.append(ga, ekey);
                    }

                    for (group_order.items) |k| {
                        const grp = groups.get(k).?;
                        if (sel.having_expr) |he| {
                            if (!evalHaving(sel, grp, hv_specs.items, he)) continue;
                        }
                        const cells = try self.allocator.alloc([]const u8, sel.projections.len);
                        for (sel.projections, 0..) |proj, i| {
                            cells[i] = switch (proj.expr) {
                                .aggregate => |agg| try self.formatAggregate(agg, grp.aggs[i]),
                                .column => try self.allocator.dupe(u8, grp.col_vals[i]),
                                .star => try self.allocator.dupe(u8, ""),
                            };
                        }
                        try rows.append(self.allocator, cells);
                    }

                    // ORDER BY over the grouped result: the keys reference output
                    // columns (a group column, or an aggregate by its alias/ordinal),
                    // so sort the emitted rows by the resolved column indices. Only
                    // when every key resolves; otherwise leave the group order.
                    if (sel.order_by) |ob| {
                        if (rows.items.len > 1 and ob.len > 0) {
                            var okeys = try self.allocator.alloc(usize, ob.len);
                            defer self.allocator.free(okeys);
                            var resolvable = true;
                            for (ob, 0..) |k, ki| {
                                okeys[ki] = resolveOrderColIndex(columns.items, k.column) orelse {
                                    resolvable = false;
                                    break;
                                };
                            }
                            if (resolvable) {
                                const n = rows.items.len;
                                const idx = try self.allocator.alloc(usize, n);
                                defer self.allocator.free(idx);
                                for (idx, 0..) |*x, i| x.* = i;
                                const SortCtx = struct {
                                    rows: []const []const []const u8,
                                    okeys: []const usize,
                                    ob: []const ast.OrderKey,
                                    fn lessThan(ctx: @This(), ia: usize, ib: usize) bool {
                                        for (ctx.ob, 0..) |ok, ki| {
                                            const col = ctx.okeys[ki];
                                            const c = orderCompareKey(ctx.rows[ia][col], ctx.rows[ib][col]);
                                            if (c != 0) return if (ok.desc) c > 0 else c < 0;
                                        }
                                        return false;
                                    }
                                };
                                std.sort.pdq(usize, idx, SortCtx{ .rows = rows.items, .okeys = okeys, .ob = ob }, SortCtx.lessThan);
                                const original = try self.allocator.dupe([]const []const u8, rows.items);
                                defer self.allocator.free(original);
                                for (idx, 0..) |src, dst| rows.items[dst] = original[src];
                            }
                        }
                    }

                    // OFFSET / LIMIT after grouping + ordering (GROUP BY already
                    // dedups, so no DISTINCT handling is needed here).
                    if (sel.limit != null or sel.offset != null) {
                        const off: usize = if (sel.offset) |o| o else 0;
                        var w: usize = 0;
                        for (rows.items, 0..) |row, pos| {
                            var keep = true;
                            if (pos < off) {
                                keep = false;
                            } else if (sel.limit) |lim| {
                                if (w >= lim) keep = false;
                            }
                            if (keep) {
                                rows.items[w] = row;
                                w += 1;
                            } else {
                                for (row) |c| self.allocator.free(c);
                                self.allocator.free(row);
                            }
                        }
                        while (rows.items.len > w) _ = rows.pop();
                    }

                    return QueryResponse{
                        .columns = try columns.toOwnedSlice(self.allocator),
                        .column_types = try col_types.toOwnedSlice(self.allocator),
                        .rows = try rows.toOwnedSlice(self.allocator),
                        .rows_affected = 0,
                    };
                }

                var count_agg: i64 = 0;
                var sum_agg: i64 = 0;
                var has_agg = false;

                const has_order = sel.order_by != null;
                // Indexed ORDER BY: when the base access is a secondary-index scan
                // whose leading column is exactly the sole ORDER BY key, the rows
                // already arrive in that column's order (order-preserving encoding),
                // so the comparison sort is unnecessary. If the SCAN direction
                // matches the requested direction (ascending scan for ASC, backward
                // scan for DESC) the collected order is FINAL and can stream + break
                // at LIMIT (`order_by_scan_asc`). If they differ (an ascending scan
                // feeding a DESC request) the collected rows just need reversing
                // (`order_by_scan_desc`). Only with no joins and a single ORDER key.
                var order_by_scan_asc = false;
                var order_by_scan_desc = false;
                if (sel.order_by) |ob| {
                    if (ob.len == 1 and sel.joins.len == 0) {
                        if (self.scan_ordered_col) |oc| {
                            if (std.mem.eql(u8, ob[0].column, oc)) {
                                if (ob[0].desc == self.scan_order_is_desc) {
                                    order_by_scan_asc = true; // scan order is final
                                } else {
                                    order_by_scan_desc = true; // reverse of final
                                }
                            }
                        }
                    }
                }
                // A comparison sort is needed only for an ORDER BY the scan does
                // not already satisfy.
                const sort_needed = has_order and !order_by_scan_asc and !order_by_scan_desc;
                // The streaming LIMIT break is safe when the collected order is the
                // final order: no ORDER BY, or an ascending scan-satisfied one.
                const can_stream = !has_order or order_by_scan_asc;
                var sort_keys = std.ArrayList([]const []const u8).empty;
                defer {
                    for (sort_keys.items) |ks| {
                        for (ks) |k| self.allocator.free(k);
                        self.allocator.free(ks);
                    }
                    sort_keys.deinit(self.allocator);
                }

                self.qprof = std.c.getenv("KAIDB_QPROF") != null;
                // Binary numeric results are the default: the driver now decodes
                // big-endian int/float cells directly from the socket buffer (no
                // string alloc, no decimal round-trip), which is faster than parsing
                // text digits. `KAIDB_TEXT_RESULTS` forces the legacy text encoding.
                self.binary_results = std.c.getenv("KAIDB_TEXT_RESULTS") == null;
                self.qp_json.reset();
                self.qp_seek.reset();
                const qp_io = self.db.pool.pager.io;
                var qp_scan = StopWatch{};
                var qp_proj = StopWatch{};
                var qp_sort = StopWatch{};
                var qp_rows: u64 = 0;

                // Streaming: when a sink is attached and this is the scan-order
                // single-table `SELECT *` fast path (no sort/offset/distinct), emit
                // each row to the client during the scan instead of buffering the
                // whole result. Send the RowDescription up front via begin(); rows
                // then stream in scan order. A sorted/offset/distinct query cannot
                // stream (its final order is not the scan order) and stays buffered.
                const do_stream = self.row_sink != null and star_expand and star_meta != null and
                    can_stream and !sort_needed and !order_by_scan_desc and
                    (sel.offset == null or self.scan_offset_pushed) and !sel.distinct;
                var streamed_rows: u64 = 0;
                if (do_stream) {
                    if (self.binary_results) result_binary = true;
                    const sink = self.row_sink.?;
                    try sink.begin(sink.ctx, columns.items, col_types.items, result_binary);
                }

                // Running total of bytes buffered into `rows` so far, checked
                // against `self.result_bytes_limit` inside the loop.
                var result_bytes: usize = 0;

                while (true) {
                    if (self.qprof) qp_scan.start(qp_io);
                    const maybe_row = try iter.next();
                    if (self.qprof) qp_scan.stop(qp_io);
                    const row = maybe_row orelse break;
                    qp_rows += 1;
                    if (self.qprof) qp_proj.start(qp_io);
                    try self.checkDeadline();
                    var row_cells = std.ArrayList([]const u8).empty;
                    errdefer {
                        for (row_cells.items) |c| self.allocator.free(c);
                        row_cells.deinit(self.allocator);
                    }

                    for (sel.projections) |proj| {
                        switch (proj.expr) {
                            .star => {
                                // Binary result path: emit one cell PER COLUMN (in
                                // schema order) straight from the row's typed cells,
                                // with NO JSON object in between. This is the hot
                                // single-table `SELECT *` path (and what the wire
                                // ships): a plain text cell per column, no per-row
                                // JSON build or re-serialise.
                                if (star_expand) {
                                    if (star_meta) |m| {
                                        for (m.columns) |col| {
                                            // Index of this column's cell in the single
                                            // source table row (null when absent / joined).
                                            const src_idx: ?usize = if (row.data.len == 1)
                                                row.data[0].find(col.name)
                                            else
                                                null;
                                            const cell: query_iter.Cell = if (src_idx) |ix|
                                                row.data[0].cells[ix]
                                            else
                                                .null;
                                            // Binary path (always): numeric columns ship
                                            // as big-endian fixed-width bytes, no dtoa on the
                                            // server and no atoi/strtod on the driver. This is
                                            // the ONLY result format for numeric columns; there
                                            // is no text-numeric fallback on the wire.
                                            if (self.binary_results and oidmap.isBinaryType(col.type) and cell != .null) {
                                                try row_cells.append(self.allocator, try encodeBinaryCell(self.allocator, col.type, cell));
                                            } else if (cell == .text) {
                                                // Zero-copy: the `.text` bytes (e.g. the
                                                // `details` blob) are already an owned heap
                                                // copy on this row. MOVE that slice straight
                                                // into the wire buffer instead of duping it
                                                // again, and null the source cell so the
                                                // iterator's `freeTableRow` will not free the
                                                // now-transferred bytes (no double free). The
                                                // row is fully drained before the iterator
                                                // advances, so this cell is not read again.
                                                try row_cells.append(self.allocator, cell.text);
                                                @constCast(row.data[0].cells)[src_idx.?] = .null;
                                            } else {
                                                const s = (try cell.toTextAlloc(self.allocator)) orelse try self.allocator.dupe(u8, "NULL");
                                                try row_cells.append(self.allocator, s);
                                            }
                                        }
                                        // Numeric columns ship binary only when explicitly
                                        // enabled (env KAIDB_BINARY_RESULTS). Default is text:
                                        // the driver's `takeI64`/`takeF64` parse digits straight
                                        // from the socket buffer with no per-cell allocation,
                                        // whereas the binary float path round-trips through a
                                        // decimal string on the client (no bytes->double
                                        // primitive), which measured slower for wide result sets.
                                        if (self.binary_results) result_binary = true;
                                        continue;
                                    }
                                }
                                // General `SELECT *` (multi-table join, or a table not
                                // in the catalog): reconstruct the single JSON-object
                                // blob cell the client expects. Not the hot path, so a
                                // transient arena-backed object is fine; row storage
                                // itself stays json-free.
                                var star_arena = std.heap.ArenaAllocator.init(self.allocator);
                                defer star_arena.deinit();
                                const aa = star_arena.allocator();
                                var merged_obj = std.json.ObjectMap.empty;
                                for (row.data, 0..) |tr, t_idx| {
                                    if (tr.is_null) continue;
                                    for (tr.names, 0..) |nm, i| {
                                        const key = if (row.tables.len > 1)
                                            try std.fmt.allocPrint(aa, "{s}.{s}", .{ row.tables[t_idx], nm })
                                        else
                                            nm;
                                        try merged_obj.put(aa, key, tr.cells[i].scalar().toJsonValue());
                                    }
                                }
                                const merged_val = std.json.Value{ .object = merged_obj };
                                var star_alloc = std.Io.Writer.Allocating.init(self.allocator);
                                defer star_alloc.deinit();
                                try star_alloc.writer.print("{f}", .{std.json.fmt(merged_val, .{})});
                                try row_cells.append(self.allocator, try self.allocator.dupe(u8, star_alloc.written()));
                            },
                            .column => |col| {
                                const val = query_iter.getVal(&ast.Expr{ .column_ref = col }, row);
                                const cell = if (val) |v| try self.scalarText(v) else try self.allocator.dupe(u8, "NULL");
                                try row_cells.append(self.allocator, cell);
                            },
                            .aggregate => |agg| {
                                has_agg = true;
                                switch (agg.kind) {
                                    .COUNT => count_agg += 1,
                                    .SUM => {
                                        switch (agg.argument) {
                                            .column => |col| {
                                                const val = query_iter.getVal(&ast.Expr{ .column_ref = col }, row);
                                                if (val) |v| {
                                                    if (v != .null) {
                                                        const t = try self.scalarText(v);
                                                        defer self.allocator.free(t);
                                                        sum_agg += std.fmt.parseInt(i64, t, 10) catch 0;
                                                    }
                                                }
                                            },
                                            else => {},
                                        }
                                    },
                                    else => {},
                                }
                            },
                        }
                    }

                    if (!has_agg) {
                        // Result-materialisation cap. A non-streamable query (an
                        // unindexed ORDER BY, a full scan without a usable LIMIT)
                        // collects every matching row here before it can sort or
                        // limit, so on a large table this buffer is what grows
                        // without bound and OOM-kills the server. Charge each
                        // kept row's bytes against the executor's budget and fail
                        // the query cleanly (surfaced as "Query Memory Limit
                        // Exceeded") the moment it would exceed the cap, so the
                        // server survives instead of being killed. Streamable
                        // queries never reach the cap because they break at LIMIT
                        // just below.
                        if (do_stream) {
                            // Send this row now and free it; nothing is buffered, so the
                            // memory cap and sort-key collection below do not apply.
                            const sink = self.row_sink.?;
                            try sink.row(sink.ctx, row_cells.items);
                            for (row_cells.items) |c| self.allocator.free(c);
                            row_cells.deinit(self.allocator);
                            streamed_rows += 1;
                        } else {
                            if (self.result_bytes_limit) |cap| {
                                for (row_cells.items) |c| result_bytes += c.len;
                                if (sort_needed) result_bytes += @sizeOf(usize); // sort-key overhead, approx
                                if (result_bytes > cap) return error.OutOfMemory;
                            }
                            if (sort_needed) {
                                const ob = sel.order_by.?;
                                const keys = try self.allocator.alloc([]const u8, ob.len);
                                for (ob, 0..) |ok, ki| {
                                    const kv = query_iter.getVal(&ast.Expr{ .column_ref = ok.column }, row);
                                    keys[ki] = try orderKeyString(self.allocator, kv);
                                }
                                try sort_keys.append(self.allocator, keys);
                            }
                            try rows.append(self.allocator, try row_cells.toOwnedSlice(self.allocator));
                        }
                    } else {
                        row_cells.deinit(self.allocator);
                    }

                    if (self.qprof) qp_proj.stop(qp_io);

                    if (can_stream and (sel.offset == null or self.scan_offset_pushed) and !sel.distinct) {
                        if (sel.limit) |lim| {
                            const produced = if (do_stream) streamed_rows else rows.items.len;
                            if (produced >= lim) break;
                        }
                    }
                }
                if (self.qprof) qp_sort.start(qp_io);

                // Descending ORDER BY satisfied by an ascending index scan: reverse
                // the collected rows into final (descending) order, no sort.
                if (order_by_scan_desc and rows.items.len > 1) {
                    std.mem.reverse([]const []const u8, rows.items);
                }

                if (sort_needed and rows.items.len > 1) {
                    const n = rows.items.len;
                    const idx = try self.allocator.alloc(usize, n);
                    defer self.allocator.free(idx);
                    for (idx, 0..) |*x, i| x.* = i;
                    const SortCtx = struct {
                        keys: []const []const []const u8,
                        ob: []const ast.OrderKey,
                        fn lessThan(ctx: @This(), ia: usize, ib: usize) bool {
                            for (ctx.ob, 0..) |ok, ki| {
                                const c = orderCompareKey(ctx.keys[ia][ki], ctx.keys[ib][ki]);
                                if (c != 0) return if (ok.desc) c > 0 else c < 0;
                            }
                            return false;
                        }
                    };
                    std.sort.pdq(usize, idx, SortCtx{ .keys = sort_keys.items, .ob = sel.order_by.? }, SortCtx.lessThan);
                    const original = try self.allocator.dupe([]const []const u8, rows.items);
                    defer self.allocator.free(original);
                    for (idx, 0..) |src, dst| rows.items[dst] = original[src];
                }
                {
                    var seen = std.StringHashMap(void).init(self.allocator);
                    var seen_keys = std.ArrayList([]const u8).empty;
                    defer {
                        for (seen_keys.items) |k| self.allocator.free(k);
                        seen_keys.deinit(self.allocator);
                        seen.deinit();
                    }
                    // The base scan already consumed the offset (pushed down), so do
                    // not skip again here.
                    const off: usize = if (self.scan_offset_pushed) 0 else if (sel.offset) |o| o else 0;
                    var distinct_pos: usize = 0;
                    var w: usize = 0;
                    for (rows.items) |row| {
                        var keep = true;
                        if (sel.distinct) {
                            var kb = std.ArrayList(u8).empty;
                            defer kb.deinit(self.allocator);
                            for (row) |c| {
                                try kb.appendSlice(self.allocator, c);
                                try kb.append(self.allocator, 0x1f);
                            }
                            if (seen.contains(kb.items)) {
                                keep = false;
                            } else {
                                const owned = try self.allocator.dupe(u8, kb.items);
                                try seen_keys.append(self.allocator, owned);
                                try seen.put(owned, {});
                            }
                        }
                        if (keep) {
                            const pos = distinct_pos;
                            distinct_pos += 1;
                            if (pos < off) keep = false
                            else if (sel.limit) |lim| {
                                if (w >= lim) keep = false;
                            }
                        }
                        if (keep) {
                            rows.items[w] = row;
                            w += 1;
                        } else {
                            for (row) |c| self.allocator.free(c);
                            self.allocator.free(row);
                        }
                    }
                    while (rows.items.len > w) _ = rows.pop();
                }

                if (has_agg) {
                    var row_cells = try self.allocator.alloc([]const u8, sel.projections.len);
                    for (sel.projections, 0..) |proj, i| {
                        row_cells[i] = switch (proj.expr) {
                            .aggregate => |agg| switch (agg.kind) {
                                .COUNT => try std.fmt.allocPrint(self.allocator, "{d}", .{count_agg}),
                                .SUM => try std.fmt.allocPrint(self.allocator, "{d}", .{sum_agg}),
                                else => try self.allocator.dupe(u8, "0"),
                            },
                            else => try self.allocator.dupe(u8, ""),
                        };
                    }
                    try rows.append(self.allocator, row_cells);
                }

                if (self.qprof) {
                    qp_sort.stop(qp_io);
                    const ns = struct {
                        fn ms(sw: StopWatch) f64 {
                            return @as(f64, @floatFromInt(sw.elapsedNs())) / 1_000_000.0;
                        }
                    };
                    const scan_ms = ns.ms(qp_scan);
                    const json_ms = ns.ms(self.qp_json);
                    const seek_ms = ns.ms(self.qp_seek);
                    std.debug.print("[QPROF] rows={d} scan={d:.1}ms (baseSeek={d:.1} idxwalk+other={d:.1} buildJson={d:.1}) project={d:.1}ms finalize(sort/limit)={d:.1}ms\n", .{
                        qp_rows, scan_ms, seek_ms, scan_ms - json_ms - seek_ms, json_ms, ns.ms(qp_proj), ns.ms(qp_sort),
                    });
                }

                return QueryResponse{
                    .columns = try columns.toOwnedSlice(self.allocator),
                    .column_types = try col_types.toOwnedSlice(self.allocator),
                    .rows = try rows.toOwnedSlice(self.allocator),
                    .rows_affected = if (do_stream) streamed_rows else 0,
                    .result_binary = result_binary,
                    .streamed = do_stream,
                };
            },
            .export_stmt => |exp| {
                switch (exp.target) {
                    .table => |tbl_name| {
                        const table_tree = try self.db.getTableTree(tbl_name);

                        var file = try Io.Dir.createFile(.cwd(), self.db.pool.pager.io, exp.file_path.?, .{ .read = false, .truncate = true });
                        defer file.close(self.db.pool.pager.io);
                        var allocating = std.Io.Writer.Allocating.init(self.allocator);
                        defer allocating.deinit();
                        var writer = allocating.writer;

                        var it = try table_tree.iterator();
                        defer it.deinit();

                        if (exp.format == .CSV) {
                            const table_meta = for (self.db.catalog.tables.items) |tbl| {
                                if (std.mem.eql(u8, tbl.name, tbl_name)) break tbl;
                            } else return error.TableNotFound;

                            for (table_meta.columns, 0..) |col, i| {
                                try writer.writeAll(col.name);
                                if (i + 1 < table_meta.columns.len) try writer.writeAll(",");
                            }
                            try writer.writeAll("\n");

                            while (try it.next()) |cell| {
                                var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, cell.value, .{});
                                defer parsed.deinit();

                                for (table_meta.columns, 0..) |col, i| {
                                    const val = if (parsed.value == .object) parsed.value.object.get(col.name) else null;
                                    if (val) |v| {
                                        try writer.writeAll(v.string);
                                    }
                                    if (i + 1 < table_meta.columns.len) try writer.writeAll(",");
                                }
                                try writer.writeAll("\n");
                            }
                        } else {
                            try writer.writeAll("[\n");
                            var first = true;
                            while (try it.next()) |cell| {
                                if (!first) try writer.writeAll(",\n");
                                first = false;
                                try writer.writeAll(cell.value);
                            }
                            try writer.writeAll("\n]\n");
                        }

                        try file.writeStreamingAll(self.db.pool.pager.io, allocating.written());
                    },
                    .manifest => |manifest_path| {
                        var file = try Io.Dir.openFile(.cwd(), self.db.pool.pager.io, manifest_path, .{ .mode = .read_only });
                        defer file.close(self.db.pool.pager.io);

                        const stat = try file.stat(self.db.pool.pager.io);
                        const buffer = try self.allocator.alloc(u8, stat.size);
                        defer self.allocator.free(buffer);
                        _ = try file.readPositionalAll(self.db.pool.pager.io, buffer, 0);

                        var yaml = Yaml{ .source = buffer };
                        try yaml.load(self.allocator);
                        defer yaml.deinit(self.allocator);

                        var arena = std.heap.ArenaAllocator.init(self.allocator);
                        defer arena.deinit();

                        const ManifestField = struct {
                            name: []const u8,
                            type: []const u8,
                        };
                        const ManifestEntity = struct {
                            name: []const u8,
                            role: []const u8,
                            file: []const u8,
                            fields: []const ManifestField = &.{},
                        };
                        const ExportManifest = struct {
                            store: []const u8,
                            format: []const u8,
                            output_dir: []const u8 = "",
                            entities: []const ManifestEntity = &.{},
                        };

                        const manifest = try yaml.parse(arena.allocator(), ExportManifest);

                        for (manifest.entities) |ent| {
                            const full_path = if (manifest.output_dir.len > 0)
                                try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ manifest.output_dir, ent.file })
                            else
                                try self.allocator.dupe(u8, ent.file);
                            defer self.allocator.free(full_path);

                            const is_csv = std.mem.eql(u8, manifest.format, "csv");
                            const format_kind: ast.ExportFormatKind = if (is_csv) .CSV else .JSON;

                            const sub_exp = ast.ExportStmt{
                                .target = .{ .table = ent.name },
                                .format = format_kind,
                                .file_path = full_path,
                            };
                            _ = try self.executeStatementInternal(.{ .export_stmt = sub_exp });
                        }
                    },
                    else => return error.UnsupportedExportTarget,
                }
                return QueryResponse{ .rows_affected = 1 };
            },
            .import_stmt => |imp| {
                switch (imp.target) {
                    .table => |tbl_name| {
                        const table_meta = for (self.db.catalog.tables.items) |tbl| {
                            if (std.mem.eql(u8, tbl.name, tbl_name)) break tbl;
                        } else return error.TableNotFound;

                        const table_tree = try self.db.getTableTree(tbl_name);

                        var file = try Io.Dir.openFile(.cwd(), self.db.pool.pager.io, imp.file_path.?, .{ .mode = .read_only });
                        defer file.close(self.db.pool.pager.io);

                        if (imp.format == .CSV) {
                            var read_buf: [65536]u8 = undefined;
                            var r = file.readerStreaming(self.db.pool.pager.io, &read_buf);
                            const r_interface = &r.interface;

                            var header_buf: [16384]u8 = undefined;
                            const header_line = (try readLineFromIoReader(r_interface, &header_buf)) orelse return error.EmptyCSV;

                            var csv_headers = std.ArrayList([]const u8).empty;
                            defer {
                                for (csv_headers.items) |h| self.allocator.free(h);
                                csv_headers.deinit(self.allocator);
                            }
                            var header_split = std.mem.splitScalar(u8, header_line, ',');
                            while (header_split.next()) |h| {
                                try csv_headers.append(self.allocator, try self.allocator.dupe(u8, std.mem.trim(u8, h, " \t\r")));
                            }

                            const col_mappings = try self.allocator.alloc(?usize, table_meta.columns.len);
                            defer self.allocator.free(col_mappings);
                            for (table_meta.columns, 0..) |db_col, db_col_idx| {
                                col_mappings[db_col_idx] = null;
                                for (csv_headers.items, 0..) |csv_h, csv_idx| {
                                    if (std.mem.eql(u8, db_col.name, csv_h)) {
                                        col_mappings[db_col_idx] = csv_idx;
                                        break;
                                    }
                                }
                            }

                            var line_buf: [16384]u8 = undefined;
                            var rows_imported: u64 = 0;

                            var pk_col_index: ?usize = null;
                            for (table_meta.columns, 0..) |col, i| {
                                if (col.is_primary_key) {
                                    pk_col_index = i;
                                    break;
                                }
                            }

                            const fields = try self.allocator.alloc([]const u8, csv_headers.items.len);
                            defer self.allocator.free(fields);

                            while (try readLineFromIoReader(r_interface, &line_buf)) |line| {
                                const field_count = parseCsvLine(line, fields);
                                if (field_count < csv_headers.items.len) continue;

                                var row_obj: CatalogCellMap = .empty;
                                defer row_obj.deinit(self.allocator);

                                var pk_val: ?[]const u8 = null;

                                for (table_meta.columns, 0..) |db_col, db_col_idx| {
                                    if (col_mappings[db_col_idx]) |csv_idx| {
                                        const trimmed = fields[csv_idx];
                                        const field_dup = try self.allocator.dupe(u8, trimmed);
                                        errdefer self.allocator.free(field_dup);

                                        try row_obj.put(self.allocator, db_col.name, query_iter.Cell{ .text = field_dup });
                                        if (pk_col_index) |pk_idx| {
                                            if (pk_idx == db_col_idx) pk_val = field_dup;
                                        }
                                    } else {
                                        try row_obj.put(self.allocator, db_col.name, query_iter.Cell{ .text = try self.allocator.dupe(u8, "NULL") });
                                    }
                                }

                                var owned_pk = false;
                                const final_pk = if (pk_val) |pk| pk else blk: {
                                    owned_pk = true;
                                    const row_id = Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds();
                                    break :blk try std.fmt.allocPrint(self.allocator, "{d}", .{row_id});
                                };
                                defer if (owned_pk) self.allocator.free(final_pk);

                                try self.writeNewVersion(table_tree, tbl_name, final_pk, row_obj, false);
                                rows_imported += 1;

                                var i: usize = 0;
                                while (i < row_obj.entries.len) : (i += 1) {
                                    self.allocator.free(row_obj.entries.items(.value)[i].text);
                                }
                            }
                            return QueryResponse{ .rows_affected = rows_imported };
                        } else {
                            return error.UnsupportedImportFormat;
                        }
                    },
                    .manifest => |manifest_path| {
                        var file = try Io.Dir.openFile(.cwd(), self.db.pool.pager.io, manifest_path, .{ .mode = .read_only });
                        defer file.close(self.db.pool.pager.io);

                        const stat = try file.stat(self.db.pool.pager.io);
                        const buffer = try self.allocator.alloc(u8, stat.size);
                        defer self.allocator.free(buffer);
                        _ = try file.readPositionalAll(self.db.pool.pager.io, buffer, 0);

                        var yaml = Yaml{ .source = buffer };
                        try yaml.load(self.allocator);
                        defer yaml.deinit(self.allocator);

                        var arena = std.heap.ArenaAllocator.init(self.allocator);
                        defer arena.deinit();

                        const ManifestField = struct {
                            name: []const u8,
                            type: []const u8,
                        };
                        const ManifestEntity = struct {
                            name: []const u8,
                            role: []const u8,
                            file: []const u8,
                            fields: []const ManifestField = &.{},
                        };
                        const ImportManifest = struct {
                            store: []const u8,
                            format: []const u8,
                            output_dir: []const u8 = "",
                            entities: []const ManifestEntity = &.{},
                        };

                        const manifest = try yaml.parse(arena.allocator(), ImportManifest);

                        var total_rows: u64 = 0;
                        for (manifest.entities) |ent| {
                            const full_path = if (manifest.output_dir.len > 0)
                                try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ manifest.output_dir, ent.file })
                            else
                                try self.allocator.dupe(u8, ent.file);
                            defer self.allocator.free(full_path);

                            const is_csv = std.mem.eql(u8, manifest.format, "csv");
                            const format_kind: ast.ExportFormatKind = if (is_csv) .CSV else .JSON;

                            const sub_imp = ast.ImportStmt{
                                .target = .{ .table = ent.name },
                                .format = format_kind,
                                .file_path = full_path,
                            };
                            const response = try self.executeStatementInternal(.{ .import_stmt = sub_imp });
                            total_rows += response.rows_affected;
                        }
                        return QueryResponse{ .rows_affected = total_rows };
                    },
                    else => return error.UnsupportedImportTarget,
                }
            },
            .update => |upd_in| {
                var sq_arena_u = std.heap.ArenaAllocator.init(self.allocator);
                defer sq_arena_u.deinit();
                var upd = upd_in;
                upd.where_expr = try self.materializeSubqueries(upd_in.where_expr, sq_arena_u.allocator());
                self.ssiTrackRead(upd.table_name);
                self.ssiTrackWrite(upd.table_name);

                const table_meta = for (self.db.catalog.tables.items) |tbl| {
                    if (std.mem.eql(u8, tbl.name, upd.table_name)) break tbl;
                } else return error.TableNotFound;

                const table_tree = try self.db.getTableTree(upd.table_name);

                var pk_col_name: ?[]const u8 = null;
                for (table_meta.columns) |col| {
                    if (col.is_primary_key) {
                        pk_col_name = col.name;
                        break;
                    }
                }

                var direct_pk_val: ?[]const u8 = null;
                if (pk_col_name) |pk_name| {
                    direct_pk_val = try self.getEqualityValueForCol(upd.where_expr, pk_name);
                }
                defer if (direct_pk_val) |val| self.allocator.free(val);

                const UpdateTask = struct {
                    key: []const u8,
                    row_obj: CatalogCellMap,
                };
                var tasks = std.ArrayList(UpdateTask).empty;
                defer {
                    for (tasks.items) |*task| {
                        self.allocator.free(task.key);
                        var map_it = task.row_obj.iterator();
                        while (map_it.next()) |entry| {
                            self.allocator.free(entry.key_ptr.*);
                            switch (entry.value_ptr.*) {
                                .text => |s| self.allocator.free(s),
                                else => {},
                            }
                        }
                        task.row_obj.deinit(self.allocator);
                    }
                    tasks.deinit(self.allocator);
                }

                const current_tx = self.current_tx_id orelse return error.NoActiveTransaction;

                if (direct_pk_val) |pk_val| {
                    const opt_val = try table_tree.search(pk_val, self.allocator);
                    if (opt_val) |val| {
                        defer self.allocator.free(val);
                        if (try self.getVisibleVersion(table_meta, val, false, current_tx)) |visible_row| {
                            defer self.freeTableRow(visible_row);

                            var row_obj: CatalogCellMap = .empty;
                            errdefer {
                                var it_err = row_obj.iterator();
                                while (it_err.next()) |entry| {
                                    self.allocator.free(entry.key_ptr.*);
                                    switch (entry.value_ptr.*) {
                                        .text => |s| self.allocator.free(s),
                                        else => {},
                                    }
                                }
                                row_obj.deinit(self.allocator);
                            }

                            for (visible_row.names, 0..) |nm, ci_| {
                                const vv: query_iter.Cell = if (try visible_row.cells[ci_].toTextAlloc(self.allocator)) |t| .{ .text = t } else .null;
                                try row_obj.put(self.allocator, try self.allocator.dupe(u8, nm), vv);
                            }

                            for (upd.assignments) |assign| {
                                const val_str = try self.updateAssignStr(assign.value, visible_row);
                                const new_key = try self.allocator.dupe(u8, assign.column);
                                errdefer self.allocator.free(new_key);
                                if (try row_obj.fetchPut(self.allocator, new_key, query_iter.Cell{ .text = val_str })) |old_entry| {
                                    self.allocator.free(new_key);
                                    switch (old_entry.value) { .text => |t| self.allocator.free(t), else => {} }
                                }
                            }

                            try self.validateForeignKeyConstraintsForUpdate(table_meta.id, visible_row, row_obj);

                            try tasks.append(self.allocator, .{
                                .key = try self.allocator.dupe(u8, pk_val),
                                .row_obj = row_obj,
                            });
                        }
                    }
                } else {
                    var it = try table_tree.iterator();
                    defer it.deinit();

                    while (try it.next()) |cell| {
                    if (try self.getVisibleVersion(table_meta, cell.value, cell.flags.value_overflow, current_tx)) |visible_row| {
                        defer self.freeTableRow(visible_row);

                        if (upd.where_expr) |we| {
                            if (!evaluateExpr(we, visible_row)) continue;
                        }

                        var row_obj: CatalogCellMap = .empty;
                        errdefer {
                            var it_err = row_obj.iterator();
                            while (it_err.next()) |entry| {
                                self.allocator.free(entry.key_ptr.*);
                                switch (entry.value_ptr.*) {
                                    .text => |s| self.allocator.free(s),
                                    else => {},
                                }
                            }
                            row_obj.deinit(self.allocator);
                        }

                        for (visible_row.names, 0..) |nm, ci_| {
                            const vv: query_iter.Cell = if (try visible_row.cells[ci_].toTextAlloc(self.allocator)) |t| .{ .text = t } else .null;
                            try row_obj.put(self.allocator, try self.allocator.dupe(u8, nm), vv);
                        }

                        for (upd.assignments) |assign| {
                            const val_str = try self.updateAssignStr(assign.value, visible_row);
                            const new_key = try self.allocator.dupe(u8, assign.column);
                            errdefer self.allocator.free(new_key);
                            if (try row_obj.fetchPut(self.allocator, new_key, query_iter.Cell{ .text = val_str })) |old_entry| {
                                self.allocator.free(new_key);
                                switch (old_entry.value) { .text => |t| self.allocator.free(t), else => {} }
                            }
                        }

                        try self.validateForeignKeyConstraintsForUpdate(table_meta.id, visible_row, row_obj);

                        try tasks.append(self.allocator, .{
                            .key = try self.allocator.dupe(u8, cell.key),
                            .row_obj = row_obj,
                        });
                    }
                }
            }

                var rows_updated: u64 = 0;
                for (tasks.items) |task| {
                    try self.writeNewVersion(table_tree, upd.table_name, task.key, task.row_obj, true);
                    rows_updated += 1;

                    for (self.db.catalog.indexes.items) |idx| {
                        if (idx.table_id == table_meta.id) {
                            const idx_tree = try self.db.getIndexTree(idx.name);

                            var list = std.ArrayList([]const u8).empty;
                            defer {
                                for (list.items) |item| self.allocator.free(item);
                                list.deinit(self.allocator);
                            }

                            for (idx.key_columns) |col| {
                                const cell_val = task.row_obj.get(col.name);
                                const raw_val = if (cell_val) |v| v.text else "NULL";
                                const str_val = try schema.types.encodeIndexValueAlloc(self.allocator, colTypeByName(table_meta, col.name), raw_val);
                                try list.append(self.allocator, str_val);
                            }

                            try list.append(self.allocator, try self.allocator.dupe(u8, task.key));

                            var key_parts = try self.allocator.alloc([]const u8, list.items.len);
                            defer self.allocator.free(key_parts);
                            for (list.items, 0..) |part, idx_j| key_parts[idx_j] = part;

                            const index_key = try std.mem.join(self.allocator, ":", key_parts);
                            defer self.allocator.free(index_key);

                            try idx_tree.insert(index_key, "");
                            try self.db.updateIndexRootPageId(idx.name, idx_tree.root_page_id, current_tx);
                        }
                    }
                }
                // A successful update re-inserts index entries for the new row
                // version and can leave the pre-update entry behind, so the
                // secondary indexes are no longer entry-exact.
                if (rows_updated > 0) try self.db.markTableIndexesInexact(table_meta.id);
                return QueryResponse{ .rows_affected = rows_updated };
            },
            .delete => |del_in| {
                var sq_arena_d = std.heap.ArenaAllocator.init(self.allocator);
                defer sq_arena_d.deinit();
                var del = del_in;
                del.where_expr = try self.materializeSubqueries(del_in.where_expr, sq_arena_d.allocator());
                self.ssiTrackRead(del.table_name);
                self.ssiTrackWrite(del.table_name);

                const table_tree = try self.db.getTableTree(del.table_name);

                const table_meta = for (self.db.catalog.tables.items) |tbl| {
                    if (std.mem.eql(u8, tbl.name, del.table_name)) break tbl;
                } else return error.TableNotFound;

                var pk_col_name: ?[]const u8 = null;
                for (table_meta.columns) |col| {
                    if (col.is_primary_key) {
                        pk_col_name = col.name;
                        break;
                    }
                }

                var direct_pk_val: ?[]const u8 = null;
                if (pk_col_name) |pk_name| {
                    direct_pk_val = try self.getEqualityValueForCol(del.where_expr, pk_name);
                }
                defer if (direct_pk_val) |val| self.allocator.free(val);

                var rows_deleted: u64 = 0;
                const current_tx = self.current_tx_id orelse return error.NoActiveTransaction;

                if (direct_pk_val) |pk_val| {
                    const opt_val = try table_tree.search(pk_val, self.allocator);
                    if (opt_val) |val| {
                        defer self.allocator.free(val);
                        if (try self.getVisibleVersion(table_meta, val, false, current_tx)) |visible_row| {
                            defer self.freeTableRow(visible_row);
                            try self.validateForeignKeyConstraintsForDelete(table_meta.id, visible_row);
                            try self.deleteRowVersion(table_tree, del.table_name, pk_val);
                            rows_deleted = 1;
                        }
                    }
                } else {
                    var it = try table_tree.iterator();
                    defer it.deinit();

                    var keys_to_delete = std.ArrayList([]const u8).empty;
                    defer {
                        for (keys_to_delete.items) |k| self.allocator.free(k);
                        keys_to_delete.deinit(self.allocator);
                    }

                    while (try it.next()) |cell| {
                        if (try self.getVisibleVersion(table_meta, cell.value, cell.flags.value_overflow, current_tx)) |visible_row| {
                            defer self.freeTableRow(visible_row);
                            if (del.where_expr) |we| {
                                if (!evaluateExpr(we, visible_row)) continue;
                            }
                            try self.validateForeignKeyConstraintsForDelete(table_meta.id, visible_row);
                            try keys_to_delete.append(self.allocator, try self.allocator.dupe(u8, cell.key));
                        }
                    }

                    for (keys_to_delete.items) |key| {
                        try self.deleteRowVersion(table_tree, del.table_name, key);
                    }
                    rows_deleted = keys_to_delete.items.len;
                }

                // A delete tombstones the base row but MVCC keeps its index entry
                // (older snapshots may still need it), so the entry is now stale:
                // the secondary indexes are no longer entry-exact.
                if (rows_deleted > 0) try self.db.markTableIndexesInexact(table_meta.id);
                return QueryResponse{ .rows_affected = rows_deleted };
            },
            .create_function => |cf| {
                // Register a wasm scalar UDF (embed-wasm.md M1). The module is given inline as
                // hex; decode it and register under the UPPER-cased name, because call sites
                // (func_call) upper-case function names, so the registry key must match.
                // (Registration mutates db.wasm_functions; a concurrent-DDL latch is a
                // follow-up, as with the source-byte persistence.)
                const hex = cf.wasm_hex;
                if (hex.len % 2 != 0)
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "CREATE FUNCTION: odd-length hex module") };
                const bytes = try self.allocator.alloc(u8, hex.len / 2);
                defer self.allocator.free(bytes);
                _ = std.fmt.hexToBytes(bytes, hex) catch
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "CREATE FUNCTION: invalid hex module") };
                const upper = try std.ascii.allocUpperString(self.allocator, cf.name);
                defer self.allocator.free(upper);
                self.db.wasm_functions.register(upper, bytes, .{}) catch
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "CREATE FUNCTION: module failed to decode or validate") };
                return QueryResponse{ .rows_affected = 1 };
            },
            .drop_function => |df| {
                const upper = try std.ascii.allocUpperString(self.allocator, df.name);
                defer self.allocator.free(upper);
                _ = self.db.wasm_functions.drop(upper);
                return QueryResponse{ .rows_affected = 1 };
            },
            .drop_table => |dt| {
                try self.db.dropTable(dt.table_name, self.current_tx_id.?);
                return QueryResponse{ .rows_affected = 1 };
            },
            .drop_index => |di| {
                try self.db.dropIndex(di.index_name, di.table_name, self.current_tx_id.?);
                return QueryResponse{ .rows_affected = 1 };
            },
            .begin => {
                if (self.current_tx_id != null) {
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Transaction already active") };
                }
                const tx = try self.db.txn_manager.begin(self.db.pool.pager.io);
                self.current_tx_id = tx;
                self.ensureLiveSub();
                self.clearSavepoints(false);
                self.clearSnapshot();
                if (self.isolation_level != .read_committed) {
                    self.snapshot = try self.db.txn_manager.captureSnapshot(self.db.pool.pager.io, self.allocator);
                }
                if (self.isolation_level == .serializable) {
                    try self.db.txn_manager.ssiBegin(self.db.pool.pager.io, tx);
                }
                try self.logWalRecord(.begin, "", "", "");
                return QueryResponse{ .rows_affected = 0 };
            },
            .commit => {
                if (self.current_tx_id) |tx| {
                    if (self.isolation_level == .serializable and self.db.txn_manager.ssiIsPivot(self.db.pool.pager.io, tx)) {
                        self.db.txn_manager.ssiEnd(self.db.pool.pager.io, tx);
                        self.logWalRecord(.rollback, "", "", "") catch {};
                        self.clearSavepoints(true);
                        self.db.txn_manager.abort(self.db.pool.pager.io, tx) catch {};
                        self.current_tx_id = null;
                        self.clearSnapshot();
                        self.durableFinish(false) catch {};
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "could not serialize access due to read/write dependencies among transactions") };
                    }
                    if (self.isolation_level == .serializable) self.db.txn_manager.ssiEnd(self.db.pool.pager.io, tx);
                    self.logWalRecord(.commit, "", "", "") catch {
                        self.db.txn_manager.abort(self.db.pool.pager.io, tx) catch {};
                        self.current_tx_id = null;
                        self.clearSnapshot();
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Commit failed: could not write the WAL commit record; transaction aborted") };
                    };
                    try self.db.txn_manager.commit(self.db.pool.pager.io, tx);
                    if (self.live_sub_init) {
                        var it = self.live_sub.keyIterator();
                        while (it.next()) |k| self.db.txn_manager.commit(self.db.pool.pager.io, k.*) catch {};
                    }
                    self.clearSavepoints(false);
                    self.current_tx_id = null;
                    self.clearSnapshot();
                    self.durableFinish(true) catch {
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Durable commit failed: replication quorum not reached within timeout") };
                    };
                    return QueryResponse{ .rows_affected = 0 };
                } else {
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "No active transaction to commit") };
                }
            },
            .rollback => |rb| {
                if (self.current_tx_id == null) {
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "No active transaction to rollback") };
                }
                const io = self.db.pool.pager.io;
                if (rb.to_savepoint) |name| {
                    var found: ?usize = null;
                    for (self.savepoints.items, 0..) |sp, i| {
                        if (std.mem.eql(u8, sp.name, name)) found = i;
                    }
                    const idx = found orelse return QueryResponse{ .error_message = try self.allocator.dupe(u8, "SAVEPOINT not found") };
                    var j: usize = self.savepoints.items.len;
                    while (j > idx + 1) {
                        j -= 1;
                        const sp = self.savepoints.items[j];
                        self.db.txn_manager.abort(io, sp.xid) catch {};
                        _ = self.live_sub.remove(sp.xid);
                        self.allocator.free(sp.name);
                        _ = self.savepoints.pop();
                    }
                    const old = self.savepoints.items[idx];
                    self.db.txn_manager.abort(io, old.xid) catch {};
                    _ = self.live_sub.remove(old.xid);
                    const fresh = try self.db.txn_manager.begin(io);
                    self.savepoints.items[idx].xid = fresh;
                    try self.live_sub.put(fresh, {});
                    return QueryResponse{ .rows_affected = 0 };
                }
                if (self.isolation_level == .serializable) self.db.txn_manager.ssiEnd(io, self.current_tx_id.?);
                try self.logWalRecord(.rollback, "", "", "");
                self.clearSavepoints(true);
                try self.db.txn_manager.abort(io, self.current_tx_id.?);
                self.current_tx_id = null;
                self.clearSnapshot();
                self.durableFinish(false) catch {};
                return QueryResponse{ .error_message = null, .rows_affected = 0 };
            },
            .savepoint => |sp| {
                if (self.current_tx_id == null) {
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "SAVEPOINT outside a transaction") };
                }
                self.ensureLiveSub();
                const io = self.db.pool.pager.io;
                const xid = try self.db.txn_manager.begin(io);
                const name_copy = try self.allocator.dupe(u8, sp.name);
                errdefer self.allocator.free(name_copy);
                try self.savepoints.append(self.allocator, .{ .name = name_copy, .xid = xid });
                try self.live_sub.put(xid, {});
                return QueryResponse{ .rows_affected = 0 };
            },
            .release_savepoint => |rs| {
                if (self.current_tx_id == null) {
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "RELEASE outside a transaction") };
                }
                var found: ?usize = null;
                for (self.savepoints.items, 0..) |sp2, i| {
                    if (std.mem.eql(u8, sp2.name, rs.name)) found = i;
                }
                const idx = found orelse return QueryResponse{ .error_message = try self.allocator.dupe(u8, "SAVEPOINT not found") };
                var j: usize = self.savepoints.items.len;
                while (j > idx) {
                    j -= 1;
                    self.allocator.free(self.savepoints.items[j].name);
                    _ = self.savepoints.pop();
                }
                return QueryResponse{ .rows_affected = 0 };
            },
            .create_user => |cu| {
                const tx_id = self.current_tx_id orelse 1;

                var salt: [32]u8 = undefined;
                std.Io.random(self.db.pool.pager.io, &salt);

                const hash = try self.db.security_manager.hashKey(cu.password, salt);
                const hex_hash = try hexEncode(self.allocator, &hash);
                defer self.allocator.free(hex_hash);
                const hex_salt = try hexEncode(self.allocator, &salt);
                defer self.allocator.free(hex_salt);
                const password_hash = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ hex_hash, hex_salt });
                defer self.allocator.free(password_hash);

                try self.db.registerUser(cu.username, password_hash, cu.role, tx_id);
                try self.db.security_manager.loadUsers(self.db);

                return QueryResponse{ .rows_affected = 1 };
            },
            .drop_user => |du| {
                const tx_id = self.current_tx_id orelse 1;
                try self.db.unregisterUser(du.username, tx_id);
                try self.db.security_manager.loadUsers(self.db);

                return QueryResponse{ .rows_affected = 1 };
            },
            .alter_user => |au| {
                const tx_id = self.current_tx_id orelse 1;

                var salt: [32]u8 = undefined;
                std.Io.random(self.db.pool.pager.io, &salt);

                const hash = try self.db.security_manager.hashKey(au.password, salt);
                const hex_hash = try hexEncode(self.allocator, &hash);
                defer self.allocator.free(hex_hash);
                const hex_salt = try hexEncode(self.allocator, &salt);
                defer self.allocator.free(hex_salt);
                const password_hash = try std.fmt.allocPrint(self.allocator, "{s}:{s}", .{ hex_hash, hex_salt });
                defer self.allocator.free(password_hash);

                self.db.updateUserPassword(au.username, password_hash, tx_id) catch |err| {
                    if (err == error.UserNotFound) {
                        return QueryResponse{ .error_message = try std.fmt.allocPrint(self.allocator, "user '{s}' does not exist", .{au.username}) };
                    }
                    return err;
                };
                try self.db.security_manager.loadUsers(self.db);

                return QueryResponse{ .rows_affected = 1 };
            },
            .login => |lg| {
                const session = self.db.security_manager.authenticate(lg.username, lg.password, null) catch |err| {
                    if (err == error.AccountLockedOut) {
                        return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Account locked out due to multiple failed login attempts") };
                    }
                    return QueryResponse{ .error_message = try self.allocator.dupe(u8, "Invalid username or password") };
                };

                const token_hex = try hexEncode(self.allocator, &session.token);

                var cols = try self.allocator.alloc([]const u8, 1);
                cols[0] = try self.allocator.dupe(u8, "session_token");

                var row_cells = try self.allocator.alloc([]const u8, 1);
                row_cells[0] = token_hex;

                var rows = try self.allocator.alloc([]const []const u8, 1);
                rows[0] = row_cells;

                return QueryResponse{
                    .columns = cols,
                    .rows = rows,
                    .rows_affected = 1,
                };
            },
            .alter_table => |at| {
                const tx_id = self.current_tx_id orelse 1;

                var table_index: ?usize = null;
                for (self.db.catalog.tables.items, 0..) |tbl, i| {
                    if (std.mem.eql(u8, tbl.name, at.table_name)) {
                        table_index = i;
                        break;
                    }
                }
                const idx_found = table_index orelse return error.TableNotFound;
                var existing_table = self.db.catalog.tables.items[idx_found];

                switch (at.action) {
                    .add_column => |new_col| {
                        if (existing_table.getColumn(new_col.name) != null) {
                            return error.DuplicateColumn;
                        }

                        var new_cols = try self.allocator.alloc(Column, existing_table.columns.len + 1);
                        errdefer {
                            for (new_cols) |*col| col.deinit(self.allocator);
                            self.allocator.free(new_cols);
                        }

                        for (existing_table.columns, 0..) |col, i| {
                            new_cols[i] = Column{
                                .name = try self.allocator.dupe(u8, col.name),
                                .type = col.type,
                                .size = col.size,
                                .offset = col.offset,
                                .is_primary_key = col.is_primary_key,
                                .is_auto_increment = col.is_auto_increment,
                                .is_nullable = col.is_nullable,
                                .default_value = if (col.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                            };
                        }

                        const col_type = mapSqlType(new_col.type_name);

                        const size: u16 = switch (col_type) {
                            .BOOL => 1,
                            .UINT32, .INT32, .FLOAT32 => 4,
                            .UINT64, .INT64, .FLOAT64, .TIMESTAMP => 8,
                            .TEXT, .BLOB => 4,
                        };

                        new_cols[existing_table.columns.len] = Column{
                            .name = try self.allocator.dupe(u8, new_col.name),
                            .type = col_type,
                            .size = size,
                            .offset = existing_table.fixed_size,
                            .is_primary_key = new_col.is_primary_key,
                            .is_auto_increment = false,
                            .is_nullable = new_col.is_nullable,
                            .default_value = if (new_col.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                        };

                        const root_page_id = self.db.table_roots.get(existing_table.name).?;
                        const new_table = try Table.init(self.allocator, existing_table.id, existing_table.name, new_cols);

                        self.db.catalog.tables.items[idx_found] = new_table;
                        existing_table.deinit();

                        var cols_meta = try self.allocator.alloc(schema.ColumnMetadata, new_table.columns.len);
                        defer {
                            for (cols_meta) |*cm| {
                                self.allocator.free(cm.name);
                                if (cm.default_value) |dv| self.allocator.free(dv);
                            }
                            self.allocator.free(cols_meta);
                        }

                        for (new_table.columns, 0..) |col, i| {
                            cols_meta[i] = schema.ColumnMetadata{
                                .name = try self.allocator.dupe(u8, col.name),
                                .type = col.type,
                                .size = col.size,
                                .offset = col.offset,
                                .is_primary_key = col.is_primary_key,
                                .is_auto_increment = col.is_auto_increment,
                                .is_nullable = col.is_nullable,
                                .default_value = if (col.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                            };
                        }

                        const meta = schema.TableMetadata{
                            .id = new_table.id,
                            .name = new_table.name,
                            .columns = cols_meta,
                            .root_page_id = root_page_id,
                        };
                        const meta_bytes = try meta.serialize(self.allocator);
                        defer self.allocator.free(meta_bytes);

                        const tables_root = self.db.table_roots.get("sys.tables") orelse return error.SystemTableNotFound;
                        var tables_tree = try BPlusTree.init(self.db.pool, tables_root, self.allocator);
                        defer tables_tree.deinit();

                        const lsn = self.db.reserveLsn();
                        try tables_tree.delete(new_table.name);
                        try tables_tree.insert(new_table.name, meta_bytes);

                        if (self.db.wal) |wal| {
                            try wal.append(.{
                                .lsn = lsn,
                                .tx_id = tx_id,
                                .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                                .kind = .update,
                                .table_name = "sys.tables",
                                .key = new_table.name,
                                .value = meta_bytes,
                            });
                        }
                    },
                    .rename_table => |new_name| {
                        for (self.db.catalog.tables.items) |tbl| {
                            if (std.mem.eql(u8, tbl.name, new_name)) return error.TableAlreadyExists;
                        }

                        const root_page_id = self.db.table_roots.get(existing_table.name).?;

                        const kv = self.db.table_roots.getEntry(existing_table.name).?;
                        const old_key = kv.key_ptr.*;
                        _ = self.db.table_roots.remove(existing_table.name);
                        self.db.allocator.free(old_key);

                        const new_key = try self.db.allocator.dupe(u8, new_name);
                        try self.db.table_roots.put(new_key, root_page_id);

                        if (self.db.table_trees.fetchRemove(existing_table.name)) |entry| {
                            self.db.allocator.free(entry.key);
                            try self.db.table_trees.put(try self.db.allocator.dupe(u8, new_name), entry.value);
                        }

                        const old_name_allocated = existing_table.name;
                        self.db.catalog.tables.items[idx_found].name = try self.db.allocator.dupe(u8, new_name);
                        self.db.allocator.free(old_name_allocated);

                        const updated_table = self.db.catalog.tables.items[idx_found];

                        const lsn1 = self.db.reserveLsn();
                        const lsn2 = self.db.reserveLsn();
                        const lsn3 = self.db.reserveLsn();
                        const lsn4 = self.db.reserveLsn();
                        try self.db.master_tree.delete(at.table_name);

                        const obj = schema.ObjectMetadata{
                            .id = updated_table.id,
                            .name = updated_table.name,
                            .type = "TABLE",
                            .root_page_id = root_page_id,
                        };
                        const obj_bytes = try obj.serialize(self.allocator);
                        defer self.allocator.free(obj_bytes);
                        try self.db.master_tree.insert(updated_table.name, obj_bytes);

                        const tables_root = self.db.table_roots.get("sys.tables") orelse return error.SystemTableNotFound;
                        var tables_tree = try BPlusTree.init(self.db.pool, tables_root, self.allocator);
                        defer tables_tree.deinit();

                        try tables_tree.delete(at.table_name);

                        var cols_meta = try self.allocator.alloc(schema.ColumnMetadata, updated_table.columns.len);
                        defer {
                            for (cols_meta) |*cm| {
                                self.allocator.free(cm.name);
                                if (cm.default_value) |dv| self.allocator.free(dv);
                            }
                            self.allocator.free(cols_meta);
                        }

                        for (updated_table.columns, 0..) |col, i| {
                            cols_meta[i] = schema.ColumnMetadata{
                                .name = try self.allocator.dupe(u8, col.name),
                                .type = col.type,
                                .size = col.size,
                                .offset = col.offset,
                                .is_primary_key = col.is_primary_key,
                                .is_auto_increment = col.is_auto_increment,
                                .is_nullable = col.is_nullable,
                                .default_value = if (col.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                            };
                        }

                        const meta = schema.TableMetadata{
                            .id = updated_table.id,
                            .name = updated_table.name,
                            .columns = cols_meta,
                            .root_page_id = root_page_id,
                        };
                        const meta_bytes = try meta.serialize(self.allocator);
                        defer self.allocator.free(meta_bytes);
                        try tables_tree.insert(updated_table.name, meta_bytes);

                        if (self.db.wal) |wal| {
                            try wal.append(.{
                                .lsn = lsn1,
                                .tx_id = tx_id,
                                .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                                .kind = .delete,
                                .table_name = "sys.objects",
                                .key = at.table_name,
                                .value = &.{},
                            });

                            try wal.append(.{
                                .lsn = lsn2,
                                .tx_id = tx_id,
                                .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                                .kind = .insert,
                                .table_name = "sys.objects",
                                .key = updated_table.name,
                                .value = obj_bytes,
                            });

                            try wal.append(.{
                                .lsn = lsn3,
                                .tx_id = tx_id,
                                .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                                .kind = .delete,
                                .table_name = "sys.tables",
                                .key = at.table_name,
                                .value = &.{},
                            });

                            try wal.append(.{
                                .lsn = lsn4,
                                .tx_id = tx_id,
                                .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                                .kind = .insert,
                                .table_name = "sys.tables",
                                .key = updated_table.name,
                                .value = meta_bytes,
                            });
                        }
                    },
                }

                return QueryResponse{ .rows_affected = 1 };
            },
            .analyze_table => |at| {
                const stats = try @import("stats.zig").analyzeTable(self.db, self, at.table_name);
                
                var columns = try self.allocator.alloc([]const u8, 2);
                columns[0] = try self.allocator.dupe(u8, "row_count");
                columns[1] = try self.allocator.dupe(u8, "page_count");

                var rows = try self.allocator.alloc([]const []const u8, 1);
                var row = try self.allocator.alloc([]const u8, 2);
                row[0] = try std.fmt.allocPrint(self.allocator, "{d}", .{stats.row_count});
                row[1] = try std.fmt.allocPrint(self.allocator, "{d}", .{stats.page_count});
                rows[0] = row;

                return QueryResponse{
                    .columns = columns,
                    .rows = rows,
                };
            },
            .backup => |bk| {
                try self.backupDatabase(bk.backup_path);
                return QueryResponse{ .rows_affected = 1 };
            },
            .create_role => |cr| {
                const roles_table = self.db.catalog.getTable("sys.roles") orelse return error.SystemTableNotFound;
                const roles_root = self.db.table_roots.get("sys.roles") orelse return error.SystemTableNotFound;
                var tree = try BPlusTree.init(self.db.pool, roles_root, self.allocator);
                defer tree.deinit();

                const fixed_buf = try self.allocator.alloc(u8, roles_table.fixed_size);
                defer self.allocator.free(fixed_buf);
                @memset(fixed_buf, 0);

                const heap_capacity = cr.role_name.len + 32;
                const heap_buf = try self.allocator.alloc(u8, heap_capacity);
                defer self.allocator.free(heap_buf);
                var heap_offset: u32 = 0;

                var builder = RowBuilder.init(roles_table, fixed_buf, heap_buf, &heap_offset);
                try builder.writeDynamic("role_name", cr.role_name);

                var versions_list = std.ArrayList(schema.DecodedVersion).empty;
                defer {
                    for (versions_list.items) |*v| v.deinit(self.allocator);
                    versions_list.deinit(self.allocator);
                }

                const new_fixed = try self.allocator.dupe(u8, fixed_buf);
                errdefer self.allocator.free(new_fixed);
                const new_heap = try self.allocator.dupe(u8, heap_buf[0..heap_offset]);

                try versions_list.append(self.allocator, schema.DecodedVersion{
                    .xmin = self.current_tx_id.?,
                    .xmax = 0,
                    .fixed = new_fixed,
                    .heap = new_heap,
                });

                const packed_bytes = try schema.packVersions(self.allocator, versions_list.items);
                defer self.allocator.free(packed_bytes);

                const lsn = self.db.reserveLsn();
                _ = try tree.insert(cr.role_name, packed_bytes);
                try self.db.updateTableRootPageId("sys.roles", tree.root_page_id, self.current_tx_id.?);

                if (self.db.wal) |wal| {
                    try wal.append(.{
                        .lsn = lsn,
                        .tx_id = self.current_tx_id.?,
                        .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                        .kind = .insert,
                        .table_name = "sys.roles",
                        .key = cr.role_name,
                        .value = packed_bytes,
                    });
                }

                try self.db.security_manager.loadRolesAndPrivileges(self.db);
                return QueryResponse{ .rows_affected = 1 };
            },
            .grant => |g| {
                const privs_table = self.db.catalog.getTable("sys.privileges") orelse return error.SystemTableNotFound;
                const privs_root = self.db.table_roots.get("sys.privileges") orelse return error.SystemTableNotFound;
                var tree = try BPlusTree.init(self.db.pool, privs_root, self.allocator);
                defer tree.deinit();

                const obj_name = g.object orelse "";
                const grant_id = try std.fmt.allocPrint(self.allocator, "{s}:{s}:{s}", .{ g.to_principal, obj_name, g.privilege });
                defer self.allocator.free(grant_id);

                const fixed_buf = try self.allocator.alloc(u8, privs_table.fixed_size);
                defer self.allocator.free(fixed_buf);
                @memset(fixed_buf, 0);

                const heap_capacity = grant_id.len + g.to_principal.len + obj_name.len + g.privilege.len + 32;
                const heap_buf = try self.allocator.alloc(u8, heap_capacity);
                defer self.allocator.free(heap_buf);
                var heap_offset: u32 = 0;

                var builder = RowBuilder.init(privs_table, fixed_buf, heap_buf, &heap_offset);
                try builder.writeDynamic("id", grant_id);
                try builder.writeDynamic("grantee", g.to_principal);
                try builder.writeDynamic("object_name", obj_name);
                try builder.writeDynamic("privilege", g.privilege);

                var versions_list = std.ArrayList(schema.DecodedVersion).empty;
                defer {
                    for (versions_list.items) |*v| v.deinit(self.allocator);
                    versions_list.deinit(self.allocator);
                }

                const new_fixed = try self.allocator.dupe(u8, fixed_buf);
                errdefer self.allocator.free(new_fixed);
                const new_heap = try self.allocator.dupe(u8, heap_buf[0..heap_offset]);

                try versions_list.append(self.allocator, schema.DecodedVersion{
                    .xmin = self.current_tx_id.?,
                    .xmax = 0,
                    .fixed = new_fixed,
                    .heap = new_heap,
                });

                const packed_bytes = try schema.packVersions(self.allocator, versions_list.items);
                defer self.allocator.free(packed_bytes);

                const lsn = self.db.reserveLsn();
                _ = try tree.insert(grant_id, packed_bytes);
                try self.db.updateTableRootPageId("sys.privileges", tree.root_page_id, self.current_tx_id.?);

                if (self.db.wal) |wal| {
                    try wal.append(.{
                        .lsn = lsn,
                        .tx_id = self.current_tx_id.?,
                        .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                        .kind = .insert,
                        .table_name = "sys.privileges",
                        .key = grant_id,
                        .value = packed_bytes,
                    });
                }

                try self.db.security_manager.loadRolesAndPrivileges(self.db);
                return QueryResponse{ .rows_affected = 1 };
            },
            .revoke => |rev| {
                const privs_root = self.db.table_roots.get("sys.privileges") orelse return error.SystemTableNotFound;
                var tree = try BPlusTree.init(self.db.pool, privs_root, self.allocator);
                defer tree.deinit();

                const obj_name = rev.object orelse "";
                const grant_id = try std.fmt.allocPrint(self.allocator, "{s}:{s}:{s}", .{ rev.from_principal, obj_name, rev.privilege });
                defer self.allocator.free(grant_id);

                const lsn = self.db.reserveLsn();
                _ = try tree.delete(grant_id);

                if (self.db.wal) |wal| {
                    try wal.append(.{
                        .lsn = lsn,
                        .tx_id = self.current_tx_id.?,
                        .timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds(),
                        .kind = .delete,
                        .table_name = "sys.privileges",
                        .key = grant_id,
                        .value = "",
                    });
                }

                try self.db.security_manager.loadRolesAndPrivileges(self.db);
                return QueryResponse{ .rows_affected = 1 };
            },
            else => return error.UnsupportedStatement,
        }
    }

    /// Evaluates a WHERE predicate against a single decoded row (JSON object).
    ///
    /// Thin adapter over `query_iter.evalExprJson`; used by the UPDATE/DELETE
    /// scan paths to decide whether a row matches. The predicate is expected to
    /// be boolean; non-matching or unresolvable predicates yield false.
    fn evaluateExpr(expr: *const ast.Expr, row_data: query_iter.TableRow) bool {
        return query_iter.evalExprJson(expr, row_data);
    }

    /// Folds one row's value into an aggregate accumulator.
    ///
    /// COUNT increments per non-NULL value (or per row for COUNT(*)), honouring
    /// DISTINCT via a lazily-built set. SUM/MIN/MAX/AVG maintain both the numeric
    /// track (parsing the value as i64; clearing `all_numeric` on failure) and
    /// the string track (lexicographic min/max), so MIN/MAX still work on text
    /// columns. `ga` is the group arena that owns any duped strings. Reads the
    /// column value out of the streaming [`query_iter.Row`].
    fn foldOneAgg(self: *QueryExecutor, acc: *AggAcc, agg: ast.AggregateCall, row: query_iter.Row, ga: std.mem.Allocator) !void {
        _ = self;
        if (agg.kind == .COUNT) {
            switch (agg.argument) {
                .column => |col| {
                    const v = query_iter.getVal(&ast.Expr{ .column_ref = col }, row);
                    if (v) |vv| {
                        if (vv != .null) {
                            if (agg.distinct) {
                                const sval = try scalarTextInto(ga, vv);
                                if (acc.distinct_seen == null) acc.distinct_seen = std.StringHashMap(void).init(ga);
                                const dop = try acc.distinct_seen.?.getOrPut(try ga.dupe(u8, sval));
                                if (!dop.found_existing) acc.count += 1;
                            } else {
                                acc.count += 1;
                            }
                        }
                    }
                },
                else => acc.count += 1,
            }
        } else {
            const col = switch (agg.argument) {
                .column => |c| c,
                else => "",
            };
            const v = query_iter.getVal(&ast.Expr{ .column_ref = col }, row);
            if (v) |vv| {
                if (vv != .null) {
                    acc.saw_value = true;
                    const s = try scalarTextInto(ga, vv);
                    // Parse as f64 so both integer and DOUBLE/decimal columns
                    // accumulate correctly (parseInt failed on "1.5").
                    if (std.fmt.parseFloat(f64, s)) |n| {
                        acc.sum += n;
                        acc.num_seen += 1;
                        if (n != @trunc(n)) acc.all_int = false;
                        if (!acc.have_num) {
                            acc.min_num = n;
                            acc.max_num = n;
                            acc.have_num = true;
                        } else {
                            if (n < acc.min_num) acc.min_num = n;
                            if (n > acc.max_num) acc.max_num = n;
                        }
                    } else |_| {
                        acc.all_numeric = false;
                    }
                    if (acc.min_str == null or std.mem.lessThan(u8, s, acc.min_str.?)) acc.min_str = try ga.dupe(u8, s);
                    if (acc.max_str == null or std.mem.lessThan(u8, acc.max_str.?, s)) acc.max_str = try ga.dupe(u8, s);
                }
            }
        }
    }

    /// Evaluates an UPDATE `SET col = expr` right-hand side against the current
    /// row and returns the new value as an owned string.
    ///
    /// The expression may reference other columns of the same row (e.g.
    /// `qty = qty + 1`); it is evaluated by `query_iter.evalScalarJson` and the
    /// numeric/text result is stringified. Returns `error.UnsupportedUpdateValue`
    /// when the expression cannot be reduced to a scalar. Caller owns the string.
    fn updateAssignStr(self: *QueryExecutor, value: ast.Expr, row: query_iter.TableRow) ![]const u8 {
        const computed = query_iter.evalScalarJson(&value, row) orelse return error.UnsupportedUpdateValue;
        return switch (computed) {
            .integer => |v| try std.fmt.allocPrint(self.allocator, "{d}", .{v}),
            .float => |v| try std.fmt.allocPrint(self.allocator, "{d}", .{v}),
            .string => |v| try self.allocator.dupe(u8, v),
            else => error.UnsupportedUpdateValue,
        };
    }

    /// Decodes a 24-character hex string into 12 raw bytes.
    ///
    /// Returns `error.InvalidHexLength` if the input is not exactly 24 chars.
    /// Sized for a 12-byte identifier (e.g. an object id); a helper kept here
    /// for value parsing.
    fn parseHex24(hex: []const u8) ![12]u8 {
        if (hex.len != 24) return error.InvalidHexLength;
        var bytes: [12]u8 = undefined;
        for (0..12) |i| {
            bytes[i] = try std.fmt.parseInt(u8, hex[i * 2 .. i * 2 + 2], 16);
        }
        return bytes;
    }

    /// Splits one CSV line into trimmed fields, writing into `out_cols`.
    ///
    /// A minimal comma splitter (no quoting/escaping) that trims surrounding
    /// spaces, tabs and carriage returns from each field and stops once
    /// `out_cols` is full. Returns the number of fields written. The slices
    /// borrow into `line`. Used by the CSV IMPORT path.
    fn parseCsvLine(line: []const u8, out_cols: [][]const u8) usize {
        var it = std.mem.splitScalar(u8, line, ',');
        var i: usize = 0;
        while (it.next()) |field| {
            if (i < out_cols.len) {
                out_cols[i] = std.mem.trim(u8, field, " \t\r");
                i += 1;
            } else {
                break;
            }
        }
        return i;
    }

    /// Reads a single newline-terminated line from a reader into `buf`.
    ///
    /// Strips a trailing `\r` (handling CRLF), returns null at clean
    /// end-of-stream with nothing buffered, and returns the final partial line
    /// if the stream ends without a newline. Returns `error.LineTooLong` if the
    /// line does not fit in `buf`. The returned slice borrows `buf`. Used to
    /// stream CSV import files without loading them whole.
    fn readLineFromIoReader(reader: *std.Io.Reader, buf: []u8) !?[]const u8 {
        var index: usize = 0;
        while (index < buf.len) {
            const b = reader.peekByte() catch |err| switch (err) {
                error.EndOfStream => {
                    if (index > 0) return buf[0..index];
                    return null;
                },
                else => return err,
            };
            reader.seek += 1;
            if (b == '\n') {
                var len = index;
                if (len > 0 and buf[len - 1] == '\r') {
                    len -= 1;
                }
                return buf[0..len];
            }
            buf[index] = b;
            index += 1;
        }
        return error.LineTooLong;
    }

    /// Decodes a stored cell into the single row version visible to
    /// `current_tx`, or null if none is.
    ///
    /// Handles the two on-disk encodings kaidb supports:
    /// - the legacy JSON encoding, where the cell is a JSON object carrying a
    ///   `versions` array of `{xmin, xmax, data}`; the first version passing
    ///   [`rowVisible`] is cloned out;
    /// - the packed binary encoding, reconstructed via
    ///   `db.reconstructVersionChain` into fixed/heap version records, from which
    ///   the visible one is rebuilt into a JSON object using [`RowReader`].
    /// When `value_overflow` is set the cell is an overflow descriptor and the
    /// real bytes are first read back from the overflow page chain. The returned
    /// [`std.json.Value`] is owned by the caller and must be released with
    /// [`freeJsonValue`]. This is the read-side counterpart to
    /// [`writeNewVersion`].
    pub fn getVisibleVersion(self: *QueryExecutor, table: Table, bytes: []const u8, value_overflow: bool, current_tx: u64) !?query_iter.TableRow {
        var actual_bytes = bytes;
        var allocated_bytes: ?[]const u8 = null;
        defer if (allocated_bytes) |ab| self.allocator.free(ab);

        if (value_overflow) {
            const desc = @import("../storage/overflow.zig").OverflowDescriptor.decode(bytes);
            allocated_bytes = try @import("../storage/overflow.zig").readChain(self.db.pool, self.allocator, desc.first_page_id, desc.total_len);
            actual_bytes = allocated_bytes.?;
        }

        if (actual_bytes.len > 0 and actual_bytes[0] == '{') {
            var parsed = try std.json.parseFromSlice(std.json.Value, self.allocator, actual_bytes, .{});
            defer parsed.deinit();

            const obj = parsed.value;
            if (obj != .object) {
                return try self.tableRowFromObject(table, obj);
            }
            const versions_val = obj.object.get("versions") orelse return try self.tableRowFromObject(table, obj);
            if (versions_val != .array) return try self.tableRowFromObject(table, obj);

            for (versions_val.array.items) |ver| {
                if (ver != .object) continue;
                const xmin_val = ver.object.get("xmin") orelse continue;
                const xmax_val = ver.object.get("xmax") orelse continue;
                const data_val = ver.object.get("data") orelse continue;

                const xmin = switch (xmin_val) {
                    .integer => |i| @as(u64, @intCast(i)),
                    else => continue,
                };
                const xmax = switch (xmax_val) {
                    .integer => |i| @as(u64, @intCast(i)),
                    else => continue,
                };

                if (self.rowVisible(current_tx, xmin, xmax)) {
                    return try self.tableRowFromObject(table, data_val);
                }
            }
            return null;
        }

        // Hot-path shortcut for the overwhelmingly common read: a single committed
        // version whose `xmin` predates every active transaction is visible to all
        // readers, so it can be decoded straight from the page bytes. This mirrors
        // the identical guard in [`Database.reconstructVersionChain`] but avoids the
        // per-read `DecodedVersion` array and the two `fixed`/`heap` dupes (the
        // `RowReader` copies only the columns it emits). `actual_bytes` stays valid
        // for this call, so borrowing slices into it is safe. Anything else (a
        // version created by a still-active txn, an undo chain, the legacy layout)
        // falls through to the full reconstruction below, unchanged.
        if (actual_bytes.len >= 36) {
            const xmin = std.mem.readInt(u64, actual_bytes[4..12], .little);
            const oldest_tx = self.db.txn_manager.getOldestActiveTxId(self.db.pool.pager.io);
            if (xmin < oldest_tx) {
                const fixed_len = std.mem.readInt(u32, actual_bytes[28..32], .little);
                const heap_len = std.mem.readInt(u32, actual_bytes[32..36], .little);
                if (36 + fixed_len + heap_len <= actual_bytes.len) {
                    const xmax = std.mem.readInt(u64, actual_bytes[12..20], .little);
                    if (!self.rowVisible(current_tx, xmin, xmax)) return null;
                    const fixed = actual_bytes[36 .. 36 + fixed_len];
                    const heap = actual_bytes[36 + fixed_len .. 36 + fixed_len + heap_len];
                    return try self.buildRowJson(table, fixed, heap);
                }
            }
        }

        const versions = try self.db.reconstructVersionChain(actual_bytes, self.allocator);
        defer {
            for (versions) |*v| v.deinit(self.allocator);
            self.allocator.free(versions);
        }

        for (versions) |v| {
            if (self.rowVisible(current_tx, v.xmin, v.xmax)) {
                return try self.buildRowJson(table, v.fixed, v.heap);
            }
        }
        return null;
    }

    /// Build the JSON object for one visible row from its decoded `fixed`/`heap`
    /// bytes. Honours [`scan_needed_cols`]: when a projection-pushdown set is
    /// active only those columns are decoded and emitted (they are guaranteed to
    /// be real columns of `table` and to cover everything the rest of the plan
    /// reads); otherwise every column is emitted, as before.
    fn buildRowJson(self: *QueryExecutor, table: Table, fixed: []const u8, heap: []const u8) !query_iter.TableRow {
        if (self.qprof) self.qp_json.start(self.db.pool.pager.io);
        defer if (self.qprof) self.qp_json.stop(self.db.pool.pager.io);
        const reader = RowReader.init(table, fixed, heap);

        // Column set for this row: the projection-pushdown subset when active,
        // else every column. `names` is a freshly-allocated array of BORROWED
        // (schema-stable) name slices so ownership is uniform for freeTableRow:
        // the array is freed, the name bytes are not.
        const ncols = if (self.scan_needed_cols) |need| need.len else table.columns.len;
        // When projection pushdown is active, `scan_needed_cols` already holds
        // exactly this row's column names and stays stable for the whole scan, so
        // BORROW it as `names` (owns_names=false) rather than allocating and copying
        // an identical array per row. Retaining callers clone the row first and the
        // clone owns its duplicated names. On the SELECT * path there is no such
        // stable array, so allocate and own it as before.
        var owns_names = true;
        const names = blk: {
            if (self.scan_needed_cols) |need| {
                owns_names = false;
                break :blk need;
            }
            const n = try self.allocator.alloc([]const u8, ncols);
            for (table.columns, 0..) |col, i| n[i] = col.name;
            break :blk n;
        };
        errdefer if (owns_names) self.allocator.free(names);

        const cells = try self.allocator.alloc(query_iter.Cell, ncols);
        errdefer self.allocator.free(cells);
        var done: usize = 0;
        errdefer for (cells[0..done]) |c| switch (c) {
            .text => |t| self.allocator.free(t),
            else => {},
        };
        // Decode each cell from the column struct directly. In the common
        // `SELECT *` path (`scan_needed_cols == null`) the column pointer comes
        // straight from `table.columns`, so there is no per-cell name lookup
        // (mirrors how Postgres/SQLite resolve column offsets once, not per cell).
        if (self.scan_needed_cols == null) {
            for (table.columns, 0..) |*col, i| {
                cells[i] = try self.readCellFromCol(reader, col);
                done = i + 1;
            }
        } else {
            for (names, 0..) |cn, i| {
                const col = table.getColumn(cn) orelse {
                    cells[i] = .null;
                    done = i + 1;
                    continue;
                };
                cells[i] = try self.readCellFromCol(reader, col);
                done = i + 1;
            }
        }
        return .{ .names = names, .cells = cells, .owns_names = owns_names };
    }

    /// Decode one column from the raw row into a typed [`query_iter.Cell`] without
    /// stringifying numerics: integers/floats/bools are carried inline, TEXT/BLOB
    /// is duped into an owned `.text`. This is what lets the wire ship binary for
    /// numeric columns (no per-cell `dtoa`/`itoa`). A column absent from the row
    /// with a `NULL` default reads as `.null`; other short-row cases take the
    /// column's zero/default value exactly as [`RowReader.read`] defines it.
    fn readCellFromCol(self: *QueryExecutor, reader: RowReader, col: *const schema.Column) !query_iter.Cell {
        const row = reader.row_buffer;
        const off = col.offset;
        // Short row (schema evolution / absent trailing column): fall back to the
        // string reader, which honours the column's default_value, so behaviour is
        // unchanged for those edge rows.
        if (off + col.size > row.len) {
            const s = try reader.readToString(self.allocator, col.name);
            return .{ .text = s };
        }
        return switch (col.type) {
            .BOOL => .{ .boolean = row[off] != 0 },
            .INT32 => .{ .int = std.mem.readInt(i32, row[off..][0..4], .little) },
            .INT64 => .{ .int = std.mem.readInt(i64, row[off..][0..8], .little) },
            .UINT32 => .{ .uint = std.mem.readInt(u32, row[off..][0..4], .little) },
            .UINT64, .TIMESTAMP => .{ .uint = std.mem.readInt(u64, row[off..][0..8], .little) },
            .FLOAT32 => .{ .float32 = @bitCast(std.mem.readInt(u32, row[off..][0..4], .little)) },
            .FLOAT64 => .{ .float = @bitCast(std.mem.readInt(u64, row[off..][0..8], .little)) },
            .TEXT, .BLOB => blk: {
                const heap_off = std.mem.readInt(u32, row[off..][0..4], .little);
                const dlen = std.mem.readInt(u32, reader.heap_buffer[heap_off..][0..4], .little);
                const start = heap_off + 4;
                break :blk .{ .text = try self.allocator.dupe(u8, reader.heap_buffer[start .. start + dlen]) };
            },
        };
    }

    /// Owned text of an evaluator scalar via the executor allocator.
    fn scalarText(self: *QueryExecutor, v: query_iter.Scalar) ![]const u8 {
        return scalarTextInto(self.allocator, v);
    }

    /// Encode a non-null numeric [`query_iter.Cell`] as a big-endian fixed-width
    /// binary wire cell for `ct` (int4/int8=4/8B two's complement, uint4/uint8 and
    /// timestamp=4/8B, float4/float8=IEEE-754 big-endian, bool=1B). Only called
    /// for [`oidmap.isBinaryType`] columns; the returned bytes are owned.
    fn encodeBinaryCell(a: std.mem.Allocator, ct: ColumnType, cell: query_iter.Cell) ![]u8 {
        switch (ct) {
            .INT32 => {
                const b = try a.alloc(u8, 4);
                std.mem.writeInt(i32, b[0..4], @intCast(cell.int), .big);
                return b;
            },
            .UINT32 => {
                const b = try a.alloc(u8, 4);
                std.mem.writeInt(u32, b[0..4], @intCast(cell.uint), .big);
                return b;
            },
            .INT64 => {
                const b = try a.alloc(u8, 8);
                std.mem.writeInt(i64, b[0..8], cell.int, .big);
                return b;
            },
            .UINT64, .TIMESTAMP => {
                const b = try a.alloc(u8, 8);
                std.mem.writeInt(u64, b[0..8], cell.uint, .big);
                return b;
            },
            .FLOAT32 => {
                const b = try a.alloc(u8, 4);
                std.mem.writeInt(u32, b[0..4], @bitCast(cell.float32), .big);
                return b;
            },
            .FLOAT64 => {
                const b = try a.alloc(u8, 8);
                std.mem.writeInt(u64, b[0..8], @bitCast(cell.float), .big);
                return b;
            },
            .BOOL => {
                const b = try a.alloc(u8, 1);
                b[0] = if (cell.boolean) 1 else 0;
                return b;
            },
            .TEXT, .BLOB => unreachable,
        }
    }

    /// True for the `sys.*` tables whose rows are synthesised from the in-memory
    /// catalog rather than scanned from storage. Their on-disk representation (for
    /// `sys.tables`/`sys.indexes`) is a raw metadata blob, not the MVCC row layout
    /// the scan decoder expects, and `sys.columns` has no storage at all, so a
    /// normal table scan cannot read any of them. See [`buildCatalogRows`].
    fn isCatalogSynthTable(name: []const u8) bool {
        return std.mem.eql(u8, name, "sys.objects") or
            std.mem.eql(u8, name, "sys.tables") or
            std.mem.eql(u8, name, "sys.indexes") or
            std.mem.eql(u8, name, "sys.columns") or
            std.mem.eql(u8, name, "sys.schemas") or
            std.mem.eql(u8, name, "sys.types");
    }

    /// Resolves a table id to its name via the in-memory catalog, or null if none.
    fn tableNameForId(self: *QueryExecutor, id: u32) ?[]const u8 {
        for (self.db.catalog.tables.items) |t| {
            if (t.id == id) return t.name;
        }
        return null;
    }

    /// Materialises the full row set of a synthesised `sys.*` catalog table from
    /// the in-memory catalog.
    ///
    /// Each row is built by [`tableRowFromObject`] from a JSON object carrying every
    /// declared column of `table_meta`, so the normal filter/projection/sort/limit
    /// pipeline then applies unchanged (a `WHERE table_name = '...'` or `ORDER BY
    /// ordinal` works exactly as over a real table). The returned rows, and their
    /// owned cell strings, are handed to a [`query_iter.MaterializedScanIterator`]
    /// which frees them on `deinit`. We temporarily clear any projection-pushdown
    /// hint so every declared column is present regardless of the SELECT list;
    /// downstream projection then picks whatever subset it needs.
    /// A synthetic catalog row under construction: column-name -> typed [`query_iter.Cell`],
    /// a native replacement for the former `std.json.ObjectMap` image. It carries no JSON;
    /// [`tableRowFromCellMap`] turns it into the positional row the scan yields.
    const CatalogCellMap = std.StringArrayHashMapUnmanaged(query_iter.Cell);

    /// Build a [`query_iter.TableRow`] from a column-name -> [`query_iter.Cell`] map, mirroring
    /// [`tableRowFromObject`] but JSON-free. Text cells are duped (the map holds borrowed
    /// schema/catalog slices, so the row owns an independent copy for `freeTableRow`); a
    /// column absent from the map becomes a NULL cell.
    fn tableRowFromCellMap(self: *QueryExecutor, table: Table, map: *const CatalogCellMap) !query_iter.TableRow {
        const ncols = if (self.scan_needed_cols) |need| need.len else table.columns.len;
        // When projection pushdown is active, `scan_needed_cols` already holds
        // exactly this row's column names and stays stable for the whole scan, so
        // BORROW it as `names` (owns_names=false) rather than allocating and copying
        // an identical array per row. Retaining callers clone the row first and the
        // clone owns its duplicated names. On the SELECT * path there is no such
        // stable array, so allocate and own it as before.
        var owns_names = true;
        const names = blk: {
            if (self.scan_needed_cols) |need| {
                owns_names = false;
                break :blk need;
            }
            const n = try self.allocator.alloc([]const u8, ncols);
            for (table.columns, 0..) |col, i| n[i] = col.name;
            break :blk n;
        };
        errdefer if (owns_names) self.allocator.free(names);
        const cells = try self.allocator.alloc(query_iter.Cell, ncols);
        errdefer self.allocator.free(cells);
        var done: usize = 0;
        errdefer for (cells[0..done]) |c| switch (c) {
            .text => |t| self.allocator.free(t),
            else => {},
        };
        for (names, 0..) |cn, i| {
            cells[i] = blk: {
                const v = map.get(cn) orelse break :blk .null;
                break :blk switch (v) {
                    .text => |t| query_iter.Cell{ .text = try self.allocator.dupe(u8, t) },
                    else => v,
                };
            };
            done = i + 1;
        }
        return .{ .names = names, .cells = cells, .owns_names = owns_names };
    }

    fn buildCatalogRows(self: *QueryExecutor, table_meta: Table, table_name: []const u8) ![]query_iter.TableRow {
        const saved_needed = self.scan_needed_cols;
        self.scan_needed_cols = null;
        defer self.scan_needed_cols = saved_needed;

        var out = std.ArrayList(query_iter.TableRow).empty;
        errdefer {
            for (out.items) |r| query_iter.freeTableRow(self.allocator, r);
            out.deinit(self.allocator);
        }

        if (std.mem.eql(u8, table_name, "sys.objects")) {
            // The superset catalog (MSSQL sys.objects): every table (user 'U' and the
            // system catalog tables 'S') plus every index ('IX'), each with a type/desc.
            for (self.db.catalog.tables.items) |t| {
                const is_sys = std.mem.startsWith(u8, t.name, "sys.");
                var obj: CatalogCellMap = .empty;
                defer obj.deinit(self.allocator);
                try obj.put(self.allocator, "object_id", query_iter.Cell{ .int = @intCast(t.id) });
                try obj.put(self.allocator, "name", query_iter.Cell{ .text = t.name });
                try obj.put(self.allocator, "schema_id", query_iter.Cell{ .int = 1 });
                try obj.put(self.allocator, "type", query_iter.Cell{ .text = if (is_sys) "S" else "U" });
                try obj.put(self.allocator, "type_desc", query_iter.Cell{ .text = if (is_sys) "SYSTEM_TABLE" else "USER_TABLE" });
                try obj.put(self.allocator, "is_ms_shipped", query_iter.Cell{ .int = if (is_sys) 1 else 0 });
                const root: i64 = @intCast(self.db.table_roots.get(t.name) orelse 0);
                try obj.put(self.allocator, "root_page_id", query_iter.Cell{ .int = root });
                try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
            }
            for (self.db.catalog.indexes.items) |ix| {
                var obj: CatalogCellMap = .empty;
                defer obj.deinit(self.allocator);
                try obj.put(self.allocator, "object_id", query_iter.Cell{ .int = @intCast(ix.id) });
                try obj.put(self.allocator, "name", query_iter.Cell{ .text = ix.name });
                try obj.put(self.allocator, "schema_id", query_iter.Cell{ .int = 1 });
                try obj.put(self.allocator, "type", query_iter.Cell{ .text = "IX" });
                try obj.put(self.allocator, "type_desc", query_iter.Cell{ .text = "INDEX" });
                try obj.put(self.allocator, "is_ms_shipped", query_iter.Cell{ .int = 0 });
                const root: i64 = @intCast(self.db.index_roots.get(ix.name) orelse 0);
                try obj.put(self.allocator, "root_page_id", query_iter.Cell{ .int = root });
                try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
            }
        } else if (std.mem.eql(u8, table_name, "sys.tables")) {
            // Only user tables (MSSQL sys.tables): the sys.* catalog tables are excluded
            // here and appear in sys.objects instead.
            for (self.db.catalog.tables.items) |t| {
                if (std.mem.startsWith(u8, t.name, "sys.")) continue;
                var obj: CatalogCellMap = .empty;
                defer obj.deinit(self.allocator);
                try obj.put(self.allocator, "object_id", query_iter.Cell{ .int = @intCast(t.id) });
                try obj.put(self.allocator, "name", query_iter.Cell{ .text = t.name });
                try obj.put(self.allocator, "schema_id", query_iter.Cell{ .int = 1 });
                try obj.put(self.allocator, "type", query_iter.Cell{ .text = "U" });
                try obj.put(self.allocator, "type_desc", query_iter.Cell{ .text = "USER_TABLE" });
                try obj.put(self.allocator, "is_ms_shipped", query_iter.Cell{ .int = 0 });
                const root: i64 = @intCast(self.db.table_roots.get(t.name) orelse 0);
                try obj.put(self.allocator, "root_page_id", query_iter.Cell{ .int = root });
                try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
            }
        } else if (std.mem.eql(u8, table_name, "sys.indexes")) {
            for (self.db.catalog.indexes.items) |ix| {
                var obj: CatalogCellMap = .empty;
                defer obj.deinit(self.allocator);
                const tname = self.tableNameForId(ix.table_id) orelse "";
                try obj.put(self.allocator, "object_id", query_iter.Cell{ .int = @intCast(ix.table_id) });
                try obj.put(self.allocator, "table_name", query_iter.Cell{ .text = tname });
                try obj.put(self.allocator, "index_id", query_iter.Cell{ .int = @intCast(ix.id) });
                try obj.put(self.allocator, "name", query_iter.Cell{ .text = ix.name });
                // kaidb secondary indexes are non-clustered B+Trees (MSSQL type 2).
                try obj.put(self.allocator, "type", query_iter.Cell{ .int = 2 });
                try obj.put(self.allocator, "type_desc", query_iter.Cell{ .text = "NONCLUSTERED" });
                const uniq: i64 = if (ix.kind == .UNIQUE) 1 else 0;
                try obj.put(self.allocator, "is_unique", query_iter.Cell{ .int = uniq });
                try obj.put(self.allocator, "is_primary_key", query_iter.Cell{ .int = 0 });
                const root: i64 = @intCast(self.db.index_roots.get(ix.name) orelse 0);
                try obj.put(self.allocator, "root_page_id", query_iter.Cell{ .int = root });
                try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
            }
        } else if (std.mem.eql(u8, table_name, "sys.schemas")) {
            // kaidb is single-schema; expose the default 'dbo' schema (MSSQL convention).
            var obj: CatalogCellMap = .empty;
            defer obj.deinit(self.allocator);
            try obj.put(self.allocator, "schema_id", query_iter.Cell{ .int = 1 });
            try obj.put(self.allocator, "name", query_iter.Cell{ .text = "dbo" });
            try obj.put(self.allocator, "principal_id", query_iter.Cell{ .int = 1 });
            try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
        } else if (std.mem.eql(u8, table_name, "sys.types")) {
            // One row per kaidb column type (the engine's system types).
            const TypeRow = struct { id: i64, name: []const u8, max_len: i64 };
            const type_rows = [_]TypeRow{
                .{ .id = 1, .name = "BOOL", .max_len = 1 },
                .{ .id = 2, .name = "UINT32", .max_len = 4 },
                .{ .id = 3, .name = "UINT64", .max_len = 8 },
                .{ .id = 4, .name = "INT32", .max_len = 4 },
                .{ .id = 5, .name = "INT64", .max_len = 8 },
                .{ .id = 6, .name = "FLOAT32", .max_len = 4 },
                .{ .id = 7, .name = "FLOAT64", .max_len = 8 },
                .{ .id = 8, .name = "TIMESTAMP", .max_len = 8 },
                .{ .id = 9, .name = "TEXT", .max_len = -1 },
                .{ .id = 10, .name = "BLOB", .max_len = -1 },
            };
            for (type_rows) |tr| {
                var obj: CatalogCellMap = .empty;
                defer obj.deinit(self.allocator);
                try obj.put(self.allocator, "user_type_id", query_iter.Cell{ .int = tr.id });
                try obj.put(self.allocator, "name", query_iter.Cell{ .text = tr.name });
                try obj.put(self.allocator, "max_length", query_iter.Cell{ .int = tr.max_len });
                try obj.put(self.allocator, "is_nullable", query_iter.Cell{ .int = 1 });
                try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
            }
        } else { // sys.columns: one row per column of every table (MSSQL sys.columns shape)
            for (self.db.catalog.tables.items) |t| {
                for (t.columns, 0..) |col, i| {
                    const var_len = col.type == .TEXT or col.type == .BLOB;
                    var obj: CatalogCellMap = .empty;
                    defer obj.deinit(self.allocator);
                    try obj.put(self.allocator, "object_id", query_iter.Cell{ .int = @intCast(t.id) });
                    try obj.put(self.allocator, "table_name", query_iter.Cell{ .text = t.name });
                    try obj.put(self.allocator, "column_id", query_iter.Cell{ .int = @intCast(i + 1) });
                    try obj.put(self.allocator, "name", query_iter.Cell{ .text = col.name });
                    try obj.put(self.allocator, "data_type", query_iter.Cell{ .text = @tagName(col.type) });
                    try obj.put(self.allocator, "max_length", query_iter.Cell{ .int = if (var_len) -1 else @as(i64, @intCast(col.size)) });
                    try obj.put(self.allocator, "ordinal", query_iter.Cell{ .int = @intCast(i + 1) });
                    try obj.put(self.allocator, "is_nullable", query_iter.Cell{ .int = if (col.is_nullable) 1 else 0 });
                    try obj.put(self.allocator, "is_identity", query_iter.Cell{ .int = if (col.is_auto_increment) 1 else 0 });
                    try obj.put(self.allocator, "is_primary_key", query_iter.Cell{ .int = if (col.is_primary_key) 1 else 0 });
                    try out.append(self.allocator, try self.tableRowFromCellMap(table_meta, &obj));
                }
            }
        }

        return out.toOwnedSlice(self.allocator);
    }

    /// Build a [`query_iter.TableRow`] from a legacy JSON-object row image (the
    /// pre-binary at-rest layout handled by [`getVisibleVersion`]). Each of the
    /// row's columns is read out of `obj` and rendered to its text cell; a missing
    /// key or JSON null becomes a NULL cell. `names` are borrowed schema slices.
    fn tableRowFromObject(self: *QueryExecutor, table: Table, obj: std.json.Value) !query_iter.TableRow {
        const ncols = if (self.scan_needed_cols) |need| need.len else table.columns.len;
        // When projection pushdown is active, `scan_needed_cols` already holds
        // exactly this row's column names and stays stable for the whole scan, so
        // BORROW it as `names` (owns_names=false) rather than allocating and copying
        // an identical array per row. Retaining callers clone the row first and the
        // clone owns its duplicated names. On the SELECT * path there is no such
        // stable array, so allocate and own it as before.
        var owns_names = true;
        const names = blk: {
            if (self.scan_needed_cols) |need| {
                owns_names = false;
                break :blk need;
            }
            const n = try self.allocator.alloc([]const u8, ncols);
            for (table.columns, 0..) |col, i| n[i] = col.name;
            break :blk n;
        };
        errdefer if (owns_names) self.allocator.free(names);
        const cells = try self.allocator.alloc(query_iter.Cell, ncols);
        errdefer self.allocator.free(cells);
        var done: usize = 0;
        errdefer for (cells[0..done]) |c| switch (c) {
            .text => |t| self.allocator.free(t),
            else => {},
        };
        for (names, 0..) |cn, i| {
            cells[i] = blk: {
                if (obj != .object) break :blk .null;
                const v = obj.object.get(cn) orelse break :blk .null;
                break :blk switch (v) {
                    .null => .null,
                    .string => |s| query_iter.Cell{ .text = try self.allocator.dupe(u8, s) },
                    .integer => |n| query_iter.Cell{ .int = n },
                    .float => |f| query_iter.Cell{ .float = f },
                    .bool => |b| query_iter.Cell{ .boolean = b },
                    else => .null,
                };
            };
            done = i + 1;
        }
        return .{ .names = names, .cells = cells, .owns_names = owns_names };
    }

    /// Reads `col_name` from the raw row as an `f64` for a numeric comparison, or
    /// null when the value is not a plain, exactly-representable number: text/bool
    /// columns, a NULL/short row (`read` errors), or an integer too large to hold
    /// exactly in an `f64`. Null propagates as "uncertain" so the caller keeps the
    /// row rather than risk a wrong comparison. Allocation-free.
    fn rawColNumeric(table: Table, reader: RowReader, col_name: []const u8) ?f64 {
        const col = table.getColumn(col_name) orelse return null;
        const row = reader.row_buffer;
        const off = col.offset;
        if (off + col.size > row.len) return null; // short row / NULL: uncertain
        const P: i64 = 1 << 53; // f64 exact-integer bound
        return switch (col.type) {
            .INT32 => @as(f64, @floatFromInt(std.mem.readInt(i32, row[off..][0..4], .little))),
            .UINT32 => @as(f64, @floatFromInt(std.mem.readInt(u32, row[off..][0..4], .little))),
            .INT64 => blk: {
                const v = std.mem.readInt(i64, row[off..][0..8], .little);
                if (v > P or v < -P) break :blk null;
                break :blk @as(f64, @floatFromInt(v));
            },
            .UINT64, .TIMESTAMP => blk: {
                const v = std.mem.readInt(u64, row[off..][0..8], .little);
                if (v > @as(u64, @intCast(P))) break :blk null;
                break :blk @as(f64, @floatFromInt(v));
            },
            .FLOAT32 => @as(f64, @floatCast(@as(f32, @bitCast(std.mem.readInt(u32, row[off..][0..4], .little))))),
            .FLOAT64 => @as(f64, @bitCast(std.mem.readInt(u64, row[off..][0..8], .little))),
            else => null,
        };
    }

    /// Numeric value of a literal expression, or null for anything else.
    fn litNumeric(e: *const ast.Expr) ?f64 {
        return switch (e.*) {
            .literal_int => |i| @floatFromInt(i),
            .literal_float => |f| f,
            else => null,
        };
    }

    /// The comparison operator equivalent to `op` with the operands swapped
    /// (`col > lit` vs `lit > col`), so the evaluator can always reason with the
    /// column on the left.
    fn flipCmp(op: ast.OpType) ast.OpType {
        return switch (op) {
            .GT => .LT,
            .LT => .GT,
            .GTE => .LTE,
            .LTE => .GTE,
            else => op, // EQ/NE are symmetric
        };
    }

    /// Evaluates the residual `expr` against a raw row and returns:
    ///   - `false` only when it can PROVE the row fails the predicate,
    ///   - `true`  only when it can prove it passes,
    ///   - `null`  for anything it does not fully model (a non-numeric column, a
    ///             column-vs-column comparison, a function, `LIKE`, `IS NULL`, a
    ///             literal it cannot read as a number, an under-determined AND/OR).
    ///
    /// The caller uses only the `false` verdict, to skip building a row's JSON. A
    /// `null` (or `true`) keeps the row, and the downstream `FilterIterator` makes
    /// the authoritative decision, so an incomplete model here can only cost an
    /// extra build, never drop a valid row. Every handled shape uses the same
    /// numeric comparison the normal filter uses, so a proven `false` is a genuine
    /// non-match. Allocation-free.
    fn rawEval(table: Table, reader: RowReader, e: *const ast.Expr) ?bool {
        switch (e.*) {
            .binary_op => |b| {
                switch (b.op) {
                    .AND => {
                        const l = rawEval(table, reader, b.left);
                        const r = rawEval(table, reader, b.right);
                        if (l == false or r == false) return false;
                        if (l == true and r == true) return true;
                        return null;
                    },
                    .OR => {
                        const l = rawEval(table, reader, b.left);
                        const r = rawEval(table, reader, b.right);
                        if (l == true or r == true) return true;
                        if (l == false and r == false) return false;
                        return null;
                    },
                    .EQ, .NE, .GT, .LT, .GTE, .LTE => {
                        var cv: ?f64 = null;
                        var lv: ?f64 = null;
                        var op = b.op;
                        if (b.left.* == .column_ref) {
                            cv = rawColNumeric(table, reader, b.left.column_ref);
                            lv = litNumeric(b.right);
                        } else if (b.right.* == .column_ref) {
                            cv = rawColNumeric(table, reader, b.right.column_ref);
                            lv = litNumeric(b.left);
                            op = flipCmp(b.op);
                        } else return null;
                        const c = cv orelse return null;
                        const v = lv orelse return null;
                        return switch (op) {
                            .EQ => c == v,
                            .NE => c != v,
                            .GT => c > v,
                            .LT => c < v,
                            .GTE => c >= v,
                            .LTE => c <= v,
                            else => null,
                        };
                    },
                    else => return null,
                }
            },
            .between => |bt| {
                if (bt.operand.* != .column_ref) return null;
                const c = rawColNumeric(table, reader, bt.operand.column_ref) orelse return null;
                const lo = litNumeric(bt.lo) orelse return null;
                const hi = litNumeric(bt.hi) orelse return null;
                const within = c >= lo and c <= hi;
                return if (bt.negated) !within else within;
            },
            .in_list => |il| {
                if (il.operand.* != .column_ref) return null;
                const c = rawColNumeric(table, reader, il.operand.column_ref) orelse return null;
                var found = false;
                for (il.items) |it| {
                    const v = litNumeric(it) orelse return null; // non-numeric member: uncertain
                    if (c == v) {
                        found = true;
                        break;
                    }
                }
                return if (il.negated) !found else found;
            },
            else => return null,
        }
    }

    /// Fetches the row for `pk_val` from `table_tree`, returning its visible
    /// version as a JSON object, or null if there is no visible row OR (when a
    /// residual is supplied) the row is a confident non-match.
    ///
    /// The point of this over `search` + `getVisibleVersion` is that when the
    /// common single-committed-version fast path applies and a residual predicate
    /// is supplied, the residual is evaluated on the RAW row first
    /// ([`rawRowMatches`]); a non-match returns null WITHOUT building the full row
    /// JSON. That removes the per-row `buildRowJson` for rows a `WHERE` will reject
    /// (e.g. an index scan on one column with a filter on another). Anything the
    /// fast path does not cover (cold MVCC versions, the legacy layout) falls
    /// through to the unchanged `getVisibleVersion`, so correctness never depends on
    /// the raw pre-filter - the normal filter still runs downstream.
    pub fn fetchVisibleFilteredJson(self: *QueryExecutor, table: Table, table_tree: *BPlusTree, pk_val: []const u8, current_tx: u64, residual: ?*const ast.Expr, residual_cols: ?[]const []const u8) !?query_iter.TableRow {
        // Reuse the leaf cursor when one is active for THIS tree (equality index
        // scan, pk-ascending); otherwise a fresh root-to-leaf descent.
        if (self.qprof) self.qp_seek.start(self.db.pool.pager.io);
        // Borrow the inline row image straight from the leaf-reuse cursor's held
        // leaf (no per-row dupe/free) when one is active for THIS tree; the bytes
        // stay valid through this call because the cursor keeps the leaf pinned
        // until the next fetch. The fresh-descent fallback and overflow values
        // return an owned copy, tracked by `val_owned`.
        var val_owned = true;
        const val = blk: {
            if (self.base_searcher) |s| {
                if (s.tree == table_tree) {
                    const r = (try s.getRef(pk_val, self.allocator)) orelse {
                        if (self.qprof) self.qp_seek.stop(self.db.pool.pager.io);
                        return null;
                    };
                    val_owned = r.owned;
                    break :blk r.bytes;
                }
            }
            break :blk (try table_tree.search(pk_val, self.allocator)) orelse {
                if (self.qprof) self.qp_seek.stop(self.db.pool.pager.io);
                return null;
            };
        };
        if (self.qprof) self.qp_seek.stop(self.db.pool.pager.io);
        defer if (val_owned) self.allocator.free(val);

        if (val.len >= 36 and val[0] != '{') {
            const xmin = std.mem.readInt(u64, val[4..12], .little);
            const oldest_tx = self.db.txn_manager.getOldestActiveTxId(self.db.pool.pager.io);
            if (xmin < oldest_tx) {
                const fixed_len = std.mem.readInt(u32, val[28..32], .little);
                const heap_len = std.mem.readInt(u32, val[32..36], .little);
                if (36 + fixed_len + heap_len <= val.len) {
                    const xmax = std.mem.readInt(u64, val[12..20], .little);
                    if (!self.rowVisible(current_tx, xmin, xmax)) return null;
                    const fixed = val[36 .. 36 + fixed_len];
                    const heap = val[36 + fixed_len .. 36 + fixed_len + heap_len];
                    if (residual) |res| {
                        _ = residual_cols; // superseded by the alloc-free raw evaluator
                        const reader = RowReader.init(table, fixed, heap);
                        if (rawEval(table, reader, res) == false) return null; // provably fails WHERE
                    }
                    return try self.buildRowJson(table, fixed, heap);
                }
            }
        }
        // Cold / legacy layout: no raw pre-filter, full reconstruction as before.
        return try self.getVisibleVersion(table, val, false, current_tx);
    }

    /// Like [`fetchVisibleFilteredJson`] but returns only whether the row for
    /// `pk_val` is visible to `current_tx` and passes `residual`, WITHOUT building
    /// the row image. Used to count rows past an OFFSET without paying the
    /// per-row materialisation (see the scan iterators' `skip_remaining`). It
    /// still fetches the stored value to check MVCC visibility and the residual,
    /// so it is a partial (not fetch-free) skip; the win is skipping buildRowJson.
    pub fn rowQualifies(self: *QueryExecutor, table: Table, table_tree: *BPlusTree, pk_val: []const u8, current_tx: u64, residual: ?*const ast.Expr, residual_cols: ?[]const []const u8) !bool {
        var val_owned = true;
        const val = blk: {
            if (self.base_searcher) |s| {
                if (s.tree == table_tree) {
                    const r = (try s.getRef(pk_val, self.allocator)) orelse return false;
                    val_owned = r.owned;
                    break :blk r.bytes;
                }
            }
            break :blk (try table_tree.search(pk_val, self.allocator)) orelse return false;
        };
        defer if (val_owned) self.allocator.free(val);

        if (val.len >= 36 and val[0] != '{') {
            const xmin = std.mem.readInt(u64, val[4..12], .little);
            const oldest_tx = self.db.txn_manager.getOldestActiveTxId(self.db.pool.pager.io);
            if (xmin < oldest_tx) {
                const fixed_len = std.mem.readInt(u32, val[28..32], .little);
                const heap_len = std.mem.readInt(u32, val[32..36], .little);
                if (36 + fixed_len + heap_len <= val.len) {
                    const xmax = std.mem.readInt(u64, val[12..20], .little);
                    if (!self.rowVisible(current_tx, xmin, xmax)) return false;
                    if (residual) |res| {
                        _ = residual_cols;
                        const fixed = val[36 .. 36 + fixed_len];
                        const heap = val[36 + fixed_len .. 36 + fixed_len + heap_len];
                        const reader = RowReader.init(table, fixed, heap);
                        if (rawEval(table, reader, res) == false) return false;
                    }
                    return true;
                }
            }
        }
        // Cold / legacy layout: reconstruct the visible row and apply the residual
        // WHERE over it (evalExprJson), so a skipped row is only counted when it
        // genuinely qualifies. Rare path (fresh rows take the fast path above).
        if (try self.getVisibleVersion(table, val, false, current_tx)) |row| {
            defer self.freeTableRow(row);
            if (residual) |res| return query_iter.evalExprJson(res, row);
            return true;
        }
        return false;
    }

    const PushdownError = error{PushdownUnsupported};

    /// A small, allocation-free deduplicating set of column names, backed by a
    /// fixed inline buffer. Projection pushdown runs on the per-query hot path,
    /// so it must not allocate: a heap set per query costs more than it saves for
    /// a point read (one row). Overflowing the buffer (an unusually wide
    /// reference set) sets `ok = false`, which the caller treats as "read all".
    const NeededSet = struct {
        buf: [32][]const u8 = undefined,
        n: usize = 0,
        ok: bool = true,
        fn add(self: *NeededSet, name: []const u8) void {
            for (self.buf[0..self.n]) |x| if (std.mem.eql(u8, x, name)) return;
            if (self.n >= self.buf.len) {
                self.ok = false;
                return;
            }
            self.buf[self.n] = name;
            self.n += 1;
        }
    };

    /// Recursively collect the column names referenced by `e` into `ns`. Bails
    /// (returns [`PushdownError.PushdownUnsupported`]) on any node that could
    /// reference columns this simple collector does not fully enumerate
    /// (functions, CASE, subqueries), so the caller falls back to reading all
    /// columns rather than risk dropping one.
    fn collectExprCols(e: *const ast.Expr, ns: *NeededSet) PushdownError!void {
        switch (e.*) {
            .column_ref => |name| ns.add(name),
            .binary_op => |b| {
                try collectExprCols(b.left, ns);
                try collectExprCols(b.right, ns);
            },
            .unary_not => |u| try collectExprCols(u, ns),
            .is_null => |n| try collectExprCols(n.operand, ns),
            .in_list => |l| {
                try collectExprCols(l.operand, ns);
                for (l.items) |it| try collectExprCols(it, ns);
            },
            .like => |lk| {
                try collectExprCols(lk.operand, ns);
                try collectExprCols(lk.pattern, ns);
            },
            .between => |bt| {
                try collectExprCols(bt.operand, ns);
                try collectExprCols(bt.lo, ns);
                try collectExprCols(bt.hi, ns);
            },
            .literal_int, .literal_float, .literal_text, .literal_null, .placeholder => {},
            else => return PushdownError.PushdownUnsupported,
        }
    }

    /// Compute the projection-pushdown column set for a SELECT, or null to read
    /// all columns. Non-null only for a simple single-table read (no joins,
    /// GROUP BY/HAVING, DISTINCT, aggregates, `*`, or unsupported expressions),
    /// where the complete set of referenced columns is knowable: the projection
    /// list, the WHERE predicate, and the ORDER BY keys. Any uncertainty returns
    /// null so correctness never depends on this optimisation. The result is
    /// written into the executor's `needed_buf` (valid until the next call) and
    /// holds borrowed column-name slices; allocation-free.
    fn collectNeededCols(self: *QueryExecutor, sel: ast.SelectStmt) ?[]const []const u8 {
        if (sel.joins.len > 0) return null;
        if (sel.distinct) return null;

        const meta = for (self.db.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, sel.table_name)) break t;
        } else return null;

        var ns = NeededSet{};
        // Projections: a bare column or an aggregate over a knowable column set.
        // A `*` still needs the whole row. Aggregates only read their argument
        // column(s), so `AVG(total_due)` needs `total_due`, not every column - the
        // key to not building 11-column JSON for every row of a GROUP BY scan.
        for (sel.projections) |p| switch (p.expr) {
            .column => |c| ns.add(c),
            .star => return null,
            .aggregate => |agg| switch (agg.argument) {
                .star => {}, // COUNT(*) reads no column
                .column => |c| ns.add(c),
                .expression => |e| collectExprCols(e, &ns) catch return null,
            },
        };
        // GROUP BY keys are column names the grouping reads from each row.
        if (sel.group_by) |gs| for (gs) |g| ns.add(g);
        if (sel.where_expr) |w| collectExprCols(w, &ns) catch return null;
        // HAVING may reference an aggregate (a func node) - collectExprCols bails
        // on those, so we fall back to reading all columns, which is safe.
        if (sel.having_expr) |h| collectExprCols(h, &ns) catch return null;
        if (sel.order_by) |keys| for (keys) |k| ns.add(k.column);
        if (!ns.ok) return null;

        // Keep only names that are real columns of the table, into needed_buf.
        var cnt: usize = 0;
        for (ns.buf[0..ns.n]) |name| {
            for (meta.columns) |col| {
                if (std.mem.eql(u8, col.name, name)) {
                    self.needed_buf[cnt] = name;
                    cnt += 1;
                    break;
                }
            }
        }
        return self.needed_buf[0..cnt];
    }

    /// Appends a WAL record for an operation, reserving a fresh LSN first.
    ///
    /// Convenience over [`logWalRecordWithLsn`] for the begin/commit/rollback
    /// markers where the LSN need not be coordinated with a data write.
    fn logWalRecord(self: *QueryExecutor, kind: OpKind, table_name: []const u8, key: []const u8, value: []const u8) !void {
        return self.logWalRecordWithLsn(self.db.reserveLsn(), kind, table_name, key, value);
    }

    /// Appends a WAL record with a caller-supplied LSN.
    ///
    /// A no-op if the database has no WAL configured. The commit and rollback
    /// records are fsynced (`appendAndSync`) when `synchronous_commit` is on, so
    /// durability of the boundary is guaranteed before returning; all other
    /// records are buffered append-only. The explicit-LSN form lets a data
    /// mutation reserve its LSN, write the page, and log with the same LSN so
    /// recovery ordering is consistent.
    fn logWalRecordWithLsn(self: *QueryExecutor, lsn: u64, kind: OpKind, table_name: []const u8, key: []const u8, value: []const u8) !void {
        const wal = self.db.wal orelse return;
        const tx_id = self.current_tx_id orelse 0;
        const timestamp = std.Io.Clock.now(.real, self.db.pool.pager.io).toMilliseconds();

        const record = LogRecord{
            .lsn = lsn,
            .tx_id = tx_id,
            .timestamp = timestamp,
            .kind = kind,
            .table_name = table_name,
            .key = key,
            .value = value,
        };

        if (kind == .commit or kind == .rollback) {
            if (self.db.synchronous_commit) {
                try wal.appendAndSync(record);
            } else {
                try wal.append(record);
            }
        } else {
            try wal.append(record);
        }
    }

    /// Deep-copies a JSON value using the database allocator (delegates to
    /// `db.cloneJsonValue`).
    ///
    /// Needed because visible-version decoding hands out values whose lifetime
    /// must outlive the parsed document they came from.
    fn cloneJsonValue(self: *QueryExecutor, val: std.json.Value) !std.json.Value {
        return try self.db.cloneJsonValue(val);
    }

    /// Recursively frees a JSON value previously produced by
    /// [`getVisibleVersion`] / [`cloneJsonValue`] (delegates to
    /// `db.freeJsonValue`).
    pub fn freeJsonValue(self: *QueryExecutor, val: std.json.Value) void {
        self.db.freeJsonValue(val);
    }

    /// Frees a [`query_iter.TableRow`] produced by the row builder
    /// ([`buildRowJson`] / [`tableRowFromObject`] / [`getVisibleVersion`]): its
    /// owned cell strings and backing arrays. Borrowed column names are untouched.
    pub fn freeTableRow(self: *QueryExecutor, tr: query_iter.TableRow) void {
        query_iter.freeTableRow(self.allocator, tr);
    }

    /// Writes a new MVCC row version for an INSERT or UPDATE.
    ///
    /// Looks up the existing version chain for `key` and finds the version
    /// visible to the current transaction; then enforces the MVCC write rules:
    /// an INSERT onto a key with a visible version is `error.DuplicateKey`, and
    /// an UPDATE of a key with no visible version is `error.KeyNotFound`. It
    /// serialises `new_data` into the table's fixed/heap row layout via
    /// [`RowBuilder`], stamps it with `xmin = currentWriteXid()` and `xmax = 0`,
    /// hands it to `db.updateRowMVCC` (which appends it to the chain and, for an
    /// update, closes the prior version's `xmax`), then logs the WAL record and
    /// updates the table's root page id. `is_update` selects the semantics and
    /// the WAL op kind. Secondary-index maintenance is done by the callers, not
    /// here.
    pub fn writeNewVersion(self: *QueryExecutor, table_tree: *BPlusTree, table_name: []const u8, key: []const u8, new_data: CatalogCellMap, is_update: bool) !void {
        const current_tx = self.current_tx_id orelse return error.NoActiveTransaction;

        const table = for (self.db.catalog.tables.items) |tbl| {
            if (std.mem.eql(u8, tbl.name, table_name)) break tbl;
        } else return error.TableNotFound;

        const opt_val = try table_tree.search(key, self.allocator);
        const exists = (opt_val != null);
        defer if (opt_val) |v| self.allocator.free(v);

        if (is_update and !exists) {
            return error.KeyNotFound;
        }
        var existing_versions: []schema.DecodedVersion = &[_]schema.DecodedVersion{};
        if (opt_val) |val| {
            existing_versions = try self.db.reconstructVersionChain(val, self.allocator);
        }
        defer {
            for (existing_versions) |*v| v.deinit(self.allocator);
            self.allocator.free(existing_versions);
        }

        var active_index: ?usize = null;
        for (existing_versions, 0..) |v, idx| {
            if (self.rowVisible(current_tx, v.xmin, v.xmax)) {
                active_index = idx;
                break;
            }
        }

        if (active_index) |_| {
            if (!is_update) {
                return error.DuplicateKey;
            }
        } else {
            if (is_update) return error.KeyNotFound;
        }

        var heap_capacity: u32 = 0;
        var it = new_data.iterator();
        while (it.next()) |entry| {
            if (entry.value_ptr.* == .text) {
                heap_capacity += @intCast(entry.value_ptr.*.text.len + 4);
            }
        }
        if (heap_capacity < 1024) heap_capacity = 1024;

        const fixed_buf = try self.allocator.alloc(u8, table.fixed_size);
        defer self.allocator.free(fixed_buf);
        @memset(fixed_buf, 0);

        const heap_buf = try self.allocator.alloc(u8, heap_capacity);
        defer self.allocator.free(heap_buf);

        var heap_offset: u32 = 0;
        var builder = RowBuilder.init(table, fixed_buf, heap_buf, &heap_offset);

        for (table.columns) |col| {
            const cell_val = new_data.get(col.name) orelse continue;
            const str_val = switch (cell_val) {
                .text => |s| try self.allocator.dupe(u8, s),
                .int => |i| try std.fmt.allocPrint(self.allocator, "{d}", .{i}),
                .uint => |u| try std.fmt.allocPrint(self.allocator, "{d}", .{u}),
                .float => |f| try std.fmt.allocPrint(self.allocator, "{d}", .{f}),
                .float32 => |f| try std.fmt.allocPrint(self.allocator, "{d}", .{f}),
                .boolean => |b| try self.allocator.dupe(u8, if (b) "true" else "false"),
                .null => try self.allocator.dupe(u8, "NULL"),
            };
            defer self.allocator.free(str_val);
            try builder.writeDynamic(col.name, str_val);
        }

        const new_fixed = try self.allocator.dupe(u8, fixed_buf);
        const new_heap = try self.allocator.dupe(u8, heap_buf[0..heap_offset]);

        const new_version = schema.DecodedVersion{
            .xmin = self.currentWriteXid(),
            .xmax = 0,
            .fixed = new_fixed,
            .heap = new_heap,
        };
        // Free on every exit (success and error). NOTE: do NOT also add `errdefer`
        // frees for new_fixed/new_heap - this `defer` already runs on the error
        // path, so an errdefer would free them a second time. That double-free was
        // benign under the Debug/GPA allocator but is undefined behaviour under the
        // release `c_allocator`, and it is exactly what crashed the server (silently,
        // no panic) when a downstream write failed - e.g. `updateRowMVCC` /
        // `logWalRecordWithLsn` returning `error.NoSpaceLeft` on a full disk. It fires
        // on ANY write error here, not just ENOSPC.
        defer {
            self.allocator.free(new_fixed);
            self.allocator.free(new_heap);
        }

        const lsn = self.db.reserveLsn();
        const packed_bytes = try self.db.updateRowMVCC(table_tree, key, new_version);
        defer self.allocator.free(packed_bytes);

        const op_kind = if (is_update) OpKind.update else OpKind.insert;
        try self.logWalRecordWithLsn(lsn, op_kind, table_name, key, packed_bytes);
        try self.db.updateTableRootPageId(table_name, table_tree.root_page_id, current_tx);
    }

    /// Marks the visible version of a row as deleted (MVCC tombstone).
    ///
    /// Rather than removing the cell, it copies the currently-visible version's
    /// payload into a new version whose `xmax` is set to the current write xid,
    /// so the row disappears for this and later transactions while older
    /// snapshots still see it. Returns `error.KeyNotFound` when there is no
    /// visible version. Logs a `.delete` WAL record and updates the table root.
    /// The inverse-shaped write to [`writeNewVersion`].
    fn deleteRowVersion(self: *QueryExecutor, table_tree: *BPlusTree, table_name: []const u8, key: []const u8) !void {
        const current_tx = self.current_tx_id orelse return error.NoActiveTransaction;

        const opt_val = try table_tree.search(key, self.allocator) orelse return error.KeyNotFound;
        defer self.allocator.free(opt_val);

        const existing_versions = try self.db.reconstructVersionChain(opt_val, self.allocator);
        defer {
            for (existing_versions) |*v| v.deinit(self.allocator);
            self.allocator.free(existing_versions);
        }

        var active_index: ?usize = null;
        for (existing_versions, 0..) |v, idx| {
            if (self.rowVisible(current_tx, v.xmin, v.xmax)) {
                active_index = idx;
                break;
            }
        }

        const idx = active_index orelse return error.KeyNotFound;
        const act = existing_versions[idx];

        var new_v = schema.DecodedVersion{
            .xmin = act.xmin,
            .xmax = self.currentWriteXid(),
            .fixed = try self.allocator.dupe(u8, act.fixed),
            .heap = try self.allocator.dupe(u8, act.heap),
        };
        defer new_v.deinit(self.allocator);

        const lsn = self.db.reserveLsn();
        const packed_bytes = try self.db.updateRowMVCC(table_tree, key, new_v);
        defer self.allocator.free(packed_bytes);

        try self.logWalRecordWithLsn(lsn, .delete, table_name, key, packed_bytes);
        try self.db.updateTableRootPageId(table_name, table_tree.root_page_id, current_tx);
    }

    /// Foreign-key helper: does a visible parent row exist with
    /// `parent_col_name = parent_val`?
    ///
    /// Fast-paths the common case where the referenced column is the parent's
    /// primary key (a single point lookup); otherwise scans the parent table
    /// checking each visible row. MVCC-aware, so an FK only passes when the
    /// referenced row is visible to the current transaction. Used when
    /// validating INSERT/UPDATE of a child row.
    fn checkParentRowExists(self: *QueryExecutor, parent_table_id: u32, parent_col_name: []const u8, parent_val: []const u8) !bool {
        const parent_table = for (self.db.catalog.tables.items) |tbl| {
            if (tbl.id == parent_table_id) break tbl;
        } else return error.TableNotFound;

        const parent_tree = try self.db.getTableTree(parent_table.name);
        defer parent_tree.deinit();

        var is_pk = false;
        for (parent_table.columns) |col| {
            if (col.is_primary_key and std.mem.eql(u8, col.name, parent_col_name)) {
                is_pk = true;
                break;
            }
        }

        const current_tx = self.current_tx_id orelse 1;

        if (is_pk) {
            if (try parent_tree.search(parent_val, self.allocator)) |val_bytes| {
                defer self.allocator.free(val_bytes);
                if (try self.getVisibleVersion(parent_table, val_bytes, false, current_tx)) |visible_row| {
                    self.freeTableRow(visible_row);
                    return true;
                }
            }
            return false;
        }

        var it = try parent_tree.iterator();
        defer it.deinit();

        while (try it.next()) |cell| {
            if (try self.getVisibleVersion(parent_table, cell.value, cell.flags.value_overflow, current_tx)) |visible_row| {
                defer self.freeTableRow(visible_row);
                if (try visible_row.getTextAlloc(self.allocator, parent_col_name)) |val| {
                    defer self.allocator.free(val);
                    if (std.mem.eql(u8, val, parent_val)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /// Foreign-key helper: does any visible child row reference `parent_val`
    /// through `child_col_name`?
    ///
    /// Scans the child table for a visible row whose FK column equals the
    /// value being deleted/changed on the parent. A true result blocks the
    /// parent DELETE/UPDATE with `error.ForeignKeyConstraintViolation`
    /// (kaidb uses restrict semantics, not cascade).
    fn checkChildRowExists(self: *QueryExecutor, child_table_id: u32, child_col_name: []const u8, parent_val: []const u8) !bool {
        const child_table = for (self.db.catalog.tables.items) |tbl| {
            if (tbl.id == child_table_id) break tbl;
        } else return error.TableNotFound;

        const child_tree = try self.db.getTableTree(child_table.name);
        defer child_tree.deinit();

        var it = try child_tree.iterator();
        defer it.deinit();

        const current_tx = self.current_tx_id orelse 1;

        while (try it.next()) |cell| {
            if (try self.getVisibleVersion(child_table, cell.value, cell.flags.value_overflow, current_tx)) |visible_row| {
                defer self.freeTableRow(visible_row);
                if (try visible_row.getTextAlloc(self.allocator, child_col_name)) |val| {
                    defer self.allocator.free(val);
                    if (std.mem.eql(u8, val, parent_val)) {
                        return true;
                    }
                }
            }
        }
        return false;
    }

    /// UNIQUE helper: is there already a visible row with `col_name = value`?
    ///
    /// Scans the table for a visible row carrying the same value in the given
    /// column, skipping the row identified by `exclude_pk` (so an UPDATE that
    /// rewrites a row does not collide with itself). MVCC-aware. Returns true on
    /// the first match, which [`validateUniqueConstraints`] turns into a
    /// violation.
    fn checkDuplicateValueExists(self: *QueryExecutor, table_id: u32, col_name: []const u8, value: []const u8, exclude_pk: ?[]const u8) !bool {
        const table = for (self.db.catalog.tables.items) |tbl| {
            if (tbl.id == table_id) break tbl;
        } else return error.TableNotFound;

        var pk_name: []const u8 = "";
        for (table.columns) |c| {
            if (c.is_primary_key) {
                pk_name = c.name;
                break;
            }
        }

        const tree = try self.db.getTableTree(table.name);
        defer tree.deinit();
        const current_tx = self.current_tx_id orelse 1;

        var it = try tree.iterator();
        defer it.deinit();
        while (try it.next()) |cell| {
            if (try self.getVisibleVersion(table, cell.value, cell.flags.value_overflow, current_tx)) |vrow| {
                defer self.freeTableRow(vrow);
                if (exclude_pk) |ex| {
                    if (pk_name.len > 0) {
                        if (try vrow.getTextAlloc(self.allocator, pk_name)) |pkv| {
                            defer self.allocator.free(pkv);
                            if (std.mem.eql(u8, pkv, ex)) continue;
                        }
                    }
                }
                if (try vrow.getTextAlloc(self.allocator, col_name)) |cv| {
                    defer self.allocator.free(cv);
                    if (std.mem.eql(u8, cv, value)) return true;
                }
            }
        }
        return false;
    }

    /// Enforces every UNIQUE index on a table against a candidate row.
    ///
    /// For each UNIQUE index column that is non-NULL in `row_obj`, checks for an
    /// existing duplicate via [`checkDuplicateValueExists`] and returns
    /// `error.UniqueConstraintViolation` on the first collision. NULLs are
    /// skipped (SQL treats NULLs as distinct). `exclude_pk` lets an UPDATE
    /// exclude the row being changed. Called before committing an INSERT/UPDATE.
    fn validateUniqueConstraints(self: *QueryExecutor, table_id: u32, row_obj: CatalogCellMap, exclude_pk: ?[]const u8) !void {
        for (self.db.catalog.indexes.items) |idx| {
            if (idx.table_id == table_id and idx.kind == .UNIQUE) {
                for (idx.key_columns) |col| {
                    const val = row_obj.get(col.name);
                    const val_str = if (val) |v| (if (v == .text) v.text else "NULL") else "NULL";
                    if (std.mem.eql(u8, val_str, "NULL")) continue;
                    if (try self.checkDuplicateValueExists(table_id, col.name, val_str, exclude_pk)) {
                        return error.UniqueConstraintViolation;
                    }
                }
            }
        }
    }

    /// Enforces outbound foreign keys for an INSERT or UPDATE of a child row.
    ///
    /// For each FK defined on this table, verifies that every non-NULL
    /// referencing value has a matching visible parent row
    /// ([`checkParentRowExists`]); a missing parent yields
    /// `error.ForeignKeyConstraintViolation`. NULL FK values are allowed
    /// (unenforced), per SQL.
    fn validateForeignKeyConstraintsForInsertOrUpdate(self: *QueryExecutor, table_id: u32, row_obj: CatalogCellMap) !void {
        for (self.db.catalog.foreign_keys.items) |fk| {
            if (fk.table_id == table_id) {
                for (fk.columns, 0..) |col, idx| {
                    const ref_col = fk.referenced_columns[idx];
                    const val = row_obj.get(col.name);
                    const val_str = if (val) |v| v.text else "NULL";

                    if (std.mem.eql(u8, val_str, "NULL")) {
                        continue;
                    }

                    const exists = try self.checkParentRowExists(fk.referenced_table_id, ref_col.name, val_str);
                    if (!exists) {
                        return error.ForeignKeyConstraintViolation;
                    }
                }
            }
        }
    }

    /// Enforces inbound foreign keys before deleting a parent row.
    ///
    /// For each FK that references this table, checks whether any child row
    /// still points at the values in `visible_row`
    /// ([`checkChildRowExists`]); if so the delete is refused with
    /// `error.ForeignKeyConstraintViolation` (restrict semantics). NULL
    /// referenced values are skipped.
    fn validateForeignKeyConstraintsForDelete(self: *QueryExecutor, table_id: u32, visible_row: query_iter.TableRow) !void {
        for (self.db.catalog.foreign_keys.items) |fk| {
            if (fk.referenced_table_id == table_id) {
                for (fk.referenced_columns, 0..) |ref_col, idx| {
                    const child_col = fk.columns[idx];
                    const ov = try visible_row.getTextAlloc(self.allocator, ref_col.name);
                    defer if (ov) |o| self.allocator.free(o);
                    const old_val_str = ov orelse "NULL";

                    if (std.mem.eql(u8, old_val_str, "NULL")) {
                        continue;
                    }

                    const child_exists = try self.checkChildRowExists(fk.table_id, child_col.name, old_val_str);
                    if (child_exists) {
                        return error.ForeignKeyConstraintViolation;
                    }
                }
            }
        }
    }

    /// Enforces foreign keys on both sides for an UPDATE of a row.
    ///
    /// First validates the new row's outbound FKs
    /// ([`validateForeignKeyConstraintsForInsertOrUpdate`]). Then, for any FK
    /// that references this table, if the referenced value actually changed and
    /// the old value is still referenced by a child, refuses the update with
    /// `error.ForeignKeyConstraintViolation`. This catches the case where
    /// editing a parent key would orphan existing children.
    fn validateForeignKeyConstraintsForUpdate(self: *QueryExecutor, table_id: u32, old_row: query_iter.TableRow, new_row: CatalogCellMap) !void {
        try self.validateForeignKeyConstraintsForInsertOrUpdate(table_id, new_row);

        for (self.db.catalog.foreign_keys.items) |fk| {
            if (fk.referenced_table_id == table_id) {
                for (fk.referenced_columns, 0..) |ref_col, idx| {
                    const child_col = fk.columns[idx];
                    const ov = try old_row.getTextAlloc(self.allocator, ref_col.name);
                    defer if (ov) |o| self.allocator.free(o);
                    const old_val_str = ov orelse "NULL";

                    const new_val = new_row.get(ref_col.name);
                    const new_val_str = if (new_val) |v| v.text else "NULL";

                    if (!std.mem.eql(u8, old_val_str, new_val_str)) {
                        if (std.mem.eql(u8, old_val_str, "NULL")) continue;

                        const child_exists = try self.checkChildRowExists(fk.table_id, child_col.name, old_val_str);
                        if (child_exists) {
                            return error.ForeignKeyConstraintViolation;
                        }
                    }
                }
            }
        }
    }
};
