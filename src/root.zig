//! NovaDB root module: the crate entry point and the engine's integration test surface.
//!
//! This is the `root.zig` that `build.zig` names as the library module's root, so
//! everything reachable from here is what the rest of the tree (and `zig build test`)
//! compiles against. It plays two distinct roles.
//!
//! First, it is the small **public re-export** point for the two protocol namespaces a
//! consumer of NovaDB-as-a-library reaches for: [`wire_proto`] (the low-level frame
//! codec in `common/proto.zig`) and [`proto`] (the higher-level server protocol in
//! `proto/protocol.zig`). Nothing else in the storage/query/durability stack is
//! surfaced here; those are pulled in directly by their own modules. Keeping the
//! re-exports thin is deliberate: the wire types are the stable seam that Nova's
//! `nova-novadb` driver talks to, so they get a single, discoverable home.
//!
//! Second, and by line-count overwhelmingly, this file is the engine's **end-to-end
//! integration test bench**. Rather than unit-test each subsystem in isolation, these
//! `test` blocks drive the real stack the way a client would: open a `Database`, run
//! SQL strings through a `QueryExecutor`, and assert on the decoded result rows. That
//! is why almost every test constructs a `std.Io.Threaded` executor and a temporary
//! on-disk `.db` file (deleted via `defer`), the code under test is the actual pager,
//! WAL, B+Tree, MVCC visibility rules, catalog, transaction manager, security manager,
//! and replication path, not mocks of them. The suites cluster into recognisable
//! themes: core CRUD/DDL and crash recovery; transaction isolation and MVCC visibility;
//! the multi-node replication protocol (fencing epochs, quorum acks for RPO=0, PITR,
//! backup/restore, partition/heal chaos and soak); the durability "D7/D8" kill-9 and
//! torn-tail cases; SQL correctness (aggregates, ordering of DECIMAL/DATE, subqueries,
//! UNION, savepoints, bound parameters); the concurrency guardrails (the `GroupLock`
//! invariant stress and the `STRESS:`/`FUZZER` consistency checks that back the
//! per-tree structure lock and per-table access lock described in `architecture.md`);
//! and the SSI/serialisable write-skew gate.
//!
//! The handful of NON-test declarations in this file exist only to serve those tests:
//! [`freeResp`] tears down the heap-allocated result of an `execute` call, [`ReplCapture`]
//! is a WAL ship-callback sink that records replicated log records, [`frameOf`] and
//! [`replayTxns`] drive the replication codec, and [`FuzzKey`] is the key encoding the
//! fuzz suites share. They are documented here as first-class helpers because a
//! misuse (a leaked row, a mis-encoded fuzz key) silently invalidates whichever test
//! depends on them.

/// The Zig standard library. Used throughout for allocators, `std.Io`, atomics,
/// threading, formatting, and the `std.testing` assertions the suites are built on.
const std = @import("std");
/// Shorthand for the `std.Io` async I/O namespace.
///
/// Every test opens a `Database` with an `Io` handle (from a `std.Io.Threaded`
/// executor), and the filesystem cleanup (`Io.Dir.deleteFile` / `deleteTree`) that
/// keeps the temp `.db` and WAL artefacts from leaking between runs goes through it.
const Io = std.Io;

test "GroupLock: R||R and W||W concurrent, R/W mutually exclusive (invariant stress)" {
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const GroupLock = @import("utils").sync.GroupLock;

    const Shared = struct {
        lock: GroupLock = .{},
        active_readers: std.atomic.Value(i64) = .init(0),
        active_writers: std.atomic.Value(i64) = .init(0),
        active_exclusive: std.atomic.Value(i64) = .init(0),
        max_readers: std.atomic.Value(i64) = .init(0),
        max_writers: std.atomic.Value(i64) = .init(0),
        violation: std.atomic.Value(u32) = .init(0),

        fn bump(v: *std.atomic.Value(i64), max: *std.atomic.Value(i64)) void {
            const n = v.fetchAdd(1, .seq_cst) + 1;
            _ = max.fetchMax(n, .seq_cst);
        }
        fn spin() void {
            var k: usize = 0;
            while (k < 40) : (k += 1) std.atomic.spinLoopHint();
        }
        fn reader(s: *@This(), io_: Io) void {
            var i: usize = 0;
            while (i < 4000) : (i += 1) {
                s.lock.lockRead(io_);
                bump(&s.active_readers, &s.max_readers);
                if (s.active_writers.load(.seq_cst) != 0 or s.active_exclusive.load(.seq_cst) != 0) _ = s.violation.fetchAdd(1, .seq_cst);
                spin();
                if (s.active_writers.load(.seq_cst) != 0 or s.active_exclusive.load(.seq_cst) != 0) _ = s.violation.fetchAdd(1, .seq_cst);
                _ = s.active_readers.fetchSub(1, .seq_cst);
                s.lock.unlockRead(io_);
            }
        }
        fn writer(s: *@This(), io_: Io) void {
            var i: usize = 0;
            while (i < 4000) : (i += 1) {
                s.lock.lockWrite(io_);
                bump(&s.active_writers, &s.max_writers);
                if (s.active_readers.load(.seq_cst) != 0 or s.active_exclusive.load(.seq_cst) != 0) _ = s.violation.fetchAdd(1, .seq_cst);
                spin();
                if (s.active_readers.load(.seq_cst) != 0 or s.active_exclusive.load(.seq_cst) != 0) _ = s.violation.fetchAdd(1, .seq_cst);
                _ = s.active_writers.fetchSub(1, .seq_cst);
                s.lock.unlockWrite(io_);
            }
        }
        fn exclusive(s: *@This(), io_: Io) void {
            var i: usize = 0;
            while (i < 2000) : (i += 1) {
                s.lock.lockExclusive(io_);
                _ = s.active_exclusive.fetchAdd(1, .seq_cst);
                if (s.active_readers.load(.seq_cst) != 0 or s.active_writers.load(.seq_cst) != 0 or s.active_exclusive.load(.seq_cst) != 1) _ = s.violation.fetchAdd(1, .seq_cst);
                spin();
                if (s.active_readers.load(.seq_cst) != 0 or s.active_writers.load(.seq_cst) != 0 or s.active_exclusive.load(.seq_cst) != 1) _ = s.violation.fetchAdd(1, .seq_cst);
                _ = s.active_exclusive.fetchSub(1, .seq_cst);
                s.lock.unlockExclusive(io_);
            }
        }
    };

    var s = Shared{};
    var group = std.Io.Group.init;
    for (0..4) |_| group.async(io, Shared.reader, .{ &s, io });
    for (0..4) |_| group.async(io, Shared.writer, .{ &s, io });
    for (0..2) |_| group.async(io, Shared.exclusive, .{ &s, io });
    group.await(io) catch {};

    try std.testing.expectEqual(@as(u32, 0), s.violation.load(.seq_cst));
    try std.testing.expect(s.max_readers.load(.seq_cst) >= 2);
    try std.testing.expect(s.max_writers.load(.seq_cst) >= 2);
}

test "sql index range scan: numeric bounds, backfill + insert maintenance, LIMIT" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_sql_range_scan.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        fn count(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !usize {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            return res.rows.len;
        }
        // The first column of the Nth result row, parsed as i64 (the tests select
        // `id` first). Used to assert ORDER BY output order.
        fn idAt(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, n: usize) !i64 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expect(res.rows.len > n);
            return try std.fmt.parseInt(i64, res.rows[n][0], 10);
        }
    };

    try run.q(&exec, allocator, "CREATE TABLE orders (id INT PRIMARY KEY, total_due INT)");
    // Rows inserted BEFORE the index exists exercise the CREATE INDEX backfill.
    try run.q(&exec, allocator, "INSERT INTO orders (id, total_due) VALUES (1, 100)");
    try run.q(&exec, allocator, "INSERT INTO orders (id, total_due) VALUES (2, 5000)");
    try run.q(&exec, allocator, "INSERT INTO orders (id, total_due) VALUES (3, 15000)");
    try run.q(&exec, allocator, "CREATE INDEX idx_td ON orders (total_due)");
    // Rows inserted AFTER the index exercise the insert index-maintenance path.
    try run.q(&exec, allocator, "INSERT INTO orders (id, total_due) VALUES (4, 25000)");
    try run.q(&exec, allocator, "INSERT INTO orders (id, total_due) VALUES (5, 60000)");
    try run.q(&exec, allocator, "INSERT INTO orders (id, total_due) VALUES (6, 45000)");

    // BETWEEN is numeric: {15000, 25000, 45000}. A broken *lexical* range would
    // wrongly include 5000 ("5000" sorts inside the text range "10000".."50000"),
    // so this count discriminates the order-preserving encoding.
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due BETWEEN 10000 AND 50000"));
    // Exclusive upper bound (< 45000) drops 45000 -> {15000, 25000}.
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due >= 10000 AND total_due < 45000"));
    // One-sided lower bound.
    try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due > 50000"));
    // One-sided upper bound: {100, 5000}.
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due < 10000"));
    // Unordered LIMIT stops early yet stays within the range (<= the 3 matches).
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due BETWEEN 10000 AND 50000 LIMIT 2"));
    // Equality on the indexed column still resolves after the encoding change.
    try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 25000"));

    // AND-conjunction index selection: the equality on the indexed `total_due`
    // drives the scan even as one arm of an AND, and the residual predicate is
    // still enforced by the wrapping FILTER (so the extra condition can reject).
    try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 25000 AND id = 4"));
    try std.testing.expectEqual(@as(usize, 0), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 25000 AND id = 999"));

    // IN via index union: {5000, 25000, 60000} -> ids 2,4,5. A non-present value
    // (99999) contributes nothing; the residual FILTER still gates membership.
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due IN (5000, 25000, 60000)"));
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due IN (5000, 25000, 99999)"));
    try std.testing.expectEqual(@as(usize, 0), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due IN (111, 222)"));

    // OR-of-equalities on the indexed column rewrites to the SAME index union as
    // `IN (...)`: `total_due = 5000 OR total_due = 25000 OR total_due = 60000`
    // seeks each value instead of a full scan. Proven by the union counter.
    {
        const ub = exec.index_in_unions_built;
        try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 5000 OR total_due = 25000 OR total_due = 60000"));
        try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 5000 OR total_due = 99999"));
        // Column on the right side of `=` is handled symmetrically.
        try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM orders WHERE 5000 = total_due OR 25000 = total_due"));
        try std.testing.expect(exec.index_in_unions_built == ub + 3);
    }
    // A single equality is NOT an OR-union (one value); it takes the range/point
    // path, so the union counter must not advance for it.
    {
        const ub = exec.index_in_unions_built;
        try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 5000"));
        try std.testing.expect(exec.index_in_unions_built == ub);
    }
    // A mixed OR (one arm is not `total_due = literal`) must NOT rewrite to a
    // union: it stays a correct full-scan FILTER. {5000:id2, 45000:id6}.
    {
        const ub = exec.index_in_unions_built;
        try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM orders WHERE total_due = 5000 OR id = 6"));
        try std.testing.expect(exec.index_in_unions_built == ub);
    }

    // Indexed ORDER BY on the range-scanned column: rows arrive in total_due order
    // so the sort is skipped. td>5000 = {15000:id3, 25000:id4, 45000:id6, 60000:id5}.
    // ASC streams (LIMIT 2 stops early) -> id3, id4.
    try std.testing.expectEqual(@as(i64, 3), try run.idAt(&exec, allocator, "SELECT id FROM orders WHERE total_due > 5000 ORDER BY total_due ASC LIMIT 2", 0));
    try std.testing.expectEqual(@as(i64, 4), try run.idAt(&exec, allocator, "SELECT id FROM orders WHERE total_due > 5000 ORDER BY total_due ASC LIMIT 2", 1));
    // DESC is served by the BACKWARD range scan (top of range first) -> id5
    // (60000), id6 (45000); the streaming LIMIT stops after 2.
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT id FROM orders WHERE total_due > 5000 ORDER BY total_due DESC LIMIT 2", 0));
    try std.testing.expectEqual(@as(i64, 6), try run.idAt(&exec, allocator, "SELECT id FROM orders WHERE total_due > 5000 ORDER BY total_due DESC LIMIT 2", 1));
    // Full DESC order over the range (no LIMIT): 60000,45000,25000,15000 -> 5,6,4,3.
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT id FROM orders WHERE total_due > 5000 ORDER BY total_due DESC", 0));
    try std.testing.expectEqual(@as(i64, 3), try run.idAt(&exec, allocator, "SELECT id FROM orders WHERE total_due > 5000 ORDER BY total_due DESC", 3));

    // Prove the range accelerator was actually chosen (not merely correct via the
    // residual full-scan filter): the four range queries above each build one.
    try std.testing.expect(exec.range_scans_built >= 4);

    // Float column (the benchmark's total_due is DOUBLE). This is the case where
    // a lexical range scan would UNDER-return: 10000.5 sorts before the "10000:"
    // seek point, so only the IEEE-754 order-preserving float encoding keeps it.
    try run.q(&exec, allocator, "CREATE TABLE fx (id INT PRIMARY KEY, amt DOUBLE)");
    try run.q(&exec, allocator, "INSERT INTO fx (id, amt) VALUES (1, 100.5)");
    try run.q(&exec, allocator, "INSERT INTO fx (id, amt) VALUES (2, 9999.99)");
    try run.q(&exec, allocator, "CREATE INDEX idx_amt ON fx (amt)");
    try run.q(&exec, allocator, "INSERT INTO fx (id, amt) VALUES (3, 10000.5)");
    try run.q(&exec, allocator, "INSERT INTO fx (id, amt) VALUES (4, 25000.25)");
    try run.q(&exec, allocator, "INSERT INTO fx (id, amt) VALUES (5, 45000.0)");
    try run.q(&exec, allocator, "INSERT INTO fx (id, amt) VALUES (6, 60000.75)");

    // {10000.5, 25000.25, 45000.0} -- must include 10000.5 (the near-boundary row).
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT id FROM fx WHERE amt > 10000 AND amt < 50000"));
    // {60000.75}.
    try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM fx WHERE amt > 50000"));
    // {100.5, 9999.99}.
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM fx WHERE amt < 10000"));

    // Prove the DESCENDING backward cursor (RangeIteratorDesc) was actually chosen
    // for the `ORDER BY total_due DESC` queries above (id5/id6 and full order),
    // not an ascending scan reversed. Cross-leaf backward navigation is covered
    // end to end by the 1M benchmark's Q5 (its output must match SQLite's DESC
    // order); a leak-checked unit test cannot span multiple leaves here because
    // the bulk-insert/split path has a pre-existing allocation leak (unrelated to
    // this read-path feature; tracked separately).
    try std.testing.expect(exec.desc_range_scans_built >= 2);

    // Index-only COUNT(*): answered by counting index entries with no base fetch,
    // when the index is exact and the predicate is fully index-covered.
    const ic0 = exec.index_only_counts;
    // Equality on the indexed column: total_due=25000 -> exactly id4.
    try std.testing.expectEqual(@as(i64, 1), try run.idAt(&exec, allocator, "SELECT COUNT(*) FROM orders WHERE total_due = 25000", 0));
    // Range: total_due>5000 -> {15000,25000,45000,60000} = 4.
    try std.testing.expectEqual(@as(i64, 4), try run.idAt(&exec, allocator, "SELECT COUNT(*) FROM orders WHERE total_due > 5000", 0));
    // No predicate: all 6 rows.
    try std.testing.expectEqual(@as(i64, 6), try run.idAt(&exec, allocator, "SELECT COUNT(*) FROM orders", 0));
    // All three took the fast path (no base fetch).
    try std.testing.expect(exec.index_only_counts == ic0 + 3);

    // Soundness fallback: a DELETE flips the index inexact (its entry lingers
    // under MVCC), so the NEXT count must NOT use the fast path and must still be
    // CORRECT (the tombstoned row is excluded via the base-row visibility check).
    const ic1 = exec.index_only_counts;
    try run.q(&exec, allocator, "DELETE FROM orders WHERE id = 4"); // removes total_due=25000
    try std.testing.expectEqual(@as(i64, 0), try run.idAt(&exec, allocator, "SELECT COUNT(*) FROM orders WHERE total_due = 25000", 0));
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT COUNT(*) FROM orders", 0));
    try std.testing.expect(exec.index_only_counts == ic1); // fell back, did not fast-count
}

test "sql group by: ORDER BY aggregate + LIMIT/OFFSET" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_sql_group_limit.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        fn count(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !usize {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            return res.rows.len;
        }
        // First column of the Nth row, as i64 (queries select `region` first).
        fn regionAt(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, n: usize) !i64 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expect(res.rows.len > n);
            return try std.fmt.parseInt(i64, res.rows[n][0], 10);
        }
    };

    try run.q(&exec, allocator, "CREATE TABLE sales (id INT PRIMARY KEY, region INT, amt INT)");
    // region 1 -> sum 30 (10+20); region 2 -> sum 100; region 3 -> sum 15 (5+5+5).
    // SUM desc: r2(100) > r1(30) > r3(15).
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (1, 1, 10)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (2, 1, 20)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (3, 2, 100)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (4, 3, 5)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (5, 3, 5)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (6, 3, 5)");

    // No LIMIT: all three groups.
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region"));
    // ORDER BY the aggregate DESC + LIMIT 2 -> top two sums: region 2 then region 1.
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 2"));
    try std.testing.expectEqual(@as(i64, 2), try run.regionAt(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 2", 0));
    try std.testing.expectEqual(@as(i64, 1), try run.regionAt(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 2", 1));
    // ORDER BY aggregate ASC + LIMIT 1 -> smallest sum: region 3.
    try std.testing.expectEqual(@as(i64, 3), try run.regionAt(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s ASC LIMIT 1", 0));
    // OFFSET after ordering: skip the top, take the next -> region 1.
    try std.testing.expectEqual(@as(i64, 1), try run.regionAt(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 1 OFFSET 1", 0));
    try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 1 OFFSET 1"));
}

test "per-query memory cap: runaway sort fails cleanly instead of OOM-crashing (blocker fix)" {
    // Regression for the 10M re-benchmark OOM: a large unindexed ORDER BY
    // materialises every matching row before it can sort, so with no cap it
    // grows the process until the OS OOM-kills the whole server. With the
    // default per-query cap in place the same query must come back as a normal
    // "Query Memory Limit Exceeded" error response, the server (here, the
    // executor) staying alive to run the next query.
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_query_mem_cap.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 256, null);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    // The blocker fix itself: every fresh executor inherits a result-size cap by
    // default (not null), so an out-of-the-box server cannot be taken down by
    // one runaway query.
    try std.testing.expect(QueryExecutor.result_bytes_limit_default != null);

    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();
    // The executor inherited the process default; the fresh executor is capped.
    try std.testing.expect(exec.result_bytes_limit != null);

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
    };

    // A wide row (a fat TEXT payload) so the full-result materialisation is
    // large relative to the parse/planner overhead, and an UNINDEXED ORDER BY so
    // the executor must collect every row before sorting (can_stream = false).
    try run.q(&exec, allocator, "CREATE TABLE big (id INT PRIMARY KEY, v INT, pad TEXT)");
    const pad = try allocator.alloc(u8, 512);
    defer allocator.free(pad);
    @memset(pad, 'x');
    var i: usize = 0;
    while (i < 1500) : (i += 1) {
        const sql = try std.fmt.allocPrint(allocator, "INSERT INTO big (id, v, pad) VALUES ({d}, {d}, '{s}')", .{ i, 1500 - i, pad });
        defer allocator.free(sql);
        try run.q(&exec, allocator, sql);
    }

    const runaway = "SELECT * FROM big ORDER BY v";

    // Control: with the generous default cap, the query succeeds and returns
    // every row. This proves the query itself is valid and it is the cap (not
    // some unrelated bug) that stops it below.
    {
        const res = try exec.execute(.{ .sql = runaway });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1500), res.rows.len);
    }

    // Capped: lower this executor's result-size cap well below the ~0.8 MB full
    // materialisation. The runaway now turns into a clean error response; note
    // execute() returns normally (no crash, no propagated Zig error, no leak).
    exec.result_bytes_limit = 64 * 1024;
    {
        const res = try exec.execute(.{ .sql = runaway });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
        try std.testing.expectEqualStrings("Query Memory Limit Exceeded", res.error_message.?);
    }

    // The executor is still fully usable after a capped-out query: a small,
    // bounded query runs fine, demonstrating the server survives the runaway.
    {
        const res = try exec.execute(.{ .sql = "SELECT * FROM big WHERE id = 7" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
    }
}

test "vacuum skip: INSERT-only workload never triggers a vacuum scan; a delete re-engages it" {
    // Regression for the 10M load drag: the background vacuum used to full-scan
    // every table every pass even with nothing to reclaim, starving a bulk load.
    // Now an INSERT-only workload leaves `garbage_ops` at 0 so vacuum() is a
    // no-op, and only an UPDATE/DELETE (which creates a dead version) makes it
    // run again.
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_vacuum_skip.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
    };

    try run.q(&exec, allocator, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
    var i: usize = 0;
    while (i < 50) : (i += 1) {
        const sql = try std.fmt.allocPrint(allocator, "INSERT INTO t (id, v) VALUES ({d}, {d})", .{ i, i * 2 });
        defer allocator.free(sql);
        try run.q(&exec, allocator, sql);
    }

    // INSERTs create no reclaimable garbage: the counter stays 0.
    try std.testing.expectEqual(@as(u64, 0), db.garbage_ops.load(.monotonic));

    // vacuum() must short-circuit: nothing to do, and `last_vacuum_garbage_ops`
    // stays 0 (it is only advanced when a real pass runs).
    try db.vacuum();
    try std.testing.expectEqual(@as(u64, 0), db.last_vacuum_garbage_ops);

    // A DELETE supersedes a live version -> dead version -> counter advances.
    try run.q(&exec, allocator, "DELETE FROM t WHERE id = 7");
    try std.testing.expect(db.garbage_ops.load(.monotonic) > 0);

    // Now vacuum() actually runs (records the counter) and does not corrupt the
    // table: the surviving rows are still all present and readable.
    try db.vacuum();
    try std.testing.expect(db.last_vacuum_garbage_ops > 0);
    {
        const res = try exec.execute(.{ .sql = "SELECT id FROM t" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 49), res.rows.len);
    }

    // A second vacuum with no new churn short-circuits again (counter unchanged).
    const ops_after = db.garbage_ops.load(.monotonic);
    try db.vacuum();
    try std.testing.expectEqual(ops_after, db.last_vacuum_garbage_ops);
}

test "loose index scan: DISTINCT / COUNT(DISTINCT) skip duplicates (correct + fast path fires)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_loose_distinct.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        fn count(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !usize {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            return res.rows.len;
        }
        // First column of row n as i64.
        fn iAt(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, n: usize) !i64 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null and res.rows.len > n);
            return try std.fmt.parseInt(i64, res.rows[n][0], 10);
        }
    };

    // `dept` is NOT NULL + indexed: it qualifies for the loose scan. `note` is
    // nullable TEXT: it must NOT (variable-width encoding + NULLs), forcing the
    // safe scan-and-dedupe fallback.
    try run.q(&exec, allocator, "CREATE TABLE emp (id INT PRIMARY KEY, dept INT NOT NULL, note TEXT)");
    try run.q(&exec, allocator, "CREATE INDEX idx_dept ON emp (dept)");
    // 8 rows across 3 distinct depts {10,20,30}; inserted out of order.
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (1, 20, 'a')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (2, 10, 'b')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (3, 30, 'a')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (4, 20, 'c')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (5, 10, 'a')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (6, 30, 'b')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (7, 10, 'c')");
    try run.q(&exec, allocator, "INSERT INTO emp (id, dept, note) VALUES (8, 20, 'a')");

    const dbase = exec.loose_distinct_used;
    // SELECT DISTINCT dept -> 3 rows, ascending (skip-scan yields numeric order): 10,20,30.
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT DISTINCT dept FROM emp"));
    try std.testing.expectEqual(@as(i64, 10), try run.iAt(&exec, allocator, "SELECT DISTINCT dept FROM emp", 0));
    try std.testing.expectEqual(@as(i64, 20), try run.iAt(&exec, allocator, "SELECT DISTINCT dept FROM emp", 1));
    try std.testing.expectEqual(@as(i64, 30), try run.iAt(&exec, allocator, "SELECT DISTINCT dept FROM emp", 2));
    // COUNT(DISTINCT dept) = 3.
    try std.testing.expectEqual(@as(i64, 3), try run.iAt(&exec, allocator, "SELECT COUNT(DISTINCT dept) FROM emp", 0));
    // Both took the loose path.
    try std.testing.expect(exec.loose_distinct_used == dbase + 5); // 4 SELECT DISTINCT executions + 1 COUNT(DISTINCT)

    // NULL handling on a NULLABLE indexed numeric column: NULLs index as the
    // literal "NULL" (len != 16), so the loose scan still fires and must count
    // NULL OUT of COUNT(DISTINCT) yet keep exactly one NULL group in DISTINCT.
    try run.q(&exec, allocator, "CREATE TABLE emp2 (id INT PRIMARY KEY, mgr INT)");
    try run.q(&exec, allocator, "CREATE INDEX idx_mgr ON emp2 (mgr)");
    try run.q(&exec, allocator, "INSERT INTO emp2 (id, mgr) VALUES (1, 100)");
    try run.q(&exec, allocator, "INSERT INTO emp2 (id, mgr) VALUES (2, 200)");
    try run.q(&exec, allocator, "INSERT INTO emp2 (id) VALUES (3)"); // mgr omitted -> NULL
    try run.q(&exec, allocator, "INSERT INTO emp2 (id, mgr) VALUES (4, 100)");
    try run.q(&exec, allocator, "INSERT INTO emp2 (id) VALUES (5)"); // mgr omitted -> NULL
    const dnull = exec.loose_distinct_used;
    // SELECT DISTINCT mgr -> {100, 200, NULL} = 3 rows (SQL keeps one NULL group).
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT DISTINCT mgr FROM emp2"));
    // COUNT(DISTINCT mgr) -> 2 (NULL excluded).
    try std.testing.expectEqual(@as(i64, 2), try run.iAt(&exec, allocator, "SELECT COUNT(DISTINCT mgr) FROM emp2", 0));
    try std.testing.expect(exec.loose_distinct_used == dnull + 2); // both fast-pathed

    // Fallback: DISTINCT on the nullable TEXT column must NOT use the loose path,
    // and must still be correct: distinct notes {a,b,c} = 3.
    const dfb = exec.loose_distinct_used;
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT DISTINCT note FROM emp"));
    try std.testing.expect(exec.loose_distinct_used == dfb);

    // Soundness fallback: a DELETE flips the index inexact, so the next
    // DISTINCT/COUNT(DISTINCT) must NOT fast-path but must stay correct.
    // Remove all dept=10 rows -> distinct depts collapse to {20,30}.
    try run.q(&exec, allocator, "DELETE FROM emp WHERE dept = 10");
    const ddel = exec.loose_distinct_used;
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT DISTINCT dept FROM emp"));
    try std.testing.expectEqual(@as(i64, 2), try run.iAt(&exec, allocator, "SELECT COUNT(DISTINCT dept) FROM emp", 0));
    try std.testing.expect(exec.loose_distinct_used == ddel); // fell back
}

test "index-only GROUP BY aggregate over a covering (g,v) index (correct + fast path fires)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_group_covering.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        fn count(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !usize {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            return res.rows.len;
        }
        // Cell [row][col] as an owned string.
        fn cell(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, r: usize, c: usize) ![]const u8 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null and res.rows.len > r);
            return a.dupe(u8, res.rows[r][c]);
        }
    };
    const wantCell = struct {
        fn f(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, r: usize, c: usize, want: []const u8) !void {
            const got = try run.cell(e, a, sql, r, c);
            defer a.free(got);
            try std.testing.expectEqualStrings(want, got);
        }
    }.f;

    // Composite covering index (region, amt); amt is DOUBLE to exercise float decode.
    try run.q(&exec, allocator, "CREATE TABLE sales (id INT PRIMARY KEY, region INT NOT NULL, amt DOUBLE NOT NULL)");
    try run.q(&exec, allocator, "CREATE INDEX idx_region_amt ON sales (region, amt)");
    // region 1: {10.5, 20.5} sum 31 avg 15.5; region 2: {100.25}; region 3: {5.5,5.5,5.5} sum 16.5 avg 5.5.
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (1, 1, 10.5)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (2, 1, 20.5)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (3, 2, 100.25)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (4, 3, 5.5)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (5, 3, 5.5)");
    try run.q(&exec, allocator, "INSERT INTO sales (id, region, amt) VALUES (6, 3, 5.5)");

    const g0 = exec.index_group_aggs;
    // 3 groups, in region order (index-ordered).
    try std.testing.expectEqual(@as(usize, 3), try run.count(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region"));
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region", 0, 0, "1");
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region", 0, 1, "31");
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region", 1, 1, "100.25");
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region", 2, 1, "16.5");
    // AVG (fractional), MIN, MAX, COUNT(*) per group -> check region 1 and 3.
    try wantCell(&exec, allocator, "SELECT region, AVG(amt) AS a FROM sales GROUP BY region", 0, 1, "15.5");
    try wantCell(&exec, allocator, "SELECT region, AVG(amt) AS a FROM sales GROUP BY region", 2, 1, "5.5");
    try wantCell(&exec, allocator, "SELECT region, MIN(amt) AS mn FROM sales GROUP BY region", 0, 1, "10.5");
    try wantCell(&exec, allocator, "SELECT region, MAX(amt) AS mx FROM sales GROUP BY region", 0, 1, "20.5");
    try wantCell(&exec, allocator, "SELECT region, COUNT(*) AS c FROM sales GROUP BY region", 2, 1, "3");
    // ORDER BY the aggregate DESC + LIMIT 2 -> region 2 (100.25) then region 1 (31).
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 2", 0, 0, "2");
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 2", 1, 0, "1");
    // All of the above fast-pathed (12 group-agg executions: 5 SUM incl. count()
    // + 2 AVG + MIN + MAX + COUNT(*) + 2 ordered).
    try std.testing.expect(exec.index_group_aggs == g0 + 12);

    // Soundness fallback + cross-check against the hash GROUP BY path: a DELETE
    // flips the index inexact, so the same queries must NOT fast-path yet must
    // return IDENTICAL answers. Remove one region-3 row -> region 3 sum 11.
    try run.q(&exec, allocator, "DELETE FROM sales WHERE id = 6");
    const g1 = exec.index_group_aggs;
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region", 0, 1, "31");
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region", 2, 1, "11");
    try wantCell(&exec, allocator, "SELECT region, AVG(amt) AS a FROM sales GROUP BY region", 0, 1, "15.5");
    try wantCell(&exec, allocator, "SELECT region, SUM(amt) AS s FROM sales GROUP BY region ORDER BY s DESC LIMIT 2", 0, 0, "2");
    try std.testing.expect(exec.index_group_aggs == g1); // fell back to the hash path
}

test "composite-index prefix ordered scan: WHERE lead=const ORDER BY 2nd col (no sort, fast path fires)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_composite_ordered.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        // id (first column) of row n as i64.
        fn idAt(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, n: usize) !i64 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null and res.rows.len > n);
            return try std.fmt.parseInt(i64, res.rows[n][0], 10);
        }
        fn count(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !usize {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            return res.rows.len;
        }
    };

    try run.q(&exec, allocator, "CREATE TABLE t (id INT PRIMARY KEY, g INT NOT NULL, v DOUBLE NOT NULL)");
    try run.q(&exec, allocator, "CREATE INDEX idx_g_v ON t (g, v)");
    // g=1: v {30,10,20,25} (ids 1,2,3,5); g=2: v {5} (id 4).
    try run.q(&exec, allocator, "INSERT INTO t (id, g, v) VALUES (1, 1, 30.0)");
    try run.q(&exec, allocator, "INSERT INTO t (id, g, v) VALUES (2, 1, 10.0)");
    try run.q(&exec, allocator, "INSERT INTO t (id, g, v) VALUES (3, 1, 20.0)");
    try run.q(&exec, allocator, "INSERT INTO t (id, g, v) VALUES (4, 2, 5.0)");
    try run.q(&exec, allocator, "INSERT INTO t (id, g, v) VALUES (5, 1, 25.0)");

    const c0 = exec.composite_ordered_scans;
    // WHERE g=1 ORDER BY v DESC -> v 30,25,20,10 -> ids 1,5,3,2 (no sort; index order).
    try std.testing.expectEqual(@as(i64, 1), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 ORDER BY v DESC", 0));
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 ORDER BY v DESC", 1));
    try std.testing.expectEqual(@as(i64, 3), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 ORDER BY v DESC", 2));
    try std.testing.expectEqual(@as(i64, 2), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 ORDER BY v DESC", 3));
    // Residual on the 2nd column + streaming LIMIT: g=1 AND v>15 ORDER BY v DESC LIMIT 2 -> 30,25 -> ids 1,5.
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 ORDER BY v DESC LIMIT 2"));
    try std.testing.expectEqual(@as(i64, 1), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 ORDER BY v DESC LIMIT 2", 0));
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 ORDER BY v DESC LIMIT 2", 1));
    // ASC variant: g=1 ORDER BY v ASC LIMIT 2 -> 10,20 -> ids 2,3.
    try std.testing.expectEqual(@as(i64, 2), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 ORDER BY v ASC LIMIT 2", 0));
    try std.testing.expectEqual(@as(i64, 3), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 ORDER BY v ASC LIMIT 2", 1));
    // ASC + a lower bound on the sort column (the bounded-seek fix): g=1 AND
    // v>15 ORDER BY v ASC LIMIT 2 -> v 20,25 -> ids 3,5. The seek must start at
    // (g=1, v=15), NOT scan the v<=15 rows (10) first.
    try std.testing.expectEqual(@as(i64, 3), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 ORDER BY v ASC LIMIT 2", 0));
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 ORDER BY v ASC LIMIT 2", 1));
    // 3-way AND: both bounds on the sort column must be found even though the
    // WHERE parses as AND(AND(g=1, v>15), v<28). g=1 AND v>15 AND v<28 -> {20,25}
    // -> ASC -> ids 3,5. If the nested v>15 were missed the seek would start at
    // the prefix bottom (id-2 row, v=10) instead of at v=15.
    try std.testing.expectEqual(@as(usize, 2), try run.count(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 AND v < 28 ORDER BY v ASC"));
    try std.testing.expectEqual(@as(i64, 3), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 AND v < 28 ORDER BY v ASC", 0));
    try std.testing.expectEqual(@as(i64, 5), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 1 AND v > 15 AND v < 28 ORDER BY v ASC", 1));
    // Fourteen executions above used the composite prefix scan (11 as noted + 3
    // for the 3-way-AND bounded ASC query).
    try std.testing.expect(exec.composite_ordered_scans == c0 + 14);

    // Only g=1's rows appear (the prefix is bounded): DESC over g=2 -> just id 4.
    try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM t WHERE g = 2 ORDER BY v DESC"));
    try std.testing.expectEqual(@as(i64, 4), try run.idAt(&exec, allocator, "SELECT id FROM t WHERE g = 2 ORDER BY v DESC", 0));
}

test "MIN/MAX answered from index endpoints (values correct, fast path fires)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_minmax_idx.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();
    const R = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        fn c00(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) ![]const u8 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null and res.rows.len == 1);
            return a.dupe(u8, res.rows[0][0]);
        }
    };
    const eq = struct {
        fn f(a: std.mem.Allocator, e: *QueryExecutor, sql: []const u8, want: []const u8) !void {
            const g = try R.c00(e, a, sql);
            defer a.free(g);
            try std.testing.expectEqualStrings(want, g);
        }
    }.f;
    try R.q(&exec, allocator, "CREATE TABLE t (id INT PRIMARY KEY, v DOUBLE)");
    try R.q(&exec, allocator, "CREATE INDEX iv ON t (v)");
    try R.q(&exec, allocator, "INSERT INTO t (id,v) VALUES (1, 42.5)");
    try R.q(&exec, allocator, "INSERT INTO t (id,v) VALUES (2, 3.25)");
    try R.q(&exec, allocator, "INSERT INTO t (id,v) VALUES (3, 1000.75)");
    const before = exec.index_minmax_used;
    // NOT lexical: "1000.75" would be the lexical min; the numeric min is 3.25.
    try eq(allocator, &exec, "SELECT MIN(v) FROM t", "3.25");
    try eq(allocator, &exec, "SELECT MAX(v) FROM t", "1000.75");
    try std.testing.expect(exec.index_minmax_used == before + 2); // both took the index-endpoint path
    // With a WHERE the fast path is skipped but the answer must still be correct.
    try eq(allocator, &exec, "SELECT MAX(v) FROM t WHERE v < 100", "42.5");
    try std.testing.expect(exec.index_minmax_used == before + 2); // unchanged: fell back
}

test "aggregates over DOUBLE columns (SUM/AVG/MIN/MAX numeric, not integer/lexical)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_float_agg.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        // Nth row, Cth cell.
        fn cell(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8, r: usize, c: usize) ![]const u8 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expect(res.rows.len > r and res.rows[r].len > c);
            return a.dupe(u8, res.rows[r][c]);
        }
    };
    const eq = struct {
        fn f(a: std.mem.Allocator, e: *QueryExecutor, sql: []const u8, r: usize, c: usize, want: []const u8) !void {
            const got = try run.cell(e, a, sql, r, c);
            defer a.free(got);
            try std.testing.expectEqualStrings(want, got);
        }
    }.f;

    try run.q(&exec, allocator, "CREATE TABLE m (id INT PRIMARY KEY, g INT, v DOUBLE)");
    try run.q(&exec, allocator, "INSERT INTO m (id,g,v) VALUES (1,1,1.5)");
    try run.q(&exec, allocator, "INSERT INTO m (id,g,v) VALUES (2,1,2.5)");
    try run.q(&exec, allocator, "INSERT INTO m (id,g,v) VALUES (3,2,100.25)");

    // Ungrouped: SUM=104.25, AVG=34.75, MIN=1.5, MAX=100.25 (NOT lexical: "2.5"<"100.25").
    try eq(allocator, &exec, "SELECT SUM(v) FROM m", 0, 0, "104.25");
    try eq(allocator, &exec, "SELECT AVG(v) FROM m", 0, 0, "34.75");
    try eq(allocator, &exec, "SELECT MIN(v) FROM m", 0, 0, "1.5");
    try eq(allocator, &exec, "SELECT MAX(v) FROM m", 0, 0, "100.25");
    // Grouped (Q10's path): g=1 -> sum 4, avg 2 ; g=2 -> sum 100.25, avg 100.25.
    try eq(allocator, &exec, "SELECT g, SUM(v) FROM m GROUP BY g ORDER BY g", 0, 1, "4");
    try eq(allocator, &exec, "SELECT g, AVG(v) FROM m GROUP BY g ORDER BY g", 0, 1, "2");
    try eq(allocator, &exec, "SELECT g, MAX(v) FROM m GROUP BY g ORDER BY g", 1, 1, "100.25");
    // Integer column still prints as an integer (no ".0").
    try eq(allocator, &exec, "SELECT SUM(g) FROM m", 0, 0, "4"); // 1+1+2
}

test "compaction: rebuild packs and preserves visible rows (drops dead versions)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const src_dir = "test_compact_src";
    const dst_dir = "test_compact_dst";
    try Io.Dir.createDirPath(.cwd(), io, src_dir);
    try Io.Dir.createDirPath(.cwd(), io, dst_dir);
    defer {
        Io.Dir.deleteTree(.cwd(), io, src_dir) catch {};
        Io.Dir.deleteTree(.cwd(), io, dst_dir) catch {};
    }

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const run = struct {
        fn q(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !void {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
        }
        fn count(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) !usize {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            return res.rows.len;
        }
        fn cell(e: *QueryExecutor, a: std.mem.Allocator, sql: []const u8) ![]const u8 {
            const res = try e.execute(.{ .sql = sql });
            defer freeResp(a, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expect(res.rows.len == 1);
            return a.dupe(u8, res.rows[0][0]);
        }
    };

    // Build a small source db: 5 rows, then delete one and update one so the
    // version chains have dead/superseded versions for compaction to drop.
    {
        var db = try Database.open(allocator, io, src_dir ++ "/nova.db", 64, null);
        defer db.close();
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();
        try run.q(&exec, allocator, "CREATE TABLE t (id INT PRIMARY KEY, v INT)");
        try run.q(&exec, allocator, "CREATE INDEX idx_v ON t (v)");
        var i: i64 = 1;
        while (i <= 5) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO t (id, v) VALUES ({d}, {d})", .{ i, i * 100 });
            defer allocator.free(sql);
            try run.q(&exec, allocator, sql);
        }
        try run.q(&exec, allocator, "DELETE FROM t WHERE id = 3"); // gone after compaction
        try run.q(&exec, allocator, "UPDATE t SET v = 999 WHERE id = 5"); // new value wins
        try db.pool.flushAllPages();
    }

    try @import("compact.zig").compact(allocator, io, src_dir, dst_dir);

    // Reopen the compacted db and verify the visible state survived exactly:
    // 4 rows (id 3 dropped), id=5 has the updated value, the index still finds
    // rows, and COUNT is correct.
    {
        var db = try Database.open(allocator, io, dst_dir ++ "/nova.db", 64, null);
        defer db.close();
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();
        try std.testing.expectEqual(@as(usize, 4), try run.count(&exec, allocator, "SELECT id FROM t"));
        try std.testing.expectEqual(@as(usize, 0), try run.count(&exec, allocator, "SELECT id FROM t WHERE id = 3"));
        const v5 = try run.cell(&exec, allocator, "SELECT v FROM t WHERE id = 5");
        defer allocator.free(v5);
        try std.testing.expectEqualStrings("999", v5);
        // Index-driven lookup still works after the rebuild.
        try std.testing.expectEqual(@as(usize, 1), try run.count(&exec, allocator, "SELECT id FROM t WHERE v = 999"));
        try std.testing.expectEqual(@as(usize, 0), try run.count(&exec, allocator, "SELECT id FROM t WHERE v = 300")); // deleted row's value
        const cnt = try run.cell(&exec, allocator, "SELECT COUNT(*) FROM t");
        defer allocator.free(cnt);
        try std.testing.expectEqualStrings("4", cnt);
    }
}

test "database end-to-end" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_end_to_end.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;

    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    {
        std.debug.print("E2E: STEP 1 - Create Table...\n", .{});
        const res = try exec.execute(.{ .sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT, age INT)" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
    }

    {
        std.debug.print("E2E: STEP 2 - Insert rows...\n", .{});
        const res1 = try exec.execute(.{ .sql = "INSERT INTO users (id, name, age) VALUES (1, 'Alice', 25)" });
        defer freeResp(allocator, res1);
        try std.testing.expect(res1.error_message == null);

        const res2 = try exec.execute(.{ .sql = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 19)" });
        defer freeResp(allocator, res2);
        try std.testing.expect(res2.error_message == null);
    }

    {
        std.debug.print("E2E: STEP 3 - Select rows...\n", .{});
        const res = try exec.execute(.{ .sql = "SELECT name, age FROM users WHERE age > 20" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 2), res.columns.len);
        try std.testing.expectEqualStrings("name", res.columns[0]);
        try std.testing.expectEqualStrings("age", res.columns[1]);

        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("Alice", res.rows[0][0]);
        try std.testing.expectEqualStrings("25", res.rows[0][1]);
    }

    {
        std.debug.print("E2E: STEP 4 - Update row...\n", .{});
        const res = try exec.execute(.{ .sql = "UPDATE users SET age = 30 WHERE id = 1" });
        defer freeResp(allocator, res);
        if (res.error_message) |msg| {
            std.debug.print("UPDATE ERROR: {s}\n", .{msg});
        }
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
    }

    {
        const res = try exec.execute(.{ .sql = "SELECT age FROM users WHERE id = 1" });
        defer freeResp(allocator, res);
        if (res.error_message) |msg| {
            std.debug.print("SELECT ERROR: {s}\n", .{msg});
        }
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("30", res.rows[0][0]);
    }

    {
        const res = try exec.execute(.{ .sql = "DELETE FROM users WHERE id = 2" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
    }

    {
        const res = try exec.execute(.{ .sql = "DROP TABLE users" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
    }

    {
        const csv_content =
            \\SalesOrderID,EmployeeID,OrderDate,CustomerID,SubTotal,TotalDue
            \\1,2,2026-07-09,5,100.50,110.00
            \\
        ;
        try Io.Dir.writeFile(.cwd(), io, .{ .sub_path = "temp_orders.csv", .data = csv_content });
        defer Io.Dir.deleteFile(.cwd(), io, "temp_orders.csv") catch {};

        const yaml_content =
            \\store: orders
            \\format: csv
            \\output_dir: .
            \\entities:
            \\  - name: orders
            \\    role: parent
            \\    file: temp_orders.csv
            \\    fields:
            \\      - name: SalesOrderID
            \\        type: int
            \\      - name: EmployeeID
            \\        type: int
            \\      - name: OrderDate
            \\        type: text
            \\      - name: CustomerID
            \\        type: int
            \\      - name: SubTotal
            \\        type: double
            \\      - name: TotalDue
            \\        type: double
            \\
        ;
        try Io.Dir.writeFile(.cwd(), io, .{ .sub_path = "temp_import.yaml", .data = yaml_content });
        defer Io.Dir.deleteFile(.cwd(), io, "temp_import.yaml") catch {};

        {
            const res = try exec.execute(.{ .sql = "CREATE TABLE orders (SalesOrderID INT PRIMARY KEY, EmployeeID INT, OrderDate TEXT, CustomerID INT, SubTotal INT, TotalDue INT)" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "IMPORT MANIFEST 'temp_import.yaml'" });
            defer freeResp(allocator, res);
            if (res.error_message) |msg| {
                std.debug.print("IMPORT MANIFEST ERROR: {s}\n", .{msg});
            }
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT SalesOrderID, CustomerID FROM orders WHERE SalesOrderID = 1" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("1", res.rows[0][0]);
            try std.testing.expectEqualStrings("5", res.rows[0][1]);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT SalesOrderID FROM orders WHERE SalesOrderID = 999" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 0), res.rows.len);
        }

        {
            const res = try exec.execute(.{ .sql = "DELETE FROM orders WHERE SalesOrderID = 999" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(u64, 0), res.rows_affected);
        }

        {
            const res = try exec.execute(.{ .sql = "DELETE FROM orders WHERE SalesOrderID = 1" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT SalesOrderID FROM orders WHERE SalesOrderID = 1" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 0), res.rows.len);
        }

        {
            const res = try exec.execute(.{ .sql = "DROP TABLE orders" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }
    }

    {
        {
            const res = try exec.execute(.{ .sql = "CREATE TABLE customers (id INT PRIMARY KEY, name TEXT, age INT)" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO customers (id, name, age) VALUES (1, 'Alice', 25)" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }
        {
            const res = try exec.execute(.{ .sql = "INSERT INTO customers (id, name, age) VALUES (2, 'Bob', 30)" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "CREATE INDEX idx_age ON customers (age)" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT id, name FROM customers WHERE age = 25" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("1", res.rows[0][0]);
            try std.testing.expectEqualStrings("Alice", res.rows[0][1]);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO customers (id, name, age) VALUES (3, 'Charlie', 25)" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT id, name FROM customers WHERE age = 25" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 2), res.rows.len);
        }

        {
            const res = try exec.execute(.{ .sql = "UPDATE customers SET age = 30 WHERE id = 3" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT id FROM customers WHERE age = 25" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("1", res.rows[0][0]);
        }

        {
            const res = try exec.execute(.{ .sql = "DELETE FROM customers WHERE id = 2" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "DROP INDEX idx_age ON customers" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }

        {
            const Context = struct {
                db: *Database,
                thread_id: usize,
                allocator: std.mem.Allocator,

                fn runReader(ctx: @This()) void {
                    var local_exec = QueryExecutor.init(ctx.allocator, ctx.db);
                    defer local_exec.deinit();

                    var i: usize = 0;
                    while (i < 30) : (i += 1) {
                        const res = local_exec.execute(.{ .sql = "SELECT id, name FROM customers WHERE age = 25" }) catch |err| {
                            std.debug.panic("Reader thread failed: {any}", .{err});
                        };
                        if (res.error_message) |m| ctx.allocator.free(m);
                        for (res.columns) |c| ctx.allocator.free(c);
                        ctx.allocator.free(res.columns);
                        for (res.rows) |r| {
                            for (r) |c| ctx.allocator.free(c);
                            ctx.allocator.free(r);
                        }
                        ctx.allocator.free(res.rows);
                        if (res.column_types.len > 0) ctx.allocator.free(res.column_types);
                    }
                }

                fn runWriter(ctx: @This()) void {
                    var local_exec = QueryExecutor.init(ctx.allocator, ctx.db);
                    defer local_exec.deinit();

                    var i: usize = 0;
                    while (i < 10) : (i += 1) {
                        var buf: [128]u8 = undefined;
                        const sql = std.fmt.bufPrint(&buf, "INSERT INTO customers (id, name, age) VALUES ({d}, 'User', 25)", .{ 100 + ctx.thread_id * 10 + i }) catch unreachable;
                        const res = local_exec.execute(.{ .sql = sql }) catch |err| {
                            std.debug.panic("Writer thread failed: {any}", .{err});
                        };
                        if (res.error_message) |m| ctx.allocator.free(m);
                        for (res.columns) |c| ctx.allocator.free(c);
                        ctx.allocator.free(res.columns);
                        for (res.rows) |r| {
                            for (r) |c| ctx.allocator.free(c);
                            ctx.allocator.free(r);
                        }
                        ctx.allocator.free(res.rows);
                        if (res.column_types.len > 0) ctx.allocator.free(res.column_types);
                    }
                }
            };

            const t1 = try std.Thread.spawn(.{}, Context.runReader, .{Context{ .db = db, .thread_id = 1, .allocator = allocator }});
            const t2 = try std.Thread.spawn(.{}, Context.runReader, .{Context{ .db = db, .thread_id = 2, .allocator = allocator }});
            const t3 = try std.Thread.spawn(.{}, Context.runReader, .{Context{ .db = db, .thread_id = 3, .allocator = allocator }});
            const t4 = try std.Thread.spawn(.{}, Context.runWriter, .{Context{ .db = db, .thread_id = 4, .allocator = allocator }});
            const t5 = try std.Thread.spawn(.{}, Context.runWriter, .{Context{ .db = db, .thread_id = 5, .allocator = allocator }});

            t1.join();
            t2.join();
            t3.join();
            t4.join();
            t5.join();
        }

        {
            const res = try exec.execute(.{ .sql = "DROP TABLE customers" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
        }
    }

    {
        const queries = [_][]const u8{
            "BEGIN",
            "COMMIT",
            "BEGIN TRANSACTION",
            "COMMIT TRANSACTION",
            "BEGIN",
            "ROLLBACK",
            "BEGIN TRANSACTION",
            "ROLLBACK TRANSACTION",
        };

        for (queries) |sql| {
            const res = try exec.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            if (res.error_message) |msg| {
                std.debug.print("TX TEST ERROR for '{s}': {s}\n", .{sql, msg});
            }
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(u64, 0), res.rows_affected);
        }
    }
}

test "database transaction isolation" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_isolation.db";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec1 = QueryExecutor.init(allocator, db);
    defer exec1.deinit();
    var exec2 = QueryExecutor.init(allocator, db);
    defer exec2.deinit();

    {
        const res = try exec1.execute(.{ .sql = "CREATE TABLE test_iso (id INT PRIMARY KEY, val TEXT)" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec1.execute(.{ .sql = "BEGIN" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec1.execute(.{ .sql = "INSERT INTO test_iso (id, val) VALUES (1, 'hello')" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec2.execute(.{ .sql = "SELECT val FROM test_iso WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 0), res.rows.len);
    }

    {
        const res = try exec1.execute(.{ .sql = "COMMIT" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec2.execute(.{ .sql = "SELECT val FROM test_iso WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("hello", res.rows[0][0]);
    }

    {
        const res = try exec1.execute(.{ .sql = "BEGIN" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }
    {
        const res = try exec1.execute(.{ .sql = "INSERT INTO test_iso (id, val) VALUES (2, 'world')" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec1.execute(.{ .sql = "ROLLBACK" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec2.execute(.{ .sql = "SELECT val FROM test_iso WHERE id = 2" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 0), res.rows.len);
    }
}

test "database crash recovery" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_recovery.db";
    const wal_dir = "test_wal_recovery";
    
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    Io.Dir.deleteFile(.cwd(), io, "test_wal_recovery/000000.wal") catch {};
    Io.Dir.deleteFile(.cwd(), io, "test_wal_recovery/000001.wal") catch {};
    Io.Dir.deleteFile(.cwd(), io, "test_wal_recovery/000002.wal") catch {};

    defer {
        Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
        Io.Dir.deleteFile(.cwd(), io, "test_wal_recovery/000000.wal") catch {};
        Io.Dir.deleteFile(.cwd(), io, "test_wal_recovery/000001.wal") catch {};
        Io.Dir.deleteFile(.cwd(), io, "test_wal_recovery/000002.wal") catch {};
    }

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();

        {
            const res = try exec.execute(.{ .sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }
        {
            const res = try exec.execute(.{ .sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }
        {
            const res = try exec.execute(.{ .sql = "INSERT INTO users (id, name) VALUES (2, 'Bob')" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }
        {
            const res = try exec.execute(.{ .sql = "UPDATE users SET name = 'Alice Updated' WHERE id = 1" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "BEGIN" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }
        {
            const res = try exec.execute(.{ .sql = "INSERT INTO users (id, name) VALUES (3, 'Charlie')" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        try db.pool.flushAllPages();

        @atomicStore(bool, &db.is_closed, true, .seq_cst);
        db.group.cancel(db.pool.pager.io);
        while (@atomicLoad(bool, &db.bg_task_running, .seq_cst)) {
            const delay = std.Io.Duration.fromMilliseconds(1);
            db.pool.pager.io.sleep(delay, .real) catch {};
        }

        if (db.wal) |w| {
            w.deinit() catch {};
            // Mirror crashSimulate: once the WAL is freed, drop the pool's gate to
            // it so the teardown flush below cannot dereference the dangling WAL.
            db.pool.wal_gate = null;
        }
        db.master_tree.deinit();
        db.catalog.deinit();
        var table_it = db.table_roots.keyIterator();
        while (table_it.next()) |k| {
            allocator.free(k.*);
        }
        db.table_roots.deinit();
        var index_it = db.index_roots.keyIterator();
        while (index_it.next()) |k| {
            allocator.free(k.*);
        }
        db.index_roots.deinit();
        db.security_manager.deinit();
        db.txn_manager.deinit();
        db.undo_log.deinit();
        db.deinitTableLocks();
        db.deinitCachedTrees();
        db.deinitQueryCache();
        if (db.base_dir.len > 0) allocator.free(db.base_dir);
        db.pool.deinit() catch {};
        allocator.destroy(db);
    }

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db.close();

        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();

        {
            const res = try exec.execute(.{ .sql = "SELECT name FROM users WHERE id = 1" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("Alice Updated", res.rows[0][0]);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT name FROM users WHERE id = 2" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("Bob", res.rows[0][0]);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT name FROM users WHERE id = 3" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 0), res.rows.len);
        }
    }
}

/// The low-level binary wire codec (frame headers, length-prefixed fields, typed
/// value encode/decode) from `common/proto.zig`.
///
/// This is the byte-level contract between NovaDB and any client, most importantly
/// Nova's `nova-novadb` driver. It is re-exported here so a library consumer can
/// reach the frame types without depending on the internal module path. See
/// [`proto`] for the higher-level message protocol layered on top of these frames.
pub const wire_proto = @import("common/proto.zig");

/// The newer binary wire protocol (`Frontend`/`DocOp`, startup + `doc_op`
/// framing) used by the session loop and the document surface. Exposed (under a
/// non-`wire` name to avoid shadowing the local `wire` in this file's tests) so
/// dependents (e.g. the YCSB harness's document backend) can reuse the frame
/// encoders/decoders instead of hand-rolling them.
pub const proto_wire = @import("proto/wire.zig");

/// Re-export the `tls` dependency module so a dependent that imports `btree` gets
/// the SAME module instance the engine uses, rather than pulling its own copy
/// (which collides as `tls`/`tls0` in the build graph).
pub const tls = @import("tls");
/// The server-side message protocol from `proto/protocol.zig`: the request/response
/// message kinds, session control, and command dispatch layered over [`wire_proto`].
///
/// Re-exported alongside [`wire_proto`] so both halves of the client-facing seam
/// (raw frames plus the message semantics) are discoverable from the crate root.
pub const proto = @import("proto/protocol.zig");

test "database security and user management" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_security.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();

    db.security_manager.enabled = true;

    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    {
        const res = try exec.execute(.{ .sql = "CREATE TABLE dummy (id INT)" });
        defer if (res.error_message) |m| allocator.free(m);
        try std.testing.expect(res.error_message != null);
        try std.testing.expect(std.mem.indexOf(u8, res.error_message.?, "Authentication Required") != null);
    }

    var session_token: ?[]const u8 = null;
    defer if (session_token) |t| allocator.free(t);
    {
        const res = try exec.execute(.{ .sql = "LOGIN admin 'admin'" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        session_token = try allocator.dupe(u8, res.rows[0][0]);
    }

    {
        const res = try exec.execute(.{
            .sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)",
            .session_token = session_token,
        });
        defer if (res.error_message) |m| allocator.free(m);
        if (res.error_message) |msg| {
            std.debug.print("CREATE TABLE ERROR: {s}\n", .{msg});
        }
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec.execute(.{
            .sql = "CREATE USER guest IDENTIFIED BY 'guestpwd' ROLE 'read_only'",
            .session_token = session_token,
        });
        defer if (res.error_message) |m| allocator.free(m);
        if (res.error_message) |msg| {
            std.debug.print("CREATE USER ERROR: {s}\n", .{msg});
        }
        try std.testing.expect(res.error_message == null);
    }

    var guest_token: ?[]const u8 = null;
    defer if (guest_token) |t| allocator.free(t);
    {
        const res = try exec.execute(.{ .sql = "LOGIN guest 'guestpwd'" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        guest_token = try allocator.dupe(u8, res.rows[0][0]);
    }

    {
        const res = try exec.execute(.{
            .sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            .session_token = guest_token,
        });
        defer if (res.error_message) |m| allocator.free(m);
        try std.testing.expect(res.error_message != null);
        try std.testing.expect(std.mem.indexOf(u8, res.error_message.?, "Permission Denied") != null);
    }

    {
        const res = try exec.execute(.{
            .sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')",
            .session_token = session_token,
        });
        defer if (res.error_message) |m| allocator.free(m);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec.execute(.{
            .sql = "SELECT id, name FROM users",
            .session_token = guest_token,
        });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("Alice", res.rows[0][1]);
    }

    {
        const res = try exec.execute(.{
            .sql = "DROP USER guest",
            .session_token = session_token,
        });
        if (res.error_message) |msg| {
            std.debug.print("DROP USER ERROR: {s}\n", .{msg});
        }
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec.execute(.{ .sql = "LOGIN guest 'guestpwd'" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
    }
}

test "database alter table schema evolution" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_alter.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    {
        var db = try Database.open(allocator, io, db_path, 64, null);
        defer db.close();

        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();

        {
            const res = try exec.execute(.{ .sql = "CREATE TABLE users (id INT PRIMARY KEY, name TEXT)" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO users (id, name) VALUES (1, 'Alice')" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "ALTER TABLE users ADD COLUMN age INT DEFAULT 25" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT id, name, age FROM users" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("25", res.rows[0][2]);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO users (id, name, age) VALUES (2, 'Bob', 30)" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT id, name, age FROM users WHERE id = 2" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("30", res.rows[0][2]);
        }

        {
            const res = try exec.execute(.{ .sql = "ALTER TABLE users RENAME TO members" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT id FROM users" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message != null);
        }

        {
            const res = try exec.execute(.{ .sql = "SELECT name, age FROM members WHERE id = 2" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("Bob", res.rows[0][0]);
            try std.testing.expectEqualStrings("30", res.rows[0][1]);
        }
    }
}

test "database foreign key constraint validation" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_fk.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    {
        var db = try Database.open(allocator, io, db_path, 64, null);
        defer db.close();

        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();

        {
            const res = try exec.execute(.{ .sql = "CREATE TABLE departments (id INT PRIMARY KEY, name TEXT)" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "CREATE TABLE employees (id INT PRIMARY KEY, name TEXT, dept_id INT REFERENCES departments(id))" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 10)" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message != null);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO departments (id, name) VALUES (10, 'Engineering')" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "INSERT INTO employees (id, name, dept_id) VALUES (1, 'Alice', 10)" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "DELETE FROM departments WHERE id = 10" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message != null);
        }

        {
            const res = try exec.execute(.{ .sql = "UPDATE departments SET id = 20 WHERE id = 10" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message != null);
        }

        {
            const res = try exec.execute(.{ .sql = "DELETE FROM employees WHERE id = 1" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }

        {
            const res = try exec.execute(.{ .sql = "DELETE FROM departments WHERE id = 10" });
            defer if (res.error_message) |m| allocator.free(m);
            try std.testing.expect(res.error_message == null);
        }
    }
}

/// A single write-ahead-log record as shipped to replicas: LSN, transaction id,
/// timestamp, record kind, and the borrowed `table_name`/`key`/`value` byte slices.
///
/// Aliased here because it is the currency of the replication tests, both as the
/// WAL ship-callback payload captured by [`ReplCapture`] and as the input to
/// [`frameOf`] / [`replayTxns`].
const LogRecord = @import("common/common.zig").LogRecord;

/// A test sink that captures the WAL records a leader ships, so a replication test
/// can replay them into a follower and compare state.
///
/// Registered as a WAL `ship_callback` (see [`ReplCapture.cb`]); every committed
/// mutation on the leader drives one callback, and this struct accumulates a
/// deep-copied [`LogRecord`] per call. The copies are owned by this struct and freed
/// in [`ReplCapture.deinit`], because the WAL only lends its slices for the duration
/// of the callback.
const ReplCapture = struct {
    /// Allocator that owns the deep-copied record fields; the caller must keep it
    /// alive until [`ReplCapture.deinit`] runs.
    allocator: std.mem.Allocator,
    /// The captured records in ship order. Each entry's `table_name`/`key`/`value`
    /// are heap copies owned by this list, not borrows of the WAL's buffers.
    records: std.ArrayList(LogRecord) = .empty,

    /// WAL ship-callback: deep-copies `record` and appends it to [`ReplCapture.records`].
    ///
    /// `ctx` is the type-erased `*ReplCapture` the WAL was handed as its
    /// `replication_manager`. The three variable-length fields are duplicated because
    /// the WAL reuses its buffers after the callback returns; an allocation failure
    /// silently drops the record (`catch return`) rather than propagating, since a
    /// `ship_callback` cannot fail the commit path.
    fn cb(ctx: ?*anyopaque, record: LogRecord) void {
        const self: *ReplCapture = @ptrCast(@alignCast(ctx.?));
        const dup = LogRecord{
            .lsn = record.lsn,
            .tx_id = record.tx_id,
            .timestamp = record.timestamp,
            .kind = record.kind,
            .table_name = self.allocator.dupe(u8, record.table_name) catch return,
            .key = self.allocator.dupe(u8, record.key) catch return,
            .value = self.allocator.dupe(u8, record.value) catch return,
        };
        self.records.append(self.allocator, dup) catch return;
    }

    /// Frees the deep-copied `table_name`/`key`/`value` of every captured record and
    /// then the backing list. Must be called (via `defer`) or the test leaks.
    fn deinit(self: *ReplCapture) void {
        for (self.records.items) |r| {
            self.allocator.free(r.table_name);
            self.allocator.free(r.key);
            self.allocator.free(r.value);
        }
        self.records.deinit(self.allocator);
    }
};

/// Frees every heap allocation owned by a query-executor result, in one call.
///
/// `execute` returns an owning result: an optional `error_message`, an array of
/// column-name strings, an array of rows where each row is an array of cell strings,
/// and an optional `column_types` array. This walks all of that and releases it with
/// `allocator`. `res` is `anytype` so it matches whatever concrete result struct the
/// executor yields without naming it. Every test that calls `execute` must pair it
/// with this (usually `defer freeResp(...)`) or it leaks under `std.testing.allocator`,
/// which fails the test. `column_types` is freed only when non-empty, matching how the
/// executor allocates it.
fn freeResp(allocator: std.mem.Allocator, res: anytype) void {
    if (res.error_message) |m| allocator.free(m);
    for (res.columns) |c| allocator.free(c);
    allocator.free(res.columns);
    for (res.rows) |r| {
        for (r) |c| allocator.free(c);
        allocator.free(r);
    }
    allocator.free(res.rows);
    if (res.column_types.len > 0) allocator.free(res.column_types);
}

test "replication: two-node apply consistency" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const leader_path = "test_repl_leader.db";
    const follower_path = "test_repl_follower.db";
    const leader_wal = "test_repl_leader_wal";
    const follower_wal = "test_repl_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();

    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    try std.testing.expect(leader.wal != null);
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = &cap;
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();

    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT, age INT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name, age) VALUES (1, 'Alice', 25)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name, age) VALUES (2, 'Bob', 19)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name, age) VALUES (3, 'Cara', 33)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE INDEX idx_kv_age ON kv (age)" }));

    try std.testing.expect(cap.records.items.len > 0);

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    _ = try follower.applyStream(cap.records.items);

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();

    const q = "SELECT id, name, age FROM kv ORDER BY id";
    const lres = try lex.execute(.{ .sql = q });
    defer freeResp(allocator, lres);
    const fres = try fex.execute(.{ .sql = q });
    defer freeResp(allocator, fres);

    try std.testing.expect(lres.error_message == null);
    try std.testing.expect(fres.error_message == null);
    try std.testing.expectEqual(@as(usize, 3), lres.rows.len);
    try std.testing.expectEqual(lres.rows.len, fres.rows.len);
    for (lres.rows, fres.rows) |lrow, frow| {
        try std.testing.expectEqual(lrow.len, frow.len);
        for (lrow, frow) |lc, fc| {
            try std.testing.expectEqualStrings(lc, fc);
        }
    }

    const follower_has_index = for (follower.catalog.indexes.items) |ix| {
        if (std.mem.eql(u8, ix.name, "idx_kv_age")) break true;
    } else false;
    try std.testing.expect(follower_has_index);

    const qi = "SELECT id, name FROM kv WHERE age = 25";
    const lqi = try lex.execute(.{ .sql = qi });
    defer freeResp(allocator, lqi);
    const fqi = try fex.execute(.{ .sql = qi });
    defer freeResp(allocator, fqi);
    try std.testing.expect(lqi.error_message == null);
    try std.testing.expect(fqi.error_message == null);
    try std.testing.expectEqual(@as(usize, 1), lqi.rows.len);
    try std.testing.expectEqual(lqi.rows.len, fqi.rows.len);
    try std.testing.expectEqualStrings("1", fqi.rows[0][0]);
    try std.testing.expectEqualStrings("Alice", fqi.rows[0][1]);

    freeResp(allocator, try lex.execute(.{ .sql = "UPDATE kv SET age = 30 WHERE id = 2" }));
    freeResp(allocator, try lex.execute(.{ .sql = "DELETE FROM kv WHERE id = 3" }));
    _ = try follower.applyStream(cap.records.items);

    const q2 = "SELECT id, name, age FROM kv ORDER BY id";
    const l2 = try lex.execute(.{ .sql = q2 });
    defer freeResp(allocator, l2);
    const f2 = try fex.execute(.{ .sql = q2 });
    defer freeResp(allocator, f2);
    try std.testing.expect(l2.error_message == null);
    try std.testing.expect(f2.error_message == null);
    try std.testing.expectEqual(@as(usize, 2), l2.rows.len);
    try std.testing.expectEqual(l2.rows.len, f2.rows.len);
    for (l2.rows, f2.rows) |lrow, frow| {
        try std.testing.expectEqual(lrow.len, frow.len);
        for (lrow, frow) |lc, fc| try std.testing.expectEqualStrings(lc, fc);
    }
    try std.testing.expectEqualStrings("2", f2.rows[1][0]);
    try std.testing.expectEqualStrings("Bob", f2.rows[1][1]);
    try std.testing.expectEqualStrings("30", f2.rows[1][2]);

    const q3 = "SELECT id, name FROM kv WHERE age = 30";
    const l3 = try lex.execute(.{ .sql = q3 });
    defer freeResp(allocator, l3);
    const f3 = try fex.execute(.{ .sql = q3 });
    defer freeResp(allocator, f3);
    try std.testing.expect(l3.error_message == null);
    try std.testing.expect(f3.error_message == null);
    try std.testing.expectEqual(l3.rows.len, f3.rows.len);
    try std.testing.expectEqual(@as(usize, 1), f3.rows.len);
    try std.testing.expectEqualStrings("2", f3.rows[0][0]);
    try std.testing.expectEqualStrings("Bob", f3.rows[0][1]);
}

/// Serialises a [`LogRecord`] to its on-the-wire byte frame and returns an owned copy.
///
/// Drives the same `LogRecord.serialize` codec the leader uses when shipping to a
/// follower, so the replication tests can assert on the exact bytes and feed them
/// back through the follower's decoder. The returned slice is `allocator`-owned and
/// the caller frees it; the intermediate `Allocating` writer is released here.
fn frameOf(allocator: std.mem.Allocator, rec: LogRecord) ![]u8 {
    var out = std.Io.Writer.Allocating.init(allocator);
    defer out.deinit();
    try rec.serialize(&out.writer);
    return allocator.dupe(u8, out.written());
}

/// Feeds a batch of [`LogRecord`]s through a `DurableReplicator`, flushing at commits.
///
/// Calls `DurableReplicator.onRecord` for each record and, on a `.commit` record,
/// invokes `shipPending(durable)` so the accumulated transaction is shipped as a unit,
/// mirroring how the live replicator batches per transaction. `durable` selects
/// whether the ship must be quorum-acked (the RPO=0 path). Ship errors are swallowed
/// (`catch {}`) because these tests assert on the replicator's resulting state, not on
/// the ship call's return.
fn replayTxns(dr: *@import("query/replication.zig").DurableReplicator, records: []const LogRecord, durable: bool) void {
    const DR = @import("query/replication.zig").DurableReplicator;
    for (records) |rec| {
        DR.onRecord(dr, rec);
        if (rec.kind == .commit) dr.shipPending(durable) catch {};
    }
}

test "replication R2: fencing epoch + wire frames + follower ack" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const leader_path = "test_r2_leader.db";
    const follower_path = "test_r2_follower.db";
    const leader_wal = "test_r2_leader_wal";
    const follower_wal = "test_r2_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (1, 'Alice')" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (2, 'Bob')" }));
    try std.testing.expect(cap.records.items.len > 0);

    const frames = try allocator.alloc([]const u8, cap.records.items.len);
    defer {
        for (frames) |f| allocator.free(f);
        allocator.free(frames);
    }
    for (cap.records.items, 0..) |rec, i| frames[i] = try frameOf(allocator, rec);

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, follower_wal);

    const batch = rp.ReplFrames{ .epoch = 5, .base_seq = 1, .frames = frames };
    const wire = try batch.serialize(allocator);
    defer allocator.free(wire);
    var rx = try rp.ReplFrames.deserialize(allocator, wire);
    defer rx.deinit(allocator);
    try std.testing.expectEqual(@as(u64, 5), rx.epoch);
    try std.testing.expectEqual(cap.records.items.len, rx.frames.len);

    const r1 = try f.recvFrames(rx);
    try std.testing.expectEqual(repl.RecvOutcome.applied, r1.outcome);
    try std.testing.expectEqual(@as(u64, 5), r1.ack.epoch);
    try std.testing.expectEqual(@as(u64, rx.frames.len), r1.ack.confirmed_seq);

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    {
        const res = try fex.execute(.{ .sql = "SELECT id, name FROM kv ORDER BY id" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 2), res.rows.len);
        try std.testing.expectEqualStrings("Alice", res.rows[0][1]);
        try std.testing.expectEqualStrings("Bob", res.rows[1][1]);
    }

    const stale_frame = try frameOf(allocator, cap.records.items[cap.records.items.len - 1]);
    defer allocator.free(stale_frame);
    const stale_frames = [_][]const u8{stale_frame};
    const stale = rp.ReplFrames{ .epoch = 3, .base_seq = r1.ack.confirmed_seq + 1, .frames = &stale_frames };
    const rs = try f.recvFrames(stale);
    try std.testing.expectEqual(repl.RecvOutcome.fenced, rs.outcome);
    try std.testing.expectEqual(@as(u64, 5), rs.ack.epoch);
    try std.testing.expectEqual(r1.ack.confirmed_seq, rs.ack.confirmed_seq);

    const gap_frames = [_][]const u8{stale_frame};
    const gap = rp.ReplFrames{ .epoch = 5, .base_seq = 999, .frames = &gap_frames };
    const rg = try f.recvFrames(gap);
    try std.testing.expectEqual(repl.RecvOutcome.gap, rg.outcome);
    try std.testing.expectEqual(r1.ack.confirmed_seq, rg.ack.confirmed_seq);

    const wire2 = try batch.serialize(allocator);
    defer allocator.free(wire2);
    wire2[wire2.len - 1] ^= 0xFF;
    try std.testing.expectError(error.ChecksumMismatch, rp.ReplFrames.deserialize(allocator, wire2));
}

test "replication R2-b: ReplFrames over a socket, follower applies + acks; stale-epoch fenced" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const leader_path = "test_r2b_leader.db";
    const follower_path = "test_r2b_follower.db";
    const leader_wal = "test_r2b_leader_wal";
    const follower_wal = "test_r2b_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (1, 'Alice')" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (2, 'Bob')" }));
    try std.testing.expect(cap.records.items.len > 0);

    const frames = try allocator.alloc([]const u8, cap.records.items.len);
    defer {
        for (frames) |fr| allocator.free(fr);
        allocator.free(frames);
    }
    for (cap.records.items, 0..) |rec, i| frames[i] = try frameOf(allocator, rec);

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, follower_wal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59321, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var client = repl.ReplClient.init(allocator, io);
    defer client.deinit();
    try client.connectRaw("127.0.0.1", 59321);

    const batch = rp.ReplFrames{ .epoch = 7, .base_seq = 1, .frames = frames };
    const ack1 = try client.shipFrames(batch);
    try std.testing.expectEqual(@as(u64, 7), ack1.epoch);
    try std.testing.expectEqual(@as(u64, frames.len), ack1.confirmed_seq);

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    {
        const res = try fex.execute(.{ .sql = "SELECT id, name FROM kv ORDER BY id" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 2), res.rows.len);
        try std.testing.expectEqualStrings("Alice", res.rows[0][1]);
        try std.testing.expectEqualStrings("Bob", res.rows[1][1]);
    }

    const stale_frame = try frameOf(allocator, cap.records.items[cap.records.items.len - 1]);
    defer allocator.free(stale_frame);
    const stale_frames = [_][]const u8{stale_frame};
    const stale = rp.ReplFrames{ .epoch = 4, .base_seq = ack1.confirmed_seq + 1, .frames = &stale_frames };
    const ack2 = try client.shipFrames(stale);
    try std.testing.expectEqual(@as(u64, 7), ack2.epoch);
    try std.testing.expectEqual(ack1.confirmed_seq, ack2.confirmed_seq);
}

test "replication R2: store-side write fencing rejects stale-epoch writes" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const db_path = "test_fence.db";
    const wal_dir = "test_fence_wal";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, "fence_epoch.json") catch {};
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const H = struct {
        fn fenced(ex: *QueryExecutor, sql: []const u8, alloc: std.mem.Allocator) !bool {
            const res = try ex.execute(.{ .sql = sql });
            defer if (res.error_message) |m| alloc.free(m);
            if (res.error_message) |m| return std.mem.indexOf(u8, m, "Fenced") != null;
            return false;
        }
    };

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db.close();
        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();

        freeResp(allocator, try ex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));

        try std.testing.expect(!try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (1, 'a')", allocator));

        try db.setWriteEpoch(5);
        try std.testing.expect(!try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (2, 'b')", allocator));

        try db.observeEpoch(7);
        try std.testing.expect(try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (3, 'c')", allocator));
        try std.testing.expect(try H.fenced(&ex, "UPDATE kv SET v = 'x' WHERE id = 1", allocator));
        try std.testing.expect(try H.fenced(&ex, "DELETE FROM kv WHERE id = 1", allocator));
        {
            const rd = try ex.execute(.{ .sql = "SELECT id, v FROM kv ORDER BY id" });
            defer freeResp(allocator, rd);
            try std.testing.expect(rd.error_message == null);
            try std.testing.expectEqual(@as(usize, 2), rd.rows.len);
        }

        try db.setWriteEpoch(8);
        try std.testing.expect(!try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (3, 'c')", allocator));
        try std.testing.expectEqual(@as(u64, 8), db.max_epoch_seen);
    }

    {
        var db2 = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db2.close();
        try std.testing.expectEqual(@as(u64, 8), db2.max_epoch_seen);
        var ex2 = QueryExecutor.init(allocator, db2);
        defer ex2.deinit();
        try std.testing.expect(try H.fenced(&ex2, "INSERT INTO kv (id, v) VALUES (9, 'z')", allocator));
        try db2.setWriteEpoch(8);
        try std.testing.expect(!try H.fenced(&ex2, "INSERT INTO kv (id, v) VALUES (9, 'z')", allocator));
    }
}

test "replication P3: SET FENCE EPOCH wire control links lease epoch to write-fence" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const db_path = "test_setfence.db";
    const wal_dir = "test_setfence_wal";
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, "fence_epoch.json") catch {};
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const H = struct {
        fn fenced(ex: *QueryExecutor, sql: []const u8, alloc: std.mem.Allocator) !bool {
            const res = try ex.execute(.{ .sql = sql });
            defer if (res.error_message) |m| alloc.free(m);
            if (res.error_message) |m| return std.mem.indexOf(u8, m, "Fenced") != null;
            return false;
        }
    };

    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var ex = QueryExecutor.init(allocator, db);
    defer ex.deinit();
    freeResp(allocator, try ex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));

    freeResp(allocator, try ex.execute(.{ .sql = "SET FENCE EPOCH 5" }));
    try std.testing.expectEqual(@as(u64, 5), db.fencing_epoch);
    try std.testing.expectEqual(@as(u64, 5), db.max_epoch_seen);
    try std.testing.expect(!try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (1, 'a')", allocator));

    try db.observeEpoch(7);
    try std.testing.expect(try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (2, 'b')", allocator));
    {
        const rd = try ex.execute(.{ .sql = "SELECT id FROM kv" });
        defer freeResp(allocator, rd);
        try std.testing.expect(rd.error_message == null);
    }

    freeResp(allocator, try ex.execute(.{ .sql = "SET FENCE EPOCH 8" }));
    try std.testing.expectEqual(@as(u64, 8), db.fencing_epoch);
    try std.testing.expect(!try H.fenced(&ex, "INSERT INTO kv (id, v) VALUES (2, 'b')", allocator));

    {
        const bad = try ex.execute(.{ .sql = "SET FENCE EPOCH notanumber" });
        defer freeResp(allocator, bad);
        try std.testing.expect(bad.error_message != null);
    }
}

test "replication R3: quorum-ack coverage (RPO=0 gate)" {
    const allocator = std.testing.allocator;
    const QuorumTracker = @import("query/replication.zig").QuorumTracker;

    {
        var qt = QuorumTracker.init(allocator, 1);
        defer qt.deinit();
        try std.testing.expectEqual(@as(u32, 1), qt.quorum());
        try std.testing.expect(qt.isCovered(100));
    }

    {
        var qt = QuorumTracker.init(allocator, 3);
        defer qt.deinit();
        try std.testing.expectEqual(@as(u32, 2), qt.quorum());

        try std.testing.expectEqual(@as(u32, 1), qt.coverage(10));
        try std.testing.expect(!qt.isCovered(10));

        try qt.recordAck(1, 10);
        try std.testing.expect(qt.isCovered(10));

        try qt.recordAck(1, 15);
        try std.testing.expect(!qt.isCovered(20));
        try qt.recordAck(2, 20);
        try std.testing.expect(qt.isCovered(20));

        try qt.recordAck(1, 5);
        try std.testing.expect(qt.isCovered(15));
    }

    {
        var qt = QuorumTracker.init(allocator, 5);
        defer qt.deinit();
        try std.testing.expectEqual(@as(u32, 3), qt.quorum());
        try qt.recordAck(1, 50);
        try std.testing.expect(!qt.isCovered(50));
        try qt.recordAck(2, 50);
        try std.testing.expect(qt.isCovered(50));
    }
}

test "replication R3: awaitQuorum immediate success + timeout" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const QuorumTracker = @import("query/replication.zig").QuorumTracker;

    var qt = QuorumTracker.init(allocator, 3);
    defer qt.deinit();

    try qt.recordAck(1, 5);
    try qt.awaitQuorum(io, 5, 1000);

    try std.testing.expectError(error.QuorumTimeout, qt.awaitQuorum(io, 999, 40));
}

test "replication R3-b: durable write is quorum-acked over a socket (RPO=0)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const leader_path = "test_r3b_leader.db";
    const follower_path = "test_r3b_follower.db";
    const leader_wal = "test_r3b_leader_wal";
    const follower_wal = "test_r3b_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (1, 'a')" }));
    try std.testing.expect(cap.records.items.len > 0);

    const frames = try allocator.alloc([]const u8, cap.records.items.len);
    defer {
        for (frames) |fr| allocator.free(fr);
        allocator.free(frames);
    }
    for (cap.records.items, 0..) |rec, i| frames[i] = try frameOf(allocator, rec);
    const batch_seq: u64 = frames.len;

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, follower_wal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59323, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var qt = repl.QuorumTracker.init(allocator, 2);
    defer qt.deinit();
    try std.testing.expectEqual(@as(u32, 2), qt.quorum());

    try std.testing.expect(!qt.isCovered(batch_seq));

    var client = repl.ReplClient.init(allocator, io);
    defer client.deinit();
    try client.connectRaw("127.0.0.1", 59323);

    const ack = try client.shipAndRecord(rp.ReplFrames{ .epoch = 1, .base_seq = 1, .frames = frames }, &qt, 1);
    try std.testing.expectEqual(batch_seq, ack.confirmed_seq);

    try qt.awaitQuorum(io, batch_seq, 1000);
    try std.testing.expect(qt.isCovered(batch_seq));

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    {
        const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("a", res.rows[0][0]);
    }

    try std.testing.expect(!qt.isCovered(batch_seq + 5));
    try std.testing.expectError(error.QuorumTimeout, qt.awaitQuorum(io, batch_seq + 5, 40));
}

test "replication P5: executor durable commit is quorum-acked end to end (RPO=0)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const leader_path = "test_p5_leader.db";
    const follower_path = "test_p5_follower.db";
    const leader_wal = "test_p5_leader_wal";
    const follower_wal = "test_p5_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, follower_wal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59341, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    try leader.becomeDurableLeader("127.0.0.1", 59341, 2, 1, 2000, "", .{});

    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();

    freeResp(allocator, try lex.execute(.{ .sql = "SET DURABLE COMMIT ON" }));
    try std.testing.expect(lex.durable_commit);

    {
        const r1 = try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" });
        defer freeResp(allocator, r1);
        try std.testing.expect(r1.error_message == null);
    }
    {
        const r2 = try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (1, 'durable')" });
        defer freeResp(allocator, r2);
        try std.testing.expect(r2.error_message == null);
    }

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    {
        const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("durable", res.rows[0][0]);
    }

    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (2, 'also')" }));
    {
        const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 2" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("also", res.rows[0][0]);
    }
}

test "replication P5: durable commit FAILS the write when no quorum is reachable" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const leader_path = "test_p5neg_leader.db";
    const follower_path = "test_p5neg_follower.db";
    const leader_wal = "test_p5neg_leader_wal";
    const follower_wal = "test_p5neg_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, follower_wal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59342, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    try leader.becomeDurableLeader("127.0.0.1", 59342, 2, 1, 150, "", .{});
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "SET DURABLE COMMIT ON" }));
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));

    server.stop();
    g.cancel(io);

    {
        const res = try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (9, 'lost')" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
    }
}

test "replication P6: replica auth handshake -- matching key accepted, wrong key refused" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const fpath = "test_p6_follower.db";
    const fwal = "test_p6_follower_wal";
    const lpath = "test_p6_leader.db";
    const lwal = "test_p6_leader_wal";
    defer Io.Dir.deleteFile(.cwd(), io, fpath) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, lpath) catch {};
    Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    Io.Dir.deleteTree(.cwd(), io, lwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, lwal) catch {};

    const secret = "super-secret-replica-key";

    var follower = try Database.open(allocator, io, fpath, 64, fwal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fwal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59351, secret);
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    {
        var badleader = try Database.open(allocator, io, lpath, 64, lwal);
        defer badleader.close();
        try std.testing.expectError(error.AuthFailed, badleader.becomeDurableLeader("127.0.0.1", 59351, 2, 1, 1000, "the-wrong-key", .{}));
        try std.testing.expect(badleader.durable_repl == null);
    }

    {
        var leader = try Database.open(allocator, io, lpath, 64, lwal);
        defer leader.close();
        try leader.becomeDurableLeader("127.0.0.1", 59351, 2, 1, 2000, secret, .{});
        var lex = QueryExecutor.init(allocator, leader);
        defer lex.deinit();
        freeResp(allocator, try lex.execute(.{ .sql = "SET DURABLE COMMIT ON" }));
        {
            const r = try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" });
            defer freeResp(allocator, r);
            try std.testing.expect(r.error_message == null);
        }
        freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (1, 'authed')" }));

        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("authed", res.rows[0][0]);
    }
}

test "replication P6: mutual TLS -- valid cert chain ships, rogue-CA client refused" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const fpath = "test_p6tls_follower.db";
    const fwal = "test_p6tls_follower_wal";
    const lpath = "test_p6tls_leader.db";
    const lwal = "test_p6tls_leader_wal";
    defer Io.Dir.deleteFile(.cwd(), io, fpath) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, lpath) catch {};
    Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    Io.Dir.deleteTree(.cwd(), io, lwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, lwal) catch {};

    const secret = "tls-plus-hmac";
    const server_tls = repl.TlsConfig{ .ca_path = "testdata/repl/ca.crt", .cert_path = "testdata/repl/server.crt", .key_path = "testdata/repl/server.key" };

    var follower = try Database.open(allocator, io, fpath, 64, fwal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fwal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59361, secret);
    server.tls_config = server_tls;
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    {
        var rogue = try Database.open(allocator, io, lpath, 64, lwal);
        defer rogue.close();
        const rogue_tls = repl.TlsConfig{ .ca_path = "testdata/repl/ca.crt", .cert_path = "testdata/repl/rogue.crt", .key_path = "testdata/repl/rogue.key", .host = "localhost" };
        if (rogue.becomeDurableLeader("127.0.0.1", 59361, 2, 1, 1000, secret, rogue_tls)) |_| {
            return error.TestRogueShouldHaveBeenRefused;
        } else |_| {}
        try std.testing.expect(rogue.durable_repl == null);
    }

    {
        var leader = try Database.open(allocator, io, lpath, 64, lwal);
        defer leader.close();
        const client_tls = repl.TlsConfig{ .ca_path = "testdata/repl/ca.crt", .cert_path = "testdata/repl/client.crt", .key_path = "testdata/repl/client.key", .host = "localhost" };
        try leader.becomeDurableLeader("127.0.0.1", 59361, 2, 1, 3000, secret, client_tls);
        var lex = QueryExecutor.init(allocator, leader);
        defer lex.deinit();
        freeResp(allocator, try lex.execute(.{ .sql = "SET DURABLE COMMIT ON" }));
        {
            const r = try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" });
            defer freeResp(allocator, r);
            try std.testing.expect(r.error_message == null);
        }
        freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (1, 'tls')" }));

        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("tls", res.rows[0][0]);
    }
}

test "P7 PITR: openAt replays the archived WAL forward to a target seq" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const src_path = "test_pitr_src.db";
    const src_wal = "test_pitr_src_wal";
    const snap_dir = "test_pitr_snapshot";
    const dst_path = "test_pitr_restored.db";
    const dst_wal = "test_pitr_restored_wal";
    for ([_][]const u8{ src_path, dst_path }) |p| Io.Dir.deleteFile(.cwd(), io, p) catch {};
    for ([_][]const u8{ src_wal, snap_dir, dst_wal }) |d| Io.Dir.deleteTree(.cwd(), io, d) catch {};
    defer for ([_][]const u8{ src_path, dst_path }) |p| Io.Dir.deleteFile(.cwd(), io, p) catch {};
    defer for ([_][]const u8{ src_wal, snap_dir, dst_wal }) |d| Io.Dir.deleteTree(.cwd(), io, d) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var target_l7: u64 = 0;
    {
        var db = try Database.open(allocator, io, src_path, 64, src_wal);
        defer db.close();
        if (db.wal) |w| {
            w.ship_callback = &ReplCapture.cb;
            w.replication_manager = @ptrCast(&cap);
        }
        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();
        freeResp(allocator, try ex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));

        var i: i64 = 1;
        while (i <= 10) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO kv (id, v) VALUES ({d}, 'row{d}')", .{ i, i });
            defer allocator.free(sql);
            freeResp(allocator, try ex.execute(.{ .sql = sql }));
            if (i == 7) target_l7 = cap.records.items[cap.records.items.len - 1].lsn;
            if (i == 5) try db.exportSnapshot(snap_dir);
        }
    }
    try std.testing.expect(target_l7 > 0);

    try Database.restoreSnapshot(allocator, io, snap_dir, dst_path, "");
    {
        std.Io.Dir.createDirPath(.cwd(), io, dst_wal) catch |e| {
            if (e != error.PathAlreadyExists) return e;
        };
        var src = try std.Io.Dir.openDir(.cwd(), io, src_wal, .{ .iterate = true });
        defer src.close(io);
        var it = src.iterate();
        while (it.next(io) catch null) |entry| {
            if (entry.kind != .file) continue;
            const sp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ src_wal, entry.name });
            defer allocator.free(sp);
            const dp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dst_wal, entry.name });
            defer allocator.free(dp);
            const b = std.Io.Dir.readFileAlloc(.cwd(), io, sp, allocator, .unlimited) catch continue;
            defer allocator.free(b);
            const of = try std.Io.Dir.createFile(.cwd(), io, dp, .{ .truncate = true });
            defer of.close(io);
            try of.writeStreamingAll(io, b);
            try of.sync(io);
        }
    }

    {
        var db = try Database.openAt(allocator, io, dst_path, 64, dst_wal, target_l7);
        defer db.close();
        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();
        for ([_]i64{ 1, 5, 7 }) |id| {
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM kv WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try ex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        }
        for ([_]i64{ 8, 9, 10 }) |id| {
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM kv WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try ex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 0), res.rows.len);
        }
    }
}

test "P7 PITR: archived WAL survives checkpoints and restores to a target LSN" {
    // This is the real point-in-time-recovery path: unlike the sibling test that
    // copies the live WAL dir wholesale, here every relevant segment is retired by
    // a CHECKPOINT (which truncates it out of the live dir) and survives ONLY
    // because WAL archiving copied it aside first. The restore then reads from the
    // base snapshot plus the ARCHIVE, never touching the live WAL, and still lands
    // exactly on the target LSN.
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const src_path = "test_pitr2_src.db";
    const src_wal = "test_pitr2_src_wal";
    const arch_dir = "test_pitr2_archive";
    const snap_dir = "test_pitr2_snapshot";
    const dst_path = "test_pitr2_restored.db";
    const dst_wal = "test_pitr2_restored_wal";
    for ([_][]const u8{ src_path, dst_path }) |p| Io.Dir.deleteFile(.cwd(), io, p) catch {};
    for ([_][]const u8{ src_wal, arch_dir, snap_dir, dst_wal }) |d| Io.Dir.deleteTree(.cwd(), io, d) catch {};
    defer for ([_][]const u8{ src_path, dst_path }) |p| Io.Dir.deleteFile(.cwd(), io, p) catch {};
    defer for ([_][]const u8{ src_wal, arch_dir, snap_dir, dst_wal }) |d| Io.Dir.deleteTree(.cwd(), io, d) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var target_l7: u64 = 0;
    {
        var db = try Database.open(allocator, io, src_path, 64, src_wal);
        defer db.close();
        try db.wal.?.setArchive(arch_dir);
        db.wal.?.ship_callback = &ReplCapture.cb;
        db.wal.?.replication_manager = @ptrCast(&cap);

        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();
        freeResp(allocator, try ex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));

        // Base data, checkpointed into the data file, then captured as the backup.
        var i: i64 = 1;
        while (i <= 5) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO kv (id, v) VALUES ({d}, 'row{d}')", .{ i, i });
            defer allocator.free(sql);
            freeResp(allocator, try ex.execute(.{ .sql = sql }));
        }
        try db.wal.?.checkpoint();
        try db.exportSnapshot(snap_dir);

        // Post-backup history. id 7 is our recovery target; everything after must
        // be excluded by the replay.
        while (i <= 15) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO kv (id, v) VALUES ({d}, 'row{d}')", .{ i, i });
            defer allocator.free(sql);
            freeResp(allocator, try ex.execute(.{ .sql = sql }));
            if (i == 7) target_l7 = cap.records.items[cap.records.items.len - 1].lsn;
        }

        // Two checkpoints retire the segments holding ids 6..15 out of the live
        // WAL directory. Archiving must have copied them aside first.
        try db.wal.?.checkpoint();
        try db.wal.?.checkpoint();
    }
    try std.testing.expect(target_l7 > 0);

    // Prove the archive actually captured segments (the checkpoints truncated the
    // live dir, so recovery must lean on these copies).
    {
        var ad = try Io.Dir.openDir(.cwd(), io, arch_dir, .{ .iterate = true });
        defer ad.close(io);
        var n: usize = 0;
        var it = ad.iterate();
        while (it.next(io) catch null) |e| {
            if (e.kind == .file and std.mem.endsWith(u8, e.name, ".wal")) n += 1;
        }
        try std.testing.expect(n >= 1);
    }

    // Restore: base snapshot + archived WAL only. The live src_wal is never read.
    try Database.restoreSnapshot(allocator, io, snap_dir, dst_path, dst_wal);
    {
        var asrc = try Io.Dir.openDir(.cwd(), io, arch_dir, .{ .iterate = true });
        defer asrc.close(io);
        var it = asrc.iterate();
        while (it.next(io) catch null) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".wal")) continue;
            const sp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ arch_dir, entry.name });
            defer allocator.free(sp);
            const b = std.Io.Dir.readFileAlloc(.cwd(), io, sp, allocator, .unlimited) catch continue;
            defer allocator.free(b);
            const dp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dst_wal, entry.name });
            defer allocator.free(dp);
            const of = try std.Io.Dir.createFile(.cwd(), io, dp, .{ .truncate = true });
            defer of.close(io);
            try of.writeStreamingAll(io, b);
            try of.sync(io);
        }
    }

    {
        var db = try Database.openAt(allocator, io, dst_path, 64, dst_wal, target_l7);
        defer db.close();
        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();
        // At/under the target: present.
        for ([_]i64{ 1, 5, 6, 7 }) |id| {
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM kv WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try ex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        }
        // Past the target: absent.
        for ([_]i64{ 8, 10, 12, 15 }) |id| {
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM kv WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try ex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 0), res.rows.len);
        }
    }
}

test "P8 chaos: a corrupted shipped frame is detected and never applied" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const leader_path = "test_p8_leader.db";
    const follower_path = "test_p8_follower.db";
    const leader_wal = "test_p8_leader_wal";
    const follower_wal = "test_p8_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (7, 'valid')" }));

    const frames = try allocator.alloc([]const u8, cap.records.items.len);
    defer {
        for (frames) |fr| allocator.free(fr);
        allocator.free(frames);
    }
    for (cap.records.items, 0..) |rec, i| frames[i] = try frameOf(allocator, rec);

    const batch = rp.ReplFrames{ .epoch = 1, .base_seq = 1, .frames = frames };
    const good = try batch.serialize(allocator);
    defer allocator.free(good);
    {
        var d = try rp.ReplFrames.deserialize(allocator, good);
        d.deinit(allocator);
    }

    const corrupt = try allocator.dupe(u8, good);
    defer allocator.free(corrupt);
    corrupt[corrupt.len / 2] ^= 0xFF;
    try std.testing.expectError(error.ChecksumMismatch, rp.ReplFrames.deserialize(allocator, corrupt));

    var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, follower_wal);
    {
        var d = try rp.ReplFrames.deserialize(allocator, good);
        defer d.deinit(allocator);
        const res = try f.recvFrames(d);
        try std.testing.expectEqual(@as(u64, frames.len), res.ack.confirmed_seq);
    }
    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    {
        const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 7" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("valid", res.rows[0][0]);
    }
}

test "P7 backup/restore: exportSnapshot -> restoreSnapshot round-trips the data" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const src_path = "test_p7_src.db";
    const src_wal = "test_p7_src_wal";
    const snap_dir = "test_p7_snapshot";
    const dst_path = "test_p7_restored.db";
    Io.Dir.deleteFile(.cwd(), io, src_path) catch {};
    Io.Dir.deleteFile(.cwd(), io, dst_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, src_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, snap_dir) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, src_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, dst_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, src_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, snap_dir) catch {};

    {
        var db = try Database.open(allocator, io, src_path, 64, src_wal);
        defer db.close();
        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();
        freeResp(allocator, try ex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));
        var i: i64 = 1;
        while (i <= 50) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO kv (id, v) VALUES ({d}, 'row{d}')", .{ i, i });
            defer allocator.free(sql);
            freeResp(allocator, try ex.execute(.{ .sql = sql }));
        }
        try db.exportSnapshot(snap_dir);
    }

    const dst_wal = "test_p7_restored_wal";
    Io.Dir.deleteTree(.cwd(), io, dst_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, dst_wal) catch {};
    try Database.restoreSnapshot(allocator, io, snap_dir, dst_path, dst_wal);

    {
        const snap_path = try std.fmt.allocPrint(allocator, "{s}/snapshot.db", .{snap_dir});
        defer allocator.free(snap_path);
        const a = try Io.Dir.readFileAlloc(.cwd(), io, snap_path, allocator, .unlimited);
        defer allocator.free(a);
        const b = try Io.Dir.readFileAlloc(.cwd(), io, dst_path, allocator, .unlimited);
        defer allocator.free(b);
        try std.testing.expect(std.mem.eql(u8, a, b));
    }

    {
        var db = try Database.open(allocator, io, dst_path, 64, dst_wal);
        defer db.close();
        var ex = QueryExecutor.init(allocator, db);
        defer ex.deinit();
        for ([_]i64{ 1, 25, 50 }) |id| {
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM kv WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try ex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            const want = try std.fmt.allocPrint(allocator, "row{d}", .{id});
            defer allocator.free(want);
            try std.testing.expectEqualStrings(want, res.rows[0][0]);
        }
    }
}

test "P6 config-API authz: control commands require an ADMIN session" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const db_path = "test_p6_authz.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    db.security_manager.enabled = true;

    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    {
        const res = try exec.execute(.{ .sql = "SET FENCE EPOCH 2" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
        try std.testing.expect(std.mem.indexOf(u8, res.error_message.?, "Authentication Required") != null);
    }
    {
        const res = try exec.execute(.{ .sql = "SET DURABLE COMMIT ON" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
    }

    var admin_token: ?[]const u8 = null;
    defer if (admin_token) |t| allocator.free(t);
    {
        const res = try exec.execute(.{ .sql = "LOGIN admin 'admin'" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        admin_token = try allocator.dupe(u8, res.rows[0][0]);
    }

    {
        const res = try exec.execute(.{ .sql = "SET FENCE EPOCH 2", .session_token = admin_token });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    {
        const res = try exec.execute(.{ .sql = "CREATE USER ops IDENTIFIED BY 'opspw' ROLE 'read_only'", .session_token = admin_token });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }
    var ops_token: ?[]const u8 = null;
    defer if (ops_token) |t| allocator.free(t);
    {
        const res = try exec.execute(.{ .sql = "LOGIN ops 'opspw'" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        ops_token = try allocator.dupe(u8, res.rows[0][0]);
    }
    {
        const res = try exec.execute(.{ .sql = "SET DURABLE COMMIT OFF", .session_token = ops_token });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
        try std.testing.expect(std.mem.indexOf(u8, res.error_message.?, "Permission Denied") != null);
    }
}

test "replication P2: becomeFollower makes an opened db a live follower" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const leader_path = "test_p2lc_leader.db";
    const follower_path = "test_p2lc_follower.db";
    const leader_wal = "test_p2lc_leader_wal";
    const follower_wal = "test_p2lc_follower_wal";
    defer Io.Dir.deleteFile(.cwd(), io, leader_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, follower_path) catch {};
    Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, leader_wal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, follower_wal) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var leader = try Database.open(allocator, io, leader_path, 64, leader_wal);
    defer leader.close();
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (1, 'alpha')" }));
    try std.testing.expect(cap.records.items.len > 0);

    const frames = try allocator.alloc([]const u8, cap.records.items.len);
    defer {
        for (frames) |fr| allocator.free(fr);
        allocator.free(frames);
    }
    for (cap.records.items, 0..) |rec, i| frames[i] = try frameOf(allocator, rec);

    {
        var follower = try Database.open(allocator, io, follower_path, 64, follower_wal);
        defer follower.close();
        try follower.becomeFollower("127.0.0.1", 59324, follower_wal, "", .{});
        try std.testing.expect(follower.repl_server != null);
        while (!follower.repl_server.?.bound.load(.seq_cst)) {
            _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
        }

        var client = repl.ReplClient.init(allocator, io);
        defer client.deinit();
        try client.connectRaw("127.0.0.1", 59324);
        const ack = try client.shipFrames(rp.ReplFrames{ .epoch = 1, .base_seq = 1, .frames = frames });
        try std.testing.expectEqual(@as(u64, frames.len), ack.confirmed_seq);

        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        {
            const res = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 1" });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
            try std.testing.expectEqualStrings("alpha", res.rows[0][0]);
        }
        try std.testing.expectEqual(@as(u64, frames.len), follower.follower.?.state.confirmed_seq);
    }

    {
        var follower2 = try Database.open(allocator, io, follower_path, 64, follower_wal);
        defer follower2.close();
        try follower2.becomeFollower("127.0.0.1", 59325, follower_wal, "", .{});
        try std.testing.expectEqual(@as(u64, frames.len), follower2.follower.?.state.confirmed_seq);

        var fex2 = QueryExecutor.init(allocator, follower2);
        defer fex2.deinit();
        const res = try fex2.execute(.{ .sql = "SELECT v FROM kv WHERE id = 1" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        try std.testing.expectEqualStrings("alpha", res.rows[0][0]);
    }
}

test "D1: SQL aggregates and GROUP BY" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_aggregates.db";
    const wal_dir = "test_aggregates_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();

    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    {
        const r = try exec.execute(.{ .sql = "CREATE TABLE emp (id INT PRIMARY KEY, dept TEXT, salary INT)" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
    }
    const inserts = [_][]const u8{
        "INSERT INTO emp (id, dept, salary) VALUES (1, 'eng', 100)",
        "INSERT INTO emp (id, dept, salary) VALUES (2, 'eng', 200)",
        "INSERT INTO emp (id, dept, salary) VALUES (3, 'sales', 50)",
        "INSERT INTO emp (id, dept, salary) VALUES (4, 'sales', 150)",
    };
    for (inserts) |sql| {
        const r = try exec.execute(.{ .sql = sql });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
    }

    const Case = struct { sql: []const u8, want: []const u8 };
    const cases = [_]Case{
        .{ .sql = "SELECT COUNT(*) FROM emp", .want = "4" },
        .{ .sql = "SELECT COUNT(dept) FROM emp", .want = "4" },
        .{ .sql = "SELECT SUM(salary) FROM emp", .want = "500" },
        .{ .sql = "SELECT MIN(salary) FROM emp", .want = "50" },
        .{ .sql = "SELECT MAX(salary) FROM emp", .want = "200" },
        .{ .sql = "SELECT AVG(salary) FROM emp", .want = "125" },
        .{ .sql = "SELECT MIN(dept) FROM emp", .want = "eng" },
        .{ .sql = "SELECT MAX(dept) FROM emp", .want = "sales" },
    };
    for (cases) |c| {
        const r = try exec.execute(.{ .sql = c.sql });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), r.rows.len);
        try std.testing.expectEqualStrings(c.want, r.rows[0][0]);
    }

    {
        const r = try exec.execute(.{ .sql = "SELECT dept, COUNT(*), SUM(salary) FROM emp GROUP BY dept" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 2), r.rows.len);
        try std.testing.expectEqualStrings("eng", r.rows[0][0]);
        try std.testing.expectEqualStrings("2", r.rows[0][1]);
        try std.testing.expectEqualStrings("300", r.rows[0][2]);
        try std.testing.expectEqualStrings("sales", r.rows[1][0]);
        try std.testing.expectEqualStrings("2", r.rows[1][1]);
        try std.testing.expectEqualStrings("200", r.rows[1][2]);
    }

    const where_cases = [_]Case{
        .{ .sql = "SELECT COUNT(*) FROM emp WHERE salary > 1000", .want = "0" },
        .{ .sql = "SELECT COUNT(*) FROM emp WHERE salary >= 150", .want = "2" },
        .{ .sql = "SELECT COUNT(*) FROM emp WHERE salary < 100", .want = "1" },
        .{ .sql = "SELECT SUM(salary) FROM emp WHERE salary >= 150", .want = "350" },
    };
    for (where_cases) |c| {
        const r = try exec.execute(.{ .sql = c.sql });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 1), r.rows.len);
        try std.testing.expectEqualStrings(c.want, r.rows[0][0]);
    }
}

test "D9: SQL parser + wire decoder fuzz (no crash, no leak)" {
    const allocator = std.testing.allocator;
    const Parser = @import("sql/parser.zig").Parser;

    var prng = std.Random.DefaultPrng.init(0xD9F5C0DE);
    const rand = prng.random();

    const vocab = [_][]const u8{
        "SELECT", "FROM",  "WHERE", "GROUP",  "BY",     "HAVING", "COUNT", "SUM",
        "MIN",    "MAX",   "AVG",   "INSERT", "INTO",   "VALUES", "UPDATE", "SET",
        "DELETE", "CREATE", "TABLE", "DROP",  "JOIN",   "ON",     "AS",    "DISTINCT",
        "*",      "(",     ")",     ",",      ".",      "=",      ">",     "<",
        ">=",     "<=",    "!=",    "AND",    "OR",     "NOT",    "NULL",  ";",
        "id",     "name",  "salary", "emp",   "'txt'",  "123",    "-5",    "0",
        "",       " ",     "\n",    "\t",     "@#$%",   "((((",   "))))",  "1e10",
        "0x1F",   "9999999999999999999", "''", "\"",   "--",     "/*",
    };

    var i: usize = 0;
    while (i < 4000) : (i += 1) {
        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(allocator);
        const parts = rand.intRangeAtMost(usize, 0, 24);
        var p: usize = 0;
        while (p < parts) : (p += 1) {
            try buf.appendSlice(allocator, vocab[rand.intRangeLessThan(usize, 0, vocab.len)]);
            if (rand.boolean()) try buf.append(allocator, ' ');
        }
        var parser = Parser.init(allocator, buf.items) catch continue;
        defer parser.deinit();
        _ = parser.parseStatement() catch {};
    }

    var j: usize = 0;
    while (j < 4000) : (j += 1) {
        var raw: [64]u8 = undefined;
        const n = rand.intRangeAtMost(usize, 0, raw.len);
        rand.bytes(raw[0..n]);
        var reader = proto.ProtocolReader.init(raw[0..n]);
        if (reader.readStruct(proto.MessageHeader)) |hdr| {
            _ = hdr.isValid();
        } else |_| {}
        _ = reader.readU32() catch {};
        _ = reader.readU16() catch {};
        _ = reader.readString(rand.intRangeAtMost(usize, 0, 128)) catch {};
    }
}

test "D7: kill -9 with unflushed pages -> recovery rebuilds committed rows from the WAL" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_kill9.db";
    const wal_dir = "test_kill9_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        db.synchronous_commit = true;
        var exec = QueryExecutor.init(allocator, db);
        freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
        freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 'a')" }));
        freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (2, 'b')" }));
        exec.deinit();
        db.crashSimulate();
    }

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db.close();
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();
        const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
        const r2 = try exec.execute(.{ .sql = "SELECT v FROM t WHERE id = 2" });
        defer freeResp(allocator, r2);
        try std.testing.expectEqualStrings("b", r2.rows[0][0]);
        freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (3, 'c')" }));
    }
}

test "D7: kill -9 under eviction pressure -> recovery returns every committed row" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_kill9_evict.db";
    const wal_dir = "test_kill9_evict_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const N: usize = 300;
    const pad = "x" ** 4000;

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        db.synchronous_commit = true;
        var exec = QueryExecutor.init(allocator, db);
        freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
        var i: usize = 0;
        while (i < N) : (i += 1) {
            const sql = try std.fmt.allocPrint(allocator, "INSERT INTO t (id, v) VALUES ({d}, '{s}')", .{ i, pad });
            defer allocator.free(sql);
            freeResp(allocator, try exec.execute(.{ .sql = sql }));
        }
        exec.deinit();
        db.crashSimulate();
    }

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db.close();
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();

        const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        const expected = try std.fmt.allocPrint(allocator, "{d}", .{N});
        defer allocator.free(expected);
        try std.testing.expectEqualStrings(expected, r.rows[0][0]);

        for ([_]usize{ 0, N / 2, N - 1 }) |id| {
            const q = try std.fmt.allocPrint(allocator, "SELECT v FROM t WHERE id = {d}", .{id});
            defer allocator.free(q);
            const rr = try exec.execute(.{ .sql = q });
            defer freeResp(allocator, rr);
            try std.testing.expect(rr.rows.len == 1);
            try std.testing.expectEqualStrings(pad, rr.rows[0][0]);
        }

        const w = try std.fmt.allocPrint(allocator, "INSERT INTO t (id, v) VALUES ({d}, '{s}')", .{ N, pad });
        defer allocator.free(w);
        freeResp(allocator, try exec.execute(.{ .sql = w }));
    }
}

test "D7: torn WAL tail -> clean recovery, committed data intact" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "test_torn.db";
    const wal_dir = "test_torn_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db.close();
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();
        db.synchronous_commit = true;
        freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
        freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 'a')" }));
        freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (2, 'b')" }));
    }

    {
        const seg = try std.fmt.allocPrint(allocator, "{s}/000001.wal", .{wal_dir});
        defer allocator.free(seg);
        const existing = try Io.Dir.readFileAlloc(.cwd(), io, seg, allocator, .unlimited);
        defer allocator.free(existing);
        var buf = try allocator.alloc(u8, existing.len + 32);
        defer allocator.free(buf);
        @memcpy(buf[0..existing.len], existing);
        @memset(buf[existing.len..], 0xAB);
        try Io.Dir.writeFile(.cwd(), io, .{ .sub_path = seg, .data = buf, .flags = .{} });
    }

    {
        var db = try Database.open(allocator, io, db_path, 64, wal_dir);
        defer db.close();
        var exec = QueryExecutor.init(allocator, db);
        defer exec.deinit();

        const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);

        freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (3, 'c')" }));
        const r2 = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r2);
        try std.testing.expectEqualStrings("3", r2.rows[0][0]);
    }
}

test "D7: ENOSPC on commit is never falsely ACKed; db stays usable" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_enospc.db";
    const wal_dir = "test_enospc_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const WalError = @import("durability/wal_error.zig").WalError;

    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    db.synchronous_commit = true;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 'a')" }));

    if (db.wal) |w| w.test_inject_write_error = WalError.NoSpaceLeft;

    var acked_ok = false;
    if (exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (2, 'b')" })) |resp| {
        defer freeResp(allocator, resp);
        if (resp.error_message == null) acked_ok = true;
    } else |_| {}
    try std.testing.expect(!acked_ok);

    const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
    defer freeResp(allocator, r);
    try std.testing.expect(r.error_message == null);
    try std.testing.expect(r.rows.len == 1);
}

test "D6/D8: RPO=0 -- follower drop + reconnect + gap backfill leaves it fully consistent" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const lp = "test_rpo_leader.db";
    const fp = "test_rpo_follower.db";
    const lw = "test_rpo_leader_wal";
    const fw = "test_rpo_follower_wal";
    Io.Dir.deleteTree(.cwd(), io, lw) catch {};
    Io.Dir.deleteTree(.cwd(), io, fw) catch {};
    Io.Dir.deleteFile(.cwd(), io, lp) catch {};
    Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, lp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, lw) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fw) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var leader = try Database.open(allocator, io, lp, 64, lw);
    defer leader.close();
    if (leader.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var lex = QueryExecutor.init(allocator, leader);
    defer lex.deinit();
    freeResp(allocator, try lex.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (1, 'a')" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (2, 'b')" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (3, 'c')" }));
    freeResp(allocator, try lex.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (4, 'd')" }));

    const n = cap.records.items.len;
    try std.testing.expect(n >= 4);
    const frames = try allocator.alloc([]const u8, n);
    defer {
        for (frames) |fr| allocator.free(fr);
        allocator.free(frames);
    }
    for (cap.records.items, 0..) |rec, i| frames[i] = try frameOf(allocator, rec);

    var follower = try Database.open(allocator, io, fp, 64, fw);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fw);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59322, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    const half = n / 2;
    try std.testing.expect(half > 0 and half < n);

    var confirmed: u64 = 0;
    {
        var client = repl.ReplClient.init(allocator, io);
        defer client.deinit();
        try client.connectRaw("127.0.0.1", 59322);
        const ack = try client.shipFrames(rp.ReplFrames{ .epoch = 1, .base_seq = 1, .frames = frames[0..half] });
        confirmed = ack.confirmed_seq;
        try std.testing.expectEqual(@as(u64, half), confirmed);
    }

    {
        var client = repl.ReplClient.init(allocator, io);
        defer client.deinit();
        try client.connectRaw("127.0.0.1", 59322);
        const ack = try client.shipFrames(rp.ReplFrames{ .epoch = 1, .base_seq = confirmed + 1, .frames = frames[half..] });
        try std.testing.expectEqual(@as(u64, n), ack.confirmed_seq);
    }

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
    defer freeResp(allocator, res);
    try std.testing.expect(res.error_message == null);
    try std.testing.expectEqualStrings("4", res.rows[0][0]);
}

test "D6: BackfillLog persists, replays from a seq, checkpoints, and survives restart" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const repl = @import("query/replication.zig");

    const dir = "test_backfill_log";
    Io.Dir.deleteTree(.cwd(), io, dir) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, dir) catch {};

    const N: u64 = 2000;
    {
        var bl = try repl.BackfillLog.init(allocator, io, dir);
        defer bl.deinit();
        var seq: u64 = 1;
        while (seq <= N) : (seq += 1) {
            var b0: [24]u8 = undefined;
            var b1: [24]u8 = undefined;
            const f0 = try std.fmt.bufPrint(&b0, "f{d}a", .{seq});
            const f1 = try std.fmt.bufPrint(&b1, "f{d}b", .{seq});
            const frames = [_][]const u8{ f0, f1 };
            try bl.append(seq, seq, &frames);
        }
        try std.testing.expectEqual(@as(usize, N), bl.batchCount());
        try std.testing.expectEqual(@as(?u64, 1), bl.earliestBase());

        const batches = try bl.batchesFrom(1500, allocator);
        defer {
            for (batches) |bt| bt.deinit(allocator);
            allocator.free(batches);
        }
        try std.testing.expectEqual(@as(usize, 500), batches.len);
        try std.testing.expectEqual(@as(u64, 1501), batches[0].base);
        try std.testing.expectEqual(@as(usize, 2), batches[0].frames.len);
        var exp: [24]u8 = undefined;
        const e0 = try std.fmt.bufPrint(&exp, "f{d}a", .{@as(u64, 1501)});
        try std.testing.expectEqualStrings(e0, batches[0].frames[0]);

        try bl.checkpoint(1000);
        try std.testing.expectEqual(@as(usize, 1000), bl.batchCount());
        try std.testing.expectEqual(@as(?u64, 1001), bl.earliestBase());
    }

    {
        var bl = try repl.BackfillLog.init(allocator, io, dir);
        defer bl.deinit();
        try std.testing.expectEqual(@as(usize, 1000), bl.batchCount());
        try std.testing.expectEqual(@as(?u64, 1001), bl.earliestBase());
        const batches = try bl.batchesFrom(1999, allocator);
        defer {
            for (batches) |bt| bt.deinit(allocator);
            allocator.free(batches);
        }
        try std.testing.expectEqual(@as(usize, 1), batches.len);
        try std.testing.expectEqual(@as(u64, 2000), batches[0].base);
        const tail = [_][]const u8{"tail"};
        try bl.append(2001, 2001, &tail);
        try std.testing.expectEqual(@as(usize, 1001), bl.batchCount());
    }
}

test "D6: ring evicted -- follower catches up from the on-disk backfill log (RPO=0)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const sp = "test_d6bf_src.db";
    const swal = "test_d6bf_src_wal";
    const fp = "test_d6bf_flw.db";
    const fwal = "test_d6bf_flw_wal";
    const bfdir = "test_d6bf_backfill";
    Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var src = try Database.open(allocator, io, sp, 64, swal);
    defer src.close();
    if (src.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var sx = QueryExecutor.init(allocator, src);
    defer sx.deinit();
    freeResp(allocator, try sx.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));
    var i: u32 = 1;
    while (i <= 6) : (i += 1) {
        var b: [96]u8 = undefined;
        const q = try std.fmt.bufPrint(&b, "INSERT INTO kv (id, name) VALUES ({d}, 'r{d}')", .{ i, i });
        freeResp(allocator, try sx.execute(.{ .sql = q }));
    }
    const split = cap.records.items.len;
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (7, 'r7')" }));

    var follower = try Database.open(allocator, io, fp, 64, fwal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fwal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59324, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var dr = repl.DurableReplicator.init(allocator, io, 2, 1, 3000, "", .{});
    defer dr.deinit();
    try dr.enableBackfill(bfdir);
    try dr.connect("127.0.0.1", 59324);

    server.partition();
    replayTxns(&dr, cap.records.items[0..split], false);
    try std.testing.expect(dr.backfill.?.batchCount() >= 7);

    dr.evictRing();
    server.heal();

    replayTxns(&dr, cap.records.items[split..], false);

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
    defer freeResp(allocator, res);
    try std.testing.expect(res.error_message == null);
    try std.testing.expectEqualStrings("7", res.rows[0][0]);
}

test "D6: catch-up returns SnapshotRequired when no retained batch reaches the follower" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const sp = "test_d6sr_src.db";
    const swal = "test_d6sr_src_wal";
    const fp = "test_d6sr_flw.db";
    const fwal = "test_d6sr_flw_wal";
    const bfdir = "test_d6sr_backfill";
    Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var src = try Database.open(allocator, io, sp, 64, swal);
    defer src.close();
    if (src.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var sx = QueryExecutor.init(allocator, src);
    defer sx.deinit();
    freeResp(allocator, try sx.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));

    var follower = try Database.open(allocator, io, fp, 64, fwal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fwal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59326, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var dr = repl.DurableReplicator.init(allocator, io, 2, 1, 500, "", .{});
    defer dr.deinit();
    try dr.enableBackfill(bfdir);
    try dr.connect("127.0.0.1", 59326);

    dr.next_seq = 50;
    dr.connected = false;
    for (cap.records.items) |rec| repl.DurableReplicator.onRecord(&dr, rec);
    try std.testing.expectError(error.SnapshotRequired, dr.shipPending(true));
}

test "D6: snapshot resync stitches confirmed_seq so the leader's tail ship is accepted" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");
    const rp = repl.proto;

    const sp = "test_d6rs_src.db";
    const swal = "test_d6rs_src_wal";
    const snapdir = "test_d6rs_snap";
    const fp = "test_d6rs_flw.db";
    const fdbwal = "test_d6rs_flw_dbwal";
    const fstate = "test_d6rs_flw_state";
    Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    Io.Dir.deleteTree(.cwd(), io, snapdir) catch {};
    Io.Dir.deleteTree(.cwd(), io, fdbwal) catch {};
    Io.Dir.deleteTree(.cwd(), io, fstate) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, snapdir) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fdbwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fstate) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var src = try Database.open(allocator, io, sp, 64, swal);
    defer src.close();
    if (src.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var sx = QueryExecutor.init(allocator, src);
    defer sx.deinit();
    freeResp(allocator, try sx.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (1, 'a')" }));
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (2, 'b')" }));
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (3, 'c')" }));

    const snap_seq: u64 = 10;
    try src.exportSnapshotForResync(snapdir, snap_seq);

    const tail_cursor = cap.records.items.len;
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, name) VALUES (4, 'd')" }));
    const tail_recs = cap.records.items[tail_cursor..];
    const tail_frames = try allocator.alloc([]const u8, tail_recs.len);
    defer {
        for (tail_frames) |fr| allocator.free(fr);
        allocator.free(tail_frames);
    }
    for (tail_recs, 0..) |rec, idx| tail_frames[idx] = try frameOf(allocator, rec);

    const restored_seq = try Database.restoreSnapshotResync(allocator, io, snapdir, fp, fdbwal, fstate);
    try std.testing.expectEqual(snap_seq, restored_seq);

    var follower = try Database.open(allocator, io, fp, 64, fdbwal);
    defer follower.close();
    try follower.becomeFollower("127.0.0.1", 59327, fstate, "", .{});

    {
        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
        defer freeResp(allocator, res);
        try std.testing.expectEqualStrings("3", res.rows[0][0]);
    }

    {
        var client = repl.ReplClient.init(allocator, io);
        defer client.deinit();
        try client.connectRaw("127.0.0.1", 59327);
        const ack = try client.shipFrames(rp.ReplFrames{ .epoch = 1, .base_seq = snap_seq + 1, .frames = tail_frames });
        try std.testing.expectEqual(snap_seq + @as(u64, tail_recs.len), ack.confirmed_seq);
    }

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
    defer freeResp(allocator, res);
    try std.testing.expectEqualStrings("4", res.rows[0][0]);
}

test "D8: partition then heal -- leader backfills, RPO=0, RTO measured" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const sp = "test_d8p_src.db";
    const swal = "test_d8p_src_wal";
    const fp = "test_d8p_flw.db";
    const fw = "test_d8p_flw_wal";
    const bfdir = "test_d8p_backfill";
    Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    Io.Dir.deleteTree(.cwd(), io, fw) catch {};
    Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fw) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var src = try Database.open(allocator, io, sp, 64, swal);
    defer src.close();
    if (src.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var sx = QueryExecutor.init(allocator, src);
    defer sx.deinit();
    freeResp(allocator, try sx.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (1, 'a')" }));
    const before_partition = cap.records.items.len;

    var follower = try Database.open(allocator, io, fp, 64, fw);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fw);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59325, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var dr = repl.DurableReplicator.init(allocator, io, 2, 1, 3000, "", .{});
    defer dr.deinit();
    try dr.enableBackfill(bfdir);
    try dr.connect("127.0.0.1", 59325);

    replayTxns(&dr, cap.records.items[0..before_partition], false);
    {
        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
        defer freeResp(allocator, res);
        try std.testing.expectEqualStrings("1", res.rows[0][0]);
    }

    server.partition();
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (2, 'b')" }));
    freeResp(allocator, try sx.execute(.{ .sql = "INSERT INTO kv (id, v) VALUES (3, 'c')" }));
    replayTxns(&dr, cap.records.items[before_partition..], false);
    var cursor = cap.records.items.len;
    {
        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
        defer freeResp(allocator, res);
        try std.testing.expectEqualStrings("1", res.rows[0][0]);
    }

    const t0 = std.Io.Clock.now(.real, io).toMilliseconds();
    server.heal();
    var caught_up = false;
    var attempts: u32 = 0;
    var rto_ms: i64 = 0;
    while (attempts < 20 and !caught_up) : (attempts += 1) {
        var b: [96]u8 = undefined;
        const q = try std.fmt.bufPrint(&b, "INSERT INTO kv (id, v) VALUES ({d}, 'h{d}')", .{ 100 + attempts, attempts });
        freeResp(allocator, try sx.execute(.{ .sql = q }));
        replayTxns(&dr, cap.records.items[cursor..], false);
        cursor = cap.records.items.len;
        var fex = QueryExecutor.init(allocator, follower);
        defer fex.deinit();
        const r2 = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 2" });
        defer freeResp(allocator, r2);
        const r3 = try fex.execute(.{ .sql = "SELECT v FROM kv WHERE id = 3" });
        defer freeResp(allocator, r3);
        if (r2.rows.len == 1 and r3.rows.len == 1) {
            rto_ms = std.Io.Clock.now(.real, io).toMilliseconds() - t0;
            caught_up = true;
        }
    }
    try std.testing.expect(caught_up);
    std.debug.print("[D8] partition->heal recovery (RTO): {d} ms\n", .{rto_ms});
}

test "D8: soak -- random partition/heal across many rounds keeps RPO=0 and the log bounded" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const repl = @import("query/replication.zig");

    const sp = "test_d8soak_src.db";
    const swal = "test_d8soak_src_wal";
    const fp = "test_d8soak_flw.db";
    const fwal = "test_d8soak_flw_wal";
    const bfdir = "test_d8soak_backfill";
    Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, sp) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, fp) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, swal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, fwal) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, bfdir) catch {};

    var cap = ReplCapture{ .allocator = allocator };
    defer cap.deinit();
    var src = try Database.open(allocator, io, sp, 64, swal);
    defer src.close();
    if (src.wal) |w| {
        w.ship_callback = &ReplCapture.cb;
        w.replication_manager = @ptrCast(&cap);
    }
    var sx = QueryExecutor.init(allocator, src);
    defer sx.deinit();
    freeResp(allocator, try sx.execute(.{ .sql = "CREATE TABLE kv (id INT PRIMARY KEY, name TEXT)" }));

    var follower = try Database.open(allocator, io, fp, 64, fwal);
    defer follower.close();
    var f = repl.Follower.init(allocator, io, follower, fwal);
    var server = repl.ReplServer.init(allocator, io, &f, "127.0.0.1", 59328, "");
    var g = Io.Group.init;
    g.async(io, repl.ReplServer.listenEntry, .{&server});
    defer {
        server.stop();
        g.cancel(io);
    }
    while (!server.bound.load(.seq_cst)) {
        _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
    }

    var dr = repl.DurableReplicator.init(allocator, io, 2, 1, 1500, "", .{});
    defer dr.deinit();
    try dr.enableBackfill(bfdir);
    try dr.connect("127.0.0.1", 59328);

    var prng = std.Random.DefaultPrng.init(0x5EED_D8);
    const rnd = prng.random();
    var cursor: usize = 0;
    var partitioned = false;
    const rounds: u32 = 40;
    var round: u32 = 1;
    while (round <= rounds) : (round += 1) {
        var b: [96]u8 = undefined;
        const q = try std.fmt.bufPrint(&b, "INSERT INTO kv (id, name) VALUES ({d}, 'v{d}')", .{ round, round });
        freeResp(allocator, try sx.execute(.{ .sql = q }));
        replayTxns(&dr, cap.records.items[cursor..], false);
        cursor = cap.records.items.len;

        if (rnd.boolean()) {
            if (partitioned) {
                server.heal();
                partitioned = false;
            } else {
                server.partition();
                partitioned = true;
            }
        }
    }
    if (partitioned) server.heal();

    var flush: u32 = 0;
    while (flush < 3) : (flush += 1) {
        var b: [96]u8 = undefined;
        const q = try std.fmt.bufPrint(&b, "INSERT INTO kv (id, name) VALUES ({d}, 'f{d}')", .{ 10000 + flush, flush });
        freeResp(allocator, try sx.execute(.{ .sql = q }));
        replayTxns(&dr, cap.records.items[cursor..], false);
        cursor = cap.records.items.len;
    }

    var fex = QueryExecutor.init(allocator, follower);
    defer fex.deinit();
    const res = try fex.execute(.{ .sql = "SELECT COUNT(*) FROM kv" });
    defer freeResp(allocator, res);
    try std.testing.expectEqualStrings("43", res.rows[0][0]);
    const spot = try fex.execute(.{ .sql = "SELECT name FROM kv WHERE id = 20" });
    defer freeResp(allocator, spot);
    try std.testing.expectEqualStrings("v20", spot.rows[0][0]);
    try std.testing.expect(dr.backfill.?.batchCount() <= 3);
}

test "O4: startup auth binds a session -- valid creds authorize, bad creds rejected" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const session = @import("proto/session.zig");
    const wire = @import("proto/wire.zig");

    const db_path = "test_o4_auth.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    db.security_manager.enabled = true;

    const F = struct {
        fn framed(a: std.mem.Allocator, ftype: wire.Frontend, parts: []const []const u8) ![]u8 {
            var b = wire.Builder.init(a);
            defer b.deinit();
            if (ftype == .startup) {
                try b.putU16(1);
                try b.putU16(0);
            }
            for (parts) |p| try b.putStr16(p);
            return b.finish(@intFromEnum(ftype));
        }
    };

    const Runner = struct {
        fn go(a: std.mem.Allocator, d: *Database, ioh: Io, user: []const u8, pw: []const u8, sql: []const u8, out: *[]u8) !bool {
            const f_su = try F.framed(a, .startup, &.{ user, "", "test" });
            defer a.free(f_su);
            const f_pw = try F.framed(a, .auth_response, &.{pw});
            defer a.free(f_pw);
            const f_q = try F.framed(a, .query, &.{sql});
            defer a.free(f_q);

            var input = std.ArrayList(u8).empty;
            defer input.deinit(a);
            try input.appendSlice(a, f_su);
            try input.appendSlice(a, f_pw);
            try input.appendSlice(a, f_q);

            var reader = std.Io.Reader.fixed(input.items);
            const outbuf = try a.alloc(u8, 64 * 1024);
            var writer = std.Io.Writer.fixed(outbuf);
            var sess = session.Session.init(a, ioh, d, 60_000, null);
            defer sess.deinit();
            session.run(&sess, &reader, &writer) catch {};
            out.* = try a.dupe(u8, writer.buffered());
            a.free(outbuf);
            return sess.authenticated;
        }
    };

    {
        var out: []u8 = undefined;
        const authed = try Runner.go(allocator, db, io, "admin", "admin", "CREATE TABLE t (id INT PRIMARY KEY)", &out);
        defer allocator.free(out);
        try std.testing.expect(authed);
        try std.testing.expect(std.mem.indexOf(u8, out, "Authentication Required") == null);
        try std.testing.expect(std.mem.indexOf(u8, out, "28P01") == null);
    }

    {
        var out: []u8 = undefined;
        const authed = try Runner.go(allocator, db, io, "admin", "wrongpass", "CREATE TABLE t2 (id INT)", &out);
        defer allocator.free(out);
        try std.testing.expect(!authed);
        try std.testing.expect(std.mem.indexOf(u8, out, "28P01") != null);
    }
}

test "O4-TLS: require_tls_for_auth refuses cleartext password on a plaintext link" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const Database = @import("schema.zig").Database;
    const session = @import("proto/session.zig");
    const wire = @import("proto/wire.zig");

    const db_path = "test_o4tls_auth.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    var db = try Database.open(allocator, io, db_path, 64, null);
    defer db.close();
    db.security_manager.enabled = true;
    db.security_manager.require_auth = true;
    db.security_manager.require_tls_for_auth = true;

    const F = struct {
        fn framed(a: std.mem.Allocator, ftype: wire.Frontend, parts: []const []const u8) ![]u8 {
            var b = wire.Builder.init(a);
            defer b.deinit();
            if (ftype == .startup) {
                try b.putU16(1);
                try b.putU16(0);
            }
            for (parts) |p| try b.putStr16(p);
            return b.finish(@intFromEnum(ftype));
        }
    };

    const Runner = struct {
        fn go(a: std.mem.Allocator, d: *Database, ioh: Io, secure: bool, out: *[]u8) !bool {
            const f_su = try F.framed(a, .startup, &.{ "admin", "", "test" });
            defer a.free(f_su);
            const f_pw = try F.framed(a, .auth_response, &.{"admin"});
            defer a.free(f_pw);

            var input = std.ArrayList(u8).empty;
            defer input.deinit(a);
            try input.appendSlice(a, f_su);
            try input.appendSlice(a, f_pw);

            var reader = std.Io.Reader.fixed(input.items);
            const outbuf = try a.alloc(u8, 64 * 1024);
            var writer = std.Io.Writer.fixed(outbuf);
            var sess = session.Session.init(a, ioh, d, 60_000, null);
            sess.secure = secure;
            defer sess.deinit();
            session.run(&sess, &reader, &writer) catch {};
            out.* = try a.dupe(u8, writer.buffered());
            a.free(outbuf);
            return sess.authenticated;
        }
    };

    // Plaintext link: the challenge must be refused with 28000 before any
    // password is read, and the session must NOT be authenticated.
    {
        var out: []u8 = undefined;
        const authed = try Runner.go(allocator, db, io, false, &out);
        defer allocator.free(out);
        try std.testing.expect(!authed);
        try std.testing.expect(std.mem.indexOf(u8, out, "28000") != null);
        // No cleartext-password challenge should have been sent on the plaintext link.
        try std.testing.expect(std.mem.indexOf(u8, out, "28P01") == null);
    }

    // Secure link: the normal challenge/authenticate path runs and succeeds.
    {
        var out: []u8 = undefined;
        const authed = try Runner.go(allocator, db, io, true, &out);
        defer allocator.free(out);
        try std.testing.expect(authed);
        try std.testing.expect(std.mem.indexOf(u8, out, "28000") == null);
    }
}

test "ALTER USER: IDENTIFIED BY rotates the password and preserves the role" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const p = "test_alteruser.db";
    Io.Dir.deleteFile(.cwd(), io, p) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, p) catch {};

    var db = try Database.open(allocator, io, p, 64, null);
    defer db.close();
    db.security_manager.enabled = true;

    var ex = QueryExecutor.init(allocator, db);
    defer ex.deinit();

    var admin_token: ?[]const u8 = null;
    defer if (admin_token) |t| allocator.free(t);
    {
        const res = try ex.execute(.{ .sql = "LOGIN admin 'admin'" });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        admin_token = try allocator.dupe(u8, res.rows[0][0]);
    }
    {
        const res = try ex.execute(.{ .sql = "CREATE USER bob IDENTIFIED BY 'oldpw' ROLE 'read_only'", .session_token = admin_token });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }
    // Old password authenticates before the change.
    _ = try db.security_manager.authenticate("bob", "oldpw", null);

    {
        const res = try ex.execute(.{ .sql = "ALTER USER bob IDENTIFIED BY 'newpw'", .session_token = admin_token });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
        try std.testing.expectEqual(@as(u64, 1), res.rows_affected);
    }

    // Old password now rejected; new password accepted.
    try std.testing.expectError(error.InvalidCredentials, db.security_manager.authenticate("bob", "oldpw", null));
    _ = try db.security_manager.authenticate("bob", "newpw", null);

    // Role preserved across the password change.
    const sec = db.security_manager;
    const bob = sec.users.get("bob") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@import("concurrency/security.zig").Role.read_only, bob.role);

    // ALTER USER on a non-existent principal is a clean error, not a crash.
    {
        const res = try ex.execute(.{ .sql = "ALTER USER ghost IDENTIFIED BY 'x'", .session_token = admin_token });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message != null);
    }
}

test "HOT BACKUP: BACKUP DATABASE TO on a live server yields a restorable snapshot" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const src_path = "test_hotbk_src.db";
    const src_wal = "test_hotbk_src_wal";
    const bk_dir = "test_hotbk_snapshot";
    const dst_path = "test_hotbk_restored.db";
    const dst_wal = "test_hotbk_restored_wal";
    Io.Dir.deleteFile(.cwd(), io, src_path) catch {};
    Io.Dir.deleteFile(.cwd(), io, dst_path) catch {};
    for ([_][]const u8{ src_wal, bk_dir, dst_wal }) |d| Io.Dir.deleteTree(.cwd(), io, d) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, src_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, dst_path) catch {};
    defer for ([_][]const u8{ src_wal, bk_dir, dst_wal }) |d| Io.Dir.deleteTree(.cwd(), io, d) catch {};

    // A LIVE server: opened once, never closed before the backup runs.
    var db = try Database.open(allocator, io, src_path, 64, src_wal);
    defer db.close();
    var ex = QueryExecutor.init(allocator, db);
    defer ex.deinit();

    freeResp(allocator, try ex.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    var i: i64 = 1;
    while (i <= 20) : (i += 1) {
        const sql = try std.fmt.allocPrint(allocator, "INSERT INTO t (id, v) VALUES ({d}, 'r{d}')", .{ i, i });
        defer allocator.free(sql);
        freeResp(allocator, try ex.execute(.{ .sql = sql }));
    }

    // Hot backup while the server is up.
    {
        const sql = try std.fmt.allocPrint(allocator, "BACKUP DATABASE TO '{s}'", .{bk_dir});
        defer allocator.free(sql);
        const res = try ex.execute(.{ .sql = sql });
        defer freeResp(allocator, res);
        try std.testing.expect(res.error_message == null);
    }

    // The server keeps serving after the backup (proves it was never stopped),
    // and these post-backup rows must NOT appear in the snapshot.
    while (i <= 30) : (i += 1) {
        const sql = try std.fmt.allocPrint(allocator, "INSERT INTO t (id, v) VALUES ({d}, 'r{d}')", .{ i, i });
        defer allocator.free(sql);
        freeResp(allocator, try ex.execute(.{ .sql = sql }));
    }

    // Restore the hot-backup artifact with the standard (offline) tooling.
    try Database.restoreSnapshot(allocator, io, bk_dir, dst_path, dst_wal);
    {
        var rdb = try Database.open(allocator, io, dst_path, 64, dst_wal);
        defer rdb.close();
        var rex = QueryExecutor.init(allocator, rdb);
        defer rex.deinit();
        for ([_]i64{ 1, 10, 20 }) |id| { // present at backup time
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM t WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try rex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 1), res.rows.len);
        }
        for ([_]i64{ 21, 25, 30 }) |id| { // inserted AFTER the backup
            const sql = try std.fmt.allocPrint(allocator, "SELECT v FROM t WHERE id = {d}", .{id});
            defer allocator.free(sql);
            const res = try rex.execute(.{ .sql = sql });
            defer freeResp(allocator, res);
            try std.testing.expect(res.error_message == null);
            try std.testing.expectEqual(@as(usize, 0), res.rows.len);
        }
    }
}

test "ADMIN ROTATION: passwordMatches detects the default and clears after rotation" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Database = @import("schema.zig").Database;

    const p = "test_adminrot.db";
    Io.Dir.deleteFile(.cwd(), io, p) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, p) catch {};

    var db = try Database.open(allocator, io, p, 64, null);
    defer db.close();
    db.security_manager.enabled = true;

    const sec = db.security_manager;
    // Fresh DB: the bootstrap default is detectable (this is what the startup
    // gate keys on) and a lockout-free check never trips the brute-force counter.
    try std.testing.expect(sec.passwordMatches("admin", "admin"));
    try std.testing.expect(!sec.passwordMatches("admin", "wrong"));
    try std.testing.expect(!sec.passwordMatches("ghost", "admin"));

    // Rotate exactly as the offline `novadb passwd` CLI does: hash + updateUserPassword.
    var salt: [32]u8 = undefined;
    std.Io.random(io, &salt);
    const hash = try sec.hashKey("newadminpw", salt);
    const hex_hash = std.fmt.bytesToHex(hash, .lower);
    const hex_salt = std.fmt.bytesToHex(salt, .lower);
    const password_hash = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ hex_hash, hex_salt });
    defer allocator.free(password_hash);
    try db.updateUserPassword("admin", password_hash, 1);
    try sec.loadUsers(db);

    // Default no longer matches; the new password does. The startup gate would
    // now permit boot.
    try std.testing.expect(!sec.passwordMatches("admin", "admin"));
    try std.testing.expect(sec.passwordMatches("admin", "newadminpw"));
}

test "WRITER-CACHE: concurrent writers past the query-cache cap don't corrupt (regression)" {
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "cachereg.db";
    const wal_dir = "cachereg_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const SEED: usize = 200;
    var db = try Database.open(alloc, io, db_path, 512, wal_dir);
    defer db.close();
    db.synchronous_commit = false;

    var seed = QueryExecutor.init(alloc, db);
    freeResp(alloc, try seed.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    var si: usize = 0;
    while (si < SEED) : (si += 1) {
        var b: [96]u8 = undefined;
        const s = try std.fmt.bufPrint(&b, "INSERT INTO t (id, v) VALUES ({d}, 'seed')", .{si});
        freeResp(alloc, try seed.execute(.{ .sql = s }));
    }
    seed.deinit();

    const OPS: usize = 4000;
    const W = struct {
        db: *Database,
        alloc: std.mem.Allocator,
        id: usize,
        fn run(w: *@This()) void {
            var ex = QueryExecutor.init(w.alloc, w.db);
            defer ex.deinit();
            var n: usize = 0;
            while (n < OPS) : (n += 1) {
                const key = (w.id *% 131 +% n) % SEED;
                var b: [160]u8 = undefined;
                const s = std.fmt.bufPrint(&b, "UPDATE t SET v = 'w{d}_op{d}_k{d}' WHERE id = {d}", .{ w.id, n, key, key }) catch unreachable;
                const res = ex.execute(.{ .sql = s }) catch continue;
                freeResp(w.alloc, res);
            }
        }
    };

    var group = std.Io.Group.init;
    var ws: [4]W = undefined;
    for (&ws, 0..) |*w, k| {
        w.* = .{ .db = db, .alloc = alloc, .id = k + 1 };
        group.async(io, W.run, .{w});
    }
    group.await(io) catch {};

    var chk = QueryExecutor.init(alloc, db);
    defer chk.deinit();
    const r = try chk.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
    defer freeResp(alloc, r);
    try std.testing.expect(r.error_message == null);
    var buf: [16]u8 = undefined;
    const want = try std.fmt.bufPrint(&buf, "{d}", .{SEED});
    try std.testing.expectEqualStrings(want, r.rows[0][0]);
}

test "STRESS: concurrent disjoint-range writers stay consistent (scan == count == point-lookup)" {
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "stress.db";
    const wal_dir = "stress_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const NW: usize = 4;
    const PER: usize = 1500;
    const BASE: usize = 1_000_000;

    var db = try Database.open(alloc, io, db_path, 512, wal_dir);
    defer db.close();
    db.synchronous_commit = false;

    {
        var s = QueryExecutor.init(alloc, db);
        defer s.deinit();
        freeResp(alloc, try s.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    }

    const Phase = enum { insert, del_odd };
    const W = struct {
        db: *Database,
        alloc: std.mem.Allocator,
        id: usize,
        phase: Phase,
        fn run(w: *@This()) void {
            var ex = QueryExecutor.init(w.alloc, w.db);
            defer ex.deinit();
            var i: usize = 0;
            while (i < PER) : (i += 1) {
                const key = w.id * BASE + i;
                var b: [160]u8 = undefined;
                const sql = switch (w.phase) {
                    .insert => std.fmt.bufPrint(&b, "INSERT INTO t (id, v) VALUES ({d}, 'v{d}')", .{ key, key }) catch unreachable,
                    .del_odd => if (i % 2 == 1)
                        (std.fmt.bufPrint(&b, "DELETE FROM t WHERE id = {d}", .{key}) catch unreachable)
                    else
                        continue,
                };
                const res = ex.execute(.{ .sql = sql }) catch continue;
                freeResp(w.alloc, res);
            }
        }
    };

    const runPhase = struct {
        fn go(d: *Database, a: std.mem.Allocator, io_: std.Io, phase: Phase) void {
            var group = std.Io.Group.init;
            var ws: [NW]W = undefined;
            for (&ws, 0..) |*w, k| {
                w.* = .{ .db = d, .alloc = a, .id = k + 1, .phase = phase };
                group.async(io_, W.run, .{w});
            }
            group.await(io_) catch {};
        }
    }.go;

    const H = struct {
        fn count(a: std.mem.Allocator, d: *Database) !usize {
            var ex = QueryExecutor.init(a, d);
            defer ex.deinit();
            const r = try ex.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
            defer freeResp(a, r);
            try std.testing.expect(r.error_message == null);
            return try std.fmt.parseInt(usize, r.rows[0][0], 10);
        }
        fn scanCount(a: std.mem.Allocator, d: *Database) !usize {
            var ex = QueryExecutor.init(a, d);
            defer ex.deinit();
            const r = try ex.execute(.{ .sql = "SELECT id FROM t" });
            defer freeResp(a, r);
            try std.testing.expect(r.error_message == null);
            return r.rows.len;
        }
        fn present(a: std.mem.Allocator, d: *Database, id: usize) !bool {
            var ex = QueryExecutor.init(a, d);
            defer ex.deinit();
            var b: [96]u8 = undefined;
            const sql = try std.fmt.bufPrint(&b, "SELECT v FROM t WHERE id = {d}", .{id});
            const r = try ex.execute(.{ .sql = sql });
            defer freeResp(a, r);
            try std.testing.expect(r.error_message == null);
            return r.rows.len == 1;
        }
    };

    runPhase(db, alloc, io, .insert);
    const total = NW * PER;
    try std.testing.expectEqual(total, try H.count(alloc, db));
    try std.testing.expectEqual(total, try H.scanCount(alloc, db));
    for (0..NW) |w| {
        var i: usize = 0;
        while (i < PER) : (i += 137) {
            try std.testing.expect(try H.present(alloc, db, (w + 1) * BASE + i));
        }
    }

    runPhase(db, alloc, io, .del_odd);
    const survivors = total - (total / 2);
    try std.testing.expectEqual(survivors, try H.count(alloc, db));
    try std.testing.expectEqual(survivors, try H.scanCount(alloc, db));
    for (0..NW) |w| {
        var i: usize = 0;
        while (i < PER) : (i += 111) {
            const key = (w + 1) * BASE + i;
            const should = (i % 2 == 0);
            try std.testing.expectEqual(should, try H.present(alloc, db, key));
        }
    }
}

test "STRESS: cross-table concurrent writers stay consistent (per-table locks)" {
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "xtable.db";
    const wal_dir = "xtable_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const NT: usize = 4;
    const PER: usize = 1500;

    var db = try Database.open(alloc, io, db_path, 512, wal_dir);
    defer db.close();
    db.synchronous_commit = false;

    {
        var s = QueryExecutor.init(alloc, db);
        defer s.deinit();
        for (0..NT) |t| {
            var b: [96]u8 = undefined;
            const sql = try std.fmt.bufPrint(&b, "CREATE TABLE t{d} (id INT PRIMARY KEY, v TEXT)", .{t});
            freeResp(alloc, try s.execute(.{ .sql = sql }));
        }
    }

    const Phase = enum { insert, del_odd };
    const W = struct {
        db: *Database,
        alloc: std.mem.Allocator,
        table: usize,
        phase: Phase,
        fn run(w: *@This()) void {
            var ex = QueryExecutor.init(w.alloc, w.db);
            defer ex.deinit();
            var i: usize = 0;
            while (i < PER) : (i += 1) {
                var b: [160]u8 = undefined;
                const sql = switch (w.phase) {
                    .insert => std.fmt.bufPrint(&b, "INSERT INTO t{d} (id, v) VALUES ({d}, 'v{d}')", .{ w.table, i, i }) catch unreachable,
                    .del_odd => if (i % 2 == 1)
                        (std.fmt.bufPrint(&b, "DELETE FROM t{d} WHERE id = {d}", .{ w.table, i }) catch unreachable)
                    else
                        continue,
                };
                const res = ex.execute(.{ .sql = sql }) catch continue;
                freeResp(w.alloc, res);
            }
        }
    };

    const runPhase = struct {
        fn go(d: *Database, a: std.mem.Allocator, io_: std.Io, phase: Phase) void {
            var group = std.Io.Group.init;
            var ws: [NT]W = undefined;
            for (&ws, 0..) |*w, t| {
                w.* = .{ .db = d, .alloc = a, .table = t, .phase = phase };
                group.async(io_, W.run, .{w});
            }
            group.await(io_) catch {};
        }
    }.go;

    const H = struct {
        fn count(a: std.mem.Allocator, d: *Database, table: usize) !usize {
            var ex = QueryExecutor.init(a, d);
            defer ex.deinit();
            var b: [64]u8 = undefined;
            const sql = try std.fmt.bufPrint(&b, "SELECT COUNT(*) FROM t{d}", .{table});
            const r = try ex.execute(.{ .sql = sql });
            defer freeResp(a, r);
            try std.testing.expect(r.error_message == null);
            return try std.fmt.parseInt(usize, r.rows[0][0], 10);
        }
        fn scanCount(a: std.mem.Allocator, d: *Database, table: usize) !usize {
            var ex = QueryExecutor.init(a, d);
            defer ex.deinit();
            var b: [64]u8 = undefined;
            const sql = try std.fmt.bufPrint(&b, "SELECT id FROM t{d}", .{table});
            const r = try ex.execute(.{ .sql = sql });
            defer freeResp(a, r);
            try std.testing.expect(r.error_message == null);
            return r.rows.len;
        }
        fn present(a: std.mem.Allocator, d: *Database, table: usize, id: usize) !bool {
            var ex = QueryExecutor.init(a, d);
            defer ex.deinit();
            var b: [80]u8 = undefined;
            const sql = try std.fmt.bufPrint(&b, "SELECT v FROM t{d} WHERE id = {d}", .{ table, id });
            const r = try ex.execute(.{ .sql = sql });
            defer freeResp(a, r);
            try std.testing.expect(r.error_message == null);
            return r.rows.len == 1;
        }
    };

    runPhase(db, alloc, io, .insert);
    for (0..NT) |t| {
        try std.testing.expectEqual(PER, try H.count(alloc, db, t));
        try std.testing.expectEqual(PER, try H.scanCount(alloc, db, t));
        var i: usize = 0;
        while (i < PER) : (i += 137) try std.testing.expect(try H.present(alloc, db, t, i));
    }

    runPhase(db, alloc, io, .del_odd);
    const survivors = PER - (PER / 2);
    for (0..NT) |t| {
        try std.testing.expectEqual(survivors, try H.count(alloc, db, t));
        try std.testing.expectEqual(survivors, try H.scanCount(alloc, db, t));
        var i: usize = 0;
        while (i < PER) : (i += 111) try std.testing.expectEqual(i % 2 == 0, try H.present(alloc, db, t, i));
    }
}

test "STRESS: concurrent readers + writers on one table stay consistent (group lock R/W exclusion)" {
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const db_path = "rw.db";
    const wal_dir = "rw_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const NW: usize = 3;
    const NR: usize = 3;
    const PER: usize = 1200;
    const BASE: usize = 1_000_000;

    var db = try Database.open(alloc, io, db_path, 512, wal_dir);
    defer db.close();
    db.synchronous_commit = false;

    {
        var s = QueryExecutor.init(alloc, db);
        defer s.deinit();
        freeResp(alloc, try s.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    }

    const total = NW * PER;
    const Ctx = struct {
        db: *Database,
        alloc: std.mem.Allocator,
        id: usize,
        done: *std.atomic.Value(u32),
        total: usize,
        bad: *std.atomic.Value(u32),

        fn writer(w: *@This()) void {
            var ex = QueryExecutor.init(w.alloc, w.db);
            defer ex.deinit();
            var i: usize = 0;
            while (i < PER) : (i += 1) {
                const key = (w.id + 1) * BASE + i;
                var b: [160]u8 = undefined;
                const sql = std.fmt.bufPrint(&b, "INSERT INTO t (id, v) VALUES ({d}, 'v{d}')", .{ key, key }) catch unreachable;
                const res = ex.execute(.{ .sql = sql }) catch continue;
                freeResp(w.alloc, res);
            }
            _ = w.done.fetchAdd(1, .seq_cst);
        }
        fn reader(w: *@This()) void {
            var ex = QueryExecutor.init(w.alloc, w.db);
            defer ex.deinit();
            var extra: usize = 0;
            while (true) {
                const all_done = w.done.load(.seq_cst) == NW;
                const rc = ex.execute(.{ .sql = "SELECT COUNT(*) FROM t" }) catch continue;
                const cnt = std.fmt.parseInt(usize, rc.rows[0][0], 10) catch {
                    freeResp(w.alloc, rc);
                    _ = w.bad.fetchAdd(1, .seq_cst);
                    continue;
                };
                freeResp(w.alloc, rc);

                const rs = ex.execute(.{ .sql = "SELECT id FROM t" }) catch continue;
                const scanned = rs.rows.len;
                freeResp(w.alloc, rs);

                if (cnt > w.total or scanned > w.total) _ = w.bad.fetchAdd(1, .seq_cst);
                if (all_done) {
                    extra += 1;
                    if (extra > 3) break;
                }
            }
        }
    };

    var done = std.atomic.Value(u32).init(0);
    var bad = std.atomic.Value(u32).init(0);
    var group = std.Io.Group.init;
    var ws: [NW]Ctx = undefined;
    var rs: [NR]Ctx = undefined;
    for (&ws, 0..) |*w, k| {
        w.* = .{ .db = db, .alloc = alloc, .id = k, .done = &done, .total = total, .bad = &bad };
        group.async(io, Ctx.writer, .{w});
    }
    for (&rs, 0..) |*r, k| {
        r.* = .{ .db = db, .alloc = alloc, .id = k, .done = &done, .total = total, .bad = &bad };
        group.async(io, Ctx.reader, .{r});
    }
    group.await(io) catch {};

    try std.testing.expectEqual(@as(u32, 0), bad.load(.seq_cst));

    var chk = QueryExecutor.init(alloc, db);
    defer chk.deinit();
    const rc = try chk.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
    defer freeResp(alloc, rc);
    try std.testing.expectEqual(total, try std.fmt.parseInt(usize, rc.rows[0][0], 10));
    const rsx = try chk.execute(.{ .sql = "SELECT id FROM t" });
    defer freeResp(alloc, rsx);
    try std.testing.expectEqual(total, rsx.rows.len);
}


/// The fixed-width key encoding shared by the B+Tree fuzz suites.
///
/// A key is exactly 11 bytes: a literal `k`, a 3-digit zero-padded namespace, and a
/// 7-digit zero-padded sequence (`k{ns:0>3}{seq:0>7}`). The fixed layout is what lets
/// [`FuzzKey.nsOf`] and [`FuzzKey.seqOf`] recover the two components by byte offset,
/// and it makes keys sort lexicographically in the same order as `(ns, seq)` numerically,
/// so the B+Tree's ordered scans line up with the fuzzer's in-memory model. The digit
/// widths bound the fuzzers to ns < 1000 and seq < 10_000_000.
// True when an environment variable whose name starts with `prefix` (include the `=`) is present.
// Used to scale the concurrency fuzzers up under NOVADB_FUZZ / opt into the perf test under NOVADB_PERF.
fn envSet(prefix: []const u8) bool {
    var i: usize = 0;
    while (std.c.environ[i]) |entry| : (i += 1) {
        if (std.mem.startsWith(u8, std.mem.sliceTo(entry, 0), prefix)) return true;
    }
    return false;
}

const FuzzKey = struct {
    /// Formats `(ns, seq)` into `buf` as the 11-byte key and returns the slice of `buf`.
    ///
    /// `buf` must be exactly 11 bytes (`*[11]u8`); the format is width-exact so it always
    /// fills the whole buffer. A format failure is `unreachable` because the widths and
    /// buffer size are fixed at comptime.
    fn encode(buf: *[11]u8, ns: u32, seq: u32) []const u8 {
        return std.fmt.bufPrint(buf, "k{d:0>3}{d:0>7}", .{ ns, seq }) catch unreachable;
    }
    /// Extracts the sequence number from bytes `[4..11]` of a key produced by
    /// [`FuzzKey.encode`]. Parse failure is `unreachable`: it only receives keys this
    /// module encoded, so the digits are guaranteed well-formed.
    fn seqOf(key: []const u8) u32 {
        return std.fmt.parseInt(u32, key[4..11], 10) catch unreachable;
    }
    /// Extracts the namespace from bytes `[1..4]` of a key produced by
    /// [`FuzzKey.encode`] (byte 0 is the `k` prefix). Parse failure is `unreachable` for
    /// the same reason as [`FuzzKey.seqOf`].
    fn nsOf(key: []const u8) u32 {
        return std.fmt.parseInt(u32, key[1..4], 10) catch unreachable;
    }
};

test "FUZZER (serial): randomized insert/delete/search stays model-consistent + structurally sound" {
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const PagePool = @import("storage/pool.zig").PagePool;
    const BPlusTree = @import("storage/btree.zig").BPlusTree;

    const db_path = "fuzz_serial.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const pool = try PagePool.init(alloc, io, db_path, 64);
    defer pool.deinit() catch {};
    const tree = try BPlusTree.create(pool, alloc);
    defer tree.deinit();

    const KS: u32 = 4000;
    const OPS: usize = 60_000;
    const NS: u32 = 0;

    var live = try alloc.alloc(bool, KS);
    defer alloc.free(live);
    @memset(live, false);

    var prng = std.Random.DefaultPrng.init(0xC0FFEE_1234);
    const rnd = prng.random();

    var kbuf: [11]u8 = undefined;
    var op: usize = 0;
    while (op < OPS) : (op += 1) {
        const seq = rnd.uintLessThan(u32, KS);
        const key = FuzzKey.encode(&kbuf, NS, seq);
        const roll = rnd.uintLessThan(u8, 100);
        if (roll < 45) {
            var vbuf: [24]u8 = undefined;
            const val = std.fmt.bufPrint(&vbuf, "v{d}", .{seq}) catch unreachable;
            if (live[seq]) {
                try std.testing.expectError(error.KeyAlreadyExists, tree.insert(key, val));
            } else {
                try tree.insert(key, val);
                live[seq] = true;
            }
        } else if (roll < 75) {
            if (live[seq]) {
                try tree.delete(key);
                live[seq] = false;
            } else {
                try std.testing.expectError(error.KeyNotFound, tree.delete(key));
            }
        } else {
            const got = try tree.search(key, alloc);
            defer if (got) |g| alloc.free(g);
            try std.testing.expectEqual(live[seq], got != null);
        }

        if (op % 2000 == 0) try tree.checkInvariants();
    }

    try tree.checkInvariants();

    var expected: usize = 0;
    for (live) |b| {
        if (b) expected += 1;
    }
    var seen = try alloc.alloc(bool, KS);
    defer alloc.free(seen);
    @memset(seen, false);

    var scanned: usize = 0;
    var it = try tree.iterator();
    defer it.deinit();
    while (try it.next()) |cell| {
        const seq = FuzzKey.seqOf(cell.key);
        try std.testing.expect(seq < KS);
        try std.testing.expect(live[seq]);
        try std.testing.expect(!seen[seq]);
        seen[seq] = true;
        scanned += 1;
    }
    try std.testing.expectEqual(expected, scanned);
}

test "FUZZER (concurrent): disjoint-namespace writers on ONE tree stay model-consistent" {
    // Default-on (a short run) so the riskiest path -- concurrent writers racing structure_lock on one
    // tree -- is exercised by the plain `zig build test` gate, not just opt-in. NOVADB_FUZZ=1 scales it up
    // to the long soak. (It used to SkipZigTest without the env, so the gate never ran it.)
    const fuzz_full = envSet("NOVADB_FUZZ=");

    const alloc = std.heap.c_allocator;

    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const PagePool = @import("storage/pool.zig").PagePool;
    const BPlusTree = @import("storage/btree.zig").BPlusTree;

    const db_path = "fuzz_concurrent.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const pool = try PagePool.init(alloc, io, db_path, 256);
    defer pool.deinit() catch {};
    const tree = try BPlusTree.create(pool, alloc);
    defer tree.deinit();

    const NW: u32 = 4;
    const KS: u32 = 3000;
    const OPS: usize = if (fuzz_full) 40_000 else 6_000;

    const Writer = struct {
        tree: *BPlusTree,
        alloc: std.mem.Allocator,
        ns: u32,
        live: []bool,
        ops: usize,
        failed: bool = false,
        detail: [128]u8 = undefined,

        fn fail(w: *@This(), comptime fmt: []const u8, args: anytype) void {
            w.failed = true;
            _ = std.fmt.bufPrint(&w.detail, fmt, args) catch {};
        }

        fn run(w: *@This()) void {
            var prng = std.Random.DefaultPrng.init(0xABCD_0000 + w.ns);
            const rnd = prng.random();
            var kbuf: [11]u8 = undefined;
            var op: usize = 0;
            while (op < w.ops and !w.failed) : (op += 1) {
                const seq = rnd.uintLessThan(u32, KS);
                const key = FuzzKey.encode(&kbuf, w.ns, seq);
                const roll = rnd.uintLessThan(u8, 100);
                if (roll < 45) {
                    var vbuf: [24]u8 = undefined;
                    const val = std.fmt.bufPrint(&vbuf, "v{d}", .{seq}) catch unreachable;
                    if (w.live[seq]) {
                        w.tree.insert(key, val) catch |e| {
                            if (e != error.KeyAlreadyExists) w.fail("insert dup got {s}", .{@errorName(e)});
                            continue;
                        };
                        w.fail("insert of live key {d} unexpectedly succeeded", .{seq});
                    } else {
                        w.tree.insert(key, val) catch |e| {
                            w.fail("insert seq {d} failed {s}", .{ seq, @errorName(e) });
                            continue;
                        };
                        w.live[seq] = true;
                    }
                } else if (roll < 75) {
                    if (w.live[seq]) {
                        w.tree.delete(key) catch |e| {
                            w.fail("delete seq {d} failed {s}", .{ seq, @errorName(e) });
                            continue;
                        };
                        w.live[seq] = false;
                    } else {
                        w.tree.delete(key) catch |e| {
                            if (e != error.KeyNotFound) w.fail("delete miss got {s}", .{@errorName(e)});
                            continue;
                        };
                        w.fail("delete of absent key {d} unexpectedly succeeded", .{seq});
                    }
                } else {
                    const got = w.tree.search(key, w.alloc) catch |e| {
                        w.fail("search seq {d} failed {s}", .{ seq, @errorName(e) });
                        continue;
                    };
                    defer if (got) |g| w.alloc.free(g);
                    if ((got != null) != w.live[seq])
                        w.fail("search seq {d}: tree={} model={}", .{ seq, got != null, w.live[seq] });
                }
            }
        }
    };

    var lives: [NW][]bool = undefined;
    for (&lives) |*l| {
        l.* = try alloc.alloc(bool, KS);
        @memset(l.*, false);
    }
    defer for (lives) |l| alloc.free(l);

    var ws: [NW]Writer = undefined;
    var group = std.Io.Group.init;
    for (&ws, 0..) |*w, k| {
        w.* = .{ .tree = tree, .alloc = alloc, .ns = @intCast(k), .live = lives[k], .ops = OPS };
        group.async(io, Writer.run, .{w});
    }
    group.await(io) catch {};

    for (&ws) |*w| {
        if (w.failed) {
            std.log.err("writer ns={d} FAILED: {s}", .{ w.ns, std.mem.sliceTo(&w.detail, 0) });
            return error.FuzzerModelMismatch;
        }
    }

    try tree.checkInvariants();

    var expected: usize = 0;
    for (lives) |l| for (l) |b| {
        if (b) expected += 1;
    };

    var scanned: usize = 0;
    var it = try tree.iterator();
    defer it.deinit();
    while (try it.next()) |cell| {
        const ns = FuzzKey.nsOf(cell.key);
        const seq = FuzzKey.seqOf(cell.key);
        try std.testing.expect(ns < NW and seq < KS);
        try std.testing.expect(lives[ns][seq]);
        scanned += 1;
    }
    try std.testing.expectEqual(expected, scanned);
}

test "FUZZER (concurrent): OVERLAPPING-key writers on one tree stay self-consistent + structurally sound" {
    // The highest-risk concurrency path: multiple writers hammering the SAME key range, so they contend
    // on the same keys and race splits/merges under the per-tree structure_lock (unlike the disjoint-
    // namespace fuzzer above, where writers never touch each other's keys). We cannot predict which keys
    // survive, so instead of a per-key model we assert INTERNAL consistency after the storm: the tree's
    // structural invariants hold, and every key reachable by a full scan is also found by a point search
    // (scan == point-lookup). No writer may hit anything but the benign KeyAlreadyExists / KeyNotFound
    // races. Default-on (short); NOVADB_FUZZ=1 scales it up.
    const fuzz_full = envSet("NOVADB_FUZZ=");
    const alloc = std.heap.c_allocator;

    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const PagePool = @import("storage/pool.zig").PagePool;
    const BPlusTree = @import("storage/btree.zig").BPlusTree;

    const db_path = "fuzz_overlap.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const pool = try PagePool.init(alloc, io, db_path, 256);
    defer pool.deinit() catch {};
    const tree = try BPlusTree.create(pool, alloc);
    defer tree.deinit();

    const NW: u32 = 4;
    const KS: u32 = 1500;                            // SHARED key space -> writers collide
    const OPS: usize = if (fuzz_full) 40_000 else 6_000;

    const Writer = struct {
        tree: *BPlusTree,
        alloc: std.mem.Allocator,
        seed: u32,
        ops: usize,
        failed: bool = false,
        detail: [128]u8 = undefined,

        fn fail(w: *@This(), comptime fmt: []const u8, args: anytype) void {
            w.failed = true;
            _ = std.fmt.bufPrint(&w.detail, fmt, args) catch {};
        }

        fn run(w: *@This()) void {
            var prng = std.Random.DefaultPrng.init(0x51A5_0000 + w.seed);
            const rnd = prng.random();
            var kbuf: [11]u8 = undefined;
            var op: usize = 0;
            while (op < w.ops and !w.failed) : (op += 1) {
                const seq = rnd.uintLessThan(u32, KS);
                const key = FuzzKey.encode(&kbuf, 0, seq);   // ns 0 for everyone: shared keys
                const roll = rnd.uintLessThan(u8, 100);
                if (roll < 50) {
                    var vbuf: [24]u8 = undefined;
                    const val = std.fmt.bufPrint(&vbuf, "v{d}", .{seq}) catch unreachable;
                    w.tree.insert(key, val) catch |e| {
                        if (e != error.KeyAlreadyExists) w.fail("insert seq {d}: {s}", .{ seq, @errorName(e) });
                    };
                } else if (roll < 80) {
                    w.tree.delete(key) catch |e| {
                        if (e != error.KeyNotFound) w.fail("delete seq {d}: {s}", .{ seq, @errorName(e) });
                    };
                } else {
                    const got = w.tree.search(key, w.alloc) catch |e| {
                        w.fail("search seq {d}: {s}", .{ seq, @errorName(e) });
                        continue;
                    };
                    if (got) |g| w.alloc.free(g);
                }
            }
        }
    };

    var ws: [NW]Writer = undefined;
    var group = std.Io.Group.init;
    for (&ws, 0..) |*w, k| {
        w.* = .{ .tree = tree, .alloc = alloc, .seed = @intCast(k), .ops = OPS };
        group.async(io, Writer.run, .{w});
    }
    group.await(io) catch {};

    for (&ws) |*w| {
        if (w.failed) {
            std.log.err("overlap writer seed={d} FAILED: {s}", .{ w.seed, std.mem.sliceTo(&w.detail, 0) });
            return error.FuzzerCorruption;
        }
    }

    // Structural soundness, then scan == point-lookup: every key a full scan yields must be found by a
    // fresh point search (a split/merge race must never leave a key visible to one path but not the other).
    try tree.checkInvariants();
    var scanned: usize = 0;
    var it = try tree.iterator();
    defer it.deinit();
    while (try it.next()) |cell| {
        const got = try tree.search(cell.key, alloc);
        if (got) |g| {
            alloc.free(g);
        } else {
            std.log.err("scan/point mismatch: key {s} scanned but not found by search", .{cell.key});
            return error.ScanPointMismatch;
        }
        scanned += 1;
    }
    // Sanity: the count is stable across a second independent scan.
    var scanned2: usize = 0;
    var it2 = try tree.iterator();
    defer it2.deinit();
    while (try it2.next()) |_| scanned2 += 1;
    try std.testing.expectEqual(scanned, scanned2);
}

test "REGRESSION: merge pin-lifetime -- concurrent delete storm with widened discard window" {
    // Guards the fix for the merge/discard PageStillPinned race: mergePages,
    // deleteExclusive's root collapse, and handleUnderflow all used to keep a page
    // (parent / old root) pinned across a discardPage of that same page, so a
    // delete that collapsed the tree tripped `error.PageStillPinned`. The natural
    // race is rare, so this test turns on the fault hook (`fault_merge_yield_ms`)
    // to WIDEN the unpin->discard window, making a reintroduced pin-lifetime bug
    // fail deterministically. Delete-heavy on a tiny shared key space -> maximal
    // split/merge/collapse churn. On the fixed code this passes with zero failures.
    const alloc = std.heap.c_allocator;
    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const PagePool = @import("storage/pool.zig").PagePool;
    const bt = @import("storage/btree.zig");

    const db_path = "regress_merge_pin.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};

    const pool = try PagePool.init(alloc, io, db_path, 64);
    defer pool.deinit() catch {};
    const tree = try bt.BPlusTree.create(pool, alloc);
    defer tree.deinit();

    bt.fault_merge_yield_ms = 2; // widen the unpin->discard window
    defer bt.fault_merge_yield_ms = 0;

    const NW: u32 = 6;
    const KS: u32 = 96; // tiny shared space -> constant split/merge/collapse
    const OPS: usize = 5_000;

    const Writer = struct {
        tree: *bt.BPlusTree,
        seed: u32,
        ops: usize,
        failed: bool = false,
        detail: [96]u8 = undefined,
        fn run(w: *@This()) void {
            var prng = std.Random.DefaultPrng.init(0xC0FFEE00 + w.seed);
            const rnd = prng.random();
            var kbuf: [11]u8 = undefined;
            var op: usize = 0;
            while (op < w.ops and !w.failed) : (op += 1) {
                const seq = rnd.uintLessThan(u32, KS);
                const key = FuzzKey.encode(&kbuf, 0, seq);
                if (rnd.uintLessThan(u8, 100) < 35) {
                    var vbuf: [24]u8 = undefined;
                    const val = std.fmt.bufPrint(&vbuf, "v{d}", .{seq}) catch unreachable;
                    w.tree.insert(key, val) catch |e| {
                        if (e != error.KeyAlreadyExists) {
                            w.failed = true;
                            _ = std.fmt.bufPrint(&w.detail, "insert {d}: {s}", .{ seq, @errorName(e) }) catch {};
                        }
                    };
                } else {
                    w.tree.delete(key) catch |e| {
                        if (e != error.KeyNotFound) {
                            w.failed = true;
                            _ = std.fmt.bufPrint(&w.detail, "delete {d}: {s}", .{ seq, @errorName(e) }) catch {};
                        }
                    };
                }
            }
        }
    };

    var ws: [NW]Writer = undefined;
    var group = std.Io.Group.init;
    for (&ws, 0..) |*w, k| {
        w.* = .{ .tree = tree, .seed = @intCast(k), .ops = OPS };
        group.async(io, Writer.run, .{w});
    }
    group.await(io) catch {};

    for (&ws) |*w| {
        if (w.failed) {
            std.debug.print("merge pin-lifetime regression: writer {d} FAILED: {s}\n", .{ w.seed, std.mem.sliceTo(&w.detail, 0) });
            return error.MergePinLifetimeRegression;
        }
    }
    try tree.checkInvariants();
}

test "STRESS: parallel disjoint inserts all land -- concurrent writers admitted without lost writes" {
    // The correctness half of "writes are not serialized": 4 writers insert N disjoint keys concurrently
    // through the GroupLock (write mode) + per-tree structure_lock. If concurrent admission were broken
    // (a writer clobbering another's split, or a lost insert under contention), the final key count would
    // fall short of N. This gates that the parallel-write path admits all writers and loses nothing. It is
    // clock-free (this toolchain's std.time has no wall clock), so it is deterministic and gate-safe;
    // the raw throughput number is a separate manual benchmark.
    const alloc = std.heap.c_allocator;

    var threaded = std.Io.Threaded.init(alloc, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const PagePool = @import("storage/pool.zig").PagePool;
    const BPlusTree = @import("storage/btree.zig").BPlusTree;

    const N: u32 = 40_000;
    const NW: u32 = 4;
    const per: u32 = N / NW;

    const db_path = "stress_parallel.db";
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    const pool = try PagePool.init(alloc, io, db_path, 2048);
    defer pool.deinit() catch {};
    const tree = try BPlusTree.create(pool, alloc);
    defer tree.deinit();

    const Inserter = struct {
        tree: *BPlusTree,
        ns: u32,
        n: u32,
        failed: bool = false,
        fn run(s: *@This()) void {
            var kbuf: [11]u8 = undefined;
            var vbuf: [24]u8 = undefined;
            var seq: u32 = 0;
            while (seq < s.n) : (seq += 1) {
                const key = FuzzKey.encode(&kbuf, s.ns, seq);
                const val = std.fmt.bufPrint(&vbuf, "v{d}", .{seq}) catch unreachable;
                s.tree.insert(key, val) catch {
                    s.failed = true;
                    return;
                };
            }
        }
    };

    var ss: [NW]Inserter = undefined;
    var group = std.Io.Group.init;
    for (&ss, 0..) |*s, k| {
        s.* = .{ .tree = tree, .ns = @intCast(k), .n = per };
        group.async(io, Inserter.run, .{s});
    }
    group.await(io) catch {};

    for (&ss) |*s| try std.testing.expect(!s.failed);

    // Every one of the N inserts must be present: count via scan, and spot-check each writer's whole range.
    try tree.checkInvariants();
    var scanned: usize = 0;
    var it = try tree.iterator();
    defer it.deinit();
    while (try it.next()) |_| scanned += 1;
    try std.testing.expectEqual(@as(usize, N), scanned);

    var kbuf: [11]u8 = undefined;
    var w: u32 = 0;
    while (w < NW) : (w += 1) {
        var seq: u32 = 0;
        while (seq < per) : (seq += 1) {
            const got = try tree.search(FuzzKey.encode(&kbuf, w, seq), alloc);
            try std.testing.expect(got != null);
            if (got) |g| alloc.free(g);
        }
    }
}


test "SQLCORRECT: DECIMAL orders numerically (not lexically) and DATE orders chronologically" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_decdate.db";
    const wal_dir = "test_decdate_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE m (id INT PRIMARY KEY, price DECIMAL, d DATE)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO m (id, price, d) VALUES (1, '12.25', '2024-03-05')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO m (id, price, d) VALUES (2, '3.5', '2024-01-10')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO m (id, price, d) VALUES (3, '100.1', '2024-12-01')" }));

    {
        const r = try exec.execute(.{ .sql = "SELECT price FROM m ORDER BY price ASC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 3), r.rows.len);
        try std.testing.expectEqualStrings("3.5", r.rows[0][0]);
        try std.testing.expectEqualStrings("12.25", r.rows[1][0]);
        try std.testing.expectEqualStrings("100.1", r.rows[2][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM m WHERE price > 5 ORDER BY id ASC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 2), r.rows.len);
        try std.testing.expectEqualStrings("1", r.rows[0][0]);
        try std.testing.expectEqualStrings("3", r.rows[1][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT d FROM m ORDER BY d ASC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("2024-01-10", r.rows[0][0]);
        try std.testing.expectEqualStrings("2024-03-05", r.rows[1][0]);
        try std.testing.expectEqualStrings("2024-12-01", r.rows[2][0]);
    }

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE p (id INT PRIMARY KEY, v DECIMAL)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO p (id, v) VALUES (1, '1.0000000000000002')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO p (id, v) VALUES (2, '1.0000000000000001')" }));
    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM p ORDER BY v ASC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
        try std.testing.expectEqualStrings("1", r.rows[1][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM p WHERE v > '1.00000000000000015' ORDER BY id ASC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 1), r.rows.len);
        try std.testing.expectEqualStrings("1", r.rows[0][0]);
    }
}

test "SQLCORRECT: order by sorts, distinct dedupes, unique rejects duplicates" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_sqlcorrect.db";
    const wal_dir = "test_sqlcorrect_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};

    const Database = @import("schema.zig").Database;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    const ddl = "CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, email TEXT UNIQUE)";
    freeResp(allocator, try exec.execute(.{ .sql = ddl }));
    const inserts = [_][]const u8{
        "INSERT INTO t (id, dept, email) VALUES (3, 'eng', 'c@x')",
        "INSERT INTO t (id, dept, email) VALUES (1, 'sales', 'a@x')",
        "INSERT INTO t (id, dept, email) VALUES (2, 'eng', 'b@x')",
    };
    for (inserts) |sql| {
        const r = try exec.execute(.{ .sql = sql });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
    }

    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM t ORDER BY id" });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 3), r.rows.len);
        try std.testing.expectEqualStrings("1", r.rows[0][0]);
        try std.testing.expectEqualStrings("2", r.rows[1][0]);
        try std.testing.expectEqualStrings("3", r.rows[2][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM t ORDER BY id DESC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("3", r.rows[0][0]);
        try std.testing.expectEqualStrings("1", r.rows[2][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT DISTINCT dept FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 2), r.rows.len);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT COUNT(DISTINCT dept) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "INSERT INTO t (id, dept, email) VALUES (4, 'eng', 'a@x')" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message != null);
    }
    {
        const r = try exec.execute(.{ .sql = "INSERT INTO t (id, dept, email) VALUES (5, 'eng', 'z@x')" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
    }
}

test "MVCC: a failed-commit transaction's rows are not visible in memory" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_abortvis.db";
    const wal_dir = "test_abortvis_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const WalError = @import("durability/wal_error.zig").WalError;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    db.synchronous_commit = true;
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 'a')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "BEGIN" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (2, 'b')" }));
    if (db.wal) |w| w.test_inject_write_error = WalError.NoSpaceLeft;
    var commit_ok = false;
    if (exec.execute(.{ .sql = "COMMIT" })) |resp| {
        defer freeResp(allocator, resp);
        if (resp.error_message == null) commit_ok = true;
    } else |_| {}
    try std.testing.expect(!commit_ok);

    const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
    defer freeResp(allocator, r);
    try std.testing.expect(r.error_message == null);
    try std.testing.expectEqualStrings("1", r.rows[0][0]);
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (3, 'c')" }));
    const r2 = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
    defer freeResp(allocator, r2);
    try std.testing.expectEqualStrings("2", r2.rows[0][0]);
}

test "CBO: cost model chooses join algorithm from statistics" {
    const CostModel = @import("query/query_executor.zig").CostModel;
    try std.testing.expect(CostModel.preferHashJoin(1000, 1000));
    try std.testing.expect(CostModel.preferHashJoin(50, 50));
    try std.testing.expect(!CostModel.preferHashJoin(1, 1));
    try std.testing.expect(!CostModel.preferHashJoin(3, 1));
    try std.testing.expect(CostModel.hashJoinCost(1000, 1000) < CostModel.nestedLoopCost(1000, 1000));
}

test "CBO: an equi-join returns correct rows via the cost-model plan" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_cbo.db";
    const wal_dir = "test_cbo_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE emp (id INT PRIMARY KEY, dept_id INT, name TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE dept (id INT PRIMARY KEY, dname TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO dept (id, dname) VALUES (1, 'eng'), (2, 'sales')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO emp (id, dept_id, name) VALUES (1, 1, 'a'), (2, 1, 'b'), (3, 2, 'c')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "ANALYZE emp" }));
    freeResp(allocator, try exec.execute(.{ .sql = "ANALYZE dept" }));

    const r = try exec.execute(.{ .sql = "SELECT emp.name FROM emp JOIN dept ON emp.dept_id = dept.id WHERE dept.dname = 'eng' ORDER BY emp.name ASC" });
    defer freeResp(allocator, r);
    try std.testing.expect(r.error_message == null);
    try std.testing.expectEqual(@as(usize, 2), r.rows.len);
    try std.testing.expectEqualStrings("a", r.rows[0][0]);
    try std.testing.expectEqualStrings("b", r.rows[1][0]);
}

test "SQLEXT: SAVEPOINT / ROLLBACK TO / RELEASE" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_savepoint.db";
    const wal_dir = "test_savepoint_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));

    const expectCount = struct {
        fn f(e: *QueryExecutor, a: std.mem.Allocator, want: []const u8) !void {
            const r = try e.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
            defer freeResp(a, r);
            try std.testing.expectEqualStrings(want, r.rows[0][0]);
        }
    }.f;

    freeResp(allocator, try exec.execute(.{ .sql = "BEGIN" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 'a')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "SAVEPOINT sp1" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (2, 'b')" }));
    try expectCount(&exec, allocator, "2");
    freeResp(allocator, try exec.execute(.{ .sql = "ROLLBACK TO sp1" }));
    try expectCount(&exec, allocator, "1");
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (3, 'c')" }));
    try expectCount(&exec, allocator, "2");
    freeResp(allocator, try exec.execute(.{ .sql = "COMMIT" }));
    try expectCount(&exec, allocator, "2");

    freeResp(allocator, try exec.execute(.{ .sql = "BEGIN" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (4, 'd')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "SAVEPOINT a" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (5, 'e')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "SAVEPOINT b" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (6, 'f')" }));
    try expectCount(&exec, allocator, "5");
    freeResp(allocator, try exec.execute(.{ .sql = "ROLLBACK TO a" }));
    try expectCount(&exec, allocator, "3");
    freeResp(allocator, try exec.execute(.{ .sql = "COMMIT" }));
    try expectCount(&exec, allocator, "3");

    freeResp(allocator, try exec.execute(.{ .sql = "BEGIN" }));
    freeResp(allocator, try exec.execute(.{ .sql = "SAVEPOINT s" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (7, 'g')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "RELEASE SAVEPOINT s" }));
    freeResp(allocator, try exec.execute(.{ .sql = "COMMIT" }));
    try expectCount(&exec, allocator, "4");
}

test "SQLEXT: multi-row INSERT is atomic" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_multirow.db";
    const wal_dir = "test_multirow_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, name TEXT)" }));
    {
        const r = try exec.execute(.{ .sql = "INSERT INTO t (id, name) VALUES (1, 'a'), (2, 'b'), (3, 'c')" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(u64, 3), r.rows_affected);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("3", r.rows[0][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "INSERT INTO t (id, name) VALUES (4, 'd'), (1, 'dup')" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message != null);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("3", r.rows[0][0]);
    }
}

test "SQLEXT: uncorrelated scalar and IN subqueries" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_subq.db";
    const wal_dir = "test_subq_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, dept TEXT, sal INT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE d (name TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO t (id, dept, sal) VALUES (1, 'eng', 100), (2, 'eng', 200), (3, 'sales', 150)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO d (name) VALUES ('eng')" }));

    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM t WHERE dept IN (SELECT name FROM d) ORDER BY id ASC" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 2), r.rows.len);
        try std.testing.expectEqualStrings("1", r.rows[0][0]);
        try std.testing.expectEqualStrings("2", r.rows[1][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM t WHERE sal > (SELECT sal FROM t WHERE id = 1) ORDER BY id ASC" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 2), r.rows.len);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
        try std.testing.expectEqualStrings("3", r.rows[1][0]);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT id FROM t WHERE dept NOT IN (SELECT name FROM d) ORDER BY id ASC" });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 1), r.rows.len);
        try std.testing.expectEqualStrings("3", r.rows[0][0]);
    }
}

test "SQLEXT: UNION dedups, UNION ALL keeps duplicates" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_union.db";
    const wal_dir = "test_union_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE a (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE b (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO a (id, v) VALUES (1, 'x'), (2, 'y')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO b (id, v) VALUES (2, 'y'), (3, 'z')" }));

    {
        const r = try exec.execute(.{ .sql = "SELECT v FROM a UNION SELECT v FROM b" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 3), r.rows.len);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT v FROM a UNION ALL SELECT v FROM b" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 4), r.rows.len);
    }
}

test "BINDPARAMS: the simple-query executor accepts server-side bound parameters (injection-safe)" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_bindparams.db";
    const wal_dir = "test_bindparams_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    const ParamClass = @import("proto/command.zig").ParamClass;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    var exec = QueryExecutor.init(allocator, db);
    defer exec.deinit();

    freeResp(allocator, try exec.execute(.{ .sql = "CREATE TABLE u (id INT PRIMARY KEY, name TEXT)" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO u (id, name) VALUES (1, 'alice')" }));
    freeResp(allocator, try exec.execute(.{ .sql = "INSERT INTO u (id, name) VALUES (2, 'bob')" }));

    {
        const params = [_]?[]const u8{"2"};
        const classes = [_]ParamClass{.numeric};
        const r = try exec.execute(.{ .sql = "SELECT name FROM u WHERE id = $1", .params = &params, .param_classes = &classes });
        defer freeResp(allocator, r);
        try std.testing.expectEqual(@as(usize, 1), r.rows.len);
        try std.testing.expectEqualStrings("bob", r.rows[0][0]);
    }

    {
        const params = [_]?[]const u8{"1'; DROP TABLE u; --"};
        const classes = [_]ParamClass{.text};
        const r = try exec.execute(.{ .sql = "SELECT name FROM u WHERE name = $1", .params = &params, .param_classes = &classes });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
        try std.testing.expectEqual(@as(usize, 0), r.rows.len);
    }
    {
        const r = try exec.execute(.{ .sql = "SELECT COUNT(*) FROM u" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
    }

    {
        const params = [_]?[]const u8{"1 OR 1=1"};
        const classes = [_]ParamClass{.numeric};
        const r = try exec.execute(.{ .sql = "SELECT name FROM u WHERE id = $1", .params = &params, .param_classes = &classes });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message != null);
    }
}

test "SSI: SERIALIZABLE prevents write skew that REPEATABLE READ allows" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;

    const H = struct {
        fn writeSkewFailures(a: std.mem.Allocator, i: std.Io, level: []const u8, path: []const u8, wdir: []const u8) !u32 {
            Io.Dir.deleteTree(.cwd(), i, wdir) catch {};
            Io.Dir.deleteFile(.cwd(), i, path) catch {};
            var db = try Database.open(a, i, path, 64, wdir);
            defer db.close();
            var setup = QueryExecutor.init(a, db);
            defer setup.deinit();
            freeResp(a, try setup.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v INT)" }));
            freeResp(a, try setup.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 100), (2, 100)" }));

            var A = QueryExecutor.init(a, db);
            defer A.deinit();
            var B = QueryExecutor.init(a, db);
            defer B.deinit();

            const setlvl = try std.fmt.allocPrint(a, "SET TRANSACTION ISOLATION LEVEL {s}", .{level});
            defer a.free(setlvl);
            freeResp(a, try A.execute(.{ .sql = setlvl }));
            freeResp(a, try B.execute(.{ .sql = setlvl }));
            freeResp(a, try A.execute(.{ .sql = "BEGIN" }));
            freeResp(a, try B.execute(.{ .sql = "BEGIN" }));
            freeResp(a, try A.execute(.{ .sql = "SELECT v FROM t" }));
            freeResp(a, try B.execute(.{ .sql = "SELECT v FROM t" }));
            freeResp(a, try A.execute(.{ .sql = "UPDATE t SET v = 0 WHERE id = 1" }));
            freeResp(a, try B.execute(.{ .sql = "UPDATE t SET v = 0 WHERE id = 2" }));

            var failures: u32 = 0;
            const ra = try A.execute(.{ .sql = "COMMIT" });
            defer freeResp(a, ra);
            if (ra.error_message != null) failures += 1;
            const rb = try B.execute(.{ .sql = "COMMIT" });
            defer freeResp(a, rb);
            if (rb.error_message != null) failures += 1;
            return failures;
        }
    };

    {
        const f = try H.writeSkewFailures(allocator, io, "REPEATABLE READ", "test_ssi_rr.db", "test_ssi_rr_wal");
        defer {
            Io.Dir.deleteFile(.cwd(), io, "test_ssi_rr.db") catch {};
            Io.Dir.deleteTree(.cwd(), io, "test_ssi_rr_wal") catch {};
        }
        try std.testing.expectEqual(@as(u32, 0), f);
    }
    {
        const f = try H.writeSkewFailures(allocator, io, "SERIALIZABLE", "test_ssi_ser.db", "test_ssi_ser_wal");
        defer {
            Io.Dir.deleteFile(.cwd(), io, "test_ssi_ser.db") catch {};
            Io.Dir.deleteTree(.cwd(), io, "test_ssi_ser_wal") catch {};
        }
        try std.testing.expect(f >= 1);
    }
}

test "ISOLATION: REPEATABLE READ freezes a snapshot; READ COMMITTED sees concurrent commits" {
    const allocator = std.testing.allocator;
    var threaded = std.Io.Threaded.init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const db_path = "test_isolation.db";
    const wal_dir = "test_isolation_wal";
    Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteFile(.cwd(), io, db_path) catch {};
    defer Io.Dir.deleteTree(.cwd(), io, wal_dir) catch {};
    const Database = @import("schema.zig").Database;
    const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
    var db = try Database.open(allocator, io, db_path, 64, wal_dir);
    defer db.close();
    db.synchronous_commit = true;

    var execA = QueryExecutor.init(allocator, db);
    defer execA.deinit();
    var execB = QueryExecutor.init(allocator, db);
    defer execB.deinit();

    freeResp(allocator, try execA.execute(.{ .sql = "CREATE TABLE t (id INT PRIMARY KEY, v TEXT)" }));
    freeResp(allocator, try execA.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (1, 'a')" }));

    freeResp(allocator, try execB.execute(.{ .sql = "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ" }));
    freeResp(allocator, try execB.execute(.{ .sql = "BEGIN" }));
    {
        const r = try execB.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("1", r.rows[0][0]);
    }

    freeResp(allocator, try execA.execute(.{ .sql = "INSERT INTO t (id, v) VALUES (2, 'b')" }));

    {
        const r = try execB.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("1", r.rows[0][0]);
    }

    {
        const r = try execA.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
    }

    freeResp(allocator, try execB.execute(.{ .sql = "COMMIT" }));
    freeResp(allocator, try execB.execute(.{ .sql = "BEGIN" }));
    {
        const r = try execB.execute(.{ .sql = "SELECT COUNT(*) FROM t" });
        defer freeResp(allocator, r);
        try std.testing.expectEqualStrings("2", r.rows[0][0]);
    }
    freeResp(allocator, try execB.execute(.{ .sql = "COMMIT" }));

    {
        const r = try execA.execute(.{ .sql = "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE" });
        defer freeResp(allocator, r);
        try std.testing.expect(r.error_message == null);
    }
}
