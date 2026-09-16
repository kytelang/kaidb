//! Table statistics collection, the `ANALYZE` half of the cost-based optimiser.
//!
//! The query planner needs to know how big a table is before it can choose a
//! join order or decide between a full scan and an index lookup. This module
//! computes those figures by walking the on-disk B+Tree and persists them into
//! the `sys.table_stats` catalog table, from where the executor and the CBO read
//! them back later. It is the producer side of the statistics; the planner (see
//! [`QueryExecutor`]) is the consumer.
//!
//! Two numbers are gathered per table, and they are gathered DIFFERENTLY on
//! purpose:
//!
//!   * `page_count` is a physical fact about storage, so it is counted by
//!     following the leaf-level `next_page_id` sibling chain of the tree from the
//!     first leaf to the terminating `0`. It counts leaf pages only (the level
//!     that actually holds the cells), which is the quantity the optimiser cares
//!     about when estimating scan I/O.
//!
//!   * `row_count` is a LOGICAL fact and must respect MVCC. A leaf cell holds a
//!     version chain, not a single row, so a raw cell count would over-report by
//!     including dead and not-yet-visible versions. Each cell is therefore
//!     reconstructed into its version list and counted only if some version is
//!     visible to the analysing transaction's snapshot (`xmin <= tx` and the row
//!     is either still live, `xmax == 0`, or was deleted by a later transaction,
//!     `xmax > tx`). This is the same visibility rule the executor applies to a
//!     normal read, so the count matches what a `SELECT COUNT(*)` under that
//!     snapshot would see.
//!
//! The result is written back through the executor's normal MVCC write path
//! ([`QueryExecutor.writeNewVersion`]) rather than by poking the tree directly,
//! so a re-analyse of a table simply supersedes the previous stats row with a new
//! version and the catalog stays transactional.

const std = @import("std");
/// The database handle: owns the buffer pool, the table catalog, and the
/// per-table B+Trees this pass walks. Used here to fetch table trees, read raw
/// pages while chasing the leaf sibling chain, and to reconstruct MVCC version
/// chains.
const Database = @import("../schema.zig").Database;
/// The slotted-page B+Tree type. Imported for the leaf-chain and iterator APIs
/// used to size a table; not referenced by name elsewhere in this file but kept
/// as the storage-layer contract this module depends on.
const BPlusTree = @import("../storage/btree.zig").BPlusTree;
/// The schema/catalog module. Pulled in for its namespace alongside the specific
/// [`Database`] alias above.
const schema = @import("../schema.zig");
/// The SQL executor. Supplies the transaction snapshot (`current_tx_id`), the
/// scratch allocator, and the MVCC-aware [`QueryExecutor.writeNewVersion`] used
/// to persist the computed stats row.
const QueryExecutor = @import("query_executor.zig").QueryExecutor;
const query_iter = @import("iterator.zig");

/// The two size figures the cost-based optimiser needs about one table.
///
/// Returned by [`analyzeTable`] and mirrored into the `sys.table_stats` catalog.
/// `row_count` is MVCC-visible (dead/invisible versions excluded); `page_count`
/// is the number of physical leaf pages backing the table.
pub const TableStats = struct {
    /// Number of live rows visible to the analysing transaction's snapshot.
    /// Counts version chains, not raw cells, so a row with multiple historical
    /// versions still counts once and a row deleted before the snapshot counts
    /// zero.
    row_count: u64,
    /// Number of leaf pages in the table's B+Tree, found by walking the
    /// `next_page_id` sibling chain. A rough proxy for scan cost; internal
    /// (non-leaf) pages are not counted.
    page_count: u64,
};

/// Collects fresh statistics for `table_name` and records them in the catalog.
///
/// Performs two independent passes over the table's B+Tree: a physical pass that
/// counts leaf pages by following the sibling chain, and a logical pass that
/// iterates every cell, reconstructs its MVCC version chain, and counts a row
/// only when a version is visible to the current transaction. The visibility
/// test mirrors the executor's read-path rule so the count agrees with what a
/// snapshot read would return.
///
/// The resulting [`TableStats`] is both returned to the caller and persisted:
/// the function builds a JSON row (`table_name`, `row_count`, `page_count` as
/// decimal strings), checks whether a stats row for this table already exists,
/// and writes a new MVCC version into `sys.table_stats` via
/// [`QueryExecutor.writeNewVersion`] (updating in place when it exists, inserting
/// otherwise). Every intermediate allocation is freed on both the success and
/// error paths, so a mid-way failure leaks nothing.
///
/// If the executor has no active transaction, `1` is used as the snapshot id,
/// which treats every committed version as visible. Returns any error raised
/// while fetching the tree, reading pages, reconstructing versions, or writing
/// the stats row.
pub fn analyzeTable(db: *Database, executor: *QueryExecutor, table_name: []const u8) !TableStats {
    const table_tree = try db.getTableTree(table_name);
    defer table_tree.deinit();

    var page_count: u64 = 0;
    const first_leaf = try table_tree.findFirstLeaf();
    var curr_pid = first_leaf.page_id.?;
    db.pool.unpinPage(curr_pid, false);

    while (curr_pid != 0) {
        page_count += 1;
        const f = try db.pool.fetchPage(curr_pid);
        const p = db.pool.pageOf(f);
        const next_pid = p.headerPtr().next_page_id;
        db.pool.unpinPage(curr_pid, false);
        curr_pid = next_pid;
    }

    var row_count: u64 = 0;
    var it = try table_tree.iterator();
    defer it.deinit();

    const current_tx = executor.current_tx_id orelse 1;

    while (try it.next()) |cell| {
        const versions = try db.reconstructVersionChain(cell.value, executor.allocator);
        defer {
            for (versions) |*v| v.deinit(executor.allocator);
            executor.allocator.free(versions);
        }

        var is_visible = false;
        for (versions) |v| {
            if (v.xmin <= current_tx) {
                if (v.xmax == 0 or v.xmax > current_tx) {
                    is_visible = true;
                    break;
                }
            }
        }
        if (is_visible) {
            row_count += 1;
        }
    }

    const stats_tree = try db.getTableTree("sys.table_stats");
    defer stats_tree.deinit();

    var row_obj: std.StringArrayHashMapUnmanaged(query_iter.Cell) = .empty;
    defer row_obj.deinit(executor.allocator);
    defer {
        if (row_obj.get("row_count")) |v| executor.allocator.free(v.text);
        if (row_obj.get("page_count")) |v| executor.allocator.free(v.text);
        if (row_obj.get("table_name")) |v| executor.allocator.free(v.text);
    }

    const row_count_str = try std.fmt.allocPrint(executor.allocator, "{d}", .{row_count});
    errdefer executor.allocator.free(row_count_str);
    try row_obj.put(executor.allocator, "row_count", .{ .text = row_count_str });

    const page_count_str = try std.fmt.allocPrint(executor.allocator, "{d}", .{page_count});
    errdefer executor.allocator.free(page_count_str);
    try row_obj.put(executor.allocator, "page_count", .{ .text = page_count_str });

    try row_obj.put(executor.allocator, "table_name", .{ .text = try executor.allocator.dupe(u8, table_name) });

    const exists = if (try stats_tree.search(table_name, executor.allocator)) |v| s: {
        executor.allocator.free(v);
        break :s true;
    } else false;

    try executor.writeNewVersion(stats_tree, "sys.table_stats", table_name, row_obj, exists);

    return TableStats{
        .row_count = row_count,
        .page_count = page_count,
    };
}
