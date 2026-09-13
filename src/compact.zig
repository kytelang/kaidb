//! Offline logical compaction: rebuilds a database into a fresh file with every
//! B+tree packed.
//!
//! The bloat a long random-order (or monotonic-but-lexical-keyed) load leaves
//! behind is intra-page free space: a 50/50 leaf split under non-append inserts
//! settles every leaf to ~66% fill. This tool reclaims it WITHOUT touching the
//! live read/write path or the on-disk key format: for each user table (and each
//! index) it walks the source tree in KEY ORDER and re-inserts every cell into a
//! fresh tree in that same order, so each insert is the new maximum and the
//! append-split (`btree.splitAndInsert`) keeps the left page full - leaves pack
//! to ~100%. Because it writes into a brand-new file, the on-disk size actually
//! shrinks (orphaned pages are simply never created, unlike an in-place rebuild).
//!
//! Values are copied verbatim (the packed MVCC version blob for table rows, the
//! empty payload for index entries), so no row semantics change; `insert`
//! re-spills any overflow value on its own. Scope: user tables + their indexes.
//! Foreign keys and sequences are not yet copied into the fresh file. Rather
//! than dropping that catalog metadata silently, `compact` refuses with
//! `error.CompactionWouldDropCatalog` when the source actually defines any (the
//! orchestrator store and the benchmarks use neither, so the intended path is
//! unaffected); carrying them over is the straightforward extension that lifts
//! the guard.

const std = @import("std");
const Database = @import("schema.zig").Database;
const types = @import("schema/types.zig");
const row = @import("schema/row.zig");

/// Packs a single, always-visible GENESIS row version (`count=1, xmin=0,
/// xmax=0`) from a live version's `fixed`/`heap` images, matching the on-disk
/// packed-versions layout. `xmin == 0` is unconditionally visible to every
/// reader (see `QueryExecutor.rowVisible`), so a compacted row needs no entry in
/// the new database's transaction-commit state - which is why copying the source
/// version blob verbatim (carrying the original load's `xmin`s, unknown to the
/// fresh db) would render nearly every row invisible. Building genesis versions
/// also drops any superseded/dead versions, so compaction reclaims MVCC bloat too.
fn buildGenesis(allocator: std.mem.Allocator, fixed: []const u8, heap: []const u8) ![]u8 {
    const total = 4 + 8 + 8 + 8 + 4 + 4 + fixed.len + heap.len;
    const buf = try allocator.alloc(u8, total);
    std.mem.writeInt(u32, buf[0..4], 1, .little); // version count
    std.mem.writeInt(u64, buf[4..12], 0, .little); // xmin = 0 (genesis)
    std.mem.writeInt(u64, buf[12..20], 0, .little); // xmax = 0 (live)
    std.mem.writeInt(u64, buf[20..28], 0, .little); // roll_ptr
    std.mem.writeInt(u32, buf[28..32], @intCast(fixed.len), .little);
    std.mem.writeInt(u32, buf[32..36], @intCast(heap.len), .little);
    @memcpy(buf[36 .. 36 + fixed.len], fixed);
    @memcpy(buf[36 + fixed.len ..], heap);
    return buf;
}

const log = std.log.scoped(.compact);

/// Pool size (pages) for the compaction Databases. Compaction is sequential, so
/// a modest cache suffices; 8192 * 16KB = 128 MB.
const COMPACT_POOL: u32 = 8192;

fn dupColumns(allocator: std.mem.Allocator, cols: []const types.Column) ![]types.Column {
    const out = try allocator.alloc(types.Column, cols.len);
    for (cols, 0..) |c, i| {
        out[i] = .{
            .name = try allocator.dupe(u8, c.name),
            .type = c.type,
            .size = c.size,
            .offset = c.offset,
            .is_primary_key = c.is_primary_key,
            .is_auto_increment = c.is_auto_increment,
            .is_nullable = c.is_nullable,
            .default_value = if (c.default_value) |d| try allocator.dupe(u8, d) else null,
        };
    }
    return out;
}

/// Rebuilds the database at `<src_dir>/nova.db` into a fresh, fully packed
/// `<dst_dir>/nova.db`. Both are opened without a WAL (compaction is a one-shot
/// rebuild made durable by the final flush+sync); the destination directory is
/// created if absent.
pub fn compact(allocator: std.mem.Allocator, io: std.Io, src_dir: []const u8, dst_dir: []const u8) !void {
    const src_path = try std.fmt.allocPrint(allocator, "{s}/nova.db", .{src_dir});
    defer allocator.free(src_path);
    const dst_path = try std.fmt.allocPrint(allocator, "{s}/nova.db", .{dst_dir});
    defer allocator.free(dst_path);

    try std.Io.Dir.createDirPath(.cwd(), io, dst_dir);

    var src = try Database.open(allocator, io, src_path, COMPACT_POOL, null);
    defer src.close();

    // Compaction copies user tables and their indexes only. Foreign keys and
    // sequences are not carried into the fresh file yet, so rather than silently
    // dropping that catalog metadata (a maintenance tool must never lose data),
    // refuse loudly when the source database actually uses them. The orchestrator
    // store and the benchmarks use neither, so this never blocks the intended use;
    // carrying them over is the straightforward extension that lifts the guard.
    if (src.catalog.foreign_keys.items.len != 0 or src.catalog.sequences.items.len != 0) {
        log.err(
            "cannot compact: source has {d} foreign key(s) and {d} sequence(s), which compaction does not yet copy; refusing rather than dropping them",
            .{ src.catalog.foreign_keys.items.len, src.catalog.sequences.items.len },
        );
        return error.CompactionWouldDropCatalog;
    }

    var dst = try Database.open(allocator, io, dst_path, COMPACT_POOL, null);
    defer dst.close();

    const tx: u64 = 1;

    // Snapshot the source's user-table names up front: creating tables on `dst`
    // mutates `dst.catalog`, and copying rows reads `src.catalog`, so keep the
    // two apart by collecting the names we will process first.
    var table_names = std.ArrayList([]const u8).empty;
    defer {
        for (table_names.items) |n| allocator.free(n);
        table_names.deinit(allocator);
    }
    for (src.catalog.tables.items) |tbl| {
        if (std.mem.startsWith(u8, tbl.name, "sys.")) continue;
        try table_names.append(allocator, try allocator.dupe(u8, tbl.name));
    }

    var total_rows: u64 = 0;
    for (table_names.items) |name| {
        const tbl = src.catalog.getTable(name) orelse continue;
        const cols = try dupColumns(allocator, tbl.columns); // createTable takes ownership
        _ = try dst.createTable(name, cols, tx);

        const src_tree = try src.getTableTree(name);
        defer src_tree.deinit();
        const dst_tree = try dst.getTableTree(name);
        defer dst_tree.deinit();

        // Collect keys in key order first (short), release the scan latch, then
        // search+insert each: this avoids holding a leaf latch while `search`
        // re-descends (needed to reassemble any overflow value), and inserting in
        // the collected ascending order triggers the append-split so leaves pack.
        var keys = std.ArrayList([]const u8).empty;
        defer {
            for (keys.items) |k| allocator.free(k);
            keys.deinit(allocator);
        }
        {
            var it = try src_tree.iterator();
            defer it.deinit();
            while (try it.next()) |cell| try keys.append(allocator, try allocator.dupe(u8, cell.key));
        }
        var copied: u64 = 0;
        for (keys.items) |k| {
            const raw = (try src_tree.search(k, allocator)) orelse continue;
            defer allocator.free(raw);
            // Take the live version (xmax == 0) - compaction runs offline on a
            // quiesced db, so any set xmax is a committed delete. A row with no
            // live version is fully deleted and is dropped (GC).
            const versions = try src.reconstructVersionChain(raw, allocator);
            defer {
                for (versions) |*v| v.deinit(allocator);
                allocator.free(versions);
            }
            var live: ?row.DecodedVersion = null;
            for (versions) |v| {
                if (v.xmax == 0) {
                    live = v;
                    break;
                }
            }
            const lv = live orelse continue;
            const gen = try buildGenesis(allocator, lv.fixed, lv.heap);
            defer allocator.free(gen);
            try dst_tree.insert(k, gen);
            copied += 1;
            total_rows += 1;
        }
        try dst.updateTableRootPageId(name, dst_tree.root_page_id, tx);
        log.info("compacted table {s}: {d} rows", .{ name, copied });
    }

    // Indexes, after all tables exist. Copy entries verbatim in key order (same
    // append-pack), and preserve the source's exactness bit so the index-only
    // COUNT gate stays sound across compaction.
    for (src.catalog.indexes.items) |idx| {
        const tname = for (src.catalog.tables.items) |t| {
            if (t.id == idx.table_id) break t.name;
        } else continue;
        if (std.mem.startsWith(u8, tname, "sys.")) continue;

        const kcols = try dupColumns(allocator, idx.key_columns);
        const vcols: ?[]types.Column = if (idx.value_columns) |vc| try dupColumns(allocator, vc) else null;
        const new_idx = try dst.createIndex(idx.name, tname, idx.kind, kcols, vcols, tx);
        _ = new_idx;

        const src_idx = try src.getIndexTree(idx.name);
        defer src_idx.deinit();
        const dst_idx = try dst.getIndexTree(idx.name);
        defer dst_idx.deinit();

        var keys = std.ArrayList([]const u8).empty;
        defer {
            for (keys.items) |k| allocator.free(k);
            keys.deinit(allocator);
        }
        var vals = std.ArrayList([]const u8).empty;
        defer {
            for (vals.items) |v| allocator.free(v);
            vals.deinit(allocator);
        }
        {
            var it = try src_idx.iterator();
            defer it.deinit();
            while (try it.next()) |cell| {
                try keys.append(allocator, try allocator.dupe(u8, cell.key));
                try vals.append(allocator, try allocator.dupe(u8, cell.value));
            }
        }
        for (keys.items, vals.items) |k, v| try dst_idx.insert(k, v);
        try dst.updateIndexRootPageId(idx.name, dst_idx.root_page_id, tx);
        // A freshly created index is marked exact; if the source had been made
        // inexact by a prior delete/update, carry that forward (compaction copies
        // entries verbatim, it does not garbage-collect stale ones).
        if (!idx.exact) try dst.markTableIndexesInexact(idx.table_id);
        log.info("compacted index {s}: {d} entries", .{ idx.name, keys.items.len });
    }

    try dst.pool.flushAllPages();
    try dst.pool.pager.file.sync(io);
    log.info("compaction done: {d} total rows -> {s}", .{ total_rows, dst_path });
}
