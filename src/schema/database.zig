//! The top-level `Database` object: the catalog owner and lifecycle root that
//! ties every kaidb subsystem together behind a single on-disk file.
//!
//! Everything else in the engine is a building block; this file is where they
//! are wired into a working database. A [`Database`] owns the buffer pool /
//! pager (the page cache over the file), the system catalog, the write-ahead
//! log, the undo log, the transaction manager, the security manager, and the
//! optional replication machinery, and it exposes the DDL surface (create /
//! drop table and index, foreign keys, users) plus the MVCC row-update and
//! recovery paths that the SQL executor and the replication layer call into.
//!
//! ## On-disk layout of a fresh file
//!
//! When a brand-new file is opened ([`Database.openInner`] with
//! `num_pages == 0`) the first pages are laid out in a fixed order:
//!
//!   * page 0 , the file [`Header`] (magic, format version, page size, the
//!     master-tree root page id, the last LSN, and the free-list head). It is
//!     rewritten on clean [`Database.close`] and by [`Database.durableFlush`].
//!   * page 1 , the root of the *master tree* (`sys.objects`), the B+Tree that
//!     maps every object name to its [`types.ObjectMetadata`] (its root page).
//!   * pages 2..18, the doublewrite buffer: page 2 holds the
//!     `DoublewriteHeader`, pages 3..18 are its 16 torn-write recovery slots.
//!     [`Database.recoverDoublewriteBuffer`] restores from these on open.
//!
//! ## The catalog
//!
//! The master tree (`sys.objects`) is the root of a two-level catalog. Beyond
//! it live the system tables `sys.tables`, `sys.indexes`, `sys.constraints`,
//! `sys.users`, `sys.view`, `sys.table_stats`, `sys.roles` and
//! `sys.privileges`, each a B+Tree registered in `sys.objects`.
//! [`Database.ensureSystemCatalogTables`] materialises them on first open and
//! [`Database.loadCatalog`] rebuilds the in-memory [`catalog.SystemCatalog`]
//! from them on subsequent opens. Every user table and index root is cached in
//! `table_roots` / `index_roots` (name → root page id) and, for hot access, in
//! `table_trees` / `index_trees` (name → live [`BPlusTree`]).
//!
//! ## MVCC
//!
//! A row's stored value is not a single tuple but a *version chain*: the latest
//! version lives in the table's B+Tree cell (packed by `row.packVersions`) and
//! older versions are threaded through the undo log via each version's
//! `roll_ptr`. Each [`row.DecodedVersion`] carries `xmin`/`xmax` (the creating
//! and deleting transaction ids). [`Database.reconstructVersionChain`] walks
//! that chain, and [`Database.updateRowMVCC`] appends the current version to the
//! undo log before overwriting the cell, so a concurrent reader can still see
//! the value it was entitled to. [`Database.vacuumTable`] prunes versions whose
//! deleting transaction committed before the oldest still-active transaction.
//!
//! ## Durability and recovery
//!
//! Mutations are logged to the WAL (LSNs handed out by [`Database.reserveLsn`])
//! before the dirty pages reach the file. [`Database.recoverTo`] replays the WAL
//! in three passes: phase 1 redoes committed catalog (`sys.*`) records, phase 2
//! redoes committed user-table DML and rebuilds secondary indexes, and phase 3
//! walks the log *backwards* to undo the effects of transactions that were still
//! active at the crash. The doublewrite buffer guards against torn page writes
//! that a redo log alone cannot repair.
//!
//! ## Concurrency and replication
//!
//! `rw_lock` is the coarse gate held across vacuum and checkpoint; finer table
//! access uses a per-table [`GroupLock`] handed out by [`Database.tableLock`].
//! A background writer coroutine ([`Database.runBgWriterTask`]) periodically
//! flushes pages, purges stale undo pages, and vacuums. For high availability a
//! database can become a follower ([`Database.becomeFollower`]) that applies a
//! leader's shipped WAL through [`Database.applyStream`] / [`Database.post`], or
//! a durable leader ([`Database.becomeDurableLeader`]); a monotonically
//! increasing fencing epoch ([`Database.guardWrite`], [`Database.setWriteEpoch`])
//! keeps a demoted old leader from writing after a failover.

const std = @import("std");
/// Scoped logger (`.db`) for every diagnostic this module emits.
const dblog = std.log.scoped(.db);
/// Shorthand for the allocator interface threaded through the whole engine.
const Allocator = std.mem.Allocator;
/// The SQL abstract syntax tree, used here only for the cached-statement type.
const ast = @import("../sql/ast.zig");
/// The SQL parser; a [`CachedQuery`] owns one so its parsed AST stays valid.
const Parser = @import("../sql/parser.zig").Parser;

/// A parsed-and-retained prepared statement kept in the query plan cache.
///
/// The [`Parser`] is stored by pointer and outlives the call that produced it
/// because [`ast.Statement`] borrows slices out of the parser's arena: freeing
/// the parser would dangle the statement. Entries are torn down in
/// [`Database.deinitQueryCache`], which frees the key, deinits the parser, then
/// destroys it.
pub const CachedQuery = struct {
    /// The owning parser whose arena backs [`CachedQuery.stmt`]; freed last.
    parser: *Parser,
    /// The parsed statement, valid only while [`CachedQuery.parser`] lives.
    stmt: ast.Statement,
};
/// Networking namespace alias (`std.Io.net`); retained for socket-typed callers.
const net = std.Io.net;

/// The slotted-page module: page format, the file header, and its codecs.
const page_mod = @import("../storage/page.zig");
/// A page number, i.e. an index into the file measured in [`PAGE_SIZE`] units.
const PageId = page_mod.PageId;
/// The fixed page size in bytes; the file header records it so opens can reject
/// a file written with a different size ([`error.PageSizeMismatch`]).
const PAGE_SIZE = page_mod.PAGE_SIZE;
/// The page-0 file header struct (magic, version, root, LSN, free-list head).
const Header = page_mod.Header;
/// Decodes a [`Header`] out of the raw bytes of page 0.
const readHeader = page_mod.readHeader;
/// Encodes a [`Header`] back into the raw bytes of page 0.
const writeHeader = page_mod.writeHeader;
/// The file-format magic number; a mismatch means "not a kaidb file".
const MAGIC = page_mod.MAGIC;
/// The on-disk format version; a mismatch aborts the open as unsupported.
const VERSION = page_mod.VERSION;

/// The buffer pool / pager: the page cache and torn-write protection over file.
const PagePool = @import("../storage/pool.zig").PagePool;
/// The slotted-page B+Tree; every table, index and catalog table is one.
const BPlusTree = @import("../storage/btree.zig").BPlusTree;
const overflow = @import("../storage/overflow.zig");
/// Reader/writer lock; the database-wide gate for vacuum and checkpoint.
const RwLock = @import("utils").sync.RwLock;
/// The per-table access lock (readers / writers / exclusive) from `utils.sync`.
const GroupLock = @import("utils").sync.GroupLock;
/// Tracks active and committed transaction ids and hands out new ones.
const TransactionManager = @import("../concurrency/transaction.zig").TransactionManager;
const WasmRegistry = @import("../wasm/registry.zig").Registry;
/// The write-ahead log; durability and the source stream for replication.
const WriteAheadLog = @import("../durability/write_ahead_log.zig").WriteAheadLog;

/// Pool `wal_gate` callback: force the WAL to disk before evicting a dirty page.
///
/// The pool cannot depend on the WAL type directly (it lives a layer below), so
/// the gate is a type-erased `*anyopaque` + function pair. This trampoline casts
/// the context back to a [`WriteAheadLog`] and flushes it, enforcing the
/// write-ahead rule: no page may reach the file ahead of its log record.
fn walGateFlush(ctx: *anyopaque) anyerror!void {
    const w: *WriteAheadLog = @ptrCast(@alignCast(ctx));
    try w.flush();
}
/// Pool `wal_gate` callback: report the highest LSN already durable on disk.
///
/// The pool uses this to decide whether a given page (stamped with the LSN of
/// the record that last dirtied it) is already covered by a flushed log record
/// and may therefore be written out without a further WAL flush.
fn walGateDurableLsn(ctx: *anyopaque) u64 {
    const w: *WriteAheadLog = @ptrCast(@alignCast(ctx));
    return w.flushed_lsn.load(.monotonic);
}
/// Authentication and authorisation: users, roles, privileges, key hashing.
const SecurityManager = @import("../concurrency/security.zig").SecurityManager;
/// Encodes a byte slice as lowercase hex (used for password hash + salt storage).
const hexEncode = @import("../concurrency/security.zig").hexEncode;
/// The WAL record operation kind (begin / commit / insert / update / delete ...).
const OpKind = @import("../common/common.zig").OpKind;
/// A single WAL log record as replayed during recovery and shipped in streams.
const LogRecord = @import("../common/common.zig").LogRecord;

/// Separator between the collection and the field path in a `COLLECTION_INDEX`
/// catalog object's name.
const coll_index_sep: u8 = 0;
/// Separator between the individual field paths inside a compound index's
/// catalog name (after the `coll_index_sep`). A comma cannot appear in a BSON
/// field name here, so it round-trips the ordered path list unambiguously.
const compound_path_sep: u8 = ',';


/// Catalog value types: columns, table/index/object metadata, foreign keys.
const types = @import("types.zig");

/// Returns the declared column type of `name` in `table_meta` (a catalog
/// `Table`), or `.TEXT` if absent. Index `key_columns` do not reliably carry
/// the column type, so index-key encoding derives it from the table here, to
/// match the encoding used on the query side (see `types.encodeIndexValueAlloc`).
fn colTypeByNameIn(table_meta: anytype, name: []const u8) types.ColumnType {
    for (table_meta.columns) |c| {
        if (std.mem.eql(u8, c.name, name)) return c.type;
    }
    return .TEXT;
}
/// The in-memory `Table` and `Index` catalog objects.
const table_mod = @import("table.zig");
/// Row (de)serialisation: version packing, [`row.RowBuilder`], [`row.RowReader`].
const row = @import("row.zig");
/// The in-memory [`catalog.SystemCatalog`] holding tables, indexes and FKs.
const catalog = @import("catalog.zig");

/// A single open kaidb database: the catalog owner and lifecycle root.
///
/// Heap-allocated and returned by [`Database.open`] / [`Database.openAt`]; it
/// owns its pool, trees, catalog, WAL and undo log and must be torn down with
/// [`Database.close`] (clean, flushes and checkpoints) or
/// [`Database.crashSimulate`] (drops everything without flushing, for tests).
/// See the module header for the on-disk layout, MVCC and recovery model.
pub const Database = struct {
    /// Allocator backing every heap allocation this database makes.
    allocator: Allocator,
    /// The buffer pool / pager: the page cache and torn-write protection.
    pool: *PagePool,
    /// Which data model this instance serves clients (see `common/config.zig`
    /// `ServerMode`). The wire session and HTTP `/query` handler reject the other
    /// surface's requests. Set once from config at startup; defaults to
    /// relational so an un-set instance is SQL-only.
    mode: @import("../common/config.zig").ServerMode = .relational,
    /// Monotonic count of operations that create a reclaimable (dead) row
    /// version. Every MVCC mutation funnels through [`Database.updateRowMVCC`]
    /// (SQL UPDATE/DELETE, user unregister, document delete/update all call it),
    /// which bumps this. INSERTs do not touch it. The background vacuum records
    /// the value it last vacuumed at ([`last_vacuum_garbage_ops`]) and skips the
    /// whole pass when it is unchanged, so a pure-INSERT bulk load (or any
    /// churn-free interval) pays nothing for vacuum instead of full-scanning
    /// every table every ~10s. Atomic because it is bumped from client threads
    /// and read from the background-writer thread.
    garbage_ops: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Lock-free latency histogram of top-level query execution, observed by
    /// [`QueryExecutor.execute`] and exported at `/metrics`. Shared across all
    /// connection-scoped executors (they all point at this one `Database`).
    query_latency: @import("../common/histogram.zig").LatencyHistogram = .{},
    /// The [`garbage_ops`] value observed at the start of the last vacuum pass;
    /// see [`Database.vacuum`]. Only touched under the vacuum's `rw_lock`.
    last_vacuum_garbage_ops: u64 = 0,
    /// The `sys.objects` B+Tree mapping every object name to its metadata; the
    /// root of the catalog and the anchor recorded in the page-0 [`Header`].
    master_tree: *BPlusTree,
    /// In-memory catalog (tables, indexes, foreign keys) rebuilt on open by
    /// [`Database.loadCatalog`].
    catalog: catalog.SystemCatalog,
    /// Registry of wasm scalar UDFs registered with `CREATE FUNCTION ... LANGUAGE wasm`
    /// (embed-wasm.md M1). The query executor points the scalar-eval hook at this so
    /// `WHERE fn(col) = ...` resolves registered functions. In-memory; WAL-backed persistence
    /// of the source bytes is a later slice.
    wasm_functions: WasmRegistry,
    /// Object name → root page id for every table, including `sys.*` tables.
    /// Keys are owned (duped) copies freed on teardown.
    table_roots: std.StringHashMap(u64),
    /// Object name → root page id for every index. Keys are owned copies.
    index_roots: std.StringHashMap(u64),
    /// Object name → a live cached [`BPlusTree`] for each table, so hot access
    /// avoids re-opening a tree per statement; rebuilt by
    /// [`Database.populateTreeCaches`]. Trees carry `is_cached = true`.
    table_trees: std.StringHashMap(*BPlusTree),
    /// Object name → a live cached [`BPlusTree`] for each index.
    index_trees: std.StringHashMap(*BPlusTree),
    /// Object name → root page id for each document collection (the NoSQL model).
    /// Keys are owned copies. Populated from `sys.objects` rows of type "COLLECTION".
    collection_roots: std.StringHashMap(u64),
    /// Object name → a live cached [`BPlusTree`] for each document collection.
    collection_trees: std.StringHashMap(*BPlusTree),
    /// Serializes the document write path's read-modify-write (unique check ->
    /// read-old -> version write -> index maintenance) so two concurrent writers
    /// to a collection cannot lose an update, corrupt the version/undo chain, or
    /// both pass a unique check (the G3/G4 concurrency guarantee). Coarse but
    /// correct: document writes are serialized database-wide, which is acceptable
    /// under the single-reactor web model where scale is horizontal.
    doc_write_lock: std.Io.Mutex = .init,
    /// Database-wide reader/writer gate; held exclusively across vacuum and
    /// checkpoint so they cannot race concurrent DML on the same trees.
    rw_lock: RwLock = .{},
    /// Table name → its per-table [`GroupLock`], created on demand by
    /// [`Database.tableLock`]; the fine-grained access lock the executor takes.
    table_locks: std.StringHashMap(*GroupLock),
    /// Guards insertion into [`Database.table_locks`] (the map itself, not the
    /// locks it holds).
    table_locks_mutex: std.Io.Mutex = .init,
    /// Serialises catalog root-page-id rewrites (see
    /// [`Database.updateTableRootPageId`]).
    catalog_mutex: std.Io.Mutex = .init,
    /// Transaction id allocator and the committed/active visibility sets used by
    /// MVCC and recovery.
    txn_manager: TransactionManager,
    /// The undo log: prior row versions reachable from a version's `roll_ptr`.
    undo_log: @import("../concurrency/undo.zig").UndoLog,
    /// The write-ahead log, or `null` when the database was opened without a WAL
    /// directory (in-memory / no-durability mode).
    wal: ?*WriteAheadLog = null,
    /// Auth/authorisation manager; `undefined` until [`Database.openInner`]
    /// initialises it, after which it is always valid.
    security_manager: *SecurityManager = undefined,
    /// Structured-concurrency group owning background coroutines (the bg writer,
    /// the replication listener); cancelled and awaited on close.
    group: std.Io.Group,
    /// Set true to tell the background writer to stop; read under seq-cst atomics.
    is_closed: bool,
    /// True while [`Database.runBgWriterTask`] is running; close spins on this
    /// going false before freeing shared state.
    bg_task_running: bool,
    /// The directory containing the database file; owned copy used to resolve
    /// sidecar files (replication checkpoint, fence epoch). Empty means unset.
    base_dir: []const u8 = "",
    /// When this database is a live follower, the applier of the leader's stream.
    follower: ?*@import("../query/replication.zig").Follower = null,
    /// When a follower, the replication listener accepting the leader's feed.
    repl_server: ?*@import("../query/replication.zig").ReplServer = null,
    /// Owned copy of the follower listen host; empty when not a follower.
    repl_host: []const u8 = "",
    /// Owned copy of the follower's WAL staging directory; empty when not set.
    repl_wal_dir: []const u8 = "",
    /// When this database is a durable leader, the quorum WAL shipper.
    durable_repl: ?*@import("../query/replication.zig").DurableReplicator = null,
    /// Highest LSN a follower has applied; persisted via
    /// [`Database.saveReplCheckpoint`] so resync resumes at the right point.
    last_applied_lsn: u64 = 0,
    /// Cache of parsed prepared statements; `undefined` until initialised in
    /// [`Database.openInner`]. Torn down by [`Database.deinitQueryCache`].
    query_cache: std.StringHashMap(CachedQuery) = undefined,
    /// Guards concurrent access to [`Database.query_cache`].
    query_cache_mutex: std.Io.Mutex = .init,
    /// When true, commits wait for the WAL to reach disk before returning.
    synchronous_commit: bool = false,
    /// This node's write fencing epoch; a write is refused when it is behind
    /// [`Database.max_epoch_seen`] (see [`Database.guardWrite`]).
    fencing_epoch: u64 = 0,
    /// The highest fencing epoch ever observed, persisted across restarts so a
    /// stale ex-leader cannot resume writing after a newer leader took over.
    max_epoch_seen: u64 = 0,

    /// Opens (or creates) a database at `file_path`, recovering fully from the
    /// WAL if one is present.
    ///
    /// `pool_size` is the buffer pool capacity in pages; `wal_dir` enables
    /// durability when non-null. On a fresh file the fixed page layout described
    /// in the module header is written; on an existing file the [`Header`] is
    /// validated ([`error.InvalidMagic`], [`error.UnsupportedVersion`],
    /// [`error.PageSizeMismatch`]) and the catalog is loaded. The returned
    /// database owns a running background writer and must be released with
    /// [`Database.close`]. Delegates to [`Database.openInner`] with no LSN limit.
    pub fn open(allocator: Allocator, io: std.Io, file_path: []const u8, pool_size: u32, wal_dir: ?[]const u8) !*Database {
        return openInner(allocator, io, file_path, pool_size, wal_dir, 0, false);
    }

    /// Opens a database but stops WAL recovery at `target_lsn` (point-in-time
    /// recovery): records with a higher LSN are skipped during replay. Otherwise
    /// identical to [`Database.open`].
    pub fn openAt(allocator: Allocator, io: std.Io, file_path: []const u8, pool_size: u32, wal_dir: ?[]const u8, target_lsn: u64) !*Database {
        return openInner(allocator, io, file_path, pool_size, wal_dir, target_lsn, false);
    }

    /// Like [`Database.open`], but turns on Phase 5 mmap reads BEFORE any page is
    /// read. This matters: recovery and tree-cache population run inside open and
    /// read the working set; if mmap is enabled only afterwards (as an external
    /// call), those startup reads have already copied the pages into slab windows,
    /// so nothing is ever served borrowed and the read cache costs full RAM. Enabled
    /// up front, every startup and query read borrows the read-only map instead, so
    /// the slab stays near-zero resident on a read-mostly workload. POSIX only.
    pub fn openWithMmap(allocator: Allocator, io: std.Io, file_path: []const u8, pool_size: u32, wal_dir: ?[]const u8) !*Database {
        return openInner(allocator, io, file_path, pool_size, wal_dir, 0, true);
    }

    /// Shared implementation of [`Database.open`] / [`Database.openAt`].
    ///
    /// Order matters and is subtle: the pool is created first, then the
    /// doublewrite buffer is recovered (torn writes from the last crash must be
    /// repaired before any page is trusted), then the header is either written
    /// (fresh file) or validated (existing file), then the WAL is replayed up to
    /// `recover_target`, the system catalog tables are ensured, users/roles are
    /// loaded (bootstrapping a default `admin` account on an empty database), the
    /// tree caches are populated, the fence epoch is loaded, and finally the
    /// background writer coroutine is spawned. The single large `errdefer` unwinds
    /// every partially initialised field if any step fails.
    fn openInner(allocator: Allocator, io: std.Io, file_path: []const u8, pool_size: u32, wal_dir: ?[]const u8, recover_target: u64, mmap_reads: bool) !*Database {
        const pool = try PagePool.init(allocator, io, file_path, pool_size);
        errdefer pool.deinit() catch {};
        // Phase 5: enable mmap reads NOW, before recovery and tree-cache population
        // read any page, so those startup reads borrow the map instead of copying
        // into slab windows (see `openWithMmap`). num_pages is already known from
        // the pager's file-size probe in `PagePool.init`.
        if (mmap_reads) pool.enableMmapReads();

        const self = try allocator.create(Database);
        errdefer allocator.destroy(self);

        const dir_path = std.fs.path.dirname(file_path) orelse ".";
        self.* = .{
            .allocator = allocator,
            .pool = pool,
            .master_tree = undefined,
            .catalog = catalog.SystemCatalog.init(allocator),
            .wasm_functions = WasmRegistry.init(allocator),
            .table_roots = std.StringHashMap(u64).init(allocator),
            .index_roots = std.StringHashMap(u64).init(allocator),
            .table_trees = std.StringHashMap(*BPlusTree).init(allocator),
            .index_trees = std.StringHashMap(*BPlusTree).init(allocator),
            .collection_roots = std.StringHashMap(u64).init(allocator),
            .collection_trees = std.StringHashMap(*BPlusTree).init(allocator),
            .rw_lock = .{},
            .table_locks = std.StringHashMap(*GroupLock).init(allocator),
            .txn_manager = TransactionManager.init(allocator),
            .undo_log = @import("../concurrency/undo.zig").UndoLog.init(allocator, pool),
            .wal = null,
            .security_manager = undefined,
            .group = std.Io.Group.init,
            .is_closed = false,
            .bg_task_running = false,
            .base_dir = try allocator.dupe(u8, dir_path),
            .last_applied_lsn = 0,
            .query_cache = std.StringHashMap(CachedQuery).init(allocator),
            .synchronous_commit = false,
        };

        var master_tree_initialized = false;
        errdefer {
            if (master_tree_initialized) {
                self.master_tree.deinit();
            }
            if (self.base_dir.len > 0) allocator.free(self.base_dir);
            self.catalog.deinit();
            self.txn_manager.deinit();
            var table_it = self.table_roots.keyIterator();
            while (table_it.next()) |k| allocator.free(k.*);
            self.table_roots.deinit();
            var index_it = self.index_roots.keyIterator();
            while (index_it.next()) |k| allocator.free(k.*);
            self.index_roots.deinit();
            var coll_root_it = self.collection_roots.keyIterator();
            while (coll_root_it.next()) |k| allocator.free(k.*);
            self.collection_roots.deinit();
            if (self.wal) |w| w.deinit() catch {};
            self.deinitCachedTrees();
            self.deinitQueryCache();
        }

        try recoverDoublewriteBuffer(pool);

        if (wal_dir) |w_dir| {
            const wal_cfg = WriteAheadLog.WalConfig{
                .dir_path = w_dir,
                .max_file_size = 10 * 1024 * 1024,
                .max_buffer_size = 64 * 1024,
                .flush_interval_in_ms = 1000,
                .io = io,
                .retain_logs_days = 7,
                .log_archive_enabled = false,
                .log_archive_dest_path = "",
            };
            self.wal = try WriteAheadLog.init(allocator, null, wal_cfg);
            pool.wal_gate = .{ .ctx = self.wal.?, .flush = walGateFlush, .durable_lsn = walGateDurableLsn };
        }

        if (pool.pager.num_pages == 0) {
            const hf = try pool.newPage(.leaf);
            const hid = hf.page_id.?;
            defer pool.unpinPage(hid, true);

            const rf = try pool.newPage(.leaf);
            const rid = rf.page_id.?;
            pool.unpinPage(rid, true);

            var dw: usize = 0;
            while (dw < 17) : (dw += 1) {
                const dw_f = try pool.newPage(.leaf);
                const dw_id = dw_f.page_id.?;
                pool.unpinPage(dw_id, true);
            }

            const hp = pool.pageOf(hf);
            @memset(hp.data[0..@sizeOf(Header)], 0);
            var hdr = Header{
                .magic = MAGIC,
                .version = VERSION,
                .page_size = PAGE_SIZE,
                .root_page_id = rid,
                .lsn = 0,
                .free_page_list_head = 0,
            };
            writeHeader(hp.data, &hdr);

            try pool.flushPage(rid);
            try pool.flushPage(hid);

            self.master_tree = try BPlusTree.init(pool, rid, allocator);
            master_tree_initialized = true;
        } else {
            const hf = try pool.fetchPage(0);
            defer pool.unpinPage(0, false);
            const hp = pool.pageOf(hf);
            const hdr = readHeader(hp.data);
            if (hdr.magic != MAGIC) return error.InvalidMagic;
            if (hdr.version != VERSION) return error.UnsupportedVersion;
            if (hdr.page_size != PAGE_SIZE) return error.PageSizeMismatch;
 
            self.master_tree = try BPlusTree.init(pool, hdr.root_page_id, allocator);
            master_tree_initialized = true;
 
            var scratch_buf: [PAGE_SIZE]u8 = undefined;
            try pool.pager.loadFreeList(hdr.free_page_list_head, &scratch_buf);

            // Restore committed-transaction visibility from the checkpoint sidecar
            // BEFORE loadCatalog, because loadCatalog rebuilds the collection
            // secondary indexes and that backfill only indexes VISIBLE documents.
            // A checkpoint truncates the WAL, so recovery cannot rebuild the
            // committed set from commit records alone; without loading it first the
            // indexes (and every read) would see an empty committed set and treat
            // all data as invisible. WAL replay below layers newer commits on top.
            if (self.wal != null) {
                self.loadCommitState() catch |err| {
                    std.log.err("loadCommitState on open failed: {any}", .{err});
                };
            }

            try self.loadCatalog();
            // Reload persisted wasm UDFs (embed-wasm.md M1). Non-fatal: a bad module file is
            // skipped so a corrupt UDF cannot block the database from opening.
            self.loadWasmFunctions() catch |err| {
                std.log.warn("loadWasmFunctions on open failed: {any}", .{err});
            };
        }

        if (self.wal) |w| {
            if (w.hasData()) {
                try self.recoverTo(recover_target);
            }
        }

        try self.ensureSystemCatalogTables();

        self.security_manager = try SecurityManager.init(allocator, false, io);
        try self.security_manager.loadUsers(self);
        try self.security_manager.loadRolesAndPrivileges(self);

        if (self.security_manager.users.count() == 0) {
            var salt: [32]u8 = undefined;
            std.Io.random(io, &salt);
            const hash = try self.security_manager.hashKey("admin", salt);
            const hex_hash = try hexEncode(allocator, &hash);
            defer allocator.free(hex_hash);
            const hex_salt = try hexEncode(allocator, &salt);
            defer allocator.free(hex_salt);
            const password_hash = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ hex_hash, hex_salt });
            defer allocator.free(password_hash);

            try self.registerUser("admin", password_hash, "admin", 1);
            try self.security_manager.loadUsers(self);
        }

        try self.populateTreeCaches();

        self.loadFence();

        @atomicStore(bool, &self.bg_task_running, true, .seq_cst);
        self.group.async(io, runBgWriterTask, .{self});

        return self;
    }

    /// Frees every per-table [`GroupLock`] and its owned key, then the map.
    pub fn deinitTableLocks(self: *Database) void {
        var it = self.table_locks.iterator();
        while (it.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.destroy(e.value_ptr.*);
        }
        self.table_locks.deinit();
    }

    /// Tears down the cached table and index [`BPlusTree`]s and their keys.
    ///
    /// Each tree's `is_cached` flag is cleared *before* `deinit` so the tree
    /// actually releases its resources (a cached tree otherwise treats deinit as
    /// a no-op, since ownership normally sits with the cache). Called both during
    /// teardown and by [`Database.populateTreeCaches`] to rebuild from scratch.
    pub fn deinitCachedTrees(self: *Database) void {
        var table_tree_it = self.table_trees.iterator();
        while (table_tree_it.next()) |entry| {
            const tree = entry.value_ptr.*;
            tree.is_cached = false;
            tree.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.table_trees.deinit();

        var index_tree_it = self.index_trees.iterator();
        while (index_tree_it.next()) |entry| {
            const tree = entry.value_ptr.*;
            tree.is_cached = false;
            tree.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.index_trees.deinit();

        var coll_tree_it = self.collection_trees.iterator();
        while (coll_tree_it.next()) |entry| {
            const tree = entry.value_ptr.*;
            tree.is_cached = false;
            tree.deinit();
            self.allocator.free(entry.key_ptr.*);
        }
        self.collection_trees.deinit();

    }

    /// Rebuilds the live-tree caches from the current `table_roots` /
    /// `index_roots` maps.
    ///
    /// Drops any existing cached trees first, then opens one [`BPlusTree`] per
    /// known root and marks it `is_cached`. Called after any operation that can
    /// invalidate cached roots wholesale: recovery, catalog reload, and follower
    /// stream application.
    pub fn populateTreeCaches(self: *Database) !void {
        self.deinitCachedTrees();

        self.table_trees = std.StringHashMap(*BPlusTree).init(self.allocator);
        self.index_trees = std.StringHashMap(*BPlusTree).init(self.allocator);
        self.collection_trees = std.StringHashMap(*BPlusTree).init(self.allocator);

        var table_it = self.table_roots.iterator();
        while (table_it.next()) |entry| {
            const tree = try BPlusTree.init(self.pool, entry.value_ptr.*, self.allocator);
            tree.is_cached = true;
            try self.table_trees.put(try self.allocator.dupe(u8, entry.key_ptr.*), tree);
        }

        var index_it = self.index_roots.iterator();
        while (index_it.next()) |entry| {
            const tree = try BPlusTree.init(self.pool, entry.value_ptr.*, self.allocator);
            tree.is_cached = true;
            try self.index_trees.put(try self.allocator.dupe(u8, entry.key_ptr.*), tree);
        }

        var coll_it = self.collection_roots.iterator();
        while (coll_it.next()) |entry| {
            const tree = try BPlusTree.init(self.pool, entry.value_ptr.*, self.allocator);
            tree.is_cached = true;
            try self.collection_trees.put(try self.allocator.dupe(u8, entry.key_ptr.*), tree);
        }

    }






    /// One encoded index key, held as a span into a shared byte blob so the whole
    /// key set can be sorted without a per-key allocation.
    const KeySpan = struct { off: u32, len: u32 };










    /// Frees the query plan cache: each key, then each [`CachedQuery`]'s parser
    /// (deinit then destroy, in that order because the statement borrows from it).
    pub fn deinitQueryCache(self: *Database) void {
        var qc_it = self.query_cache.iterator();
        while (qc_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.parser.deinit();
            self.allocator.destroy(entry.value_ptr.parser);
        }
        self.query_cache.deinit();
    }

    // ---- wasm UDF persistence (embed-wasm.md M1) ----
    //
    // File-backed: each registered module is written to `<base_dir>/udf/<NAME>.wasm` so it
    // survives restart, and reloaded on open. This is durable across a restart but is NOT
    // WAL-consistent or replicated; moving the modules into the catalog (so they ride the WAL
    // and replicate) is a later slice. Names are already upper-cased by the caller.

    /// Persist a registered wasm UDF's module bytes so it survives restart.
    pub fn persistWasmFunction(self: *Database, name: []const u8, bytes: []const u8) !void {
        if (self.base_dir.len == 0) return; // no on-disk home (e.g. transient/test db)
        const io = self.pool.pager.io;
        var dbuf: [512]u8 = undefined;
        const dir = try std.fmt.bufPrint(&dbuf, "{s}/udf", .{self.base_dir});
        std.Io.Dir.createDirPath(.cwd(), io, dir) catch {};
        var pbuf: [700]u8 = undefined;
        const path = try std.fmt.bufPrint(&pbuf, "{s}/{s}.wasm", .{ dir, name });
        const f = try std.Io.Dir.createFile(.cwd(), io, path, .{ .truncate = true });
        defer f.close(io);
        try f.writeStreamingAll(io, bytes);
    }

    /// Remove a persisted wasm UDF module (best-effort; missing file is fine).
    pub fn removeWasmFunction(self: *Database, name: []const u8) void {
        if (self.base_dir.len == 0) return;
        const io = self.pool.pager.io;
        var pbuf: [700]u8 = undefined;
        const path = std.fmt.bufPrint(&pbuf, "{s}/udf/{s}.wasm", .{ self.base_dir, name }) catch return;
        std.Io.Dir.deleteFile(.cwd(), io, path) catch {};
    }

    /// Reload persisted wasm UDFs into the registry on open (called after loadCatalog). A
    /// missing directory means none were registered; a bad module file is skipped, not fatal.
    fn loadWasmFunctions(self: *Database) !void {
        if (self.base_dir.len == 0) return;
        const io = self.pool.pager.io;
        var dbuf: [512]u8 = undefined;
        const dir = try std.fmt.bufPrint(&dbuf, "{s}/udf", .{self.base_dir});
        var d = std.Io.Dir.openDir(.cwd(), io, dir, .{ .iterate = true }) catch return;
        defer d.close(io);
        var it = d.iterate();
        while (it.next(io) catch null) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".wasm")) continue;
            const fn_name = entry.name[0 .. entry.name.len - ".wasm".len];
            var pbuf: [700]u8 = undefined;
            const path = std.fmt.bufPrint(&pbuf, "{s}/{s}", .{ dir, entry.name }) catch continue;
            const bytes = std.Io.Dir.readFileAlloc(.cwd(), io, path, self.allocator, .unlimited) catch continue;
            defer self.allocator.free(bytes);
            self.wasm_functions.register(fn_name, bytes, .{}) catch continue;
        }
    }

    /// Cleanly shuts the database down and destroys it.
    ///
    /// Signals and waits for the background writer to stop, then persists the
    /// authoritative header to page 0 (updating the master-tree root, the last
    /// LSN and the free-list head), flushes and fsyncs every page, checkpoints
    /// and closes the WAL, and finally frees the catalog, roots, locks,
    /// transaction/undo state, caches and the [`Database`] itself. This is the
    /// durable counterpart to [`Database.crashSimulate`]. After it returns the
    /// pointer is invalid.
    pub fn close(self: *Database) void {
        @atomicStore(bool, &self.is_closed, true, .seq_cst);
        self.stopFollower();
        self.group.cancel(self.pool.pager.io);
        self.group.await(self.pool.pager.io) catch {};

        while (@atomicLoad(bool, &self.bg_task_running, .seq_cst)) {
            const delay = std.Io.Duration.fromMilliseconds(1);
            self.pool.pager.io.sleep(delay, .real) catch |err| {
                std.log.warn("Database close wait loop sleep error: {any}", .{err});
            };
        }

        self.security_manager.deinit();

        {
            const hf = self.pool.fetchPage(0) catch return;
            defer self.pool.unpinPage(0, true);
            const hp = self.pool.pageOf(hf);
            var hdr = readHeader(hp.data);
            hdr.root_page_id = self.master_tree.root_page_id;
            hdr.lsn = self.master_tree.lsn;
            var scratch_buf: [PAGE_SIZE]u8 = undefined;
            hdr.free_page_list_head = self.pool.pager.persistFreeList(&scratch_buf) catch 0;
            writeHeader(hp.data, &hdr);
        }

        self.pool.flushAllPagesFast() catch |err| {
            std.log.err("Failed to flush all pages during close: {any}", .{err});
        };
        self.pool.pager.sync() catch |err| {
            std.log.err("Failed to sync pager during close: {any}", .{err});
        };

        // Clean shutdown: the index pages are now durable, so record each index's
        // current root. The next open loads the persisted trees instead of
        // rebuilding them by scanning every document (a 10M collection made that
        // a multi-minute restart). A crash writes no sidecar, so recovery still
        // falls back to the safe rebuild.

        if (self.wal) |w| {
            _ = w.checkpoint() catch {};
            // Persist committed-txn visibility before dropping the log, so a clean
            // shutdown that truncated the WAL still recovers all committed data.
            self.persistCommitState() catch |err| {
                std.log.err("persistCommitState during close failed: {any}", .{err});
            };
            w.deinit() catch {};
            self.pool.wal_gate = null;
        }

        self.master_tree.deinit();
        self.wasm_functions.deinit();
        self.catalog.deinit();
        var table_it = self.table_roots.keyIterator();
        while (table_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.table_roots.deinit();

        var index_it = self.index_roots.keyIterator();
        while (index_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.index_roots.deinit();

        var coll_root_it = self.collection_roots.keyIterator();
        while (coll_root_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.collection_roots.deinit();
        self.deinitTableLocks();
        self.txn_manager.deinit();
        self.undo_log.deinit();

        self.deinitCachedTrees();

        self.deinitQueryCache();

        if (self.base_dir.len > 0) self.allocator.free(self.base_dir);
        if (self.durable_repl) |dr| {
            dr.deinit();
            self.allocator.destroy(dr);
            self.durable_repl = null;
        }
        self.pool.deinit() catch |err| {
            std.log.err("Failed to deinit page pool: {any}", .{err});
        };
        self.allocator.destroy(self);
    }

    /// Tears the database down *without* flushing pages or writing the header,
    /// deliberately mimicking a process crash for durability tests.
    ///
    /// It still stops the background writer and frees in-memory state (so the
    /// test process leaks nothing), but the file is left exactly as the pool last
    /// wrote it: whatever the WAL has not yet been checkpointed into pages is only
    /// recoverable by replaying the log on the next [`Database.open`]. Contrast
    /// [`Database.close`], which makes the file self-consistent before exit.
    pub fn crashSimulate(self: *Database) void {
        @atomicStore(bool, &self.is_closed, true, .seq_cst);
        self.stopFollower();
        self.group.cancel(self.pool.pager.io);
        self.group.await(self.pool.pager.io) catch {};
        while (@atomicLoad(bool, &self.bg_task_running, .seq_cst)) {
            const delay = std.Io.Duration.fromMilliseconds(1);
            self.pool.pager.io.sleep(delay, .real) catch {};
        }

        if (self.wal) |w| {
            w.deinit() catch {};
            self.pool.wal_gate = null;
        }
        self.security_manager.deinit();

        self.master_tree.deinit();
        self.catalog.deinit();
        var table_it = self.table_roots.keyIterator();
        while (table_it.next()) |k| self.allocator.free(k.*);
        self.table_roots.deinit();
        var index_it = self.index_roots.keyIterator();
        while (index_it.next()) |k| self.allocator.free(k.*);
        self.index_roots.deinit();
        var coll_it = self.collection_roots.keyIterator();
        while (coll_it.next()) |k| self.allocator.free(k.*);
        self.collection_roots.deinit();
        self.deinitTableLocks();
        self.txn_manager.deinit();
        self.undo_log.deinit();
        self.deinitCachedTrees();
        self.deinitQueryCache();
        if (self.base_dir.len > 0) self.allocator.free(self.base_dir);
        if (self.durable_repl) |dr| {
            dr.deinit();
            self.allocator.destroy(dr);
            self.durable_repl = null;
        }
        self.pool.deinitNoFlush();
        self.allocator.destroy(self);
    }

    /// Flushes all dirty pages and truncates the WAL to a checkpoint.
    ///
    /// Held under [`Database.rw_lock`] exclusively so no writer can dirty pages
    /// between the flush and the WAL checkpoint. A no-op when there is no WAL.
    ///
    /// Before the flush it also persists the pager free list into the page-0
    /// header. The free list is otherwise written to the header only at a clean
    /// [`Database.close`], so a crash lost every page freed since the last clean
    /// shutdown (B+Tree merges, deletes and `DROP` reclamation all feed it). That
    /// only ever leaked space (recovery's [`Pager.loadFreeList`] is checksum
    /// guarded, so a torn or stale chain truncates to a leak, never hands out a
    /// live page), but persisting it here bounds the leak window to one checkpoint.
    /// This is why the rewrite is safe under the exclusive `rw_lock`:
    /// [`Pager.persistFreeList`] writes a link into each freed page, and the lock
    /// excludes every writer, hence every page allocation, so no page in the
    /// snapshot can be handed out and overwritten while we are writing its chain
    /// link. The following `flushAllPages` issues a single `sync`, so the header
    /// and the chain pages it points at are made durable together.
    pub fn checkpoint(self: *Database) !void {
        self.rw_lock.lock(self.pool.pager.io);
        defer self.rw_lock.unlock(self.pool.pager.io);

        if (self.wal) |w| {
            {
                const hf = try self.pool.fetchPage(0);
                defer self.pool.unpinPage(0, true);
                const hp = self.pool.pageOf(hf);
                var hdr = readHeader(hp.data);
                hdr.root_page_id = self.master_tree.root_page_id;
                hdr.lsn = self.master_tree.lsn;
                var scratch_buf: [PAGE_SIZE]u8 = undefined;
                hdr.free_page_list_head = self.pool.pager.persistFreeList(&scratch_buf) catch |err| blk: {
                    std.log.err("persistFreeList during checkpoint failed: {any}", .{err});
                    break :blk hdr.free_page_list_head;
                };
                writeHeader(hp.data, &hdr);
            }
            try self.pool.flushAllPages();
            try w.checkpoint();
            self.persistCommitState() catch |err| {
                std.log.err("persistCommitState after checkpoint failed: {any}", .{err});
            };
        }
    }

    /// Persist the committed-transaction set to a `COMMITTED` sidecar beside the
    /// WAL, atomically (write `.tmp`, fsync, rename).
    ///
    /// Recovery rebuilds transaction visibility solely by replaying WAL commit
    /// records, but a checkpoint TRUNCATES the WAL, so once old segments are
    /// dropped their commit records are gone and the data they committed would be
    /// invisible after restart. This snapshot is the durable record of "these
    /// transactions had committed as of the checkpoint" so recovery can restore
    /// visibility without the old log.
    ///
    /// Bounded layout (native-endian), tagged with `COMMIT_STATE_MAGIC` so the
    /// loader can tell it from the old whole-set format:
    /// `[u64 magic][u64 next_tx_id][u64 committed_below]`
    /// `[u64 abort_count][abort_count x u64][u64 commit_count][commit_count x u64]`.
    /// Everything below `committed_below` is committed unless it appears in the
    /// abort list, so only the recent committed window is written - the whole point
    /// of the watermark (see [`TransactionManager.advanceWatermark`]).
    const COMMIT_STATE_MAGIC: u64 = 0x4E4F5641_434D5431; // commit-state sidecar format tag (fixed on-disk value)
    fn persistCommitState(self: *Database) !void {
        const w = self.wal orelse return;
        const io = self.pool.pager.io;

        // Bound the state before writing it: advancing the watermark drops every
        // committed id now implied by "below the watermark == committed", so the
        // sidecar holds only next_tx_id, the watermark, the (rare) aborts, and the
        // recent committed window instead of one entry per committed transaction.
        // A 10M-insert load persists a handful of bytes here, not ~80 MB.
        self.txn_manager.advanceWatermark(io);

        var buf = std.ArrayList(u8).empty;
        defer buf.deinit(self.allocator);
        {
            self.txn_manager.mutex.lockUncancelable(io);
            defer self.txn_manager.mutex.unlock(io);
            const magic: u64 = COMMIT_STATE_MAGIC;
            try buf.appendSlice(self.allocator, std.mem.asBytes(&magic));
            const next: u64 = self.txn_manager.next_tx_id;
            const below: u64 = self.txn_manager.committed_below;
            try buf.appendSlice(self.allocator, std.mem.asBytes(&next));
            try buf.appendSlice(self.allocator, std.mem.asBytes(&below));
            const abort_count: u64 = self.txn_manager.aborted.count();
            try buf.appendSlice(self.allocator, std.mem.asBytes(&abort_count));
            var ait = self.txn_manager.aborted.keyIterator();
            while (ait.next()) |k| {
                const id: u64 = @intCast(k.*);
                try buf.appendSlice(self.allocator, std.mem.asBytes(&id));
            }
            const count: u64 = self.txn_manager.committed_txns.count();
            try buf.appendSlice(self.allocator, std.mem.asBytes(&count));
            var it = self.txn_manager.committed_txns.keyIterator();
            while (it.next()) |k| {
                const id: u64 = @intCast(k.*);
                try buf.appendSlice(self.allocator, std.mem.asBytes(&id));
            }
        }

        var tmp_buf: [std.fs.max_path_bytes]u8 = undefined;
        const tmp = try std.fmt.bufPrint(&tmp_buf, "{s}/COMMITTED.tmp", .{w.dir_path});
        var file = try std.Io.Dir.createFile(.cwd(), io, tmp, .{});
        defer file.close(io);
        try file.writeStreamingAll(io, buf.items);
        try file.sync(io);

        var final_buf: [std.fs.max_path_bytes]u8 = undefined;
        const final = try std.fmt.bufPrint(&final_buf, "{s}/COMMITTED", .{w.dir_path});
        try std.Io.Dir.rename(.cwd(), tmp, .cwd(), final, io);
    }

    /// Load the `COMMITTED` sidecar (written by [`persistCommitState`]) into the
    /// transaction manager before WAL replay, restoring visibility for
    /// transactions whose commit records were dropped by a checkpoint. WAL replay
    /// then layers newer commits on top. A missing file is a clean no-op (fresh db
    /// or a db that never checkpointed).
    fn loadCommitState(self: *Database) !void {
        const w = self.wal orelse return;
        const io = self.pool.pager.io;

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/COMMITTED", .{w.dir_path}) catch return;
        const data = std.Io.Dir.readFileAlloc(.cwd(), io, path, self.allocator, .unlimited) catch |err| {
            if (err == error.FileNotFound) return;
            return err;
        };
        defer self.allocator.free(data);
        if (data.len < 16) return;

        self.txn_manager.mutex.lockUncancelable(io);
        defer self.txn_manager.mutex.unlock(io);

        const first = std.mem.bytesToValue(u64, data[0..8]);
        if (first == COMMIT_STATE_MAGIC) {
            // Bounded format: magic, next, watermark, aborts, recent-committed.
            if (data.len < 32) return;
            const next = std.mem.bytesToValue(u64, data[8..16]);
            const below = std.mem.bytesToValue(u64, data[16..24]);
            if (next > self.txn_manager.next_tx_id) self.txn_manager.next_tx_id = next;
            if (below > self.txn_manager.committed_below) self.txn_manager.committed_below = below;
            var off: usize = 24;
            const abort_count = std.mem.bytesToValue(u64, data[off..][0..8]);
            off += 8;
            var i: u64 = 0;
            while (i < abort_count and off + 8 <= data.len) : (i += 1) {
                try self.txn_manager.aborted.put(@intCast(std.mem.bytesToValue(u64, data[off..][0..8])), {});
                off += 8;
            }
            if (off + 8 > data.len) return;
            const count = std.mem.bytesToValue(u64, data[off..][0..8]);
            off += 8;
            i = 0;
            while (i < count and off + 8 <= data.len) : (i += 1) {
                try self.txn_manager.committed_txns.put(@intCast(std.mem.bytesToValue(u64, data[off..][0..8])), {});
                off += 8;
            }
            return;
        }

        // Legacy whole-set format `[next][count][ids...]`: load it as-is. These ids
        // stay in committed_txns and are correct; the watermark stays where it is
        // and only advances on the next checkpoint.
        const next = first;
        const count = std.mem.bytesToValue(u64, data[8..16]);
        if (next > self.txn_manager.next_tx_id) self.txn_manager.next_tx_id = next;
        var i: u64 = 0;
        var off: usize = 16;
        while (i < count and off + 8 <= data.len) : (i += 1) {
            try self.txn_manager.committed_txns.put(@intCast(std.mem.bytesToValue(u64, data[off..][0..8])), {});
            off += 8;
        }
    }

    /// One record parsed from the `INDEX_ROOTS` sidecar: the catalog `type`/`name`
    /// that identify a collection index, and the page id of its persisted B+Tree
    /// root at the last clean shutdown.

    const INDEX_ROOTS_MAGIC: u64 = 0x4E4F5641_49445831; // index-roots format tag (fixed on-disk value)



    /// Garbage-collects dead MVCC versions of one user table (lock-free helper).
    ///
    /// For every key it reconstructs the version chain and drops any version
    /// whose deleting transaction (`xmax`) has committed *and* is older than the
    /// oldest still-active transaction, since no live snapshot can ever see it
    /// again. If every version is dead the key is deleted; if only some are, the
    /// survivors are re-packed and written back. System tables (`sys.*`) are
    /// skipped. The caller must already hold [`Database.rw_lock`]; the public
    /// entry points are [`Database.vacuumTable`] and [`Database.vacuum`].
    fn vacuumTableInternal(self: *Database, table_name: []const u8) !void {
        if (std.mem.startsWith(u8, table_name, "sys.")) return;

        const root = self.table_roots.get(table_name) orelse return error.TableNotFound;
        var tree = try BPlusTree.init(self.pool, root, self.allocator);
        defer tree.deinit();

        const oldest_tx = self.txn_manager.getOldestActiveTxId(self.pool.pager.io);

        var keys = std.ArrayList([]const u8).empty;
        defer {
            for (keys.items) |k| self.allocator.free(k);
            keys.deinit(self.allocator);
        }

        var it = try tree.iterator();
        defer it.deinit();
        while (try it.next()) |cell| {
            try keys.append(self.allocator, try self.allocator.dupe(u8, cell.key));
        }

        for (keys.items) |key| {
            const opt_val = try tree.search(key, self.allocator);
            if (opt_val) |val| {
                defer self.allocator.free(val);
                const versions = try self.reconstructVersionChain(val, self.allocator);
                defer {
                    for (versions) |*v| v.deinit(self.allocator);
                    self.allocator.free(versions);
                }

                var kept_versions = std.ArrayList(row.DecodedVersion).empty;
                // Each surviving version is MOVED here (its `fixed`/`heap` slices
                // are nulled out of `versions` below so the `versions` defer will
                // not touch them), so ownership of those buffers now lives with
                // `kept_versions`. Deinit each one, not just the list storage, or
                // every kept version's payload leaks - which, since "keep" is the
                // norm (nothing dead), meant the background vacuum leaked on
                // essentially every pass. `packVersions` serialises into a fresh
                // buffer and does not take ownership, so freeing here is correct
                // whether the row is repacked, deleted, or left unchanged.
                defer {
                    for (kept_versions.items) |*kv| kv.deinit(self.allocator);
                    kept_versions.deinit(self.allocator);
                }

                for (versions, 0..) |v, idx| {
                    var is_dead = false;
                    if (v.xmax != 0) {
                        // Watermark-aware: below the commit watermark an id is
                        // committed unless it aborted; the recent window uses set
                        // membership. isCommitted takes the manager mutex itself.
                        const xmax_committed = self.txn_manager.isCommitted(self.pool.pager.io, v.xmax);

                        if (xmax_committed and v.xmax < oldest_tx) {
                            is_dead = true;
                        }
                    }

                    if (is_dead) {
                    } else {
                        try kept_versions.append(self.allocator, v);
                        versions[idx].fixed = &[_]u8{};
                        versions[idx].heap = &[_]u8{};
                    }
                }

                if (kept_versions.items.len == 0) {
                    try tree.delete(key);
                } else if (kept_versions.items.len < versions.len) {
                    const packed_bytes = try row.packVersions(self.allocator, kept_versions.items);
                    defer self.allocator.free(packed_bytes);
                    try tree.insert(key, packed_bytes);
                }
            }
        }
    }

    /// Vacuums a single table under the database-wide write lock. See
    /// [`Database.vacuumTableInternal`] for what "dead" means.
    pub fn vacuumTable(self: *Database, table_name: []const u8) !void {
        self.rw_lock.lock(self.pool.pager.io);
        defer self.rw_lock.unlock(self.pool.pager.io);

        try self.vacuumTableInternal(table_name);
    }

    /// Vacuums every user table under a single hold of the write lock.
    ///
    /// Table names are snapshotted into an owned list first so the catalog is not
    /// iterated while individual vacuums mutate trees. Invoked periodically by the
    /// background writer ([`Database.runBgWriterTask`]).
    pub fn vacuum(self: *Database) !void {
        // Skip entirely when nothing has created a dead version since the last
        // pass. Reclaimable garbage only ever comes from `updateRowMVCC` (which
        // bumps `garbage_ops`); an INSERT-only or otherwise churn-free interval
        // leaves the counter unchanged, so there is nothing to reclaim and the
        // O(all-rows) full-table scan (and the exclusive `rw_lock` it holds) is
        // pure waste. The check is a cheap atomic read taken BEFORE the lock so a
        // quiescent server does not even contend for it. Read the counter first,
        // vacuum, then store that same value: any mutation racing in during the
        // pass leaves the counter ahead, so the next pass still runs.
        const ops_before = self.garbage_ops.load(.monotonic);
        if (ops_before == self.last_vacuum_garbage_ops) return;

        self.rw_lock.lock(self.pool.pager.io);
        defer self.rw_lock.unlock(self.pool.pager.io);
        self.last_vacuum_garbage_ops = ops_before;

        var table_names = std.ArrayList([]const u8).empty;
        defer {
            for (table_names.items) |name| self.allocator.free(name);
            table_names.deinit(self.allocator);
        }

        for (self.catalog.tables.items) |tbl| {
            try table_names.append(self.allocator, try self.allocator.dupe(u8, tbl.name));
        }

        for (table_names.items) |tbl_name| {
            try self.vacuumTableInternal(tbl_name);
        }
    }

    /// The background maintenance coroutine spawned at open time.
    ///
    /// Every 500 ms while the database is open it flushes dirty pages and purges
    /// stale undo pages; every 20th tick (~10 s) it also runs a full vacuum.
    /// It observes [`Database.is_closed`] under seq-cst atomics, treats
    /// `error.Canceled` as a clean stop, and clears
    /// [`Database.bg_task_running`] on exit so [`Database.close`] can proceed.
    fn runBgWriterTask(self: *Database) std.Io.Cancelable!void {
        dblog.debug("background writer task started", .{});
        const io = self.pool.pager.io;
        const interval = std.Io.Duration.fromMilliseconds(500);
        defer {
            dblog.debug("background writer task exiting", .{});
            @atomicStore(bool, &self.bg_task_running, false, .seq_cst);
        }

        var ticks: usize = 0;
        var cp_ticks: usize = 0;
        while (!@atomicLoad(bool, &self.is_closed, .seq_cst)) {
            try io.sleep(interval, .real);
            if (@atomicLoad(bool, &self.is_closed, .seq_cst)) return;
            dblog.debug("bgwriter flushing pages", .{});
            self.pool.flushAllPages() catch |err| {
                if (err == error.Canceled) return;
                dblog.err("bgwriter flushAllPages failed: {any}", .{err});
            };
            self.purgeStaleUndoPages() catch |err| {
                if (err == error.Canceled) return;
                dblog.err("bgwriter purgeStaleUndoPages failed: {any}", .{err});
            };

            // Truncate the WAL. `flushAllPages` above just made every dirty page
            // durable, so the pages described by already-retired WAL segments are
            // safely on disk and those segments can be dropped. Without this the
            // WAL only ever shrank at shutdown, so a sustained write load (e.g. a
            // bulk import) grew it without bound (~9 GB for a 10M-row load) and
            // could fill the disk. Done every few ticks (~2s) rather than every
            // wake so we are not rotating a near-empty segment each 500 ms.
            if (self.wal) |w| {
                _ = w;
                if (cp_ticks % 4 == 0) {
                    // Full checkpoint (not just `w.checkpoint()`): it truncates the
                    // WAL, persists the committed-txn set so the dropped segments do
                    // not take commit visibility with them, AND rewrites the page-0
                    // header (free-list head + master root/lsn) so the pages freed
                    // since the last one become durable. It takes `rw_lock`
                    // exclusively, which is exactly the exclusion `persistFreeList`
                    // needs, so no live writer can reallocate a page mid-persist.
                    self.checkpoint() catch |err| {
                        if (err == error.Canceled) return;
                        dblog.err("bgwriter checkpoint failed: {any}", .{err});
                    };
                }
                cp_ticks += 1;
            }

            ticks += 1;
            if (ticks >= 20) {
                ticks = 0;
                self.vacuum() catch |err| {
                    dblog.err("bgwriter vacuum failed: {any}", .{err});
                };
            }
        }
    }

    /// Frees whole undo pages whose every record predates any live snapshot.
    ///
    /// Scans undo pages from the front (oldest first), stopping at the active
    /// page. A page is reclaimable only when the maximum `xmin`/`xmax` of all its
    /// records is below the oldest active transaction id, and only a leading run
    /// of such pages is freed (encountering a still-needed page stops the scan,
    /// since undo pages are ordered by age). Survivors are compacted forward in
    /// `undo_pages`. Runs under the undo log's own mutex.
    pub fn purgeStaleUndoPages(self: *Database) !void {
        const oldest_tx = self.txn_manager.getOldestActiveTxId(self.pool.pager.io);

        self.undo_log.mutex.lockUncancelable(self.pool.pager.io);
        defer self.undo_log.mutex.unlock(self.pool.pager.io);

        var pages_to_free = std.ArrayList(PageId).empty;
        defer pages_to_free.deinit(self.allocator);

        var keep_idx: usize = 0;
        while (keep_idx < self.undo_log.undo_pages.items.len) : (keep_idx += 1) {
            const pid = self.undo_log.undo_pages.items[keep_idx];
            if (pid == self.undo_log.active_page_id) {
                break;
            }

            const f = try self.pool.fetchPage(pid);
            const p = self.pool.pageOf(f);
            const h = p.headerPtr();

            var max_tx: u64 = 0;
            var offset: usize = @sizeOf(page_mod.PageHeader);
            const undo_hdr_sz = @sizeOf(@import("../concurrency/undo.zig").UndoRecordHeader);
            while (offset < h.free_space_start) {
                var rec_hdr: @import("../concurrency/undo.zig").UndoRecordHeader = undefined;
                @memcpy(std.mem.asBytes(&rec_hdr), p.data[offset..][0..undo_hdr_sz]);
                
                if (rec_hdr.xmin > max_tx) max_tx = rec_hdr.xmin;
                if (rec_hdr.xmax > max_tx) max_tx = rec_hdr.xmax;

                offset += undo_hdr_sz + rec_hdr.fixed_len + rec_hdr.heap_len;
            }
            self.pool.unpinPage(pid, false);

            if (max_tx < oldest_tx) {
                try pages_to_free.append(self.allocator, pid);
            } else {
                break;
            }
        }

        if (pages_to_free.items.len > 0) {
            for (pages_to_free.items) |pid| {
                try self.pool.discardPage(pid);
            }

            const num_freed = pages_to_free.items.len;
            var i: usize = 0;
            while (i < self.undo_log.undo_pages.items.len - num_freed) : (i += 1) {
                self.undo_log.undo_pages.items[i] = self.undo_log.undo_pages.items[i + num_freed];
            }
            self.undo_log.undo_pages.items.len -= num_freed;
        }
    }

    /// Repairs torn page writes from the doublewrite buffer on open.
    ///
    /// The doublewrite header lives at page 2 with its data slots at pages 3..;
    /// before a batch of pages is written in place the pool first mirrors them
    /// here and fsyncs, so a crash mid-write can be undone by copying the intact
    /// mirror back to each target page. If the header's magic (`0x44574252`,
    /// "DWBR") is set and 1..16 pages are pending, they are restored, fsynced,
    /// and the header is cleared. A no-op on a fresh file (`num_pages <= 2`) or a
    /// clean shutdown. Must run before any page is read for real use.
    fn recoverDoublewriteBuffer(pool: *PagePool) !void {
        const DoublewriteHeader = @import("../storage/pool.zig").DoublewriteHeader;
        var hdr_buf: [PAGE_SIZE]u8 = undefined;
        if (pool.pager.num_pages <= 2) return;

        pool.pager.readPage(2, &hdr_buf) catch |err| {
            if (err == error.EndOfFile) return;
            return err;
        };

        var hdr: DoublewriteHeader = undefined;
        @memcpy(std.mem.asBytes(&hdr), hdr_buf[0..@sizeOf(DoublewriteHeader)]);

        if (hdr.magic == 0x44574252 and hdr.num_pages > 0 and hdr.num_pages <= 16) {
            dblog.debug("doublewrite recovery: restoring {d} pages", .{hdr.num_pages});
            var slot_buf: [PAGE_SIZE]u8 = undefined;
            var i: usize = 0;
            while (i < hdr.num_pages) : (i += 1) {
                const target_id = hdr.page_ids[i];
                try pool.pager.readPage(@intCast(3 + i), &slot_buf);
                try pool.pager.writePage(target_id, &slot_buf);
            }
            try pool.pager.sync();

            hdr.num_pages = 0;
            @memset(&hdr_buf, 0);
            @memcpy(hdr_buf[0..@sizeOf(DoublewriteHeader)], std.mem.asBytes(&hdr));
            try pool.pager.writePage(2, &hdr_buf);
            try pool.pager.sync();
            dblog.debug("doublewrite recovery complete", .{});
        }
    }

    /// Deep-copies a `std.json.Value`, duplicating every owned string and
    /// recursively cloning arrays and objects, using the database allocator.
    ///
    /// The counterpart is [`Database.freeJsonValue`]; the two must be paired so a
    /// cloned value is freed with the same allocator that owns its parts.
    pub fn cloneJsonValue(self: *Database, val: std.json.Value) error{OutOfMemory}!std.json.Value {
        switch (val) {
            .null => return .null,
            .bool => |b| return .{ .bool = b },
            .integer => |i| return .{ .integer = i },
            .float => |f| return .{ .float = f },
            .string => |s| return .{ .string = try self.allocator.dupe(u8, s) },
            .number_string => |s| return .{ .number_string = try self.allocator.dupe(u8, s) },
            .array => |arr| {
                var new_arr = std.json.Array.init(self.allocator);
                errdefer new_arr.deinit();
                for (arr.items) |item| {
                    try new_arr.append(try self.cloneJsonValue(item));
                }
                return .{ .array = new_arr };
            },
            .object => |obj| {
                var new_obj = std.json.ObjectMap.empty;
                errdefer new_obj.deinit(self.allocator);
                var it = obj.iterator();
                while (it.next()) |entry| {
                    try new_obj.put(self.allocator, try self.allocator.dupe(u8, entry.key_ptr.*), try self.cloneJsonValue(entry.value_ptr.*));
                }
                return .{ .object = new_obj };
            },
        }
    }

    /// Recursively frees a `std.json.Value` previously produced by
    /// [`Database.cloneJsonValue`]; frees strings and, depth-first, the contents
    /// of arrays and objects. Scalars (null/bool/int/float) own nothing.
    pub fn freeJsonValue(self: *Database, val: std.json.Value) void {
        switch (val) {
            .string => |s| self.allocator.free(s),
            .number_string => |s| self.allocator.free(s),
            .array => |arr| {
                for (arr.items) |item| {
                    self.freeJsonValue(item);
                }
                var mutable_arr = arr;
                mutable_arr.deinit();
            },
            .object => |obj| {
                var mutable_obj = obj;
                var it = mutable_obj.iterator();
                while (it.next()) |entry| {
                    self.allocator.free(entry.key_ptr.*);
                    self.freeJsonValue(entry.value_ptr.*);
                }
                mutable_obj.deinit(self.allocator);
            },
            else => {},
        }
    }

    /// Materialises the full MVCC version chain for one row from its stored bytes.
    ///
    /// Returns newest-first: index 0 is the version living in the B+Tree cell,
    /// followed by successively older versions pulled from the undo log by
    /// walking each version's `roll_ptr` until it reaches one whose `xmin`
    /// predates the oldest active transaction (older versions can no longer be
    /// visible to anyone) or an invalid pointer. Two fast paths short-circuit the
    /// walk: an empty input yields an empty slice, and a single already-old
    /// in-cell version is decoded directly without unpacking. The walk is bounded
    /// (1000 hops) and self-loop guarded to survive a corrupt chain rather than
    /// spin. Every returned [`row.DecodedVersion`] owns its `fixed`/`heap` and
    /// must be freed by the caller with the same `allocator`.
    pub fn reconstructVersionChain(self: *Database, bytes: []const u8, allocator: Allocator) ![]row.DecodedVersion {
        if (bytes.len == 0) return &[_]row.DecodedVersion{};

        if (bytes.len >= 36) {
            const xmin = std.mem.readInt(u64, bytes[4..12], .little);
            const oldest_tx = self.txn_manager.getOldestActiveTxId(self.pool.pager.io);
            if (xmin < oldest_tx) {
                const fixed_len = std.mem.readInt(u32, bytes[28..32], .little);
                const heap_len = std.mem.readInt(u32, bytes[32..36], .little);
                if (36 + fixed_len + heap_len <= bytes.len) {
                    var list = try std.ArrayList(row.DecodedVersion).initCapacity(allocator, 1);
                    errdefer {
                        for (list.items) |*v| v.deinit(allocator);
                        list.deinit(allocator);
                    }

                    const xmax = std.mem.readInt(u64, bytes[12..20], .little);
                    const roll_ptr = std.mem.readInt(u64, bytes[20..28], .little);
                    const fixed = try allocator.dupe(u8, bytes[36 .. 36 + fixed_len]);
                    errdefer allocator.free(fixed);
                    const heap = try allocator.dupe(u8, bytes[36 + fixed_len .. 36 + fixed_len + heap_len]);

                    list.appendAssumeCapacity(row.DecodedVersion{
                        .xmin = xmin,
                        .xmax = xmax,
                        .roll_ptr = roll_ptr,
                        .fixed = fixed,
                        .heap = heap,
                    });
                    return try list.toOwnedSlice(allocator);
                }
            }
        }

        const latest_versions = try row.unpackVersions(allocator, bytes);
        if (latest_versions.len == 0) return latest_versions;
        errdefer {
            for (latest_versions) |*v| v.deinit(allocator);
            allocator.free(latest_versions);
        }

        const latest = latest_versions[0];

        var list = std.ArrayList(row.DecodedVersion).empty;
        errdefer {
            for (list.items) |*v| v.deinit(allocator);
            list.deinit(allocator);
        }

        try list.append(allocator, row.DecodedVersion{
            .xmin = latest.xmin,
            .xmax = latest.xmax,
            .roll_ptr = latest.roll_ptr,
            .fixed = try allocator.dupe(u8, latest.fixed),
            .heap = try allocator.dupe(u8, latest.heap),
        });

        for (latest_versions) |*v| v.deinit(allocator);
        allocator.free(latest_versions);

        const oldest_tx = self.txn_manager.getOldestActiveTxId(self.pool.pager.io);

        if (list.items[0].xmin < oldest_tx) {
            return try list.toOwnedSlice(allocator);
        }

        var cur_roll_ptr = list.items[0].roll_ptr;
        var visited_count: usize = 0;
        while (self.isValidRollPtr(cur_roll_ptr)) {
            if (visited_count > 1000) {
                std.log.warn("reconstructVersionChain: suspected version chain loop detected at length {d}, terminating chain.", .{visited_count});
                break;
            }
            visited_count += 1;

            const rec = self.undo_log.getRecord(cur_roll_ptr, allocator) catch |err| {
                std.log.err("reconstructVersionChain: failed to get record for roll_ptr {x}: {any}", .{cur_roll_ptr, err});
                break;
            };
            defer {
                allocator.free(rec.fixed);
                allocator.free(rec.heap);
            }
            const v = row.DecodedVersion{
                .xmin = rec.xmin,
                .xmax = rec.xmax,
                .roll_ptr = rec.roll_ptr,
                .fixed = try allocator.dupe(u8, rec.fixed),
                .heap = try allocator.dupe(u8, rec.heap),
            };
            try list.append(allocator, v);
            if (rec.roll_ptr == cur_roll_ptr) {
                std.log.warn("reconstructVersionChain: self-loop cycle detected, breaking.", .{});
                break;
            }
            if (v.xmin < oldest_tx) {
                break;
            }
            cur_roll_ptr = rec.roll_ptr;
        }

        return try list.toOwnedSlice(allocator);
    }

    /// Validates an undo-log roll pointer before it is dereferenced.
    ///
    /// A `roll_ptr` packs `page_id << 16 | offset`. This confirms the page is a
    /// currently tracked undo page and that `offset` leaves room for at least an
    /// undo record header within that page's used space, so
    /// [`Database.reconstructVersionChain`] can stop safely at the end of a chain
    /// (or at a stale/garbage pointer) instead of reading out of bounds. Zero is
    /// always invalid (the chain terminator). Taken under the undo log mutex.
    pub fn isValidRollPtr(self: *Database, roll_ptr: u64) bool {
        if (roll_ptr == 0) return false;
        const page_id = @as(PageId, @intCast(roll_ptr >> 16));
        const offset = @as(u16, @intCast(roll_ptr & 0xFFFF));

        self.undo_log.mutex.lockUncancelable(self.pool.pager.io);
        defer self.undo_log.mutex.unlock(self.pool.pager.io);
        for (self.undo_log.undo_pages.items) |pid| {
            if (pid == page_id) {
                const f = self.pool.fetchPage(pid) catch return false;
                defer self.pool.unpinPage(pid, false);
                const p = self.pool.pageOf(f);
                const h = p.headerPtr();
                
                const undo_hdr_sz = @sizeOf(@import("../concurrency/undo.zig").UndoRecordHeader);
                if (offset + undo_hdr_sz <= h.free_space_start) {
                    return true;
                }
            }
        }
        return false;
    }

    /// Installs a new row version into `tree` while preserving MVCC history.
    ///
    /// If a current version exists it is first appended to the undo log, and the
    /// resulting roll pointer becomes the new version's `roll_ptr` so readers can
    /// still reach the value it replaced. The tree cell is then rewritten to hold
    /// only the new version (older versions live in the undo log, not the cell).
    /// Returns the packed bytes actually stored, owned by the caller. This is the
    /// single choke point every insert/update/delete of a user row flows through
    /// (delete is modelled as a new version with `xmax` set).
    pub fn updateRowMVCC(self: *Database, tree: *BPlusTree, key: []const u8, new_version: row.DecodedVersion) ![]const u8 {
        const opt_val = try tree.search(key, self.allocator);
        defer if (opt_val) |v| self.allocator.free(v);

        var next_roll_ptr: u64 = 0;
        var superseded = false;
        if (opt_val) |val| {
            const latest_versions = try row.unpackVersions(self.allocator, val);
            defer {
                for (latest_versions) |*v| v.deinit(self.allocator);
                self.allocator.free(latest_versions);
            }
            if (latest_versions.len > 0) {
                const latest = latest_versions[0];
                next_roll_ptr = try self.undo_log.appendRecord(latest.xmin, new_version.xmin, latest.roll_ptr, latest.fixed, latest.heap);
                superseded = true;
            }
        }

        var new_v_copy = row.DecodedVersion{
            .xmin = new_version.xmin,
            .xmax = new_version.xmax,
            .roll_ptr = next_roll_ptr,
            .fixed = try self.allocator.dupe(u8, new_version.fixed),
            .heap = try self.allocator.dupe(u8, new_version.heap),
        };
        defer new_v_copy.deinit(self.allocator);

        const packed_bytes = try row.packVersions(self.allocator, &[_]row.DecodedVersion{new_v_copy});
        errdefer self.allocator.free(packed_bytes);

        tree.delete(key) catch {};
        try tree.insert(key, packed_bytes);
        // Only count this as garbage when it actually superseded a prior version
        // (the old image went to the undo log, or this is a tombstone over a live
        // row). A fresh-key insert reaches here with `opt_val == null` and creates
        // nothing reclaimable, so it must NOT bump the counter - otherwise a bulk
        // load would keep the background vacuum busy for no reason. Signalling
        // here lets vacuum skip itself entirely on an INSERT-only workload.
        if (superseded) _ = self.garbage_ops.fetchAdd(1, .monotonic);
        return packed_bytes;
    }

    /// Inserts a user row into `sys.users` and logs the change to the WAL.
    ///
    /// `password_hash` is stored as the driver already formats it (the bootstrap
    /// path in [`Database.openInner`] writes `hex(hash):hex(salt)`). The row is
    /// built with a [`row.RowBuilder`], written through
    /// [`Database.updateRowMVCC`], and mirrored to the WAL as an insert against
    /// `sys.users` so a follower/recovery reproduces the account.
    pub fn registerUser(self: *Database, username: []const u8, password_hash: []const u8, role: []const u8, tx_id: u64) !void {
        const user_table = self.catalog.getTable("sys.users") orelse return error.SystemTableNotFound;
        const users_root = self.table_roots.get("sys.users") orelse return error.SystemTableNotFound;
        var tree = try BPlusTree.init(self.pool, users_root, self.allocator);
        defer tree.deinit();

        const fixed_buf = try self.allocator.alloc(u8, user_table.fixed_size);
        defer self.allocator.free(fixed_buf);
        @memset(fixed_buf, 0);

        const heap_capacity = username.len + password_hash.len + role.len + 32;
        const heap_buf = try self.allocator.alloc(u8, heap_capacity);
        defer self.allocator.free(heap_buf);

        var heap_offset: u32 = 0;
        var builder = row.RowBuilder.init(user_table, fixed_buf, heap_buf, &heap_offset);

        try builder.writeDynamic("username", username);
        try builder.writeDynamic("password_hash", password_hash);
        try builder.writeDynamic("role", role);

        const new_fixed = try self.allocator.dupe(u8, fixed_buf);
        errdefer self.allocator.free(new_fixed);
        const new_heap = try self.allocator.dupe(u8, heap_buf[0..heap_offset]);
        errdefer self.allocator.free(new_heap);

        const new_version = row.DecodedVersion{
            .xmin = tx_id,
            .xmax = 0,
            .fixed = new_fixed,
            .heap = new_heap,
        };
        defer {
            self.allocator.free(new_fixed);
            self.allocator.free(new_heap);
        }

        const lsn = self.reserveLsn();
        const packed_bytes = try self.updateRowMVCC(tree, username, new_version);
        defer self.allocator.free(packed_bytes);

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .insert,
                .table_name = "sys.users",
                .key = username,
                .value = packed_bytes,
            });
        }
    }

    /// Rotates an existing user's password in `sys.users`, preserving their role.
    ///
    /// Reads the live row to recover the current role string, then re-writes the
    /// row through [`Database.registerUser`] (an MVCC update keyed on username)
    /// with the new `password_hash`. Returns [`error.UserNotFound`] when there is
    /// no live version. This is the storage half of `ALTER USER ... IDENTIFIED BY`.
    pub fn updateUserPassword(self: *Database, username: []const u8, password_hash: []const u8, tx_id: u64) !void {
        const user_table = self.catalog.getTable("sys.users") orelse return error.SystemTableNotFound;
        const users_root = self.table_roots.get("sys.users") orelse return error.SystemTableNotFound;
        var tree = try BPlusTree.init(self.pool, users_root, self.allocator);
        defer tree.deinit();

        const val = (try tree.search(username, self.allocator)) orelse return error.UserNotFound;
        defer self.allocator.free(val);

        const versions = try self.reconstructVersionChain(val, self.allocator);
        defer {
            for (versions) |*v| v.deinit(self.allocator);
            self.allocator.free(versions);
        }
        if (versions.len == 0) return error.UserNotFound;
        const latest = versions[versions.len - 1];
        if (latest.xmax > 0) return error.UserNotFound;

        const reader = row.RowReader.init(user_table, latest.fixed, latest.heap);
        const role_str = try reader.readToString(self.allocator, "role");
        defer self.allocator.free(role_str);

        try self.registerUser(username, password_hash, role_str, tx_id);
    }

    /// Marks a user as deleted in `sys.users` (MVCC tombstone) and logs it.
    ///
    /// Rather than erasing the row, it finds the live version (the one with
    /// `xmax == 0`), writes a copy with `xmax = tx_id` through
    /// [`Database.updateRowMVCC`], and appends a delete WAL record. Returns
    /// [`error.UserNotFound`] if no live version exists.
    pub fn unregisterUser(self: *Database, username: []const u8, tx_id: u64) !void {
        const users_root = self.table_roots.get("sys.users") orelse return error.SystemTableNotFound;
        var tree = try BPlusTree.init(self.pool, users_root, self.allocator);
        defer tree.deinit();

        const opt_val = try tree.search(username, self.allocator);
        const val = opt_val orelse return error.UserNotFound;
        defer self.allocator.free(val);

        const versions = try self.reconstructVersionChain(val, self.allocator);
        defer {
            for (versions) |*v| v.deinit(self.allocator);
            self.allocator.free(versions);
        }

        var active_version: ?row.DecodedVersion = null;
        for (versions) |v| {
            if (v.xmax == 0) {
                active_version = v;
                break;
            }
        }

        const act = active_version orelse return error.UserNotFound;

        var new_v = row.DecodedVersion{
            .xmin = act.xmin,
            .xmax = tx_id,
            .fixed = try self.allocator.dupe(u8, act.fixed),
            .heap = try self.allocator.dupe(u8, act.heap),
        };
        defer new_v.deinit(self.allocator);

        const lsn = self.reserveLsn();
        const packed_bytes = try self.updateRowMVCC(tree, username, new_v);
        defer self.allocator.free(packed_bytes);

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .delete,
                .table_name = "sys.users",
                .key = username,
                .value = packed_bytes,
            });
        }
    }

    /// Defines a single-column foreign key and persists it as a constraint.
    ///
    /// Validates that both tables and columns exist and that their types match
    /// ([`error.ForeignKeyTypeMismatch`]), registers a [`types.ForeignKey`] in
    /// the in-memory catalog, then stores the constraint in `sys.constraints`
    /// as a row whose `definition` column is the JSON-serialised
    /// [`types.ForeignKeyDefinition`] (so [`Database.loadCatalog`] can rebuild
    /// it on the next open) and logs the insert to the WAL. Referential actions
    /// are fixed at `NO_ACTION`.
    pub fn createForeignKey(
        self: *Database,
        name: []const u8,
        child_table_name: []const u8,
        parent_table_name: []const u8,
        child_col_name: []const u8,
        parent_col_name: []const u8,
        tx_id: u64,
    ) !void {
        const child_table = self.catalog.getTable(child_table_name) orelse return error.TableNotFound;
        const parent_table = self.catalog.getTable(parent_table_name) orelse return error.TableNotFound;
        const child_col = child_table.getColumn(child_col_name) orelse return error.ColumnNotFound;
        const parent_col = parent_table.getColumn(parent_col_name) orelse return error.ColumnNotFound;

        if (child_col.type != parent_col.type) {
            return error.ForeignKeyTypeMismatch;
        }

        var child_cols = try self.allocator.alloc(types.Column, 1);
        child_cols[0] = types.Column{
            .name = try self.allocator.dupe(u8, child_col.name),
            .type = child_col.type,
            .size = child_col.size,
            .offset = child_col.offset,
            .is_primary_key = child_col.is_primary_key,
            .is_auto_increment = child_col.is_auto_increment,
            .is_nullable = child_col.is_nullable,
            .default_value = if (child_col.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
        };

        var parent_cols = try self.allocator.alloc(types.Column, 1);
        parent_cols[0] = types.Column{
            .name = try self.allocator.dupe(u8, parent_col.name),
            .type = parent_col.type,
            .size = parent_col.size,
            .offset = parent_col.offset,
            .is_primary_key = parent_col.is_primary_key,
            .is_auto_increment = parent_col.is_auto_increment,
            .is_nullable = parent_col.is_nullable,
            .default_value = if (parent_col.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
        };

        const fk_id = @as(u32, @intCast(self.catalog.foreign_keys.items.len + 1));
        const fk = try types.ForeignKey.init(
            self.allocator,
            fk_id,
            name,
            child_table.id,
            parent_table.id,
            child_cols,
            parent_cols,
            .NO_ACTION,
            .NO_ACTION,
        );
        try self.catalog.addForeignKey(fk);

        const def = types.ForeignKeyDefinition{
            .table_name = child_table_name,
            .referenced_table_name = parent_table_name,
            .columns = &.{child_col_name},
            .referenced_columns = &.{parent_col_name},
        };

        const def_json = try std.fmt.allocPrint(self.allocator, "{f}", .{std.json.fmt(def, .{})});
        defer self.allocator.free(def_json);

        const const_table = self.catalog.getTable("sys.constraints") orelse return error.SystemTableNotFound;
        const const_root = self.table_roots.get("sys.constraints") orelse return error.SystemTableNotFound;
        var tree = try BPlusTree.init(self.pool, const_root, self.allocator);
        defer tree.deinit();

        const fixed_buf = try self.allocator.alloc(u8, const_table.fixed_size);
        defer self.allocator.free(fixed_buf);
        @memset(fixed_buf, 0);

        const heap_capacity = name.len + 16 + def_json.len + 32;
        const heap_buf = try self.allocator.alloc(u8, heap_capacity);
        defer self.allocator.free(heap_buf);

        var heap_offset: u32 = 0;
        var builder = row.RowBuilder.init(const_table, fixed_buf, heap_buf, &heap_offset);

        try builder.writeDynamic("name", name);
        try builder.writeDynamic("type", "FOREIGN_KEY");
        try builder.writeDynamic("definition", def_json);

        var versions_list = std.ArrayList(row.DecodedVersion).empty;
        defer versions_list.deinit(self.allocator);

        const new_fixed = try self.allocator.dupe(u8, fixed_buf);
        errdefer self.allocator.free(new_fixed);
        const new_heap = try self.allocator.dupe(u8, heap_buf[0..heap_offset]);

        try versions_list.append(self.allocator, row.DecodedVersion{
            .xmin = tx_id,
            .xmax = 0,
            .fixed = new_fixed,
            .heap = new_heap,
        });

        const lsn = self.reserveLsn();
        const packed_bytes = try row.packVersions(self.allocator, versions_list.items);
        defer self.allocator.free(packed_bytes);

        _ = try tree.insert(name, packed_bytes);

        for (versions_list.items) |*v| {
            v.deinit(self.allocator);
        }

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .insert,
                .table_name = "sys.constraints",
                .key = name,
                .value = packed_bytes,
            });
        }
    }

    /// Redoes one catalog (`sys.*`) WAL record during recovery phase 1.
    ///
    /// `sys.objects` records go straight to the master tree; other system-table
    /// records go to the corresponding cached table tree. Insert and update both
    /// collapse to delete-then-insert (idempotent redo); delete removes the key.
    /// Called only for records of committed transactions by [`Database.recoverTo`].
    fn applyCatalogRecord(self: *Database, rec: LogRecord) !void {
        if (std.mem.eql(u8, rec.table_name, "sys.objects")) {
            switch (rec.kind) {
                .insert, .update => {
                    _ = self.master_tree.delete(rec.key) catch {};
                    try self.master_tree.insert(rec.key, rec.value);
                },
                .delete => {
                    _ = self.master_tree.delete(rec.key) catch {};
                },
                else => {},
            }
        } else {
            const table_tree = self.getTableTree(rec.table_name) catch |err| {
                dblog.debug("Phase 1 getTableTree failed for '{s}': {s}", .{ rec.table_name, @errorName(err) });
                return err;
            };
            defer table_tree.deinit();
            switch (rec.kind) {
                .insert, .update => {
                    _ = table_tree.delete(rec.key) catch {};
                    try table_tree.insert(rec.key, rec.value);
                },
                .delete => {
                    _ = table_tree.delete(rec.key) catch {};
                },
                else => {},
            }
        }
    }

    /// Redoes one user-table DML WAL record and rebuilds affected index entries.
    ///
    /// Applies the row change to the table tree (delete-then-insert for
    /// insert/update, tombstone insert for delete), then for every index on that
    /// table re-derives the composite index key (`col:col:...:pk`) from each
    /// visible version whose `xmin` is in `committed_txns` and not deleted, and
    /// writes it into the index tree. Used in recovery phase 2 and by follower
    /// stream application; a missing table tree is logged and skipped, not fatal.
    fn applyDmlRecord(self: *Database, rec: LogRecord, committed_txns: *std.AutoHashMap(u64, void)) !void {
        switch (rec.kind) {
            .insert, .update => {
                const table_tree = self.getTableTree(rec.table_name) catch |err| {
                    std.log.err("WAL recovery: failed to find table tree '{s}': {s}", .{ rec.table_name, @errorName(err) });
                    return;
                };
                defer table_tree.deinit();

                _ = table_tree.delete(rec.key) catch {};
                try table_tree.insert(rec.key, rec.value);

                const table_meta = for (self.catalog.tables.items) |tbl| {
                    if (std.mem.eql(u8, tbl.name, rec.table_name)) break tbl;
                } else return;

                for (self.catalog.indexes.items) |idx| {
                    if (idx.table_id == table_meta.id) {
                        const idx_tree = self.getIndexTree(idx.name) catch continue;
                        defer idx_tree.deinit();

                        const versions = try self.reconstructVersionChain(rec.value, self.allocator);
                        defer {
                            for (versions) |*v| v.deinit(self.allocator);
                            self.allocator.free(versions);
                        }

                        for (versions) |v| {
                            const xmin = v.xmin;
                            const xmax = v.xmax;

                            if (committed_txns.contains(@intCast(xmin))) {
                                const is_deleted = if (xmax > 0) committed_txns.contains(@intCast(xmax)) else false;
                                if (!is_deleted) {
                                    var list = std.ArrayList([]const u8).empty;
                                    defer {
                                        for (list.items) |item| self.allocator.free(item);
                                        list.deinit(self.allocator);
                                    }

                                    const reader = row.RowReader.init(table_meta, v.fixed, v.heap);
                                    for (idx.key_columns) |col| {
                                        const raw_val = reader.readToString(self.allocator, col.name) catch |err| {
                                            std.log.err("WAL recovery: failed to read index col '{s}': {s}", .{ col.name, @errorName(err) });
                                            continue;
                                        };
                                        defer self.allocator.free(raw_val);
                                        const ct = colTypeByNameIn(table_meta, col.name);
                                        const str_val = try types.encodeIndexValueAlloc(self.allocator, ct, raw_val);
                                        try list.append(self.allocator, str_val);
                                    }

                                    try list.append(self.allocator, try self.allocator.dupe(u8, rec.key));

                                    var key_parts = try self.allocator.alloc([]const u8, list.items.len);
                                    defer self.allocator.free(key_parts);
                                    for (list.items, 0..) |part, idx_col| key_parts[idx_col] = part;

                                    const index_key = try std.mem.join(self.allocator, ":", key_parts);
                                    defer self.allocator.free(index_key);

                                    _ = idx_tree.delete(index_key) catch {};
                                    try idx_tree.insert(index_key, "");
                                }
                            }
                        }
                    }
                }
            },
            .delete => {
                const table_tree = self.getTableTree(rec.table_name) catch return;
                defer table_tree.deinit();

                _ = table_tree.delete(rec.key) catch {};
                try table_tree.insert(rec.key, rec.value);
            },
            else => {},
        }
    }

    /// Recreates a table on a follower from a leader's `sys.tables` insert.
    ///
    /// Deserialises the [`types.TableMetadata`], records the leader's table id →
    /// name mapping in `tid_name` (so a later index record can resolve the table
    /// by the leader's id, which need not match this replica's), and calls
    /// [`Database.createTable`] unless a table of that name already exists.
    fn applyTableCreateFromLeader(self: *Database, rec: LogRecord, tid_name: *std.AutoHashMap(u32, []const u8)) !void {
        const meta = try types.TableMetadata.deserialize(self.allocator, rec.value);
        defer {
            for (meta.columns) |c| {
                self.allocator.free(c.name);
                if (c.default_value) |dv| self.allocator.free(dv);
            }
            self.allocator.free(meta.columns);
            self.allocator.free(meta.name);
        }

        if (!tid_name.contains(meta.id)) {
            try tid_name.put(meta.id, try self.allocator.dupe(u8, meta.name));
        }

        for (self.catalog.tables.items) |t| {
            if (std.mem.eql(u8, t.name, meta.name)) return;
        }

        const cols = try self.allocator.alloc(types.Column, meta.columns.len);
        for (meta.columns, 0..) |cm, i| {
            cols[i] = .{
                .name = try self.allocator.dupe(u8, cm.name),
                .type = cm.type,
                .size = cm.size,
                .offset = cm.offset,
                .is_primary_key = cm.is_primary_key,
                .is_auto_increment = cm.is_auto_increment,
                .is_nullable = cm.is_nullable,
                .default_value = if (cm.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
            };
        }
        _ = try self.createTable(meta.name, cols, rec.tx_id);
    }

    /// Recreates an index on a follower from a leader's `sys.indexes` insert.
    ///
    /// Resolves the owning table via the leader-id → name map built by
    /// [`Database.applyTableCreateFromLeader`] (warning and skipping if the id is
    /// unknown), then calls [`Database.createIndex`]. Skips indexes that already
    /// exist by name.
    fn applyIndexCreateFromLeader(self: *Database, rec: LogRecord, tid_name: *std.AutoHashMap(u32, []const u8)) !void {
        const meta = try types.IndexMetadata.deserialize(self.allocator, rec.value);
        defer {
            self.allocator.free(meta.name);
            for (meta.key_columns) |c| {
                self.allocator.free(c.name);
                if (c.default_value) |dv| self.allocator.free(dv);
            }
            self.allocator.free(meta.key_columns);
            if (meta.value_columns) |vc| {
                for (vc) |c| {
                    self.allocator.free(c.name);
                    if (c.default_value) |dv| self.allocator.free(dv);
                }
                self.allocator.free(vc);
            }
        }

        for (self.catalog.indexes.items) |ix| {
            if (std.mem.eql(u8, ix.name, meta.name)) return;
        }

        const table_name = tid_name.get(meta.table_id) orelse {
            std.log.warn("follower applyStream: index '{s}' references unknown leader table_id={d}", .{ meta.name, meta.table_id });
            return;
        };

        const kcols = try self.allocator.alloc(types.Column, meta.key_columns.len);
        for (meta.key_columns, 0..) |cm, i| {
            kcols[i] = .{
                .name = try self.allocator.dupe(u8, cm.name),
                .type = cm.type,
                .size = cm.size,
                .offset = cm.offset,
                .is_primary_key = cm.is_primary_key,
                .is_auto_increment = cm.is_auto_increment,
                .is_nullable = cm.is_nullable,
                .default_value = if (cm.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
            };
        }

        var vcols: ?[]types.Column = null;
        if (meta.value_columns) |vc| {
            const vv = try self.allocator.alloc(types.Column, vc.len);
            for (vc, 0..) |cm, i| {
                vv[i] = .{
                    .name = try self.allocator.dupe(u8, cm.name),
                    .type = cm.type,
                    .size = cm.size,
                    .offset = cm.offset,
                    .is_primary_key = cm.is_primary_key,
                    .is_auto_increment = cm.is_auto_increment,
                    .is_nullable = cm.is_nullable,
                    .default_value = if (cm.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                };
            }
            vcols = vv;
        }

        _ = try self.createIndex(meta.name, table_name, meta.kind, kcols, vcols, rec.tx_id);
    }


    /// Applies a batch of leader WAL records on a follower and returns the
    /// highest committed transaction id in the batch.
    ///
    /// The apply order is deliberate and layered: first the committed set is
    /// computed and merged into the transaction manager; then table creates, then
    /// index creates (DDL before the DML that depends on it); then the catalog is
    /// fully reloaded and tree caches rebuilt; then committed non-system DML is
    /// applied via [`Database.applyDmlRecord`]; finally a commit record per
    /// committed transaction is written to this node's own WAL and everything is
    /// made durable. Only records belonging to committed transactions take effect.
    pub fn applyStream(self: *Database, records: []const LogRecord) !u64 {
        var committed_txns = std.AutoHashMap(u64, void).init(self.allocator);
        defer committed_txns.deinit();
        var max_committed: u64 = 0;
        for (records) |rec| {
            if (rec.kind == .commit) {
                try committed_txns.put(rec.tx_id, {});
                if (rec.tx_id > max_committed) max_committed = rec.tx_id;
            }
        }

        {
            self.txn_manager.mutex.lockUncancelable(self.pool.pager.io);
            defer self.txn_manager.mutex.unlock(self.pool.pager.io);
            var it = committed_txns.keyIterator();
            while (it.next()) |tx_id| {
                try self.txn_manager.committed_txns.put(tx_id.*, {});
                if (tx_id.* >= self.txn_manager.next_tx_id) self.txn_manager.next_tx_id = tx_id.* + 1;
            }
        }

        var tid_name = std.AutoHashMap(u32, []const u8).init(self.allocator);
        defer {
            var it = tid_name.valueIterator();
            while (it.next()) |v| self.allocator.free(v.*);
            tid_name.deinit();
        }

        for (records) |rec| {
            if (!committed_txns.contains(rec.tx_id)) continue;
            if (rec.kind == .insert and std.mem.eql(u8, rec.table_name, "sys.tables")) {
                self.applyTableCreateFromLeader(rec, &tid_name) catch |err| {
                    std.log.warn("follower applyStream: create table from '{s}' failed: {s}", .{ rec.key, @errorName(err) });
                };
            }
        }

        for (records) |rec| {
            if (!committed_txns.contains(rec.tx_id)) continue;
            if (rec.kind == .insert and std.mem.eql(u8, rec.table_name, "sys.indexes")) {
                self.applyIndexCreateFromLeader(rec, &tid_name) catch |err| {
                    std.log.warn("follower applyStream: create index from '{s}' failed: {s}", .{ rec.key, @errorName(err) });
                };
            }
        }

        self.catalog.deinit();
        self.catalog = catalog.SystemCatalog.init(self.allocator);
        {
            var table_it = self.table_roots.keyIterator();
            while (table_it.next()) |k| self.allocator.free(k.*);
            self.table_roots.clearRetainingCapacity();
            var index_it = self.index_roots.keyIterator();
            while (index_it.next()) |k| self.allocator.free(k.*);
            self.index_roots.clearRetainingCapacity();
        }
        try self.loadCatalog();
        try self.populateTreeCaches();

        for (records) |rec| {
            if (committed_txns.contains(rec.tx_id) and !std.mem.startsWith(u8, rec.table_name, "sys.")) {
                try self.applyDmlRecord(rec, &committed_txns);
            }
        }

        if (self.wal) |wal| {
            var cit = committed_txns.keyIterator();
            while (cit.next()) |tx_id| {
                const lsn = wal.incrementLSN();
                try wal.append(.{
                    .lsn = lsn,
                    .tx_id = tx_id.*,
                    .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                    .kind = .commit,
                    .table_name = "",
                    .key = "",
                    .value = "",
                });
            }
            try wal.flush();
        }

        try self.durableFlush();

        return max_committed;
    }

    /// Allocates the next log sequence number and advances the pool's high-water
    /// mark to it.
    ///
    /// Returns 0 when there is no WAL (durability disabled). The pool tracks the
    /// current LSN so its `wal_gate` can decide when a dirty page is already
    /// covered by a flushed record. Callers reserve the LSN *before* mutating so
    /// the WAL record and the page stamp agree.
    pub fn reserveLsn(self: *Database) u64 {
        const lsn = if (self.wal) |wal| wal.incrementLSN() else 0;
        _ = self.pool.current_lsn.fetchMax(lsn, .monotonic);
        return lsn;
    }

    /// Returns the [`GroupLock`] for a table, creating it on first request.
    ///
    /// Locks are interned by table name so all sessions share one lock per table;
    /// the returned pointer is stable for the database's lifetime. The map
    /// insertion is serialised by `table_locks_mutex`. This is the fine-grained
    /// access lock the SQL executor takes (read for SELECT, write for INSERT,
    /// exclusive for UPDATE/DELETE), distinct from the database-wide
    /// [`Database.rw_lock`].
    pub fn tableLock(self: *Database, name: []const u8) !*GroupLock {
        self.table_locks_mutex.lockUncancelable(self.pool.pager.io);
        defer self.table_locks_mutex.unlock(self.pool.pager.io);
        if (self.table_locks.get(name)) |l| return l;
        const l = try self.allocator.create(GroupLock);
        errdefer self.allocator.destroy(l);
        l.* = .{};
        const key = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(key);
        try self.table_locks.put(key, l);
        return l;
    }

    /// Repairs catalog objects whose recorded root page lies past the end of file.
    ///
    /// A crash can leave a `sys.objects` entry pointing at a root page that was
    /// never actually persisted (root id >= `num_pages`). Reading such a tree
    /// would fault, so each affected non-system table/index is given a fresh empty
    /// root, its `sys.objects` metadata is rewritten, and recovery then rebuilds
    /// its contents from the WAL. Returns true if anything was re-rooted, which
    /// signals [`Database.recoverTo`] to repopulate the tree caches.
    fn rerootBeyondEofObjects(self: *Database) !bool {
        const num_pages = self.pool.pager.num_pages;
        var rerooted = false;

        var names = std.ArrayList([]const u8).empty;
        defer {
            for (names.items) |n| self.allocator.free(n);
            names.deinit(self.allocator);
        }
        for ([_]*std.StringHashMap(u64){ &self.table_roots, &self.index_roots, &self.collection_roots }) |map| {
            var it = map.iterator();
            while (it.next()) |e| {
                if (std.mem.startsWith(u8, e.key_ptr.*, "sys.")) continue;
                if (e.value_ptr.* >= num_pages) try names.append(self.allocator, try self.allocator.dupe(u8, e.key_ptr.*));
            }
        }

        for (names.items) |name| {
            const fresh = try BPlusTree.create(self.pool, self.allocator);
            const new_root = fresh.root_page_id;
            fresh.deinit();

            if (self.table_roots.getEntry(name)) |ent| ent.value_ptr.* = new_root;
            if (self.index_roots.getEntry(name)) |ent| ent.value_ptr.* = new_root;
            if (self.collection_roots.getEntry(name)) |ent| ent.value_ptr.* = new_root;

            if (try self.master_tree.search(name, self.allocator)) |obj_bytes| {
                defer self.allocator.free(obj_bytes);
                var obj = try types.ObjectMetadata.deserialize(self.allocator, obj_bytes);
                defer {
                    self.allocator.free(obj.name);
                    self.allocator.free(obj.type);
                }
                obj.root_page_id = new_root;
                const new_bytes = try obj.serialize(self.allocator);
                defer self.allocator.free(new_bytes);
                _ = self.master_tree.delete(name) catch {};
                try self.master_tree.insert(name, new_bytes);
            }
            rerooted = true;
            std.log.warn("recovery: object '{s}' root was beyond EOF; re-rooted fresh, rebuilding from the WAL", .{name});
        }
        return rerooted;
    }

    /// Full crash recovery: replays the entire WAL. Equivalent to
    /// [`Database.recoverTo`] with no LSN ceiling.
    pub fn recover(self: *Database) !void {
        return self.recoverTo(0);
    }

    /// Replays the WAL up to `target_lsn` (0 = no limit) in three passes.
    ///
    /// After classifying every record into committed / active transaction sets,
    /// it runs: **phase 1**, redo committed catalog (`sys.*`) records, then
    /// reload the catalog and re-root any beyond-EOF objects; **phase 2**, redo
    /// committed user-table DML (and rebuild indexes); **phase 3**, walk the log
    /// *backwards* and undo every change made by transactions that were still
    /// active at the crash, reversing inserts (delete the key) and restoring the
    /// pre-image for updates/deletes, including reconstructing the surviving MVCC
    /// version set for user rows. The `target_lsn` ceiling is what makes
    /// point-in-time recovery via [`Database.openAt`] possible.
    pub fn recoverTo(self: *Database, target_lsn: u64) !void {
        const wal = self.wal orelse return;

        try self.populateTreeCaches();

        try self.ensureSystemCatalogTables();
        try self.populateTreeCaches();

        // (Committed-txn visibility is restored unconditionally in `openInner`
        // via `loadCommitState`, before this replay runs.)

        var replay_res = try wal.replay();
        defer {
            replay_res.arena.deinit();
            std.heap.page_allocator.destroy(replay_res.arena);
        }

        var committed_txns = std.AutoHashMap(u64, void).init(self.allocator);
        defer committed_txns.deinit();
        var active_txns = std.AutoHashMap(u64, void).init(self.allocator);
        defer active_txns.deinit();

        for (replay_res.records) |rec| {
            if (target_lsn != 0 and rec.lsn > target_lsn) continue;
            switch (rec.kind) {
                .begin => {
                    try active_txns.put(rec.tx_id, {});
                },
                .commit => {
                    try committed_txns.put(rec.tx_id, {});
                    _ = active_txns.remove(rec.tx_id);
                },
                .rollback => {
                    _ = active_txns.remove(rec.tx_id);
                },
                else => {},
            }
        }

        {
            self.txn_manager.mutex.lockUncancelable(self.pool.pager.io);
            defer self.txn_manager.mutex.unlock(self.pool.pager.io);
            var it = committed_txns.keyIterator();
            while (it.next()) |tx_id| {
                try self.txn_manager.committed_txns.put(tx_id.*, {});
                if (tx_id.* >= self.txn_manager.next_tx_id) {
                    self.txn_manager.next_tx_id = tx_id.* + 1;
                }
            }
            var active_it = active_txns.keyIterator();
            while (active_it.next()) |tx_id| {
                if (tx_id.* >= self.txn_manager.next_tx_id) {
                    self.txn_manager.next_tx_id = tx_id.* + 1;
                }
            }
        }

        dblog.debug("Starting Phase 1...", .{});
        for (replay_res.records) |rec| {
            if (target_lsn != 0 and rec.lsn > target_lsn) continue;
            if (committed_txns.contains(rec.tx_id) and std.mem.startsWith(u8, rec.table_name, "sys.")) {
                dblog.debug("Phase 1 replaying master key={s} kind={any}", .{rec.key, rec.kind});
                try self.applyCatalogRecord(rec);
            }
        }

        dblog.debug("Loading catalog...", .{});
        self.catalog.deinit();
        self.catalog = catalog.SystemCatalog.init(self.allocator);
        
        var table_it = self.table_roots.keyIterator();
        while (table_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.table_roots.clearRetainingCapacity();

        var index_it = self.index_roots.keyIterator();
        while (index_it.next()) |k| {
            self.allocator.free(k.*);
        }
        self.index_roots.clearRetainingCapacity();

        try self.loadCatalog();
        try self.populateTreeCaches();

        if (try self.rerootBeyondEofObjects()) {
            try self.populateTreeCaches();
        }

        dblog.debug("Starting Phase 2...", .{});
        for (replay_res.records) |rec| {
            if (target_lsn != 0 and rec.lsn > target_lsn) continue;
            if (committed_txns.contains(rec.tx_id) and !std.mem.startsWith(u8, rec.table_name, "sys.")) {
                dblog.debug("Phase 2 replaying table={s} key={s} kind={any}", .{rec.table_name, rec.key, rec.kind});
                try self.applyDmlRecord(rec, &committed_txns);
            }
        }

        dblog.debug("Starting Phase 3...", .{});
        var u_idx: usize = replay_res.records.len;
        while (u_idx > 0) {
            u_idx -= 1;
            const rec = replay_res.records[u_idx];
            if (target_lsn != 0 and rec.lsn > target_lsn) continue;
            if (active_txns.contains(rec.tx_id)) {
                dblog.debug("Phase 3 reverting active tx={d} table={s} key={s} kind={any}", .{rec.tx_id, rec.table_name, rec.key, rec.kind});
                if (std.mem.startsWith(u8, rec.table_name, "sys.")) {
                    if (std.mem.eql(u8, rec.table_name, "sys.objects")) {
                        switch (rec.kind) {
                            .insert => {
                                _ = self.master_tree.delete(rec.key) catch {};
                            },
                            .update, .delete => {
                                try self.master_tree.insert(rec.key, rec.value);
                            },
                            else => {},
                        }
                    } else {
                        const table_tree = self.getTableTree(rec.table_name) catch continue;
                        defer table_tree.deinit();
                        switch (rec.kind) {
                            .insert => {
                                _ = table_tree.delete(rec.key) catch {};
                            },
                            .update, .delete => {
                                try table_tree.insert(rec.key, rec.value);
                            },
                            else => {},
                        }
                    }
                } else {
                    switch (rec.kind) {
                        .insert => {
                            const table_tree = self.getTableTree(rec.table_name) catch continue;
                            defer table_tree.deinit();
                            _ = table_tree.delete(rec.key) catch {};
                        },
                        .update, .delete => {
                            const table_tree = self.getTableTree(rec.table_name) catch continue;
                            defer table_tree.deinit();

                            const opt_val = try table_tree.search(rec.key, self.allocator);
                            if (opt_val) |val| {
                                defer self.allocator.free(val);

                                const versions = try self.reconstructVersionChain(val, self.allocator);
                                defer {
                                    for (versions) |*v| v.deinit(self.allocator);
                                    self.allocator.free(versions);
                                }

                                var list = std.ArrayList(row.DecodedVersion).empty;
                                defer list.deinit(self.allocator);

                                for (versions) |v| {
                                    if (v.xmin == rec.tx_id) continue;
                                    var v_copy = v;
                                    v_copy.fixed = try self.allocator.dupe(u8, v.fixed);
                                    v_copy.heap = try self.allocator.dupe(u8, v.heap);
                                    if (v_copy.xmax == rec.tx_id) v_copy.xmax = 0;
                                    try list.append(self.allocator, v_copy);
                                }

                                if (list.items.len == 0) {
                                    _ = try table_tree.delete(rec.key);
                                } else {
                                    const packed_bytes = try row.packVersions(self.allocator, list.items);
                                    defer self.allocator.free(packed_bytes);
                                    try table_tree.insert(rec.key, packed_bytes);
                                }

                                for (list.items) |*v| {
                                    v.deinit(self.allocator);
                                }
                            }
                        },
                        else => {},
                    }
                }
            }
        }
        dblog.debug("Recover done.", .{});
    }

    /// Persists an up-to-date page-0 header and flushes all pages to disk.
    ///
    /// Rewrites the header's master-tree root, LSN and free-list head, then does a
    /// fast flush of every page. Used after applying a replication stream so the
    /// follower's file is self-consistent without a full checkpoint. Unlike
    /// [`Database.close`] it does not tear the database down.
    pub fn durableFlush(self: *Database) !void {
        {
            const hf = try self.pool.fetchPage(0);
            defer self.pool.unpinPage(0, true);
            const hp = self.pool.pageOf(hf);
            var hdr = readHeader(hp.data);
            hdr.root_page_id = self.master_tree.root_page_id;
            hdr.lsn = self.master_tree.lsn;
            var scratch_buf: [PAGE_SIZE]u8 = undefined;
            hdr.free_page_list_head = self.pool.pager.persistFreeList(&scratch_buf) catch 0;
            writeHeader(hp.data, &hdr);
        }
        try self.pool.flushAllPagesFast();
    }


    /// Rejects a write from a fenced-off (stale) leader.
    ///
    /// Returns [`error.FencedWrite`] when this node's [`Database.fencing_epoch`]
    /// is behind the highest epoch it has ever seen: a newer leader has taken
    /// over, so this node must not mutate data. Called on the write path to
    /// prevent split-brain corruption after a failover.
    pub fn guardWrite(self: *Database) !void {
        if (self.max_epoch_seen > 0 and self.fencing_epoch < self.max_epoch_seen) {
            return error.FencedWrite;
        }
    }

    /// Promotes this node to write at `epoch`, updating and persisting the
    /// high-water epoch if this is the newest seen.
    ///
    /// Sets [`Database.fencing_epoch`] so [`Database.guardWrite`] will allow
    /// writes; a persisted [`Database.max_epoch_seen`] means a later restart of
    /// an even-older leader still refuses to write.
    pub fn setWriteEpoch(self: *Database, epoch: u64) !void {
        self.fencing_epoch = epoch;
        if (epoch > self.max_epoch_seen) {
            self.max_epoch_seen = epoch;
            try self.persistFence();
        }
    }

    /// Records a higher fencing epoch observed from a peer (e.g. from a shipped
    /// record or heartbeat) without granting this node write rights.
    ///
    /// Raising [`Database.max_epoch_seen`] here is what later causes
    /// [`Database.guardWrite`] to fence this node until it is legitimately
    /// promoted with [`Database.setWriteEpoch`].
    pub fn observeEpoch(self: *Database, epoch: u64) !void {
        if (epoch > self.max_epoch_seen) {
            self.max_epoch_seen = epoch;
            try self.persistFence();
        }
    }

    /// The directory used to persist the fence epoch sidecar, i.e. the WAL
    /// directory, or empty when there is no WAL.
    fn fenceDir(self: *Database) []const u8 {
        if (self.wal) |w| return w.dir_path;
        return "";
    }

    /// Writes a consistent physical copy of the database into `dest_dir`.
    ///
    /// Flushes the WAL and all pages and fsyncs first, then copies the whole page
    /// file to `dest_dir/snapshot.db` page by page and copies every WAL segment
    /// into `dest_dir/wal`. Because it snapshots the raw file plus the WAL, a
    /// restore ([`Database.restoreSnapshot`]) reproduces the exact durable state,
    /// including any not-yet-checkpointed changes recoverable from the log. Used
    /// to seed a fresh follower.
    pub fn exportSnapshot(self: *Database, dest_dir: []const u8) !void {
        const io = self.pool.pager.io;
        if (self.wal) |w| w.flush() catch {};

        // Rewrite the page-0 header from the LIVE master tree before copying pages.
        // The stored `root_page_id` / `lsn` / free-list head are only refreshed at a
        // checkpoint or a clean close, so a snapshot taken between checkpoints would
        // otherwise copy a STALE catalog root: the data pages are all present but the
        // header points at an older catalog, so the restored database opens missing any
        // table / rows created since the last checkpoint (recovery does not re-run when
        // the checkpoint marker says the pages are already current). Same header-currency
        // requirement as `checkpoint` / `durableFlush`. Safe here: the hot `BACKUP
        // DATABASE TO` path holds the executor's exclusive `rw_lock` (so no writer can
        // allocate a page while `persistFreeList` writes its chain), and the cold
        // `kaidb backup` CLI has its own single-threaded handle. Do NOT call
        // `checkpoint()` here: it re-takes `rw_lock`, which the hot path already holds
        // and which is not reentrant.
        {
            const hf = try self.pool.fetchPage(0);
            defer self.pool.unpinPage(0, true);
            const hp = self.pool.pageOf(hf);
            var hdr = readHeader(hp.data);
            hdr.root_page_id = self.master_tree.root_page_id;
            hdr.lsn = self.master_tree.lsn;
            var scratch_buf: [PAGE_SIZE]u8 = undefined;
            hdr.free_page_list_head = self.pool.pager.persistFreeList(&scratch_buf) catch |err| blk: {
                std.log.err("persistFreeList during exportSnapshot failed: {any}", .{err});
                break :blk hdr.free_page_list_head;
            };
            writeHeader(hp.data, &hdr);
        }

        try self.pool.flushAllPages();
        try self.pool.pager.file.sync(io);

        std.Io.Dir.createDirPath(.cwd(), io, dest_dir) catch |e| {
            if (e != error.PathAlreadyExists) return e;
        };

        const snap_path = try std.fmt.allocPrint(self.allocator, "{s}/snapshot.db", .{dest_dir});
        defer self.allocator.free(snap_path);
        const snap = try std.Io.Dir.createFile(.cwd(), io, snap_path, .{ .truncate = true });
        defer snap.close(io);
        var page_buf: [PAGE_SIZE]u8 = undefined;
        var pid: u64 = 0;
        while (pid < self.pool.pager.num_pages) : (pid += 1) {
            const off = pid * PAGE_SIZE;
            _ = try self.pool.pager.file.readPositionalAll(io, &page_buf, off);
            try snap.writePositionalAll(io, &page_buf, off);
        }
        try snap.sync(io);

        if (self.wal) |w| {
            const wal_dest = try std.fmt.allocPrint(self.allocator, "{s}/wal", .{dest_dir});
            defer self.allocator.free(wal_dest);
            std.Io.Dir.createDirPath(.cwd(), io, wal_dest) catch |e| {
                if (e != error.PathAlreadyExists) return e;
            };
            var src = std.Io.Dir.openDir(.cwd(), io, w.dir_path, .{ .iterate = true }) catch return;
            defer src.close(io);
            var it = src.iterate();
            while (it.next(io) catch null) |entry| {
                if (entry.kind != .file) continue;
                const sp = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ w.dir_path, entry.name });
                defer self.allocator.free(sp);
                const dp = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ wal_dest, entry.name });
                defer self.allocator.free(dp);
                const b = std.Io.Dir.readFileAlloc(.cwd(), io, sp, self.allocator, .unlimited) catch continue;
                defer self.allocator.free(b);
                const of = try std.Io.Dir.createFile(.cwd(), io, dp, .{ .truncate = true });
                defer of.close(io);
                try of.writeStreamingAll(io, b);
                try of.sync(io);
            }
        }
    }

    /// Restores a database file (and WAL) from a snapshot directory, offline.
    ///
    /// A free function (no open [`Database`]): it copies `snapshot_dir/snapshot.db`
    /// to `db_path` and, if `wal_dir` is non-empty, every WAL segment from
    /// `snapshot_dir/wal` into it. The caller then [`Database.open`]s `db_path`,
    /// which replays the copied WAL. The inverse of [`Database.exportSnapshot`].
    pub fn restoreSnapshot(allocator: Allocator, io: std.Io, snapshot_dir: []const u8, db_path: []const u8, wal_dir: []const u8) !void {
        const snap_path = try std.fmt.allocPrint(allocator, "{s}/snapshot.db", .{snapshot_dir});
        defer allocator.free(snap_path);
        const bytes = try std.Io.Dir.readFileAlloc(.cwd(), io, snap_path, allocator, .unlimited);
        defer allocator.free(bytes);
        const out = try std.Io.Dir.createFile(.cwd(), io, db_path, .{ .truncate = true });
        defer out.close(io);
        try out.writeStreamingAll(io, bytes);
        try out.sync(io);

        if (wal_dir.len == 0) return;
        std.Io.Dir.createDirPath(.cwd(), io, wal_dir) catch |e| {
            if (e != error.PathAlreadyExists) return e;
        };
        const wal_src = try std.fmt.allocPrint(allocator, "{s}/wal", .{snapshot_dir});
        defer allocator.free(wal_src);
        var src = std.Io.Dir.openDir(.cwd(), io, wal_src, .{ .iterate = true }) catch return;
        defer src.close(io);
        var it = src.iterate();
        while (it.next(io) catch null) |entry| {
            if (entry.kind != .file) continue;
            const sp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_src, entry.name });
            defer allocator.free(sp);
            const dp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wal_dir, entry.name });
            defer allocator.free(dp);
            const b = std.Io.Dir.readFileAlloc(.cwd(), io, sp, allocator, .unlimited) catch continue;
            defer allocator.free(b);
            const of = try std.Io.Dir.createFile(.cwd(), io, dp, .{ .truncate = true });
            defer of.close(io);
            try of.writeStreamingAll(io, b);
            try of.sync(io);
        }
    }

    /// Exports a snapshot plus a `snapshot_meta.json` recording the replication
    /// sequence it was taken at.
    ///
    /// The extra metadata lets a re-syncing follower ([`Database.restoreSnapshotResync`])
    /// resume streaming from exactly the point the snapshot represents, avoiding
    /// gaps or replays.
    pub fn exportSnapshotForResync(self: *Database, dest_dir: []const u8, repl_seq: u64) !void {
        try self.exportSnapshot(dest_dir);
        const io = self.pool.pager.io;
        const meta_path = try std.fmt.allocPrint(self.allocator, "{s}/snapshot_meta.json", .{dest_dir});
        defer self.allocator.free(meta_path);
        var buf: [64]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "{{\"repl_seq\":{}}}", .{repl_seq});
        const file = try std.Io.Dir.createFile(.cwd(), io, meta_path, .{ .truncate = true });
        defer file.close(io);
        try file.writeStreamingAll(io, data);
        try file.sync(io);
    }

    /// Restores a resync snapshot and seeds the follower's confirmed sequence.
    ///
    /// Restores the file/WAL as [`Database.restoreSnapshot`], then reads
    /// `snapshot_meta.json` for the `repl_seq` it was taken at and writes it into
    /// the follower state at `follower_state_dir` as `confirmed_seq`, returning
    /// that sequence. Returns 0 (and leaves state untouched) if the metadata is
    /// missing or unparseable.
    pub fn restoreSnapshotResync(allocator: Allocator, io: std.Io, snapshot_dir: []const u8, db_path: []const u8, wal_dir: []const u8, follower_state_dir: []const u8) !u64 {
        try restoreSnapshot(allocator, io, snapshot_dir, db_path, wal_dir);
        const repl = @import("../query/replication.zig");
        const meta_path = try std.fmt.allocPrint(allocator, "{s}/snapshot_meta.json", .{snapshot_dir});
        defer allocator.free(meta_path);
        const Meta = struct { repl_seq: u64 };
        const data = std.Io.Dir.readFileAlloc(.cwd(), io, meta_path, allocator, .unlimited) catch return 0;
        defer allocator.free(data);
        const parsed = std.json.parseFromSlice(Meta, allocator, data, .{}) catch return 0;
        defer parsed.deinit();
        const seq = parsed.value.repl_seq;
        std.Io.Dir.createDirPath(.cwd(), io, follower_state_dir) catch |e| {
            if (e != error.PathAlreadyExists) return e;
        };
        var st = repl.FollowerState.load(allocator, io, follower_state_dir);
        st.confirmed_seq = seq;
        try st.save(io, follower_state_dir);
        return seq;
    }

    /// Writes `max_epoch_seen` to `fence_epoch.json` in the WAL directory.
    ///
    /// Making the fence durable is what stops an old leader that restarts from
    /// silently regaining write rights: on load ([`Database.loadFence`]) it sees
    /// the higher epoch again and stays fenced. Silently no-ops without a WAL dir.
    fn persistFence(self: *Database) !void {
        const dir = self.fenceDir();
        if (dir.len == 0) return;
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try std.fmt.bufPrint(&path_buf, "{s}/fence_epoch.json", .{dir});
        var file = try std.Io.Dir.createFile(.cwd(), self.pool.pager.io, path, .{});
        defer file.close(self.pool.pager.io);
        var buf: [64]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "{{\"max_epoch_seen\":{}}}", .{self.max_epoch_seen});
        try file.writeStreamingAll(self.pool.pager.io, data);
        try file.sync(self.pool.pager.io);
    }

    /// Loads the persisted fence epoch at open time, restoring
    /// [`Database.max_epoch_seen`].
    ///
    /// Best-effort: any error (no WAL dir, missing or bad file) leaves the field
    /// at its default, since a missing fence just means "no failover recorded".
    pub fn loadFence(self: *Database) void {
        const dir = self.fenceDir();
        if (dir.len == 0) return;
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/fence_epoch.json", .{dir}) catch return;
        const data = std.Io.Dir.readFileAlloc(.cwd(), self.pool.pager.io, path, self.allocator, .unlimited) catch return;
        defer self.allocator.free(data);
        const Fence = struct { max_epoch_seen: u64 };
        const parsed = std.json.parseFromSlice(Fence, self.allocator, data, .{}) catch return;
        defer parsed.deinit();
        self.max_epoch_seen = parsed.value.max_epoch_seen;
    }

    /// Turns this database into a live replication follower.
    ///
    /// Allocates a `Follower` (the stream applier that feeds
    /// [`Database.applyStream`]) and a `ReplServer` listening on `host:port` for
    /// the leader's feed, wires TLS and auth, and spawns the listener on the
    /// database's concurrency group. The database keeps serving reads while
    /// applying the leader's writes.
    pub fn becomeFollower(self: *Database, host: []const u8, port: u16, wal_dir: []const u8, auth_key: []const u8, tls_config: @import("../query/replication.zig").TlsConfig) !void {
        const repl = @import("../query/replication.zig");
        self.repl_host = try self.allocator.dupe(u8, host);
        self.repl_wal_dir = try self.allocator.dupe(u8, wal_dir);

        const follower = try self.allocator.create(repl.Follower);
        follower.* = repl.Follower.init(self.allocator, self.pool.pager.io, self, self.repl_wal_dir);

        const server = try self.allocator.create(repl.ReplServer);
        server.* = repl.ReplServer.init(self.allocator, self.pool.pager.io, follower, self.repl_host, port, auth_key);
        server.tls_config = tls_config;

        self.follower = follower;
        self.repl_server = server;
        self.group.async(self.pool.pager.io, repl.ReplServer.listenEntry, .{server});
        std.log.info("Database is a live FOLLOWER, replication listener on {s}:{d}", .{ host, port });
    }

    /// Turns this database into a durable (quorum-acknowledged) leader.
    ///
    /// Creates a `DurableReplicator` that ships each WAL record to `total_replicas`
    /// followers and blocks a commit until a majority acknowledge (quorum =
    /// `total_replicas / 2 + 1`), connects it to the replica set, enables a
    /// backfill log so a lagging replica can catch up, and installs it as the
    /// WAL's `ship_callback`. `epoch` fences older leaders.
    pub fn becomeDurableLeader(self: *Database, host: []const u8, port: u16, total_replicas: u32, epoch: u64, timeout_ms: u64, auth_key: []const u8, tls_config: @import("../query/replication.zig").TlsConfig) !void {
        const repl = @import("../query/replication.zig");
        const dr = try self.allocator.create(repl.DurableReplicator);
        errdefer {
            dr.deinit();
            self.allocator.destroy(dr);
        }
        dr.* = repl.DurableReplicator.init(self.allocator, self.pool.pager.io, total_replicas, epoch, timeout_ms, auth_key, tls_config);
        if (self.wal) |w| {
            const bf_dir = try std.fmt.allocPrint(self.allocator, "{s}/repl_backfill", .{w.dir_path});
            defer self.allocator.free(bf_dir);
            dr.enableBackfill(bf_dir) catch |err| std.log.warn("durable leader: backfill log disabled: {any}", .{err});
        }
        try dr.connect(host, port);
        self.durable_repl = dr;
        if (self.wal) |w| {
            w.ship_callback = repl.DurableReplicator.onRecord;
            w.replication_manager = @ptrCast(dr);
        }
        std.log.info("Database is a DURABLE LEADER, shipping to {s}:{d} (quorum {d})", .{ host, port, total_replicas / 2 + 1 });
    }

    /// Stops and frees the durable-leader shipping machinery, if any. Symmetric to
    /// [`Database.stopFollower`]; unlike `close`'s teardown it also `destroy`s the
    /// allocation, because it runs at runtime (on demote) and must not leak across
    /// repeated role flips. Unsets the WAL ship callback FIRST so no further record
    /// is handed to the replicator once it is being torn down.
    pub fn stopDurableLeader(self: *Database) void {
        if (self.wal) |w| {
            w.ship_callback = null;
            w.replication_manager = null;
        }
        if (self.durable_repl) |dr| {
            dr.deinit();
            self.allocator.destroy(dr);
            self.durable_repl = null;
        }
    }

    /// Promotes this node to a writable primary at a fresh, strictly-greater fence
    /// epoch, and returns that epoch.
    ///
    /// Safe-by-construction against split-brain: the new epoch is
    /// `max_epoch_seen + 1`. A former follower has been raising `max_epoch_seen`
    /// via [`Database.observeEpoch`] as it applied the old leader's stream, so this
    /// is strictly greater than any epoch the old leader used; the old leader is
    /// fenced ([`Database.guardWrite`] fails) the moment it observes the new epoch.
    /// [`Database.setWriteEpoch`] persists the raised `max_epoch_seen` BEFORE this
    /// returns, so a crash mid-promote still leaves the higher epoch on disk and the
    /// old leader stays fenced. Stops any follower apply loop first. Shipping to
    /// remaining replicas, if wanted, is configured separately.
    pub fn promote(self: *Database) !u64 {
        self.stopFollower();
        const new_epoch = self.max_epoch_seen + 1;
        try self.setWriteEpoch(new_epoch);
        std.log.info("Database PROMOTED to writable primary at epoch {d}", .{new_epoch});
        return new_epoch;
    }

    /// Demotes this node to a follower that listens on `host:port` for the new
    /// primary's pushed stream (replication is push-model: the leader dials the
    /// follower). Fences local writes FIRST, then stops any shipping, then starts
    /// following, so there is never a window where the node both accepts client
    /// writes and applies a leader's stream. Any writes that committed locally
    /// before the fence are not shipped onward and will be superseded by the new
    /// leader's history once it re-syncs this follower (the new leader is
    /// authoritative after a failover).
    pub fn demote(self: *Database, host: []const u8, port: u16, auth_key: []const u8, tls_config: @import("../query/replication.zig").TlsConfig) !void {
        // 1. Fence: raise max_epoch_seen above our own writable epoch so guardWrite
        //    rejects every NEW client write immediately. Persisted by observeEpoch.
        if (self.fencing_epoch >= self.max_epoch_seen) {
            try self.observeEpoch(self.max_epoch_seen + 1);
        }
        // 2. Stop shipping (if we were a leader) and any prior follow loop.
        self.stopDurableLeader();
        self.stopFollower();
        // 3. Start following the new primary.
        try self.becomeFollower(host, port, self.fenceDir(), auth_key, tls_config);
        std.log.info("Database DEMOTED to follower, listening on {s}:{d}", .{ host, port });
    }

    /// Stops and frees the follower replication machinery, if any.
    ///
    /// Stops and destroys the `ReplServer` and `Follower` and frees the owned
    /// `repl_host` / `repl_wal_dir` strings, resetting them to empty. Safe to call
    /// on a non-follower (all fields null). Invoked from close/crash paths.
    fn stopFollower(self: *Database) void {
        if (self.repl_server) |s| {
            s.stop();
            self.allocator.destroy(s);
            self.repl_server = null;
        }
        if (self.follower) |f| {
            self.allocator.destroy(f);
            self.follower = null;
        }
        if (self.repl_host.len > 0) {
            self.allocator.free(self.repl_host);
            self.repl_host = "";
        }
        if (self.repl_wal_dir.len > 0) {
            self.allocator.free(self.repl_wal_dir);
            self.repl_wal_dir = "";
        }
    }

    /// Persists the highest applied LSN to `repl_checkpoint.json` in `base_dir`.
    ///
    /// A follower records how far it has durably applied so that after a restart
    /// it resumes at the right point rather than re-applying or skipping records.
    /// Paired with [`Database.loadReplCheckpoint`].
    pub fn saveReplCheckpoint(self: *Database, lsn: u64) !void {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const checkpoint_path = try std.fmt.bufPrint(&path_buf, "{s}/repl_checkpoint.json", .{self.base_dir});
        var file = try std.Io.Dir.createFile(.cwd(), self.pool.pager.io, checkpoint_path, .{});
        defer file.close(self.pool.pager.io);

        var buf: [128]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "{{\"last_applied_lsn\":{}}}", .{lsn});
        try file.writeStreamingAll(self.pool.pager.io, data);
        try file.sync(self.pool.pager.io);
    }

    /// Loads the saved replication checkpoint into [`Database.last_applied_lsn`].
    ///
    /// A missing file is treated as "never applied" (sets the LSN to 0 and
    /// returns cleanly); other read/parse errors propagate.
    pub fn loadReplCheckpoint(self: *Database) !void {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const checkpoint_path = try std.fmt.bufPrint(&path_buf, "{s}/repl_checkpoint.json", .{self.base_dir});
        const data = std.Io.Dir.readFileAlloc(.cwd(), self.pool.pager.io, checkpoint_path, self.allocator, .unlimited) catch |err| {
            if (err == error.FileNotFound) {
                self.last_applied_lsn = 0;
                return;
            }
            return err;
        };
        defer self.allocator.free(data);

        const Checkpoint = struct {
            last_applied_lsn: u64,
        };
        const parsed = try std.json.parseFromSlice(Checkpoint, self.allocator, data, .{});
        defer parsed.deinit();
        self.last_applied_lsn = parsed.value.last_applied_lsn;
    }

    /// Applies a single shipped WAL operation on a follower, record by record.
    ///
    /// This is the streaming (per-record) apply path, distinct from the batched
    /// [`Database.applyStream`]. It updates the transaction manager for
    /// begin/commit/rollback and, for DML, either replays a catalog change
    /// (materialising a new table/index tree and reloading the catalog for a
    /// `sys.*` insert, or a plain cell overwrite otherwise) or applies a user-row
    /// change and rebuilds affected index entries for every version currently
    /// visible under this replica's transaction state. Kind 254 is a sentinel that
    /// is ignored. Held under the transaction manager mutex throughout.
    pub fn post(self: *Database, ship: @import("../query/replication.zig").proto.ShipWalRequest) !void {
        if (ship.kind == 254) return;
        const kind = @as(OpKind, @enumFromInt(ship.kind));

        self.txn_manager.mutex.lockUncancelable(self.pool.pager.io);
        defer self.txn_manager.mutex.unlock(self.pool.pager.io);

        switch (kind) {
            .begin => {
                try self.txn_manager.active_txns.put(ship.tx_id, {});
            },
            .commit => {
                try self.txn_manager.committed_txns.put(ship.tx_id, {});
                _ = self.txn_manager.active_txns.remove(ship.tx_id);
                if (ship.tx_id >= self.txn_manager.next_tx_id) {
                    self.txn_manager.next_tx_id = ship.tx_id + 1;
                }
            },
            .rollback => {
                _ = self.txn_manager.active_txns.remove(ship.tx_id);
                if (ship.tx_id >= self.txn_manager.next_tx_id) {
                    self.txn_manager.next_tx_id = ship.tx_id + 1;
                }
            },
            .insert, .update, .delete => {
                if (ship.tx_id >= self.txn_manager.next_tx_id) {
                    self.txn_manager.next_tx_id = ship.tx_id + 1;
                }

                if (std.mem.startsWith(u8, ship.table_name, "sys.")) {
                    if (std.mem.eql(u8, ship.table_name, "sys.objects")) {
                        if (kind == .insert) {
                            return;
                        }
                        _ = self.master_tree.delete(ship.key) catch {};
                        try self.master_tree.insert(ship.key, ship.value);
                    } else if (std.mem.eql(u8, ship.table_name, "sys.tables")) {
                        if (kind == .insert) {
                            var meta = try types.TableMetadata.deserialize(self.allocator, ship.value);
                            defer {
                                self.allocator.free(meta.name);
                                for (meta.columns) |col| {
                                    self.allocator.free(col.name);
                                    if (col.default_value) |dv| self.allocator.free(dv);
                                }
                                self.allocator.free(meta.columns);
                            }

                            const table_tree = try BPlusTree.create(self.pool, self.allocator);
                            defer table_tree.deinit();

                            const obj = types.ObjectMetadata{
                                .id = @intCast(self.table_roots.count() + self.index_roots.count() + 1),
                                .name = meta.name,
                                .type = "TABLE",
                                .root_page_id = table_tree.root_page_id,
                            };
                            const obj_bytes = try obj.serialize(self.allocator);
                            defer self.allocator.free(obj_bytes);
                            _ = self.master_tree.delete(meta.name) catch {};
                            try self.master_tree.insert(meta.name, obj_bytes);

                            meta.root_page_id = table_tree.root_page_id;
                            const replica_meta_bytes = try meta.serialize(self.allocator);
                            defer self.allocator.free(replica_meta_bytes);

                            const tables_root = self.table_roots.get("sys.tables") orelse return error.SystemTableNotFound;
                            var tables_tree = try BPlusTree.init(self.pool, tables_root, self.allocator);
                            defer tables_tree.deinit();
                            _ = tables_tree.delete(meta.name) catch {};
                            try tables_tree.insert(meta.name, replica_meta_bytes);
                        } else {
                            const table_tree = try self.getTableTree(ship.table_name);
                            defer table_tree.deinit();
                            _ = table_tree.delete(ship.key) catch {};
                            try table_tree.insert(ship.key, ship.value);
                        }
                    } else if (std.mem.eql(u8, ship.table_name, "sys.indexes")) {
                        if (kind == .insert) {
                            var meta = try types.IndexMetadata.deserialize(self.allocator, ship.value);
                            defer {
                                self.allocator.free(meta.name);
                                for (meta.key_columns) |col| {
                                    self.allocator.free(col.name);
                                    if (col.default_value) |dv| self.allocator.free(dv);
                                }
                                self.allocator.free(meta.key_columns);
                                if (meta.value_columns) |vcols| {
                                    for (vcols) |col| {
                                        self.allocator.free(col.name);
                                        if (col.default_value) |dv| self.allocator.free(dv);
                                    }
                                    self.allocator.free(vcols);
                                }
                            }

                            const index_tree = try BPlusTree.create(self.pool, self.allocator);
                            defer index_tree.deinit();

                            const obj = types.ObjectMetadata{
                                .id = @intCast(self.table_roots.count() + self.index_roots.count() + 1),
                                .name = meta.name,
                                .type = "INDEX",
                                .root_page_id = index_tree.root_page_id,
                            };
                            const obj_bytes = try obj.serialize(self.allocator);
                            defer self.allocator.free(obj_bytes);
                            _ = self.master_tree.delete(meta.name) catch {};
                            try self.master_tree.insert(meta.name, obj_bytes);

                            meta.root_page_id = index_tree.root_page_id;
                            const replica_meta_bytes = try meta.serialize(self.allocator);
                            defer self.allocator.free(replica_meta_bytes);

                            const indexes_root = self.table_roots.get("sys.indexes") orelse return error.SystemTableNotFound;
                            var indexes_tree = try BPlusTree.init(self.pool, indexes_root, self.allocator);
                            defer indexes_tree.deinit();
                            _ = indexes_tree.delete(meta.name) catch {};
                            try indexes_tree.insert(meta.name, replica_meta_bytes);
                        } else {
                            const table_tree = try self.getTableTree(ship.table_name);
                            defer table_tree.deinit();
                            _ = table_tree.delete(ship.key) catch {};
                            try table_tree.insert(ship.key, ship.value);
                        }
                    } else {
                        const table_tree = try self.getTableTree(ship.table_name);
                        defer table_tree.deinit();
                        _ = table_tree.delete(ship.key) catch {};
                        try table_tree.insert(ship.key, ship.value);
                    }

                    self.catalog.deinit();
                    self.catalog = catalog.SystemCatalog.init(self.allocator);

                    var table_it = self.table_roots.keyIterator();
                    while (table_it.next()) |k| self.allocator.free(k.*);
                    self.table_roots.clearRetainingCapacity();

                    var index_it = self.index_roots.keyIterator();
                    while (index_it.next()) |k| self.allocator.free(k.*);
                    self.index_roots.clearRetainingCapacity();

                    try self.loadCatalog();
                } else {
                    const table_tree = try self.getTableTree(ship.table_name);
                    defer table_tree.deinit();
                    _ = table_tree.delete(ship.key) catch {};
                    try table_tree.insert(ship.key, ship.value);

                    const table_meta = for (self.catalog.tables.items) |tbl| {
                        if (std.mem.eql(u8, tbl.name, ship.table_name)) break tbl;
                    } else return;

                    for (self.catalog.indexes.items) |idx| {
                        if (idx.table_id == table_meta.id) {
                            const idx_tree = try self.getIndexTree(idx.name);
                            defer idx_tree.deinit();

                            const versions = try self.reconstructVersionChain(ship.value, self.allocator);
                            defer {
                                for (versions) |*v| v.deinit(self.allocator);
                                self.allocator.free(versions);
                            }

                            for (versions) |v| {
                                const xmin = v.xmin;
                                const xmax = v.xmax;

                                const is_active = self.txn_manager.active_txns.contains(xmin);
                                const is_committed = self.txn_manager.committed_txns.contains(xmin) or xmin == 0 or xmin < self.txn_manager.next_tx_id;

                                if (is_committed and !is_active) {
                                    const is_deleted = if (xmax > 0) (self.txn_manager.committed_txns.contains(xmax) or xmax < self.txn_manager.next_tx_id) else false;
                                    if (!is_deleted) {
                                        var list = std.ArrayList([]const u8).empty;
                                        defer {
                                            for (list.items) |item| self.allocator.free(item);
                                            list.deinit(self.allocator);
                                        }

                                        const reader = row.RowReader.init(table_meta, v.fixed, v.heap);
                                        for (idx.key_columns) |col| {
                                            const raw_val = try reader.readToString(self.allocator, col.name);
                                            defer self.allocator.free(raw_val);
                                            const ct = colTypeByNameIn(table_meta, col.name);
                                            const str_val = try types.encodeIndexValueAlloc(self.allocator, ct, raw_val);
                                            try list.append(self.allocator, str_val);
                                        }

                                        try list.append(self.allocator, try self.allocator.dupe(u8, ship.key));

                                        var key_parts = try self.allocator.alloc([]const u8, list.items.len);
                                        defer self.allocator.free(key_parts);
                                        for (list.items, 0..) |part, idx_col| key_parts[idx_col] = part;

                                        const index_key = try std.mem.join(self.allocator, ":", key_parts);
                                        defer self.allocator.free(index_key);

                                        _ = idx_tree.delete(index_key) catch {};
                                        try idx_tree.insert(index_key, "");
                                    }
                                }
                            }
                        }
                    }
                }
            },
        }
    }

    /// Registers a system object in `sys.objects` and the in-memory root maps.
    ///
    /// Serialises a [`types.ObjectMetadata`] into the master tree under `name` and
    /// records `name -> root_page_id` in `table_roots` (for `"TABLE"`) or
    /// `index_roots` (for `"INDEX"`). The low-level building block used by
    /// [`Database.ensureSystemCatalogTables`] to bootstrap the catalog.
    fn registerSystemObject(self: *Database, name: []const u8, obj_type: []const u8, root_page_id: u64) !void {
        const obj = types.ObjectMetadata{
            .id = @intCast(self.table_roots.count() + self.index_roots.count() + 1),
            .name = name,
            .type = obj_type,
            .root_page_id = root_page_id,
        };
        const obj_bytes = try obj.serialize(self.allocator);
        defer self.allocator.free(obj_bytes);

        try self.master_tree.insert(name, obj_bytes);

        if (std.mem.eql(u8, obj_type, "TABLE")) {
            try self.table_roots.put(try self.allocator.dupe(u8, name), root_page_id);
        } else if (std.mem.eql(u8, obj_type, "INDEX")) {
            try self.index_roots.put(try self.allocator.dupe(u8, name), root_page_id);
        }
    }

    /// Adds a system table's column schema to the catalog and `sys.tables`.
    ///
    /// Given the already-registered object `name` and its column metadata, builds
    /// a [`table_mod.Table`], adds it to the in-memory catalog, and writes its
    /// [`types.TableMetadata`] row into `sys.tables` so it survives a reopen. The
    /// schema-half companion to [`Database.registerSystemObject`].
    fn writeSystemTableSchema(self: *Database, name: []const u8, columns: []const types.ColumnMetadata) !void {
        var cols = try self.allocator.alloc(types.Column, columns.len);
        for (columns, 0..) |col_meta, i| {
            cols[i] = types.Column{
                .name = try self.allocator.dupe(u8, col_meta.name),
                .type = col_meta.type,
                .size = col_meta.size,
                .offset = col_meta.offset,
                .is_primary_key = col_meta.is_primary_key,
                .is_auto_increment = col_meta.is_auto_increment,
                .is_nullable = col_meta.is_nullable,
                .default_value = null,
            };
        }

        const table_id: u32 = @intCast(self.catalog.tables.items.len + 1);
        const table = try table_mod.Table.init(self.allocator, table_id, name, cols);
        try self.catalog.addTable(table);

        const tables_root = self.table_roots.get("sys.tables") orelse return error.SystemTableNotFound;
        var tables_tree = try BPlusTree.init(self.pool, tables_root, self.allocator);
        defer tables_tree.deinit();

        const meta = types.TableMetadata{
            .id = table_id,
            .name = name,
            .columns = columns,
            .root_page_id = self.table_roots.get(name).?,
        };
        const meta_bytes = try meta.serialize(self.allocator);
        defer self.allocator.free(meta_bytes);

        try tables_tree.insert(name, meta_bytes);
    }

    /// Materialises the full set of system catalog tables on first open.
    ///
    /// Idempotent: returns immediately if `sys.objects` already exists. Otherwise
    /// it registers `sys.objects` at the fixed root page 1 and creates fresh trees
    /// for `sys.tables`, `sys.indexes`, `sys.constraints`, `sys.users`,
    /// `sys.view`, `sys.table_stats`, `sys.roles` and `sys.privileges`, then
    /// writes each one's hard-coded column schema. The exact column layouts here
    /// (offsets and types) are the contract the rest of the engine reads back.
    fn ensureSystemCatalogTables(self: *Database) !void {
        if (self.table_roots.contains("sys.objects")) {
            // Reopen of an existing database: the persisted sys.* tables are already
            // present, but the purely in-memory synthetic ones still need registering.
            try self.ensureSyntheticCatalogTables();
            return;
        }

        try self.registerSystemObject("sys.objects", "TABLE", 1);

        const tables_tree = try BPlusTree.create(self.pool, self.allocator);
        defer tables_tree.deinit();
        try self.registerSystemObject("sys.tables", "TABLE", tables_tree.root_page_id);

        const indexes_tree = try BPlusTree.create(self.pool, self.allocator);
        defer indexes_tree.deinit();
        try self.registerSystemObject("sys.indexes", "TABLE", indexes_tree.root_page_id);

        const constraints_tree = try BPlusTree.create(self.pool, self.allocator);
        defer constraints_tree.deinit();
        try self.registerSystemObject("sys.constraints", "TABLE", constraints_tree.root_page_id);

        const users_tree = try BPlusTree.create(self.pool, self.allocator);
        defer users_tree.deinit();
        try self.registerSystemObject("sys.users", "TABLE", users_tree.root_page_id);

        const view_tree = try BPlusTree.create(self.pool, self.allocator);
        defer view_tree.deinit();
        try self.registerSystemObject("sys.view", "TABLE", view_tree.root_page_id);

        const stats_tree = try BPlusTree.create(self.pool, self.allocator);
        defer stats_tree.deinit();
        try self.registerSystemObject("sys.table_stats", "TABLE", stats_tree.root_page_id);

        const roles_tree = try BPlusTree.create(self.pool, self.allocator);
        defer roles_tree.deinit();
        try self.registerSystemObject("sys.roles", "TABLE", roles_tree.root_page_id);

        const privileges_tree = try BPlusTree.create(self.pool, self.allocator);
        defer privileges_tree.deinit();
        try self.registerSystemObject("sys.privileges", "TABLE", privileges_tree.root_page_id);

        // The catalog views below are read back through the synthetic materialiser
        // (buildCatalogRows) rather than a raw scan, so these column lists are the shape
        // users see. They mirror SQL Server's system catalog: sys.objects is the superset
        // of every object, sys.tables is the user-table subset, sys.indexes the indexes.
        try self.writeSystemTableSchema("sys.objects", &.{
            .{ .name = "object_id", .type = .UINT32, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "name", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "schema_id", .type = .UINT32, .size = 4, .offset = 8, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type", .type = .TEXT, .size = 4, .offset = 12, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type_desc", .type = .TEXT, .size = 4, .offset = 16, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "is_ms_shipped", .type = .UINT32, .size = 4, .offset = 20, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "root_page_id", .type = .UINT64, .size = 8, .offset = 24, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.tables", &.{
            .{ .name = "object_id", .type = .UINT32, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "name", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "schema_id", .type = .UINT32, .size = 4, .offset = 8, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type", .type = .TEXT, .size = 4, .offset = 12, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type_desc", .type = .TEXT, .size = 4, .offset = 16, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "is_ms_shipped", .type = .UINT32, .size = 4, .offset = 20, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "root_page_id", .type = .UINT64, .size = 8, .offset = 24, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.indexes", &.{
            .{ .name = "object_id", .type = .UINT32, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "table_name", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "index_id", .type = .UINT32, .size = 4, .offset = 8, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "name", .type = .TEXT, .size = 4, .offset = 12, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type", .type = .UINT32, .size = 4, .offset = 16, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type_desc", .type = .TEXT, .size = 4, .offset = 20, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "is_unique", .type = .UINT32, .size = 4, .offset = 24, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "is_primary_key", .type = .UINT32, .size = 4, .offset = 28, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "root_page_id", .type = .UINT64, .size = 8, .offset = 32, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.constraints", &.{
            .{ .name = "name", .type = .TEXT, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "type", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "definition", .type = .TEXT, .size = 4, .offset = 8, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.users", &.{
            .{ .name = "username", .type = .TEXT, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "password_hash", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "role", .type = .TEXT, .size = 4, .offset = 8, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.view", &.{
            .{ .name = "name", .type = .TEXT, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "definition", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.table_stats", &.{
            .{ .name = "table_name", .type = .TEXT, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "row_count", .type = .UINT64, .size = 8, .offset = 4, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "page_count", .type = .UINT64, .size = 8, .offset = 12, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.roles", &.{
            .{ .name = "role_name", .type = .TEXT, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.writeSystemTableSchema("sys.privileges", &.{
            .{ .name = "id", .type = .TEXT, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "grantee", .type = .TEXT, .size = 4, .offset = 4, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "object_name", .type = .TEXT, .size = 4, .offset = 8, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            .{ .name = "privilege", .type = .TEXT, .size = 4, .offset = 12, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        // Fresh database: register the in-memory synthetic catalog tables too.
        try self.ensureSyntheticCatalogTables();
    }

    /// Rebuilds the in-memory catalog from the on-disk system tables.
    ///
    /// Walks `sys.objects` to repopulate the `table_roots` / `index_roots` maps,
    /// then reads `sys.tables` and `sys.indexes` to reconstruct every
    /// [`table_mod.Table`] and [`table_mod.Index`], and finally reads
    /// `sys.constraints`, decoding each live foreign-key row's JSON definition
    /// back into a [`types.ForeignKey`]. Called on open and after every catalog
    /// reload during recovery and replication. Assumes the root maps for the
    /// `sys.*` tables are already present.
    fn loadCatalog(self: *Database) !void {
        // loadCatalog can run more than once (open, then recovery). Callers clear the
        // table/index root maps before each run; do the same for collection roots here
        // so a reload replaces the entries without leaking the previous owned keys.
        var prev_coll = self.collection_roots.keyIterator();
        while (prev_coll.next()) |k| self.allocator.free(k.*);
        self.collection_roots.clearRetainingCapacity();

        var it = try self.master_tree.iterator();
        defer it.deinit();

        while (try it.next()) |cell| {
            const obj = try types.ObjectMetadata.deserialize(self.allocator, cell.value);
            defer {
                self.allocator.free(obj.name);
                self.allocator.free(obj.type);
            }

            if (std.mem.eql(u8, obj.type, "TABLE")) {
                try self.table_roots.put(try self.allocator.dupe(u8, obj.name), obj.root_page_id);
            } else if (std.mem.eql(u8, obj.type, "INDEX")) {
                try self.index_roots.put(try self.allocator.dupe(u8, obj.name), obj.root_page_id);
            } else if (std.mem.eql(u8, obj.type, "COLLECTION")) {
                try self.collection_roots.put(try self.allocator.dupe(u8, obj.name), obj.root_page_id);
            }
        }

        if (self.table_roots.get("sys.tables")) |tables_root| {
            var tables_tree = try BPlusTree.init(self.pool, tables_root, self.allocator);
            defer tables_tree.deinit();

            var table_it = try tables_tree.iterator();
            defer table_it.deinit();

            while (try table_it.next()) |cell| {
                const table_meta = try types.TableMetadata.deserialize(self.allocator, cell.value);
                defer {
                    for (table_meta.columns) |col| {
                        self.allocator.free(col.name);
                        if (col.default_value) |dv| self.allocator.free(dv);
                    }
                    self.allocator.free(table_meta.columns);
                    self.allocator.free(table_meta.name);
                }

                var cols = try self.allocator.alloc(types.Column, table_meta.columns.len);
                for (table_meta.columns, 0..) |col_meta, i| {
                    cols[i] = types.Column{
                        .name = try self.allocator.dupe(u8, col_meta.name),
                        .type = col_meta.type,
                        .size = col_meta.size,
                        .offset = col_meta.offset,
                        .is_primary_key = col_meta.is_primary_key,
                        .is_auto_increment = col_meta.is_auto_increment,
                        .is_nullable = col_meta.is_nullable,
                        .default_value = if (col_meta.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                    };
                }

                const table = try table_mod.Table.init(self.allocator, table_meta.id, table_meta.name, cols);
                try self.catalog.addTable(table);
            }
        }

        if (self.table_roots.get("sys.indexes")) |indexes_root| {
            var indexes_tree = try BPlusTree.init(self.pool, indexes_root, self.allocator);
            defer indexes_tree.deinit();

            var index_it = try indexes_tree.iterator();
            defer index_it.deinit();

            while (try index_it.next()) |cell| {
                const index_meta = try types.IndexMetadata.deserialize(self.allocator, cell.value);
                defer {
                    for (index_meta.key_columns) |col| {
                        self.allocator.free(col.name);
                        if (col.default_value) |dv| self.allocator.free(dv);
                    }
                    self.allocator.free(index_meta.key_columns);
                    if (index_meta.value_columns) |vcols| {
                        for (vcols) |col| {
                            self.allocator.free(col.name);
                            if (col.default_value) |dv| self.allocator.free(dv);
                        }
                        self.allocator.free(vcols);
                    }
                    self.allocator.free(index_meta.name);
                }

                var key_cols = try self.allocator.alloc(types.Column, index_meta.key_columns.len);
                for (index_meta.key_columns, 0..) |col_meta, i| {
                    key_cols[i] = types.Column{
                        .name = try self.allocator.dupe(u8, col_meta.name),
                        .type = col_meta.type,
                        .size = col_meta.size,
                        .offset = col_meta.offset,
                        .is_primary_key = col_meta.is_primary_key,
                        .is_auto_increment = col_meta.is_auto_increment,
                        .is_nullable = col_meta.is_nullable,
                        .default_value = if (col_meta.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                    };
                }

                var val_cols: ?[]types.Column = null;
                if (index_meta.value_columns) |vcols| {
                    var temp_vcols = try self.allocator.alloc(types.Column, vcols.len);
                    for (vcols, 0..) |col_meta, i| {
                        temp_vcols[i] = types.Column{
                            .name = try self.allocator.dupe(u8, col_meta.name),
                            .type = col_meta.type,
                            .size = col_meta.size,
                            .offset = col_meta.offset,
                            .is_primary_key = col_meta.is_primary_key,
                            .is_auto_increment = col_meta.is_auto_increment,
                            .is_nullable = col_meta.is_nullable,
                            .default_value = if (col_meta.default_value) |dv| try self.allocator.dupe(u8, dv) else null,
                        };
                    }
                    val_cols = temp_vcols;
                }

                const index = table_mod.Index{
                    .id = index_meta.id,
                    .name = try self.allocator.dupe(u8, index_meta.name),
                    .table_id = index_meta.table_id,
                    .kind = index_meta.kind,
                    .key_columns = key_cols,
                    .value_columns = val_cols,
                    .exact = index_meta.exact,
                    .allocator = self.allocator,
                };
                try self.catalog.addIndex(index);
            }
        }

        if (self.table_roots.get("sys.constraints")) |const_root| {
            const const_table = self.catalog.getTable("sys.constraints") orelse return;
            var const_tree = try BPlusTree.init(self.pool, const_root, self.allocator);
            defer const_tree.deinit();

            var const_it = try const_tree.iterator();
            defer const_it.deinit();

            while (try const_it.next()) |cell| {
                const versions = try self.reconstructVersionChain(cell.value, self.allocator);
                defer {
                    for (versions) |*v| v.deinit(self.allocator);
                    self.allocator.free(versions);
                }

                if (versions.len == 0) continue;
                const latest = versions[versions.len - 1];
                if (latest.xmax > 0) continue;

                const reader = row.RowReader.init(const_table, latest.fixed, latest.heap);
                const name = try reader.readToString(self.allocator, "name");
                defer self.allocator.free(name);
                const c_type = try reader.readToString(self.allocator, "type");
                defer self.allocator.free(c_type);
                const def_json = try reader.readToString(self.allocator, "definition");
                defer self.allocator.free(def_json);

                if (std.mem.eql(u8, c_type, "FOREIGN_KEY")) {
                    const parsed = try std.json.parseFromSlice(types.ForeignKeyDefinition, self.allocator, def_json, .{});
                    defer parsed.deinit();

                    const child_table = self.catalog.getTable(parsed.value.table_name) orelse continue;
                    const parent_table = self.catalog.getTable(parsed.value.referenced_table_name) orelse continue;

                    var child_cols = try self.allocator.alloc(types.Column, parsed.value.columns.len);
                    errdefer self.allocator.free(child_cols);
                    for (parsed.value.columns, 0..) |col_name, i| {
                        const col = child_table.getColumn(col_name) orelse return error.ColumnNotFound;
                        child_cols[i] = types.Column{
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

                    var parent_cols = try self.allocator.alloc(types.Column, parsed.value.referenced_columns.len);
                    errdefer self.allocator.free(parent_cols);
                    for (parsed.value.referenced_columns, 0..) |col_name, i| {
                        const col = parent_table.getColumn(col_name) orelse return error.ColumnNotFound;
                        parent_cols[i] = types.Column{
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

                    const fk_id = @as(u32, @intCast(self.catalog.foreign_keys.items.len + 1));
                    const fk = try types.ForeignKey.init(
                        self.allocator,
                        fk_id,
                        name,
                        child_table.id,
                        parent_table.id,
                        child_cols,
                        parent_cols,
                        .NO_ACTION,
                        .NO_ACTION,
                    );
                    try self.catalog.addForeignKey(fk);
                }
            }
        }

        // Register the purely-synthetic catalog tables (today just `sys.columns`).
        // These have no B+Tree of their own; the query executor generates their
        // rows on demand from the in-memory catalog. Done here, at the tail of
        // every catalog load, so the entry exists on fresh and reopened databases
        // alike.
        try self.ensureSyntheticCatalogTables();
    }

    /// Registers the purely-synthetic catalog tables that have no physical storage.
    ///
    /// Today that is `sys.columns`, which projects every table's column list as
    /// queryable rows so a client can introspect column names, types, ordinal and
    /// nullability with plain SQL (`SELECT ... FROM sys.columns WHERE table_name =
    /// '...'`). It is registered in the in-memory catalog only, never written to
    /// disk and given no B+Tree: the executor recognises it (and the other `sys.*`
    /// tables) as a catalog scan and synthesises the rows from `self.catalog`. The
    /// guard keeps this idempotent across the repeated `loadCatalog` calls.
    /// Registers one purely in-memory catalog table (no on-disk tree) from a column
    /// definition list, if it is not already present. Its rows are produced on demand by
    /// the synthetic materialiser (`QueryExecutor.buildCatalogRows`).
    fn registerSyntheticTable(self: *Database, name: []const u8, defs: []const types.ColumnMetadata) !void {
        if (self.catalog.getTable(name) != null) return;
        var cols = try self.allocator.alloc(types.Column, defs.len);
        for (defs, 0..) |d, i| {
            cols[i] = types.Column{
                .name = try self.allocator.dupe(u8, d.name),
                .type = d.type,
                .size = d.size,
                .offset = d.offset,
                .is_primary_key = d.is_primary_key,
                .is_auto_increment = d.is_auto_increment,
                .is_nullable = d.is_nullable,
                .default_value = null,
            };
        }
        const table_id: u32 = @intCast(self.catalog.tables.items.len + 1);
        const table = try table_mod.Table.init(self.allocator, table_id, name, cols);
        try self.catalog.addTable(table);
    }

    /// Registers the purely in-memory catalog views (no on-disk storage): sys.columns,
    /// sys.schemas and sys.types. Runs on every open (they are never persisted), so their
    /// shape can evolve without a catalog migration. Column sets mirror SQL Server.
    fn ensureSyntheticCatalogTables(self: *Database) !void {
        const F = types.ColumnMetadata; // shorthand
        const text = types.ColumnType.TEXT;
        const u32t = types.ColumnType.UINT32;
        const i32t = types.ColumnType.INT32;

        try self.registerSyntheticTable("sys.columns", &.{
            F{ .name = "object_id", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "table_name", .type = text, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "column_id", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "name", .type = text, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "data_type", .type = text, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "max_length", .type = i32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "ordinal", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "is_nullable", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "is_identity", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "is_primary_key", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.registerSyntheticTable("sys.schemas", &.{
            F{ .name = "schema_id", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "name", .type = text, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "principal_id", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });

        try self.registerSyntheticTable("sys.types", &.{
            F{ .name = "user_type_id", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "name", .type = text, .size = 4, .offset = 0, .is_primary_key = true, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "max_length", .type = i32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
            F{ .name = "is_nullable", .type = u32t, .size = 4, .offset = 0, .is_primary_key = false, .is_auto_increment = false, .is_nullable = false, .default_value = null },
        });
    }

    /// Creates a user table: a new tree, catalog rows, WAL records, and caches.
    ///
    /// Allocates a fresh B+Tree, assigns the next table id, writes the object into
    /// `sys.objects` and the [`types.TableMetadata`] into `sys.tables`, reserving
    /// two LSNs and logging both inserts, then registers the table in the catalog
    /// and both root/tree caches (marking the tree `is_cached` so the cache owns
    /// it). Takes ownership of `columns` via [`table_mod.Table.init`]. Returns the
    /// created table.
    pub fn createTable(self: *Database, name: []const u8, columns: []types.Column, tx_id: u64) !table_mod.Table {
        const table_tree = try BPlusTree.create(self.pool, self.allocator);
        defer table_tree.deinit();

        const table_id = @as(u32, @intCast(self.catalog.tables.items.len + 1));
        const table = try table_mod.Table.init(self.allocator, table_id, name, columns);

        var col_meta = try self.allocator.alloc(types.ColumnMetadata, table.columns.len);
        defer self.allocator.free(col_meta);
        for (table.columns, 0..) |col, i| {
            col_meta[i] = .{
                .name = col.name,
                .type = col.type,
                .size = col.size,
                .offset = col.offset,
                .is_primary_key = col.is_primary_key,
                .is_auto_increment = col.is_auto_increment,
                .is_nullable = col.is_nullable,
                .default_value = col.default_value,
            };
        }

        const meta = types.TableMetadata{
            .id = table.id,
            .name = table.name,
            .columns = col_meta,
            .root_page_id = table_tree.root_page_id,
        };

        const meta_bytes = try meta.serialize(self.allocator);
        defer self.allocator.free(meta_bytes);

        const obj = types.ObjectMetadata{
            .id = @intCast(self.table_roots.count() + self.index_roots.count() + 1),
            .name = name,
            .type = "TABLE",
            .root_page_id = table_tree.root_page_id,
        };
        const obj_bytes = try obj.serialize(self.allocator);
        defer self.allocator.free(obj_bytes);
        const lsn1 = self.reserveLsn();
        const lsn2 = self.reserveLsn();
        try self.master_tree.insert(name, obj_bytes);

        const tables_root = self.table_roots.get("sys.tables") orelse return error.SystemTableNotFound;
        var tables_tree = try BPlusTree.init(self.pool, tables_root, self.allocator);
        defer tables_tree.deinit();
        try tables_tree.insert(name, meta_bytes);

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn1,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .insert,
                .table_name = "sys.objects",
                .key = name,
                .value = obj_bytes,
            });

            try wal.append(.{
                .lsn = lsn2,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .insert,
                .table_name = "sys.tables",
                .key = name,
                .value = meta_bytes,
            });
        }

        try self.catalog.addTable(table);
        try self.table_roots.put(try self.allocator.dupe(u8, name), table_tree.root_page_id);
        table_tree.is_cached = true;
        try self.table_trees.put(try self.allocator.dupe(u8, name), table_tree);

        return table;
    }

    /// Creates a secondary index on an existing table, mirroring
    /// [`Database.createTable`].
    ///
    /// Resolves the owning table ([`error.TableNotFound`] if absent), allocates a
    /// fresh index tree, writes the object into `sys.objects` and the
    /// [`types.IndexMetadata`] (key and optional value columns) into
    /// `sys.indexes` with two logged inserts, and registers the index in the
    /// catalog and caches. Takes ownership of `key_columns` / `value_columns`.
    /// Does not backfill existing rows; population happens on subsequent writes
    /// and during recovery's index rebuild.
    pub fn createIndex(self: *Database, name: []const u8, table_name: []const u8, kind: types.IndexKind, key_columns: []types.Column, value_columns: ?[]types.Column, tx_id: u64) !table_mod.Index {
        const table = for (self.catalog.tables.items) |tbl| {
            if (std.mem.eql(u8, tbl.name, table_name)) break tbl;
        } else return error.TableNotFound;

        const index_tree = try BPlusTree.create(self.pool, self.allocator);
        defer index_tree.deinit();

        const index_id = @as(u32, @intCast(self.catalog.indexes.items.len + 1));
        const index = table_mod.Index{
            .id = index_id,
            .name = try self.allocator.dupe(u8, name),
            .table_id = table.id,
            .kind = kind,
            .key_columns = key_columns,
            .value_columns = value_columns,
            // A freshly built index (backfilled from current visible rows, or
            // rebuilt during recovery) contains one entry per live row and no
            // stale entries, so it starts exact. A later delete/update flips it.
            .exact = true,
            .allocator = self.allocator,
        };

        var k_meta = try self.allocator.alloc(types.ColumnMetadata, key_columns.len);
        defer self.allocator.free(k_meta);
        for (key_columns, 0..) |col, i| {
            k_meta[i] = .{
                .name = col.name,
                .type = col.type,
                .size = col.size,
                .offset = col.offset,
                .is_primary_key = col.is_primary_key,
                .is_auto_increment = col.is_auto_increment,
                .is_nullable = col.is_nullable,
                .default_value = col.default_value,
            };
        }

        var v_meta: ?[]types.ColumnMetadata = null;
        if (value_columns) |vcols| {
            var vm = try self.allocator.alloc(types.ColumnMetadata, vcols.len);
            for (vcols, 0..) |col, i| {
                vm[i] = .{
                    .name = col.name,
                    .type = col.type,
                    .size = col.size,
                    .offset = col.offset,
                    .is_primary_key = col.is_primary_key,
                    .is_auto_increment = col.is_auto_increment,
                    .is_nullable = col.is_nullable,
                    .default_value = col.default_value,
                };
            }
            v_meta = vm;
        }
        defer if (v_meta) |vm| self.allocator.free(vm);

        const meta = types.IndexMetadata{
            .id = index.id,
            .name = index.name,
            .table_id = index.table_id,
            .kind = index.kind,
            .key_columns = k_meta,
            .value_columns = v_meta,
            .root_page_id = index_tree.root_page_id,
            .exact = true,
        };

        const meta_bytes = try meta.serialize(self.allocator);
        defer self.allocator.free(meta_bytes);

        const obj = types.ObjectMetadata{
            .id = @intCast(self.table_roots.count() + self.index_roots.count() + 1),
            .name = name,
            .type = "INDEX",
            .root_page_id = index_tree.root_page_id,
        };
        const obj_bytes = try obj.serialize(self.allocator);
        defer self.allocator.free(obj_bytes);
        const lsn1 = self.reserveLsn();
        const lsn2 = self.reserveLsn();
        try self.master_tree.insert(name, obj_bytes);

        const indexes_root = self.table_roots.get("sys.indexes") orelse return error.SystemTableNotFound;
        var indexes_tree = try BPlusTree.init(self.pool, indexes_root, self.allocator);
        defer indexes_tree.deinit();
        try indexes_tree.insert(name, meta_bytes);

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn1,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .insert,
                .table_name = "sys.objects",
                .key = name,
                .value = obj_bytes,
            });

            try wal.append(.{
                .lsn = lsn2,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .insert,
                .table_name = "sys.indexes",
                .key = name,
                .value = meta_bytes,
            });
        }

        try self.catalog.addIndex(index);
        try self.index_roots.put(try self.allocator.dupe(u8, name), index_tree.root_page_id);
        index_tree.is_cached = true;
        try self.index_trees.put(try self.allocator.dupe(u8, name), index_tree);

        return index;
    }

    /// Returns the cached live [`BPlusTree`] for a table, or
    /// [`error.TableNotFound`]. The pointer is owned by the cache; do not deinit
    /// it (callers that `defer tree.deinit()` rely on a cached tree treating
    /// deinit as a no-op).
    pub fn getTableTree(self: *Database, name: []const u8) !*BPlusTree {
        return self.table_trees.get(name) orelse error.TableNotFound;
    }




    // --- Durable document writes (slice D-6) ---------------------------------
    // Document writes reuse the SQL WAL machinery: each write runs in its own
    // transaction (begin + op + commit records), keyed by the collection name and
    // the 12-byte _id, so recovery redoes committed document writes exactly as it
    // redoes table DML. Collection names and table names live in disjoint maps, so
    // the record's `table_name` classifies it unambiguously at replay.

























    // --- Document MVCC primitive (slice G3-a1) --------------------------------
    // A document is stored as a version chain, exactly like a row: the leaf cell
    // holds the newest `row.DecodedVersion` (fixed = BSON, heap = ""), older
    // versions live in the undo log via `roll_ptr`. This reuses the SQL version
    // machinery unchanged; only the payload is opaque BSON instead of typed
    // columns. See docs/nosql-mvcc-design.md.





    /// Returns the cached live [`BPlusTree`] for an index, or
    /// [`error.IndexNotFound`]. Same ownership caveat as [`Database.getTableTree`].
    pub fn getIndexTree(self: *Database, name: []const u8) !*BPlusTree {
        return self.index_trees.get(name) orelse error.IndexNotFound;
    }

    /// Drops a table: removes it from the catalog, both system tables, and caches,
    /// and logs the deletes.
    ///
    /// Deletes the `sys.objects` and `sys.tables` rows (two logged deletes),
    /// frees the owned root-map key, tears down and frees the cached tree, and
    /// removes the table from the in-memory catalog. The underlying data pages are
    /// not explicitly reclaimed here beyond dropping the root reference.
    /// [`error.TableNotFound`] if the table is unknown.
    /// Frees every page of the B+Tree rooted at `root_id` back to the pager's
    /// free list, so a DROP actually reclaims the space instead of stranding the
    /// whole tree (the file then stops growing across drop/recreate churn).
    ///
    /// Walks the tree with an explicit stack: an internal node queues its
    /// `leftmost_child_id` plus the child id in every separator cell; a leaf frees
    /// each overflow-flagged cell's chain. Child ids and overflow heads are copied
    /// out while the page is pinned, then the page is unpinned and discarded
    /// (`discardPage` requires no outstanding pin). A zero root is a no-op. This
    /// is NOT WAL-logged: a crash mid-drop can leak the not-yet-freed pages (the
    /// pager persists its free list only at a checkpoint), which degrades to
    /// wasted space, never corruption. Undo pages of prior row versions, if any,
    /// are not reached here and remain a separate reclamation concern.
    fn freeTreePages(self: *Database, root_id: PageId) !void {
        if (root_id == 0) return;
        var stack = std.ArrayList(PageId).empty;
        defer stack.deinit(self.allocator);
        var children = std.ArrayList(PageId).empty;
        defer children.deinit(self.allocator);
        var ovf = std.ArrayList(PageId).empty;
        defer ovf.deinit(self.allocator);
        try stack.append(self.allocator, root_id);
        while (stack.pop()) |pid| {
            children.clearRetainingCapacity();
            ovf.clearRetainingCapacity();
            // An unreadable page (bad checksum / beyond EOF) is skipped rather than
            // crashing the drop: leak it, do not fault.
            const frame = self.pool.fetchPage(pid) catch continue;
            {
                const p = self.pool.pageOf(frame);
                const h = p.headerPtr();
                switch (h.page_type) {
                    .internal => {
                        if (h.leftmost_child_id != 0) try children.append(self.allocator, h.leftmost_child_id);
                        var i: u16 = 0;
                        while (i < h.num_cells) : (i += 1) {
                            const c = p.getCell(i) orelse continue;
                            if (c.value.len >= @sizeOf(PageId))
                                try children.append(self.allocator, std.mem.readInt(PageId, c.value[0..@sizeOf(PageId)], .little));
                        }
                    },
                    .leaf => {
                        var i: u16 = 0;
                        while (i < h.num_cells) : (i += 1) {
                            const c = p.getCell(i) orelse continue;
                            if (c.flags.value_overflow and c.value.len >= overflow.OverflowDescriptor.SIZE) {
                                const d = overflow.OverflowDescriptor.decode(c.value);
                                try ovf.append(self.allocator, d.first_page_id);
                            }
                        }
                    },
                    else => {},
                }
            }
            self.pool.unpinPage(pid, false);
            for (ovf.items) |ofp| overflow.freeChain(self.pool, ofp) catch {};
            self.pool.discardPage(pid) catch {};
            for (children.items) |ch| try stack.append(self.allocator, ch);
        }
    }

    pub fn dropTable(self: *Database, name: []const u8, tx_id: u64) !void {
        const root_id = self.table_roots.get(name) orelse return error.TableNotFound;

        // Cascade: drop every secondary index of this table first (each frees its
        // own tree pages), so a later CREATE of the same name does not collide on
        // index names and the index storage is reclaimed too. Collect the names
        // up front because dropIndex mutates the catalog.
        {
            var tbl_id: ?u32 = null;
            for (self.catalog.tables.items) |tbl| {
                if (std.mem.eql(u8, tbl.name, name)) {
                    tbl_id = tbl.id;
                    break;
                }
            }
            if (tbl_id) |tid| {
                var idx_names = std.ArrayList([]const u8).empty;
                defer {
                    for (idx_names.items) |n| self.allocator.free(n);
                    idx_names.deinit(self.allocator);
                }
                for (self.catalog.indexes.items) |idx| {
                    if (idx.table_id == tid) try idx_names.append(self.allocator, try self.allocator.dupe(u8, idx.name));
                }
                for (idx_names.items) |n| self.dropIndex(n, name, tx_id) catch {};
            }
        }
        const lsn1 = self.reserveLsn();
        const lsn2 = self.reserveLsn();
        try self.master_tree.delete(name);

        const tables_root = self.table_roots.get("sys.tables") orelse return error.SystemTableNotFound;
        var tables_tree = try BPlusTree.init(self.pool, tables_root, self.allocator);
        defer tables_tree.deinit();
        try tables_tree.delete(name);

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn1,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .delete,
                .table_name = "sys.objects",
                .key = name,
                .value = "",
            });

            try wal.append(.{
                .lsn = lsn2,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .delete,
                .table_name = "sys.tables",
                .key = name,
                .value = "",
            });
        }

        if (self.table_roots.fetchRemove(name)) |entry| {
            self.allocator.free(entry.key);
        }
        if (self.table_trees.fetchRemove(name)) |entry| {
            const tree = entry.value;
            tree.is_cached = false;
            tree.deinit();
            self.allocator.free(entry.key);
        }

        var table_idx: ?usize = null;
        for (self.catalog.tables.items, 0..) |tbl, i| {
            if (std.mem.eql(u8, tbl.name, name)) {
                table_idx = i;
                break;
            }
        }
        if (table_idx) |idx| {
            var tbl = self.catalog.tables.swapRemove(idx);
            tbl.deinit();
        }
        // Reclaim the base tree's pages (was previously stranded).
        self.freeTreePages(root_id) catch {};
    }

    /// Drops an index, mirroring [`Database.dropTable`] for `sys.indexes`.
    ///
    /// Removes the `sys.objects` and `sys.indexes` rows (two logged deletes),
    /// frees the owned root-map key, tears down and frees the cached index tree,
    /// and removes the index from the catalog. `table_name` is accepted for API
    /// symmetry but unused (the index name is globally unique).
    /// [`error.IndexNotFound`] if the index is unknown.
    pub fn dropIndex(self: *Database, name: []const u8, table_name: []const u8, tx_id: u64) !void {
        _ = table_name;
        const root_id = self.index_roots.get(name) orelse return error.IndexNotFound;
        const lsn1 = self.reserveLsn();
        const lsn2 = self.reserveLsn();
        try self.master_tree.delete(name);

        const indexes_root = self.table_roots.get("sys.indexes") orelse return error.SystemTableNotFound;
        var indexes_tree = try BPlusTree.init(self.pool, indexes_root, self.allocator);
        defer indexes_tree.deinit();
        try indexes_tree.delete(name);

        if (self.wal) |wal| {
            try wal.append(.{
                .lsn = lsn1,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .delete,
                .table_name = "sys.objects",
                .key = name,
                .value = "",
            });

            try wal.append(.{
                .lsn = lsn2,
                .tx_id = tx_id,
                .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                .kind = .delete,
                .table_name = "sys.indexes",
                .key = name,
                .value = "",
            });
        }

        if (self.index_roots.fetchRemove(name)) |entry| {
            self.allocator.free(entry.key);
        }
        if (self.index_trees.fetchRemove(name)) |entry| {
            const tree = entry.value;
            tree.is_cached = false;
            tree.deinit();
            self.allocator.free(entry.key);
        }

        var index_idx: ?usize = null;
        for (self.catalog.indexes.items, 0..) |idx, i| {
            if (std.mem.eql(u8, idx.name, name)) {
                index_idx = i;
                break;
            }
        }
        if (index_idx) |idx| {
            var idx_val = self.catalog.indexes.swapRemove(idx);
            idx_val.deinit();
        }
        // Reclaim the index tree's pages (was previously stranded).
        self.freeTreePages(root_id) catch {};
    }

    /// Records that a table's B+Tree root has moved to a new page.
    ///
    /// A root split changes the root page id, and that new id must be persisted in
    /// both `sys.objects` and `sys.tables` (and the in-memory `table_roots` map)
    /// or a reopen would read a stale root. Serialised by
    /// [`Database.catalog_mutex`]; a no-op if the root is unchanged; both metadata
    /// rewrites are logged to the WAL.
    pub fn updateTableRootPageId(self: *Database, name: []const u8, new_root_id: PageId, tx_id: u64) !void {
        self.catalog_mutex.lockUncancelable(self.pool.pager.io);
        defer self.catalog_mutex.unlock(self.pool.pager.io);

        const entry = self.table_roots.getEntry(name) orelse return error.TableNotFound;
        if (entry.value_ptr.* == new_root_id) return;
        entry.value_ptr.* = new_root_id;

        const obj = types.ObjectMetadata{
            .id = 0,
            .name = name,
            .type = "TABLE",
            .root_page_id = new_root_id,
        };
        const obj_bytes = try obj.serialize(self.allocator);
        defer self.allocator.free(obj_bytes);
        const lsn1 = self.reserveLsn();
        const lsn2 = self.reserveLsn();
        try self.master_tree.delete(name);
        try self.master_tree.insert(name, obj_bytes);

        const tables_tree = self.table_trees.get("sys.tables") orelse return error.SystemTableNotFound;

        const opt_meta_bytes = try tables_tree.search(name, self.allocator);
        if (opt_meta_bytes) |meta_bytes| {
            defer self.allocator.free(meta_bytes);
            var existing_meta = try types.TableMetadata.deserialize(self.allocator, meta_bytes);
            defer {
                for (existing_meta.columns) |col| {
                    self.allocator.free(col.name);
                    if (col.default_value) |dv| self.allocator.free(dv);
                }
                self.allocator.free(existing_meta.columns);
                self.allocator.free(existing_meta.name);
            }
            
            existing_meta.root_page_id = new_root_id;
            const new_meta_bytes = try existing_meta.serialize(self.allocator);
            defer self.allocator.free(new_meta_bytes);
            try tables_tree.delete(name);
            try tables_tree.insert(name, new_meta_bytes);

            if (self.wal) |wal| {
                try wal.append(.{
                    .lsn = lsn1,
                    .tx_id = tx_id,
                    .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                    .kind = .insert,
                    .table_name = "sys.objects",
                    .key = name,
                    .value = obj_bytes,
                });

                try wal.append(.{
                    .lsn = lsn2,
                    .tx_id = tx_id,
                    .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                    .kind = .insert,
                    .table_name = "sys.tables",
                    .key = name,
                    .value = new_meta_bytes,
                });
            }
        }
    }

    /// Marks every secondary index on `table_id` as no-longer-exact, in memory
    /// and in the persisted `sys.indexes` record, because a delete or update has
    /// (or may have) left a stale index entry behind under MVCC. Cheap and
    /// idempotent: an index already flagged inexact is skipped, so the persisted
    /// rewrite happens at most once per index until it is rebuilt. Called by the
    /// executor's delete/update paths. Flipping to `false` is the safe direction,
    /// so doing it at statement time is correct even if the statement's
    /// transaction later rolls back (an unnecessarily-pessimistic flag only costs
    /// the index-only-count fast path, never correctness). See
    /// [`types.IndexMetadata.exact`].
    pub fn markTableIndexesInexact(self: *Database, table_id: u32) !void {
        const indexes_root = self.table_roots.get("sys.indexes") orelse return;
        for (self.catalog.indexes.items) |*idx| {
            if (idx.table_id != table_id or !idx.exact) continue;
            idx.exact = false;

            var indexes_tree = try BPlusTree.init(self.pool, indexes_root, self.allocator);
            defer indexes_tree.deinit();
            const opt_meta_bytes = try indexes_tree.search(idx.name, self.allocator);
            const meta_bytes = opt_meta_bytes orelse continue;
            defer self.allocator.free(meta_bytes);
            var meta = try types.IndexMetadata.deserialize(self.allocator, meta_bytes);
            defer {
                for (meta.key_columns) |col| {
                    self.allocator.free(col.name);
                    if (col.default_value) |dv| self.allocator.free(dv);
                }
                self.allocator.free(meta.key_columns);
                if (meta.value_columns) |vcols| {
                    for (vcols) |col| {
                        self.allocator.free(col.name);
                        if (col.default_value) |dv| self.allocator.free(dv);
                    }
                    self.allocator.free(vcols);
                }
                self.allocator.free(meta.name);
            }
            if (!meta.exact) continue; // already persisted inexact
            meta.exact = false;
            const new_bytes = try meta.serialize(self.allocator);
            defer self.allocator.free(new_bytes);
            try indexes_tree.delete(idx.name);
            try indexes_tree.insert(idx.name, new_bytes);
        }
    }

    /// Records that an index's B+Tree root has moved to a new page.
    ///
    /// The index counterpart of [`Database.updateTableRootPageId`]: updates
    /// `index_roots`, `sys.objects` and `sys.indexes` and logs both rewrites, so a
    /// root split of an index survives a reopen. A no-op if the root is unchanged;
    /// [`error.IndexNotFound`] if the index is unknown.
    pub fn updateIndexRootPageId(self: *Database, name: []const u8, new_root_id: PageId, tx_id: u64) !void {
        const old_root = self.index_roots.get(name) orelse return error.IndexNotFound;
        if (old_root == new_root_id) return;
        
        try self.index_roots.put(try self.allocator.dupe(u8, name), new_root_id);

        const obj = types.ObjectMetadata{
            .id = 0,
            .name = name,
            .type = "INDEX",
            .root_page_id = new_root_id,
        };
        const obj_bytes = try obj.serialize(self.allocator);
        defer self.allocator.free(obj_bytes);
        const lsn1 = self.reserveLsn();
        const lsn2 = self.reserveLsn();
        try self.master_tree.delete(name);
        try self.master_tree.insert(name, obj_bytes);

        const indexes_tree = self.table_trees.get("sys.indexes") orelse return error.SystemTableNotFound;

        const opt_meta_bytes = try indexes_tree.search(name, self.allocator);
        if (opt_meta_bytes) |meta_bytes| {
            defer self.allocator.free(meta_bytes);
            var existing_meta = try types.IndexMetadata.deserialize(self.allocator, meta_bytes);
            defer {
                for (existing_meta.key_columns) |col| {
                    self.allocator.free(col.name);
                    if (col.default_value) |dv| self.allocator.free(dv);
                }
                self.allocator.free(existing_meta.key_columns);
                if (existing_meta.value_columns) |vcols| {
                    for (vcols) |col| {
                        self.allocator.free(col.name);
                        if (col.default_value) |dv| self.allocator.free(dv);
                    }
                    self.allocator.free(vcols);
                }
                self.allocator.free(existing_meta.name);
            }
            
            existing_meta.root_page_id = new_root_id;
            const new_meta_bytes = try existing_meta.serialize(self.allocator);
            defer self.allocator.free(new_meta_bytes);
            try indexes_tree.delete(name);
            try indexes_tree.insert(name, new_meta_bytes);

            if (self.wal) |wal| {
                try wal.append(.{
                    .lsn = lsn1,
                    .tx_id = tx_id,
                    .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                    .kind = .insert,
                    .table_name = "sys.objects",
                    .key = name,
                    .value = obj_bytes,
                });

                try wal.append(.{
                    .lsn = lsn2,
                    .tx_id = tx_id,
                    .timestamp = std.Io.Clock.now(.real, self.pool.pager.io).toMilliseconds(),
                    .kind = .insert,
                    .table_name = "sys.indexes",
                    .key = name,
                    .value = new_meta_bytes,
                });
            }
        }
    }
};
