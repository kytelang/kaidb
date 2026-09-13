//! Slotted-page B+Tree: the ordered index at the heart of NovaDB's storage engine.
//!
//! This file implements the B+Tree that every table and secondary index in
//! NovaDB is built on. Keys and values are variable-length byte slices; the
//! tree keeps keys in sorted order and links all leaves in a singly-linked
//! chain (`next_page_id`) so that a range scan is a leftmost descent followed
//! by a straight walk along the leaf chain, never revisiting internal nodes.
//! Lookups, inserts, and range scans are all O(log n) in the number of keys.
//!
//! ## Page layout and node roles
//!
//! Every node is one [`page.PAGE_SIZE`] page managed by the buffer pool
//! ([`PagePool`]). A page is a slotted page (see `page.zig`): a header, a slot
//! directory that grows down, and cell payloads that grow up, so both free
//! lists share the middle gap. Two node kinds share the same physical format:
//!
//!   * `leaf` nodes hold the real `key -> value` cells in sorted order and a
//!     `next_page_id` pointer to the next leaf, forming the scan chain.
//!   * `internal` nodes hold `separator_key -> child_page_id` cells plus a
//!     `leftmost_child_id` in the header. A separator key routes any search
//!     key `>=` it (but `<` the next separator) into that child; keys below
//!     the first separator go to `leftmost_child_id`. Child ids are stored as
//!     little-endian [`PageId`] in the cell value (see [`mem.readInt`] usages).
//!
//! Values larger than [`overflow.OVERFLOW_THRESHOLD`] are not stored inline:
//! [`overflow.writeChain`] spills them to a chain of overflow pages and the
//! leaf cell instead holds a fixed-size [`overflow.OverflowDescriptor`], with
//! `CellFlags.value_overflow` set so readers know to follow the chain.
//!
//! ## Concurrency: the two-level latching protocol
//!
//! This tree is designed for concurrent access and uses TWO independent locks,
//! matching the protocol documented in `architecture.md`:
//!
//!   1. A per-tree [`sync.RwLock`] `structure_lock`. It is held SHARED for
//!      operations that only touch a single leaf in place (point read, in-place
//!      update, non-splitting insert, non-merging delete) and EXCLUSIVE for any
//!      operation that changes the shape of the tree (a split, a merge, a root
//!      change). This is what makes concurrent writers on one tree safe: they
//!      may proceed in parallel as long as none of them restructures, and the
//!      moment one must split or merge it upgrades to the exclusive path.
//!   2. Per-frame latches ([`Frame.latch`]) taken during the root-to-leaf
//!      descent. The descent uses latch crabbing: it locks the child before
//!      releasing the parent, so the path can never be observed half-modified.
//!      [`findLeafShared`] crabs with shared latches, [`findLeafExclusive`]
//!      with exclusive latches, and [`findLeafOptimistic`] takes NO latches on
//!      internal nodes (betting the leaf will not need restructuring) and only
//!      latches the leaf at the end.
//!
//! The fast paths in [`insert`] and [`delete`] first try the optimistic route
//! under a SHARED `structure_lock`: descend without internal latches, latch the
//! leaf, and if the single-leaf mutation fits (has space / stays above the
//! underflow limit) commit it there. If it does not fit, they drop everything,
//! retake `structure_lock` EXCLUSIVE, and redo the work through
//! [`insertExclusive`] / [`deleteExclusive`], which are allowed to split, merge,
//! borrow, and move the root. Pages are pinned in the pool for the duration of
//! any access and unpinned (with the dirty flag) exactly once on every path.
//!
//! `MAX_TREE_DEPTH` bounds every descent and every recursive split/merge so a
//! corrupted parent pointer that forms a cycle fails with
//! [`BPlusTree.BTreeError.TreeTooDeepOrCyclic`] instead of looping forever.
//!
//! ## Durability and rebalancing
//!
//! The tree mutates pages through the pool; the WAL and checkpoint machinery
//! live in `durability/` and observe the dirty frames this file produces. On
//! delete, a leaf that drops below half full triggers [`handleUnderflow`],
//! which prefers borrowing a single cell from a sibling that can spare one and
//! otherwise [`mergePages`] two siblings and recurses upward, collapsing the
//! root when it empties. Splits ([`splitAndInsert`]) push a promoted separator
//! up through [`insertIntoParent`], growing a new root when the old root splits.
//! Rebalancing rebuilds cell lists in the per-tree `scratch` arena, which is
//! reset after each exclusive operation.

const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const page_pool = @import("pool.zig");
const page = @import("page.zig");
const overflow = @import("overflow.zig");
const sync = @import("utils").sync;
const PageId = page.PageId;
const PAGE_SIZE = page.PAGE_SIZE;
const Header = page.Header;
const Cell = page.Cell;
const CellFlags = page.CellFlags;
const readHeader = page.readHeader;
const writeHeader = page.writeHeader;
const MAGIC = page.MAGIC;
const VERSION = page.VERSION;

/// Buffer-pool type that owns the page cache and pins/unpins frames.
const PagePool = page_pool.PagePool;
/// One pinned in-memory page slot returned by the pool; carries the page
/// bytes, its owning [`PageId`], and the per-page [`Frame.latch`].
const Frame = page_pool.Frame;


/// Hard cap on tree height and on split/merge recursion depth.
///
/// Every descent and every recursive restructure counts levels against this
/// bound and aborts with [`BPlusTree.BTreeError.TreeTooDeepOrCyclic`] once it is
/// exceeded. A healthy B+Tree is never this deep, so hitting the cap means a
/// parent/child pointer has been corrupted into a cycle; the bound turns an
/// otherwise infinite loop into a clean error.
const MAX_TREE_DEPTH: u32 = 32;

/// Forward, shared-latch cursor over the whole leaf chain.
///
/// Yields cells in key order by walking `next_page_id` from a starting leaf.
/// It holds exactly one leaf latched-shared and pinned at a time (recorded in
/// `pinned_page_id`); [`Iterator.next`] hands the current leaf's latch to the
/// next one before releasing, and [`Iterator.deinit`] must be called to drop
/// the final latch/pin. Because it keeps a shared latch, concurrent writers
/// that would restructure block against it, so an iteration should be short.
pub const Iterator = struct {
    /// Tree being scanned; used to reach the pool and its I/O context.
    tree: *BPlusTree,
    /// The leaf frame currently being read from.
    current_frame: *Frame,
    /// Index of the next cell to return within `current_frame`.
    current_index: u16,
    /// Page id of the leaf currently latched-shared and pinned, or `null`
    /// once released (after exhaustion or in [`Iterator.deinit`]).
    pinned_page_id: ?PageId,

    /// Returns the next cell in key order, or `null` at the end of the chain.
    ///
    /// Emits every remaining cell on the current leaf, then follows
    /// `next_page_id`: it releases the current leaf's shared latch and pin only
    /// after fetching and latching the next leaf, so the scan never sees a gap.
    /// Skips slot-directory holes where [`page.Page.getCell`] returns `null`.
    /// Returns `null` (and leaves nothing pinned) when `next_page_id == 0`.
    pub fn next(self: *Iterator) !?Cell {
        while (true) {
            const p = self.tree.pool.pageOf(self.current_frame);
            while (self.current_index < p.headerPtr().num_cells) {
                const i = self.current_index;
                self.current_index += 1;
                if (p.getCell(i)) |c| return c;
            }
            const next_id = p.headerPtr().next_page_id;
            if (self.pinned_page_id) |pid| {
                self.current_frame.latch.unlockShared(self.tree.pool.pager.io);
                self.tree.pool.unpinPage(pid, false);
                self.pinned_page_id = null;
            }
            if (next_id == 0) return null;
            self.current_frame = try self.tree.pool.fetchPage(next_id);
            self.current_frame.latch.lockShared(self.tree.pool.pager.io);
            self.pinned_page_id = next_id;
            self.current_index = 0;
        }
    }

    /// Releases any leaf still held (shared latch + pin) when the scan is
    /// abandoned before exhaustion. Safe to call after [`Iterator.next`] has
    /// already returned `null`, since that path clears `pinned_page_id`.
    pub fn deinit(self: *Iterator) void {
        if (self.pinned_page_id) |pid| {
            self.current_frame.latch.unlockShared(self.tree.pool.pager.io);
            self.tree.pool.unpinPage(pid, false);
        }
    }
};

/// Forward cursor that pins a small window of leaves ahead of the read point.
///
/// Same key-order walk as [`Iterator`], but it keeps up to `PREFETCH_DEPTH`
/// upcoming leaves already fetched (pinned) in `prefetch_buffer`, so a large
/// sequential scan overlaps page fetches with consumption instead of stalling
/// on each `next_page_id` hop. Unlike [`Iterator`] it does NOT hold leaf
/// latches, only pins, so it must not be used where a concurrent restructure
/// could move the cells out from under it; it is meant for read-mostly bulk
/// scans. [`PrefetchIterator.deinit`] unpins the current leaf and the whole
/// prefetch window.
pub const PrefetchIterator = struct {
    /// Tree being scanned; used to reach the pool.
    tree: *BPlusTree,
    /// The leaf frame currently being read from.
    current_frame: *Frame,
    /// Index of the next cell to return within `current_frame`.
    current_index: u16,
    /// Ring of already-fetched, pinned leaves ahead of `current_frame`, in
    /// chain order; `prefetch_buffer[0]` is the immediate next leaf.
    prefetch_buffer: [PREFETCH_DEPTH]*Frame,
    /// Number of valid entries in `prefetch_buffer` (0..`PREFETCH_DEPTH`).
    prefetch_count: u8,
    /// Allocator captured at construction (currently unused by the walk; kept
    /// so the cursor can be extended without changing its constructors).
    allocator: Allocator,

    /// How many leaves to keep fetched ahead of the current read position.
    const PREFETCH_DEPTH = 4;

    /// Builds a prefetch cursor starting at `start_frame` and eagerly fills the
    /// prefetch window via [`PrefetchIterator.prefetchAhead`].
    ///
    /// `start_frame` must already be fetched/pinned by the caller; ownership of
    /// that pin transfers to the returned cursor.
    pub fn init(tree: *BPlusTree, start_frame: *Frame, allocator: Allocator) !PrefetchIterator {
        var self = PrefetchIterator{
            .tree = tree,
            .current_frame = start_frame,
            .current_index = 0,
            .prefetch_buffer = undefined,
            .prefetch_count = 0,
            .allocator = allocator,
        };
        try self.prefetchAhead();
        return self;
    }

    /// Fills `prefetch_buffer` by following `next_page_id` from the current
    /// leaf, fetching (pinning) up to `PREFETCH_DEPTH` successors or until the
    /// chain ends. Called once at construction to prime the window; the steady
    /// state is maintained incrementally inside [`PrefetchIterator.next`].
    fn prefetchAhead(self: *PrefetchIterator) !void {
        const cur_page = self.tree.pool.pageOf(self.current_frame);
        var pid = cur_page.headerPtr().next_page_id;
        var count: u8 = 0;
        while (count < PREFETCH_DEPTH and pid != 0) : (count += 1) {
            const f = try self.tree.pool.fetchPage(pid);
            self.prefetch_buffer[count] = f;
            pid = self.tree.pool.pageOf(f).headerPtr().next_page_id;
        }
        self.prefetch_count = count;
    }

    /// Returns the next cell in key order, or `null` at the end of the chain.
    ///
    /// When the current leaf is exhausted it unpins it, promotes
    /// `prefetch_buffer[0]` to current, shifts the window down by one, and (if
    /// room remains) fetches one more leaf from beyond the window's tail so the
    /// prefetch depth is maintained. Returns `null` once the window is empty and
    /// the chain has no successor.
    pub fn next(self: *PrefetchIterator) !?Cell {
        while (true) {
            const p = self.tree.pool.pageOf(self.current_frame);
            while (self.current_index < p.headerPtr().num_cells) {
                const i = self.current_index;
                self.current_index += 1;
                if (p.getCell(i)) |c| return c;
            }
            self.tree.pool.unpinPage(self.current_frame.page_id.?, false);
            if (self.prefetch_count == 0) return null;

            self.current_frame = self.prefetch_buffer[0];
            self.current_index = 0;

            var i: u8 = 0;
            while (i < self.prefetch_count - 1) : (i += 1)
                self.prefetch_buffer[i] = self.prefetch_buffer[i + 1];
            self.prefetch_count -= 1;

            if (self.prefetch_count < PREFETCH_DEPTH) {
                const last = if (self.prefetch_count > 0)
                    self.prefetch_buffer[self.prefetch_count - 1]
                else
                    self.current_frame;
                const nxt = self.tree.pool.pageOf(last).headerPtr().next_page_id;
                if (nxt != 0) {
                    self.prefetch_buffer[self.prefetch_count] = try self.tree.pool.fetchPage(nxt);
                    self.prefetch_count += 1;
                }
            }
        }
    }

    /// Unpins the current leaf and every leaf still held in the prefetch
    /// window. Must be called to avoid leaking pool pins when the scan is
    /// dropped before it is fully consumed.
    pub fn deinit(self: *PrefetchIterator) void {
        self.tree.pool.unpinPage(self.current_frame.page_id.?, false);
        var i: u8 = 0;
        while (i < self.prefetch_count) : (i += 1)
            self.tree.pool.unpinPage(self.prefetch_buffer[i].page_id.?, false);
    }
};

/// Forward, shared-latch cursor bounded by an inclusive upper key.
///
/// Behaves like [`Iterator`] but stops as soon as a cell's key sorts past
/// `end_key`, so `[start_key, end_key]` range queries do not walk the rest of
/// the chain. A `null` `end_key` means unbounded (scan to the end). Produced by
/// [`BPlusTree.rangeScan`], which positions `current_index` at the first key
/// `>= start_key`. Holds one leaf latched-shared and pinned at a time; call
/// [`RangeIterator.deinit`] to release it.
pub const RangeIterator = struct {
    /// Tree being scanned; used to reach the pool and its I/O context.
    tree: *BPlusTree,
    /// The leaf frame currently being read from.
    current_frame: *Frame,
    /// Index of the next cell to return within `current_frame`.
    current_index: u16,
    /// Inclusive upper bound on the key, or `null` for an unbounded scan. The
    /// scan ends when a cell's key sorts `.gt` this slice.
    end_key: ?[]const u8,
    /// Page id of the leaf currently latched-shared and pinned, or `null` once
    /// released.
    pinned_page_id: ?PageId,

    /// Returns the next cell whose key is `<= end_key`, or `null` when the
    /// range or the leaf chain is exhausted.
    ///
    /// Emits cells from the current leaf until it hits `end_key` or runs out,
    /// then crabs to the next leaf exactly as [`Iterator.next`] does. The
    /// `end_key` check happens per cell, so the very first key past the bound
    /// terminates the scan (returning `null` without advancing further).
    pub fn next(self: *RangeIterator) !?Cell {
        while (true) {
            const p = self.tree.pool.pageOf(self.current_frame);
            while (self.current_index < p.headerPtr().num_cells) {
                const i = self.current_index;
                self.current_index += 1;
                const cell = p.getCell(i) orelse continue;
                if (self.end_key) |ek| {
                    if (mem.order(u8, cell.key, ek) == .gt) return null;
                }
                return cell;
            }
            const next_id = p.headerPtr().next_page_id;
            if (self.pinned_page_id) |pid| {
                self.current_frame.latch.unlockShared(self.tree.pool.pager.io);
                self.tree.pool.unpinPage(pid, false);
                self.pinned_page_id = null;
            }
            if (next_id == 0) return null;
            self.current_frame = try self.tree.pool.fetchPage(next_id);
            self.current_frame.latch.lockShared(self.tree.pool.pager.io);
            self.pinned_page_id = next_id;
            self.current_index = 0;
        }
    }

    /// Releases the leaf still held (shared latch + pin) if the range scan is
    /// abandoned before it returns `null`.
    pub fn deinit(self: *RangeIterator) void {
        if (self.pinned_page_id) |pid| {
            self.current_frame.latch.unlockShared(self.tree.pool.pager.io);
            self.tree.pool.unpinPage(pid, false);
        }
    }
};

/// Backward, shared-latch cursor over an inclusive key range `[start_key,
/// end_key]`, yielding cells from HIGHEST key to lowest.
///
/// The forward [`RangeIterator`] serves `ORDER BY col ASC LIMIT k` with an early
/// stop; this is its mirror for `ORDER BY col DESC LIMIT k`. Leaves are only
/// singly linked (`next_page_id`), so "previous leaf" is reached by climbing to
/// the parent (`parent_page_id`, present in every node), finding the current
/// child's slot, stepping one child left, and descending to that subtree's
/// RIGHTMOST leaf (climbing further when the current node is its parent's
/// leftmost child). This adds NO storage-format field and touches only the read
/// path, so recovery is unaffected. Like [`PrefetchIterator`], the interior
/// climb/descent holds pins but not latches (it is meant for read-mostly scans;
/// a concurrent restructure could truncate it early, which is acceptable for the
/// SELECT paths that use it); the leaf currently being read is held
/// shared-latched and pinned. Call [`RangeIteratorDesc.deinit`] to release it.
pub const RangeIteratorDesc = struct {
    /// Tree being scanned.
    tree: *BPlusTree,
    /// The leaf frame currently being read from (shared-latched + pinned).
    current_frame: *Frame,
    /// Index of the next cell to return, walking DOWN; `< 0` = leaf exhausted.
    current_index: i32,
    /// Inclusive lower bound; the scan ends at the first key sorting `.lt` this.
    start_key: ?[]const u8,
    /// Inclusive upper bound; keys sorting `.gt` this are skipped (they sit
    /// above the range on the starting leaf). `null` = unbounded top.
    end_key: ?[]const u8,
    /// Page id of the leaf currently latched-shared and pinned, or `null`.
    pinned_page_id: ?PageId,

    /// Returns the next cell in DESCENDING key order within the range, or `null`
    /// when the lower bound or the left end of the tree is reached.
    pub fn next(self: *RangeIteratorDesc) !?Cell {
        while (true) {
            const p = self.tree.pool.pageOf(self.current_frame);
            while (self.current_index >= 0) {
                const i: u16 = @intCast(self.current_index);
                self.current_index -= 1;
                const cell = p.getCell(i) orelse continue;
                if (self.end_key) |ek| {
                    if (mem.order(u8, cell.key, ek) == .gt) continue; // above range
                }
                if (self.start_key) |sk| {
                    if (mem.order(u8, cell.key, sk) == .lt) return null; // below range: done
                }
                return cell;
            }
            // Leaf exhausted downward; release it and move to the previous leaf.
            const cur_id = self.pinned_page_id.?;
            self.current_frame.latch.unlockShared(self.tree.pool.pager.io);
            self.tree.pool.unpinPage(cur_id, false);
            self.pinned_page_id = null;

            const prev = try self.tree.prevLeafShared(cur_id);
            if (prev == null) return null;
            self.current_frame = prev.?;
            self.pinned_page_id = prev.?.page_id.?;
            self.current_index = @as(i32, @intCast(self.tree.pool.pageOf(prev.?).headerPtr().num_cells)) - 1;
        }
    }

    /// Releases the leaf still held if the scan is abandoned before `null`.
    pub fn deinit(self: *RangeIteratorDesc) void {
        if (self.pinned_page_id) |pid| {
            self.current_frame.latch.unlockShared(self.tree.pool.pager.io);
            self.tree.pool.unpinPage(pid, false);
        }
    }
};

/// A single B+Tree instance: one ordered index over variable-length key/value
/// cells, layered on a shared [`PagePool`].
///
/// The struct itself holds only the root page id and coordination state; all
/// node data lives in pool-managed pages. Access is governed by the two-level
/// latching protocol described in the file header: the per-tree
/// `structure_lock` (shared for in-place ops, exclusive for restructures) plus
/// per-frame latches during descent. Construct with [`BPlusTree.create`] (fresh
/// empty tree) or [`BPlusTree.init`] (attach to an existing root), and tear down
/// with [`BPlusTree.deinit`].
pub const BPlusTree = struct {
    /// Buffer pool backing this tree; owns page cache, pinning, and I/O.
    pool: *PagePool,
    /// Page id of the current root. Mutated (under `structure_lock` exclusive)
    /// when the root splits upward or collapses after a merge.
    root_page_id: PageId,
    /// Log sequence number stamp associated with this tree's mutations; used
    /// by the durability layer to order changes.
    lsn: u64 = 0,
    /// Allocator for the tree object and durable per-operation scratch (for
    /// example the temporary key buffer in [`BPlusTree.checkInvariants`]).
    allocator: Allocator,
    /// Arena reset (`.free_all`) after each exclusive operation. Restructures
    /// rebuild whole cell lists here so they never alias the live page bytes
    /// they are about to overwrite.
    scratch: std.heap.ArenaAllocator,
    /// When `true`, this tree is owned by a cache and [`BPlusTree.deinit`] is a
    /// no-op (the cache frees it), so callers can `deinit` freely.
    is_cached: bool = false,
    /// The per-tree structure lock. Shared = single-leaf in-place mutation may
    /// proceed concurrently; exclusive = a split/merge/root change is in
    /// progress and must be the only structural writer.
    structure_lock: sync.RwLock = .{},

    /// Errors returned by tree operations.
    pub const BTreeError = error{
        /// A point lookup/update/delete found no cell with the given key.
        KeyNotFound,
        /// An insert refused to overwrite an existing key (the tree enforces
        /// uniqueness; callers do read-modify-write for upserts).
        KeyAlreadyExists,
        /// A descent or a recursive split/merge exceeded [`MAX_TREE_DEPTH`],
        /// indicating a corrupted parent/child pointer cycle rather than a
        /// legitimately deep tree.
        TreeTooDeepOrCyclic,
        /// A page compaction could not reclaim enough space to proceed.
        CompactFailed,
        /// [`BPlusTree.update`] was asked to change a value that is (or would
        /// become) an overflow value; only in-place, inline updates are
        /// supported on the fast path, so callers must delete+insert instead.
        OverflowUpdateNotSupported,
    };

    /// Attaches a [`BPlusTree`] to an already-existing root page.
    ///
    /// Allocates the tree object and its scratch arena but does NOT create any
    /// pages; `root_page_id` must name a valid root already present in `pool`.
    /// Use [`BPlusTree.create`] for a brand-new, empty tree.
    pub fn init(pool: *PagePool, root_page_id: PageId, allocator: Allocator) !*BPlusTree {
        const self = try allocator.create(BPlusTree);
        errdefer allocator.destroy(self);
        self.* = .{
            .pool = pool,
            .root_page_id = root_page_id,
            .lsn = 0,
            .allocator = allocator,
            .scratch = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
        return self;
    }

    /// Creates a fresh, empty tree by allocating a single leaf page as the
    /// root, then attaching to it via [`BPlusTree.init`].
    ///
    /// The new root page is marked dirty on unpin so the empty root is
    /// persisted. Returns the owned tree; free with [`BPlusTree.deinit`].
    pub fn create(pool: *PagePool, allocator: Allocator) !*BPlusTree {
        const rf = try pool.newPage(.leaf);
        const rid = rf.page_id.?;
        pool.unpinPage(rid, true);
        return try init(pool, rid, allocator);
    }

    /// Frees the tree object and its scratch arena.
    ///
    /// A no-op when `is_cached` is set (the owning cache is responsible for the
    /// memory). Does not touch pool pages: the on-disk tree outlives this
    /// in-memory handle.
    pub fn deinit(self: *BPlusTree) void {
        if (self.is_cached) return;
        self.scratch.deinit();
        self.allocator.destroy(self);
    }

    /// Number of internal levels between the root and the leaves (0 when the
    /// root is itself a leaf). Used to resolve a key to its leaf page id without
    /// reading the leaf. Reads only internal nodes plus the leftmost leaf, which
    /// are small and stay cached, so this is cheap to call once per batch. Must
    /// be called under a held shared `structure_lock` so the shape is stable.
    fn internalDepth(self: *BPlusTree) !u32 {
        var cur_id = self.root_page_id;
        var cur = try self.pool.fetchPage(cur_id);
        var d: u32 = 0;
        while (self.pool.pageOf(cur).headerPtr().page_type == .internal) {
            d += 1;
            if (d > MAX_TREE_DEPTH) {
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const next_id = self.pool.pageOf(cur).headerPtr().leftmost_child_id;
            self.pool.unpinPage(cur_id, false);
            cur = try self.pool.fetchPage(next_id);
            cur_id = next_id;
        }
        self.pool.unpinPage(cur_id, false);
        return d;
    }

    /// Leaf page id that would hold `key`, resolved by descending exactly
    /// `depth` internal levels (which must be [`internalDepth`]) and reading the
    /// last internal node's child pointer WITHOUT fetching the leaf itself. That
    /// is the whole point: it yields the page a subsequent point lookup will hit,
    /// so callers can prefetch it before paying the leaf read.
    fn leafPageIdForDepth(self: *BPlusTree, key: []const u8, depth: u32) !PageId {
        if (depth == 0) return self.root_page_id;
        var cur_id = self.root_page_id;
        var level: u32 = 0;
        while (true) {
            const cur = try self.pool.fetchPage(cur_id);
            const next_id = self.pool.pageOf(cur).findChildPageId(key);
            self.pool.unpinPage(cur_id, false);
            level += 1;
            if (level == depth) return next_id; // next_id is a leaf; do not fetch it
            cur_id = next_id;
        }
    }

    /// Best-effort: append the base-table leaf page ids that `keys` map to, for
    /// prefetch. Runs under a shared `structure_lock` (stable shape) and reads
    /// only internal nodes. Never fails the caller: on any structural error it
    /// stops early and the collected prefix is used as a prefetch hint. The ids
    /// are NOT deduped here; [`Pager.prefetchPages`] coalesces duplicates and
    /// contiguous runs when it issues the readahead.
    pub fn collectLeafPageIds(self: *BPlusTree, keys: []const []const u8, out: *std.ArrayList(PageId), a: Allocator) void {
        self.structure_lock.lockShared(self.pool.pager.io);
        defer self.structure_lock.unlockShared(self.pool.pager.io);
        const depth = self.internalDepth() catch return;
        for (keys) |k| {
            const pid = self.leafPageIdForDepth(k, depth) catch return;
            out.append(a, pid) catch return;
        }
    }

    /// Point lookup: returns a freshly-allocated copy of the value for `key`,
    /// or `null` if the key is absent.
    ///
    /// Runs entirely under a SHARED `structure_lock` and a shared-crabbed
    /// descent ([`findLeafShared`]), so it never blocks other readers. If the
    /// found cell is an overflow value, the chain is reassembled with
    /// [`overflow.readChain`]; otherwise the inline value is duplicated. The
    /// returned slice is owned by `allocator` and must be freed by the caller.
    pub fn search(self: *BPlusTree, key: []const u8, allocator: Allocator) !?[]const u8 {
        self.structure_lock.lockShared(self.pool.pager.io);
        defer self.structure_lock.unlockShared(self.pool.pager.io);

        const f = try self.findLeafShared(key);
        defer {
            f.latch.unlockShared(self.pool.pager.io);
            self.pool.unpinPage(f.page_id.?, false);
        }
        const p = self.pool.pageOf(f);
        const idx = p.findCellByKey(key) orelse return null;
        const cell = p.getCell(idx).?;

        if (cell.flags.value_overflow) {
            const desc = overflow.OverflowDescriptor.decode(cell.value);
            return try overflow.readChain(self.pool, allocator, desc.first_page_id, desc.total_len);
        }
        return try allocator.dupe(u8, cell.value);
    }

    /// A point-lookup cursor that reuses its current leaf across a run of keys.
    ///
    /// For a batch of lookups whose keys arrive in ascending order (e.g. the
    /// primary keys yielded by an equality index scan, which are pk-ordered), a
    /// large fraction of consecutive keys land on the same base leaf. This cursor
    /// keeps that leaf pinned + shared-latched and answers such lookups by a
    /// binary search within it, re-descending from the root only when a key falls
    /// outside the current leaf's key range. That turns N root-to-leaf descents
    /// into ~one-per-distinct-leaf, mirroring SQLite's `sqlite3BtreeTableMoveto`
    /// "start the search on the current page" fast path. The shared
    /// `structure_lock` is held for the cursor's whole lifetime (reads only), so
    /// no writer can restructure the tree mid-batch. Call [`deinit`] to release
    /// the held leaf and the lock.
    pub const LeafReuseSearcher = struct {
        tree: *BPlusTree,
        leaf: ?*Frame = null,

        pub fn deinit(self: *LeafReuseSearcher) void {
            self.releaseLeaf();
            self.tree.structure_lock.unlockShared(self.tree.pool.pager.io);
        }

        fn releaseLeaf(self: *LeafReuseSearcher) void {
            if (self.leaf) |f| {
                f.latch.unlockShared(self.tree.pool.pager.io);
                self.tree.pool.unpinPage(f.page_id.?, false);
                self.leaf = null;
            }
        }

        /// Owned value for `key`, or null if absent. Reuses the current leaf when
        /// `key` is within its `[min,max]`; otherwise re-descends from the root.
        pub fn get(self: *LeafReuseSearcher, key: []const u8, allocator: Allocator) !?[]const u8 {
            if (self.leaf) |f| {
                const p = self.tree.pool.pageOf(f);
                const n = p.headerPtr().num_cells;
                const in_range = n > 0 and
                    std.mem.order(u8, key, p.getCell(0).?.key) != .lt and
                    std.mem.order(u8, key, p.getCell(n - 1).?.key) != .gt;
                if (!in_range) self.releaseLeaf();
            }
            if (self.leaf == null) {
                self.leaf = try self.tree.findLeafShared(key);
            }
            const p = self.tree.pool.pageOf(self.leaf.?);
            const idx = p.findCellByKey(key) orelse return null;
            const cell = p.getCell(idx).?;
            if (cell.flags.value_overflow) {
                const desc = overflow.OverflowDescriptor.decode(cell.value);
                return try overflow.readChain(self.tree.pool, allocator, desc.first_page_id, desc.total_len);
            }
            return try allocator.dupe(u8, cell.value);
        }
    };

    /// Open a [`LeafReuseSearcher`] over this tree. Takes the shared
    /// `structure_lock`; the caller MUST `deinit` the returned cursor.
    pub fn leafReuseSearcher(self: *BPlusTree) LeafReuseSearcher {
        self.structure_lock.lockShared(self.pool.pager.io);
        return .{ .tree = self };
    }

    /// In-place update of an existing inline value.
    ///
    /// Uses the optimistic descent ([`findLeafOptimistic`]) under a shared
    /// `structure_lock`, then latches the leaf exclusively and rewrites the cell
    /// via [`page.Page.updateCell`]. Returns [`BTreeError.KeyNotFound`] if the
    /// key is absent, and [`BTreeError.OverflowUpdateNotSupported`] if the
    /// current value is an overflow value or the new value would exceed
    /// [`overflow.OVERFLOW_THRESHOLD`] (those cases require delete + insert so
    /// the overflow chain is managed correctly). The leaf is marked dirty.
    pub fn update(self: *BPlusTree, key: []const u8, new_value: []const u8) !void {
        self.structure_lock.lockShared(self.pool.pager.io);
        defer self.structure_lock.unlockShared(self.pool.pager.io);

        const f = try self.findLeafOptimistic(key);
        defer {
            f.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(f.page_id.?, true);
        }
        const p = self.pool.pageOf(f);
        const idx = p.findCellByKey(key) orelse return BTreeError.KeyNotFound;
        const cell = p.getCell(idx).?;
        if (cell.flags.value_overflow) return BTreeError.OverflowUpdateNotSupported;
        if (new_value.len > overflow.OVERFLOW_THRESHOLD) return BTreeError.OverflowUpdateNotSupported;
        try p.updateCell(idx, new_value);
    }

    /// Inserts a new `key -> value` cell, failing if `key` already exists.
    ///
    /// Two-phase for concurrency. For inline-sized values it first tries the
    /// OPTIMISTIC fast path under a SHARED `structure_lock`: descend without
    /// internal latches, latch the leaf, reject duplicates with
    /// [`BTreeError.KeyAlreadyExists`], and if the cell fits, insert it in place
    /// and return. If the value needs an overflow chain, or the target leaf has
    /// no room (a split is required), it drops the shared lock, retakes
    /// `structure_lock` EXCLUSIVE, and delegates to [`insertExclusive`], which
    /// may split and grow the tree. Every early exit unpins the leaf and
    /// releases the shared lock before the exclusive retry.
    pub fn insert(self: *BPlusTree, key: []const u8, value: []const u8) !void {
        const io = self.pool.pager.io;

        if (value.len <= overflow.OVERFLOW_THRESHOLD) {
            self.structure_lock.lockShared(io);
            const f = try self.findLeafOptimistic(key);
            const lid = f.page_id.?;
            const p = self.pool.pageOf(f);

            if (p.findCellByKey(key) != null) {
                f.latch.unlock(io);
                self.pool.unpinPage(lid, false);
                self.structure_lock.unlockShared(io);
                return BTreeError.KeyAlreadyExists;
            }
            const cell = Cell{ .key = key, .value = value, .flags = .{} };
            if (p.hasSpace(cell.len())) {
                try p.insertCell(p.findInsertIndex(key), cell);
                f.latch.unlock(io);
                self.pool.unpinPage(lid, true);
                self.structure_lock.unlockShared(io);
                return;
            }
            f.latch.unlock(io);
            self.pool.unpinPage(lid, false);
            self.structure_lock.unlockShared(io);
        }

        self.structure_lock.lock(io);
        defer self.structure_lock.unlock(io);
        return self.insertExclusive(key, value);
    }

    /// Insert under an EXCLUSIVE `structure_lock`, allowed to split and grow.
    ///
    /// Called when the optimistic path in [`insert`] could not commit. If the
    /// value exceeds [`overflow.OVERFLOW_THRESHOLD`] it is first spilled with
    /// [`overflow.writeChain`] and the cell stores the encoded
    /// [`overflow.OverflowDescriptor`] with `value_overflow` set; the `errdefer`
    /// frees that chain if anything later fails. Descends with exclusive latch
    /// crabbing ([`findLeafExclusive`]), rejects duplicates (freeing any
    /// just-written overflow chain), inserts in place if the cell fits, and
    /// otherwise hands off to [`splitAndInsert`]. The `scratch` arena is reset
    /// on return.
    fn insertExclusive(self: *BPlusTree, key: []const u8, value: []const u8) !void {
        defer _ = self.scratch.reset(.free_all);

        var descriptor_buf: [overflow.OverflowDescriptor.SIZE]u8 = undefined;
        var stored_value = value;
        var flags = CellFlags{};
        var overflow_first_id: ?PageId = null;

        if (value.len > overflow.OVERFLOW_THRESHOLD) {
            const first_id = try overflow.writeChain(self.pool, value);
            overflow_first_id = first_id;
            const desc = overflow.OverflowDescriptor{ .total_len = @intCast(value.len), .first_page_id = first_id };
            desc.encode(&descriptor_buf);
            stored_value = &descriptor_buf;
            flags.value_overflow = true;
        }
        errdefer if (overflow_first_id) |fid| overflow.freeChain(self.pool, fid) catch {};

        const f = try self.findLeafExclusive(key);
        const p = self.pool.pageOf(f);
        const lid = f.page_id.?;

        if (p.findCellByKey(key) != null) {
            f.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(lid, false);
            if (overflow_first_id) |fid| try overflow.freeChain(self.pool, fid);
            return BTreeError.KeyAlreadyExists;
        }

        const cell = Cell{ .key = key, .value = stored_value, .flags = flags };
        if (p.hasSpace(cell.len())) {
            try p.insertCell(p.findInsertIndex(key), cell);
            f.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(lid, true);
        } else {
            f.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(lid, true);
            try self.splitAndInsert(lid, cell, 0);
        }
    }

    /// Deletes the cell for `key`, rebalancing if the leaf underflows.
    ///
    /// Two-phase like [`insert`]. It first tries the OPTIMISTIC path under a
    /// SHARED `structure_lock`: if the key is missing it returns
    /// [`BTreeError.KeyNotFound`] immediately; if the cell is inline and the
    /// leaf is either the root OR will still hold at least half a page after
    /// removal (`predicted_free <= underflow_limit`), it deletes and compacts in
    /// place and returns. Otherwise (overflow cell, or the deletion would drop
    /// the leaf below half full and may cascade a merge) it releases the shared
    /// lock, retakes `structure_lock` EXCLUSIVE, and calls [`deleteExclusive`].
    /// `underflow_limit` is the free-space threshold marking "less than half
    /// full": a page whose free space would exceed it has underflowed.
    pub fn delete(self: *BPlusTree, key: []const u8) !void {
        const io = self.pool.pager.io;

        {
            self.structure_lock.lockShared(io);
            const f = try self.findLeafOptimistic(key);
            const lid = f.page_id.?;
            const p = self.pool.pageOf(f);

            if (p.findCellByKey(key)) |i| {
                const cell = p.getCell(i).?;
                if (!cell.flags.value_overflow) {
                    const predicted_free = @as(u32, p.freeSpace()) + cell.len() + @sizeOf(page.CellPtr);
                    const underflow_limit = PAGE_SIZE - @sizeOf(page.PageHeader) - PAGE_SIZE / 2;
                    if (lid == self.root_page_id or predicted_free <= underflow_limit) {
                        p.deleteCell(i);
                        try p.compact(self.allocator); // compact ignores the allocator (stack scratch)
                        f.latch.unlock(io);
                        self.pool.unpinPage(lid, true);
                        self.structure_lock.unlockShared(io);
                        return;
                    }
                }
            } else {
                f.latch.unlock(io);
                self.pool.unpinPage(lid, false);
                self.structure_lock.unlockShared(io);
                return BTreeError.KeyNotFound;
            }
            f.latch.unlock(io);
            self.pool.unpinPage(lid, false);
            self.structure_lock.unlockShared(io);
        }

        self.structure_lock.lock(io);
        defer self.structure_lock.unlock(io);
        return self.deleteExclusive(key);
    }

    /// Delete under an EXCLUSIVE `structure_lock`, allowed to merge and shrink.
    ///
    /// Descends with exclusive latch crabbing, frees any overflow chain the
    /// cell owned ([`overflow.freeChain`]), removes and compacts the cell, then
    /// decides on rebalancing:
    ///   * If the leaf is the root and it is an internal node that emptied, the
    ///     root collapses onto its `leftmost_child_id` (height decreases) and
    ///     the old root page is discarded.
    ///   * Otherwise, if the leaf's free space now exceeds the half-page
    ///     underflow limit, [`handleUnderflow`] borrows from or merges with a
    ///     sibling. Returns [`BTreeError.KeyNotFound`] if the key was absent.
    /// The `scratch` arena is reset on return.
    fn deleteExclusive(self: *BPlusTree, key: []const u8) !void {
        defer _ = self.scratch.reset(.free_all);

        const f = try self.findLeafExclusive(key);
        const lid = f.page_id.?;
        const p = self.pool.pageOf(f);

        if (p.findCellByKey(key)) |i| {
            const cell = p.getCell(i).?;
            if (cell.flags.value_overflow) {
                const desc = overflow.OverflowDescriptor.decode(cell.value);
                try overflow.freeChain(self.pool, desc.first_page_id);
            }

            p.deleteCell(i);
            try p.compact(self.scratch.allocator());

            const parent_id = p.headerPtr().parent_page_id;
            const free_sp = p.freeSpace();
            const num_cells = p.headerPtr().num_cells;
            const is_leaf = p.headerPtr().page_type == .leaf;

            f.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(lid, true);

            if (lid == self.root_page_id) {
                if (!is_leaf and num_cells == 0) {
                    const rf = try self.pool.fetchPage(self.root_page_id);
                    defer self.pool.unpinPage(self.root_page_id, true);
                    const root_p = self.pool.pageOf(rf);
                    const new_root = root_p.headerPtr().leftmost_child_id;
                    if (new_root != 0) {
                        self.root_page_id = new_root;
                        const nrf = try self.pool.fetchPage(new_root);
                        self.pool.pageOf(nrf).headerPtr().parent_page_id = 0;
                        self.pool.unpinPage(new_root, true);
                        try self.pool.discardPage(lid);
                    }
                }
            } else {
                if (free_sp > (PAGE_SIZE - @sizeOf(page.PageHeader) - PAGE_SIZE / 2)) {
                    try self.handleUnderflow(lid, parent_id, 0);
                }
            }
        } else {
            f.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(lid, false);
            return BTreeError.KeyNotFound;
        }
    }

    /// Opens a [`RangeIterator`] over `[start_key, end_key]`.
    ///
    /// Positions the cursor at the first cell whose key is `>= start_key` on the
    /// leaf that would contain `start_key` ([`findLeafShared`] +
    /// [`page.Page.findInsertIndex`]), leaving that leaf latched-shared and
    /// pinned. A `null` `end_key` scans to the end of the chain. The caller must
    /// call [`RangeIterator.deinit`] to release the held leaf.
    pub fn rangeScan(self: *BPlusTree, start_key: []const u8, end_key: ?[]const u8) !RangeIterator {
        const sf = try self.findLeafShared(start_key);
        const sp = self.pool.pageOf(sf);
        return RangeIterator{
            .tree = self,
            .current_frame = sf,
            .current_index = sp.findInsertIndex(start_key),
            .end_key = end_key,
            .pinned_page_id = sf.page_id.?,
        };
    }

    /// Opens a DESCENDING range scan over `[start_key, end_key]` (see
    /// [`RangeIteratorDesc`]): positions at the leaf holding the top of the range
    /// (the leaf for `end_key`, or the tree's rightmost leaf when `end_key` is
    /// null) with the read cursor at its last cell, then yields cells high to low.
    pub fn rangeScanDesc(self: *BPlusTree, start_key: ?[]const u8, end_key: ?[]const u8) !RangeIteratorDesc {
        const top = if (end_key) |ek| try self.findLeafShared(ek) else try self.descendRightmostShared(self.root_page_id);
        const p = self.pool.pageOf(top);
        return RangeIteratorDesc{
            .tree = self,
            .current_frame = top,
            .current_index = @as(i32, @intCast(p.headerPtr().num_cells)) - 1,
            .start_key = start_key,
            .end_key = end_key,
            .pinned_page_id = top.page_id.?,
        };
    }

    /// The child page id at child-slot `j` of an internal node (`0` =
    /// `leftmost_child_id`, `j` = the `(j-1)`-th cell's value). Mirror of the
    /// layout [`page.SlottedPage.findChildIndex`] indexes into.
    fn childAtSlot(p: *page.SlottedPage, j: u16) PageId {
        if (j == 0) return p.headerPtr().leftmost_child_id;
        const cell = p.getCell(j - 1).?;
        return mem.readInt(PageId, cell.value[0..@sizeOf(PageId)], .little);
    }

    /// Descends from `start_id` to the RIGHTMOST leaf beneath it, returning that
    /// leaf latched-shared and pinned. Interior nodes are pinned only (no latch),
    /// matching [`PrefetchIterator`]; used by the descending cursor.
    fn descendRightmostShared(self: *BPlusTree, start_id: PageId) !*Frame {
        var cur_id = start_id;
        var cur_frame = try self.pool.fetchPage(cur_id);
        var depth: u32 = 0;
        while (self.pool.pageOf(cur_frame).headerPtr().page_type == .internal) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) {
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const p = self.pool.pageOf(cur_frame);
            const n = p.headerPtr().num_cells;
            const next_id = childAtSlot(p, n); // rightmost child slot
            self.pool.unpinPage(cur_id, false);
            cur_id = next_id;
            cur_frame = try self.pool.fetchPage(cur_id);
        }
        cur_frame.latch.lockShared(self.pool.pager.io);
        return cur_frame;
    }

    /// Returns the leaf immediately to the LEFT of the leaf `cur_id` in key
    /// order, latched-shared and pinned, or `null` if `cur_id` is the leftmost
    /// leaf. Climbs via `parent_page_id` until it finds an ancestor that is not
    /// its parent's leftmost child, then descends that left sibling's rightmost
    /// leaf. Interior climb/descent is pins-only (see [`RangeIteratorDesc`]).
    fn prevLeafShared(self: *BPlusTree, cur_id: PageId) !?*Frame {
        var child_id = cur_id;
        var depth: u32 = 0;
        while (true) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) return BTreeError.TreeTooDeepOrCyclic;
            const cf = try self.pool.fetchPage(child_id);
            const parent_id = self.pool.pageOf(cf).headerPtr().parent_page_id;
            self.pool.unpinPage(child_id, false);
            if (parent_id == 0) return null; // reached the root: no previous leaf

            const pf = try self.pool.fetchPage(parent_id);
            const slot = self.pool.pageOf(pf).findChildIndex(child_id);
            if (slot == null or slot.? == 0) {
                // Not found (concurrent restructure) or current is the leftmost
                // child: climb to the parent and continue.
                self.pool.unpinPage(parent_id, false);
                if (slot == null) return null;
                child_id = parent_id;
                continue;
            }
            const sib_id = childAtSlot(self.pool.pageOf(pf), slot.? - 1);
            self.pool.unpinPage(parent_id, false);
            return try self.descendRightmostShared(sib_id);
        }
    }

    /// Descends from the root to the leaf that would contain `key`, holding
    /// shared latches with crabbing.
    ///
    /// At each internal node it fetches and latches-shared the child chosen by
    /// [`page.Page.findChildPageId`] BEFORE releasing the parent, so the path
    /// can never be seen half-changed by a concurrent writer. Returns the leaf
    /// still latched-shared and pinned (caller releases both). Aborts with
    /// [`BTreeError.TreeTooDeepOrCyclic`] past [`MAX_TREE_DEPTH`].
    fn findLeafShared(self: *BPlusTree, key: []const u8) !*Frame {
        var cur_id = self.root_page_id;
        var cur_frame = try self.pool.fetchPage(cur_id);
        cur_frame.latch.lockShared(self.pool.pager.io);

        var depth: u32 = 0;
        while (self.pool.pageOf(cur_frame).headerPtr().page_type == .internal) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) {
                cur_frame.latch.unlockShared(self.pool.pager.io);
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const next_id = self.pool.pageOf(cur_frame).findChildPageId(key);

            const next_frame = try self.pool.fetchPage(next_id);
            next_frame.latch.lockShared(self.pool.pager.io);

            cur_frame.latch.unlockShared(self.pool.pager.io);
            self.pool.unpinPage(cur_id, false);

            cur_id = next_id;
            cur_frame = next_frame;
        }
        return cur_frame;
    }

    /// Descends from the root to the leaf for `key`, holding EXCLUSIVE latches
    /// with crabbing.
    ///
    /// Identical routing to [`findLeafShared`] but with write latches, used by
    /// the restructuring paths ([`insertExclusive`], [`deleteExclusive`]) so the
    /// whole root-to-leaf path is write-protected during a split/merge. Returns
    /// the leaf latched-exclusive and pinned. Aborts with
    /// [`BTreeError.TreeTooDeepOrCyclic`] past [`MAX_TREE_DEPTH`].
    fn findLeafExclusive(self: *BPlusTree, key: []const u8) !*Frame {
        var cur_id = self.root_page_id;
        var cur_frame = try self.pool.fetchPage(cur_id);
        cur_frame.latch.lock(self.pool.pager.io);
        self.pool.beginWrite(cur_frame); // Phase 4: writable before any modify

        var depth: u32 = 0;
        while (self.pool.pageOf(cur_frame).headerPtr().page_type == .internal) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) {
                cur_frame.latch.unlock(self.pool.pager.io);
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const next_id = self.pool.pageOf(cur_frame).findChildPageId(key);

            const next_frame = try self.pool.fetchPage(next_id);
            next_frame.latch.lock(self.pool.pager.io);
            self.pool.beginWrite(next_frame); // Phase 4: writable before any modify

            cur_frame.latch.unlock(self.pool.pager.io);
            self.pool.unpinPage(cur_id, false);

            cur_id = next_id;
            cur_frame = next_frame;
        }
        return cur_frame;
    }

    /// Descends to the leaf for `key` taking NO latches on internal nodes,
    /// latching only the final leaf exclusively.
    ///
    /// This is the optimistic route used by the [`insert`], [`update`], and
    /// [`delete`] fast paths: it bets the mutation will fit in one leaf and so
    /// avoids the cost of crabbing latches down the interior. Correctness rests
    /// on the SHARED `structure_lock` the caller holds, which blocks concurrent
    /// restructures, so the interior it reads unlatched cannot be reshaped
    /// beneath it. Aborts with [`BTreeError.TreeTooDeepOrCyclic`] past
    /// [`MAX_TREE_DEPTH`]; returns the leaf latched-exclusive and pinned.
    fn findLeafOptimistic(self: *BPlusTree, key: []const u8) !*Frame {
        var cur_id = self.root_page_id;
        var cur_frame = try self.pool.fetchPage(cur_id);

        var depth: u32 = 0;
        while (self.pool.pageOf(cur_frame).headerPtr().page_type == .internal) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) {
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const next_id = self.pool.pageOf(cur_frame).findChildPageId(key);
            self.pool.unpinPage(cur_id, false);
            cur_id = next_id;
            cur_frame = try self.pool.fetchPage(cur_id);
        }
        cur_frame.latch.lock(self.pool.pager.io);
        self.pool.beginWrite(cur_frame); // Phase 4: writable before any modify
        return cur_frame;
    }

    /// Splits the full page `page_id` in two and inserts `cell`, promoting a
    /// separator key to the parent.
    ///
    /// Collects the existing cells plus the new `cell` into the `scratch` arena
    /// (skipping the empty-key sentinel), sorts them, and splits at the midpoint
    /// `sp`. A new sibling page of the same type is allocated; the median
    /// (`pkc`) becomes the promoted separator. For a LEAF split the median cell
    /// stays (its copy goes to the right sibling) and the leaf chain is relinked
    /// (`old -> new -> old_next`). For an INTERNAL split the median is NOT kept
    /// as a cell: its child pointer becomes the new sibling's
    /// `leftmost_child_id`, and every child moved to the sibling has its
    /// `parent_page_id` fixed up. Finally the promoted key + new-page id are
    /// pushed up through [`insertIntoParent`] at `depth + 1`. Aborts past
    /// [`MAX_TREE_DEPTH`].
    fn splitAndInsert(self: *BPlusTree, page_id: PageId, cell: Cell, depth: u32) !void {
        if (depth > MAX_TREE_DEPTH) return BTreeError.TreeTooDeepOrCyclic;

        const sc = self.scratch.allocator();

        const of = try self.pool.fetchPage(page_id);
        defer self.pool.unpinPage(page_id, true);
        self.pool.beginWrite(of); // Phase 4: the split page is rewritten in place
        const op = self.pool.pageOf(of);
        const is_leaf = op.headerPtr().page_type == .leaf;
        const orig_parent = op.headerPtr().parent_page_id;

        var all = std.ArrayList(Cell).empty;
        {
            var it = op.cells();
            while (it.next()) |b| {
                if (b.key.len == 0) continue;
                try all.append(sc, .{
                    .key = try sc.dupe(u8, b.key),
                    .value = try sc.dupe(u8, b.value),
                    .flags = b.flags,
                });
            }
            try all.append(sc, .{
                .key = try sc.dupe(u8, cell.key),
                .value = try sc.dupe(u8, cell.value),
                .flags = cell.flags,
            });
            std.sort.pdq(Cell, all.items, {}, struct {
                fn lt(_: void, a: Cell, b: Cell) bool {
                    return mem.order(u8, a.key, b.key) == .lt;
                }
            }.lt);
        }

        // Split point. Default is the midpoint (best for random-order inserts).
        // For a LEAF, bias the split when the newly inserted cell is at one end of
        // the key range - the signature of a sequential (append or prepend) load,
        // e.g. monotonic ids. An append then keeps the left page full and starts a
        // fresh right page with just the new cell, so leaves fill to ~100% instead
        // of the ~65-75% a midpoint split leaves; a prepend is the mirror image.
        // This is SQLite's "quickbalance" append optimisation and it is the main
        // lever on on-disk footprint for monotonically-keyed collections.
        var sp = all.items.len / 2;
        if (is_leaf and all.items.len >= 2) {
            const n = all.items.len;
            if (mem.order(u8, cell.key, all.items[n - 1].key) == .eq) {
                sp = n - 1; // append: new cell is the max -> left keeps the rest
            } else if (mem.order(u8, cell.key, all.items[0].key) == .eq) {
                sp = 1; // prepend: new cell is the min -> right keeps the rest
            }
        }
        // Byte-safety: the chosen split point balances by CELL COUNT, which is fine
        // when cells are small but can, with large (near-half-page) inline cells,
        // leave one half over a page's capacity and make `insertCell` below fail.
        // Guarantee both halves fit: if the count-based cut is byte-unsafe, fall
        // back to the largest prefix that fits `usable`. Because the overflow
        // threshold caps any inline cell at <= usable/2, that prefix always leaves
        // a remainder < usable, so a byte-safe contiguous cut is always found.
        if (is_leaf and all.items.len >= 2) {
            const usable: u32 = PAGE_SIZE - @as(u32, @sizeOf(page.PageHeader));
            const slot: u32 = @sizeOf(page.CellPtr);
            var prefix_bytes: u32 = 0;
            for (all.items[0..sp]) |c| prefix_bytes += c.len() + slot;
            var suffix_bytes: u32 = 0;
            for (all.items[sp..]) |c| suffix_bytes += c.len() + slot;
            if (prefix_bytes > usable or suffix_bytes > usable) {
                var acc: u32 = 0;
                var cut: usize = 0;
                for (all.items, 0..) |c, i| {
                    const need = c.len() + slot;
                    if (acc + need > usable) break;
                    acc += need;
                    cut = i + 1;
                }
                if (cut < 1) cut = 1;
                if (cut > all.items.len - 1) cut = all.items.len - 1;
                sp = cut;
            }
        }
        const pkc = all.items[sp];
        const promoted_key = try sc.dupe(u8, pkc.key);

        const nf = try self.pool.newPage(op.headerPtr().page_type);
        const nid = nf.page_id.?;
        defer self.pool.unpinPage(nid, true);
        const np = self.pool.pageOf(nf);
        np.headerPtr().parent_page_id = orig_parent;

        const old_next = op.headerPtr().next_page_id;
        op.clear();
        op.headerPtr().parent_page_id = orig_parent;
        op.headerPtr().next_page_id = if (is_leaf) nid else old_next;
        if (is_leaf) np.headerPtr().next_page_id = old_next;

        var promoted_pid_bytes: [@sizeOf(PageId)]u8 = undefined;

        if (is_leaf) {
            for (all.items[0..sp]) |c| try op.insertCell(op.headerPtr().num_cells, c);
            for (all.items[sp..]) |c| try np.insertCell(np.headerPtr().num_cells, c);
        } else {
            np.headerPtr().leftmost_child_id =
                mem.readInt(PageId, pkc.value[0..@sizeOf(PageId)], .little);
            for (all.items[0..sp]) |c| try op.insertCell(op.headerPtr().num_cells, c);
            for (all.items[sp + 1 ..]) |c| try np.insertCell(np.headerPtr().num_cells, c);

            var cf = try self.pool.fetchPage(np.headerPtr().leftmost_child_id);
            self.pool.pageOf(cf).headerPtr().parent_page_id = nid;
            self.pool.unpinPage(cf.page_id.?, true);
            var nit = np.cells();
            while (nit.next()) |c| {
                cf = try self.pool.fetchPage(mem.readInt(PageId, c.value[0..@sizeOf(PageId)], .little));
                self.pool.pageOf(cf).headerPtr().parent_page_id = nid;
                self.pool.unpinPage(cf.page_id.?, true);
            }
        }

        mem.writeInt(PageId, &promoted_pid_bytes, nid, .little);
        try self.insertIntoParent(orig_parent, promoted_key, &promoted_pid_bytes, depth + 1);
    }

    /// Inserts a promoted separator (`key -> value_slice`, where `value_slice`
    /// is a child page id) into a parent, splitting it or growing a new root.
    ///
    /// When `parent_id == 0` the split was at the root: a fresh internal page is
    /// allocated, the OLD root becomes its `leftmost_child_id`, the promoted
    /// separator becomes its first cell, and both children's `parent_page_id`
    /// are repointed at the new root, increasing tree height by one. Otherwise
    /// the separator is inserted into the existing parent if it fits, or the
    /// parent itself is split via [`splitAndInsert`] (which recurses back here).
    /// `anyerror` return because the recursion mixes error sets. Aborts past
    /// [`MAX_TREE_DEPTH`].
    fn insertIntoParent(self: *BPlusTree, parent_id: PageId, key: []const u8, value_slice: []const u8, depth: u32) anyerror!void {
        if (depth > MAX_TREE_DEPTH) return BTreeError.TreeTooDeepOrCyclic;

        if (parent_id == 0) {
            const rf = try self.pool.newPage(.internal);
            defer self.pool.unpinPage(rf.page_id.?, true);
            const rp = self.pool.pageOf(rf);
            const old = self.root_page_id;
            self.root_page_id = rf.page_id.?;
            rp.headerPtr().leftmost_child_id = old;
            try rp.insertCell(0, .{ .key = key, .value = value_slice });

            const ocf = try self.pool.fetchPage(old);
            self.pool.pageOf(ocf).headerPtr().parent_page_id = self.root_page_id;
            self.pool.unpinPage(old, true);

            const ncf = try self.pool.fetchPage(mem.readInt(PageId, value_slice[0..@sizeOf(PageId)], .little));
            self.pool.pageOf(ncf).headerPtr().parent_page_id = self.root_page_id;
            self.pool.unpinPage(ncf.page_id.?, true);
            return;
        }

        const pf = try self.pool.fetchPage(parent_id);
        const pp = self.pool.pageOf(pf);
        const c = Cell{ .key = key, .value = value_slice };
        if (pp.hasSpace(c.len())) {
            try pp.insertCell(pp.findInsertIndex(key), c);
            self.pool.unpinPage(parent_id, true);
        } else {
            self.pool.unpinPage(parent_id, true);
            try self.splitAndInsert(parent_id, c, depth);
        }
    }

    /// Repairs an underflowed page by borrowing from, or merging with, a
    /// sibling.
    ///
    /// Locates the underflowed child's position `pos` within its parent
    /// ([`page.Page.findChildIndex`]; the leftmost child is `pos == 0`). Policy,
    /// in order: (1) if a LEFT sibling exists and is more than half full, borrow
    /// its last cell ([`borrowFromLeft`]); (2) else if a RIGHT sibling exists and
    /// is more than half full, borrow its first cell ([`borrowFromRight`]); (3)
    /// else merge, with the left sibling if there is one, otherwise with the
    /// right ([`mergePages`], which may recurse upward). Borrowing keeps both
    /// nodes legal without changing tree shape; merging removes a page and a
    /// parent separator and can propagate underflow. `anyerror` because the
    /// merge recursion mixes error sets. Aborts past [`MAX_TREE_DEPTH`].
    fn handleUnderflow(self: *BPlusTree, page_id: PageId, parent_id: PageId, depth: u32) anyerror!void {
        if (depth > MAX_TREE_DEPTH) return BTreeError.TreeTooDeepOrCyclic;

        const pf = try self.pool.fetchPage(parent_id);
        defer self.pool.unpinPage(parent_id, true);
        const pp = self.pool.pageOf(pf);
        const pos = pp.findChildIndex(page_id) orelse return;

        if (pos > 0) {
            const lid = if (pos == 1)
                pp.headerPtr().leftmost_child_id
            else
                mem.readInt(PageId, pp.getCell(pos - 2).?.value[0..@sizeOf(PageId)], .little);
            const lf = try self.pool.fetchPage(lid);
            defer self.pool.unpinPage(lid, true);
            if (self.pool.pageOf(lf).freeSpace() < PAGE_SIZE / 2)
                return self.borrowFromLeft(page_id, lid, parent_id, pos - 1);
        }

        if (pos < pp.headerPtr().num_cells) {
            const rid = mem.readInt(PageId, pp.getCell(pos).?.value[0..@sizeOf(PageId)], .little);
            const rf = try self.pool.fetchPage(rid);
            defer self.pool.unpinPage(rid, true);
            if (self.pool.pageOf(rf).freeSpace() < PAGE_SIZE / 2)
                return self.borrowFromRight(page_id, rid, parent_id, pos);
        }

        if (pos > 0) {
            const lid = if (pos == 1)
                pp.headerPtr().leftmost_child_id
            else
                mem.readInt(PageId, pp.getCell(pos - 2).?.value[0..@sizeOf(PageId)], .little);
            try self.mergePages(lid, page_id, parent_id, pos - 1, depth);
        } else {
            const rid = mem.readInt(PageId, pp.getCell(0).?.value[0..@sizeOf(PageId)], .little);
            try self.mergePages(page_id, rid, parent_id, 0, depth);
        }
    }


    /// Moves one cell from the left sibling into the underflowed page,
    /// rotating through the parent separator at `key_idx`.
    ///
    /// LEAF case: the left sibling's last cell moves to the front of `target`,
    /// and the parent separator is rewritten to that borrowed key so routing
    /// stays correct. INTERNAL case: this is a rotation through the parent, the
    /// classic B+Tree redistribute. The parent separator key descends to become
    /// `target`'s first cell (pointing at `target`'s old `leftmost_child_id`),
    /// the left sibling's last child pointer becomes `target`'s new
    /// `leftmost_child_id`, and that moved child's `parent_page_id` is repointed
    /// at `page_id`. All temporary keys/values are duplicated into `scratch`
    /// first because deletes compact the source pages out from under the slices.
    /// Tree shape is unchanged.
    fn borrowFromLeft(self: *BPlusTree, page_id: PageId, left_id: PageId, parent_id: PageId, key_idx: u16) !void {
        const sc = self.scratch.allocator();

        const pf = try self.pool.fetchPage(page_id);
        defer self.pool.unpinPage(page_id, true);
        const lf = try self.pool.fetchPage(left_id);
        defer self.pool.unpinPage(left_id, true);
        const rf = try self.pool.fetchPage(parent_id);
        defer self.pool.unpinPage(parent_id, true);
        const target = self.pool.pageOf(pf);
        const left = self.pool.pageOf(lf);
        const par = self.pool.pageOf(rf);

        const last = left.headerPtr().num_cells - 1;
        const ctm = left.getCell(last).?;
        const ck = try sc.dupe(u8, ctm.key);
        const cv = try sc.dupe(u8, ctm.value);

        const pkc = par.getCell(key_idx).?;
        const pk = try sc.dupe(u8, pkc.key);
        const pv = try sc.dupe(u8, pkc.value);

        if (target.headerPtr().page_type == .leaf) {
            left.deleteCell(last);
            try left.compact(sc);
            par.deleteCell(key_idx);
            try par.compact(sc);

            try target.insertCell(0, .{ .key = ck, .value = cv, .flags = ctm.flags });
            try par.insertCell(key_idx, .{ .key = ck, .value = pv, .flags = pkc.flags });
        } else {
            const target_lc = target.headerPtr().leftmost_child_id;
            var target_lc_b: [@sizeOf(PageId)]u8 = undefined;
            mem.writeInt(PageId, &target_lc_b, target_lc, .little);

            target.headerPtr().leftmost_child_id = mem.readInt(PageId, cv[0..@sizeOf(PageId)], .little);
            left.deleteCell(last);
            try left.compact(sc);
            par.deleteCell(key_idx);
            try par.compact(sc);

            try target.insertCell(0, .{ .key = pk, .value = &target_lc_b });
            try par.insertCell(key_idx, .{ .key = ck, .value = pv, .flags = pkc.flags });

            const new_lc = target.headerPtr().leftmost_child_id;
            const cf = try self.pool.fetchPage(new_lc);
            self.pool.pageOf(cf).headerPtr().parent_page_id = page_id;
            self.pool.unpinPage(new_lc, true);
        }
    }

    /// Moves one cell from the right sibling into the underflowed page,
    /// rotating through the parent separator at `key_idx`.
    ///
    /// Mirror of [`borrowFromLeft`]. LEAF case: the right sibling's first cell
    /// appends to `target`, and the parent separator is reset to the right
    /// sibling's NEW first key so it keeps routing to the right subtree.
    /// INTERNAL case: the parent separator descends to become `target`'s last
    /// cell (pointing at the right sibling's old `leftmost_child_id`, whose
    /// `parent_page_id` is repointed at `page_id`), and the right sibling's
    /// first child pointer is promoted to be its new `leftmost_child_id`. Values
    /// are duplicated into `scratch` before the source compactions run. Tree
    /// shape is unchanged.
    fn borrowFromRight(self: *BPlusTree, page_id: PageId, right_id: PageId, parent_id: PageId, key_idx: u16) !void {
        const sc = self.scratch.allocator();

        const pf = try self.pool.fetchPage(page_id);
        defer self.pool.unpinPage(page_id, true);
        const rf = try self.pool.fetchPage(right_id);
        defer self.pool.unpinPage(right_id, true);
        const qf = try self.pool.fetchPage(parent_id);
        defer self.pool.unpinPage(parent_id, true);
        const target = self.pool.pageOf(pf);
        const right = self.pool.pageOf(rf);
        const par = self.pool.pageOf(qf);

        const ctm = right.getCell(0).?;
        const ck = try sc.dupe(u8, ctm.key);
        const cv = try sc.dupe(u8, ctm.value);

        const pkc = par.getCell(key_idx).?;
        const pk = try sc.dupe(u8, pkc.key);
        const pv = try sc.dupe(u8, pkc.value);

        if (target.headerPtr().page_type == .leaf) {
            right.deleteCell(0);
            try right.compact(sc);
            par.deleteCell(key_idx);
            try par.compact(sc);

            try target.insertCell(target.headerPtr().num_cells, .{ .key = ck, .value = cv, .flags = ctm.flags });

            const sep = try sc.dupe(u8, right.getCell(0).?.key);
            try par.insertCell(key_idx, .{ .key = sep, .value = pv, .flags = pkc.flags });
        } else {
            const rlc = right.headerPtr().leftmost_child_id;
            var rlc_b: [@sizeOf(PageId)]u8 = undefined;
            mem.writeInt(PageId, &rlc_b, rlc, .little);

            right.headerPtr().leftmost_child_id =
                mem.readInt(PageId, cv[0..@sizeOf(PageId)], .little);
            right.deleteCell(0);
            try right.compact(sc);
            par.deleteCell(key_idx);
            try par.compact(sc);

            try target.insertCell(target.headerPtr().num_cells, .{ .key = pk, .value = &rlc_b });
            try par.insertCell(key_idx, .{ .key = ck, .value = pv, .flags = pkc.flags });

            const cf = try self.pool.fetchPage(rlc);
            self.pool.pageOf(cf).headerPtr().parent_page_id = page_id;
            self.pool.unpinPage(rlc, true);
        }
    }

    /// Merges the `right_id` page into `left_id`, deletes the parent separator,
    /// and discards the emptied right page.
    ///
    /// First checks the merge actually fits: `need` is the total space the right
    /// page's cells (plus, for internal nodes, the demoted parent separator and
    /// its `CellPtr`) require, and if `left` cannot hold it the merge is aborted
    /// (the right page is unpinned clean and the function returns without
    /// changing anything). INTERNAL merge: the parent separator descends into
    /// `left` pointing at the right page's `leftmost_child_id`, then every child
    /// of the right page is re-parented to `left_id` and appended. LEAF merge:
    /// cells are appended and `left` inherits the right page's `next_page_id`,
    /// keeping the leaf chain intact. The parent separator at `key_idx` is
    /// removed; then, if the parent was the root and is now empty the tree height
    /// drops (root becomes `left_id`), otherwise if the parent itself underflowed
    /// the fix propagates via [`handleUnderflow`] at `depth + 1`. Aborts past
    /// [`MAX_TREE_DEPTH`] through that recursion.
    fn mergePages(self: *BPlusTree, left_id: PageId, right_id: PageId, parent_id: PageId, key_idx: u16, depth: u32) !void {
        const sc = self.scratch.allocator();

        const lf = try self.pool.fetchPage(left_id);
        defer self.pool.unpinPage(left_id, true);
        const rf = try self.pool.fetchPage(right_id);
        const pf = try self.pool.fetchPage(parent_id);
        defer self.pool.unpinPage(parent_id, true);
        const left = self.pool.pageOf(lf);
        const right = self.pool.pageOf(rf);
        const par = self.pool.pageOf(pf);

        const sep = par.getCell(key_idx).?;
        var need: u32 = 0;
        if (left.headerPtr().page_type == .internal)
            need += sep.len() + @sizeOf(page.CellPtr);
        {
            var it = right.cells();
            while (it.next()) |c| need += c.len() + @sizeOf(page.CellPtr);
        }
        if (@as(u32, left.freeSpace()) < need) {
            self.pool.unpinPage(right_id, false);
            return;
        }

        if (left.headerPtr().page_type == .internal) {
            const rlc = right.headerPtr().leftmost_child_id;
            var rlc_b: [@sizeOf(PageId)]u8 = undefined;
            mem.writeInt(PageId, &rlc_b, rlc, .little);
            try left.insertCell(left.headerPtr().num_cells, .{ .key = sep.key, .value = &rlc_b });

            const cf_rlc = try self.pool.fetchPage(rlc);
            self.pool.pageOf(cf_rlc).headerPtr().parent_page_id = left_id;
            self.pool.unpinPage(rlc, true);

            var it = right.cells();
            while (it.next()) |c| {
                const child_id = mem.readInt(PageId, c.value[0..@sizeOf(PageId)], .little);
                const cf = try self.pool.fetchPage(child_id);
                self.pool.pageOf(cf).headerPtr().parent_page_id = left_id;
                self.pool.unpinPage(child_id, true);
                try left.insertCell(left.headerPtr().num_cells, c);
            }
        } else {
            var it = right.cells();
            while (it.next()) |c| try left.insertCell(left.headerPtr().num_cells, c);
        }

        if (left.headerPtr().page_type == .leaf)
            left.headerPtr().next_page_id = right.headerPtr().next_page_id;

        par.deleteCell(key_idx);
        try par.compact(sc);

        self.pool.unpinPage(right_id, false);
        try self.pool.discardPage(right_id);

        if (self.root_page_id == parent_id) {
            if (par.headerPtr().num_cells == 0) {
                self.root_page_id = left_id;
                const nrf = try self.pool.fetchPage(left_id);
                self.pool.pageOf(nrf).headerPtr().parent_page_id = 0;
                self.pool.unpinPage(left_id, true);
                try self.pool.discardPage(parent_id);
            }
        } else if (par.freeSpace() > (PAGE_SIZE - @sizeOf(page.PageHeader) - PAGE_SIZE / 2)) {
            try self.handleUnderflow(parent_id, par.headerPtr().parent_page_id, depth + 1);
        }
    }

    /// Returns the leftmost leaf, pinned but UNLATCHED.
    ///
    /// Follows `leftmost_child_id` from the root down to the first leaf, the
    /// start of the key-ordered chain. Used by the prefetch iterators and
    /// [`checkInvariants`], which coordinate via pins/`structure_lock` rather
    /// than per-frame latches. Aborts with [`BTreeError.TreeTooDeepOrCyclic`]
    /// past [`MAX_TREE_DEPTH`].
    pub fn findFirstLeaf(self: *BPlusTree) !*Frame {
        var cur_id = self.root_page_id;
        var cur_frame = try self.pool.fetchPage(cur_id);
        var depth: u32 = 0;
        while (self.pool.pageOf(cur_frame).headerPtr().page_type == .internal) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) {
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const next_id = self.pool.pageOf(cur_frame).headerPtr().leftmost_child_id;
            self.pool.unpinPage(cur_id, false);
            cur_id = next_id;
            cur_frame = try self.pool.fetchPage(cur_id);
        }
        return cur_frame;
    }

    /// Returns the leftmost leaf, latched-SHARED and pinned, crabbing down.
    ///
    /// Same leftmost descent as [`findFirstLeaf`] but holding shared latches
    /// with crabbing, so the returned leaf is safe to iterate against
    /// concurrent writers. Used by [`iterator`]. The caller releases the shared
    /// latch and pin. Aborts with [`BTreeError.TreeTooDeepOrCyclic`] past
    /// [`MAX_TREE_DEPTH`].
    pub fn findFirstLeafShared(self: *BPlusTree) !*Frame {
        var cur_id = self.root_page_id;
        var cur_frame = try self.pool.fetchPage(cur_id);
        cur_frame.latch.lockShared(self.pool.pager.io);
        var depth: u32 = 0;
        while (self.pool.pageOf(cur_frame).headerPtr().page_type == .internal) {
            depth += 1;
            if (depth > MAX_TREE_DEPTH) {
                cur_frame.latch.unlockShared(self.pool.pager.io);
                self.pool.unpinPage(cur_id, false);
                return BTreeError.TreeTooDeepOrCyclic;
            }
            const next_id = self.pool.pageOf(cur_frame).headerPtr().leftmost_child_id;
            const next_frame = try self.pool.fetchPage(next_id);
            next_frame.latch.lockShared(self.pool.pager.io);
            cur_frame.latch.unlockShared(self.pool.pager.io);
            self.pool.unpinPage(cur_id, false);
            cur_id = next_id;
            cur_frame = next_frame;
        }
        return cur_frame;
    }

    /// Opens a full-tree [`Iterator`] positioned at the first key.
    ///
    /// Starts at the leftmost leaf via [`findFirstLeafShared`] (latched-shared),
    /// so the cursor scans the entire chain in key order. Caller must
    /// [`Iterator.deinit`] it.
    pub fn iterator(self: *BPlusTree) !Iterator {
        const first = try self.findFirstLeafShared();
        return .{
            .tree = self,
            .current_frame = first,
            .current_index = 0,
            .pinned_page_id = first.page_id.?,
        };
    }

    /// Opens an [`Iterator`] positioned at the first key strictly greater than
    /// `after_key` (an exclusive-lower-bound scan for keyset pagination).
    ///
    /// Descends to the leaf that would hold `after_key` ([`findLeafShared`],
    /// latched-shared) and binary-searches for the first cell whose key is
    /// `>= after_key`. Because keys are unique, that first `>=` position is
    /// exactly the first key strictly greater than `after_key` when `after_key`
    /// itself is present, giving an "everything after this key" cursor. The
    /// binary search tolerates slot holes by advancing `left` past a `null`
    /// cell. Caller must [`Iterator.deinit`] it.
    pub fn iteratorAfter(self: *BPlusTree, after_key: []const u8) !Iterator {
        const leaf = try self.findLeafShared(after_key);
        const p = self.pool.pageOf(leaf);

        var left: u16 = 0;
        var right: u16 = p.headerPtr().num_cells;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const cell = p.getCell(mid) orelse {
                left = mid + 1;
                continue;
            };
            if (std.mem.order(u8, cell.key, after_key) == .lt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return Iterator{
            .tree = self,
            .current_frame = leaf,
            .current_index = left,
            .pinned_page_id = leaf.page_id.?,
        };
    }

    /// Opens a prefetching full-tree scan from the first key.
    ///
    /// Like [`iterator`] but backed by [`PrefetchIterator`] for bulk sequential
    /// reads; starts at the unlatched leftmost leaf ([`findFirstLeaf`]).
    pub fn prefetchIterator(self: *BPlusTree) !PrefetchIterator {
        return PrefetchIterator.init(self, try self.findFirstLeaf(), self.allocator);
    }

    /// Opens a prefetching scan positioned at the first key strictly greater
    /// than `after_key`.
    ///
    /// Descends to the leaf for `after_key` and binary-searches for the first
    /// key `> after_key` (note the `!= .gt` comparison, an upper-bound search,
    /// so equal keys are skipped too). If that position falls past the end of
    /// the leaf, it advances to the next leaf in the chain, unpinning the
    /// current one first; if there is no next leaf it returns a
    /// [`PrefetchIterator`] parked at the end of the last leaf (an immediately
    /// exhausted cursor). Otherwise it starts the prefetch cursor at the found
    /// index. Caller must [`PrefetchIterator.deinit`] it.
    pub fn prefetchIteratorAfter(self: *BPlusTree, after_key: []const u8) !PrefetchIterator {
        const leaf = try self.findLeafShared(after_key);
        const p = self.pool.pageOf(leaf);

        var start_index: u16 = 0;
        var left: u16 = 0;
        var right: u16 = p.headerPtr().num_cells;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const cell = p.getCell(mid) orelse {
                left = mid + 1;
                continue;
            };
            if (std.mem.order(u8, cell.key, after_key) != .gt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        start_index = left;

        if (start_index >= p.headerPtr().num_cells) {
            const next_id = p.headerPtr().next_page_id;
            self.pool.unpinPage(leaf.page_id.?, false);
            if (next_id == 0) {
                const first = try self.findFirstLeaf();
                var it = try PrefetchIterator.init(self, first, self.allocator);
                it.current_index = self.pool.pageOf(first).headerPtr().num_cells;
                return it;
            }
            const next_frame = try self.pool.fetchPage(next_id);
            return PrefetchIterator.init(self, next_frame, self.allocator);
        }

        var it = try PrefetchIterator.init(self, leaf, self.allocator);
        it.current_index = start_index;
        return it;
    }

    /// Full structural consistency check; returns `error.InvariantViolation`
    /// on the first violation and logs a detailed diagnostic.
    ///
    /// Two passes. First it walks the LEAF CHAIN via `next_page_id` and asserts
    /// (a) every chain page is actually a leaf and (b) keys are strictly
    /// increasing across the whole chain, dumping the offending leaf and its
    /// parent when order breaks. Then [`checkNodeInvariants`] recursively
    /// verifies the key-range and parent-pointer invariants of every node from
    /// the root down. This is a debugging/test guardrail (see the Stage-3
    /// concurrency and stress tests), not part of the hot path. `prev_key` is
    /// kept in `allocator` memory and freed on return.
    pub fn checkInvariants(self: *BPlusTree) !void {
        const cur_frame = try self.findFirstLeaf();
        var cur_id = cur_frame.page_id.?;
        self.pool.unpinPage(cur_id, false);
        var prev_key: ?[]const u8 = null;
        defer if (prev_key) |k| self.allocator.free(k);

        while (cur_id != 0) {
            const frame = try self.pool.fetchPage(cur_id);
            defer self.pool.unpinPage(cur_id, false);
            const p = self.pool.pageOf(frame);
            if (p.headerPtr().page_type != .leaf) {
                std.log.err("Invariant violation: leaf chain page {d} has type {s}", .{cur_id, @tagName(p.headerPtr().page_type)});
                return error.InvariantViolation;
            }

            var it = p.cells();
            while (it.next()) |cell| {
                if (prev_key) |pk| {
                    if (mem.order(u8, pk, cell.key) != .lt) {
                        std.log.err("Invariant violation: keys out of order. Prev '{s}', Curr '{s}' on leaf {d}", .{pk, cell.key, cur_id});
                        std.log.err("Leaf {d} header: parent={d} next={d} num_cells={d}", .{cur_id, p.headerPtr().parent_page_id, p.headerPtr().next_page_id, p.headerPtr().num_cells});
                        std.log.err("All keys on leaf {d}:", .{cur_id});
                        var dump_it = p.cells();
                        while (dump_it.next()) |c| {
                            std.log.err("  - '{s}'", .{c.key});
                        }
                        const parent_id = p.headerPtr().parent_page_id;
                        if (parent_id != 0) {
                            const pf = try self.pool.fetchPage(parent_id);
                            defer self.pool.unpinPage(parent_id, false);
                            const pp = self.pool.pageOf(pf);
                            std.log.err("Parent page {d} header: leftmost_child_id={d} num_cells={d}", .{parent_id, pp.headerPtr().leftmost_child_id, pp.headerPtr().num_cells});
                            std.log.err("Parent page {d} cells:", .{parent_id});
                            var pit = pp.cells();
                            while (pit.next()) |c| {
                                const child = mem.readInt(PageId, c.value[0..@sizeOf(PageId)], .little);
                                std.log.err("  - key='{s}' -> child={d}", .{c.key, child});
                            }
                        }
                        return error.InvariantViolation;
                    }
                    self.allocator.free(pk);
                }
                prev_key = try self.allocator.dupe(u8, cell.key);
            }
            cur_id = p.headerPtr().next_page_id;
        }

        try self.checkNodeInvariants(self.root_page_id, null, null);
    }

    /// Recursively verifies a subtree's key-range and parent-pointer
    /// invariants, given the `[min_key, max_key)` bound the node must live in.
    ///
    /// For every cell it asserts `min_key <= cell.key < max_key` (a `null` bound
    /// means unbounded on that side). For an INTERNAL node it also checks that
    /// the `leftmost_child_id` and every child cell point to a page whose
    /// `parent_page_id` equals this node, then recurses into each child with the
    /// tightened bounds derived from the surrounding separators (leftmost child
    /// is bounded above by the first key; each cell's child is bounded by that
    /// cell's key and the next cell's key, or the inherited `max_key` for the
    /// last). `anyerror` for the recursion; logs and returns
    /// `error.InvariantViolation` on the first breach.
    fn checkNodeInvariants(self: *BPlusTree, page_id: PageId, min_key: ?[]const u8, max_key: ?[]const u8) anyerror!void {
        const frame = try self.pool.fetchPage(page_id);
        defer self.pool.unpinPage(page_id, false);
        const p = self.pool.pageOf(frame);

        var it = p.cells();
        while (it.next()) |cell| {
            if (min_key) |min_k| {
                if (mem.order(u8, cell.key, min_k) == .lt) {
                    std.log.err("Invariant violation: key '{s}' on page {d} is less than min_key '{s}'", .{cell.key, page_id, min_k});
                    return error.InvariantViolation;
                }
            }
            if (max_key) |max_k| {
                if (mem.order(u8, cell.key, max_k) != .lt) {
                    std.log.err("Invariant violation: key '{s}' on page {d} is >= max_key '{s}'", .{cell.key, page_id, max_k});
                    return error.InvariantViolation;
                }
            }
        }

        if (p.headerPtr().page_type == .internal) {
            const left_id = p.headerPtr().leftmost_child_id;
            const left_frame = try self.pool.fetchPage(left_id);
            const left_p = self.pool.pageOf(left_frame);
            if (left_p.headerPtr().parent_page_id != page_id) {
                std.log.err("Invariant violation: leftmost child {d} of internal page {d} has wrong parent_page_id {d}", .{left_id, page_id, left_p.headerPtr().parent_page_id});
                self.pool.unpinPage(left_id, false);
                return error.InvariantViolation;
            }
            self.pool.unpinPage(left_id, false);

            var first_key: ?[]const u8 = null;
            if (p.headerPtr().num_cells > 0) {
                first_key = p.getCell(0).?.key;
            }
            try self.checkNodeInvariants(left_id, min_key, first_key);

            var i: u16 = 0;
            while (i < p.headerPtr().num_cells) : (i += 1) {
                const cell = p.getCell(i).?;
                const child_id = mem.readInt(PageId, cell.value[0..@sizeOf(PageId)], .little);
                const child_frame = try self.pool.fetchPage(child_id);
                const child_p = self.pool.pageOf(child_frame);
                if (child_p.headerPtr().parent_page_id != page_id) {
                    std.log.err("Invariant violation: child {d} at index {d} of internal page {d} has wrong parent_page_id {d}", .{child_id, i, page_id, child_p.headerPtr().parent_page_id});
                    self.pool.unpinPage(child_id, false);
                    return error.InvariantViolation;
                }
                self.pool.unpinPage(child_id, false);

                const next_max = if (i + 1 < p.headerPtr().num_cells) p.getCell(i + 1).?.key else max_key;
                try self.checkNodeInvariants(child_id, cell.key, next_max);
            }
        }
    }
};

test "btree - constants" {
    try testing.expect(MAX_TREE_DEPTH >= 16);
    try testing.expect(MAX_TREE_DEPTH <= 64);
}
