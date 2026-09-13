//! Append-only undo log: the version chain that makes MVCC reads possible.
//!
//! When a transaction UPDATEs or DELETEs a row, the engine cannot simply
//! overwrite the tuple in place, because other transactions may still need to
//! see the *pre-image* under their own snapshot. Instead, the prior version of
//! the tuple is copied here as an [`UndoRecord`] before the in-place change, and
//! the live tuple is left holding a `roll_ptr` back to it. Following those
//! `roll_ptr`s from newest to oldest walks the version chain, so a reader whose
//! snapshot predates the latest write can reconstruct the row as it was. The
//! same chain is what ROLLBACK replays to restore pre-images.
//!
//! ## Storage layout
//!
//! Records are packed head-to-tail into dedicated pages of type `.undo`, drawn
//! from the shared [`PagePool`] like any other page (so they are buffered,
//! checkpointed, and recovered by the same machinery as B+Tree pages). Within a
//! page, allocation is a simple bump of the page header's `free_space_start`
//! field: each record is `[UndoRecordHeader][fixed bytes][heap bytes]` laid out
//! contiguously starting at that offset. Undo data is only ever appended and
//! read back by pointer; individual records are never freed in place (the whole
//! page is reclaimed when its versions are no longer visible to any snapshot),
//! which is why a plain bump allocator suffices and no slot directory is needed.
//!
//! Pages are chained through the page header's `next_page_id` so the log forms a
//! singly linked list, with [`UndoLog.undo_pages`] additionally holding every
//! page id in allocation order.
//!
//! ## The roll pointer encoding
//!
//! A `roll_ptr` is a `u64` locating one record: the page id in the high bits and
//! the byte offset within that page in the low 16 bits, i.e.
//! `(page_id << 16) | offset`. The low 16 bits are exactly enough to address any
//! byte in a page because [`PAGE_SIZE`] fits in 16 bits. Value `0` doubles as the
//! null pointer / "no prior version" sentinel, which is also why [`UndoLog`]
//! starts with `active_page_id == 0` meaning "no page allocated yet".
//!
//! ## Concurrency
//!
//! Appends are serialised by [`UndoLog.mutex`]: page allocation, the
//! `next_page_id` link, and the `free_space_start` bump must happen atomically so
//! two writers cannot hand out overlapping offsets. Reads via
//! [`UndoLog.getRecord`] are lock-free with respect to the undo mutex, relying on
//! the append-only invariant (a record, once written, is immutable) and on the
//! pool's own pinning to keep the page resident while it is copied out.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const page = @import("../storage/page.zig");
/// Page identifier type re-exported from the storage layer; `0` is the null id.
const PageId = page.PageId;
/// Bytes per page. The 16-bit offset field of a `roll_ptr` addresses within this.
const PAGE_SIZE = page.PAGE_SIZE;
/// The fixed-size header at the start of every pool page (type, free-space
/// cursors, sibling links); undo pages set its `page_type` to `.undo`.
const PageHeader = page.PageHeader;
/// The buffer pool / pager that owns undo pages and lends them out pinned.
const PagePool = @import("../storage/pool.zig").PagePool;
/// A pinned buffer-pool slot holding one page's bytes plus its page id.
const Frame = @import("../storage/pool.zig").Frame;

/// On-disk fixed-size prefix of an undo record, written verbatim into the page.
///
/// `extern struct` pins the field order and layout so the bytes can be
/// `@memcpy`'d straight to/from page storage with no serialisation step. It is
/// immediately followed on the page by `fixed_len` bytes of fixed-width column
/// data and then `heap_len` bytes of variable-length (heap) column data.
pub const UndoRecordHeader = extern struct {
    /// Transaction id that created this tuple version (its insert stamp).
    xmin: u64,
    /// Transaction id that deleted/superseded this version, or `0` while live;
    /// used with a reader's snapshot to decide visibility.
    xmax: u64,
    /// `roll_ptr` to the *previous* (older) version in the chain, or `0` if this
    /// is the oldest recorded version. Following it walks further back in time.
    roll_ptr: u64,
    /// Length in bytes of the fixed-width column image that follows the header.
    fixed_len: u32,
    /// Length in bytes of the variable-length (heap) image that follows `fixed`.
    heap_len: u32,
};

/// A decoded undo record with its payload copied into caller-owned memory.
///
/// Returned by [`UndoLog.getRecord`], which allocates and owns the `fixed`/`heap`
/// slices so the record stays valid after its source page is unpinned. Call
/// [`UndoRecord.deinit`] to release them.
pub const UndoRecord = struct {
    /// Creating transaction id, copied from [`UndoRecordHeader.xmin`].
    xmin: u64,
    /// Deleting/superseding transaction id, copied from [`UndoRecordHeader.xmax`].
    xmax: u64,
    /// `roll_ptr` to the previous version, copied from the on-page header.
    roll_ptr: u64,
    /// Owned copy of the fixed-width column image (freed by [`UndoRecord.deinit`]).
    fixed: []const u8,
    /// Owned copy of the heap column image (freed by [`UndoRecord.deinit`]).
    heap: []const u8,

    /// Frees the `fixed` and `heap` slices previously allocated by
    /// [`UndoLog.getRecord`] using the same `allocator` that produced them.
    pub fn deinit(self: *UndoRecord, allocator: Allocator) void {
        allocator.free(self.fixed);
        allocator.free(self.heap);
    }
};

/// The append-only undo log over a set of `.undo` pages in the buffer pool.
///
/// Holds no data of its own beyond bookkeeping: records live in pool pages. The
/// log grows a page at a time and chains pages through their headers; see the
/// module header for the layout and roll-pointer encoding.
pub const UndoLog = struct {
    /// Buffer pool that owns the backing pages and provides pin/unpin + I/O.
    pool: *PagePool,
    /// Page currently receiving appends, or `0` when none has been allocated yet
    /// (the first append allocates one).
    active_page_id: PageId = 0,
    /// Every undo page id in allocation order; the tail is [`active_page_id`].
    undo_pages: std.ArrayList(PageId),
    /// Allocator for [`undo_pages`] growth and for decoded [`UndoRecord`] payloads.
    allocator: Allocator,
    /// Serialises appends so page allocation and the `free_space_start` bump are
    /// atomic; reads do not take it (append-only records are immutable).
    mutex: std.Io.Mutex = .init,

    /// Creates an empty undo log bound to `pool`; no page is allocated until the
    /// first [`UndoLog.appendRecord`]. `allocator` backs the page-id list and
    /// later record decodes.
    pub fn init(allocator: Allocator, pool: *PagePool) UndoLog {
        return .{
            .pool = pool,
            .undo_pages = std.ArrayList(PageId).empty,
            .allocator = allocator,
        };
    }

    /// Releases the [`undo_pages`] list. Does not free the pool pages themselves;
    /// their lifetime is the pool's, and their contents survive as long as any
    /// snapshot may still traverse the version chains they hold.
    pub fn deinit(self: *UndoLog) void {
        self.undo_pages.deinit(self.allocator);
    }

    /// Appends one version image and returns the `roll_ptr` now addressing it.
    ///
    /// Writes `[UndoRecordHeader][fixed][heap]` at the active page's
    /// `free_space_start` and bumps the cursor. `prev_roll_ptr` becomes the new
    /// record's back-link, so passing the caller's current live-tuple roll
    /// pointer extends the version chain; pass `0` for the first version.
    ///
    /// A fresh page is allocated when there is no active page yet, or when the
    /// record would not fit in the remaining free space of the current one. On
    /// allocation the old page's `next_page_id` is linked to the new page so the
    /// log stays a traversable list, the new id is appended to [`undo_pages`],
    /// and [`active_page_id`] advances. Records are never split across pages,
    /// which is why the fit check is done up front.
    ///
    /// Holds [`mutex`] for the whole operation so allocation and the bump are
    /// atomic. Returns an error if the pool cannot fetch or allocate a page.
    /// The returned pointer is `(active_page_id << 16) | offset`; see the module
    /// header for the encoding and [`UndoLog.getRecord`] to read it back.
    pub fn appendRecord(self: *UndoLog, xmin: u64, xmax: u64, prev_roll_ptr: u64, fixed: []const u8, heap: []const u8) !u64 {
        self.mutex.lockUncancelable(self.pool.pager.io);
        defer self.mutex.unlock(self.pool.pager.io);

        const record_len = @sizeOf(UndoRecordHeader) + fixed.len + heap.len;

        var needs_new_page = (self.active_page_id == 0);
        if (!needs_new_page) {
            const f = try self.pool.fetchPage(self.active_page_id);
            const p = self.pool.pageOf(f);
            const h = p.headerPtr();
            needs_new_page = (@as(usize, h.free_space_start) + record_len > PAGE_SIZE);
            self.pool.unpinPage(self.active_page_id, false);
        }

        if (needs_new_page) {
            const f = try self.pool.newPage(.undo);
            const new_page_id = f.page_id.?;
            const p = self.pool.pageOf(f);
            const h = p.headerPtr();

            h.page_type = .undo;
            h.free_space_start = @sizeOf(PageHeader);
            h.free_space_end = PAGE_SIZE;
            h.parent_page_id = 0;
            h.next_page_id = 0;
            h.leftmost_child_id = 0;
            h.num_cells = 0;

            if (self.active_page_id != 0) {
                const old_f = try self.pool.fetchPage(self.active_page_id);
                const old_p = self.pool.pageOf(old_f);
                old_p.headerPtr().next_page_id = new_page_id;
                self.pool.unpinPage(self.active_page_id, true);
            }

            self.pool.unpinPage(new_page_id, true);
            try self.undo_pages.append(self.allocator, new_page_id);
            self.active_page_id = new_page_id;
        }

        const f = try self.pool.fetchPage(self.active_page_id);
        defer self.pool.unpinPage(self.active_page_id, true);
        const p = self.pool.pageOf(f);
        const h = p.headerPtr();

        const offset = h.free_space_start;
        const rec_hdr = UndoRecordHeader{
            .xmin = xmin,
            .xmax = xmax,
            .roll_ptr = prev_roll_ptr,
            .fixed_len = @intCast(fixed.len),
            .heap_len = @intCast(heap.len),
        };

        @memcpy(p.data[offset..][0..@sizeOf(UndoRecordHeader)], std.mem.asBytes(&rec_hdr));
        @memcpy(p.data[offset + @sizeOf(UndoRecordHeader) ..][0..fixed.len], fixed);
        @memcpy(p.data[offset + @sizeOf(UndoRecordHeader) + fixed.len ..][0..heap.len], heap);

        h.free_space_start += @intCast(record_len);

        const roll_ptr = (@as(u64, self.active_page_id) << 16) | offset;
        return roll_ptr;
    }

    /// Decodes the undo record addressed by `roll_ptr` into an owned
    /// [`UndoRecord`].
    ///
    /// Splits `roll_ptr` back into `page_id = roll_ptr >> 16` and
    /// `offset = roll_ptr & 0xFFFF`, fetches (and pins) that page, copies the
    /// header out, then `dupe`s the `fixed` and `heap` images into `allocator`
    /// so the result outlives the pin, which is released before returning. The
    /// caller owns the copies and must call [`UndoRecord.deinit`] with the same
    /// `allocator`.
    ///
    /// Takes no lock: undo records are immutable once written, so a concurrent
    /// [`UndoLog.appendRecord`] (which only ever advances into fresh space)
    /// cannot disturb an already-written record. Returns an error if the page
    /// cannot be fetched or an allocation fails; the `errdefer` frees `fixed` if
    /// duplicating `heap` fails, so no partial record leaks.
    pub fn getRecord(self: *UndoLog, roll_ptr: u64, allocator: Allocator) !UndoRecord {
        const page_id = @as(PageId, @intCast(roll_ptr >> 16));
        const offset = @as(u16, @intCast(roll_ptr & 0xFFFF));

        const f = try self.pool.fetchPage(page_id);
        defer self.pool.unpinPage(page_id, false);
        const p = self.pool.pageOf(f);

        var rec_hdr: UndoRecordHeader = undefined;
        @memcpy(std.mem.asBytes(&rec_hdr), p.data[offset..][0..@sizeOf(UndoRecordHeader)]);

        const fixed = try allocator.dupe(u8, p.data[offset + @sizeOf(UndoRecordHeader) ..][0..rec_hdr.fixed_len]);
        errdefer allocator.free(fixed);

        const heap = try allocator.dupe(u8, p.data[offset + @sizeOf(UndoRecordHeader) + rec_hdr.fixed_len ..][0..rec_hdr.heap_len]);

        return UndoRecord{
            .xmin = rec_hdr.xmin,
            .xmax = rec_hdr.xmax,
            .roll_ptr = rec_hdr.roll_ptr,
            .fixed = fixed,
            .heap = heap,
        };
    }
};
