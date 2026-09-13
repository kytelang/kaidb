//! Slotted-page layout: the on-disk unit of the NovaDB B+Tree.
//!
//! Every node of the B+Tree, plus every overflow and undo page, is one
//! fixed-size [`PAGE_SIZE`] byte block laid out as a *slotted page*. This file
//! owns that byte-level format and nothing above it: it knows how cells are
//! packed into a page and how to insert/find/delete/compact them, but it has no
//! knowledge of tree structure, splitting policy, locking, the buffer pool, or
//! the WAL. `btree.zig` drives all of that on top of the primitives here.
//!
//! ## Physical layout
//!
//! A page grows from *both ends* toward the middle, which is the classic
//! slotted-page trick that lets variable-length records coexist with an
//! ordered directory without moving payloads on every insert:
//!
//! ```text
//!  0                                                             PAGE_SIZE
//!  +----------------+------------------+............+--------------------+
//!  |  PageHeader    | CellPtr[0..n)    |  free gap  |  cell payloads     |
//!  |  (fixed)       | slot directory → |            |  ← key||value      |
//!  +----------------+------------------+............+--------------------+
//!  ^                ^                  ^            ^
//!  0        sizeOf(PageHeader)   free_space_start   free_space_end
//! ```
//!
//! The [`PageHeader`] sits at offset 0. Immediately after it the *slot
//! directory* grows UP: a densely packed array of [`CellPtr`] entries, one per
//! live cell, kept in **key order**. Cell payloads (`key` bytes followed by
//! `value` bytes) grow DOWN from the top of the page. The gap between
//! `free_space_start` (end of the directory) and `free_space_end` (start of the
//! lowest payload) is the free space; a cell fits only if that gap can hold both
//! its payload AND one more `CellPtr` (see [`SlottedPage.hasSpace`]).
//!
//! Because the directory is ordered but the payloads are not, inserting a cell
//! in the middle shifts only the small fixed-size `CellPtr` entries, never the
//! variable payload bytes. Deletion likewise only removes a directory slot; the
//! payload bytes are left as dead space until [`SlottedPage.compact`] reclaims
//! them.
//!
//! ## Cell semantics per page type
//!
//! The `value` field of a cell is interpreted by the caller, not here, and its
//! meaning depends on [`PageType`]:
//!  - **leaf**: `value` is the record payload (or an overflow pointer when the
//!    [`CellFlags`] overflow bits are set).
//!  - **internal**: `value` is an 8-byte little-endian child [`PageId`] for the
//!    subtree of keys `>=` this cell's key. [`SlottedPage.findChildPageId`] and
//!    [`SlottedPage.findChildIndex`] encode that convention, and
//!    `leftmost_child_id` in the header holds the child for keys smaller than
//!    every separator.
//!
//! ## Durability and integrity
//!
//! `checksum` is deliberately the first field of both [`Header`] and
//! [`PageHeader`] (asserted in the module `comptime` block) so the checksum can
//! cover "everything after the first 8 bytes" uniformly. `page_lsn` records the
//! log sequence number of the last WAL record that modified the page, which is
//! what recovery compares against to decide whether a redo has already been
//! applied. This file only stores these fields; the pager and WAL maintain them.
//!
//! ## Concurrency
//!
//! Nothing here is thread-safe on its own. A [`SlottedPage`] is a thin view over
//! a `[]u8` buffer; callers in `btree.zig` serialise mutation through the
//! per-tree `structure_lock` and per-table `GroupLock`. Treat every mutating
//! method (`insertCell`, `deleteCell`, `updateCell`, `compact`, `reset`,
//! `clear`) as requiring exclusive access to the underlying bytes.

const std = @import("std");
/// Alias for `std.mem`, used for the byte-order (`readInt`/`writeInt`) and
/// ordering (`order`) helpers that the slot manipulation relies on.
const mem = std.mem;
/// Allocator interface used when a page owns its backing buffer
/// ([`SlottedPage.initOwned`]) or needs scratch space.
const Allocator = std.mem.Allocator;
/// Standard testing namespace for the in-file unit tests.
const testing = std.testing;

/// Identifier of a page within the file, as a 0-based index into the
/// [`PAGE_SIZE`]-strided page array. `0` doubles as the "no page" sentinel in
/// header link fields (`parent_page_id`, `next_page_id`, `leftmost_child_id`,
/// `free_page_list_head`), so page 0 is reserved for the file [`Header`].
pub const PageId = u64;

/// The fixed size of every page, in bytes.
///
/// 16 KiB is chosen so a page holds many cells (amortising the per-page header
/// and directory overhead) while staying a multiple of the OS page size for
/// aligned I/O. The module `comptime` block asserts it fits in a `u16`, because
/// every in-page offset (`offset`, `free_space_start`/`end`) is stored as a
/// `u16`.
pub const PAGE_SIZE: u32 = 16384;

/// Magic number written into the file [`Header`] to identify a NovaDB data file
/// (ASCII "ATSS" little-endian). A mismatch on open means the file is not a
/// NovaDB database or is corrupt.
pub const MAGIC: u32 = 0x53535441;
/// On-disk format version stamped into the file [`Header`]. Bump this on any
/// incompatible change to the page or header layout so old files are rejected.
pub const VERSION: u8 = 1;

/// Decodes the file-level [`Header`] from the first bytes of page 0.
///
/// Copies exactly `@sizeOf(Header)` bytes out of `data` by value; the returned
/// header does not alias the buffer. `data` must be at least `@sizeOf(Header)`
/// bytes or the `@memcpy` bounds-slice will trip. Pairs with [`writeHeader`].
pub fn readHeader(data: []const u8) Header {
    var h: Header = undefined;
    @memcpy(std.mem.asBytes(&h), data[0..@sizeOf(Header)]);
    return h;
}

/// Serialises the file-level [`Header`] into the first bytes of page 0.
///
/// Byte-for-byte the inverse of [`readHeader`]; `data` must have room for
/// `@sizeOf(Header)` bytes. The checksum is expected to already be computed into
/// `h` by the caller (this routine does not recompute it).
pub fn writeHeader(data: []u8, h: *const Header) void {
    @memcpy(data[0..@sizeOf(Header)], std.mem.asBytes(h));
}

// Compile-time invariants that lock the on-disk binary layout.
//
// These run at compile time and fail the build rather than at runtime, so an
// accidental field reorder or size change to CellPtr/PageHeader/Header, which
// would silently corrupt every existing file, is caught immediately. In
// particular they pin the exact byte offsets a CellPtr is hand-serialised at in
// SlottedPage.insertCell, and that both checksums sit at offset 0 (see the
// module header for why that matters).
comptime {
    std.debug.assert(PAGE_SIZE <= std.math.maxInt(u16));
    std.debug.assert(@sizeOf(CellPtr) == 8);
    std.debug.assert(@offsetOf(CellPtr, "offset") == 0);
    std.debug.assert(@offsetOf(CellPtr, "key_size") == 2);
    std.debug.assert(@offsetOf(CellPtr, "value_size") == 4);
    std.debug.assert(@offsetOf(CellPtr, "flags") == 6);
    std.debug.assert(@offsetOf(PageHeader, "checksum") == 0);
    std.debug.assert(@offsetOf(Header, "checksum") == 0);
    std.debug.assert(@sizeOf(PageHeader) < PAGE_SIZE);
}

/// The role a page plays in the storage engine, stored in [`PageHeader.page_type`].
///
/// The explicit `u8` backing and fixed discriminants are part of the on-disk
/// format, so the numeric values must never be reassigned.
pub const PageType = enum(u8) {
    /// B+Tree leaf: cells hold real keys and record payloads; `next_page_id`
    /// chains leaves for range scans.
    leaf = 0,
    /// B+Tree internal node: cells hold separator keys whose `value` is an
    /// 8-byte child [`PageId`]; `leftmost_child_id` holds the below-all child.
    internal = 1,
    /// Overflow page: spillover storage for a key or value too large to fit
    /// inline in a leaf cell (flagged via [`CellFlags`]).
    overflow = 2,
    /// Undo page: stores prior row versions for MVCC / rollback.
    undo = 3,
};

/// Fixed-size header at offset 0 of every page (leaf, internal, overflow, undo).
///
/// `extern struct` so its layout is C-ABI-stable and can be `@memcpy`'d straight
/// to and from the page bytes. Field order is frozen by the module `comptime`
/// asserts; do not reorder.
pub const PageHeader = extern struct {
    /// Integrity checksum over the page. Kept first (offset 0) so it can cover
    /// all bytes that follow it uniformly. Maintained by the pager/WAL.
    checksum: u64,
    /// Which kind of page this is; see [`PageType`].
    page_type: PageType,
    /// Number of live cells, i.e. the length of the slot directory.
    num_cells: u16,
    /// Byte offset where the slot directory ends and the free gap begins;
    /// equivalently `@sizeOf(PageHeader) + num_cells * @sizeOf(CellPtr)`.
    free_space_start: u16,
    /// Byte offset of the lowest cell payload, i.e. the top of the free gap.
    /// Payloads occupy `[free_space_end, PAGE_SIZE)`.
    free_space_end: u16,
    /// Parent node's [`PageId`], or `0` for the root. Maintained by the tree.
    parent_page_id: PageId,
    /// For leaves, the next leaf in key order (sibling chain for range scans);
    /// `0` terminates the chain. Meaning is tree-defined for other page types.
    next_page_id: PageId,
    /// For internal nodes, the child subtree for keys smaller than every
    /// separator in this node. Unused (`0`) on leaves.
    leftmost_child_id: PageId,
    /// Log sequence number of the last WAL record that modified this page.
    /// Recovery compares it against the log to skip already-applied redos.
    page_lsn: u64 = 0,
};

/// The database file header, living in page 0.
///
/// `extern struct` for a stable on-disk layout, `@memcpy`'d via [`readHeader`]/
/// [`writeHeader`]. `checksum` is deliberately first (asserted at comptime) to
/// match [`PageHeader`]'s convention.
pub const Header = extern struct {
    /// Integrity checksum over the header; first field so it covers the rest.
    checksum: u64 = 0,
    /// [`MAGIC`] identifying this as a NovaDB file; validated on open.
    magic: u32,
    /// On-disk format [`VERSION`]; a mismatch rejects the file.
    version: u8,
    /// The page size the file was created with (must equal [`PAGE_SIZE`]).
    page_size: u32,
    /// [`PageId`] of the B+Tree root page.
    root_page_id: PageId,
    /// The current log sequence number / durability watermark for the file.
    lsn: u64,
    /// Head of the free-page list (reclaimed pages available for reuse); `0`
    /// means the list is empty and growth must extend the file.
    free_page_list_head: PageId = 0,
};

/// Per-cell flags packed into a single byte, stored in [`CellPtr.flags`].
///
/// `packed struct(u8)` so it is exactly one byte on disk at `CellPtr` offset 6.
/// The overflow bits tell the caller that the corresponding inline bytes are not
/// the real data but a pointer into an [`PageType.overflow`] page chain.
pub const CellFlags = packed struct(u8) {
    /// Set when the cell's `value` is stored on an overflow page rather than
    /// inline (value exceeded the inline threshold).
    value_overflow: bool = false,
    /// Set when the cell's `key` is stored on an overflow page rather than
    /// inline.
    key_overflow: bool = false,
    /// Reserved bits, kept zero to preserve the byte width and allow future
    /// flags without a format change.
    _reserved: u6 = 0,
};

/// A slot-directory entry: the fixed 8-byte record that locates one cell's
/// payload within the page.
///
/// `extern struct` with its exact field offsets asserted at comptime, because
/// [`SlottedPage.insertCell`] writes these fields by hand at literal offsets
/// (0/2/4/6) rather than through the struct, so the two representations must
/// agree. The directory is an array of these, kept in key order.
pub const CellPtr = extern struct {
    /// Byte offset within the page where the cell payload (`key || value`)
    /// begins.
    offset: u16,
    /// Length in bytes of the key portion of the payload.
    key_size: u16,
    /// Length in bytes of the value portion of the payload.
    value_size: u16,
    /// Packed [`CellFlags`] byte for this cell.
    flags: u8,
};

/// A decoded, in-memory view of one cell: its key, value, and flags.
///
/// The `key`/`value` slices point INTO the owning page buffer (see
/// [`SlottedPage.getCell`]), so a `Cell` returned from a page is only valid
/// while that page's bytes are unmodified and alive. When passed INTO
/// [`SlottedPage.insertCell`] the slices are the caller's source data, copied
/// into the page.
pub const Cell = struct {
    /// The cell's key bytes.
    key: []const u8,
    /// The cell's value bytes (payload for leaves, child [`PageId`] for
    /// internal nodes).
    value: []const u8,
    /// Overflow/status flags for this cell.
    flags: CellFlags = .{},

    /// Total payload size of the cell in bytes: `key.len + value.len`.
    ///
    /// This is the payload cost only; it excludes the 8-byte [`CellPtr`] the
    /// directory also spends. [`SlottedPage.hasSpace`] adds that separately.
    /// The `@intCast`s are safe because callers reject over-`u16` keys/values
    /// before this is used for sizing.
    pub fn len(self: *const Cell) u32 {
        return @as(u32, @intCast(self.key.len)) +
            @as(u32, @intCast(self.value.len));
    }
};

/// A mutable view over one page's raw bytes, exposing slotted-page operations.
///
/// This is intentionally a thin wrapper: it owns nothing unless created via
/// [`initOwned`], and all state lives in `data`. Not thread-safe; see the
/// module header's concurrency note. The directory is kept sorted by key, so the
/// lookup helpers ([`findCellByKey`], [`findInsertIndex`], [`findChildPageId`])
/// can binary-search.
pub const SlottedPage = struct {
    /// The page's backing bytes, exactly [`PAGE_SIZE`] long. All header, slot,
    /// and payload accesses index into this slice.
    data: []u8,

    /// Reinitialises the page as an empty page of `page_type`, zeroing all bytes.
    ///
    /// Unlike [`clear`], this discards the sibling/parent links too (they are set
    /// to `0`). Use when repurposing a freshly allocated or recycled page whose
    /// prior linkage is meaningless.
    pub fn reset(self: *SlottedPage, page_type: PageType) void {
        @memset(self.data, 0);
        self.headerPtr().* = .{
            .checksum = 0,
            .page_type = page_type,
            .num_cells = 0,
            .free_space_start = @sizeOf(PageHeader),
            .free_space_end = PAGE_SIZE,
            .parent_page_id = 0,
            .next_page_id = 0,
            .leftmost_child_id = 0,
        };
    }

    /// Allocates a heap-backed [`SlottedPage`] and its [`PAGE_SIZE`] buffer,
    /// initialised empty as `page_type`.
    ///
    /// Ownership passes to the caller, who must release it with [`deinitOwned`]
    /// using the SAME allocator. `errdefer` unwinds the struct allocation if the
    /// buffer allocation fails. Intended for tests and standalone use; the real
    /// engine pages come from the buffer pool, not here.
    pub fn initOwned(allocator: Allocator, page_type: PageType) !*SlottedPage {
        const self = try allocator.create(SlottedPage);
        errdefer allocator.destroy(self);
        const buf = try allocator.alloc(u8, PAGE_SIZE);
        self.* = .{ .data = buf };
        self.reset(page_type);
        return self;
    }

    /// Frees a page previously produced by [`initOwned`], releasing both the
    /// buffer and the struct. Must be given the allocator that created it.
    pub fn deinitOwned(self: *SlottedPage, allocator: Allocator) void {
        allocator.free(self.data);
        allocator.destroy(self);
    }

    /// Reinterprets the start of `data` as the [`PageHeader`].
    ///
    /// Takes `anytype` so it works for both `*SlottedPage` and
    /// `*const SlottedPage` receivers, but always returns a MUTABLE `*PageHeader`
    /// (the const-view callers only read through it). The `@ptrCast`/`@alignCast`
    /// are sound because a freshly allocated page buffer is suitably aligned and
    /// the header lives at offset 0.
    pub fn headerPtr(self: anytype) *PageHeader {
        return @ptrCast(@alignCast(self.data.ptr));
    }

    /// Bytes currently free in the gap between the slot directory and the lowest
    /// payload (`free_space_end - free_space_start`).
    ///
    /// This is raw contiguous free space and does NOT include dead space left by
    /// deleted cells below `free_space_end`; [`compact`] reclaims that.
    pub fn freeSpace(self: *const SlottedPage) u16 {
        const h = self.headerPtr();
        return h.free_space_end - h.free_space_start;
    }

    /// Whether a cell of payload size `cell_size` can be inserted right now.
    ///
    /// Accounts for BOTH the payload and the extra [`CellPtr`] the directory
    /// grows by, so it is the true admission test used by [`insertCell`]. Uses
    /// `u32` arithmetic to avoid `u16` overflow on the sum. A `false` here after
    /// deletions may become `true` once [`compact`] reclaims dead space.
    pub fn hasSpace(self: *const SlottedPage, cell_size: u32) bool {
        return @as(u32, self.freeSpace()) >= cell_size + @as(u32, @sizeOf(CellPtr));
    }

    /// On an internal node, returns the child [`PageId`] to descend into for
    /// `key`.
    ///
    /// Binary-searches the ordered directory for the LAST separator `<= key`
    /// (the `.lt → right`, else `left` branch walks the boundary): if no
    /// separator is `<= key`, `key` is smaller than everything and the
    /// `leftmost_child_id` is returned; otherwise the found cell's `value`
    /// (an 8-byte little-endian [`PageId`]) is the child. A slot whose payload
    /// fails to decode ([`getCell`] returns null, e.g. a tombstoned slot) is
    /// treated as ">=" and skipped left. Caller must ensure this is an internal
    /// page; the routine does not check `page_type`.
    pub fn findChildPageId(self: *const SlottedPage, key: []const u8) PageId {
        const h = self.headerPtr();
        var left: i32 = -1;
        var right: i32 = @intCast(h.num_cells);
        while (right - left > 1) {
            const mid: i32 = left + @divTrunc(right - left, 2);
            const cell = self.getCell(@intCast(mid)) orelse {
                right = mid;
                continue;
            };
            switch (mem.order(u8, key, cell.key)) {
                .lt => right = mid,
                else => left = mid,
            }
        }
        if (left == -1) return h.leftmost_child_id;
        const cell = self.getCell(@intCast(left)).?;
        return mem.readInt(PageId, cell.value[0..@sizeOf(PageId)], .little);
    }

    /// Returns the directory index at which `key` should be inserted to keep the
    /// order sorted, i.e. the count of existing cells whose key is `< key`.
    ///
    /// This is a lower-bound binary search: on an exact match it returns the
    /// index of the matching cell (so a subsequent insert shifts it right).
    /// A slot that fails to decode is treated as `< key` and skipped right,
    /// which keeps a corrupt/tombstoned slot from stalling the search.
    pub fn findInsertIndex(self: *const SlottedPage, key: []const u8) u16 {
        var left: u16 = 0;
        var right: u16 = self.headerPtr().num_cells;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const mid_cell = self.getCell(mid) orelse {
                left = mid + 1;
                continue;
            };
            if (mem.order(u8, mid_cell.key, key) == .lt) {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        return left;
    }

    /// On an internal node, finds the directory position of the child pointer
    /// equal to `child_id`, expressed as a *child slot* index.
    ///
    /// Returns `0` when `child_id` is the `leftmost_child_id`, and `i + 1` when
    /// it is the child stored in cell `i` (the `+1` offset accounts for the
    /// leftmost child occupying logical slot 0). Returns `null` if no child
    /// pointer matches. This is a LINEAR scan (children are not ordered by id),
    /// used when the tree must locate a known child within its parent, e.g.
    /// during split/merge fix-ups.
    pub fn findChildIndex(self: *const SlottedPage, child_id: PageId) ?u16 {
        if (self.headerPtr().leftmost_child_id == child_id) return 0;
        var i: u16 = 0;
        while (i < self.headerPtr().num_cells) : (i += 1) {
            const cell = self.getCell(i) orelse continue;
            const pid = mem.readInt(PageId, cell.value[0..@sizeOf(PageId)], .little);
            if (pid == child_id) return i + 1;
        }
        return null;
    }

    /// Inserts `cell` into the directory at position `index`, copying its payload
    /// into the page.
    ///
    /// The subtle part is the two-ended write. The slot directory is shifted:
    /// `mem.copyBackwards` opens a one-`CellPtr` hole at `index` by moving the
    /// `[index, num_cells)` slots up (backwards copy because source and
    /// destination overlap). The payload is written at `free_space_end -
    /// cell.len()`, growing DOWN, so existing payloads never move. The new
    /// `CellPtr` is assembled in a local buffer at the exact frozen offsets
    /// (0/2/4/6) and memcpy'd into the hole. Finally `num_cells`,
    /// `free_space_start` and `free_space_end` are advanced.
    ///
    /// `index` must be a valid insertion position (typically from
    /// [`findInsertIndex`]); the directory is NOT re-sorted, so inserting at the
    /// wrong index breaks the ordering invariant every lookup relies on.
    ///
    /// Errors: `error.ZeroLengthKey` (empty keys are disallowed because a
    /// zero `key_size` is the tombstone/"absent" marker in [`getCell`]),
    /// `error.KeyTooLarge`/`error.ValueTooLarge` when a length exceeds `u16`,
    /// and `error.PageFull` when [`hasSpace`] is false (caller should split or
    /// [`compact`] and retry).
    pub fn insertCell(self: *SlottedPage, index: u16, cell: Cell) !void {
        if (cell.key.len == 0) return error.ZeroLengthKey;
        if (cell.key.len > std.math.maxInt(u16)) return error.KeyTooLarge;
        if (cell.value.len > std.math.maxInt(u16)) return error.ValueTooLarge;
        if (!self.hasSpace(cell.len())) return error.PageFull;

        const h = self.headerPtr();
        const n = h.num_cells;
        const cps = @sizeOf(PageHeader);
        const new_off = cps + index * @sizeOf(CellPtr);
        const to_move = (n - index) * @sizeOf(CellPtr);

        if (to_move > 0) {
            mem.copyBackwards(
                u8,
                self.data[new_off + @sizeOf(CellPtr) ..][0..to_move],
                self.data[new_off..][0..to_move],
            );
        }

        const data_off: u16 = @intCast(h.free_space_end - cell.len());
        @memcpy(self.data[data_off..][0..cell.key.len], cell.key);
        @memcpy(self.data[data_off + cell.key.len ..][0..cell.value.len], cell.value);

        var pb: [@sizeOf(CellPtr)]u8 = undefined;
        mem.writeInt(u16, pb[0..2], data_off, .little);
        mem.writeInt(u16, pb[2..4], @intCast(cell.key.len), .little);
        mem.writeInt(u16, pb[4..6], @intCast(cell.value.len), .little);
        pb[6] = @bitCast(cell.flags);
        @memcpy(self.data[new_off..][0..@sizeOf(CellPtr)], &pb);

        h.num_cells += 1;
        h.free_space_start += @sizeOf(CellPtr);
        h.free_space_end = data_off;
    }

    /// Decodes the cell at directory position `index`, returning a [`Cell`] whose
    /// slices alias the page buffer.
    ///
    /// Returns `null` for three distinct "not a live cell" cases, all of which
    /// callers treat uniformly: `index` past `num_cells`, a slot whose
    /// `key_size` is `0` (a deleted/tombstoned slot), and a slot whose decoded
    /// `offset + key_size + value_size` runs past [`PAGE_SIZE`] (a corrupt or
    /// out-of-bounds pointer, rejected instead of producing an OOB slice). The
    /// returned slices are only valid until the page is next mutated.
    pub fn getCell(self: *const SlottedPage, index: u16) ?Cell {
        const h = self.headerPtr();
        if (index >= h.num_cells) return null;

        const cpo = @sizeOf(PageHeader) + index * @sizeOf(CellPtr);
        const offset = mem.readInt(u16, self.data[cpo..][0..2], .little);
        const key_size = mem.readInt(u16, self.data[cpo + 2 ..][0..2], .little);
        if (key_size == 0) return null;

        const value_size = mem.readInt(u16, self.data[cpo + 4 ..][0..2], .little);
        const flags: CellFlags = @bitCast(self.data[cpo + 6]);
        const end = @as(u32, offset) + key_size + value_size;
        if (end > PAGE_SIZE) return null;

        return Cell{
            .key = self.data[offset..][0..key_size],
            .value = self.data[offset + key_size ..][0..value_size],
            .flags = flags,
        };
    }

    /// Overwrites the value bytes of the cell at `index` in place.
    ///
    /// This is the fast update path and works ONLY for a same-length value: the
    /// payload cannot be moved or resized in place, so a differing length is
    /// rejected with `error.ValueSizeMismatch` (the caller must delete and
    /// reinsert instead). Other errors: `error.InvalidCellIndex` when `index`
    /// is out of range, and `error.CellDeleted` when the slot is a tombstone
    /// (`key_size == 0`). The key is left untouched.
    pub fn updateCell(self: *SlottedPage, index: u16, new_value: []const u8) !void {
        const h = self.headerPtr();
        if (index >= h.num_cells) return error.InvalidCellIndex;
        const cpo = @sizeOf(PageHeader) + index * @sizeOf(CellPtr);
        const offset = mem.readInt(u16, self.data[cpo..][0..2], .little);
        const key_size = mem.readInt(u16, self.data[cpo + 2 ..][0..2], .little);
        if (key_size == 0) return error.CellDeleted;
        const value_size = mem.readInt(u16, self.data[cpo + 4 ..][0..2], .little);
        if (new_value.len != value_size) return error.ValueSizeMismatch;
        @memcpy(self.data[offset + key_size ..][0..value_size], new_value);
    }

    /// Removes the directory slot at `index`, shifting the following slots down.
    ///
    /// Only the [`CellPtr`] directory is compacted (`copyForwards` because the
    /// move is downward and overlaps); the cell's PAYLOAD bytes are intentionally
    /// left in place as dead space, to be reclaimed later by [`compact`]. This
    /// keeps deletion O(directory) rather than O(page). Out-of-range `index` is a
    /// no-op. `free_space_start` shrinks by one `CellPtr`, but `free_space_end`
    /// does NOT grow (the dead payload still sits below it).
    pub fn deleteCell(self: *SlottedPage, index: u16) void {
        const h = self.headerPtr();
        if (index >= h.num_cells) return;

        const cpo = @sizeOf(PageHeader) + index * @sizeOf(CellPtr);
        const to_move = (h.num_cells - 1 - index) * @sizeOf(CellPtr);
        if (to_move > 0) {
            mem.copyForwards(
                u8,
                self.data[cpo..][0..to_move],
                self.data[cpo + @sizeOf(CellPtr)..][0..to_move],
            );
        }
        h.num_cells -= 1;
        h.free_space_start -= @sizeOf(CellPtr);
    }

    /// Compacts the page, reclaiming the dead space left behind by deletes and
    /// same-key overwrites so the free gap becomes contiguous again.
    ///
    /// Algorithm: sort a scratch index array by each live cell's payload
    /// `offset` DESCENDING, then walk from the top of the page pushing every
    /// payload as high as it will go, closing the holes. Sorting highest-offset
    /// first guarantees the `copyBackwards` destination never overlaps a payload
    /// not yet moved, so no live bytes are clobbered. Each moved cell's `offset`
    /// field in its `CellPtr` is rewritten; the directory order (and thus key
    /// order) is untouched.
    ///
    /// The `scratch` allocator parameter is currently unused (the index array is
    /// stack-allocated with a compile-time `MAX_CELLS` bound); it is kept in the
    /// signature for callers and possible future spill. Returns
    /// `error.TooManyCells` if `num_cells` somehow exceeds the theoretical
    /// maximum the page geometry allows (a corruption guard). An empty page is
    /// reset to full free space fast-path. After compaction the reclaimed region
    /// `[free_space_start, free_space_end)` is zeroed.
    pub fn compact(self: *SlottedPage, scratch: Allocator) !void {
        _ = scratch;
        const h = self.headerPtr();
        if (h.num_cells == 0) {
            h.free_space_start = @sizeOf(PageHeader);
            h.free_space_end = PAGE_SIZE;
            return;
        }

        const MAX_CELLS: usize = (PAGE_SIZE - @sizeOf(PageHeader)) / (@sizeOf(CellPtr) + 1);
        var idxs: [MAX_CELLS]u16 = undefined;
        if (h.num_cells > idxs.len) return error.TooManyCells;

        for (0..h.num_cells) |i| {
            idxs[i] = @intCast(i);
        }

        // Sort context that orders directory indices by their payload offset,
        // highest first, so compact can slide payloads up without
        // overwriting ones it has not moved yet.
        const Context = struct {
            page: *const SlottedPage,
            pub fn lessThan(ctx: @This(), a: u16, b: u16) bool {
                const cpo_a = @sizeOf(PageHeader) + a * @sizeOf(CellPtr);
                const cpo_b = @sizeOf(PageHeader) + b * @sizeOf(CellPtr);
                const offset_a = mem.readInt(u16, ctx.page.data[cpo_a..][0..2], .little);
                const offset_b = mem.readInt(u16, ctx.page.data[cpo_b..][0..2], .little);
                return offset_a > offset_b;
            }
        };

        std.sort.block(u16, idxs[0..h.num_cells], Context{ .page = self }, Context.lessThan);

        var free_end: u16 = PAGE_SIZE;
        for (idxs[0..h.num_cells]) |idx| {
            const cpo = @sizeOf(PageHeader) + idx * @sizeOf(CellPtr);
            const offset = mem.readInt(u16, self.data[cpo..][0..2], .little);
            const key_size = mem.readInt(u16, self.data[cpo + 2 ..][0..2], .little);
            const value_size = mem.readInt(u16, self.data[cpo + 4 ..][0..2], .little);
            const len = key_size + value_size;

            if (offset != free_end - len) {
                const new_offset = free_end - len;
                mem.copyBackwards(u8, self.data[new_offset..][0..len], self.data[offset..][0..len]);
                mem.writeInt(u16, self.data[cpo..][0..2], new_offset, .little);
            }
            free_end -= len;
        }

        h.free_space_start = @sizeOf(PageHeader) + h.num_cells * @sizeOf(CellPtr);
        h.free_space_end = free_end;

        if (h.free_space_end > h.free_space_start) {
            @memset(self.data[h.free_space_start..h.free_space_end], 0);
        }
    }

    /// Binary-searches the ordered directory for an exact-match `key`, returning
    /// its slot index or `null`.
    ///
    /// Relies on the directory being kept in key order. A slot that fails to
    /// decode is treated as `< key` (skipped right), matching
    /// [`findInsertIndex`]. This is the point lookup used by leaf reads;
    /// [`findChildPageId`] is its internal-node counterpart.
    pub fn findCellByKey(self: *const SlottedPage, key: []const u8) ?u16 {
        var left: u16 = 0;
        var right: u16 = self.headerPtr().num_cells;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const cell = self.getCell(mid) orelse {
                left = mid + 1;
                continue;
            };
            switch (mem.order(u8, key, cell.key)) {
                .lt => right = mid,
                .gt => left = mid + 1,
                .eq => return mid,
            }
        }
        return null;
    }

    /// Empties the page of all cells while PRESERVING its structural linkage.
    ///
    /// Unlike [`reset`], this snapshots and restores `page_type`,
    /// `parent_page_id`, `next_page_id` and `leftmost_child_id`, so the page
    /// keeps its place in the tree/leaf chain while losing its contents. Used
    /// when a node must be reused in situ (e.g. after redistributing all its
    /// cells) without re-linking it.
    pub fn clear(self: *SlottedPage) void {
        const h = self.headerPtr();
        const pt = h.page_type;
        const pid = h.parent_page_id;
        const nid = h.next_page_id;
        const lc = h.leftmost_child_id;
        @memset(self.data, 0);
        h.* = .{
            .checksum = 0,
            .page_type = pt,
            .num_cells = 0,
            .free_space_start = @sizeOf(PageHeader),
            .free_space_end = PAGE_SIZE,
            .parent_page_id = pid,
            .next_page_id = nid,
            .leftmost_child_id = lc,
        };
    }

    /// Returns a forward [`CellsIterator`] over the live cells in key order,
    /// starting at index 0. A convenient way to scan a page without hand-managing
    /// the index and the tombstone-skipping [`getCell`] returns.
    pub fn cells(self: *const SlottedPage) CellsIterator {
        return .{ .page = self, .index = 0 };
    }
};

/// Forward iterator over the live cells of a [`SlottedPage`], in directory
/// (key) order.
///
/// Holds a `*const` view of the page and the next index to visit; it does not
/// own the page. Obtained from [`SlottedPage.cells`]. Because the yielded
/// [`Cell`] slices alias the page, the page must not be mutated during iteration.
pub const CellsIterator = struct {
    /// The page being iterated.
    page: *const SlottedPage,
    /// The next directory index to attempt; advances past skipped tombstones.
    index: u16,

    /// Returns the next live cell, or `null` at end of page.
    ///
    /// Skips slots that [`SlottedPage.getCell`] rejects (tombstones / corrupt
    /// pointers) by looping rather than returning `null` prematurely, so a
    /// deleted slot in the middle does not truncate the scan.
    pub fn next(self: *CellsIterator) ?Cell {
        while (self.index < self.page.headerPtr().num_cells) {
            const i = self.index;
            self.index += 1;
            if (self.page.getCell(i)) |c| return c;
        }
        return null;
    }
};

test "page - constants" {
    try testing.expectEqual(@as(u32, 0x53535441), MAGIC);
    try testing.expectEqual(@as(u8, 1), VERSION);
    try testing.expectEqual(@as(u32, 16384), PAGE_SIZE);
}

test "page - CellPtr layout locked (8 bytes: offset,key_size,value_size,flags)" {
    try testing.expectEqual(@as(usize, 8), @sizeOf(CellPtr));
    try testing.expectEqual(@as(usize, 0), @offsetOf(CellPtr, "offset"));
    try testing.expectEqual(@as(usize, 2), @offsetOf(CellPtr, "key_size"));
    try testing.expectEqual(@as(usize, 4), @offsetOf(CellPtr, "value_size"));
    try testing.expectEqual(@as(usize, 6), @offsetOf(CellPtr, "flags"));
}

test "page - checksum fields at offset 0" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(PageHeader, "checksum"));
    try testing.expectEqual(@as(usize, 0), @offsetOf(Header, "checksum"));
}

test "page - PageType values" {
    try testing.expectEqual(@as(u8, 0), @intFromEnum(PageType.leaf));
    try testing.expectEqual(@as(u8, 1), @intFromEnum(PageType.internal));
    try testing.expectEqual(@as(u8, 2), @intFromEnum(PageType.overflow));
}

test "page - Cell len" {
    try testing.expectEqual(@as(u32, 25), (Cell{ .key = "test_key", .value = "test_value_longer" }).len());
    try testing.expectEqual(@as(u32, 3), (Cell{ .key = "key", .value = "" }).len());
}

test "page - SlottedPage initOwned" {
    const page = try SlottedPage.initOwned(testing.allocator, .leaf);
    defer page.deinitOwned(testing.allocator);
    try testing.expectEqual(PageType.leaf, page.headerPtr().page_type);
    try testing.expectEqual(@as(u16, 0), page.headerPtr().num_cells);
    try testing.expectEqual(@as(u16, PAGE_SIZE - @sizeOf(PageHeader)), page.freeSpace());
}

test "page - insertCell round-trips flags" {
    const page = try SlottedPage.initOwned(testing.allocator, .leaf);
    defer page.deinitOwned(testing.allocator);
    try page.insertCell(0, .{ .key = "k", .value = "v", .flags = .{ .value_overflow = true } });
    const g = page.getCell(0).?;
    try testing.expect(g.flags.value_overflow);
    try testing.expect(!g.flags.key_overflow);
}

test "page - insertCell defaults to no overflow flags" {
    const page = try SlottedPage.initOwned(testing.allocator, .leaf);
    defer page.deinitOwned(testing.allocator);
    try page.insertCell(0, .{ .key = "k", .value = "v" });
    const g = page.getCell(0).?;
    try testing.expect(!g.flags.value_overflow);
}

test "page - insertCell rejects zero-length key" {
    const page = try SlottedPage.initOwned(testing.allocator, .leaf);
    defer page.deinitOwned(testing.allocator);
    try testing.expectError(error.ZeroLengthKey, page.insertCell(0, .{ .key = "", .value = "v" }));
}

test "page - multiple cells" {
    const page = try SlottedPage.initOwned(testing.allocator, .leaf);
    defer page.deinitOwned(testing.allocator);
    try page.insertCell(0, .{ .key = "aaa", .value = "1" });
    try page.insertCell(1, .{ .key = "bbb", .value = "2" });
    try page.insertCell(2, .{ .key = "ccc", .value = "3" });
    try testing.expectEqualStrings("aaa", page.getCell(0).?.key);
    try testing.expectEqualStrings("bbb", page.getCell(1).?.key);
    try testing.expectEqualStrings("ccc", page.getCell(2).?.key);
}

test "page - compact preserves flags" {
    const page = try SlottedPage.initOwned(testing.allocator, .leaf);
    defer page.deinitOwned(testing.allocator);
    try page.insertCell(0, .{ .key = "a", .value = "1" });
    try page.insertCell(1, .{ .key = "b", .value = "234567890123", .flags = .{ .value_overflow = true } });
    page.deleteCell(0);
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    try page.compact(arena.allocator());
    try testing.expectEqual(@as(u16, 1), page.headerPtr().num_cells);
    try testing.expect(page.getCell(0).?.flags.value_overflow);
}
