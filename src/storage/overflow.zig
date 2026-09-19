//! Overflow-page chains: storing values too large to live inside a B+Tree cell.
//!
//! The B+Tree keeps records (cells) inline in slotted pages, but a single value
//! can be far larger than a page. Rather than let one oversized value blow up
//! the page layout, the tree spills it into a singly-linked chain of dedicated
//! *overflow pages* and stores only a small fixed-size [`OverflowDescriptor`] in
//! the cell. This file owns that spill format: how a byte blob is split across a
//! chain, how the chain is read back into one contiguous buffer, and how it is
//! reclaimed.
//!
//! ## When a value overflows
//!
//! [`OVERFLOW_THRESHOLD`] is the cutoff the tree consults: a value at or above it
//! is a candidate for spilling instead of being stored inline. Keeping the
//! threshold well below the page size (an eighth of it) means a handful of large
//! values cannot each monopolise a whole leaf page, which would wreck fan-out and
//! range-scan locality.
//!
//! ## On-disk layout of one overflow page
//!
//! Every page in the chain begins with an [`OverflowPageHeader`], then carries up
//! to [`OVERFLOW_PAYLOAD_CAPACITY`] bytes of the value immediately after it. The
//! header's `checksum` field is deliberately the first field (offset 0): the
//! buffer pool computes and verifies a per-page checksum at a fixed offset across
//! ALL page kinds, so every page type must place its checksum word first. The
//! writers here leave `checksum` as `0`; the pool stamps the real value when the
//! frame is flushed, exactly as it does for B+Tree pages.
//!
//! `next_page_id` links to the following page, or `0` to mark the end of the
//! chain (page id `0` is the file header and never a valid overflow target, so it
//! doubles as the null terminator). `payload_len` records how many bytes of this
//! page's payload region are live, which is `OVERFLOW_PAYLOAD_CAPACITY` for every
//! page except the last, whose tail is short.
//!
//! ## The descriptor stored inline
//!
//! [`OverflowDescriptor`] is the 12-byte handle the cell keeps: the `total_len`
//! of the reassembled value and the `first_page_id` of its chain. `total_len` is
//! authoritative on read back: [`readChain`] pre-allocates exactly that many bytes
//! and treats any disagreement between the sum of the per-page `payload_len`s and
//! `total_len` as corruption, which is the cheap end-to-end integrity check that
//! guards against a truncated or mis-linked chain.
//!
//! ## Concurrency and durability
//!
//! These functions do not lock. They operate through the [`PagePool`], pinning a
//! frame for the duration of each page touch and unpinning it (dirty on write)
//! immediately after, so the pool's own eviction and checkpoint machinery carries
//! the pages to disk and the WAL. Callers hold whatever table/tree lock the
//! surrounding B+Tree operation already took; overflow work happens under that
//! umbrella. There is no in-place update: a changed value frees its old chain and
//! writes a fresh one.

const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const page = @import("page.zig");
const PageId = page.PageId;
const PAGE_SIZE = page.PAGE_SIZE;

const pool_mod = @import("pool.zig");
const PagePool = pool_mod.PagePool;

/// Size cutoff at or above which a value is spilled to an overflow chain rather
/// than stored inline in a B+Tree cell.
///
/// Set to an EIGHTH of the page. The split-correctness geometry would allow up to
/// HALF the usable page inline (kaidb's 2-way contiguous split, made byte-safe in
/// `btree.splitAndInsert`, keeps both halves within a page for cells <= usable/2),
/// but a SEPARATE, deeper constraint holds the cutoff down: CRASH RECOVERY.
///
/// kaidb recovers by LOGICAL redo - it replays committed row inserts from the
/// WAL. That cannot correctly reconstruct a B+Tree when the buffer pool evicts a
/// PARTIAL structural change: a split (or a cascading split) mutates several pages
/// together, and if a kill -9 leaves some of them on disk and others not, redo
/// produces a tree with duplicated or missing rows (verified with `D7` at large
/// inline sizes: recovered counts of 285/326/333 vs the expected 300, varying with
/// crash timing). Large values at/above this `PAGE_SIZE/8` cutoff overflow to
/// dedicated chains (written whole, torn-write-protected), so the leaf cells stay
/// tiny, splits are rare, and recovery is correct - which is why `D7` passes here.
///
/// Raising the cutoff so multi-KB rows stay inline is safe for footprint and
/// query speed (and the `CREATE TABLE` column-width fix already makes real SQL
/// rows a few hundred bytes, well inside this cutoff), but it is GATED on making
/// recovery correct for partial structural changes. That needs ARIES-style
/// physical/structural redo logging (log a split as replayable page images with
/// page LSNs) or atomic multi-level structural writes - a recovery-engine change,
/// not a threshold tweak. Until then the cutoff stays at `PAGE_SIZE/8`.
///
/// The B+Tree layer compares a candidate value's length against this; this file
/// does not enforce it (both [`writeChain`] and [`readChain`] work for any blob).
pub const OVERFLOW_THRESHOLD: u32 = PAGE_SIZE / 8;

/// Fixed header at the start of every overflow page in a chain.
///
/// `extern struct` so its layout is stable and it can be `@memcpy`'d to and from
/// the raw page bytes. Field order matters: `checksum` MUST be first (offset 0)
/// because the buffer pool checksums every page kind at that fixed offset. See
/// the module header for the full page layout.
pub const OverflowPageHeader = extern struct {
    /// Per-page checksum slot, kept first so the pool can verify it uniformly
    /// across all page types. Left `0` by the writers here; the pool computes and
    /// stamps the real value on flush.
    checksum: u64 = 0,
    /// Page id of the next page in the chain, or `0` to terminate it. Page id `0`
    /// is the file header, never a valid overflow page, so it safely doubles as
    /// the end-of-chain sentinel.
    next_page_id: PageId = 0,
    /// Number of live payload bytes in this page's payload region. Equals
    /// [`OVERFLOW_PAYLOAD_CAPACITY`] on every page except the final one, whose
    /// tail is partial.
    payload_len: u32 = 0,
};

/// Byte size of the [`OverflowPageHeader`] prefix on every overflow page.
pub const OVERFLOW_HEADER_SIZE = @sizeOf(OverflowPageHeader);
/// Maximum payload bytes one overflow page can hold: the page minus its header.
pub const OVERFLOW_PAYLOAD_CAPACITY = PAGE_SIZE - OVERFLOW_HEADER_SIZE;

// Static guards on the on-disk layout: the checksum word must sit at offset 0 (so
// the pool's uniform checksum machinery finds it) and the header must leave room
// for at least some payload in a page.
comptime {
    std.debug.assert(@offsetOf(OverflowPageHeader, "checksum") == 0);
    std.debug.assert(OVERFLOW_HEADER_SIZE < PAGE_SIZE);
}

/// The compact 12-byte handle stored inline in a B+Tree cell that points at a
/// spilled value's overflow chain.
///
/// This is what replaces the value in the cell when it overflows: it records the
/// total reassembled length and the first page of the chain, and nothing else.
/// The length is what [`readChain`] uses both to size its output buffer and to
/// detect a corrupt or truncated chain.
pub const OverflowDescriptor = struct {
    /// Total byte length of the reassembled value across the whole chain.
    total_len: u32,
    /// Page id of the first page in the chain (its head).
    first_page_id: PageId,

    /// Serialised size of a descriptor: a `u32` length plus a [`PageId`].
    ///
    /// This is the exact byte count [`encode`] writes and [`decode`] reads, and
    /// the amount of inline cell space a spilled value costs.
    pub const SIZE: usize = @sizeOf(u32) + @sizeOf(PageId);

    /// Serialises this descriptor into `buf` as little-endian: `total_len` in the
    /// first 4 bytes, `first_page_id` in the next 8. Little-endian is the wire and
    /// on-disk byte order used throughout the engine.
    pub fn encode(self: OverflowDescriptor, buf: *[SIZE]u8) void {
        mem.writeInt(u32, buf[0..4], self.total_len, .little);
        mem.writeInt(PageId, buf[4..12], self.first_page_id, .little);
    }

    /// Deserialises a descriptor from the first [`SIZE`] bytes of `bytes`.
    ///
    /// The caller must ensure `bytes` is at least [`SIZE`] long; the fixed slice
    /// bounds (`bytes[0..4]`, `bytes[4..12]`) would trap otherwise. Inverse of
    /// [`encode`].
    pub fn decode(bytes: []const u8) OverflowDescriptor {
        return .{
            .total_len = mem.readInt(u32, bytes[0..4], .little),
            .first_page_id = mem.readInt(PageId, bytes[4..12], .little),
        };
    }
};

/// Copies an [`OverflowPageHeader`] out of the raw page bytes at `data`.
///
/// A byte-for-byte `@memcpy` of the header prefix into a typed value, safe because
/// the header is an `extern struct` with a fixed layout. `data` must be at least
/// [`OVERFLOW_HEADER_SIZE`] bytes.
fn readOverflowHeader(data: []const u8) OverflowPageHeader {
    var h: OverflowPageHeader = undefined;
    @memcpy(mem.asBytes(&h), data[0..OVERFLOW_HEADER_SIZE]);
    return h;
}

/// Writes an [`OverflowPageHeader`] into the raw page bytes at `data`.
///
/// Inverse of [`readOverflowHeader`]: a byte-for-byte `@memcpy` of the typed
/// header over the first [`OVERFLOW_HEADER_SIZE`] bytes of the page. The caller is
/// responsible for marking the frame dirty (via `pool.unpinPage(..., true)`) so
/// the change is flushed.
fn writeOverflowHeader(data: []u8, h: *const OverflowPageHeader) void {
    @memcpy(data[0..OVERFLOW_HEADER_SIZE], mem.asBytes(h));
}

/// Spills `data` into a fresh overflow chain and returns the id of its first page.
///
/// Allocates as many overflow pages as needed via [`PagePool.newPage`], filling
/// each with up to [`OVERFLOW_PAYLOAD_CAPACITY`] bytes of `data` and back-patching
/// the previous page's `next_page_id` to link the chain forward. Every page is
/// pinned only for the duration of its own write and unpinned dirty immediately,
/// so the pool owns durability.
///
/// The linking is deliberately two-touch: a page is written with `next_page_id`
/// of `0` (so a crash mid-chain leaves a well-terminated, if short, chain rather
/// than a dangling link), and the PREVIOUS page is then re-fetched and its link
/// patched to point at it. `data` must be non-empty (asserted); the returned id is
/// what a caller stores in an [`OverflowDescriptor.first_page_id`]. Errors
/// propagate from the pool if a page cannot be allocated or fetched.
pub fn writeChain(pool: *PagePool, data: []const u8) !PageId {
    std.debug.assert(data.len > 0);

    var remaining = data;
    var first_id: ?PageId = null;
    var prev_id: ?PageId = null;

    while (remaining.len > 0) {
        const frame = try pool.newPage(.overflow);
        const p = pool.pageOf(frame);
        const id = frame.page_id.?;
        if (first_id == null) first_id = id;

        const chunk_len = @min(remaining.len, OVERFLOW_PAYLOAD_CAPACITY);
        @memcpy(p.data[OVERFLOW_HEADER_SIZE..][0..chunk_len], remaining[0..chunk_len]);
        const hdr = OverflowPageHeader{
            .next_page_id = 0,
            .payload_len = @intCast(chunk_len),
        };
        writeOverflowHeader(p.data, &hdr);
        pool.unpinPage(id, true);

        if (prev_id) |pid| {
            const pf = try pool.fetchPage(pid);
            const pp = pool.pageOf(pf);
            var phdr = readOverflowHeader(pp.data);
            phdr.next_page_id = id;
            writeOverflowHeader(pp.data, &phdr);
            pool.unpinPage(pid, true);
        }

        prev_id = id;
        remaining = remaining[chunk_len..];
    }

    return first_id.?;
}

/// Reassembles an overflow chain into one freshly allocated contiguous buffer.
///
/// Allocates exactly `total_len` bytes (from the caller's [`OverflowDescriptor`]),
/// then walks the chain from `first_page_id` following each page's `next_page_id`
/// until the `0` terminator, copying each page's live `payload_len` bytes into the
/// output. The caller owns the returned buffer and must free it with `allocator`.
///
/// `total_len` is the integrity oracle: if the running total would exceed it, or
/// if the walked bytes do not sum to exactly `total_len` at the end, the chain is
/// truncated, over-long, or mis-linked, and the function logs the offending
/// counters and returns `error.CorruptOverflowChain`. On that error the partially
/// filled buffer is freed by the `errdefer`. Each page is pinned only while being
/// copied and then unpinned clean (reads never dirty a page). Pool fetch errors
/// (e.g. a bad checksum) propagate.
pub fn readChain(pool: *PagePool, allocator: Allocator, first_page_id: PageId, total_len: u32) ![]u8 {
    const out = try allocator.alloc(u8, total_len);
    errdefer allocator.free(out);

    var written: usize = 0;
    var cur = first_page_id;
    while (cur != 0) {
        const frame = try pool.fetchPage(cur);
        const p = pool.pageOf(frame);
        const hdr = readOverflowHeader(p.data);
        const n = hdr.payload_len;
        if (written + n > out.len) {
            std.log.err("CorruptOverflowChain: written={d} payload_len={d} out.len={d} cur={d} next={d}", .{written, n, out.len, cur, hdr.next_page_id});
            pool.unpinPage(cur, false);
            return error.CorruptOverflowChain;
        }
        @memcpy(out[written..][0..n], p.data[OVERFLOW_HEADER_SIZE..][0..n]);
        written += n;
        const next = hdr.next_page_id;
        pool.unpinPage(cur, false);
        cur = next;
    }

    if (written != out.len) {
        std.log.err("CorruptOverflowChain: final written={d} expected out.len={d}", .{written, out.len});
        return error.CorruptOverflowChain;
    }
    return out;
}

/// Reclaims every page of an overflow chain, returning them to the pager's free
/// list.
///
/// Walks from `first_page_id` following `next_page_id` to the `0` terminator,
/// discarding each page via [`PagePool.discardPage`]. The subtlety is ordering:
/// the next-link is read (and the page unpinned) BEFORE the page is discarded,
/// because once discarded the page's contents are no longer valid to read. Called
/// when a spilled value is deleted or replaced (there is no in-place overflow
/// update: a changed value frees its old chain and writes a new one). Fetch or
/// discard errors propagate.
pub fn freeChain(pool: *PagePool, first_page_id: PageId) !void {
    var cur = first_page_id;
    while (cur != 0) {
        const frame = try pool.fetchPage(cur);
        const p = pool.pageOf(frame);
        const next = readOverflowHeader(p.data).next_page_id;
        pool.unpinPage(cur, false);
        try pool.discardPage(cur);
        cur = next;
    }
}

test "overflow - descriptor round trip" {
    var buf: [OverflowDescriptor.SIZE]u8 = undefined;
    const d = OverflowDescriptor{ .total_len = 123456, .first_page_id = 77 };
    d.encode(&buf);
    const back = OverflowDescriptor.decode(&buf);
    try testing.expectEqual(d.total_len, back.total_len);
    try testing.expectEqual(d.first_page_id, back.first_page_id);
}

test "overflow - header checksum field at offset 0" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(OverflowPageHeader, "checksum"));
}
