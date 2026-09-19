//! Segmented buffer pool: the page cache that sits between the B+Tree and the
//! on-disk file.
//!
//! Every access to a page goes through here. The pool owns a fixed number of
//! in-memory [`Frame`]s (one per `PAGE_SIZE` slot in a single backing `slab`),
//! and a [`Pager`] that does the actual file I/O. Callers ask for a page by
//! [`PageId`] via [`PagePool.fetchPage`], work with it while it is *pinned*, and
//! release it with [`PagePool.unpinPage`]. A page that has been modified is
//! marked dirty and written back lazily, either when its frame is chosen as an
//! eviction victim or when the pool is flushed/checkpointed.
//!
//! ## Sharding (why "segmented")
//!
//! A single global lock over the whole page table would serialise every fetch.
//! Instead the pool is split into [`PagePool.num_instances`] independent
//! [`PoolInstance`]s (16 for a normal-sized pool, 4 for a tiny one). A page is
//! owned by exactly one instance, chosen by `page_id % num_instances`, and each
//! instance has its OWN `page_table`, `free_list`, CLOCK hand and `rw_lock`.
//! Two threads touching pages in different instances never contend. The frame
//! array is one contiguous allocation, carved into equal `instance_size` runs;
//! instance `i` owns frames `[i*instance_size, (i+1)*instance_size)`.
//!
//! ## Eviction (CLOCK / second-chance)
//!
//! Each instance replaces pages with the CLOCK algorithm, an approximation of
//! LRU that needs no per-access bookkeeping: a `clock_hand` sweeps the frames,
//! giving a referenced-but-unpinned frame a second chance (clearing its
//! `is_referenced` bit) and evicting the first unpinned frame whose bit is
//! already clear. Pinned frames (`pin_count > 0`) are never evicted. See
//! [`PoolInstance.findVictimFrame`].
//!
//! ## Write-ahead logging invariant
//!
//! Durability is layered on top of a WAL owned elsewhere and plugged in as a
//! [`WalGate`]. The rule the pool must never break: a dirty page may only reach
//! the file AFTER every log record up to that page's `page_lsn` is durable
//! (WAL-before-page, the standard write-ahead ordering that makes crash
//! recovery possible). [`PagePool.walBeforePageLsn`] enforces exactly that at
//! every eviction write, and the whole-pool flush paths call
//! [`PagePool.walBeforePage`] up front.
//!
//! ## Torn-write protection (doublewrite buffer)
//!
//! A `PAGE_SIZE` write is not atomic on power loss; a page can be half-written
//! ("torn"). The flush paths therefore use a doublewrite buffer, mirroring
//! InnoDB: the batch is first written to a reserved staging area (page 2 holds a
//! [`DoublewriteHeader`] describing the batch, pages 3.. hold the copies) and
//! `sync`ed, THEN written in place and `sync`ed, THEN the header is cleared. On
//! recovery a non-zero header means a crash happened mid-flush and the in-place
//! pages can be restored from the doublewrite copies. Pages 0, 1 (metadata) and
//! 2, 3.. (doublewrite region) are reserved by this scheme.
//!
//! ## Integrity
//!
//! Each page carries a checksum in the first 8 bytes of its header, computed
//! over the remaining bytes. It is refreshed on the way out
//! ([`PagePool.writeChecksum`]) and verified on the way in
//! ([`PagePool.validateChecksum`]); a mismatch surfaces `error.InvalidChecksum`
//! and increments [`PagePool.checksum_failed`].

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const HashMap = std.HashMap;
const Allocator = std.mem.Allocator;
const testing = std.testing;
const sync = @import("utils").sync;
const StopWatch = @import("utils").StopWatch;
const crc32_fast = @import("crc32_fast.zig");

const page = @import("page.zig");
const PageId = page.PageId;
const PAGE_SIZE = page.PAGE_SIZE;
const PageType = page.PageType;
const SlottedPage = page.SlottedPage;
const Header = page.Header;

const pager_mod = @import("pager.zig");
/// The lower-level file abstraction ([`Pager`]) the pool reads and writes
/// through; it maps [`PageId`]s to file offsets and owns page allocation.
const Pager = pager_mod.Pager;

/// Index of a [`Frame`] within the pool's flat `frames`/`pages` arrays.
///
/// Distinct from a [`PageId`]: a frame is a fixed in-memory slot that different
/// pages occupy over time as they are cached and evicted. The `page_table` of
/// each [`PoolInstance`] maps the currently-resident `PageId` to its `FrameId`.
pub const FrameId = u32;

/// Smallest pool the constructor will accept, in frames.
///
/// Below this the 16-way sharding (see [`PagePool.init`]) would give each
/// instance too few frames to keep the working set pinned during a B+Tree
/// operation; [`PagePool.init`] returns `error.PoolTooSmall` for anything less.
pub const MIN_POOL_SIZE: u32 = 64;

/// Byte distance between consecutive page slots in the backing `slab`.
///
/// `PAGE_SIZE` rounded up to [`Header`]'s alignment, so that every page's header
/// (interpreted in place over the slab bytes) is correctly aligned. Padding
/// between the `PAGE_SIZE` payload and the next slot is left unused.
pub const FRAME_STRIDE: usize = std.mem.alignForward(usize, PAGE_SIZE, @alignOf(Header));

comptime {
    // The sharding and eviction logic assume at least MIN_POOL_SIZE frames; make
    // the floor a hard compile-time fact rather than a stale magic number.
    std.debug.assert(MIN_POOL_SIZE >= 64);
}

/// In-memory descriptor for one cacheable page slot.
///
/// The frame does NOT hold the page bytes; those live in the shared `slab` and
/// are reached through the parallel `pages` array at the same index. A frame
/// records only who currently occupies the slot and its cache/eviction state.
/// `pin_count` and the two flag bits are touched with atomics because readers
/// under the instance's SHARED lock may bump them concurrently (see
/// [`PagePool.fetchPage`] and [`PagePool.unpinPage`]).
pub const Frame = struct {
    /// The page currently resident in this frame, or `null` when the frame is
    /// free (on an instance's `free_list` or freshly evicted).
    page_id: ?PageId = null,
    /// Number of live pins. A frame with `pin_count > 0` is in use by some
    /// caller and must not be evicted or reused. Incremented on fetch,
    /// decremented on unpin.
    pin_count: u32 = 0,
    /// The in-memory copy has been modified since it was read from disk and
    /// must be written back before the frame is reused or the pool is closed.
    is_dirty: bool = false,
    /// CLOCK reference bit: set on every access, cleared to grant a "second
    /// chance" as the eviction hand sweeps past. See
    /// [`PoolInstance.findVictimFrame`].
    is_referenced: bool = false,
    /// Per-frame reader/writer latch for page-content synchronisation. Present
    /// for callers that latch page bytes; distinct from the instance-level
    /// `rw_lock` that guards the page table and eviction metadata.
    latch: sync.RwLock = .{},
    /// Phase 4 (mmap): when true, this frame's page view (`pages[fid].data`)
    /// borrows a READ-ONLY window of the pager's mmap instead of owning its slab
    /// window - a clean read served with no `pread` copy. A borrowed frame is
    /// never dirty; the first write copies the page out into the slab window and
    /// clears this flag (see [`PagePool.beginWrite`]). Eviction of a borrowed
    /// frame writes nothing back (the file/map is authoritative).
    borrowed: bool = false,
};

/// On-disk header of the doublewrite staging area (page 2).
///
/// `extern` so its byte layout is fixed and it can be `memcpy`ed straight into a
/// page buffer. A non-zero `num_pages` on startup means a flush was interrupted
/// by a crash and the listed pages must be recovered from the doublewrite copies
/// in pages 3.. before they can be trusted. Written by every doublewrite flush
/// path ([`PagePool.flushAllPages`], [`PagePool.flushPage`]).
pub const DoublewriteHeader = extern struct {
    /// Signature identifying a valid doublewrite header (`0x44574252`, the ASCII
    /// "DWBR"). Guards against interpreting arbitrary bytes as a batch.
    magic: u32,
    /// Number of valid entries in `page_ids`; `0` means "no batch in flight",
    /// the cleared state written once an in-place flush has been fully synced.
    num_pages: u32,
    /// Destination page ids of the batch being protected, one per staged copy.
    /// Caps a doublewrite batch at 16 pages.
    page_ids: [16]PageId,
};

/// One independent shard of the buffer pool.
///
/// Owns a contiguous run of `instance_size` frames starting at
/// `instance_index * instance_size`, plus all the metadata to manage them: the
/// resident-page lookup table, the free-frame list, and a CLOCK hand. Its
/// `rw_lock` is the ONLY lock a fetch/unpin on this shard's pages takes, which
/// is what makes accesses to different shards contention-free. Pages are routed
/// to a shard by `page_id % num_instances`.
pub const PoolInstance = struct {
    /// Guards this shard's `page_table`, `free_list` and `clock_hand`. Taken
    /// shared for a cache hit and for unpin, exclusive for a miss/eviction or
    /// structural change.
    rw_lock: sync.RwLock = .{},
    /// Maps each resident [`PageId`] to the [`FrameId`] that holds it. The
    /// authoritative "is this page cached, and where" index for the shard.
    page_table: HashMap(PageId, FrameId, std.hash_map.AutoContext(PageId), 80),
    /// Frames in this shard that hold no page and can be handed out without
    /// eviction. Popped first by [`PoolInstance.findVictimFrame`]; refilled by
    /// [`PagePool.discardPage`].
    free_list: std.ArrayList(FrameId),
    /// CLOCK sweep position, an offset WITHIN this shard's frame run (0 ..
    /// `instance_size`). Advances each time [`PoolInstance.findVictimFrame`]
    /// inspects a frame.
    clock_hand: FrameId = 0,
    /// This shard's ordinal (0 .. `num_instances`); multiplied by
    /// `instance_size` it gives the global base frame index of the run.
    instance_index: u32,
    /// Number of frames owned by this shard. Equal for every shard
    /// (`pool_size / num_instances`).
    instance_size: u32,

    /// Frees this shard's owned allocations (the page table and free list).
    ///
    /// Does not touch the shared `frames`/`pages`/`slab` arrays, which are owned
    /// by [`PagePool`] and freed once in [`PagePool.freeAll`].
    pub fn deinit(self: *PoolInstance, allocator: Allocator) void {
        self.page_table.deinit();
        self.free_list.deinit(allocator);
    }

    /// Picks a frame to reuse: a free one, or a CLOCK-selected eviction victim.
    ///
    /// Returns a free frame immediately if the `free_list` is non-empty.
    /// Otherwise runs the CLOCK/second-chance sweep over THIS shard's frames
    /// only: it skips pinned frames, clears the `is_referenced` bit on
    /// referenced-but-unpinned frames (their second chance), and returns the
    /// first unpinned frame whose bit was already clear. The returned id is a
    /// GLOBAL frame index; the caller ([`PagePool.fetchPage`] /
    /// [`PagePool.newPage`]) is responsible for writing back its old page if
    /// dirty and re-keying the page table.
    ///
    /// The scan is bounded to `instance_size * 2` steps so that a shard whose
    /// every frame is pinned cannot loop forever; it returns `null` in that
    /// case, which the caller maps to `error.NoFreeFrames`. Two full passes are
    /// enough because one pass at most clears every reference bit and the second
    /// is then guaranteed to find a victim unless all frames are pinned.
    /// Must be called with the shard's `rw_lock` held exclusively.
    fn findVictimFrame(self: *PoolInstance, pool: *PagePool) ?FrameId {
        if (self.free_list.pop()) |fid| return fid;
        var i: u32 = 0;
        const start_fid = self.instance_index * self.instance_size;
        while (i < self.instance_size * 2) : (i += 1) {
            const global_hand = start_fid + self.clock_hand;
            const f = &pool.frames[global_hand];
            if (f.pin_count == 0) {
                if (f.is_referenced) {
                    f.is_referenced = false;
                } else {
                    const v = global_hand;
                    self.clock_hand = (self.clock_hand + 1) % self.instance_size;
                    return v;
                }
            }
            self.clock_hand = (self.clock_hand + 1) % self.instance_size;
        }
        return null;
    }
};

/// Type-erased hook back into the write-ahead log, so the pool can enforce
/// WAL-before-page without depending on the WAL module directly.
///
/// The WAL owner installs one of these on [`PagePool.wal_gate`]. When absent
/// (`null`) the pool skips the ordering step, which is used in tests and
/// non-durable modes.
pub const WalGate = struct {
    /// Opaque pointer to the WAL implementation, passed back to `flush` and
    /// `durable_lsn`.
    ctx: *anyopaque,
    /// Makes all buffered log records durable on disk. Called before a dirty
    /// page is written back.
    flush: *const fn (*anyopaque) anyerror!void,
    /// Returns the highest LSN currently durable in the log, used to decide
    /// whether a given page's `page_lsn` already satisfies the ordering rule.
    durable_lsn: *const fn (*anyopaque) u64,
};

/// The buffer pool proper: owns the frames, the page bytes, the shards and the
/// pager, and exposes fetch/pin/flush to the rest of the engine.
///
/// Allocated on the heap (see [`PagePool.init`]) and passed around by pointer;
/// its `frames`/`pages` slices are referenced by address elsewhere, so it must
/// not be moved.
pub const PagePool = struct {
    /// Allocator backing every allocation the pool owns; retained so
    /// [`PagePool.freeAll`] can release them.
    allocator: Allocator,
    /// The file-level pager the pool reads/writes/allocates pages through. Owned
    /// by the pool and destroyed in [`PagePool.freeAll`].
    pager: *Pager,
    /// Total number of frames across all shards.
    pool_size: u32,
    /// Number of [`PoolInstance`] shards (16 for a normal pool, 4 for a small
    /// one). Also the modulus that routes a `page_id` to its shard.
    num_instances: u32,
    /// Flat array of all frame descriptors, one contiguous allocation shared by
    /// every shard. A [`FrameId`] indexes directly into it.
    frames: []Frame,
    /// Parallel array of page views, one per frame at the same index; each
    /// `SlottedPage.data` aliases a `PAGE_SIZE` window of `slab`.
    pages: []SlottedPage,
    /// Single backing allocation for ALL page bytes, laid out at
    /// [`FRAME_STRIDE`] intervals. One big slab keeps pages contiguous and
    /// cache-friendly and lets the whole pool be allocated in a handful of
    /// calls.
    slab: []u8,
    /// The `num_instances` shards, each managing its own run of frames.
    instances: []PoolInstance,
    /// Running count of checksum verification failures, incremented by
    /// [`PagePool.validateChecksum`]. Diagnostic; not reset.
    checksum_failed: u32 = 0,
    /// Running count of [`PagePool.fetchPage`] calls (hits + misses). Diagnostic
    /// only; used by tests to prove that a read path descends the tree fewer
    /// times (e.g. cursor reuse vs a fresh root-to-leaf search per document).
    /// Relaxed atomic so it never affects the hot path's correctness or ordering.
    fetch_count: std.atomic.Value(u64) = .init(0),
    /// Running count of [`PagePool.fetchPage`] calls served from a resident frame
    /// (a cache hit). `fetch_count - hit_count` is the miss count; `hit_count /
    /// fetch_count` is the buffer-pool hit ratio exported at `/metrics`. Relaxed
    /// atomic, same as `fetch_count`.
    hit_count: std.atomic.Value(u64) = .init(0),
    /// Running count of REAL evictions: a fetch/newPage that reused an
    /// already-occupied frame (its old page dropped from the table). Distinct
    /// from a free-list hand-out, which costs nothing. Stays 0 while the working
    /// set fits in the pool; climbs once it does not (the pool is thrashing).
    /// Relaxed atomic; diagnostic only.
    evict_count: std.atomic.Value(u64) = .init(0),
    /// Phase 5 diagnostics: how read misses are served and how often a borrowed
    /// page is later copied out for a write. `borrow_serves` = a miss served from
    /// the mmap with no copy (the slab window stays untouched, so no physical RAM).
    /// `pread_serves` = a miss copied into the slab window (materialised, costs a
    /// resident page). `copyout_writes` = a borrowed page made writable
    /// (`beginWrite`), which also materialises. If `borrow_serves` dominates on a
    /// read-only workload the slab stays near-zero resident; if `pread_serves` or
    /// `copyout_writes` dominate, the mmap borrow is not actually saving memory.
    borrow_serves: std.atomic.Value(u64) = .init(0),
    pread_serves: std.atomic.Value(u64) = .init(0),
    copyout_writes: std.atomic.Value(u64) = .init(0),
    /// Perf instrumentation (gated by KAIDB_QPROF, read once): breaks a cache MISS
    /// into victim selection, the `pread`, and checksum validation so the dominant
    /// per-miss cost is measurable. Not thread-safe; single-client profiling only.
    miss_prof: bool = false,
    miss_prof_checked: bool = false,
    sw_victim: StopWatch = .{},
    sw_read: StopWatch = .{},
    sw_checksum: StopWatch = .{},
    /// Phase 4 (mmap): when true, a clean read miss binds the frame view to the
    /// pager's read-only mmap (no `pread` copy), and evicting/re-fetching such a
    /// frame is nearly free, so a read set far larger than the frame count is
    /// served from the OS page cache. Off by default; enabled via
    /// [`PagePool.enableMmapReads`].
    mmap_enabled: bool = false,
    /// Serialises the doublewrite flush paths against each other, so two flushes
    /// cannot interleave in the single shared staging area (pages 2, 3..). Held
    /// uncancelably for the duration of a flush.
    doublewrite_mutex: Io.Mutex = .init,
    /// Optional hook enforcing write-ahead ordering; `null` disables the check.
    /// See [`WalGate`].
    wal_gate: ?WalGate = null,
    /// The LSN stamped onto pages dirtied through this pool ([`PagePool.newPage`]
    /// and [`PagePool.unpinPage`]). Atomic because it is advanced by the WAL as
    /// records are appended while pages are dirtied concurrently.
    current_lsn: std.atomic.Value(u64) = .init(0),

    /// Flushes the WAL unconditionally, if a gate is installed.
    ///
    /// Used by the whole-pool flush paths, which are about to write many pages
    /// and so simply make the entire log durable once up front rather than
    /// checking each page's LSN.
    inline fn walBeforePage(self: *PagePool) !void {
        if (self.wal_gate) |g| try g.flush(g.ctx);
    }

    /// Flushes the WAL only if `page_lsn` is not yet durable.
    ///
    /// The per-page form of the write-ahead rule used on the eviction path: a
    /// dirty page about to be written back is only safe once the log up to its
    /// `page_lsn` is on disk. Skips the flush when the durable LSN already
    /// covers the page, avoiding an unnecessary `fsync` on most evictions.
    inline fn walBeforePageLsn(self: *PagePool, page_lsn: u64) !void {
        if (self.wal_gate) |g| {
            if (page_lsn > g.durable_lsn(g.ctx)) try g.flush(g.ctx);
        }
    }

    /// Allocates and initialises a pool over `file_path` with `pool_size`
    /// frames.
    ///
    /// Creates the [`Pager`], the shared `frames`/`slab`/`pages` arrays and the
    /// shards, wiring every page view onto its slab window and seeding each
    /// shard's `free_list` with its whole frame run. The number of shards is 16
    /// when `pool_size >= 64` and 4 otherwise, and `instance_size` is
    /// `pool_size / num_instances` (so a `pool_size` not divisible by the shard
    /// count leaves a few tail frames unowned and unused).
    ///
    /// Returns `error.ZeroSizedPool` for `pool_size == 0` and
    /// `error.PoolTooSmall` for anything below [`MIN_POOL_SIZE`]. All partial
    /// allocations are unwound via `errdefer` on any failure. The returned
    /// pointer is heap-owned; free it with [`PagePool.deinit`] (flushes first)
    /// or [`PagePool.deinitNoFlush`].
    pub fn init(allocator: Allocator, io: Io, file_path: []const u8, pool_size: u32) !*PagePool {
        if (pool_size == 0) return error.ZeroSizedPool;
        if (pool_size < MIN_POOL_SIZE) return error.PoolTooSmall;

        const pager = try allocator.create(Pager);
        errdefer allocator.destroy(pager);
        pager.* = try Pager.init(io, file_path, allocator);
        errdefer pager.deinit();

        const pool = try allocator.create(PagePool);
        errdefer allocator.destroy(pool);

        const frames = try allocator.alloc(Frame, pool_size);
        errdefer allocator.free(frames);

        const slab = try allocator.alloc(u8, @as(usize, pool_size) * FRAME_STRIDE);
        errdefer allocator.free(slab);
        @memset(slab, 0);

        const pages = try allocator.alloc(SlottedPage, pool_size);
        errdefer allocator.free(pages);

        for (0..pool_size) |i| {
            pages[i] = .{ .data = slab[i * FRAME_STRIDE .. i * FRAME_STRIDE + PAGE_SIZE] };
            slab[i * FRAME_STRIDE] = 0;
            frames[i] = .{};
        }

        const num_instances = if (pool_size >= 64) @as(u32, 16) else @as(u32, 4);
        const instance_size = pool_size / num_instances;

        const instances = try allocator.alloc(PoolInstance, num_instances);
        errdefer allocator.free(instances);

        pool.* = .{
            .allocator = allocator,
            .pager = pager,
            .pool_size = pool_size,
            .num_instances = num_instances,
            .frames = frames,
            .pages = pages,
            .slab = slab,
            .instances = instances,
        };

        for (0..num_instances) |i| {
            instances[i] = .{
                .page_table = HashMap(PageId, FrameId, std.hash_map.AutoContext(PageId), 80).init(allocator),
                .free_list = .empty,
                .instance_index = @intCast(i),
                .instance_size = instance_size,
            };
            const start_fid = @as(FrameId, @intCast(i)) * instance_size;
            for (0..instance_size) |j| {
                try instances[i].free_list.append(allocator, start_fid + @as(FrameId, @intCast(j)));
            }
        }

        // Test/stress hook: `KAIDB_MMAP=1` turns on Phase 4 mmap reads for the
        // whole process, so the unit suite and the concurrency fuzzer exercise the
        // borrow/copy-out path (a missed copy-out then SIGBUSes on the read-only
        // map). The server enables it from config instead (see `enableMmapReads`).
        if (builtin.os.tag != .windows) {
            if (std.c.getenv("KAIDB_MMAP")) |v| {
                if (v[0] != 0 and v[0] != '0') pool.enableMmapReads();
            }
        }

        return pool;
    }

    /// Flushes all dirty pages, then tears the pool down and frees it.
    ///
    /// The normal shutdown path: [`PagePool.flushAllPagesFast`] persists dirty
    /// pages (and can fail, which is why this returns an error) before
    /// [`PagePool.freeAll`] releases memory. Use [`PagePool.deinitNoFlush`] when
    /// discarding an unrecoverable/aborted pool.
    pub fn deinit(self: *PagePool) !void {
        try self.flushAllPagesFast();
        self.freeAll();
    }

    /// Tears the pool down WITHOUT writing back dirty pages.
    ///
    /// For teardown after a fatal error or when the file is being discarded;
    /// any un-flushed modifications are lost. Cannot fail.
    pub fn deinitNoFlush(self: *PagePool) void {
        self.freeAll();
    }

    /// Releases every allocation the pool owns and destroys the pool itself.
    ///
    /// Deinitialises each shard, frees the four shared arrays, tears down and
    /// destroys the [`Pager`], then destroys the `PagePool`. After this the
    /// pointer is invalid. Writes nothing to disk; callers wanting persistence
    /// must flush first (see [`PagePool.deinit`]).
    fn freeAll(self: *PagePool) void {
        if (self.mmap_enabled) {
            std.log.info(
                "pool page-serve stats: fetches={d} evictions={d} borrow_serves={d} pread_serves={d} copyout_writes={d}",
                .{
                    self.fetch_count.load(.monotonic),
                    self.evict_count.load(.monotonic),
                    self.borrow_serves.load(.monotonic),
                    self.pread_serves.load(.monotonic),
                    self.copyout_writes.load(.monotonic),
                },
            );
        }
        if (self.miss_prof) {
            std.log.info(
                "pool MISS breakdown (ms): victim={d:.1} read(pread)={d:.1} checksum={d:.1} (evictions={d} preads={d})",
                .{ self.sw_victim.elapsedMs(), self.sw_read.elapsedMs(), self.sw_checksum.elapsedMs(), self.evict_count.load(.monotonic), self.pread_serves.load(.monotonic) },
            );
        }
        for (self.instances) |*inst| {
            inst.deinit(self.allocator);
        }
        self.allocator.free(self.instances);
        self.allocator.free(self.pages);
        self.allocator.free(self.slab);
        self.allocator.free(self.frames);
        self.pager.deinit();
        self.allocator.destroy(self.pager);
        self.allocator.destroy(self);
    }

    /// Returns the page view for a frame index.
    ///
    /// Thin accessor over the parallel `pages` array; kept as a named helper so
    /// the frame-to-bytes mapping lives in one place.
    fn pageForFrame(self: *PagePool, fid: FrameId) *SlottedPage {
        return &self.pages[fid];
    }

    /// Returns the page view for a frame, given the [`Frame`] pointer itself.
    ///
    /// Recovers the frame's index by pointer arithmetic against the base of the
    /// `frames` array (valid because frames are one contiguous allocation) and
    /// indexes `pages`. For callers that hold a `*Frame` from a fetch but not
    /// its id.
    pub fn pageOf(self: *PagePool, f: *Frame) *SlottedPage {
        const fid: FrameId = @intCast((@intFromPtr(f) - @intFromPtr(self.frames.ptr)) / @sizeOf(Frame));
        return &self.pages[fid];
    }

    /// The frame's OWN writable slab window (its canonical `PAGE_SIZE` slot in the
    /// backing slab). A materialised (non-borrowed) frame's `pages[fid].data`
    /// points here; a borrowed frame's points into the mmap instead until it is
    /// written (see [`PagePool.beginWrite`]).
    fn slabWindow(self: *PagePool, fid: FrameId) []u8 {
        const base = @as(usize, fid) * FRAME_STRIDE;
        return self.slab[base .. base + PAGE_SIZE];
    }

    /// Frame index of a `*Frame` pointer.
    fn frameId(self: *PagePool, f: *Frame) FrameId {
        return @intCast((@intFromPtr(f) - @intFromPtr(self.frames.ptr)) / @sizeOf(Frame));
    }

    /// Turn on Phase 4 mmap reads: map the file read-only and route clean read
    /// misses to the map. Best-effort; if the mapping cannot be established the
    /// flag still flips but [`Pager.mapView`] returns null and every read falls
    /// back to `pread`, so this never affects correctness. Call once after open.
    pub fn enableMmapReads(self: *PagePool) void {
        self.pager.enableMmap();
        self.mmap_enabled = true;
    }

    /// Grow the mmap to cover the file's current size, so pages written since the
    /// last map (e.g. during a bulk load) can be served borrowed rather than via
    /// `pread`. Safe to call from a background checkpoint: growth retires the old
    /// mapping without unmapping it (see [`Pager.remapForPages`]).
    pub fn refreshMap(self: *PagePool) void {
        if (self.mmap_enabled) self.pager.remapForPages(self.pager.num_pages);
    }

    /// Make a fetched frame WRITABLE before the caller modifies its page bytes.
    /// If the frame borrows the read-only mmap (a clean read), copy the page out
    /// into the frame's own slab window and repoint the view there, clearing the
    /// borrow. Idempotent and cheap for an already-materialised frame. MUST be
    /// called by the B+Tree under the frame's EXCLUSIVE latch before any write;
    /// because the map is `PROT_READ`, a missed call faults (SIGBUS) loudly in
    /// testing rather than silently corrupting the file.
    pub fn beginWrite(self: *PagePool, f: *Frame) void {
        if (!f.borrowed) return;
        const fid = self.frameId(f);
        const win = self.slabWindow(fid);
        const p = &self.pages[fid];
        @memcpy(win, p.data[0..PAGE_SIZE]);
        p.data = win;
        f.borrowed = false;
        _ = self.copyout_writes.fetchAdd(1, .monotonic);
    }

    /// Fetches a page into the cache and returns its pinned [`Frame`].
    ///
    /// The core read path. It first tries a cache hit under the shard's SHARED
    /// lock (cheap, concurrent): on a hit it atomically bumps `pin_count` and
    /// sets `is_referenced`, and returns. On a miss it re-takes the shard lock
    /// EXCLUSIVELY and re-checks the table (another thread may have loaded the
    /// page in the gap), then selects a victim frame via
    /// [`PoolInstance.findVictimFrame`], writing the victim back first if dirty
    /// (honouring [`PagePool.walBeforePageLsn`] and refreshing its checksum) and
    /// evicting its old page from the table. It then reads the requested page
    /// from disk and, for any page other than page 0, verifies its checksum,
    /// logging a detailed diagnostic and propagating `error.InvalidChecksum` on
    /// mismatch. The frame is returned pinned (`pin_count = 1`, clean,
    /// referenced) and MUST be released with [`PagePool.unpinPage`].
    ///
    /// Returns `error.NoFreeFrames` if the shard has no unpinned frame to evict.
    /// The double-checked locking is what keeps the common hit path off the
    /// exclusive lock.
    pub fn fetchPage(self: *PagePool, page_id: PageId) !*Frame {
        _ = self.fetch_count.fetchAdd(1, .monotonic);
        const inst_idx = page_id % self.num_instances;
        const inst = &self.instances[inst_idx];

        inst.rw_lock.lockShared(self.pager.io);
        if (inst.page_table.get(page_id)) |fid| {
            const f = &self.frames[fid];
            // Phase 3 (cheaper hit): `.monotonic` is sufficient here. The pin is
            // taken under the shard SHARED lock and only ever observed by the
            // evictor under the shard EXCLUSIVE lock, so the rwlock's release (on
            // unlockShared) / acquire (on the evictor's lock) already provides the
            // happens-before ordering; the atomic only needs to be atomic, not
            // sequentially consistent. `is_referenced` is a pure CLOCK hint. On
            // ARM64 this drops the per-hit memory barriers `.seq_cst` would emit;
            // on x86 it is identical.
            _ = @atomicRmw(u32, &f.pin_count, .Add, 1, .monotonic);
            @atomicStore(bool, &f.is_referenced, true, .monotonic);
            inst.rw_lock.unlockShared(self.pager.io);
            _ = self.hit_count.fetchAdd(1, .monotonic);
            return f;
        }
        inst.rw_lock.unlockShared(self.pager.io);

        inst.rw_lock.lock(self.pager.io);
        defer inst.rw_lock.unlock(self.pager.io);

        if (inst.page_table.get(page_id)) |fid| {
            const f = &self.frames[fid];
            _ = @atomicRmw(u32, &f.pin_count, .Add, 1, .seq_cst);
            @atomicStore(bool, &f.is_referenced, true, .seq_cst);
            _ = self.hit_count.fetchAdd(1, .monotonic);
            return f;
        }

        if (!self.miss_prof_checked) {
            self.miss_prof_checked = true;
            if (std.c.getenv("KAIDB_QPROF")) |v| self.miss_prof = v[0] != 0 and v[0] != '0';
        }
        const mp = self.miss_prof;
        if (mp) self.sw_victim.start(self.pager.io);
        const fid = inst.findVictimFrame(self) orelse return error.NoFreeFrames;
        if (mp) self.sw_victim.stop(self.pager.io);
        const f = &self.frames[fid];
        const p = self.pageForFrame(fid);

        if (f.is_dirty) {
            try self.walBeforePageLsn(p.headerPtr().page_lsn);
            try self.writeChecksum(p);
            try self.pager.writePage(f.page_id.?, p.data);
        }
        if (f.page_id) |old| {
            _ = inst.page_table.remove(old);
            _ = self.evict_count.fetchAdd(1, .monotonic);
        }

        // Phase 4: serve a clean read from the read-only mmap when it covers this
        // page - bind the frame's view to the map with NO `pread` copy. Otherwise
        // (mmap off/disabled, page beyond the mapping, or the header page) restore
        // the frame's own slab window and `pread` into it. Either way `p.data` is
        // the page bytes and the checksum is validated over them below.
        var view: ?[]u8 = if (self.mmap_enabled and page_id != 0) self.pager.mapView(page_id) else null;
        // If the page is within the file but past the current mapping (the file
        // grew, e.g. during a bulk load), grow the mapping once and retry, so
        // freshly written pages are served borrowed rather than copied.
        if (view == null and self.mmap_enabled and page_id != 0 and page_id < self.pager.num_pages) {
            self.pager.remapForPages(self.pager.num_pages);
            view = self.pager.mapView(page_id);
        }
        if (view) |v| {
            p.data = v;
            f.borrowed = true;
            _ = self.borrow_serves.fetchAdd(1, .monotonic);
        } else {
            p.data = self.slabWindow(fid);
            f.borrowed = false;
            if (mp) self.sw_read.start(self.pager.io);
            try self.pager.readPage(page_id, p.data);
            if (mp) self.sw_read.stop(self.pager.io);
            _ = self.pread_serves.fetchAdd(1, .monotonic);
        }

        if (page_id != 0) {
            if (mp) self.sw_checksum.start(self.pager.io);
            defer if (mp) self.sw_checksum.stop(self.pager.io);
            self.validateChecksum(p) catch |err| {
                const hdr = p.headerPtr();
                const expected = std.hash.Wyhash.hash(0, p.data[@sizeOf(u64)..]);
                std.log.err(
                    "InvalidChecksum: file={s} page_id={d} num_pages={d} stored={x} expected={x} beyond_eof={}",
                    .{ self.pager.file_path, page_id, self.pager.num_pages, hdr.checksum, expected, page_id >= self.pager.num_pages },
                );
                return err;
            };
        }

        f.page_id = page_id;
        f.pin_count = 1;
        f.is_dirty = false;
        f.is_referenced = true;
        try inst.page_table.put(page_id, fid);
        return f;
    }

    /// Allocates a brand-new page of `page_type` and returns its pinned frame.
    ///
    /// Asks the [`Pager`] for a fresh [`PageId`], routes it to its shard, evicts
    /// a victim frame (writing it back if dirty, under the same WAL/checksum
    /// rules as [`PagePool.fetchPage`]), then `reset`s the page in memory to an
    /// empty page of the requested type rather than reading from disk. The frame
    /// is returned pinned and already marked dirty (`is_dirty = true`) since a
    /// new page must be written out, with its `page_lsn` stamped from
    /// `current_lsn`.
    ///
    /// Returns `error.NoFreeFrames` if no victim is available. Release with
    /// [`PagePool.unpinPage`].
    pub fn newPage(self: *PagePool, page_type: PageType) !*Frame {
        const new_id = try self.pager.allocPage();

        const inst_idx = new_id % self.num_instances;
        const inst = &self.instances[inst_idx];

        inst.rw_lock.lock(self.pager.io);
        defer inst.rw_lock.unlock(self.pager.io);

        const fid = inst.findVictimFrame(self) orelse return error.NoFreeFrames;
        const f = &self.frames[fid];
        const p = self.pageForFrame(fid);

        if (f.is_dirty) {
            try self.walBeforePageLsn(p.headerPtr().page_lsn);
            try self.writeChecksum(p);
            try self.pager.writePage(f.page_id.?, p.data);
        }
        if (f.page_id) |old| {
            _ = inst.page_table.remove(old);
            _ = self.evict_count.fetchAdd(1, .monotonic);
        }

        // A brand-new page is written immediately, so it must own a writable slab
        // window, never borrow the read-only map (which the victim may have been).
        p.data = self.slabWindow(fid);
        f.borrowed = false;
        p.reset(page_type);

        f.page_id = new_id;
        f.pin_count = 1;
        f.is_dirty = true;
        f.is_referenced = true;
        p.headerPtr().page_lsn = self.current_lsn.load(.monotonic);
        try inst.page_table.put(new_id, fid);
        return f;
    }

    /// Releases one pin on a page and optionally marks it dirty.
    ///
    /// The counterpart to [`PagePool.fetchPage`]/[`PagePool.newPage`]; every
    /// successful fetch must be balanced by exactly one unpin. Runs under the
    /// shard's SHARED lock (it mutates only atomic frame fields, not the table),
    /// decrementing `pin_count` via a compare-and-swap loop that refuses to go
    /// below zero (a defensive guard against an over-unpin). When `is_dirty` is
    /// true it sets the dirty flag and advances the page's `page_lsn` to
    /// `current_lsn` if that is newer, keeping the WAL-before-page invariant
    /// meaningful for the eventual write-back. A `page_id` no longer resident is
    /// silently ignored.
    pub fn unpinPage(self: *PagePool, page_id: PageId, is_dirty: bool) void {
        const inst_idx = page_id % self.num_instances;
        const inst = &self.instances[inst_idx];

        inst.rw_lock.lockShared(self.pager.io);
        defer inst.rw_lock.unlockShared(self.pager.io);

        if (inst.page_table.get(page_id)) |fid| {
            const f = &self.frames[fid];
            while (true) {
                // Phase 3: `.monotonic` suffices - this decrement runs under the
                // shard SHARED lock and the evictor observes the result under the
                // EXCLUSIVE lock, so the rwlock provides the ordering (see the
                // hit path in `fetchPage`). The CAS still guards against underflow.
                const current = @atomicLoad(u32, &f.pin_count, .monotonic);
                if (current == 0) break;
                if (@cmpxchgWeak(u32, &f.pin_count, current, current - 1, .monotonic, .monotonic) == null) {
                    break;
                }
            }
            if (is_dirty) {
                // Kept `.seq_cst`: the dirty flag gates write-back/durability and
                // is read by flush paths, so it is left strongly ordered.
                @atomicStore(bool, &f.is_dirty, true, .seq_cst);
                const hdr = self.pageForFrame(@intCast(fid)).headerPtr();
                const cl = self.current_lsn.load(.monotonic);
                if (cl > hdr.page_lsn) hdr.page_lsn = cl;
            }
        }
    }

    /// Evicts a page from the cache and frees it on disk.
    ///
    /// Used when a page is being deleted (for example a merged/emptied B+Tree
    /// node): under the shard's EXCLUSIVE lock it removes the page from the
    /// table, resets the frame to free state, returns the frame to the
    /// `free_list`, and asks the [`Pager`] to free the on-disk page. The dirty
    /// contents are intentionally discarded, not written back, because the page
    /// is going away.
    ///
    /// Returns `error.PageStillPinned` if any caller still holds a pin (a bug on
    /// the caller's part). A `page_id` not resident is a no-op.
    pub fn discardPage(self: *PagePool, page_id: PageId) !void {
        const inst_idx = page_id % self.num_instances;
        const inst = &self.instances[inst_idx];

        inst.rw_lock.lock(self.pager.io);
        defer inst.rw_lock.unlock(self.pager.io);

        const fid = inst.page_table.get(page_id) orelse return;
        const f = &self.frames[fid];
        if (f.pin_count > 0) return error.PageStillPinned;
        _ = inst.page_table.remove(page_id);
        f.page_id = null;
        f.is_dirty = false;
        f.is_referenced = false;
        try inst.free_list.append(self.allocator, fid);
        try self.pager.freePage(page_id);
    }

    /// Durably writes back every dirty page using the doublewrite buffer.
    ///
    /// The crash-safe checkpoint path. Holds `doublewrite_mutex` for the whole
    /// operation and flushes the WAL up front ([`PagePool.walBeforePage`]). For
    /// each shard it collects the dirty frames and processes them in batches of
    /// up to 16 (the [`DoublewriteHeader.page_ids`] capacity). Per batch the
    /// torn-write-safe sequence is: refresh each page's checksum and write the
    /// copies to the staging pages (3..), write the header describing the batch
    /// to page 2, `sync`; then write every page in place and clear its dirty
    /// flag, `sync`; then zero the header and `sync`. After the final sync the
    /// header's `num_pages = 0` tells recovery there is nothing to replay for
    /// that batch.
    ///
    /// Slower but fully torn-write-protected, unlike [`PagePool.flushAllPagesFast`].
    pub fn flushAllPages(self: *PagePool) !void {
        self.doublewrite_mutex.lockUncancelable(self.pager.io);
        defer self.doublewrite_mutex.unlock(self.pager.io);

        try self.walBeforePage();

        const instance_size = self.pool_size / self.num_instances;
        
        var inst_idx: u32 = 0;
        while (inst_idx < self.num_instances) : (inst_idx += 1) {
            const inst = &self.instances[inst_idx];
            inst.rw_lock.lock(self.pager.io);
            // Release unconditionally: a cancellation or I/O error mid-batch must
            // never leak this shard's lock, or a later flush/close deadlocks on it.
            defer inst.rw_lock.unlock(self.pager.io);

            var dirty_fids = std.ArrayList(usize).empty;
            defer dirty_fids.deinit(self.allocator);

            const start_fid = @as(usize, inst_idx) * instance_size;
            const end_fid = start_fid + instance_size;
            var fid = start_fid;
            while (fid < end_fid) : (fid += 1) {
                const f = &self.frames[fid];
                if (f.is_dirty) {
                    try dirty_fids.append(self.allocator, fid);
                }
            }

            if (dirty_fids.items.len == 0) {
                continue;
            }

            var batch_start: usize = 0;
            while (batch_start < dirty_fids.items.len) : (batch_start += 16) {
                const batch_end = @min(batch_start + 16, dirty_fids.items.len);
                const batch = dirty_fids.items[batch_start..batch_end];

                var hdr = DoublewriteHeader{
                    .magic = 0x44574252,
                    .num_pages = @intCast(batch.len),
                    .page_ids = undefined,
                };
                @memset(&hdr.page_ids, 0);

                for (batch, 0..) |bfid, i| {
                    const f = &self.frames[bfid];
                    hdr.page_ids[i] = f.page_id.?;

                    const p = self.pageForFrame(@intCast(bfid));
                    try self.writeChecksum(p);

                    try self.pager.writePage(@intCast(3 + i), p.data);
                }

                var hdr_buf: [PAGE_SIZE]u8 = undefined;
                @memset(&hdr_buf, 0);
                @memcpy(hdr_buf[0..@sizeOf(DoublewriteHeader)], std.mem.asBytes(&hdr));
                try self.pager.writePage(2, &hdr_buf);

                try self.pager.sync();

                for (batch, 0..) |bfid, i| {
                    _ = i;
                    const f = &self.frames[bfid];
                    const p = self.pageForFrame(@intCast(bfid));
                    try self.pager.writePage(f.page_id.?, p.data);
                    f.is_dirty = false;
                }

                try self.pager.sync();

                hdr.num_pages = 0;
                @memset(&hdr_buf, 0);
                @memcpy(hdr_buf[0..@sizeOf(DoublewriteHeader)], std.mem.asBytes(&hdr));
                try self.pager.writePage(2, &hdr_buf);
                try self.pager.sync();
            }
        }
    }

    /// Writes back every dirty page directly, without the doublewrite buffer.
    ///
    /// The fast checkpoint/shutdown path (used by [`PagePool.deinit`]): under
    /// `doublewrite_mutex` and after a WAL flush, it walks each shard, and for
    /// every dirty frame refreshes the checksum, writes the page in place, and
    /// clears the dirty flag, then issues a single `sync` at the end. It skips
    /// the staging-copy dance, so a crash mid-write can leave a torn page. Use
    /// [`PagePool.flushAllPages`] where torn-write protection is required.
    pub fn flushAllPagesFast(self: *PagePool) !void {
        self.doublewrite_mutex.lockUncancelable(self.pager.io);
        defer self.doublewrite_mutex.unlock(self.pager.io);

        try self.walBeforePage();

        const instance_size = self.pool_size / self.num_instances;
        
        var inst_idx: u32 = 0;
        while (inst_idx < self.num_instances) : (inst_idx += 1) {
            const inst = &self.instances[inst_idx];
            inst.rw_lock.lock(self.pager.io);
            // Release unconditionally: a cancellation or I/O error mid-loop must
            // never leak this shard's lock, or a later flush/close deadlocks on it.
            defer inst.rw_lock.unlock(self.pager.io);

            const start_fid = @as(usize, inst_idx) * instance_size;
            const end_fid = start_fid + instance_size;
            var fid = start_fid;
            while (fid < end_fid) : (fid += 1) {
                const f = &self.frames[fid];
                if (f.is_dirty) {
                    const p = self.pageForFrame(@intCast(fid));
                    try self.writeChecksum(p);
                    try self.pager.writePage(f.page_id.?, p.data);
                    f.is_dirty = false;
                }
            }
        }

        try self.pager.sync();
    }

    /// Durably writes back a single dirty page using the doublewrite buffer.
    ///
    /// The one-page analogue of [`PagePool.flushAllPages`]: under
    /// `doublewrite_mutex` (and after a WAL flush) it looks the page up in its
    /// shard, and if dirty, stages one copy to page 3, records a one-entry
    /// header on page 2, `sync`s, writes the page in place and clears its dirty
    /// flag, `sync`s, then clears the header and `sync`s. A `page_id` not
    /// resident, or resident but clean, is a no-op.
    pub fn flushPage(self: *PagePool, page_id: PageId) !void {
        self.doublewrite_mutex.lockUncancelable(self.pager.io);
        defer self.doublewrite_mutex.unlock(self.pager.io);

        try self.walBeforePage();

        const inst_idx = page_id % self.num_instances;
        const inst = &self.instances[inst_idx];

        inst.rw_lock.lock(self.pager.io);
        defer inst.rw_lock.unlock(self.pager.io);

        const fid = inst.page_table.get(page_id) orelse return;
        const f = &self.frames[fid];
        if (f.is_dirty) {
            const p = self.pageForFrame(fid);
            try self.writeChecksum(p);

            var hdr = DoublewriteHeader{
                .magic = 0x44574252,
                .num_pages = 1,
                .page_ids = undefined,
            };
            @memset(&hdr.page_ids, 0);
            hdr.page_ids[0] = page_id;

            try self.pager.writePage(3, p.data);

            var hdr_buf: [PAGE_SIZE]u8 = undefined;
            @memset(&hdr_buf, 0);
            @memcpy(hdr_buf[0..@sizeOf(DoublewriteHeader)], std.mem.asBytes(&hdr));
            try self.pager.writePage(2, &hdr_buf);

            try self.pager.sync();

            try self.pager.writePage(page_id, p.data);
            f.is_dirty = false;

            try self.pager.sync();

            hdr.num_pages = 0;
            @memset(&hdr_buf, 0);
            @memcpy(hdr_buf[0..@sizeOf(DoublewriteHeader)], std.mem.asBytes(&hdr));
            try self.pager.writePage(2, &hdr_buf);
            try self.pager.sync();
        }
    }

    /// Recomputes and stores a page's integrity checksum.
    ///
    /// Hashes the page bytes AFTER the first `@sizeOf(u64)` (the checksum field
    /// itself is excluded so it does not hash into its own value) with CRC-32
    /// and writes the result into the header. Called just before every write to
    /// disk, so the stored checksum always describes the bytes on their way out.
    /// Takes no `self` state (the receiver is ignored); it is a method only for
    /// call-site symmetry. Pairs with [`PagePool.validateChecksum`].
    pub fn writeChecksum(_: *PagePool, p: *SlottedPage) !void {
        p.headerPtr().checksum = @as(u64, crc32_fast.hash(p.data[@sizeOf(u64)..]));
    }

    /// Verifies a freshly-read page's checksum against its stored value.
    ///
    /// Recomputes the CRC-32 over the same range [`PagePool.writeChecksum`]
    /// hashed and compares. On mismatch it increments
    /// [`PagePool.checksum_failed`] and returns `error.InvalidChecksum`,
    /// signalling corruption or a torn/short read; the caller
    /// ([`PagePool.fetchPage`]) logs context before propagating. Private because
    /// only the read path should ever validate.
    fn validateChecksum(self: *PagePool, p: *SlottedPage) !void {
        const stored = p.headerPtr().checksum;
        const expected = @as(u64, crc32_fast.hash(p.data[@sizeOf(u64)..]));
        if (stored != expected) {
            self.checksum_failed += 1;
            return error.InvalidChecksum;
        }
    }
};

test "pool - slab: single allocation for all frames" {
    const pool_size: u32 = 8;
    const slab = try testing.allocator.alloc(u8, @as(usize, pool_size) * FRAME_STRIDE);
    defer testing.allocator.free(slab);
    const pages = try testing.allocator.alloc(SlottedPage, pool_size);
    defer testing.allocator.free(pages);

    for (0..pool_size) |i| {
        pages[i] = .{ .data = slab[i * FRAME_STRIDE .. i * FRAME_STRIDE + PAGE_SIZE] };
        pages[i].reset(.leaf);
    }

    for (0..pool_size) |i| {
        try testing.expectEqual(@as(usize, PAGE_SIZE), pages[i].data.len);
        if (i + 1 < pool_size)
            try testing.expect(pages[i].data.ptr != pages[i + 1].data.ptr);
    }

    pages[0].reset(.leaf);
    try pages[0].insertCell(0, .{ .key = "hello", .value = "world" });
    pages[1].reset(.internal);
    try testing.expectEqual(PageType.leaf, pages[0].headerPtr().page_type);
    try testing.expectEqual(PageType.internal, pages[1].headerPtr().page_type);
}

test "page-mgmt Phase 3: relaxed-atomic pin/unpin balances exactly and guards underflow" {
    const allocator = testing.allocator;
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const path = "kaidb_pool_pin_test.tmp";
    std.Io.Dir.deleteFile(.cwd(), io, path) catch {};
    defer std.Io.Dir.deleteFile(.cwd(), io, path) catch {};

    var pool = try PagePool.init(allocator, io, path, 64);
    defer pool.deinit() catch {};

    // Allocate a page: newPage returns it pinned once.
    const f = try pool.newPage(.leaf);
    const pid = f.page_id.?;
    try testing.expectEqual(@as(u32, 1), @atomicLoad(u32, &f.pin_count, .seq_cst));

    // Many hit-path pins (the `.monotonic` @atomicRmw add) then the same number of
    // unpins (the `.monotonic` CAS decrement): the count must return exactly to
    // the newPage pin. This is the balance the relaxed ordering must preserve.
    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const g = try pool.fetchPage(pid);
        try testing.expectEqual(pid, g.page_id.?);
    }
    try testing.expectEqual(@as(u32, 101), @atomicLoad(u32, &f.pin_count, .seq_cst));
    i = 0;
    while (i < 100) : (i += 1) pool.unpinPage(pid, false);
    try testing.expectEqual(@as(u32, 1), @atomicLoad(u32, &f.pin_count, .seq_cst));

    // Drop the last pin: the page becomes unpinned (evictable).
    pool.unpinPage(pid, false);
    try testing.expectEqual(@as(u32, 0), @atomicLoad(u32, &f.pin_count, .seq_cst));

    // Underflow guard: an extra unpin on an already-unpinned page must NOT wrap
    // the count below zero (the CAS loop breaks at 0).
    pool.unpinPage(pid, false);
    try testing.expectEqual(@as(u32, 0), @atomicLoad(u32, &f.pin_count, .seq_cst));
}

test "pool - Frame defaults" {
    const f = Frame{};
    try testing.expect(f.page_id == null);
    try testing.expectEqual(@as(u32, 0), f.pin_count);
    try testing.expect(!f.is_dirty);
    try testing.expect(!f.is_referenced);
}
