//! On-disk page allocator: the lowest layer of NovaDB's storage stack.
//!
//! The database file is nothing but a flat array of fixed-size pages
//! ([`PAGE_SIZE`] bytes each), addressed by a [`PageId`] which is simply the
//! zero-based index of the page within the file (byte offset `id * PAGE_SIZE`).
//! Everything above this file, the slotted-page B+Tree, the WAL, the catalog,
//! speaks in [`PageId`]s and never touches the file directly; the [`Pager`]
//! owns the single file handle and is the only place raw positional reads and
//! writes happen. That keeps the "which byte range is which page" invariant in
//! one place and lets higher layers reason purely in page numbers.
//!
//! ## What the pager provides
//!
//! Two responsibilities, deliberately kept small:
//!
//!   1. **Page allocation.** [`Pager.allocPage`] hands out a page number to
//!      write into, and [`Pager.freePage`] returns one to the pool. New numbers
//!      are minted by growing the logical file (`num_pages`), but freed numbers
//!      are recycled first so the file does not grow without bound as rows churn.
//!
//!   2. **Raw page I/O.** [`Pager.readPage`] / [`Pager.writePage`] move exactly
//!      one page between a caller buffer and the file at the correct offset, and
//!      [`Pager.sync`] forces the OS write-back cache to durable storage. There
//!      is no page cache here: buffering and MVCC live in the layers above (see
//!      `pool.zig`). This type is intentionally a thin, correct file mapper.
//!
//! ## The free list and how it survives a restart
//!
//! While the process runs, freed page numbers live in an in-memory LIFO stack
//! ([`Pager.free_pages`]); allocation pops from it before extending the file, so
//! a just-freed page is the next one reused. That stack is pure RAM and would be
//! lost on shutdown, stranding those pages forever, so it is persisted as an
//! on-disk singly linked chain: each freed page is itself used to store the link
//! to the previous one. [`Pager.persistFreeList`] writes that chain and returns
//! the head [`PageId`] (which the caller records in the database header);
//! [`Pager.loadFreeList`] walks it back into memory at startup. Every chain page
//! carries a [`std.hash.Wyhash`] checksum in its first 8 bytes, and the load walk
//! stops on a bad checksum, an out-of-range link, or a cycle, so a torn or
//! corrupt free list degrades to "leak some pages" rather than crashing or
//! handing out a live page number.
//!
//! ## Reserved pages and concurrency
//!
//! Page numbers `0` and `1` are reserved for the database header / metadata and
//! are never recycled: [`Pager.freePage`] silently ignores any id `<= 1`.
//! [`Pager.free_pages`] and `num_pages` are shared mutable state, so allocation
//! and freeing take [`Pager.mutex`]; the raw page reads and writes do NOT lock,
//! because they address disjoint page offsets and the caller (the buffer pool /
//! B+Tree) already serialises access to any single page.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const File = Io.File;
const Dir = Io.Dir;
const Allocator = std.mem.Allocator;
const testing = std.testing;

const page = @import("page.zig");
const sync_mod = @import("utils").sync;
const PageId = page.PageId;
const PAGE_SIZE = page.PAGE_SIZE;

/// The on-disk page allocator and raw page-I/O gateway for one database file.
///
/// A `Pager` owns exactly one open file and treats it as a contiguous array of
/// [`PAGE_SIZE`]-byte pages indexed by [`PageId`]. It is the single choke point
/// for turning page numbers into byte offsets; no other layer opens or seeks the
/// file. See the module header for the allocation model and the persisted free
/// list. Construct with [`Pager.init`] and release with [`Pager.deinit`].
pub const Pager = struct {
    /// The open database file, in read-write mode. All page I/O is positional
    /// (`writePositionalAll` / `readPositionalAll`) against this handle, so the
    /// file offset is never relied upon and concurrent page ops do not race on a
    /// shared seek position.
    file: File,
    /// The async I/O context threaded through every file operation. It is stored
    /// so callers of [`Pager.readPage`] / [`Pager.writePage`] need not pass it in;
    /// the same `io` used at [`Pager.init`] is reused for the pager's lifetime.
    io: Io,
    /// The logical page count: the number of pages the file currently spans, and
    /// therefore the next page number to mint when the free list is empty (see
    /// [`Pager.allocPage`]). Equals `file_size / PAGE_SIZE` at [`Pager.init`] and
    /// only grows.
    num_pages: PageId,
    /// The database file path, kept purely for diagnostics / error messages.
    /// Defaults to `"<unknown>"` for a `Pager` built directly (e.g. in tests)
    /// rather than through [`Pager.init`].
    file_path: []const u8 = "<unknown>",
    /// In-memory LIFO stack of page numbers freed this session and available for
    /// reuse before the file is extended. Backed by `allocator`; persisted across
    /// restarts as an on-disk chain via [`Pager.persistFreeList`] /
    /// [`Pager.loadFreeList`]. Guarded by [`Pager.mutex`].
    free_pages: std.ArrayList(PageId),
    /// Allocator backing [`Pager.free_pages`]. The same allocator must be used to
    /// deinit it (done by [`Pager.deinit`]).
    allocator: Allocator,
    /// Serialises the allocation state ([`Pager.free_pages`] and `num_pages`) so
    /// [`Pager.allocPage`] and [`Pager.freePage`] are safe under concurrent
    /// callers. Raw page reads/writes are deliberately NOT covered by it, as they
    /// touch disjoint offsets already serialised by the caller.
    mutex: Io.Mutex = .init,

    /// Optional READ-ONLY memory map of the whole file (Phase 4, page-mgmt plan).
    /// When present, [`Pager.mapView`] can hand the buffer pool a borrowed
    /// pointer into a page WITHOUT a `pread` copy, so a clean read is served from
    /// the OS page cache directly. It is `PROT_READ` deliberately: the pool must
    /// copy a page out before writing it (see `pool.beginWrite`), and a missed
    /// copy-out then SIGBUSes loudly in tests instead of silently corrupting the
    /// file. `MAP_SHARED` keeps it coherent with the pager's `pwrite`s. Null when
    /// mmap reads are disabled or unavailable (non-POSIX host, or a map failure);
    /// every read then falls back to `pread`. `map_len` is the mapped byte length
    /// (a whole number of pages), re-established by [`Pager.remapForPages`] when
    /// the file grows.
    map: ?[]align(std.heap.page_size_min) u8 = null,
    map_len: usize = 0,
    /// Guards `map`/`map_len` against a reader racing a growth remap.
    map_lock: sync_mod.RwLock = .{},
    /// Old, smaller mappings superseded by a growth remap. A grown mapping is a
    /// larger view of the SAME file from offset 0, so a reader still holding a
    /// pointer into an old mapping sees valid, coherent bytes; we therefore keep
    /// retired mappings alive (rather than `munmap`ing them mid-run and dangling a
    /// borrowed pointer) and free them all only at [`Pager.deinit`]. They cost
    /// address space, not physical memory, and there are at most a handful.
    retired_maps: std.ArrayList([]align(std.heap.page_size_min) u8) = .empty,

    /// Opens (or creates) the database file and builds a [`Pager`] over it.
    ///
    /// If `file_path` does not exist it is created empty (a fresh database);
    /// otherwise it is opened read-write. The existing file size must be a whole
    /// multiple of [`PAGE_SIZE`], else `error.InvalidDbFile` is returned, since a
    /// partial trailing page means the file is not a valid page array. `num_pages`
    /// is initialised from that size. The free list starts empty; call
    /// [`Pager.loadFreeList`] afterwards to repopulate it from a persisted chain.
    /// On any error after the file is opened the handle is closed via `errdefer`.
    pub fn init(io: Io, file_path: []const u8, allocator: Allocator) !Pager {
        const file = Dir.openFile(.cwd(), io, file_path, .{ .mode = .read_write }) catch |err| switch (err) {
            error.FileNotFound => try Dir.createFile(.cwd(), io, file_path, .{ .read = true, .truncate = false }),
            else => return err,
        };
        errdefer file.close(io);

        const stat = try file.stat(io);
        const file_size = stat.size;
        if (file_size % PAGE_SIZE != 0) return error.InvalidDbFile;

        return .{
            .file = file,
            .io = io,
            .num_pages = file_size / PAGE_SIZE,
            .file_path = file_path,
            .free_pages = std.ArrayList(PageId).empty,
            .allocator = allocator,
        };
    }

    /// Releases the free-list storage and closes the underlying file.
    ///
    /// Does NOT flush: any pending page writes should be committed with
    /// [`Pager.sync`] before deinit if durability is required. After this call the
    /// `Pager` must not be used again.
    pub fn deinit(self: *Pager) void {
        self.disableMmap();
        self.free_pages.deinit(self.allocator);
        self.file.close(self.io);
    }

    /// Allocates a page number to write into, reusing a freed one if available.
    ///
    /// Pops the most recently freed page from [`Pager.free_pages`] (LIFO, so a
    /// just-freed page is reused first, which keeps the working set hot); if the
    /// free list is empty it mints a fresh number by incrementing `num_pages`,
    /// logically extending the file. The returned page is NOT zeroed or written,
    /// the caller is expected to write its contents via [`Pager.writePage`]. Takes
    /// [`Pager.mutex`].
    pub fn allocPage(self: *Pager) !PageId {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.free_pages.items.len > 0) return self.free_pages.pop().?;
        const id = self.num_pages;
        self.num_pages += 1;
        return id;
    }

    /// Returns a page number to the free pool for later reuse.
    ///
    /// Page numbers `0` and `1` are reserved (the database header / metadata) and
    /// are silently ignored, so calling this with such an id is a safe no-op
    /// rather than an error. Any other id is pushed onto [`Pager.free_pages`] and
    /// becomes the next candidate from [`Pager.allocPage`]. Takes [`Pager.mutex`].
    /// The file is not truncated; freeing only makes the number reusable.
    pub fn freePage(self: *Pager, id: PageId) !void {
        if (id <= 1) return;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        try self.free_pages.append(self.allocator, id);
    }

    /// Writes one page's worth of bytes to the file at `page_id`'s offset.
    ///
    /// The write is positional (offset `page_id * PAGE_SIZE`) and complete
    /// (`writePositionalAll` loops until every byte is written), so it never
    /// disturbs a shared file position and never short-writes. `data` is expected
    /// to be exactly [`PAGE_SIZE`] bytes. This does not flush; durability requires
    /// a later [`Pager.sync`]. No lock is taken, distinct page offsets do not race.
    pub fn writePage(self: *Pager, page_id: PageId, data: []const u8) !void {
        try self.file.writePositionalAll(self.io, data, page_id * PAGE_SIZE);
    }

    /// Reads the page at `page_id` into `buf`.
    ///
    /// Positional read at offset `page_id * PAGE_SIZE`; `buf` should be
    /// [`PAGE_SIZE`] bytes. The byte count returned by `readPositionalAll` is
    /// discarded, this is used only where a full page is known to exist (the
    /// caller allocated it), so a short read there indicates file truncation the
    /// higher layer would treat as corruption. No lock is taken.
    pub fn readPage(self: *Pager, page_id: PageId, buf: []u8) !void {
        _ = try self.file.readPositionalAll(self.io, buf, page_id * PAGE_SIZE);
    }

    /// macOS `F_RDADVISE` command number and its argument struct. `F_RDADVISE`
    /// tells the kernel to start reading a byte range into the unified buffer
    /// cache asynchronously (the closest BSD equivalent of Linux `readahead`).
    const F_RDADVISE: c_int = 44;
    const radvisory = extern struct { ra_offset: i64, ra_count: c_int };

    /// Issue OS-level readahead for a set of pages so their reads land in the
    /// file cache before the caller synchronously fetches them. Purely a hint:
    /// on any error, an unsupported platform, or a stale/wrong page id it does
    /// nothing observable, and the real read still goes through the normal path.
    ///
    /// `page_ids` is sorted in place and its contiguous runs are coalesced into
    /// one readahead call each, so a batch of index lookups whose base rows
    /// cluster onto neighbouring pages issues a few large sequential hints rather
    /// than many tiny ones. This is what lets a disk-bound random-fetch scan
    /// overlap I/O latency (the device services the range reads in parallel)
    /// instead of paying a serial seek per row.
    pub fn prefetchPages(self: *Pager, page_ids: []PageId) void {
        if (builtin.os.tag == .windows) return;
        if (page_ids.len == 0) return;
        std.sort.pdq(PageId, page_ids, {}, comptime std.sort.asc(PageId));
        const fd = self.file.handle;

        var run_first = page_ids[0];
        var run_last = page_ids[0];
        for (page_ids[1..]) |id| {
            if (id == run_last) continue; // duplicate leaf (many rows share a page)
            if (id == run_last + 1) {
                run_last = id;
                continue;
            }
            issueReadahead(fd, run_first, run_last);
            run_first = id;
            run_last = id;
        }
        issueReadahead(fd, run_first, run_last);
    }

    fn issueReadahead(fd: std.posix.fd_t, first: PageId, last: PageId) void {
        const off: i64 = @intCast(first * PAGE_SIZE);
        const len: usize = @intCast((last - first + 1) * PAGE_SIZE);
        switch (builtin.os.tag) {
            .linux => {
                // `readahead(2)` is not exposed as a named wrapper in this Zig std,
                // so issue the raw syscall: readahead(int fd, off64_t off, size_t len).
                _ = std.os.linux.syscall3(
                    .readahead,
                    @as(usize, @intCast(fd)),
                    @as(usize, @bitCast(off)),
                    len,
                );
            },
            .macos, .ios, .tvos, .watchos, .visionos => {
                var ra = radvisory{ .ra_offset = off, .ra_count = @intCast(len) };
                _ = std.c.fcntl(fd, F_RDADVISE, @intFromPtr(&ra));
            },
            else => {},
        }
    }

    // --- Phase 4: read-only mmap of the file (page-mgmt plan) -----------------

    /// Establish a READ-ONLY `MAP_SHARED` mapping of the whole file so
    /// [`Pager.mapView`] can serve clean page reads with no copy. Best-effort:
    /// on a non-POSIX host, an empty file, or any map failure it leaves `map`
    /// null and every read falls back to `pread` (the map is a pure optimisation,
    /// never a correctness dependency). Safe to call once after the file size is
    /// known.
    pub fn enableMmap(self: *Pager) void {
        if (builtin.os.tag == .windows) return; // POSIX-only for now; pread fallback
        const len: usize = @intCast(self.num_pages * PAGE_SIZE);
        if (len == 0) return; // nothing mapped yet; a later remap picks it up
        const m = std.posix.mmap(
            null,
            len,
            .{ .READ = true },
            .{ .TYPE = .SHARED },
            self.file.handle,
            0,
        ) catch |e| {
            std.log.warn("enableMmap: mmap FAILED ({any}) num_pages={d} len={d}", .{ e, self.num_pages, len });
            return;
        };
        self.map = m;
        self.map_len = len;
        std.log.info("enableMmap: mapped num_pages={d} map_len={d} bytes", .{ self.num_pages, len });
    }

    /// A borrowed, read-only view of page `page_id` from the mmap, or null when
    /// the page is not covered by the current mapping (mmap disabled, or the page
    /// lies past `map_len` because the file grew since the last remap). The
    /// returned slice aliases the mapping and is valid only until a growth remap;
    /// callers must hold whatever lock excludes a concurrent writer to the page
    /// (the buffer pool holds the frame while it uses this).
    pub fn mapView(self: *Pager, page_id: PageId) ?[]u8 {
        self.map_lock.lockShared(self.io);
        defer self.map_lock.unlockShared(self.io);
        const m = self.map orelse return null;
        const off: usize = @intCast(page_id * PAGE_SIZE);
        if (off + PAGE_SIZE > self.map_len) return null;
        return m[off .. off + PAGE_SIZE];
    }

    /// Re-establish the mapping to cover at least `n_pages` pages after the file
    /// has grown. Remaps under an EXCLUSIVE `map_lock` so no reader observes a
    /// half-torn mapping; any borrowed view from before the remap must have been
    /// released (the pool releases a mmap-backed frame's borrow before growth can
    /// reach it, because a page being written is resident in a slab-backed frame,
    /// not map-backed). Best-effort: a remap failure drops the map (subsequent
    /// reads fall back to `pread`) rather than failing the caller.
    pub fn remapForPages(self: *Pager, n_pages: PageId) void {
        if (builtin.os.tag == .windows) return;
        self.map_lock.lock(self.io);
        defer self.map_lock.unlock(self.io);
        const want: usize = @intCast(n_pages * PAGE_SIZE);
        if (self.map == null) return; // mmap not enabled; nothing to grow
        if (want <= self.map_len) return; // already covers it
        const m = std.posix.mmap(
            null,
            want,
            .{ .READ = true },
            .{ .TYPE = .SHARED },
            self.file.handle,
            0,
        ) catch return; // keep the old mapping; grown pages fall back to pread
        // Retire (do NOT unmap) the old mapping: a reader may still hold a pointer
        // into it, and it is a valid view of the same file, so it stays alive
        // until deinit. Then publish the new, larger mapping.
        self.retired_maps.append(self.allocator, self.map.?) catch {
            // Cannot track the old mapping to free it later; unmapping it now would
            // dangle any borrowed pointer, so leak it rather than risk a crash.
        };
        self.map = m;
        self.map_len = want;
    }

    /// Drop the current mapping and every retired one. Called by [`Pager.deinit`].
    fn disableMmap(self: *Pager) void {
        if (builtin.os.tag != .windows) {
            for (self.retired_maps.items) |m| std.posix.munmap(m);
            if (self.map) |m| std.posix.munmap(m);
        }
        self.retired_maps.deinit(self.allocator);
        self.map = null;
        self.map_len = 0;
    }

    /// Forces all buffered page writes to durable storage (`fsync`).
    ///
    /// This is the durability barrier: until it returns, prior [`Pager.writePage`]
    /// calls may live only in the OS page cache. Checkpointing and WAL flushing
    /// layers call it to establish an on-disk consistency point.
    pub fn sync(self: *Pager) !void {
        try self.file.sync(self.io);
    }

    /// Writes the in-memory free list to disk as a linked chain and returns its
    /// head [`PageId`].
    ///
    /// The trick is that each freed page stores, in its own bytes, the link to the
    /// previously written free page, so the free list needs no separate storage.
    /// Iterating [`Pager.free_pages`] in order, each page is laid out as:
    /// bytes `[0, 8)` hold a [`std.hash.Wyhash`] checksum of the rest of the page,
    /// bytes `[8, 8 + @sizeOf(PageId))` hold `head` (the previous page written, or
    /// `0` for the first), and the remainder is zeroed. `head` advances to the
    /// current page after each write, so the returned value is the LAST page
    /// written and thus the entry point for [`Pager.loadFreeList`]; the caller must
    /// persist it (typically in the database header). A returned `0` means the free
    /// list was empty. `scratch` must be at least [`PAGE_SIZE`] bytes (asserted)
    /// and is used as the write buffer. Note this reverses order on reload, which
    /// is harmless because the free list is an unordered set of reusable numbers.
    pub fn persistFreeList(self: *Pager, scratch: []u8) !PageId {
        std.debug.assert(scratch.len >= PAGE_SIZE);
        var head: PageId = 0;
        for (self.free_pages.items) |pid| {
            @memset(scratch[0..PAGE_SIZE], 0);
            std.mem.writeInt(PageId, scratch[@sizeOf(u64) .. @sizeOf(u64) + @sizeOf(PageId)], head, .little);
            const cs = std.hash.Wyhash.hash(0, scratch[@sizeOf(u64)..PAGE_SIZE]);
            std.mem.writeInt(u64, scratch[0..@sizeOf(u64)], cs, .little);
            try self.writePage(pid, scratch[0..PAGE_SIZE]);
            head = pid;
        }
        return head;
    }

    /// Rebuilds the in-memory free list by walking the on-disk chain from `head`.
    ///
    /// Reverses [`Pager.persistFreeList`]: starting at `head` (the value that call
    /// returned, recovered from the database header), it reads each page, verifies
    /// its [`std.hash.Wyhash`] checksum against the stored one, appends the page
    /// number to [`Pager.free_pages`], and follows the embedded link to the next.
    /// The list is cleared first (capacity retained). A `head` of `0` yields an
    /// empty list.
    ///
    /// The walk is defensively bounded so a corrupt or torn chain cannot loop
    /// forever or resurrect an out-of-bounds page: it stops on a link `>= num_pages`
    /// (out of range), after more iterations than `num_pages` (cycle guard), or on
    /// a checksum mismatch (torn / partially written page). Any of these silently
    /// truncates the recovered list, the cost is leaking the un-recovered pages,
    /// never handing out a bad or in-use page number. `scratch` must be at least
    /// [`PAGE_SIZE`] bytes (asserted).
    pub fn loadFreeList(self: *Pager, head: PageId, scratch: []u8) !void {
        std.debug.assert(scratch.len >= PAGE_SIZE);
        self.free_pages.clearRetainingCapacity();
        var cur = head;
        var guard: u64 = 0;
        while (cur != 0) {
            if (cur >= self.num_pages) break;
            guard += 1;
            if (guard > self.num_pages) break;
            try self.readPage(cur, scratch[0..PAGE_SIZE]);
            const stored_cs = std.mem.readInt(u64, scratch[0..@sizeOf(u64)], .little);
            const calc_cs = std.hash.Wyhash.hash(0, scratch[@sizeOf(u64)..PAGE_SIZE]);
            if (stored_cs != calc_cs) break;
            const next = std.mem.readInt(PageId, scratch[@sizeOf(u64) .. @sizeOf(u64) + @sizeOf(PageId)], .little);
            try self.free_pages.append(self.allocator, cur);
            cur = next;
        }
    }
};

test "pager - free page list recycles ids" {
    var pager = Pager{
        .file = undefined,
        .io = undefined,
        .num_pages = 10,
        .free_pages = std.ArrayList(PageId).empty,
        .allocator = testing.allocator,
    };
    defer pager.free_pages.deinit(testing.allocator);

    try pager.freePage(5);
    try pager.freePage(7);

    const id1 = try pager.allocPage();
    const id2 = try pager.allocPage();
    const id3 = try pager.allocPage();

    try testing.expect(id1 == 7 or id1 == 5);
    try testing.expect(id2 == 7 or id2 == 5);
    try testing.expectEqual(@as(PageId, 10), id3);
    try testing.expectEqual(@as(PageId, 11), pager.num_pages);
}
