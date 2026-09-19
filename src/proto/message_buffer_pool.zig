//! Thread-safe free-list of reusable byte buffers for the binary wire protocol.
//!
//! Every request/response cycle on a kaidb session needs a scratch `[]u8` to
//! decode an incoming frame into or to encode an outgoing frame from. Allocating
//! and freeing that buffer on the general-purpose allocator on every message is
//! both a per-message syscall-class cost and a source of heap fragmentation on a
//! busy server. This pool amortises that: released buffers are parked on a stack
//! and handed back out to the next [`MessageBufferPool.acquire`], so a session
//! that keeps talking recycles the same handful of allocations.
//!
//! Design decisions and invariants:
//!
//!   - **Bounded.** At most `max_size` buffers are ever retained. A
//!     [`MessageBufferPool.release`] beyond that cap frees the buffer instead of
//!     growing the pool without limit, so an idle-then-bursty workload does not
//!     leave a large permanent footprint.
//!
//!   - **Size-fit reuse, not one-size-fits-all.** Frames vary in length, so a
//!     parked buffer is only reused when it is big enough for the request AND not
//!     wastefully oversized (see the fit test in [`MessageBufferPool.acquire`]):
//!     it must be `>= needed_size` and `<= needed_size*2 + 64`. A buffer that
//!     fails either test is dropped and a right-sized one is allocated, which
//!     stops a single huge message from pinning an oversized buffer in the pool
//!     forever.
//!
//!   - **LIFO.** The pool is a stack (pop the most-recently-released buffer), so
//!     the hottest, most-cache-resident buffer is the one reused first.
//!
//!   - **Coarse-grained locking.** A single [`std.Io.Mutex`] serialises all
//!     access. The critical sections are O(1) pointer shuffles, so contention is
//!     negligible relative to the I/O the buffers are used for. The lock is taken
//!     uncancelably: buffer bookkeeping must complete even if the surrounding
//!     async operation is being cancelled, or the free-list would be corrupted.
//!
//! It fits under the session layer (`session.zig` / `session_pool.zig`): each
//! session borrows a buffer for the duration of a message and returns it, and the
//! pool itself is shared across sessions on one server.

const std = @import("std");
/// The std async I/O namespace, source of the [`std.Io.Mutex`] used for locking.
const Io = std.Io;
/// The allocator interface backing every buffer this pool owns or hands out.
const Allocator = std.mem.Allocator;

/// A bounded, thread-safe free-list of reusable message buffers.
///
/// Buffers are borrowed via [`MessageBufferPool.acquire`] and returned via
/// [`MessageBufferPool.release`]. The pool never owns a buffer that is currently
/// checked out; ownership transfers to the caller on acquire and back to the pool
/// on release. See the module header for the reuse and cap policy.
pub const MessageBufferPool = struct {
    /// Allocator used to grow the free-list and to alloc/free the buffers
    /// themselves. Every buffer the pool holds or returns is owned by this
    /// allocator, so the caller must free an acquired buffer with the same one
    /// (or, preferably, return it via [`MessageBufferPool.release`]).
    allocator: Allocator,
    /// The parked buffers, used as a LIFO stack (push on release, pop on acquire).
    /// Its length never exceeds `max_size`.
    pool: std.ArrayList([]u8),
    /// Guards every read and write of `pool`. Taken uncancelably so the free-list
    /// is never left half-updated by a cancelled async op. Defaults to `.init`
    /// so the struct is usable straight out of [`MessageBufferPool.init`].
    mutex: std.Io.Mutex = .init,
    /// The async I/O context the [`std.Io.Mutex`] is driven by; passed to every
    /// lock/unlock call.
    io: Io,
    /// The default buffer length requested at construction time. Retained for
    /// callers/inspection; per-message sizing is driven by the `needed_size`
    /// argument to [`MessageBufferPool.acquire`], not by this field.
    buffer_size: usize,
    /// The maximum number of buffers the pool will retain. Releases past this cap
    /// free the buffer instead of parking it.
    max_size: usize,

    /// Creates an empty pool sized to hold up to `pool_size` buffers.
    ///
    /// Pre-reserves capacity for `pool_size` slots so that later
    /// [`MessageBufferPool.release`] calls never have to grow the backing array
    /// under the lock. No message buffers are allocated yet; the pool fills lazily
    /// as buffers are released into it. `buffer_size` records the default frame
    /// size and `pool_size` becomes both the reserved capacity and `max_size`.
    ///
    /// Returns an allocator error if reserving the slot array fails; on that path
    /// the partially built array is cleaned up via `errdefer`.
    pub fn init(allocator: Allocator, io: Io, buffer_size: usize, pool_size: usize) !MessageBufferPool {
        var pool: std.ArrayList([]u8) = .empty;
        errdefer pool.deinit(allocator);
        try pool.ensureTotalCapacity(allocator, pool_size);
        return .{
            .allocator = allocator,
            .pool = pool,
            .io = io,
            .buffer_size = buffer_size,
            .max_size = pool_size,
        };
    }

    /// Frees every parked buffer and the backing array, emptying the pool.
    ///
    /// Takes the lock so it is safe to call while other threads might still touch
    /// the pool, though callers should ensure no buffer is checked out: a buffer
    /// currently held by a session is NOT tracked here and will not be freed by
    /// this call (it is the borrower's responsibility). Only buffers resident in
    /// the free-list at deinit time are released.
    pub fn deinit(self: *MessageBufferPool) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.pool.items) |buffer| self.allocator.free(buffer);
        self.pool.deinit(self.allocator);
    }

    /// Borrows a buffer of at least `needed_size` bytes, reusing a parked one when
    /// it is a good size fit and otherwise allocating fresh.
    ///
    /// Pops the most-recently-released buffer and reuses it only if it satisfies
    /// both bounds: `len >= needed_size` (big enough) and
    /// `len <= needed_size*2 + 64` (not wastefully oversized). A popped buffer
    /// that fails either test is freed and a right-sized buffer is allocated in
    /// its place, which prevents an occasional jumbo frame from leaving an
    /// oversized buffer that starves later small requests of a clean fit. When the
    /// pool is empty a new buffer is allocated directly.
    ///
    /// Ownership of the returned slice transfers to the caller; it must be given
    /// back via [`MessageBufferPool.release`] (or freed on the same allocator).
    /// Returns an allocator error if a fresh allocation is required and fails.
    pub fn acquire(self: *MessageBufferPool, needed_size: usize) ![]u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.pool.items.len > 0) {
            const buffer = self.pool.pop().?;
            if (buffer.len >= needed_size and buffer.len <= needed_size * 2 + 64) {
                return buffer;
            }
            self.allocator.free(buffer);
        }
        return try self.allocator.alloc(u8, needed_size);
    }

    /// Returns a buffer to the pool, or frees it if the pool is already full.
    ///
    /// A zero-length buffer is ignored (nothing to reclaim, and it would never fit
    /// any future request). If the free-list still has room (`len < max_size`) the
    /// buffer is pushed for reuse; the append is capacity-reserved by
    /// [`MessageBufferPool.init`] so it should not allocate, but if it somehow
    /// does and fails the buffer is freed rather than leaked. Past the cap the
    /// buffer is freed immediately, keeping the retained set bounded.
    ///
    /// After this call the caller must not touch `buffer` again: ownership has
    /// transferred back to the pool (or the allocator has reclaimed it).
    pub fn release(self: *MessageBufferPool, buffer: []u8) void {
        if (buffer.len == 0) return;
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.pool.items.len < self.max_size) {
            self.pool.append(self.allocator, buffer) catch self.allocator.free(buffer);
        } else {
            self.allocator.free(buffer);
        }
    }
};
