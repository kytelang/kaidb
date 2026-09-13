//! Reuse pool for binary-protocol [`Session`] objects.
//!
//! Each client connection accepted by the wire server needs a [`Session`] to
//! hold its per-connection state (parse buffers, prepared-statement scratch,
//! the message-buffer arena, and so on). Allocating and tearing one down on
//! every connect/disconnect is wasteful when a busy server churns short-lived
//! connections, so this pool keeps a bounded stack of already-constructed,
//! reset-to-clean sessions ready to hand out.
//!
//! Design decisions and invariants:
//!
//!   - **LIFO reuse.** [`SessionPool.pool`] is used as a stack (append on
//!     release, pop on acquire). LIFO keeps the most recently touched session,
//!     and its still-warm buffers, at the top, which is friendlier to the
//!     allocator and CPU cache than round-robin reuse.
//!
//!   - **Bounded, drop-on-overflow.** The pool never grows past
//!     [`SessionPool.max_size`]. When a session is released and the pool is
//!     already full it is destroyed instead of retained, so idle memory is
//!     capped no matter how large the peak connection count was. Conversely,
//!     when the pool is empty [`SessionPool.acquire`] constructs a fresh
//!     session on demand, so the pool never blocks a caller waiting for a free
//!     slot: `max_size` bounds what is *cached*, not what is *live*.
//!
//!   - **Thread safety.** Every operation takes [`SessionPool.mutex`], an
//!     `std.Io.Mutex`, because the accept loop and worker fibers can acquire
//!     and release concurrently. The lock is held only for the short list
//!     manipulation, never across session I/O.
//!
//!   - **Ownership.** The pool owns every session while it sits in the list:
//!     [`SessionPool.deinit`] deinitialises and frees all cached sessions.
//!     Once a session is handed out by `acquire` the caller owns it until it is
//!     handed back to [`SessionPool.release`]; failing to release it (or
//!     releasing into a full pool) simply destroys it, so there is no leak
//!     either way.
//!
//! The [`SessionPool.io`], [`SessionPool.db`], [`SessionPool.idle_timeout_ms`]
//! and [`SessionPool.msg_pool`] fields are the construction parameters every
//! new [`Session`] is stamped with, captured once so `acquire` can build a
//! session without the caller re-supplying them.

const std = @import("std");
/// Async I/O interface handed to each [`Session`] (event loop, timers, sockets).
const Io = std.Io;
/// General-purpose allocator used for both the session objects and the backing list.
const Allocator = std.mem.Allocator;
/// The SQL database/catalog every session runs its commands against.
const Database = @import("../schema.zig").Database;
/// The session module; re-exports the concrete [`Session`] and pool types below.
const session = @import("session.zig");
/// Per-connection protocol state object this pool caches and recycles.
const Session = session.Session;
/// Shared arena of reusable wire message buffers passed through to each session.
const MessageBufferPool = session.MessageBufferPool;

/// A bounded, thread-safe free-list of reusable [`Session`] objects.
///
/// Acts as a cache in front of the allocator: [`acquire`] prefers a recycled
/// session over a fresh allocation, and [`release`] returns a finished session
/// for reuse (or destroys it if the cache is already full). See the module
/// header for the LIFO / drop-on-overflow / ownership rules.
pub const SessionPool = struct {
    /// Allocator used to create and destroy [`Session`] objects and to back [`pool`].
    allocator: Allocator,
    /// Async I/O interface stamped into every session built by [`acquire`].
    io: Io,
    /// Database handle every pooled session executes SQL against.
    db: *Database,
    /// LIFO stack of idle, reset sessions available for reuse; the pool owns these.
    pool: std.ArrayList(*Session),
    /// Guards all access to [`pool`]; taken briefly by every acquire/release/deinit.
    mutex: std.Io.Mutex = .init,
    /// Maximum number of idle sessions to cache; releases beyond this destroy the session.
    max_size: usize,
    /// Idle timeout in milliseconds propagated to each new [`Session`].
    idle_timeout_ms: u64,
    /// Optional shared message-buffer pool passed to each session, or null if unused.
    msg_pool: ?*MessageBufferPool,

    /// Creates an empty pool sized for `max_size` cached sessions.
    ///
    /// Pre-reserves capacity for `max_size` entries up front so that later
    /// [`release`] calls append into an already-sized list and cannot fail on
    /// growth (the append error path in `release` is a fallback, not the norm).
    /// The `io`, `db`, `idle_timeout_ms` and `msg_pool` arguments are stored and
    /// later used to construct sessions in [`acquire`]. Returns an error only if
    /// the initial capacity reservation fails; the reserved list is freed via
    /// `errdefer` in that case.
    pub fn init(allocator: Allocator, io: Io, db: *Database, max_size: usize, idle_timeout_ms: u64, msg_pool: ?*MessageBufferPool) !SessionPool {
        var pool: std.ArrayList(*Session) = .empty;
        errdefer pool.deinit(allocator);
        try pool.ensureTotalCapacity(allocator, max_size);
        return .{
            .allocator = allocator,
            .io = io,
            .db = db,
            .pool = pool,
            .max_size = max_size,
            .idle_timeout_ms = idle_timeout_ms,
            .msg_pool = msg_pool,
        };
    }

    /// Destroys the pool and every session still cached in it.
    ///
    /// Takes [`mutex`] (uncancelable, since teardown must complete) and, for
    /// each pooled session, calls its `deinit` then frees the object, finally
    /// releasing the backing list. Only sessions *currently in the pool* are
    /// freed: any session that was handed out by [`acquire`] and not yet
    /// returned is the caller's responsibility. Call exactly once, after all
    /// live sessions have been released.
    pub fn deinit(self: *SessionPool) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);
        for (self.pool.items) |s| {
            s.deinit();
            self.allocator.destroy(s);
        }
        self.pool.deinit(self.allocator);
    }

    /// Obtains a ready-to-use session, recycling one if available.
    ///
    /// Under [`mutex`]: if the cache is non-empty it pops the top session and
    /// calls [`Session.reset`] to clear any state left over from its previous
    /// connection before returning it. If the cache is empty it allocates and
    /// [`Session.init`]-constructs a brand-new session from the stored `io`,
    /// `db`, `idle_timeout_ms` and `msg_pool`. Returns `error.OutOfMemory` only
    /// on the fresh-allocation path. The returned session is owned by the
    /// caller until passed to [`release`].
    pub fn acquire(self: *SessionPool) !*Session {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.pool.items.len > 0) {
            const s = self.pool.pop().?;
            s.reset();
            return s;
        }
        const s = try self.allocator.create(Session);
        s.* = Session.init(self.allocator, self.io, self.db, self.idle_timeout_ms, self.msg_pool);
        return s;
    }

    /// Returns a finished session to the pool, or destroys it if the pool is full.
    ///
    /// Under [`mutex`]: if fewer than [`max_size`] sessions are cached, `s` is
    /// appended for reuse (note it is NOT reset here; the reset happens lazily
    /// in [`acquire`] when it is next popped). If appending fails despite the
    /// upfront capacity reservation, or if the cache is already at `max_size`,
    /// `s` is deinitialised and freed instead. Either way ownership of `s`
    /// transfers back to the pool and the caller must not touch it afterwards.
    pub fn release(self: *SessionPool, s: *Session) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (self.pool.items.len < self.max_size) {
            self.pool.append(self.allocator, s) catch {
                s.deinit();
                self.allocator.destroy(s);
            };
        } else {
            s.deinit();
            self.allocator.destroy(s);
        }
    }
};
