//! Concurrency primitives layered over Zig's async `std.Io` synchronisation.
//!
//! Every lock here takes an `Io` argument on the blocking calls rather than
//! parking a raw OS thread: they cooperate with NovaDB's async scheduler, so a
//! coroutine that cannot acquire a lock yields its executor instead of
//! spinning a kernel thread. All acquisition uses the *uncancelable* variants
//! (`lockUncancelable`, `waitUncancelable`), because a half-taken storage lock
//! that got cancelled mid-await would corrupt the invariant the lock protects;
//! these locks are held across page mutations and must always run to release.
//!
//! Three primitives, in ascending order of specialisation:
//!
//!   * [`Mutex`] and [`RwLock`] are thin, zero-overhead wrappers around
//!     `Io.Mutex` / `Io.RwLock`. They exist mainly to give the rest of the
//!     engine a single import surface and a stable API even if the underlying
//!     `std.Io` types shift.
//!
//!   * [`GroupLock`] is the heart of NovaDB's per-table access protocol. Unlike
//!     an ordinary reader/writer lock it has THREE co-operating admission
//!     classes, chosen to match how SQL statements touch a table:
//!       - **read** mode  (SELECT): many readers run concurrently;
//!       - **write** mode (INSERT): many *appending* writers run concurrently,
//!         because concurrent inserts on one B+Tree are made safe lower down by
//!         the per-tree `structure_lock` in `btree.zig`, so they do not need to
//!         exclude each other at the table level;
//!       - **exclusive** mode (UPDATE / DELETE / DDL): the statement scans and
//!         mutates in place, so it must run alone on the table.
//!
//! Read and write are mutually exclusive with each other (a scan must not see a
//! half-applied insert), but each admits an unbounded group of its own kind,
//! hence "GroupLock". Exclusive excludes everything, including other exclusives.
//!
//! The admission state is a single signed counter [`GroupLock.mode`] guarded by
//! [`GroupLock.mutex`]: `mode > 0` counts active readers, `mode < 0` counts
//! active writers, `mode == 0` means idle. [`GroupLock.exclusive_active`] is a
//! separate boolean because an exclusive holder leaves `mode` at 0. To prevent
//! writer/exclusive starvation, readers defer whenever any writer or exclusive
//! is *waiting*, not only when one is active, so a steady stream of SELECTs
//! cannot indefinitely postpone an UPDATE. Wakeups follow a fixed priority
//! (exclusive, then write, then read) via [`GroupLock.wakeSuccessors`].
//!
//! See `architecture.md` section 2 ("Per-Table Access Lock" and "Per-Tree
//! Structure Lock") for how this layer combines with the B+Tree's own locking.

const std = @import("std");
/// Alias for the async I/O namespace whose `Mutex`, `RwLock` and `Semaphore`
/// these primitives are built from; every blocking call threads an `Io` value
/// so the scheduler can suspend the caller instead of blocking a thread.
const Io = std.Io;

/// Async-aware mutual-exclusion lock, a wrapper over [`Io.Mutex`].
///
/// Exists to give the engine one import point and a stable surface. Acquisition
/// is uncancelable (see the module header): a coroutine waiting on the lock
/// yields to the scheduler rather than blocking an OS thread, but once it starts
/// acquiring it always completes.
pub const Mutex = struct {
    /// The wrapped async mutex, initialised to its unlocked state.
    impl: Io.Mutex = Io.Mutex.init,

    /// Acquires the mutex, suspending the caller (never a spin) until it is
    /// free. Uncancelable, so the acquisition cannot be interrupted once begun.
    pub fn lock(self: *Mutex, io: Io) void {
        self.impl.lockUncancelable(io);
    }

    /// Releases a lock previously taken by [`Mutex.lock`]. Must be called by the
    /// same logical holder; releasing an unheld mutex is a programming error.
    pub fn unlock(self: *Mutex, io: Io) void {
        self.impl.unlock(io);
    }

    /// Attempts to acquire without waiting, returning `true` on success and
    /// `false` if the lock is currently held. Never suspends, so it needs no
    /// `Io`.
    pub fn tryLock(self: *Mutex) bool {
        return self.impl.tryLock();
    }
};

/// Async-aware reader/writer lock, a wrapper over [`Io.RwLock`].
///
/// Admits many concurrent shared (reader) holders or one exclusive (writer)
/// holder. Provided alongside [`Mutex`] for call sites that genuinely have a
/// two-class read/write split; table-level access instead uses [`GroupLock`],
/// which adds a third (append-writer) class.
pub const RwLock = struct {
    /// The wrapped async reader/writer lock, initialised unlocked.
    impl: Io.RwLock = Io.RwLock.init,

    /// Takes the lock for exclusive (write) access, suspending until no reader
    /// or writer holds it. Uncancelable.
    pub fn lock(self: *RwLock, io: Io) void {
        self.impl.lockUncancelable(io);
    }

    /// Releases the exclusive hold taken by [`RwLock.lock`].
    pub fn unlock(self: *RwLock, io: Io) void {
        self.impl.unlock(io);
    }

    /// Takes the lock for shared (read) access, suspending only while a writer
    /// holds it; concurrent with other shared holders. Uncancelable.
    pub fn lockShared(self: *RwLock, io: Io) void {
        self.impl.lockSharedUncancelable(io);
    }

    /// Releases a shared hold taken by [`RwLock.lockShared`].
    pub fn unlockShared(self: *RwLock, io: Io) void {
        self.impl.unlockShared(io);
    }

    /// Attempts an exclusive acquisition without waiting; returns whether it
    /// succeeded.
    pub fn tryLock(self: *RwLock, io: Io) bool {
        return self.impl.tryLock(io);
    }

    /// Attempts a shared acquisition without waiting; returns whether it
    /// succeeded.
    pub fn tryLockShared(self: *RwLock, io: Io) bool {
        return self.impl.tryLockShared(io);
    }
};

/// Three-mode per-table access lock: read, write and exclusive groups.
///
/// This is the table-granularity lock in NovaDB's Stage-3 concurrency design.
/// It generalises a reader/writer lock with a third class so that INSERTs (which
/// only append and are made mutually safe by the B+Tree's own `structure_lock`)
/// can run as a concurrent *group* distinct from SELECTs, while UPDATE/DELETE/
/// DDL take the whole table exclusively. See the module header for the full
/// admission rules and the `mode` sign convention.
///
/// All state is protected by [`GroupLock.mutex`]; waiters park on the three
/// counting semaphores and are released in priority order by
/// [`GroupLock.wakeSuccessors`]. Callers MUST pair each `lockX` with the
/// matching `unlockX` on the same lock; mixing modes (e.g. `unlockRead` after
/// `lockWrite`) corrupts the counter.
pub const GroupLock = struct {
    /// Guards every other field; every public method takes it on entry and
    /// releases it before suspending on a semaphore, re-taking it on wake.
    mutex: Io.Mutex = Io.Mutex.init,
    /// Signed admission counter: `> 0` is that many active readers, `< 0` is
    /// that many active writers, `0` is idle. Read and write holders can never
    /// coexist because a reader requires `mode >= 0` and a writer `mode <= 0`.
    mode: i64 = 0,
    /// True while an exclusive holder owns the table. Kept separate because an
    /// exclusive holder leaves [`GroupLock.mode`] at `0` (it is neither reader
    /// nor writer), yet must still block all three classes.
    exclusive_active: bool = false,
    /// Number of readers currently parked on [`GroupLock.reader_sem`]. Used to
    /// post exactly enough permits when readers become admissible.
    reader_waiters: u32 = 0,
    /// Number of writers currently parked on [`GroupLock.writer_sem`]. Non-zero
    /// here makes new readers defer, preventing writer starvation.
    writer_waiters: u32 = 0,
    /// Number of exclusive acquirers parked on [`GroupLock.exclusive_sem`].
    /// Non-zero here makes both new readers and new writers defer, giving
    /// exclusive requests top priority.
    exclusive_waiters: u32 = 0,
    /// Semaphore readers wait on; one permit is posted per waiting reader when a
    /// read group becomes admissible.
    reader_sem: Io.Semaphore = .{},
    /// Semaphore writers wait on; one permit per waiting writer when a write
    /// group becomes admissible.
    writer_sem: Io.Semaphore = .{},
    /// Semaphore exclusive acquirers wait on; posted when the table falls idle.
    exclusive_sem: Io.Semaphore = .{},

    /// Wakes the next eligible group of waiters, in strict priority order:
    /// exclusive first, then writers, then readers.
    ///
    /// Called with [`GroupLock.mutex`] held, whenever the lock transitions to a
    /// state where new holders could be admitted (last reader/writer left, or an
    /// exclusive holder released). It posts one permit for EVERY waiter in the
    /// chosen class in a single burst, since an entire read or write group may
    /// proceed together; each woken waiter re-checks the admission predicate
    /// under the mutex, so a spurious over-post simply re-parks. Only the
    /// highest-priority non-empty class is served per call, which is what makes
    /// exclusive and write requests eventually win over a reader stream.
    fn wakeSuccessors(self: *GroupLock, io: Io) void {
        if (self.exclusive_waiters > 0) {
            var i: u32 = 0;
            while (i < self.exclusive_waiters) : (i += 1) self.exclusive_sem.post(io);
        } else if (self.writer_waiters > 0) {
            var i: u32 = 0;
            while (i < self.writer_waiters) : (i += 1) self.writer_sem.post(io);
        } else if (self.reader_waiters > 0) {
            var i: u32 = 0;
            while (i < self.reader_waiters) : (i += 1) self.reader_sem.post(io);
        }
    }

    /// Enters read mode (SELECT); admits many concurrent readers.
    ///
    /// Suspends while a writer or exclusive holder is active, OR while any
    /// writer or exclusive acquirer is merely *waiting* (`writer_waiters > 0` or
    /// `exclusive_waiters > 0`). That extra condition is deliberate: it stops a
    /// continuous flow of readers from starving a pending write/exclusive. On
    /// admission it does `mode += 1`. Pair with [`GroupLock.unlockRead`].
    pub fn lockRead(self: *GroupLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        while (self.mode < 0 or self.exclusive_active or self.writer_waiters > 0 or self.exclusive_waiters > 0) {
            self.reader_waiters += 1;
            self.mutex.unlock(io);
            self.reader_sem.waitUncancelable(io);
            self.mutex.lockUncancelable(io);
            self.reader_waiters -= 1;
        }
        self.mode += 1;
        self.mutex.unlock(io);
    }

    /// Leaves read mode, decrementing [`GroupLock.mode`]. When the count reaches
    /// zero (this was the last reader) it wakes the next group via
    /// [`GroupLock.wakeSuccessors`], handing the table to any pending writer or
    /// exclusive acquirer.
    pub fn unlockRead(self: *GroupLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        self.mode -= 1;
        if (self.mode == 0) self.wakeSuccessors(io);
        self.mutex.unlock(io);
    }

    /// Enters write mode (INSERT); admits many concurrent appending writers.
    ///
    /// Suspends while any reader is active (`mode > 0`) or an exclusive holder
    /// is active or waiting. It does NOT exclude other writers: concurrent
    /// inserts on one B+Tree are kept safe by the per-tree `structure_lock` in
    /// `btree.zig`, so at the table level they form a group. Note there is no
    /// `writer_waiters`-style deferral against readers, so writers and readers
    /// alternate rather than one class starving the other. On admission it does
    /// `mode -= 1`. Pair with [`GroupLock.unlockWrite`].
    pub fn lockWrite(self: *GroupLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        while (self.mode > 0 or self.exclusive_active or self.exclusive_waiters > 0) {
            self.writer_waiters += 1;
            self.mutex.unlock(io);
            self.writer_sem.waitUncancelable(io);
            self.mutex.lockUncancelable(io);
            self.writer_waiters -= 1;
        }
        self.mode -= 1;
        self.mutex.unlock(io);
    }

    /// Leaves write mode. Because writers make [`GroupLock.mode`] negative, this
    /// increments back towards zero; when it reaches zero (last writer of the
    /// group) it wakes the next group via [`GroupLock.wakeSuccessors`].
    pub fn unlockWrite(self: *GroupLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        self.mode += 1;
        if (self.mode == 0) self.wakeSuccessors(io);
        self.mutex.unlock(io);
    }

    /// Enters exclusive mode (UPDATE / DELETE / DDL); admits exactly one holder.
    ///
    /// Suspends until the table is fully idle: no readers and no writers
    /// (`mode == 0`) and no other exclusive holder. On admission it sets
    /// [`GroupLock.exclusive_active`] rather than touching `mode`, so the table
    /// reads as idle to the counter while still blocking every class. This mode
    /// is used by statements that scan and mutate in place and therefore need
    /// the whole table to themselves. Pair with [`GroupLock.unlockExclusive`].
    pub fn lockExclusive(self: *GroupLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        while (self.mode != 0 or self.exclusive_active) {
            self.exclusive_waiters += 1;
            self.mutex.unlock(io);
            self.exclusive_sem.waitUncancelable(io);
            self.mutex.lockUncancelable(io);
            self.exclusive_waiters -= 1;
        }
        self.exclusive_active = true;
        self.mutex.unlock(io);
    }

    /// Leaves exclusive mode, clearing [`GroupLock.exclusive_active`]. Since the
    /// table is now idle it unconditionally wakes the next group via
    /// [`GroupLock.wakeSuccessors`] (which will prefer another exclusive, then
    /// writers, then readers).
    pub fn unlockExclusive(self: *GroupLock, io: Io) void {
        self.mutex.lockUncancelable(io);
        self.exclusive_active = false;
        self.wakeSuccessors(io);
        self.mutex.unlock(io);
    }
};
