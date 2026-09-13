//! Transaction manager: MVCC visibility plus serializable snapshot isolation.
//!
//! This module owns the engine's notion of "which transactions exist, and which
//! versions of a row may a given transaction see". It sits underneath the B+Tree
//! and the SQL executor: every heap tuple carries an `xmin` (the transaction that
//! created it) and an `xmax` (the transaction that deleted it), and the executor
//! asks [`TransactionManager.isVisible`] whether a tuple is live for the reader.
//! There is no on-disk transaction table here, the manager keeps the live state
//! (active set, committed set, next id) purely in memory and the WAL is what makes
//! commits durable elsewhere in the engine.
//!
//! ## MVCC visibility (the core)
//!
//! A monotonically increasing [`TxId`] is handed out per transaction. A tuple is
//! visible to reader `T` when its creator is "in the past and real" and its
//! deleter is "not yet real":
//!
//!   - the creator `xmin` counts as valid if it is the bootstrap id `0`, is `T`
//!     itself (T sees its own writes), or has committed;
//!   - the deleter `xmax` hides the tuple only if it is `0`-means-not-deleted
//!     inverted, i.e. a non-zero `xmax` that is `T` itself or a committed
//!     transaction hides the row. A deleter that aborted or is still in flight
//!     leaves the row visible.
//!
//! Two visibility entry points exist by design. [`TransactionManager.isVisible`]
//! reads the LIVE committed set under the manager mutex, giving read-committed
//! style semantics where each check sees the latest commits. [`TransactionManager.isVisibleIn`]
//! evaluates against a frozen [`Snapshot`] captured at statement or transaction
//! start, giving repeatable-read / snapshot isolation: a transaction that committed
//! AFTER the snapshot was taken is treated as not-yet-committed, so the reader's
//! view never shifts under it. The snapshot form is lock-free precisely because it
//! never touches shared mutable state.
//!
//! ## Serializable snapshot isolation (SSI)
//!
//! Snapshot isolation alone permits write skew (two transactions each read a set,
//! then each writes based on what the other did not yet see). The SSI half
//! implements Cahill's algorithm to detect and break the dangerous structure that
//! causes it. For each serializable transaction we track the set of tables it read
//! and wrote (table granularity, not row granularity, which is coarse but cheap and
//! sound). When transaction A reads a table that a concurrent transaction B has
//! written, that is an rw-antidependency A -> B: A gets an OUT edge and B gets an IN
//! edge. A transaction that has BOTH an inbound and an outbound rw-antidependency is
//! a "pivot", and every serialization anomaly under SI contains such a pivot, so at
//! commit time [`TransactionManager.ssiIsPivot`] aborts it. This over-aborts (not
//! every pivot is a real cycle) but never lets an anomaly through.
//!
//! All SSI bookkeeping lives behind its OWN mutex ([`TransactionManager.ssi_mutex`]),
//! separate from the MVCC [`TransactionManager.mutex`], so conflict tracking on the
//! serializable path never contends with the visibility checks that every read
//! performs. The read/write table names are duped into the manager's allocator and
//! freed when the SSI record is torn down.

const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;

/// Monotonic transaction identifier, unique for the life of the process.
///
/// Id `0` is reserved as the bootstrap / "always visible" sentinel used for
/// tuples that predate any transaction (see the `xmin == 0` case in
/// [`TransactionManager.isVisible`]); real transactions start at `1`. Stored on
/// every heap tuple as its `xmin`/`xmax` stamps.
pub const TxId = u64;

/// Lifecycle state of a transaction.
///
/// Retained as the canonical vocabulary for a transaction's status. Note the
/// manager itself does not persist a per-transaction `TransactionState`; it
/// infers status from membership in the active/committed sets (absent from both
/// after an [`TransactionManager.abort`] means aborted), so this enum documents
/// the state machine callers reason about rather than a stored field.
pub const TransactionState = enum(u8) {
    /// In progress: present in [`TransactionManager.active_txns`].
    ACTIVE,
    /// Successfully committed: moved into [`TransactionManager.committed_txns`].
    COMMITTED,
    /// Rolled back: removed from the active set and never committed.
    ABORTED,
};

/// An immutable point-in-time view of committed transactions.
///
/// Captured by [`TransactionManager.captureSnapshot`] and consumed by
/// [`TransactionManager.isVisibleIn`] to give a reader a stable view: any
/// transaction that commits after this snapshot is deliberately treated as
/// still-uncommitted, which is what makes repeatable-read / snapshot isolation
/// non-shifting. Owns its own `committed` map (allocated from the caller's
/// allocator) and must be released with [`Snapshot.deinit`].
pub const Snapshot = struct {
    /// The set of [`TxId`]s that had committed at capture time. Membership plus
    /// the `< xmax` bound decides whether a creator/deleter counts as committed
    /// for a reader using this snapshot.
    committed: std.AutoHashMap(TxId, void),
    /// The exclusive upper bound on visible ids: the manager's `next_tx_id` at
    /// capture time. Any [`TxId`] `>= xmax` started after the snapshot and is
    /// invisible regardless of the `committed` set, which also cheaply rejects
    /// ids that could not possibly be in the set.
    xmax: TxId,
    /// The manager's commit watermark at capture time: any id `< committed_below`
    /// had already committed (unless in `aborted`), so `committed` need only hold
    /// the recent window at/above it. This is what keeps a snapshot bounded even
    /// after millions of commits.
    committed_below: TxId,
    /// A copy of the manager's `aborted` set at capture time - the exceptions
    /// below `committed_below`. Normally empty.
    aborted: std.AutoHashMap(TxId, void),

    /// Frees the snapshot's owned maps. Call exactly once when the reader that
    /// captured it is done; the [`Snapshot`] is invalid afterwards.
    pub fn deinit(self: *Snapshot) void {
        self.committed.deinit();
        self.aborted.deinit();
    }
};

/// Per-transaction conflict bookkeeping for serializable snapshot isolation.
///
/// One instance exists per in-flight serializable transaction, keyed by [`TxId`]
/// in [`TransactionManager.ssi_txns`]. It records the tables the transaction
/// touched so that rw-antidependencies with concurrent transactions can be
/// detected, and carries the two edge flags whose conjunction marks a pivot (see
/// the SSI overview at the top of this file and [`TransactionManager.ssiIsPivot`]).
pub const SsiTxn = struct {
    /// Set of table names this transaction has read. Keys are owned copies
    /// duped into the manager allocator (freed in [`SsiTxn.deinit`]); the set is
    /// consulted when another transaction writes a table to detect the read half
    /// of an rw-antidependency.
    reads: std.StringHashMap(void),
    /// Set of table names this transaction has written. Keys are owned copies;
    /// consulted when another transaction reads a table to detect the write half
    /// of an rw-antidependency.
    writes: std.StringHashMap(void),
    /// True once some concurrent transaction has an rw-antidependency INTO this
    /// one (another txn read a table this txn wrote, or this txn wrote a table
    /// another read). Half of the pivot condition.
    in_conflict: bool = false,
    /// True once this transaction has an rw-antidependency OUT to some concurrent
    /// transaction (this txn read a table another wrote, or another wrote a table
    /// this txn read). The other half of the pivot condition.
    out_conflict: bool = false,

    /// Frees both table-name sets, including the duped key strings.
    ///
    /// Iterates each set freeing the owned keys before deiniting the maps, since
    /// the keys were allocated with `a.dupe` in [`TransactionManager.ssiRead`] /
    /// [`TransactionManager.ssiWrite`]. `a` must be the same allocator those
    /// dupes came from.
    fn deinit(self: *SsiTxn, a: Allocator) void {
        var rit = self.reads.keyIterator();
        while (rit.next()) |k| a.free(k.*);
        self.reads.deinit();
        var wit = self.writes.keyIterator();
        while (wit.next()) |k| a.free(k.*);
        self.writes.deinit();
    }
};

/// The engine-wide transaction registry and visibility oracle.
///
/// Holds all in-memory transaction state and answers the MVCC visibility
/// question for the storage/executor layers. It is shared across worker threads,
/// so all mutable state is guarded: the active/committed sets and `next_tx_id` by
/// [`TransactionManager.mutex`], and the SSI conflict graph by the independent
/// [`TransactionManager.ssi_mutex`]. Locks are taken with `lockUncancelable`
/// because these critical sections are short and must not be interrupted mid-way
/// by async cancellation, which would leave the sets inconsistent.
pub const TransactionManager = struct {
    /// Allocator backing the hash maps and the duped SSI key strings. Every
    /// allocation this manager makes (and frees) comes from here.
    allocator: Allocator,
    /// The id to hand out for the next [`TransactionManager.begin`], also serving
    /// as the exclusive upper bound stamped into a [`Snapshot.xmax`]. Guarded by
    /// [`TransactionManager.mutex`].
    next_tx_id: TxId = 1,
    /// The set of currently in-flight transactions. A transaction lives here from
    /// [`TransactionManager.begin`] until it commits or aborts; drives
    /// [`TransactionManager.getOldestActiveTxId`] which vacuum uses as its horizon.
    active_txns: std.AutoHashMap(TxId, void),
    /// The set of transactions that have committed. Membership is the source of
    /// truth for "did xmin/xmax commit" in [`TransactionManager.isVisible`], and
    /// is snapshotted wholesale by [`TransactionManager.captureSnapshot`].
    committed_txns: std.AutoHashMap(TxId, void),
    /// Commit watermark: every [`TxId`] strictly BELOW this is DONE (committed or
    /// aborted). A tx `< committed_below` counts as COMMITTED iff it is NOT in
    /// `aborted`. This lets `committed_txns` hold only the recent window (ids at or
    /// above the watermark) instead of every committed id forever: without it the
    /// committed set - and the checkpoint sidecar that persists it, and the copy
    /// [`TransactionManager.captureSnapshot`] makes - grew one entry per committed
    /// transaction, so a 10M-insert load produced an ~80 MB sidecar and an O(n)
    /// restore that stalled startup for minutes. Advanced (and `committed_txns`
    /// pruned) by [`TransactionManager.advanceWatermark`] at checkpoint/persist.
    committed_below: TxId = 1,
    /// The aborted transactions - the exceptions below `committed_below`, since
    /// "done and not aborted" means committed. Normally tiny (an all-commit
    /// workload leaves it empty); it is what keeps the watermark shortcut correct
    /// when a transaction rolls back.
    aborted: std.AutoHashMap(TxId, void),
    /// Guards `next_tx_id`, `active_txns`, `committed_txns`, `committed_below` and
    /// `aborted`. Held only for the duration of each short operation.
    mutex: std.Io.Mutex = .init,
    /// Guards `ssi_txns` and the flags inside each [`SsiTxn`]. Kept separate from
    /// [`TransactionManager.mutex`] so serializable conflict tracking does not
    /// contend with the visibility checks on the hot read path.
    ssi_mutex: std.Io.Mutex = .init,
    /// The SSI conflict graph: one [`SsiTxn`] per live serializable transaction,
    /// owned (heap-allocated) by the manager and destroyed on
    /// [`TransactionManager.ssiEnd`]. Empty for non-serializable transactions,
    /// which never call the `ssi*` methods.
    ssi_txns: std.AutoHashMap(TxId, *SsiTxn),

    /// Constructs an empty manager with all sets initialised and ids starting at
    /// `1`. The returned value owns nothing until transactions begin; pair with
    /// [`TransactionManager.deinit`].
    pub fn init(allocator: Allocator) TransactionManager {
        return .{
            .allocator = allocator,
            .active_txns = std.AutoHashMap(TxId, void).init(allocator),
            .committed_txns = std.AutoHashMap(TxId, void).init(allocator),
            .aborted = std.AutoHashMap(TxId, void).init(allocator),
            .ssi_txns = std.AutoHashMap(TxId, *SsiTxn).init(allocator),
        };
    }

    /// Tears down all state: the active/committed sets, and every live
    /// [`SsiTxn`] (each is deinit'd then `destroy`d, since they are individually
    /// heap-allocated). Any transaction still active at this point is simply
    /// dropped; no rollback semantics are applied here.
    pub fn deinit(self: *TransactionManager) void {
        self.active_txns.deinit();
        self.committed_txns.deinit();
        self.aborted.deinit();
        var it = self.ssi_txns.valueIterator();
        while (it.next()) |v| {
            v.*.deinit(self.allocator);
            self.allocator.destroy(v.*);
        }
        self.ssi_txns.deinit();
    }

    /// Registers `tx` as a serializable transaction, creating its empty
    /// [`SsiTxn`] conflict record.
    ///
    /// Idempotent: a second call for the same `tx` is a no-op, so re-entering
    /// SSI tracking for an already-tracked transaction will not leak a second
    /// record. Only serializable transactions call this; the read-committed and
    /// snapshot paths never enter the SSI graph.
    pub fn ssiBegin(self: *TransactionManager, io: Io, tx: TxId) !void {
        self.ssi_mutex.lockUncancelable(io);
        defer self.ssi_mutex.unlock(io);
        if (self.ssi_txns.contains(tx)) return;
        const t = try self.allocator.create(SsiTxn);
        t.* = .{ .reads = std.StringHashMap(void).init(self.allocator), .writes = std.StringHashMap(void).init(self.allocator) };
        try self.ssi_txns.put(tx, t);
    }

    /// Records that serializable transaction `tx` read `table`, and updates the
    /// conflict graph for the READ side of any rw-antidependency.
    ///
    /// The table name is duped into the manager allocator on first read. Then,
    /// for every OTHER live serializable transaction that has WRITTEN this table,
    /// an rw-antidependency `tx -> other` is recorded: `tx` gains an out-edge and
    /// `other` gains an in-edge. If `tx` is not tracked (not serializable, or
    /// already ended) the call is a silent no-op. Table granularity means this
    /// can flag conflicts on disjoint rows of the same table, which is sound but
    /// conservative. See [`TransactionManager.ssiIsPivot`].
    pub fn ssiRead(self: *TransactionManager, io: Io, tx: TxId, table: []const u8) !void {
        self.ssi_mutex.lockUncancelable(io);
        defer self.ssi_mutex.unlock(io);
        const t = self.ssi_txns.get(tx) orelse return;
        if (!t.reads.contains(table)) try t.reads.put(try self.allocator.dupe(u8, table), {});
        var it = self.ssi_txns.iterator();
        while (it.next()) |e| {
            if (e.key_ptr.* == tx) continue;
            if (e.value_ptr.*.writes.contains(table)) {
                t.out_conflict = true;
                e.value_ptr.*.in_conflict = true;
            }
        }
    }

    /// Records that serializable transaction `tx` wrote `table`, and updates the
    /// conflict graph for the WRITE side of any rw-antidependency.
    ///
    /// Mirror of [`TransactionManager.ssiRead`]: for every other live
    /// serializable transaction that has READ this table, an rw-antidependency
    /// `other -> tx` is recorded (the reader gets the out-edge, `tx` gets the
    /// in-edge). No-op if `tx` is untracked. Also table-granular and thus
    /// conservative.
    pub fn ssiWrite(self: *TransactionManager, io: Io, tx: TxId, table: []const u8) !void {
        self.ssi_mutex.lockUncancelable(io);
        defer self.ssi_mutex.unlock(io);
        const t = self.ssi_txns.get(tx) orelse return;
        if (!t.writes.contains(table)) try t.writes.put(try self.allocator.dupe(u8, table), {});
        var it = self.ssi_txns.iterator();
        while (it.next()) |e| {
            if (e.key_ptr.* == tx) continue;
            if (e.value_ptr.*.reads.contains(table)) {
                e.value_ptr.*.out_conflict = true;
                t.in_conflict = true;
            }
        }
    }

    /// Reports whether `tx` is a pivot and must therefore be aborted to preserve
    /// serializability.
    ///
    /// A transaction is a pivot when it has BOTH an inbound and an outbound
    /// rw-antidependency ([`SsiTxn.in_conflict`] and [`SsiTxn.out_conflict`]).
    /// Every write-skew anomaly under snapshot isolation contains such a pivot,
    /// so aborting pivots at commit time is sufficient to guarantee
    /// serializability. It over-aborts (not every pivot lies on a real cycle),
    /// which is the accepted false-positive cost of Cahill's algorithm. Returns
    /// false for an untracked `tx`.
    pub fn ssiIsPivot(self: *TransactionManager, io: Io, tx: TxId) bool {
        self.ssi_mutex.lockUncancelable(io);
        defer self.ssi_mutex.unlock(io);
        const t = self.ssi_txns.get(tx) orelse return false;
        return t.in_conflict and t.out_conflict;
    }

    /// Removes and destroys `tx`'s SSI record, freeing its table-name sets.
    ///
    /// Called when a serializable transaction commits or aborts, after any
    /// [`TransactionManager.ssiIsPivot`] check. No-op if `tx` was never tracked.
    /// Note that once removed, `tx` no longer participates in conflict detection
    /// for still-live transactions, which is correct because a finished
    /// transaction can no longer form new rw-antidependencies.
    pub fn ssiEnd(self: *TransactionManager, io: Io, tx: TxId) void {
        self.ssi_mutex.lockUncancelable(io);
        defer self.ssi_mutex.unlock(io);
        if (self.ssi_txns.fetchRemove(tx)) |kv| {
            kv.value.deinit(self.allocator);
            self.allocator.destroy(kv.value);
        }
    }

    /// Starts a new transaction, returning its freshly allocated [`TxId`].
    ///
    /// Allocates the next id, bumps `next_tx_id`, and adds the id to
    /// [`TransactionManager.active_txns`]. Ids increase monotonically and are
    /// never reused, which is what lets `xmin`/`xmax` comparisons and snapshot
    /// bounds work. This is the MVCC begin only; a serializable transaction must
    /// additionally call [`TransactionManager.ssiBegin`].
    pub fn begin(self: *TransactionManager, io: Io) !TxId {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        const tx_id = self.next_tx_id;
        self.next_tx_id += 1;
        try self.active_txns.put(tx_id, {});
        return tx_id;
    }

    /// Commits `tx_id`: moves it from the active set to the committed set.
    ///
    /// The move is conditional on the transaction actually being active, so
    /// double-commit or committing an already-aborted id does nothing. Once in
    /// [`TransactionManager.committed_txns`] the transaction's writes become
    /// visible to live readers via [`TransactionManager.isVisible`] and to any
    /// snapshot captured after this point. Durability (WAL flush) is the caller's
    /// responsibility elsewhere; this only updates in-memory visibility state.
    pub fn commit(self: *TransactionManager, io: Io, tx_id: TxId) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        if (self.active_txns.remove(tx_id)) {
            try self.committed_txns.put(tx_id, {});
        }
    }

    /// Aborts `tx_id`: removes it from the active set without ever committing it.
    ///
    /// Because it never enters [`TransactionManager.committed_txns`], an aborted
    /// creator makes its tuples invisible and an aborted deleter leaves the
    /// deleted tuple visible again (the `xmax`-not-committed case in
    /// [`TransactionManager.isVisible`]). Rolling back the actual data changes is
    /// handled by the undo log elsewhere; this only retracts visibility.
    pub fn abort(self: *TransactionManager, io: Io, tx_id: TxId) !void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        _ = self.active_txns.remove(tx_id);
        // Record the abort so the watermark shortcut (below `committed_below` ==
        // committed) can except it. Cheap: aborts are the exceptional case.
        try self.aborted.put(tx_id, {});
    }

    /// Whether `xid` counts as committed, watermark-aware. Below the watermark a
    /// transaction is committed unless it is a recorded abort; at or above it,
    /// membership in the recent `committed_txns` window is the source of truth.
    /// Caller must hold `mutex`.
    inline fn committedContainsLocked(self: *const TransactionManager, xid: TxId) bool {
        if (xid < self.committed_below) return !self.aborted.contains(xid);
        return self.committed_txns.contains(xid);
    }

    /// Decides whether a tuple stamped `(xmin, xmax)` is visible to `current_tx`
    /// against the LIVE committed set (read-committed style).
    ///
    /// The creator `xmin` must be valid: `0` (bootstrap, always valid),
    /// `current_tx` itself (a transaction sees its own inserts), or a committed
    /// transaction. An invalid creator hides the tuple immediately. Then the
    /// deleter `xmax` is checked: `0` means never deleted so the tuple is
    /// visible; `current_tx` means the reader deleted it so it is hidden; a
    /// committed `xmax` means some other transaction's delete is durable so it is
    /// hidden; anything else (deleter aborted or still in flight) leaves the tuple
    /// visible. Because it reads the live set under the mutex, successive calls in
    /// one transaction can see newly committed data, unlike
    /// [`TransactionManager.isVisibleIn`].
    pub fn isVisible(self: *TransactionManager, io: Io, current_tx: TxId, xmin: TxId, xmax: TxId) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var xmin_valid = false;
        if (xmin == 0) {
            xmin_valid = true;
        } else if (xmin == current_tx) {
            xmin_valid = true;
        } else if (self.committedContainsLocked(xmin)) {
            xmin_valid = true;
        }

        if (!xmin_valid) return false;

        if (xmax == 0) return true;
        if (xmax == current_tx) return false;

        if (self.committedContainsLocked(xmax)) {
            return false;
        }

        return true;
    }

    /// Captures an immutable [`Snapshot`] of the currently committed
    /// transactions plus the current `next_tx_id` as the visibility bound.
    ///
    /// Copies the entire committed set into a fresh map owned by `allocator` (the
    /// caller's, which may differ from the manager's) so the snapshot survives
    /// independently of later commits. On allocation failure the partial map is
    /// cleaned up via `errdefer`. The resulting snapshot is used with
    /// [`TransactionManager.isVisibleIn`] to give a transaction a stable,
    /// repeatable view; release it with [`Snapshot.deinit`].
    pub fn captureSnapshot(self: *TransactionManager, io: Io, allocator: Allocator) !Snapshot {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var m = std.AutoHashMap(TxId, void).init(allocator);
        errdefer m.deinit();
        var it = self.committed_txns.keyIterator();
        while (it.next()) |k| {
            try m.put(k.*, {});
        }
        var ab = std.AutoHashMap(TxId, void).init(allocator);
        errdefer ab.deinit();
        var ait = self.aborted.keyIterator();
        while (ait.next()) |k| {
            try ab.put(k.*, {});
        }
        return Snapshot{ .committed = m, .xmax = self.next_tx_id, .committed_below = self.committed_below, .aborted = ab };
    }

    /// Decides visibility of a tuple stamped `(xmin, xmax)` for `current_tx`
    /// against a FROZEN [`Snapshot`] (snapshot / repeatable-read isolation).
    ///
    /// Same logic as [`TransactionManager.isVisible`] but "committed" is decided
    /// by the snapshot rather than the live set: a transaction counts as
    /// committed only if it is below the snapshot's `xmax` bound AND present in
    /// its `committed` set. A transaction that committed after the snapshot was
    /// taken is therefore treated as not-yet-committed, keeping the reader's view
    /// stable across statements. This method takes no lock (`self` is discarded)
    /// because a captured snapshot is immutable and reads no shared mutable state.
    pub fn isVisibleIn(self: *TransactionManager, snap: *const Snapshot, current_tx: TxId, xmin: TxId, xmax: TxId) bool {
        _ = self;
        const committedIn = struct {
            fn f(s: *const Snapshot, xid: TxId) bool {
                if (xid < s.committed_below) return !s.aborted.contains(xid);
                return xid < s.xmax and s.committed.contains(xid);
            }
        }.f;
        var xmin_valid = false;
        if (xmin == 0 or xmin == current_tx) {
            xmin_valid = true;
        } else if (committedIn(snap, xmin)) {
            xmin_valid = true;
        }
        if (!xmin_valid) return false;

        if (xmax == 0) return true;
        if (xmax == current_tx) return false;
        if (committedIn(snap, xmax)) return false;
        return true;
    }

    /// Reports whether `xid` is in the live committed set. A direct membership
    /// probe used where a caller needs a transaction's commit status without a
    /// full tuple-visibility check.
    pub fn isCommitted(self: *TransactionManager, io: Io, xid: TxId) bool {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.committedContainsLocked(xid);
    }

    /// Advances the commit watermark to the oldest still-active transaction (or
    /// `next_tx_id` when none are active) and prunes `committed_txns` of everything
    /// now implied by the watermark. After this, `committed_txns` holds only the
    /// ids at or above the new watermark, so the persisted commit-state sidecar and
    /// every captured snapshot stay bounded regardless of how many transactions
    /// have committed. Idempotent and cheap when the watermark cannot move. Takes
    /// `mutex`.
    pub fn advanceWatermark(self: *TransactionManager, io: Io) void {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var oldest: TxId = self.next_tx_id;
        var ait = self.active_txns.keyIterator();
        while (ait.next()) |k| {
            if (k.* < oldest) oldest = k.*;
        }
        if (oldest <= self.committed_below) return;

        if (oldest >= self.next_tx_id) {
            // No active transactions: every committed id is below the watermark and
            // now implied, so the whole recent window can be dropped in one shot.
            self.committed_txns.clearRetainingCapacity();
        } else {
            // Some transaction is still active: keep only ids at/above `oldest`.
            var to_remove = std.ArrayList(TxId).empty;
            defer to_remove.deinit(self.allocator);
            var it = self.committed_txns.keyIterator();
            while (it.next()) |k| {
                if (k.* < oldest) to_remove.append(self.allocator, k.*) catch return;
            }
            for (to_remove.items) |k| _ = self.committed_txns.remove(k);
        }
        self.committed_below = oldest;
    }

    /// Returns the smallest [`TxId`] still active, or `next_tx_id` if none are.
    ///
    /// This is the vacuum / garbage-collection horizon: no active transaction can
    /// see a row version deleted by a transaction older than this id, so versions
    /// below it may be reclaimed. Returning `next_tx_id` when the active set is
    /// empty correctly means "everything up to now is reclaimable". Computed by a
    /// linear scan of [`TransactionManager.active_txns`], which is fine given the
    /// small number of concurrent transactions.
    pub fn getOldestActiveTxId(self: *TransactionManager, io: Io) TxId {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);

        var min_active = self.next_tx_id;
        var it = self.active_txns.keyIterator();
        while (it.next()) |k| {
            if (k.* < min_active) {
                min_active = k.*;
            }
        }
        return min_active;
    }

    /// Number of transactions currently in progress. Used by the index-only
    /// COUNT fast path: when this is 1 (only the counting transaction itself),
    /// no concurrent writer can be adding index entries a snapshot shouldn't see,
    /// so counting index entries is safe.
    pub fn activeTxnCount(self: *TransactionManager, io: Io) usize {
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.active_txns.count();
    }
};

test "watermark: advancing it preserves visibility of committed and aborted txns" {
    const testing = std.testing;
    var threaded = std.Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var tm = TransactionManager.init(testing.allocator);
    defer tm.deinit();

    // t1 commits, t2 aborts, t3 commits. (begin allocates 1,2,3.)
    const t1 = try tm.begin(io);
    const t2 = try tm.begin(io);
    const t3 = try tm.begin(io);
    try tm.commit(io, t1);
    try tm.abort(io, t2);
    try tm.commit(io, t3);

    // Baseline visibility (reader tx 0 = read-committed against the live set):
    // a row created by a committed tx is visible; by an aborted tx is not.
    try testing.expect(tm.isVisible(io, 0, t1, 0)); // committed creator -> visible
    try testing.expect(!tm.isVisible(io, 0, t2, 0)); // aborted creator -> invisible
    try testing.expect(tm.isVisible(io, 0, t3, 0));
    // A row deleted by a committed tx is hidden; deleted by an aborted tx stays.
    try testing.expect(!tm.isVisible(io, 0, t1, t3)); // xmax committed -> hidden
    try testing.expect(tm.isVisible(io, 0, t1, t2)); // xmax aborted -> still visible

    // No active txns, so advancing the watermark folds every committed id below it
    // and clears the recent window - the exact prune the checkpoint relies on.
    tm.advanceWatermark(io);
    try testing.expect(tm.committed_below == tm.next_tx_id);
    try testing.expectEqual(@as(usize, 0), tm.committed_txns.count());

    // Every visibility answer must be IDENTICAL after the prune.
    try testing.expect(tm.isVisible(io, 0, t1, 0));
    try testing.expect(!tm.isVisible(io, 0, t2, 0)); // still hidden via `aborted`
    try testing.expect(tm.isVisible(io, 0, t3, 0));
    try testing.expect(!tm.isVisible(io, 0, t1, t3));
    try testing.expect(tm.isVisible(io, 0, t1, t2));
    try testing.expect(tm.isCommitted(io, t1));
    try testing.expect(!tm.isCommitted(io, t2));

    // A snapshot captured after the prune sees the same committed/aborted split.
    var snap = try tm.captureSnapshot(io, testing.allocator);
    defer snap.deinit();
    try testing.expect(tm.isVisibleIn(&snap, 0, t1, 0));
    try testing.expect(!tm.isVisibleIn(&snap, 0, t2, 0));
    try testing.expect(!tm.isVisibleIn(&snap, 0, t1, t3));
}
