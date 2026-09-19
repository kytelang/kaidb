//! Common utilities facade: the single import surface for cross-cutting helpers.
//!
//! This module owns nothing of its own. It is a thin re-export barrel that
//! gathers the small, self-contained support types used throughout kaidb, so
//! that the rest of the engine (storage, durability, proto, schema) can write
//! `@import("common/utils.zig").StopWatch` instead of reaching into the exact
//! file each helper happens to live in. When `build.zig` wires the whole
//! `common/` directory as a named module, this file is that module's root, so
//! the names published here are the module's public API.
//!
//! Keeping the aggregation in one place means a helper can be moved between
//! files, or an implementation swapped, without every call site changing: only
//! the alias on the corresponding line below moves. That is the whole reason
//! the barrel exists, and why it deliberately holds no logic of its own, every
//! declaration here is a `pub const` alias, so there is nothing to test or
//! break in this file itself.
//!
//! The helpers fall into a few loose groups: timing and clocks
//! ([`DateTime`], [`parseIso`], [`Now`], [`StopWatch`]), concurrency
//! ([`Mutex`], [`sync`], the latter carrying the `GroupLock` that gates
//! per-table access), and a small server helper ([`StaticContentStore`]).

const std = @import("std");

/// Calendar date-and-time value with parsing and formatting.
///
/// Re-exported from `datetime.zig`. This is the engine's representation for the
/// SQL `DATE`/`TIMESTAMP` family and for anything that has to render or compare
/// wall-clock instants. Parse raw ISO-8601 text into one with [`parseIso`].
pub const DateTime = @import("datetime.zig").DateTime;

/// Parse an ISO-8601 string into a [`DateTime`].
///
/// Re-exported from `datetime.zig`. The counterpart to [`DateTime`] used when
/// accepting date/time literals from SQL text or the wire protocol; it returns
/// an error for input that is not valid ISO-8601 rather than guessing.
pub const parseIso = @import("datetime.zig").parseIso;

/// Read the current instant from the system clock.
///
/// Re-exported from `time.zig`. Centralising the "what time is it now" call
/// behind one name keeps clock access consistent (and mockable in one place)
/// across MVCC timestamping, WAL/checkpoint bookkeeping, and metrics.
pub const Now = @import("time.zig").Now;

/// Monotonic elapsed-time timer for measuring durations.
///
/// Re-exported from `stopwatch.zig`. Used for latency and throughput
/// measurement; unlike [`Now`] it is intended for measuring intervals, so it is
/// backed by a monotonic source and is unaffected by wall-clock adjustments.
pub const StopWatch = @import("stopwatch.zig").StopWatch;

/// Standard-library async mutex, aliased for a stable local name.
///
/// This points at `std.Io.Mutex` so call sites depend on
/// `common/utils.Mutex` rather than the exact std path; if the engine's mutex
/// choice ever changes, only this line moves. For per-table access control see
/// the `GroupLock` in [`sync`] instead, which is kaidb's own primitive.
pub const Mutex = std.Io.Mutex;

/// In-memory store of static assets served by the built-in HTTP surface.
///
/// Re-exported from `static_content.zig`. Holds preloaded static content
/// (for example the admin/status pages) so the server can answer those
/// requests without touching the storage engine or the filesystem per request.
pub const StaticContentStore = @import("static_content.zig").StaticContentStore;

/// Concurrency primitives module, notably the per-table `GroupLock`.
///
/// Re-exported whole from `sync.zig` (a module alias, not a single type). This
/// is where kaidb's own locking lives: the `GroupLock` that gates per-table
/// access (SELECT in read mode with concurrent readers, INSERT in write mode
/// with concurrent writers, UPDATE/DELETE exclusive), as described in
/// `architecture.md`. Import it as `utils.sync.GroupLock`.
pub const sync = @import("sync.zig");
