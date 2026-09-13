//! Wall-clock time source, expressed as a thin wrapper over Zig's `std.Io`
//! clock facility.
//!
//! NovaDB threads I/O capability explicitly rather than reaching for global
//! syscalls, so reading the clock is also routed through an [`Io`] handle
//! instead of calling `std.time.milliTimestamp` directly. This file exists to
//! give the rest of the engine a single, ergonomic entry point for "what time
//! is it right now" that carries its `Io` dependency with it: a caller holds a
//! [`Now`] value (constructed from whatever `Io` it already has) and asks for
//! the current instant in whichever unit it needs.
//!
//! All three accessors read the `.real` (real-time / wall-clock) clock, i.e.
//! calendar time that can jump when the system clock is adjusted, NOT a
//! monotonic clock. That is deliberate: the values here feed things that must
//! agree with the outside world (timestamps, TTLs, log/record times), not
//! interval measurement. Code that needs a gap-free elapsed duration must use a
//! monotonic source elsewhere, not [`Now`].
//!
//! The unit conversions come straight from the underlying `Io.Clock` instant,
//! so they share its epoch (the Unix epoch for the real clock) and its
//! resolution. Nanoseconds are returned as `i96` because a signed 64-bit count
//! of nanoseconds since the epoch would overflow within the range of dates the
//! engine may legitimately handle; the wider integer keeps the full-resolution
//! value exact.

const std = @import("std");
/// Local alias for `std.Io`, the capability handle through which all timing
/// (and other I/O) is performed. Kept as a shorthand so the [`Now`] methods can
/// spell `Io.Clock` without repeating the `std.` prefix.
const Io = std.Io;

/// A wall-clock reader bound to a specific [`Io`] capability.
///
/// [`Now`] is a value type holding only the `io` handle; constructing one is
/// free and it can be copied freely. Each accessor samples the current real-time
/// instant at the moment it is called, so a single [`Now`] can be reused to take
/// many independent readings. Because it carries its own `Io`, callers can pass
/// a time source around without also plumbing the raw capability through every
/// signature.
pub const Now = struct {
    /// The I/O capability used to reach the system clock. Every accessor routes
    /// its `Io.Clock.now(.real, ...)` call through this handle rather than a
    /// global syscall, which is what lets timing be injected/redirected in the
    /// same way as the rest of NovaDB's I/O.
    io: Io,

    /// Current real-time instant in milliseconds since the Unix epoch.
    ///
    /// Fits in `i64` comfortably for any realistic date, so this is the
    /// convenient default for timestamps and TTL bookkeeping. Reflects wall-clock
    /// time, so the result can move backwards if the system clock is set back;
    /// do not use it to measure elapsed intervals.
    pub fn toMilliSeconds(self: Now) i64 {
        return Io.Clock.now(.real, self.io).toMilliseconds();
    }

    /// Current real-time instant in whole seconds since the Unix epoch.
    ///
    /// Truncates sub-second precision. Same wall-clock caveat as
    /// [`Now.toMilliSeconds`]: this tracks calendar time, not a monotonic
    /// counter.
    pub fn toSeconds(self: Now) i64 {
        return Io.Clock.now(.real, self.io).toSeconds();
    }

    /// Current real-time instant in nanoseconds since the Unix epoch.
    ///
    /// Returned as `i96` rather than `i64` on purpose: a signed 64-bit
    /// nanosecond count overflows a little after the year 2262, which is inside
    /// the range of dates the engine may store, so the wider integer preserves
    /// the exact full-resolution value. Use this when nanosecond granularity
    /// matters; prefer [`Now.toMilliSeconds`] otherwise.
    pub fn toNanoSeconds(self: Now) i96 {
        return Io.Clock.now(.real, self.io).toNanoseconds();
    }
};
