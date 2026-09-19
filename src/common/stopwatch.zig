//! A minimal monotonic-clock stopwatch for coarse timing inside kaidb.
//!
//! This is a tiny instrumentation helper, not a database subsystem. It exists
//! so that engine code (query executor stages, WAL flush timing, checkpoint
//! durations, benchmark harnesses) can accumulate wall-clock intervals without
//! each call site having to reach into `std.Io.Clock` and do the nanosecond
//! bookkeeping by hand.
//!
//! Design decisions and invariants:
//!
//!   * The clock read is [`std.Io.Clock.now`] on the `.awake` clock, i.e. a
//!     MONOTONIC source that does not count time the machine spends suspended
//!     and is immune to wall-clock adjustments (NTP steps, manual clock
//!     changes). That is what you want for measuring elapsed durations, since a
//!     backwards jump on the real-time clock could otherwise produce a negative
//!     interval.
//!
//!   * Timing is accumulative. [`StopWatch.start`] and [`StopWatch.stop`] can
//!     be called repeatedly and each start/stop pair ADDS its interval to a
//!     running total, so the same watch can time several disjoint spans (for
//!     example, the time spent inside a hot loop body across many iterations)
//!     and report the sum. [`StopWatch.reset`] clears the total back to zero.
//!
//!   * The `Io` handle is threaded in per call rather than captured, because
//!     kaidb's I/O abstraction (`std.Io`) is passed explicitly through the
//!     call graph rather than held as global state.
//!
//! It is deliberately allocation-free and holds only two integers, so it is
//! cheap to embed by value in any struct that wants a running timer.

const std = @import("std");
/// The standard I/O interface type, aliased for brevity. Instances are the
/// `io` handle passed to [`StopWatch.start`] and [`StopWatch.stop`], and its
/// nested `Clock` provides the monotonic time source.
const Io = std.Io;

/// An accumulating monotonic stopwatch measured in nanoseconds.
///
/// A watch is either RUNNING (`start_ns` holds the timestamp of the most
/// recent [`StopWatch.start`]) or STOPPED (`start_ns` is `null`). Elapsed time
/// is only folded into the running total on [`StopWatch.stop`], so the
/// `elapsed*` accessors report the total across all completed start/stop spans
/// and do NOT include the currently in-flight span while the watch is running.
///
/// The struct is trivially copyable and default-constructs to a stopped watch
/// with a zero total, so `StopWatch{}` is a ready-to-use, never-started timer.
pub const StopWatch = struct {
    /// Timestamp (monotonic nanoseconds) captured at the last [`StopWatch.start`],
    /// or `null` when the watch is not currently running. Used as the lower
    /// bound of the next interval computed by [`StopWatch.stop`].
    start_ns: ?i128 = null,
    /// Running total of all completed intervals, in nanoseconds. Signed and
    /// 128-bit to match the clock's `toNanoseconds()` return type and to give
    /// the subtraction in [`StopWatch.stop`] head-room; in practice it is
    /// always non-negative because the `.awake` clock is monotonic.
    elapsed_ns: i128 = 0,

    /// Begins (or resumes) timing by recording the current monotonic timestamp.
    ///
    /// Reads the `.awake` clock via `io` and stores it in [`StopWatch.start_ns`].
    /// Calling `start` again without an intervening [`StopWatch.stop`] simply
    /// overwrites the stored timestamp, discarding the not-yet-closed span, so a
    /// start must be paired with a stop for its interval to count.
    pub fn start(self: *StopWatch, io: Io) void {
        self.start_ns = Io.Clock.now(.awake, io).toNanoseconds();
    }

    /// Closes the current timing span and adds its duration to the total.
    ///
    /// If the watch is running, reads the clock again, adds `now - start_ns` to
    /// [`StopWatch.elapsed_ns`], and clears `start_ns` so the watch is stopped.
    /// If the watch was not running (no matching [`StopWatch.start`]), this is a
    /// no-op, which makes an unbalanced stop harmless rather than corrupting the
    /// total with a bogus interval.
    pub fn stop(self: *StopWatch, io: Io) void {
        if (self.start_ns) |begin| {
            const now: i128 = Io.Clock.now(.awake, io).toNanoseconds();
            self.elapsed_ns += now - begin;
            self.start_ns = null;
        }
    }

    /// Clears the accumulated total and stops the watch.
    ///
    /// Returns the watch to its default state (`start_ns = null`,
    /// `elapsed_ns = 0`) so it can be reused for a fresh measurement. Takes no
    /// `io` because it reads no clock.
    pub fn reset(self: *StopWatch) void {
        self.start_ns = null;
        self.elapsed_ns = 0;
    }

    /// Returns the accumulated total in whole nanoseconds.
    ///
    /// The stored total is narrowed from `i128` to `u64` via `@intCast`, which
    /// asserts the value fits and is non-negative in a safety-checked build.
    /// That holds for any realistic measurement (a `u64` of nanoseconds spans
    /// ~584 years) given the monotonic clock never yields a negative interval.
    pub fn elapsedNs(self: *const StopWatch) u64 {
        return @intCast(self.elapsed_ns);
    }

    /// Returns the accumulated total in microseconds as a floating-point value.
    ///
    /// Uses floating-point division so sub-microsecond resolution is preserved
    /// (the fractional part carries the remaining nanoseconds), unlike integer
    /// division which would truncate.
    pub fn elapsedUs(self: *const StopWatch) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000.0;
    }

    /// Returns the accumulated total in milliseconds as a floating-point value.
    ///
    /// See [`StopWatch.elapsedUs`]: the fractional part retains sub-millisecond
    /// precision down to the nanosecond.
    pub fn elapsedMs(self: *const StopWatch) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000.0;
    }

    /// Returns the accumulated total in seconds as a floating-point value.
    ///
    /// Convenient for human-readable reports; the fractional part retains full
    /// nanosecond precision. See [`StopWatch.elapsedUs`].
    pub fn elapsedS(self: *const StopWatch) f64 {
        return @as(f64, @floatFromInt(self.elapsed_ns)) / 1_000_000_000.0;
    }
};
