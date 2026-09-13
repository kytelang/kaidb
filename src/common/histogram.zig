//! A lock-free, Prometheus-shaped latency histogram.
//!
//! Observations are recorded in nanoseconds; the exported buckets follow the
//! Prometheus convention (cumulative `le` buckets in seconds, plus `_sum` and
//! `_count`). Every field is an atomic counter so any connection thread can
//! observe concurrently without a lock, and the `/metrics` reader sees a
//! monotonically consistent-enough snapshot (Prometheus tolerates the tiny skew
//! between buckets that a lock-free read implies).
//!
//! The bucket boundaries are fixed at comptime and cover roughly half a
//! millisecond to ten seconds, which spans the range from an in-RAM point read
//! to a slow, contended write on this engine.

const std = @import("std");

pub const LatencyHistogram = struct {
    /// Cumulative upper bounds, in seconds, matching the `le` labels emitted.
    /// An implicit `+Inf` bucket is represented by `count`.
    pub const bound_seconds = [_]f64{
        0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0,
    };
    /// Pre-rendered `le` label strings, parallel to `bound_seconds`, so the
    /// exporter never has to format a float (avoids locale/precision surprises).
    pub const bound_labels = [_][]const u8{
        "0.0005", "0.001", "0.0025", "0.005", "0.01", "0.025", "0.05", "0.1", "0.25", "0.5", "1", "2.5", "5", "10",
    };
    /// The same bounds in integer nanoseconds, for a branch-free integer compare
    /// on the hot observe path.
    const bound_ns = blk: {
        var arr: [bound_seconds.len]u64 = undefined;
        for (bound_seconds, 0..) |s, i| arr[i] = @intFromFloat(s * 1_000_000_000.0);
        break :blk arr;
    };

    /// Cumulative per-bucket counts: `buckets[i]` counts every observation whose
    /// latency is <= `bound_ns[i]`.
    buckets: [bound_seconds.len]std.atomic.Value(u64) =
        [_]std.atomic.Value(u64){std.atomic.Value(u64).init(0)} ** bound_seconds.len,
    /// Sum of all observed latencies, in nanoseconds.
    sum_ns: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Total number of observations (the implicit `+Inf` bucket).
    count: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Records one observation of `ns` nanoseconds. Lock-free.
    pub fn observeNs(self: *LatencyHistogram, ns: u64) void {
        _ = self.count.fetchAdd(1, .monotonic);
        _ = self.sum_ns.fetchAdd(ns, .monotonic);
        inline for (bound_ns, 0..) |b, i| {
            if (ns <= b) _ = self.buckets[i].fetchAdd(1, .monotonic);
        }
    }

    /// Appends the Prometheus text exposition for this histogram to `w`, named
    /// `metric` (e.g. `"kaidb_query_duration_seconds"`), with `help` as the HELP
    /// line. Emits the cumulative `le` buckets, the `+Inf` bucket, `_sum` (in
    /// seconds), and `_count`.
    pub fn writeProm(self: *const LatencyHistogram, w: anytype, comptime metric: []const u8, comptime help: []const u8) !void {
        try w.print("# HELP " ++ metric ++ " " ++ help ++ "\n", .{});
        try w.print("# TYPE " ++ metric ++ " histogram\n", .{});
        inline for (bound_labels, 0..) |label, i| {
            try w.print(metric ++ "_bucket{{le=\"" ++ label ++ "\"}} {d}\n", .{self.buckets[i].load(.monotonic)});
        }
        const total = self.count.load(.monotonic);
        try w.print(metric ++ "_bucket{{le=\"+Inf\"}} {d}\n", .{total});
        const sum_seconds = @as(f64, @floatFromInt(self.sum_ns.load(.monotonic))) / 1_000_000_000.0;
        try w.print(metric ++ "_sum {d:.6}\n", .{sum_seconds});
        try w.print(metric ++ "_count {d}\n", .{total});
    }
};

test "cumulative buckets and count" {
    var h = LatencyHistogram{};
    h.observeNs(300_000); // 0.3 ms  -> falls in the 0.0005s bucket and up
    h.observeNs(3_000_000); // 3 ms  -> 0.005s bucket and up
    h.observeNs(20_000_000_000); // 20 s -> only +Inf
    try std.testing.expectEqual(@as(u64, 3), h.count.load(.monotonic));
    // le=0.0005 (500us): only the 0.3ms observation.
    try std.testing.expectEqual(@as(u64, 1), h.buckets[0].load(.monotonic));
    // le=0.005 (5ms): the 0.3ms and 3ms observations.
    try std.testing.expectEqual(@as(u64, 2), h.buckets[3].load(.monotonic));
    // le=10s: still only 2 (the 20s one exceeds every finite bound).
    try std.testing.expectEqual(@as(u64, 2), h.buckets[LatencyHistogram.bound_labels.len - 1].load(.monotonic));
}
