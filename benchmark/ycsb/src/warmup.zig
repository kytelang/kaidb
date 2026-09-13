const std = @import("std");
const metrics_mod = @import("metrics.zig");
const Metrics = metrics_mod.Metrics;
const milliTimestamp = metrics_mod.milliTimestamp;

pub const WarmupManager = struct {
    allocator: std.mem.Allocator,
    config: WarmupConfig,

    warmup_complete: bool,
    measurement_started: bool,
    warmup_start_time: i64,
    warmup_ops_count: u64,

    window_throughputs: std.ArrayList(f64),
    current_window_start: i64,
    current_window_ops: u64,
    steady_state_detected: bool,

    pub const WarmupConfig = struct {
        warmup_ops: u64 = 1000,
        warmup_seconds: u32 = 10,
        measurement_seconds: u32 = 60,
        steady_state_window_count: u32 = 10,
        steady_state_threshold: f64 = 0.05,
        window_duration_ms: u64 = 1000,
    };

    pub fn init(allocator: std.mem.Allocator, config: WarmupConfig) !*WarmupManager {
        const manager = try allocator.create(WarmupManager);
        manager.* = .{
            .allocator = allocator,
            .config = config,
            .warmup_complete = false,
            .measurement_started = false,
            .warmup_start_time = 0,
            .warmup_ops_count = 0,
            .window_throughputs = .empty,
            .current_window_start = 0,
            .current_window_ops = 0,
            .steady_state_detected = false,
        };
        return manager;
    }

    pub fn deinit(self: *WarmupManager) void {
        self.window_throughputs.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn startWarmup(self: *WarmupManager) void {
        self.warmup_start_time = milliTimestamp();
        self.warmup_complete = false;
        self.measurement_started = false;
        self.warmup_ops_count = 0;
        self.current_window_start = self.warmup_start_time;
        self.current_window_ops = 0;
    }

    pub fn recordOperation(self: *WarmupManager) bool {
        const now = milliTimestamp();

        if (!self.warmup_complete) {
            self.warmup_ops_count += 1;

            const elapsed_ms = now - self.warmup_start_time;
            const elapsed_s = @as(u32, @intCast(@divTrunc(elapsed_ms, 1000)));

            if (self.warmup_ops_count >= self.config.warmup_ops or
                elapsed_s >= self.config.warmup_seconds)
            {
                self.warmup_complete = true;
                self.measurement_started = true;
                self.current_window_start = now;
                self.current_window_ops = 0;
                return true;
            }
            return true;
        }

        self.current_window_ops += 1;

        const window_elapsed = now - self.current_window_start;
        if (window_elapsed >= @as(i64, @intCast(self.config.window_duration_ms))) {
            const window_seconds = @as(f64, @floatFromInt(window_elapsed)) / 1000.0;
            const window_throughput = @as(f64, @floatFromInt(self.current_window_ops)) / window_seconds;

            self.window_throughputs.append(self.allocator, window_throughput) catch {};

            self.checkSteadyState();

            self.current_window_start = now;
            self.current_window_ops = 0;
        }

        return false;
    }

    fn checkSteadyState(self: *WarmupManager) void {
        if (self.window_throughputs.items.len < self.config.steady_state_window_count) {
            return;
        }

        const start_idx = self.window_throughputs.items.len - self.config.steady_state_window_count;
        const recent_windows = self.window_throughputs.items[start_idx..];

        var sum: f64 = 0;
        for (recent_windows) |tp| {
            sum += tp;
        }
        const mean = sum / @as(f64, @floatFromInt(recent_windows.len));

        var variance_sum: f64 = 0;
        for (recent_windows) |tp| {
            const diff = tp - mean;
            variance_sum += diff * diff;
        }
        const variance = variance_sum / @as(f64, @floatFromInt(recent_windows.len));
        const std_dev = @sqrt(variance);
        const coeff_of_variation = if (mean > 0) std_dev / mean else 1.0;

        if (coeff_of_variation < self.config.steady_state_threshold) {
            self.steady_state_detected = true;
        }
    }

    pub fn isWarmupComplete(self: *WarmupManager) bool {
        return self.warmup_complete;
    }

    pub fn isSteadyState(self: *WarmupManager) bool {
        return self.steady_state_detected;
    }

    pub fn getSteadyStateThroughput(self: *WarmupManager) ?f64 {
        if (!self.steady_state_detected) return null;
        if (self.window_throughputs.items.len < self.config.steady_state_window_count) return null;

        const start_idx = self.window_throughputs.items.len - self.config.steady_state_window_count;
        const recent_windows = self.window_throughputs.items[start_idx..];

        var sum: f64 = 0;
        for (recent_windows) |tp| {
            sum += tp;
        }
        return sum / @as(f64, @floatFromInt(recent_windows.len));
    }

    pub fn getWarmupStats(self: *WarmupManager) WarmupStats {
        const warmup_duration_ms = if (self.warmup_complete)
            (if (self.measurement_started) milliTimestamp() - self.warmup_start_time else 0)
        else
            milliTimestamp() - self.warmup_start_time;

        return .{
            .warmup_ops = self.warmup_ops_count,
            .warmup_duration_ms = @intCast(@max(0, warmup_duration_ms)),
            .warmup_complete = self.warmup_complete,
            .steady_state_detected = self.steady_state_detected,
            .measurement_windows = @intCast(self.window_throughputs.items.len),
            .steady_state_throughput = self.getSteadyStateThroughput(),
        };
    }

    pub const WarmupStats = struct {
        warmup_ops: u64,
        warmup_duration_ms: u64,
        warmup_complete: bool,
        steady_state_detected: bool,
        measurement_windows: u32,
        steady_state_throughput: ?f64,
    };

    pub fn printReport(self: *WarmupManager, writer: anytype) !void {
        const stats = self.getWarmupStats();

        try writer.writeAll("\n=== Warmup & Steady State Report ===\n");
        try writer.print("Warmup Operations:    {d}\n", .{stats.warmup_ops});
        try writer.print("Warmup Duration:      {d} ms\n", .{stats.warmup_duration_ms});
        try writer.print("Warmup Complete:      {}\n", .{stats.warmup_complete});
        try writer.print("Measurement Windows:  {d}\n", .{stats.measurement_windows});
        try writer.print("Steady State:         {}\n", .{stats.steady_state_detected});

        if (stats.steady_state_throughput) |tp| {
            try writer.print("Steady Throughput:    {d:.2} ops/sec\n", .{tp});
        }

        if (self.window_throughputs.items.len > 0) {
            try writer.writeAll("\n--- Throughput Trend (ops/sec) ---\n");
            const max_display: usize = 20;
            const step = if (self.window_throughputs.items.len > max_display)
                self.window_throughputs.items.len / max_display
            else
                1;

            var i: usize = 0;
            while (i < self.window_throughputs.items.len) : (i += step) {
                const tp = self.window_throughputs.items[i];
                const bar_len = @min(40, @as(usize, @intFromFloat(tp / 1000.0)));
                var bar_buf: [41]u8 = undefined;
                for (0..bar_len) |j| bar_buf[j] = '#';
                for (bar_len..40) |j| bar_buf[j] = ' ';
                bar_buf[40] = 0;
                try writer.print("  {d:>4}: {s} {d:.0}\n", .{ i, bar_buf[0..40], tp });
            }
        }

        try writer.writeAll("=====================================\n");
    }

    pub fn printReportDebug(self: *WarmupManager) void {
        const stats = self.getWarmupStats();

        std.debug.print("\n=== Warmup & Steady State Report ===\n", .{});
        std.debug.print("Warmup Operations:    {d}\n", .{stats.warmup_ops});
        std.debug.print("Warmup Duration:      {d} ms\n", .{stats.warmup_duration_ms});
        std.debug.print("Warmup Complete:      {}\n", .{stats.warmup_complete});
        std.debug.print("Measurement Windows:  {d}\n", .{stats.measurement_windows});
        std.debug.print("Steady State:         {}\n", .{stats.steady_state_detected});

        if (stats.steady_state_throughput) |tp| {
            std.debug.print("Steady Throughput:    {d:.2} ops/sec\n", .{tp});
        }

        std.debug.print("=====================================\n", .{});
    }
};

pub const FilteredMetrics = struct {
    allocator: std.mem.Allocator,
    warmup_metrics: *Metrics,
    measurement_metrics: *Metrics,
    warmup_manager: *WarmupManager,

    pub fn init(allocator: std.mem.Allocator, warmup_config: WarmupManager.WarmupConfig) !*FilteredMetrics {
        const filtered = try allocator.create(FilteredMetrics);
        filtered.* = .{
            .allocator = allocator,
            .warmup_metrics = try Metrics.init(allocator),
            .measurement_metrics = try Metrics.init(allocator),
            .warmup_manager = try WarmupManager.init(allocator, warmup_config),
        };
        return filtered;
    }

    pub fn deinit(self: *FilteredMetrics) void {
        self.warmup_metrics.deinit();
        self.measurement_metrics.deinit();
        self.warmup_manager.deinit();
        self.allocator.destroy(self);
    }

    pub fn start(self: *FilteredMetrics) void {
        self.warmup_metrics.start();
        self.warmup_manager.startWarmup();
    }

    pub fn stop(self: *FilteredMetrics) void {
        if (self.warmup_manager.isWarmupComplete()) {
            self.measurement_metrics.stop();
        } else {
            self.warmup_metrics.stop();
        }
    }

    pub fn recordSuccess(self: *FilteredMetrics, latency_us: u64) !void {
        const is_warmup = self.warmup_manager.recordOperation();

        if (is_warmup) {
            try self.warmup_metrics.recordSuccess(latency_us);
        } else {
            if (self.measurement_metrics.start_time == 0) {
                self.measurement_metrics.start();
            }
            try self.measurement_metrics.recordSuccess(latency_us);
        }
    }

    pub fn recordFailure(self: *FilteredMetrics) void {
        if (self.warmup_manager.isWarmupComplete()) {
            self.measurement_metrics.recordFailure();
        } else {
            self.warmup_metrics.recordFailure();
        }
    }

    pub fn getMeasurementMetrics(self: *FilteredMetrics) *Metrics {
        return self.measurement_metrics;
    }

    pub fn getWarmupMetrics(self: *FilteredMetrics) *Metrics {
        return self.warmup_metrics;
    }

    pub fn printReport(self: *FilteredMetrics, title: []const u8) void {
        self.warmup_manager.printReportDebug();

        self.measurement_metrics.printReport(title);
    }
};
