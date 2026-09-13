const std = @import("std");
const results = @import("results.zig");
const BenchmarkResult = results.BenchmarkResult;

pub const ComparisonResult = struct {
    baseline: ResultInfo,
    candidate: ResultInfo,
    changes: PerformanceChanges,

    pub const ResultInfo = struct {
        name: []const u8,
        workload: []const u8,
        timestamp: i64,
    };

    pub const PerformanceChanges = struct {
        throughput_change_percent: f64,
        avg_latency_change_percent: f64,
        p50_latency_change_percent: f64,
        p95_latency_change_percent: f64,
        p99_latency_change_percent: f64,
        error_rate_change: f64,
        is_improvement: bool,
    };
};

pub const ComparisonTool = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ComparisonTool {
        return .{ .allocator = allocator };
    }

    pub fn compare(
        _: *ComparisonTool,
        baseline: BenchmarkResult,
        candidate: BenchmarkResult,
    ) ComparisonResult {
        const throughput_change = calculatePercentChange(
            baseline.summary.throughput_ops_sec,
            candidate.summary.throughput_ops_sec,
        );

        const avg_latency_change = calculatePercentChange(
            baseline.summary.avg_latency_us,
            candidate.summary.avg_latency_us,
        );

        const p50_change = calculatePercentChangeInt(
            baseline.summary.p50_latency_us,
            candidate.summary.p50_latency_us,
        );

        const p95_change = calculatePercentChangeInt(
            baseline.summary.p95_latency_us,
            candidate.summary.p95_latency_us,
        );

        const p99_change = calculatePercentChangeInt(
            baseline.summary.p99_latency_us,
            candidate.summary.p99_latency_us,
        );

        const error_rate_change = candidate.summary.error_rate_percent - baseline.summary.error_rate_percent;

        const is_improvement = (throughput_change > 5.0 or avg_latency_change < -5.0) and error_rate_change <= 0.1;

        return .{
            .baseline = .{
                .name = baseline.name,
                .workload = baseline.workload,
                .timestamp = baseline.timestamp,
            },
            .candidate = .{
                .name = candidate.name,
                .workload = candidate.workload,
                .timestamp = candidate.timestamp,
            },
            .changes = .{
                .throughput_change_percent = throughput_change,
                .avg_latency_change_percent = avg_latency_change,
                .p50_latency_change_percent = p50_change,
                .p95_latency_change_percent = p95_change,
                .p99_latency_change_percent = p99_change,
                .error_rate_change = error_rate_change,
                .is_improvement = is_improvement,
            },
        };
    }

    pub fn findRegressions(
        self: *ComparisonTool,
        baseline_results: []const BenchmarkResult,
        candidate_results: []const BenchmarkResult,
        threshold_percent: f64,
    ) ![]ComparisonResult {
        var regressions: std.ArrayList(ComparisonResult) = .empty;

        for (candidate_results) |candidate| {
            for (baseline_results) |baseline| {
                if (std.mem.eql(u8, baseline.workload, candidate.workload)) {
                    const cmp = self.compare(baseline, candidate);

                    if (cmp.changes.throughput_change_percent < -threshold_percent or
                        cmp.changes.avg_latency_change_percent > threshold_percent or
                        cmp.changes.p99_latency_change_percent > threshold_percent)
                    {
                        try regressions.append(self.allocator, cmp);
                    }
                    break;
                }
            }
        }

        return regressions.toOwnedSlice(self.allocator);
    }

    pub fn printReport(_: *ComparisonTool, cmp: ComparisonResult, writer: anytype) !void {
        try writer.writeAll("\n");
        try writer.writeAll("╔══════════════════════════════════════════════════════════════════╗\n");
        try writer.writeAll("║                    BENCHMARK COMPARISON                           ║\n");
        try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");

        try writer.print("║  Baseline:   {s:<51}  ║\n", .{cmp.baseline.name});
        try writer.print("║  Candidate:  {s:<51}  ║\n", .{cmp.candidate.name});
        try writer.print("║  Workload:   {s:<51}  ║\n", .{cmp.baseline.workload});

        try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");
        try writer.writeAll("║  PERFORMANCE CHANGES                                             ║\n");
        try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");

        const tp_indicator = if (cmp.changes.throughput_change_percent > 0) "+" else "";
        const tp_status = if (cmp.changes.throughput_change_percent > 5) "BETTER" else if (cmp.changes.throughput_change_percent < -5) "WORSE" else "SAME";
        try writer.print("║  Throughput:      {s}{d:.2}%  ({s}){s:<24}  ║\n", .{ tp_indicator, cmp.changes.throughput_change_percent, tp_status, "" });

        const lat_indicator = if (cmp.changes.avg_latency_change_percent > 0) "+" else "";
        const lat_status = if (cmp.changes.avg_latency_change_percent < -5) "BETTER" else if (cmp.changes.avg_latency_change_percent > 5) "WORSE" else "SAME";
        try writer.print("║  Avg Latency:     {s}{d:.2}%  ({s}){s:<24}  ║\n", .{ lat_indicator, cmp.changes.avg_latency_change_percent, lat_status, "" });

        const p50_indicator = if (cmp.changes.p50_latency_change_percent > 0) "+" else "";
        try writer.print("║  P50 Latency:     {s}{d:.2}%{s:<38}  ║\n", .{ p50_indicator, cmp.changes.p50_latency_change_percent, "" });

        const p95_indicator = if (cmp.changes.p95_latency_change_percent > 0) "+" else "";
        try writer.print("║  P95 Latency:     {s}{d:.2}%{s:<38}  ║\n", .{ p95_indicator, cmp.changes.p95_latency_change_percent, "" });

        const p99_indicator = if (cmp.changes.p99_latency_change_percent > 0) "+" else "";
        const p99_status = if (cmp.changes.p99_latency_change_percent < -5) "BETTER" else if (cmp.changes.p99_latency_change_percent > 5) "WORSE" else "SAME";
        try writer.print("║  P99 Latency:     {s}{d:.2}%  ({s}){s:<24}  ║\n", .{ p99_indicator, cmp.changes.p99_latency_change_percent, p99_status, "" });

        const err_indicator = if (cmp.changes.error_rate_change > 0) "+" else "";
        try writer.print("║  Error Rate:      {s}{d:.4}%{s:<37}  ║\n", .{ err_indicator, cmp.changes.error_rate_change, "" });

        try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");

        if (cmp.changes.is_improvement) {
            try writer.writeAll("║  VERDICT: IMPROVEMENT                                            ║\n");
        } else if (cmp.changes.throughput_change_percent < -5 or cmp.changes.avg_latency_change_percent > 5) {
            try writer.writeAll("║  VERDICT: REGRESSION                                             ║\n");
        } else {
            try writer.writeAll("║  VERDICT: NO SIGNIFICANT CHANGE                                  ║\n");
        }

        try writer.writeAll("╚══════════════════════════════════════════════════════════════════╝\n");
    }

    pub fn printReportDebug(_: *ComparisonTool, cmp: ComparisonResult) void {
        std.debug.print("\n", .{});
        std.debug.print("╔══════════════════════════════════════════════════════════════════╗\n", .{});
        std.debug.print("║                    BENCHMARK COMPARISON                           ║\n", .{});
        std.debug.print("╠══════════════════════════════════════════════════════════════════╣\n", .{});

        std.debug.print("║  Baseline:   {s:<51}  ║\n", .{cmp.baseline.name});
        std.debug.print("║  Candidate:  {s:<51}  ║\n", .{cmp.candidate.name});
        std.debug.print("║  Workload:   {s:<51}  ║\n", .{cmp.baseline.workload});

        std.debug.print("╠══════════════════════════════════════════════════════════════════╣\n", .{});
        std.debug.print("║  PERFORMANCE CHANGES                                             ║\n", .{});
        std.debug.print("╟──────────────────────────────────────────────────────────────────╢\n", .{});

        const tp_indicator = if (cmp.changes.throughput_change_percent > 0) "+" else "";
        const tp_status = if (cmp.changes.throughput_change_percent > 5) "BETTER" else if (cmp.changes.throughput_change_percent < -5) "WORSE" else "SAME";
        std.debug.print("║  Throughput:      {s}{d:.2}%  ({s})                             ║\n", .{ tp_indicator, cmp.changes.throughput_change_percent, tp_status });

        const lat_indicator = if (cmp.changes.avg_latency_change_percent > 0) "+" else "";
        const lat_status = if (cmp.changes.avg_latency_change_percent < -5) "BETTER" else if (cmp.changes.avg_latency_change_percent > 5) "WORSE" else "SAME";
        std.debug.print("║  Avg Latency:     {s}{d:.2}%  ({s})                             ║\n", .{ lat_indicator, cmp.changes.avg_latency_change_percent, lat_status });

        const p99_indicator = if (cmp.changes.p99_latency_change_percent > 0) "+" else "";
        const p99_status = if (cmp.changes.p99_latency_change_percent < -5) "BETTER" else if (cmp.changes.p99_latency_change_percent > 5) "WORSE" else "SAME";
        std.debug.print("║  P99 Latency:     {s}{d:.2}%  ({s})                             ║\n", .{ p99_indicator, cmp.changes.p99_latency_change_percent, p99_status });

        const err_indicator = if (cmp.changes.error_rate_change > 0) "+" else "";
        std.debug.print("║  Error Rate:      {s}{d:.4}%                                    ║\n", .{ err_indicator, cmp.changes.error_rate_change });

        std.debug.print("╠══════════════════════════════════════════════════════════════════╣\n", .{});

        if (cmp.changes.is_improvement) {
            std.debug.print("║  VERDICT: IMPROVEMENT                                            ║\n", .{});
        } else if (cmp.changes.throughput_change_percent < -5 or cmp.changes.avg_latency_change_percent > 5) {
            std.debug.print("║  VERDICT: REGRESSION                                             ║\n", .{});
        } else {
            std.debug.print("║  VERDICT: NO SIGNIFICANT CHANGE                                  ║\n", .{});
        }

        std.debug.print("╚══════════════════════════════════════════════════════════════════╝\n", .{});
    }

    pub fn printJsonReport(_: *ComparisonTool, cmp: ComparisonResult, writer: anytype) !void {
        try writer.writeAll("{\n");
        try writer.writeAll("  \"comparison\": {\n");
        try writer.print("    \"baseline\": \"{s}\",\n", .{cmp.baseline.name});
        try writer.print("    \"candidate\": \"{s}\",\n", .{cmp.candidate.name});
        try writer.print("    \"workload\": \"{s}\"\n", .{cmp.baseline.workload});
        try writer.writeAll("  },\n");
        try writer.writeAll("  \"changes\": {\n");
        try writer.print("    \"throughput_change_percent\": {d:.2},\n", .{cmp.changes.throughput_change_percent});
        try writer.print("    \"avg_latency_change_percent\": {d:.2},\n", .{cmp.changes.avg_latency_change_percent});
        try writer.print("    \"p50_latency_change_percent\": {d:.2},\n", .{cmp.changes.p50_latency_change_percent});
        try writer.print("    \"p95_latency_change_percent\": {d:.2},\n", .{cmp.changes.p95_latency_change_percent});
        try writer.print("    \"p99_latency_change_percent\": {d:.2},\n", .{cmp.changes.p99_latency_change_percent});
        try writer.print("    \"error_rate_change\": {d:.4},\n", .{cmp.changes.error_rate_change});
        try writer.print("    \"is_improvement\": {}\n", .{cmp.changes.is_improvement});
        try writer.writeAll("  }\n");
        try writer.writeAll("}\n");
    }

    pub fn loadResultFromFile(self: *ComparisonTool, path: []const u8) !BenchmarkResult {
        var threaded: std.Io.Threaded = .init(self.allocator, .{});
        defer threaded.deinit();
        const io = threaded.io();

        const content = try std.Io.Dir.readFileAlloc(.cwd(), io, path, self.allocator, .unlimited);
        defer self.allocator.free(content);

        return try parseJsonResult(self.allocator, content);
    }
};

fn calculatePercentChange(baseline: f64, candidate: f64) f64 {
    if (baseline == 0) return 0;
    return ((candidate - baseline) / baseline) * 100.0;
}

fn calculatePercentChangeInt(baseline: u64, candidate: u64) f64 {
    if (baseline == 0) return 0;
    const b: f64 = @floatFromInt(baseline);
    const c: f64 = @floatFromInt(candidate);
    return ((c - b) / b) * 100.0;
}

fn parseJsonResult(allocator: std.mem.Allocator, json_content: []const u8) !BenchmarkResult {
    _ = allocator;

    var result: BenchmarkResult = undefined;

    if (findJsonString(json_content, "name")) |name| {
        result.name = name;
    } else {
        result.name = "unknown";
    }

    if (findJsonString(json_content, "workload")) |workload| {
        result.workload = workload;
    } else {
        result.workload = "unknown";
    }

    result.timestamp = findJsonInt(json_content, "timestamp") orelse 0;
    result.duration_ms = findJsonInt(json_content, "duration_ms") orelse 0;

    result.config = .{
        .host = findJsonString(json_content, "host") orelse "127.0.0.1",
        .port = @intCast(findJsonInt(json_content, "port") orelse 23469),
        .record_count = @intCast(findJsonInt(json_content, "record_count") orelse 0),
        .operation_count = @intCast(findJsonInt(json_content, "operation_count") orelse 0),
        .document_size = @intCast(findJsonInt(json_content, "document_size") orelse 0),
        .thread_count = @intCast(findJsonInt(json_content, "thread_count") orelse 0),
        .warmup_ops = @intCast(findJsonInt(json_content, "warmup_ops") orelse 0),
    };

    result.summary = .{
        .total_ops = @intCast(findJsonInt(json_content, "total_ops") orelse 0),
        .successful_ops = @intCast(findJsonInt(json_content, "successful_ops") orelse 0),
        .failed_ops = @intCast(findJsonInt(json_content, "failed_ops") orelse 0),
        .throughput_ops_sec = findJsonFloat(json_content, "throughput_ops_sec") orelse 0,
        .avg_latency_us = findJsonFloat(json_content, "avg_latency_us") orelse 0,
        .min_latency_us = @intCast(findJsonInt(json_content, "min_latency_us") orelse 0),
        .max_latency_us = @intCast(findJsonInt(json_content, "max_latency_us") orelse 0),
        .p50_latency_us = @intCast(findJsonInt(json_content, "p50_latency_us") orelse 0),
        .p95_latency_us = @intCast(findJsonInt(json_content, "p95_latency_us") orelse 0),
        .p99_latency_us = @intCast(findJsonInt(json_content, "p99_latency_us") orelse 0),
        .p999_latency_us = @intCast(findJsonInt(json_content, "p999_latency_us") orelse 0),
        .error_rate_percent = findJsonFloat(json_content, "error_rate_percent") orelse 0,
    };

    result.per_operation = null;
    result.histogram = null;

    return result;
}

fn findJsonString(content: []const u8, key: []const u8) ?[]const u8 {
    const search_pattern = std.fmt.allocPrint(std.heap.page_allocator, "\"{s}\": \"", .{key}) catch return null;
    defer std.heap.page_allocator.free(search_pattern);

    if (std.mem.indexOf(u8, content, search_pattern)) |start| {
        const value_start = start + search_pattern.len;
        if (std.mem.indexOfScalarPos(u8, content, value_start, '"')) |end| {
            return content[value_start..end];
        }
    }
    return null;
}

fn findJsonInt(content: []const u8, key: []const u8) ?i64 {
    const search_pattern = std.fmt.allocPrint(std.heap.page_allocator, "\"{s}\": ", .{key}) catch return null;
    defer std.heap.page_allocator.free(search_pattern);

    if (std.mem.indexOf(u8, content, search_pattern)) |start| {
        const value_start = start + search_pattern.len;
        var end = value_start;
        while (end < content.len and (content[end] >= '0' and content[end] <= '9' or content[end] == '-')) {
            end += 1;
        }
        if (end > value_start) {
            return std.fmt.parseInt(i64, content[value_start..end], 10) catch null;
        }
    }
    return null;
}

fn findJsonFloat(content: []const u8, key: []const u8) ?f64 {
    const search_pattern = std.fmt.allocPrint(std.heap.page_allocator, "\"{s}\": ", .{key}) catch return null;
    defer std.heap.page_allocator.free(search_pattern);

    if (std.mem.indexOf(u8, content, search_pattern)) |start| {
        const value_start = start + search_pattern.len;
        var end = value_start;
        while (end < content.len and (content[end] >= '0' and content[end] <= '9' or content[end] == '.' or content[end] == '-' or content[end] == 'e' or content[end] == 'E' or content[end] == '+')) {
            end += 1;
        }
        if (end > value_start) {
            return std.fmt.parseFloat(f64, content[value_start..end]) catch null;
        }
    }
    return null;
}
