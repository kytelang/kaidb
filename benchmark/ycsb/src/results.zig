const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const Metrics = @import("metrics.zig").Metrics;
const MetricsTracker = @import("metrics.zig").MetricsTracker;
const milliTimestamp = @import("metrics.zig").milliTimestamp;
const OperationType = @import("operation_chooser.zig").OperationType;
const config_mod = @import("config.zig");

pub const BenchmarkResult = struct {
    name: []const u8,
    workload: []const u8,
    timestamp: i64,
    duration_ms: i64,
    config: ResultConfig,
    summary: ResultSummary,
    per_operation: ?PerOperationResults,
    histogram: ?LatencyHistogram,

    pub const ResultConfig = struct {
        host: []const u8,
        port: u16,
        record_count: u64,
        operation_count: u64,
        document_size: u32,
        thread_count: u32,
        warmup_ops: u64,
    };

    pub const ResultSummary = struct {
        total_ops: u64,
        successful_ops: u64,
        failed_ops: u64,
        throughput_ops_sec: f64,
        avg_latency_us: f64,
        min_latency_us: u64,
        max_latency_us: u64,
        p50_latency_us: u64,
        p95_latency_us: u64,
        p99_latency_us: u64,
        p999_latency_us: u64,
        error_rate_percent: f64,
    };

    pub const PerOperationResults = struct {
        read: ?OperationStats,
        insert: ?OperationStats,
        update: ?OperationStats,
        delete: ?OperationStats,
        scan: ?OperationStats,
        read_modify_write: ?OperationStats,
    };

    pub const OperationStats = struct {
        total_ops: u64,
        successful_ops: u64,
        failed_ops: u64,
        avg_latency_us: f64,
        min_latency_us: u64,
        max_latency_us: u64,
        p50_latency_us: u64,
        p95_latency_us: u64,
        p99_latency_us: u64,
        p999_latency_us: u64,
    };

    pub const LatencyHistogram = struct {
        buckets: []const HistogramBucket,
        total_count: u64,
    };

    pub const HistogramBucket = struct {
        lower_bound_us: u64,
        upper_bound_us: u64,
        count: u64,
        cumulative_percent: f64,
    };
};

pub const ResultExporter = struct {
    allocator: std.mem.Allocator,
    io: Io,

    pub fn init(allocator: std.mem.Allocator, io: Io) ResultExporter {
        return .{ .allocator = allocator, .io = io };
    }

    pub fn fromMetrics(
        self: *ResultExporter,
        metrics: *Metrics,
        name: []const u8,
        workload: []const u8,
        cfg: config_mod.BenchmarkConfig,
    ) !BenchmarkResult {
        const total = metrics.total_ops.load(.monotonic);
        const successful = metrics.successful_ops.load(.monotonic);
        const failed = metrics.failed_ops.load(.monotonic);
        const error_rate = if (total > 0)
            @as(f64, @floatFromInt(failed)) / @as(f64, @floatFromInt(total)) * 100.0
        else
            0.0;

        const histogram = try self.buildHistogram(metrics);

        return BenchmarkResult{
            .name = name,
            .workload = workload,
            .timestamp = milliTimestamp(),
            .duration_ms = metrics.durationMs(),
            .config = .{
                .host = cfg.host,
                .port = cfg.port,
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .thread_count = cfg.thread_count,
                .warmup_ops = cfg.warmup_ops,
            },
            .summary = .{
                .total_ops = total,
                .successful_ops = successful,
                .failed_ops = failed,
                .throughput_ops_sec = metrics.throughput(),
                .avg_latency_us = metrics.avgLatency(),
                .min_latency_us = metrics.min_latency.load(.monotonic),
                .max_latency_us = metrics.max_latency.load(.monotonic),
                .p50_latency_us = metrics.percentile(0.50),
                .p95_latency_us = metrics.percentile(0.95),
                .p99_latency_us = metrics.percentile(0.99),
                .p999_latency_us = metrics.percentile(0.999),
                .error_rate_percent = error_rate,
            },
            .per_operation = null,
            .histogram = histogram,
        };
    }

    pub fn fromMetricsTracker(
        self: *ResultExporter,
        tracker: *MetricsTracker,
        overall_metrics: *Metrics,
        name: []const u8,
        workload: []const u8,
        cfg: config_mod.BenchmarkConfig,
    ) !BenchmarkResult {
        var result = try self.fromMetrics(overall_metrics, name, workload, cfg);

        result.per_operation = .{
            .read = if (tracker.read_metrics) |m| try self.getOpStats(m) else null,
            .insert = if (tracker.insert_metrics) |m| try self.getOpStats(m) else null,
            .update = if (tracker.update_metrics) |m| try self.getOpStats(m) else null,
            .delete = if (tracker.delete_metrics) |m| try self.getOpStats(m) else null,
            .scan = if (tracker.scan_metrics) |m| try self.getOpStats(m) else null,
            .read_modify_write = if (tracker.rmw_metrics) |m| try self.getOpStats(m) else null,
        };

        return result;
    }

    fn getOpStats(_: *ResultExporter, m: *Metrics) !BenchmarkResult.OperationStats {
        return .{
            .total_ops = m.total_ops.load(.monotonic),
            .successful_ops = m.successful_ops.load(.monotonic),
            .failed_ops = m.failed_ops.load(.monotonic),
            .avg_latency_us = m.avgLatency(),
            .min_latency_us = m.min_latency.load(.monotonic),
            .max_latency_us = m.max_latency.load(.monotonic),
            .p50_latency_us = m.percentile(0.50),
            .p95_latency_us = m.percentile(0.95),
            .p99_latency_us = m.percentile(0.99),
            .p999_latency_us = m.percentile(0.999),
        };
    }

    fn buildHistogram(self: *ResultExporter, metrics: *Metrics) !?BenchmarkResult.LatencyHistogram {
        if (metrics.latencies.items.len == 0) return null;

        const bucket_bounds = [_]u64{
            0,
            100,
            250,
            500,
            1000,
            2500,
            5000,
            10000,
            25000,
            50000,
            100000,
            250000,
            500000,
            1000000,
            std.math.maxInt(u64),
        };

        var buckets = try self.allocator.alloc(BenchmarkResult.HistogramBucket, bucket_bounds.len - 1);
        var counts = try self.allocator.alloc(u64, bucket_bounds.len - 1);
        defer self.allocator.free(counts);

        for (counts) |*c| c.* = 0;

        for (metrics.latencies.items) |latency| {
            for (0..bucket_bounds.len - 1) |i| {
                if (latency >= bucket_bounds[i] and latency < bucket_bounds[i + 1]) {
                    counts[i] += 1;
                    break;
                }
            }
        }

        const total: f64 = @floatFromInt(metrics.latencies.items.len);
        var cumulative: u64 = 0;
        for (0..bucket_bounds.len - 1) |i| {
            cumulative += counts[i];
            buckets[i] = .{
                .lower_bound_us = bucket_bounds[i],
                .upper_bound_us = bucket_bounds[i + 1],
                .count = counts[i],
                .cumulative_percent = @as(f64, @floatFromInt(cumulative)) / total * 100.0,
            };
        }

        return .{
            .buckets = buckets,
            .total_count = @intCast(metrics.latencies.items.len),
        };
    }

    pub fn exportJson(_: *ResultExporter, result: BenchmarkResult, writer: anytype) !void {
        try writer.writeAll("{\n");

        try writer.print("  \"name\": \"{s}\",\n", .{result.name});
        try writer.print("  \"workload\": \"{s}\",\n", .{result.workload});
        try writer.print("  \"timestamp\": {d},\n", .{result.timestamp});
        try writer.print("  \"duration_ms\": {d},\n", .{result.duration_ms});

        try writer.writeAll("  \"config\": {\n");
        try writer.print("    \"host\": \"{s}\",\n", .{result.config.host});
        try writer.print("    \"port\": {d},\n", .{result.config.port});
        try writer.print("    \"record_count\": {d},\n", .{result.config.record_count});
        try writer.print("    \"operation_count\": {d},\n", .{result.config.operation_count});
        try writer.print("    \"document_size\": {d},\n", .{result.config.document_size});
        try writer.print("    \"thread_count\": {d},\n", .{result.config.thread_count});
        try writer.print("    \"warmup_ops\": {d}\n", .{result.config.warmup_ops});
        try writer.writeAll("  },\n");

        try writer.writeAll("  \"summary\": {\n");
        try writer.print("    \"total_ops\": {d},\n", .{result.summary.total_ops});
        try writer.print("    \"successful_ops\": {d},\n", .{result.summary.successful_ops});
        try writer.print("    \"failed_ops\": {d},\n", .{result.summary.failed_ops});
        try writer.print("    \"throughput_ops_sec\": {d:.2},\n", .{result.summary.throughput_ops_sec});
        try writer.print("    \"avg_latency_us\": {d:.2},\n", .{result.summary.avg_latency_us});
        try writer.print("    \"min_latency_us\": {d},\n", .{result.summary.min_latency_us});
        try writer.print("    \"max_latency_us\": {d},\n", .{result.summary.max_latency_us});
        try writer.print("    \"p50_latency_us\": {d},\n", .{result.summary.p50_latency_us});
        try writer.print("    \"p95_latency_us\": {d},\n", .{result.summary.p95_latency_us});
        try writer.print("    \"p99_latency_us\": {d},\n", .{result.summary.p99_latency_us});
        try writer.print("    \"p999_latency_us\": {d},\n", .{result.summary.p999_latency_us});
        try writer.print("    \"error_rate_percent\": {d:.4}\n", .{result.summary.error_rate_percent});
        try writer.writeAll("  }");

        if (result.per_operation) |per_op| {
            try writer.writeAll(",\n  \"per_operation\": {\n");
            var first = true;
            inline for (@typeInfo(BenchmarkResult.PerOperationResults).@"struct".fields) |field| {
                if (@field(per_op, field.name)) |stats| {
                    if (!first) try writer.writeAll(",\n");
                    first = false;
                    try writer.print("    \"{s}\": {{\n", .{field.name});
                    try writeOpStatsJson(writer, stats);
                    try writer.writeAll("    }");
                }
            }
            try writer.writeAll("\n  }");
        }

        if (result.histogram) |hist| {
            try writer.writeAll(",\n  \"histogram\": {\n");
            try writer.print("    \"total_count\": {d},\n", .{hist.total_count});
            try writer.writeAll("    \"buckets\": [\n");
            for (hist.buckets, 0..) |bucket, i| {
                if (i > 0) try writer.writeAll(",\n");
                try writer.writeAll("      {");
                try writer.print("\"lower_us\": {d}, ", .{bucket.lower_bound_us});
                if (bucket.upper_bound_us == std.math.maxInt(u64)) {
                    try writer.writeAll("\"upper_us\": \"inf\", ");
                } else {
                    try writer.print("\"upper_us\": {d}, ", .{bucket.upper_bound_us});
                }
                try writer.print("\"count\": {d}, \"cumulative_pct\": {d:.2}", .{ bucket.count, bucket.cumulative_percent });
                try writer.writeAll("}");
            }
            try writer.writeAll("\n    ]\n  }");
        }

        try writer.writeAll("\n}\n");
    }

    fn writeOpStatsJson(writer: anytype, stats: BenchmarkResult.OperationStats) !void {
        try writer.print("      \"total_ops\": {d},\n", .{stats.total_ops});
        try writer.print("      \"successful_ops\": {d},\n", .{stats.successful_ops});
        try writer.print("      \"failed_ops\": {d},\n", .{stats.failed_ops});
        try writer.print("      \"avg_latency_us\": {d:.2},\n", .{stats.avg_latency_us});
        try writer.print("      \"min_latency_us\": {d},\n", .{stats.min_latency_us});
        try writer.print("      \"max_latency_us\": {d},\n", .{stats.max_latency_us});
        try writer.print("      \"p50_latency_us\": {d},\n", .{stats.p50_latency_us});
        try writer.print("      \"p95_latency_us\": {d},\n", .{stats.p95_latency_us});
        try writer.print("      \"p99_latency_us\": {d},\n", .{stats.p99_latency_us});
        try writer.print("      \"p999_latency_us\": {d}\n", .{stats.p999_latency_us});
    }

    pub fn exportCsv(_: *ResultExporter, result: BenchmarkResult, writer: anytype) !void {
        try writer.writeAll("name,workload,timestamp,duration_ms,record_count,operation_count,");
        try writer.writeAll("total_ops,successful_ops,failed_ops,throughput_ops_sec,");
        try writer.writeAll("avg_latency_us,min_latency_us,max_latency_us,");
        try writer.writeAll("p50_latency_us,p95_latency_us,p99_latency_us,p999_latency_us,error_rate_percent\n");

        try writer.print("{s},{s},{d},{d},{d},{d},", .{
            result.name,
            result.workload,
            result.timestamp,
            result.duration_ms,
            result.config.record_count,
            result.config.operation_count,
        });
        try writer.print("{d},{d},{d},{d:.2},", .{
            result.summary.total_ops,
            result.summary.successful_ops,
            result.summary.failed_ops,
            result.summary.throughput_ops_sec,
        });
        try writer.print("{d:.2},{d},{d},", .{
            result.summary.avg_latency_us,
            result.summary.min_latency_us,
            result.summary.max_latency_us,
        });
        try writer.print("{d},{d},{d},{d},{d:.4}\n", .{
            result.summary.p50_latency_us,
            result.summary.p95_latency_us,
            result.summary.p99_latency_us,
            result.summary.p999_latency_us,
            result.summary.error_rate_percent,
        });
    }

    pub fn exportHuman(_: *ResultExporter, result: BenchmarkResult, writer: anytype) !void {
        try writer.writeAll("\n");
        try writer.writeAll("╔══════════════════════════════════════════════════════════════════╗\n");
        try writer.print("║  Benchmark: {s:<52} ║\n", .{result.name});
        try writer.print("║  Workload:  {s:<52} ║\n", .{result.workload});
        try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");

        try writer.writeAll("║  CONFIGURATION                                                   ║\n");
        try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");
        try writer.print("║  Host:            {s}:{d:<36}  ║\n", .{ result.config.host, result.config.port });
        try writer.print("║  Records:         {d:<45}  ║\n", .{result.config.record_count});
        try writer.print("║  Operations:      {d:<45}  ║\n", .{result.config.operation_count});
        try writer.print("║  Doc Size:        {d} bytes{s:<36}  ║\n", .{ result.config.document_size, "" });
        try writer.print("║  Threads:         {d:<45}  ║\n", .{result.config.thread_count});

        try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");
        try writer.writeAll("║  RESULTS                                                         ║\n");
        try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");
        try writer.print("║  Duration:        {d:.2} seconds{s:<33}  ║\n", .{ @as(f64, @floatFromInt(result.duration_ms)) / 1000.0, "" });
        try writer.print("║  Total Ops:       {d:<45}  ║\n", .{result.summary.total_ops});
        try writer.print("║  Successful:      {d:<45}  ║\n", .{result.summary.successful_ops});
        try writer.print("║  Failed:          {d:<45}  ║\n", .{result.summary.failed_ops});
        try writer.print("║  Error Rate:      {d:.2}%{s:<41}  ║\n", .{ result.summary.error_rate_percent, "" });
        try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");
        try writer.print("║  THROUGHPUT:      {d:.2} ops/sec{s:<27}  ║\n", .{ result.summary.throughput_ops_sec, "" });
        try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");
        try writer.writeAll("║  LATENCY (microseconds)                                          ║\n");
        try writer.print("║    Average:       {d:.2} µs{s:<36}  ║\n", .{ result.summary.avg_latency_us, "" });
        try writer.print("║    Min:           {d} µs{s:<40}  ║\n", .{ result.summary.min_latency_us, "" });
        try writer.print("║    Max:           {d} µs{s:<40}  ║\n", .{ result.summary.max_latency_us, "" });
        try writer.print("║    P50:           {d} µs{s:<40}  ║\n", .{ result.summary.p50_latency_us, "" });
        try writer.print("║    P95:           {d} µs{s:<40}  ║\n", .{ result.summary.p95_latency_us, "" });
        try writer.print("║    P99:           {d} µs{s:<40}  ║\n", .{ result.summary.p99_latency_us, "" });
        try writer.print("║    P99.9:         {d} µs{s:<40}  ║\n", .{ result.summary.p999_latency_us, "" });

        if (result.per_operation) |per_op| {
            try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");
            try writer.writeAll("║  PER-OPERATION STATS                                             ║\n");
            inline for (@typeInfo(BenchmarkResult.PerOperationResults).@"struct".fields) |field| {
                if (@field(per_op, field.name)) |stats| {
                    try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");
                    try writer.print("║  {s:<15} Ops: {d:<8} Avg: {d:.0}µs  P99: {d}µs{s:<8}  ║\n", .{
                        field.name,
                        stats.total_ops,
                        stats.avg_latency_us,
                        stats.p99_latency_us,
                        "",
                    });
                }
            }
        }

        if (result.histogram) |hist| {
            try writer.writeAll("╠══════════════════════════════════════════════════════════════════╣\n");
            try writer.writeAll("║  LATENCY HISTOGRAM                                               ║\n");
            try writer.writeAll("╟──────────────────────────────────────────────────────────────────╢\n");
            for (hist.buckets) |bucket| {
                if (bucket.count > 0) {
                    const bar_len = @min(30, @as(usize, @intFromFloat(bucket.cumulative_percent * 0.3)));
                    var bar_buf: [31]u8 = undefined;
                    for (0..bar_len) |i| bar_buf[i] = '#';
                    for (bar_len..30) |i| bar_buf[i] = ' ';
                    bar_buf[30] = 0;

                    if (bucket.upper_bound_us == std.math.maxInt(u64)) {
                        try writer.print("║  >{d:>6}µs: {s} {d:>5} ({d:>5.1}%) ║\n", .{
                            bucket.lower_bound_us,
                            bar_buf[0..30],
                            bucket.count,
                            bucket.cumulative_percent,
                        });
                    } else {
                        try writer.print("║  {d:>6}-{d:<6}µs: {s} {d:>5} ({d:>5.1}%) ║\n", .{
                            bucket.lower_bound_us,
                            bucket.upper_bound_us,
                            bar_buf[0..30],
                            bucket.count,
                            bucket.cumulative_percent,
                        });
                    }
                }
            }
        }

        try writer.writeAll("╚══════════════════════════════════════════════════════════════════╝\n");
    }

    pub fn exportToFile(self: *ResultExporter, result: BenchmarkResult, path: []const u8, format: config_mod.ExportFormat) !void {
        const file = try Dir.createFile(.cwd(), self.io, path, .{});
        defer file.close(self.io);

        var buf: [8192]u8 = undefined;
        var file_writer = file.writer(self.io, &buf);
        const writer = &file_writer.interface;

        switch (format) {
            .json => try self.exportJson(result, writer),
            .csv => try self.exportCsv(result, writer),
            .ycsb => try self.exportYcsb(result, writer),
            .human => try self.exportHuman(result, writer),
        }

        try file_writer.flush();
        std.debug.print("Results exported to: {s}\n", .{path});
    }

    pub fn exportYcsb(_: *ResultExporter, result: BenchmarkResult, writer: anytype) !void {
        try writer.print("[OVERALL], RunTime(ms), {d}\n", .{result.duration_ms});
        try writer.print("[OVERALL], Throughput(ops/sec), {d:.2}\n", .{result.summary.throughput_ops_sec});
        try writer.print("[OVERALL], Operations, {d}\n", .{result.summary.total_ops});
        try writer.print("[OVERALL], SuccessfulOperations, {d}\n", .{result.summary.successful_ops});
        try writer.print("[OVERALL], FailedOperations, {d}\n", .{result.summary.failed_ops});
        try writer.print("[OVERALL], ErrorRate(%), {d:.4}\n", .{result.summary.error_rate_percent});

        try writer.print("[OVERALL], AverageLatency(us), {d:.2}\n", .{result.summary.avg_latency_us});
        try writer.print("[OVERALL], MinLatency(us), {d}\n", .{result.summary.min_latency_us});
        try writer.print("[OVERALL], MaxLatency(us), {d}\n", .{result.summary.max_latency_us});
        try writer.print("[OVERALL], 50thPercentileLatency(us), {d}\n", .{result.summary.p50_latency_us});
        try writer.print("[OVERALL], 95thPercentileLatency(us), {d}\n", .{result.summary.p95_latency_us});
        try writer.print("[OVERALL], 99thPercentileLatency(us), {d}\n", .{result.summary.p99_latency_us});
        try writer.print("[OVERALL], 99.9thPercentileLatency(us), {d}\n", .{result.summary.p999_latency_us});

        if (result.per_operation) |per_op| {
            inline for (@typeInfo(BenchmarkResult.PerOperationResults).@"struct".fields) |field| {
                if (@field(per_op, field.name)) |stats| {
                    const tag = comptime blk: {
                        var buf: [field.name.len]u8 = undefined;
                        for (field.name, 0..) |c, i| {
                            buf[i] = if (c >= 'a' and c <= 'z') c - 32 else if (c == '_') '-' else c;
                        }
                        break :blk buf;
                    };
                    const section = &tag;
                    try writer.print("[{s}], Operations, {d}\n", .{ section, stats.total_ops });
                    try writer.print("[{s}], AverageLatency(us), {d:.2}\n", .{ section, stats.avg_latency_us });
                    try writer.print("[{s}], MinLatency(us), {d}\n", .{ section, stats.min_latency_us });
                    try writer.print("[{s}], MaxLatency(us), {d}\n", .{ section, stats.max_latency_us });
                    try writer.print("[{s}], 50thPercentileLatency(us), {d}\n", .{ section, stats.p50_latency_us });
                    try writer.print("[{s}], 95thPercentileLatency(us), {d}\n", .{ section, stats.p95_latency_us });
                    try writer.print("[{s}], 99thPercentileLatency(us), {d}\n", .{ section, stats.p99_latency_us });
                    try writer.print("[{s}], 99.9thPercentileLatency(us), {d}\n", .{ section, stats.p999_latency_us });
                }
            }
        }

        if (result.histogram) |hist| {
            for (hist.buckets) |bucket| {
                if (bucket.count > 0) {
                    if (bucket.upper_bound_us == std.math.maxInt(u64)) {
                        try writer.print("[HISTOGRAM], >{d}us, {d}\n", .{ bucket.lower_bound_us, bucket.count });
                    } else {
                        try writer.print("[HISTOGRAM], {d}-{d}us, {d}\n", .{ bucket.lower_bound_us, bucket.upper_bound_us, bucket.count });
                    }
                }
            }
        }
    }

    pub fn exportResult(
        self: *ResultExporter,
        cfg: config_mod.BenchmarkConfig,
        summaries: anytype,
        path: []const u8,
    ) !void {
        const file = try Dir.createFile(.cwd(), self.io, path, .{});
        defer file.close(self.io);

        var buf: [8192]u8 = undefined;
        var file_writer = file.writer(self.io, &buf);
        const writer = &file_writer.interface;

        try writer.print("# YCSB Benchmark Suite Report\n\n", .{});
        try writer.print("- **Timestamp:** {d}\n", .{milliTimestamp()});
        try writer.print("- **Record Count:** {d}\n", .{cfg.record_count});
        try writer.print("- **Operation Count:** {d}\n", .{cfg.operation_count});
        try writer.print("- **Thread Count:** {d}\n\n", .{cfg.thread_count});

        try writer.print("| Workload | Description | Mix | Throughput (ops/sec) | Avg Latency (us) | Min Latency (us) | Max Latency (us) | p95 (us) | p99 (us) |\n", .{});
        try writer.print("|---|---|---|---|---|---|---|---|---|\n", .{});
        for (summaries) |s| {
            try writer.print("| Workload {s} | {s} | {s} | {d:.1} | {d:.1} | {d} | {d} | {d} | {d} |\n", .{
                s.label, s.description, s.mix, s.throughput_ops_sec, s.avg_latency_us, s.min_latency_us, s.max_latency_us, s.p95_latency_us, s.p99_latency_us,
            });
        }
        try file_writer.flush();
        std.debug.print("Suite results exported to: {s}\n", .{path});
    }

    pub fn exportToStdoutHuman(_: *ResultExporter, result: BenchmarkResult) void {
        std.debug.print("\n", .{});
        std.debug.print("╔══════════════════════════════════════════════════════════════════╗\n", .{});
        std.debug.print("║  Benchmark: {s:<52} ║\n", .{result.name});
        std.debug.print("║  Workload:  {s:<52} ║\n", .{result.workload});
        std.debug.print("╠══════════════════════════════════════════════════════════════════╣\n", .{});
        std.debug.print("║  RESULTS                                                         ║\n", .{});
        std.debug.print("╟──────────────────────────────────────────────────────────────────╢\n", .{});
        std.debug.print("║  Duration:        {d:.2} seconds                                   ║\n", .{@as(f64, @floatFromInt(result.duration_ms)) / 1000.0});
        std.debug.print("║  Total Ops:       {d:<45}  ║\n", .{result.summary.total_ops});
        std.debug.print("║  Successful:      {d:<45}  ║\n", .{result.summary.successful_ops});
        std.debug.print("║  Failed:          {d:<45}  ║\n", .{result.summary.failed_ops});
        std.debug.print("╟──────────────────────────────────────────────────────────────────╢\n", .{});
        std.debug.print("║  THROUGHPUT:      {d:.2} ops/sec                                  ║\n", .{result.summary.throughput_ops_sec});
        std.debug.print("╟──────────────────────────────────────────────────────────────────╢\n", .{});
        std.debug.print("║  LATENCY (microseconds)                                          ║\n", .{});
        std.debug.print("║    Average:       {d:.2} µs                                       ║\n", .{result.summary.avg_latency_us});
        std.debug.print("║    P50:           {d} µs                                          ║\n", .{result.summary.p50_latency_us});
        std.debug.print("║    P99:           {d} µs                                          ║\n", .{result.summary.p99_latency_us});
        std.debug.print("║    P99.9:         {d} µs                                          ║\n", .{result.summary.p999_latency_us});
        std.debug.print("╚══════════════════════════════════════════════════════════════════╝\n", .{});
    }
};
