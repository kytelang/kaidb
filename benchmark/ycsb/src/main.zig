const std = @import("std");
const builtin = @import("builtin");
const client_mod = @import("client.zig");
const Client = client_mod.Client;
const Io = std.Io;

const config_mod = @import("config.zig");
const orders_mod = @import("orders.zig");
const ConfigManager = config_mod.ConfigManager;
const results_mod = @import("results.zig");
const ResultExporter = results_mod.ResultExporter;
const compare_mod = @import("compare.zig");
const ComparisonTool = compare_mod.ComparisonTool;
const stability_mod = @import("stability.zig");
const StabilityTester = stability_mod.StabilityTester;
const StabilityTests = stability_mod.StabilityTests;
const Metrics = @import("metrics.zig").Metrics;
const MetricsTracker = @import("metrics.zig").MetricsTracker;
const milliTimestamp = @import("metrics.zig").milliTimestamp;
const Timer = @import("metrics.zig").Timer;

const WorkloadA = @import("workloads/workload_a.zig").WorkloadA;
const WorkloadB = @import("workloads/workload_b.zig").WorkloadB;
const WorkloadC = @import("workloads/workload_c.zig").WorkloadC;
const WorkloadD = @import("workloads/workload_d.zig").WorkloadD;
const WorkloadE = @import("workloads/workload_e.zig").WorkloadE;
const WorkloadF = @import("workloads/workload_f.zig").WorkloadF;

pub fn main(init: std.process.Init.Minimal) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;

    defer if (builtin.mode == .Debug) {
        if (gpa.detectLeaks() > 0) {
            std.process.exit(1);
        }
    };

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const args = try init.args.toSlice(arena.allocator());

    if (args.len < 2) {
        try printUsage();
        return;
    }

    const command = args[1];

    var config_manager = ConfigManager.init(allocator);
    defer config_manager.deinit();

    config_manager.loadFromFile("config.yaml") catch {};

    if (args.len > 2) {
        try config_manager.parseArgs(args[2..]);
    }

    const cfg = config_manager.config;

    if (std.mem.eql(u8, command, "workload-a")) {
        try runWorkloadA(allocator, cfg);
    } else if (std.mem.eql(u8, command, "workload-b")) {
        try runWorkloadB(allocator, cfg);
    } else if (std.mem.eql(u8, command, "workload-c")) {
        try runWorkloadC(allocator, cfg);
    } else if (std.mem.eql(u8, command, "workload-d")) {
        try runWorkloadD(allocator, cfg);
    } else if (std.mem.eql(u8, command, "workload-e")) {
        try runWorkloadE(allocator, cfg);
    } else if (std.mem.eql(u8, command, "workload-f")) {
        try runWorkloadF(allocator, cfg);
    } else if (std.mem.eql(u8, command, "workload-all")) {
        try runAllWorkloads(allocator, cfg);
    } else if (std.mem.eql(u8, command, "orders-sql") or
        std.mem.eql(u8, command, "orders-sql-query"))
    {
        // Optional --data_dir=... override for the source json/ directory.
        var data_dir: []const u8 = orders_mod.DEFAULT_DATA_DIR;
        for (args[2..]) |arg| {
            if (std.mem.startsWith(u8, arg, "--data_dir=")) data_dir = arg["--data_dir=".len..];
        }
        const mode: orders_mod.Mode = if (std.mem.indexOf(u8, command, "sql") != null) .sql else .doc;
        const do_load = std.mem.indexOf(u8, command, "query") == null;
        try orders_mod.run(allocator, cfg, mode, data_dir, do_load);
    } else if (std.mem.eql(u8, command, "orders-sql-dump")) {
        // Emit the same-seed dataset as a standalone .sql file (for loading a
        // peer engine with byte-identical rows). Flags: --out=PATH (required),
        // --data_dir=DIR (optional); record count comes from the usual config.
        var data_dir: []const u8 = orders_mod.DEFAULT_DATA_DIR;
        var out_path: []const u8 = "orders_dump.sql";
        for (args[2..]) |arg| {
            if (std.mem.startsWith(u8, arg, "--data_dir=")) data_dir = arg["--data_dir=".len..];
            if (std.mem.startsWith(u8, arg, "--out=")) out_path = arg["--out=".len..];
        }
        try orders_mod.dumpRun(allocator, cfg, data_dir, out_path);
    } else if (std.mem.eql(u8, command, "stability-quick")) {
        try runStabilityTest(allocator, &config_manager, 5);
    } else if (std.mem.eql(u8, command, "stability-1h")) {
        try runStabilityTest(allocator, &config_manager, 60);
    } else if (std.mem.eql(u8, command, "stability-24h")) {
        try runStabilityTest(allocator, &config_manager, 24 * 60);
    } else if (std.mem.eql(u8, command, "compare")) {
        try runCompare(allocator, args);
    } else if (std.mem.eql(u8, command, "config")) {
        config_manager.printConfig();
    } else if (std.mem.eql(u8, command, "generate-config")) {
        try generateConfigFile(allocator);
    } else {
        std.debug.print("Unknown command: {s}\n", .{command});
        try printUsage();
    }
}

fn printUsage() !void {
    const usage =
        \\shinydb-ycsb - YCSB Benchmark suite for shinydb
        \\
        \\Usage:
        \\  shinydb-ycsb <command> [options]
        \\
        \\YCSB Standard Workloads:
        \\  workload-a          Update Heavy (50% reads, 50% updates)
        \\  workload-b          Read Mostly (95% reads, 5% updates)
        \\  workload-c          Read Only (100% reads)
        \\  workload-d          Read Latest (95% reads, 5% inserts)
        \\  workload-e          Short Ranges (95% scans, 5% inserts)
        \\  workload-f          Read-Modify-Write (50% reads, 50% RMW)
        \\  workload-all        Run ALL workloads (A-F) and generate Markdown report
        \\
        \\Stability Tests:
        \\  stability-quick     Run 5-minute stability check (CI/CD)
        \\  stability-1h        Run 1-hour endurance test
        \\  stability-24h       Run 24-hour stability test
        \\
        \\Tools:
        \\  compare <a> <b>     Compare two benchmark results (JSON files)
        \\  config              Show current configuration
        \\  generate-config     Generate default config.yaml
        \\
        \\Options:
        \\  -c <file>           Load config from file
        \\  --host=<host>       Override host (default: 127.0.0.1)
        \\  --port=<port>       Override port (default: 3009)
        \\  --record_count=<n>  Override record count
        \\  --operation_count=<n> Override operation count
        \\  --document_size=<n> Override document size (bytes)
        \\  --thread_count=<n>  Override thread count
        \\  --export_format=<f> Output format: human, json, csv, ycsb
        \\  --export_path=<p>   Export results to file
        \\
        \\Examples:
        \\  shinydb-ycsb workload-a --record_count=100000
        \\  shinydb-ycsb workload-c --export_format=ycsb
        \\  shinydb-ycsb workload-all --record_count=1000 --operation_count=1000
        \\  shinydb-ycsb workload-all --export_path=results/report.md
        \\  shinydb-ycsb stability-quick
        \\  shinydb-ycsb compare baseline.json candidate.json
        \\
    ;
    std.debug.print("{s}\n", .{usage});
}

fn connectClient(allocator: std.mem.Allocator, io: Io, cfg: config_mod.BenchmarkConfig) !*Client {
    const client = try Client.init(allocator, io, cfg.host, cfg.port, cfg.tls);
    errdefer client.deinit();

    client.login(cfg.uid, cfg.key) catch |err| {
        std.debug.print("Client connection login failed: {}\n", .{err});
        return err;
    };
    return client;
}


fn printYcsbOutput(allocator: std.mem.Allocator, tracker: *MetricsTracker, duration_ms: i64) !void {
    _ = allocator;
    var total_ops: u64 = 0;
    var successful_ops: u64 = 0;
    var failed_ops: u64 = 0;
    var min_lat: u64 = std.math.maxInt(u64);
    var max_lat: u64 = 0;
    var total_lat: u64 = 0;

    const op_metrics = [_]?*Metrics{
        tracker.read_metrics,
        tracker.insert_metrics,
        tracker.update_metrics,
        tracker.delete_metrics,
        tracker.scan_metrics,
        tracker.rmw_metrics,
    };
    const op_names = [_][]const u8{ "READ", "INSERT", "UPDATE", "DELETE", "SCAN", "READ-MODIFY-WRITE" };

    for (op_metrics, 0..) |metric_opt, idx| {
        if (metric_opt) |m| {
            const total = m.total_ops.load(.monotonic);
            if (total == 0) continue;
            const name = op_names[idx];
            total_ops += total;
            successful_ops += m.successful_ops.load(.monotonic);
            failed_ops += m.failed_ops.load(.monotonic);
            const m_min = m.min_latency.load(.monotonic);
            const m_max = m.max_latency.load(.monotonic);
            if (m_min < min_lat) min_lat = m_min;
            if (m_max > max_lat) max_lat = m_max;
            total_lat += m.total_latency.load(.monotonic);

            std.debug.print("[{s}], Operations, {d}\n", .{ name, total });
            std.debug.print("[{s}], AverageLatency(us), {d:.2}\n", .{ name, m.avgLatency() });
            std.debug.print("[{s}], MinLatency(us), {d}\n", .{ name, m_min });
            std.debug.print("[{s}], MaxLatency(us), {d}\n", .{ name, m_max });
            std.debug.print("[{s}], 50thPercentileLatency(us), {d}\n", .{ name, m.percentile(0.50) });
            std.debug.print("[{s}], 95thPercentileLatency(us), {d}\n", .{ name, m.percentile(0.95) });
            std.debug.print("[{s}], 99thPercentileLatency(us), {d}\n", .{ name, m.percentile(0.99) });
            std.debug.print("[{s}], 99.9thPercentileLatency(us), {d}\n", .{ name, m.percentile(0.999) });
        }
    }

    if (total_ops > 0) {
        const overall_throughput = (@as(f64, @floatFromInt(total_ops)) * 1000.0) / @as(f64, @floatFromInt(duration_ms));
        const overall_avg = @as(f64, @floatFromInt(total_lat)) / @as(f64, @floatFromInt(total_ops));
        std.debug.print("[OVERALL], RunTime(ms), {d}\n", .{duration_ms});
        std.debug.print("[OVERALL], Throughput(ops/sec), {d:.2}\n", .{overall_throughput});
        std.debug.print("[OVERALL], AverageLatency(us), {d:.2}\n", .{overall_avg});
        std.debug.print("[OVERALL], MinLatency(us), {d}\n", .{min_lat});
        std.debug.print("[OVERALL], MaxLatency(us), {d}\n", .{max_lat});
    }
}

fn runWorkloadA(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const config = WorkloadA.Config{
        .record_count = cfg.record_count,
        .operation_count = cfg.operation_count,
        .document_size = cfg.document_size,
        .warmup_ops = cfg.warmup_ops,
    };

    var workload = try WorkloadA.init(allocator, io, client, "workload_a", config);
    defer workload.deinit();

    try workload.load();
    try workload.run();

    if (cfg.export_format == .ycsb) {
        const run_duration = milliTimestamp() - workload.metrics_tracker.run_start_time;
        try printYcsbOutput(allocator, &workload.metrics_tracker, run_duration);
    }
}

fn runWorkloadB(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const config = WorkloadB.Config{
        .record_count = cfg.record_count,
        .operation_count = cfg.operation_count,
        .document_size = cfg.document_size,
        .warmup_ops = cfg.warmup_ops,
    };

    var workload = try WorkloadB.init(allocator, io, client, "workload_b", config);
    defer workload.deinit();

    try workload.load();
    try workload.run();

    if (cfg.export_format == .ycsb) {
        const run_duration = milliTimestamp() - workload.metrics_tracker.run_start_time;
        try printYcsbOutput(allocator, &workload.metrics_tracker, run_duration);
    }
}

fn runWorkloadC(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const config = WorkloadC.Config{
        .record_count = cfg.record_count,
        .operation_count = cfg.operation_count,
        .document_size = cfg.document_size,
        .warmup_ops = cfg.warmup_ops,
    };

    var workload = try WorkloadC.init(allocator, io, client, "workload_c", config);
    defer workload.deinit();

    try workload.load();
    try workload.run();

    if (cfg.export_format == .ycsb) {
        const run_duration = milliTimestamp() - workload.metrics_tracker.run_start_time;
        try printYcsbOutput(allocator, &workload.metrics_tracker, run_duration);
    }
}

fn runWorkloadD(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const config = WorkloadD.Config{
        .record_count = cfg.record_count,
        .operation_count = cfg.operation_count,
        .document_size = cfg.document_size,
        .warmup_ops = cfg.warmup_ops,
    };

    var workload = try WorkloadD.init(allocator, io, client, "workload_d", config);
    defer workload.deinit();

    try workload.load();
    try workload.run();

    if (cfg.export_format == .ycsb) {
        const run_duration = milliTimestamp() - workload.metrics_tracker.run_start_time;
        try printYcsbOutput(allocator, &workload.metrics_tracker, run_duration);
    }
}

fn runWorkloadE(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const config = WorkloadE.Config{
        .record_count = cfg.record_count,
        .operation_count = cfg.operation_count,
        .document_size = cfg.document_size,
        .warmup_ops = cfg.warmup_ops,
        .scan_length = cfg.scan_length,
    };

    var workload = try WorkloadE.init(allocator, io, client, "workload_e", config);
    defer workload.deinit();

    try workload.load();
    try workload.run();

    if (cfg.export_format == .ycsb) {
        const run_duration = milliTimestamp() - workload.metrics_tracker.run_start_time;
        try printYcsbOutput(allocator, &workload.metrics_tracker, run_duration);
    }
}

fn runWorkloadF(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const config = WorkloadF.Config{
        .record_count = cfg.record_count,
        .operation_count = cfg.operation_count,
        .document_size = cfg.document_size,
        .warmup_ops = cfg.warmup_ops,
    };

    var workload = try WorkloadF.init(allocator, io, client, "workload_f", config);
    defer workload.deinit();

    try workload.load();
    try workload.run();

    if (cfg.export_format == .ycsb) {
        const run_duration = milliTimestamp() - workload.metrics_tracker.run_start_time;
        try printYcsbOutput(allocator, &workload.metrics_tracker, run_duration);
    }
}

const WorkloadSummary = struct {
    label: []const u8,
    description: []const u8,
    mix: []const u8,
    total_ops: u64,
    successful_ops: u64,
    failed_ops: u64,
    duration_ms: i64,
    throughput_ops_sec: f64,
    avg_latency_us: f64,
    min_latency_us: u64,
    max_latency_us: u64,
    p95_latency_us: u64,
    p99_latency_us: u64,
};

const WorkloadType = enum { a, b, c, d, e, f };

fn collectMetrics(allocator: std.mem.Allocator, tracker: *MetricsTracker, duration_ms: i64, label: []const u8, desc: []const u8, mix: []const u8) !WorkloadSummary {
    var total_ops: u64 = 0;
    var successful_ops: u64 = 0;
    var failed_ops: u64 = 0;
    var total_lat: u64 = 0;
    var min_lat: u64 = std.math.maxInt(u64);
    var max_lat: u64 = 0;

    var all_latencies: std.ArrayList(u64) = .empty;
    defer all_latencies.deinit(allocator);

    const op_metrics = [_]?*Metrics{
        tracker.read_metrics,
        tracker.insert_metrics,
        tracker.update_metrics,
        tracker.delete_metrics,
        tracker.scan_metrics,
        tracker.rmw_metrics,
    };

    for (op_metrics) |metric_opt| {
        if (metric_opt) |m| {
            const total = m.total_ops.load(.monotonic);
            if (total == 0) continue;
            total_ops += total;
            successful_ops += m.successful_ops.load(.monotonic);
            failed_ops += m.failed_ops.load(.monotonic);
            const m_min = m.min_latency.load(.monotonic);
            const m_max = m.max_latency.load(.monotonic);
            if (m_min < min_lat) min_lat = m_min;
            if (m_max > max_lat) max_lat = m_max;
            total_lat += m.total_latency.load(.monotonic);

            var i: usize = 0;
            while (i < m.latencies.items.len) : (i += 1) {
                try all_latencies.append(allocator, m.latencies.items[i]);
            }
        }
    }

    const throughput = (@as(f64, @floatFromInt(total_ops)) * 1000.0) / @as(f64, @floatFromInt(duration_ms));
    const avg_latency = if (total_ops > 0) @as(f64, @floatFromInt(total_lat)) / @as(f64, @floatFromInt(total_ops)) else 0.0;

    var p95: u64 = 0;
    var p99: u64 = 0;
    if (all_latencies.items.len > 0) {
        std.mem.sort(u64, all_latencies.items, {}, std.sort.asc(u64));
        const idx_95 = @as(usize, @intFromFloat(@as(f64, @floatFromInt(all_latencies.items.len)) * 0.95));
        const idx_99 = @as(usize, @intFromFloat(@as(f64, @floatFromInt(all_latencies.items.len)) * 0.99));
        p95 = all_latencies.items[@min(idx_95, all_latencies.items.len - 1)];
        p99 = all_latencies.items[@min(idx_99, all_latencies.items.len - 1)];
    }

    return WorkloadSummary{
        .label = try allocator.dupe(u8, label),
        .description = try allocator.dupe(u8, desc),
        .mix = try allocator.dupe(u8, mix),
        .total_ops = total_ops,
        .successful_ops = successful_ops,
        .failed_ops = failed_ops,
        .duration_ms = duration_ms,
        .throughput_ops_sec = throughput,
        .avg_latency_us = avg_latency,
        .min_latency_us = if (min_lat == std.math.maxInt(u64)) 0 else min_lat,
        .max_latency_us = max_lat,
        .p95_latency_us = p95,
        .p99_latency_us = p99,
    };
}

fn freeSummary(allocator: std.mem.Allocator, s: WorkloadSummary) void {
    allocator.free(s.label);
    allocator.free(s.description);
    allocator.free(s.mix);
}

fn runAllWorkloads(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig) !void {
    std.debug.print("\n=== Running YCSB Workload Suite (A-F) ===\n", .{});
    std.debug.print("Records: {d}, Operations: {d}, Threads: {d}\n\n", .{ cfg.record_count, cfg.operation_count, cfg.thread_count });

    var summaries: std.ArrayList(WorkloadSummary) = .empty;
    defer {
        for (summaries.items) |s| freeSummary(allocator, s);
        summaries.deinit(allocator);
    }

    const types = [_]WorkloadType{ .a, .b, .c, .d, .e, .f };
    for (types) |wl| {
        const summary = try runSingleWorkload(allocator, cfg, wl);
        try summaries.append(allocator, summary);
    }

    std.debug.print("\n=========================================================================================================\n", .{});
    std.debug.print("YCSB Benchmark Suite Summary\n", .{});
    std.debug.print("=========================================================================================================\n", .{});
    std.debug.print("Workload  Description          Mix         Throughput  Avg Latency  Min Latency  Max Latency  p95    p99\n", .{});
    std.debug.print("---------------------------------------------------------------------------------------------------------\n", .{});
    for (summaries.items) |s| {
        std.debug.print("Workload {s}  {s:<20} {s:<11} {d:8.1} op/s {d:9.1}us {d:10}us {d:10}us {d:6} {d:6}\n", .{
            s.label, s.description, s.mix, s.throughput_ops_sec, s.avg_latency_us, s.min_latency_us, s.max_latency_us, s.p95_latency_us, s.p99_latency_us,
        });
    }
    std.debug.print("=========================================================================================================\n\n", .{});

    if (cfg.export_path) |path| {
        var threaded = std.Io.Threaded.init(allocator, .{});
        defer threaded.deinit();
        const io = threaded.io();

        var exporter = ResultExporter.init(allocator, io);
        try exporter.exportResult(cfg, summaries.items, path);
    }
}

fn runSingleWorkload(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig, wl_type: WorkloadType) !WorkloadSummary {
    var threaded: Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const suffix = switch (wl_type) {
        .a => "workload_a",
        .b => "workload_b",
        .c => "workload_c",
        .d => "workload_d",
        .e => "workload_e",
        .f => "workload_f",
    };
    const label = switch (wl_type) {
        .a => "A",
        .b => "B",
        .c => "C",
        .d => "D",
        .e => "E",
        .f => "F",
    };
    const desc = switch (wl_type) {
        .a => "Update Heavy",
        .b => "Read Mostly",
        .c => "Read Only",
        .d => "Read Latest",
        .e => "Short Ranges",
        .f => "Read-Modify-Write",
    };
    const mix = switch (wl_type) {
        .a => "50R/50U",
        .b => "95R/5U",
        .c => "100R",
        .d => "95R/5I",
        .e => "95S/5I",
        .f => "50R/50RMW",
    };

    switch (wl_type) {
        .a => {
            var workload = try WorkloadA.init(allocator, io, client, suffix, .{
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .warmup_ops = cfg.warmup_ops,
            });
            defer workload.deinit();
            try workload.load();
            try workload.run();
            const dur = milliTimestamp() - workload.metrics_tracker.run_start_time;
            return try collectMetrics(allocator, &workload.metrics_tracker, dur, label, desc, mix);
        },
        .b => {
            var workload = try WorkloadB.init(allocator, io, client, suffix, .{
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .warmup_ops = cfg.warmup_ops,
            });
            defer workload.deinit();
            try workload.load();
            try workload.run();
            const dur = milliTimestamp() - workload.metrics_tracker.run_start_time;
            return try collectMetrics(allocator, &workload.metrics_tracker, dur, label, desc, mix);
        },
        .c => {
            var workload = try WorkloadC.init(allocator, io, client, suffix, .{
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .warmup_ops = cfg.warmup_ops,
            });
            defer workload.deinit();
            try workload.load();
            try workload.run();
            const dur = milliTimestamp() - workload.metrics_tracker.run_start_time;
            return try collectMetrics(allocator, &workload.metrics_tracker, dur, label, desc, mix);
        },
        .d => {
            var workload = try WorkloadD.init(allocator, io, client, suffix, .{
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .warmup_ops = cfg.warmup_ops,
            });
            defer workload.deinit();
            try workload.load();
            try workload.run();
            const dur = milliTimestamp() - workload.metrics_tracker.run_start_time;
            return try collectMetrics(allocator, &workload.metrics_tracker, dur, label, desc, mix);
        },
        .e => {
            var workload = try WorkloadE.init(allocator, io, client, suffix, .{
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .warmup_ops = cfg.warmup_ops,
                .scan_length = cfg.scan_length,
            });
            defer workload.deinit();
            try workload.load();
            try workload.run();
            const dur = milliTimestamp() - workload.metrics_tracker.run_start_time;
            return try collectMetrics(allocator, &workload.metrics_tracker, dur, label, desc, mix);
        },
        .f => {
            var workload = try WorkloadF.init(allocator, io, client, suffix, .{
                .record_count = cfg.record_count,
                .operation_count = cfg.operation_count,
                .document_size = cfg.document_size,
                .warmup_ops = cfg.warmup_ops,
            });
            defer workload.deinit();
            try workload.load();
            try workload.run();
            const dur = milliTimestamp() - workload.metrics_tracker.run_start_time;
            return try collectMetrics(allocator, &workload.metrics_tracker, dur, label, desc, mix);
        },
    }
}

const StabilityDoc = struct {
    id: u64,
    data: []const u8,
};

fn runStabilityTest(allocator: std.mem.Allocator, config_manager: *ConfigManager, duration_minutes: u32) !void {
    const cfg = config_manager.config;

    std.debug.print("\n=== Starting Stability Test ({d} minutes) ===\n", .{duration_minutes});
    std.debug.print("Host: {s}:{d}\n", .{ cfg.host, cfg.port });
    std.debug.print("Document Size: {d} bytes\n\n", .{cfg.document_size});

    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    var client = try connectClient(allocator, io, cfg);
    defer client.deinit();
    defer client.disconnect();

    const store_ns = try allocator.dupe(u8, "stability_test");
    defer allocator.free(store_ns);

    _ = client.execute("DROP TABLE stability_test") catch {};
    const c_res = try client.execute("CREATE TABLE stability_test (id VARCHAR(32) PRIMARY KEY, data TEXT)");
    allocator.free(c_res);

    var tester = try StabilityTester.init(allocator, io, .{
        .duration_minutes = duration_minutes,
        .memory_check_interval_seconds = cfg.memory_check_interval_seconds,
        .throughput_sample_interval_seconds = 10,
    });
    defer tester.deinit();

    tester.start();

    const doc_size = @min(cfg.document_size, 16000);
    const data_buf = try allocator.alloc(u8, doc_size);
    defer allocator.free(data_buf);
    @memset(data_buf, 'x');

    var key_counter: u64 = 0;
    var keys = std.ArrayList(u64).empty;
    defer keys.deinit(allocator);
    var progress_counter: u64 = 0;
    const progress_interval: u64 = 1000;

    std.debug.print("Running stability test...\n", .{});

    while (!tester.isComplete()) {
        const timer = Timer.start();

        const success = if (key_counter % 2 == 0) blk: {
            var key_buf: [32]u8 = undefined;
            _ = std.fmt.bufPrint(&key_buf, "{x:0>32}", .{key_counter}) catch break :blk false;

            const insert_sql = std.fmt.allocPrint(allocator,
                "INSERT INTO stability_test (id, data) VALUES ('{s}', '{s}')",
                .{ key_buf, data_buf }
            ) catch break :blk false;
            defer allocator.free(insert_sql);

            if (client.execute(insert_sql)) |reply| {
                allocator.free(reply);
                keys.append(allocator, key_counter) catch break :blk false;
            } else |_| {
                break :blk false;
            }
            break :blk true;
        } else blk: {
            if (keys.items.len > 0) {
                const read_idx = (key_counter / 2) % keys.items.len;
                const key_id = keys.items[read_idx];

                var key_buf: [32]u8 = undefined;
                _ = std.fmt.bufPrint(&key_buf, "{x:0>32}", .{key_id}) catch break :blk false;

                const select_sql = std.fmt.allocPrint(allocator,
                    "SELECT id, data FROM stability_test WHERE id = '{s}'",
                    .{key_buf}
                ) catch break :blk false;
                defer allocator.free(select_sql);

                if (client.execute(select_sql)) |reply| {
                    allocator.free(reply);
                } else |_| {
                    break :blk false;
                }
            }
            break :blk true;
        };

        const latency = timer.elapsed();
        try tester.recordOperation(latency, success);

        key_counter += 1;
        progress_counter += 1;

        if (progress_counter >= progress_interval) {
            const elapsed = tester.getElapsedMinutes();
            std.debug.print("\r  Progress: {d:.1}/{d} minutes, {d} ops", .{ elapsed, duration_minutes, key_counter });
            progress_counter = 0;
        }
    }

    std.debug.print("\n\nTest complete. Analyzing results...\n", .{});

    const result = try tester.stop();

    StabilityTester.printReportDebug(result);

    if (cfg.export_path) |path| {
        const json_report = try std.fmt.allocPrint(allocator, "{}", .{result});
        defer allocator.free(json_report);
        const file = std.Io.Dir.createFile(.cwd(), io, path, .{}) catch |err| {
            std.debug.print("Warning: Could not create export file '{s}': {}\n", .{ path, err });
            return;
        };
        defer file.close(io);
        file.writeStreamingAll(io, json_report) catch |err| {
            std.debug.print("Warning: Could not write export file: {}\n", .{err});
        };
        std.debug.print("Results exported to: {s}\n", .{path});
    }
}

fn runCompare(allocator: std.mem.Allocator, args: []const []const u8) !void {
    if (args.len < 4) {
        std.debug.print("Usage: shinydb-ycsb compare <baseline.json> <candidate.json>\n", .{});
        return;
    }

    var tool = ComparisonTool.init(allocator);

    const baseline = try tool.loadResultFromFile(args[2]);
    const candidate = try tool.loadResultFromFile(args[3]);

    const comparison = tool.compare(baseline, candidate);

    tool.printReportDebug(comparison);
}

fn generateConfigFile(allocator: std.mem.Allocator) !void {
    const content = try config_mod.generateDefaultConfig(allocator);
    defer allocator.free(content);

    var threaded: std.Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const file = std.Io.Dir.createFile(.cwd(), io, "config.yaml", .{}) catch |err| {
        std.debug.print("Could not create config.yaml: {}\n", .{err});
        std.debug.print("Printing to stdout instead:\n\n{s}\n", .{content});
        return;
    };
    defer file.close(io);

    file.writeStreamingAll(io, content) catch |err| {
        std.debug.print("Could not write config.yaml: {}\n", .{err});
        return;
    };

    std.debug.print("Generated config.yaml\n", .{});
}
