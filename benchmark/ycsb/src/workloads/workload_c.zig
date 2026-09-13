const std = @import("std");
const client_mod = @import("../client.zig");
const Client = client_mod.Client;
const Allocator = std.mem.Allocator;
const Io = std.Io;

const distributions = @import("../distributions.zig");
const metrics = @import("../metrics.zig");

pub const WorkloadC = struct {
    allocator: Allocator,
    client: *Client,
    store_name: []const u8,
    config: Config,
    prng: *std.Random.DefaultPrng,
    key_distribution: distributions.Distribution,
    keys: std.ArrayList(u128),
    io: Io,
    metrics_tracker: metrics.MetricsTracker,
    field_data: []const u8,

    pub const Config = struct {
        record_count: usize = 1_000,
        operation_count: usize = 1_000,
        document_size: usize = 1024,
        thread_count: usize = 1,
        warmup_ops: usize = 100,
    };

    pub fn init(allocator: Allocator, io: Io, client: *Client, store_name: []const u8, config: Config) !WorkloadC {
        const prng = try allocator.create(std.Random.DefaultPrng);
        prng.* = std.Random.DefaultPrng.init(@intCast(metrics.milliTimestamp()));
        const random = prng.random();

        const key_dist = distributions.createDistribution(
            .zipfian,
            random,
            0,
            config.record_count,
        );

        const field_size = config.document_size / 10;
        const field_data = try allocator.alloc(u8, field_size);
        @memset(field_data, 'x');

        return .{
            .allocator = allocator,
            .client = client,
            .store_name = store_name,
            .config = config,
            .prng = prng,
            .key_distribution = key_dist,
            .io = io,
            .keys = .empty,
            .metrics_tracker = try metrics.MetricsTracker.init(allocator, io),
            .field_data = field_data,
        };
    }

    pub fn deinit(self: *WorkloadC) void {
        self.keys.deinit(self.allocator);
        self.metrics_tracker.deinit();
        self.allocator.free(self.field_data);
        self.allocator.destroy(self.prng);
    }

    pub fn load(self: *WorkloadC) !void {
        std.debug.print("\n=== Workload C: Loading Phase ===\n", .{});

        const drop_sql = try std.fmt.allocPrint(self.allocator, "DROP TABLE {s}", .{self.store_name});
        defer self.allocator.free(drop_sql);
        _ = self.client.execute(drop_sql) catch {};

        std.debug.print("Creating table {s}...\n", .{self.store_name});
        const create_sql = try std.fmt.allocPrint(self.allocator,
            "CREATE TABLE {s} (id VARCHAR(32) PRIMARY KEY, field0 TEXT, field1 TEXT, field2 TEXT, field3 TEXT, field4 TEXT, field5 TEXT, field6 TEXT, field7 TEXT, field8 TEXT, field9 TEXT)",
            .{self.store_name}
        );
        defer self.allocator.free(create_sql);
        const create_reply = try self.client.execute(create_sql);
        self.allocator.free(create_reply);

        std.debug.print("Inserting {d} records...\n", .{self.config.record_count});

        const start_time = metrics.milliTimestamp();
        var inserted: usize = 0;

        while (inserted < self.config.record_count) : (inserted += 1) {
            var key_buf: [32]u8 = undefined;
            _ = try std.fmt.bufPrint(&key_buf, "{x:0>32}", .{inserted});

            const insert_sql = try std.fmt.allocPrint(self.allocator,
                "INSERT INTO {s} (id, field0, field1, field2, field3, field4, field5, field6, field7, field8, field9) VALUES ('{s}', '{s}', '{s}', '{s}', '{s}', '{s}', '{s}', '{s}', '{s}', '{s}', '{s}')",
                .{self.store_name, key_buf, self.field_data, self.field_data, self.field_data, self.field_data, self.field_data, self.field_data, self.field_data, self.field_data, self.field_data, self.field_data}
            );
            defer self.allocator.free(insert_sql);

            const reply = try self.client.execute(insert_sql);
            self.allocator.free(reply);

            const doc_key = try std.fmt.parseInt(u128, &key_buf, 16);
            try self.keys.append(self.allocator, doc_key);

            if (inserted > 0 and inserted % 10_000 == 0) {
                const elapsed = metrics.milliTimestamp() - start_time;
                const ops_per_sec = (@as(f64, @floatFromInt(inserted)) * 1000.0) / @as(f64, @floatFromInt(elapsed));
                std.debug.print("Progress: {d}/{d} records ({d:.0} ops/sec)\n", .{
                    inserted,
                    self.config.record_count,
                    ops_per_sec,
                });
            }
        }

        const total_time = metrics.milliTimestamp() - start_time;
        const ops_per_sec = (@as(f64, @floatFromInt(inserted)) * 1000.0) / @as(f64, @floatFromInt(total_time));
        std.debug.print("\nLoad complete: {d} records in {d}ms ({d:.2} ops/sec)\n", .{
            inserted,
            total_time,
            ops_per_sec,
        });
    }

    pub fn run(self: *WorkloadC) !void {
        std.debug.print("\n=== Workload C: Transaction Phase ===\n", .{});
        std.debug.print("Operation mix: 100% reads\n", .{});
        std.debug.print("Operations: {d}\n", .{self.config.operation_count});
        std.debug.print("Distribution: Zipfian\n\n", .{});

        if (self.config.warmup_ops > 0) {
            try self.runWarmup();
        }

        self.metrics_tracker.reset();

        const start_time = metrics.milliTimestamp();
        var completed: usize = 0;

        while (completed < self.config.operation_count) : (completed += 1) {
            const op_start = metrics.microTimestamp();

            const result = try self.executeRead();

            const op_latency = metrics.microTimestamp() - op_start;
            try self.metrics_tracker.record(.read, op_latency, result);

            if (completed > 0 and completed % 10_000 == 0) {
                const elapsed = metrics.milliTimestamp() - start_time;
                const ops_per_sec = (@as(f64, @floatFromInt(completed)) * 1000.0) / @as(f64, @floatFromInt(elapsed));
                std.debug.print("Progress: {d}/{d} ops ({d:.0} ops/sec)\n", .{
                    completed,
                    self.config.operation_count,
                    ops_per_sec,
                });
            }
        }

        const total_time = metrics.milliTimestamp() - start_time;
        try self.printResults(total_time);
    }

    fn runWarmup(self: *WorkloadC) !void {
        std.debug.print("Warmup: {d} operations...\n", .{self.config.warmup_ops});
        var i: usize = 0;
        while (i < self.config.warmup_ops) : (i += 1) {
            _ = try self.executeRead();
        }
        std.debug.print("Warmup complete\n\n", .{});
    }

    fn executeRead(self: *WorkloadC) !bool {
        if (self.keys.items.len == 0) return false;
        const key_idx = self.key_distribution.next() % self.keys.items.len;
        const doc_key = self.keys.items[key_idx];

        var key_buf: [32]u8 = undefined;
        _ = try std.fmt.bufPrint(&key_buf, "{x:0>32}", .{doc_key});

        const select_sql = try std.fmt.allocPrint(self.allocator,
            "SELECT id, field0 FROM {s} WHERE id = '{s}'",
            .{self.store_name, key_buf}
        );
        defer self.allocator.free(select_sql);

        const reply = self.client.execute(select_sql) catch |err| {
            std.debug.print("Read error: {}\n", .{err});
            return false;
        };
        self.allocator.free(reply);
        return true;
    }

    fn printResults(self: *WorkloadC, total_time_ms: i64) !void {
        std.debug.print("\n============================================================\n", .{});
        std.debug.print("Workload C Results\n", .{});
        std.debug.print("============================================================\n\n", .{});

        const total_ops = self.metrics_tracker.total_operations;
        const throughput = (@as(f64, @floatFromInt(total_ops)) * 1000.0) / @as(f64, @floatFromInt(total_time_ms));

        std.debug.print("Total Operations:  {d}\n", .{total_ops});
        std.debug.print("Duration:          {d}ms ({d:.2}s)\n", .{ total_time_ms, @as(f64, @floatFromInt(total_time_ms)) / 1000.0 });
        std.debug.print("Throughput:        {d:.2} ops/sec\n\n", .{throughput});

        try self.metrics_tracker.printOperationStats(.read);

        std.debug.print("\n============================================================\n", .{});
    }
};
