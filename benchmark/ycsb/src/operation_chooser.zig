const std = @import("std");
const Random = std.Random;

pub const OperationType = enum {
    read,
    insert,
    update,
    delete,
    scan,
    read_modify_write,

    pub fn toString(self: OperationType) []const u8 {
        return switch (self) {
            .read => "READ",
            .insert => "INSERT",
            .update => "UPDATE",
            .delete => "DELETE",
            .scan => "SCAN",
            .read_modify_write => "RMW",
        };
    }
};

pub const OperationMix = struct {
    read_proportion: f64 = 0.5,
    insert_proportion: f64 = 0.0,
    update_proportion: f64 = 0.5,
    delete_proportion: f64 = 0.0,
    scan_proportion: f64 = 0.0,
    read_modify_write_proportion: f64 = 0.0,

    pub fn validate(self: OperationMix) !void {
        const total = self.read_proportion + self.insert_proportion +
                     self.update_proportion + self.delete_proportion +
                     self.scan_proportion + self.read_modify_write_proportion;

        if (@abs(total - 1.0) > 0.01) {
            return error.InvalidProportions;
        }
    }

    pub fn normalize(self: *OperationMix) void {
        const total = self.read_proportion + self.insert_proportion +
                     self.update_proportion + self.delete_proportion +
                     self.scan_proportion + self.read_modify_write_proportion;

        if (total == 0.0) return;

        self.read_proportion /= total;
        self.insert_proportion /= total;
        self.update_proportion /= total;
        self.delete_proportion /= total;
        self.scan_proportion /= total;
        self.read_modify_write_proportion /= total;
    }

    pub fn readOnly() OperationMix {
        return .{
            .read_proportion = 1.0,
            .insert_proportion = 0.0,
            .update_proportion = 0.0,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn writeOnly() OperationMix {
        return .{
            .read_proportion = 0.0,
            .insert_proportion = 1.0,
            .update_proportion = 0.0,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn balanced() OperationMix {
        return .{
            .read_proportion = 0.5,
            .insert_proportion = 0.5,
            .update_proportion = 0.0,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn workloadA() OperationMix {
        return .{
            .read_proportion = 0.5,
            .insert_proportion = 0.0,
            .update_proportion = 0.5,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn workloadB() OperationMix {
        return .{
            .read_proportion = 0.95,
            .insert_proportion = 0.0,
            .update_proportion = 0.05,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn workloadC() OperationMix {
        return readOnly();
    }

    pub fn workloadD() OperationMix {
        return .{
            .read_proportion = 0.95,
            .insert_proportion = 0.05,
            .update_proportion = 0.0,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn workloadE() OperationMix {
        return .{
            .read_proportion = 0.0,
            .insert_proportion = 0.05,
            .update_proportion = 0.0,
            .delete_proportion = 0.0,
            .scan_proportion = 0.95,
            .read_modify_write_proportion = 0.0,
        };
    }

    pub fn workloadF() OperationMix {
        return .{
            .read_proportion = 0.5,
            .insert_proportion = 0.0,
            .update_proportion = 0.0,
            .delete_proportion = 0.0,
            .scan_proportion = 0.0,
            .read_modify_write_proportion = 0.5,
        };
    }
};

pub const OperationChooser = struct {
    random: Random,
    mix: OperationMix,

    read_threshold: f64,
    insert_threshold: f64,
    update_threshold: f64,
    delete_threshold: f64,
    scan_threshold: f64,
    rmw_threshold: f64,

    pub fn init(random: Random, mix: OperationMix) !OperationChooser {
        try mix.validate();

        const read_threshold = mix.read_proportion;
        const insert_threshold = read_threshold + mix.insert_proportion;
        const update_threshold = insert_threshold + mix.update_proportion;
        const delete_threshold = update_threshold + mix.delete_proportion;
        const scan_threshold = delete_threshold + mix.scan_proportion;
        const rmw_threshold = scan_threshold + mix.read_modify_write_proportion;

        return .{
            .random = random,
            .mix = mix,
            .read_threshold = read_threshold,
            .insert_threshold = insert_threshold,
            .update_threshold = update_threshold,
            .delete_threshold = delete_threshold,
            .scan_threshold = scan_threshold,
            .rmw_threshold = rmw_threshold,
        };
    }

    pub fn choose(self: *OperationChooser) OperationType {
        const rand_val = self.random.float(f64);

        if (rand_val < self.read_threshold) {
            return .read;
        } else if (rand_val < self.insert_threshold) {
            return .insert;
        } else if (rand_val < self.update_threshold) {
            return .update;
        } else if (rand_val < self.delete_threshold) {
            return .delete;
        } else if (rand_val < self.scan_threshold) {
            return .scan;
        } else if (rand_val < self.rmw_threshold) {
            return .read_modify_write;
        }

        return .read;
    }

    pub fn getStatistics(self: *OperationChooser, operation_count: usize) OperationStats {
        var stats = OperationStats{};

        var i: usize = 0;
        while (i < operation_count) : (i += 1) {
            const op = self.choose();
            switch (op) {
                .read => stats.read_count += 1,
                .insert => stats.insert_count += 1,
                .update => stats.update_count += 1,
                .delete => stats.delete_count += 1,
                .scan => stats.scan_count += 1,
                .read_modify_write => stats.rmw_count += 1,
            }
        }

        return stats;
    }
};

pub const OperationStats = struct {
    read_count: usize = 0,
    insert_count: usize = 0,
    update_count: usize = 0,
    delete_count: usize = 0,
    scan_count: usize = 0,
    rmw_count: usize = 0,

    pub fn total(self: OperationStats) usize {
        return self.read_count + self.insert_count + self.update_count +
               self.delete_count + self.scan_count + self.rmw_count;
    }

    pub fn format(
        self: OperationStats,
        comptime fmt: []const u8,
        options: std.fmt.FormatOptions,
        writer: anytype,
    ) !void {
        _ = fmt;
        _ = options;

        const t = self.total();
        if (t == 0) {
            try writer.writeAll("No operations");
            return;
        }

        try writer.print("READ: {d} ({d:.1}%), ", .{
            self.read_count,
            @as(f64, @floatFromInt(self.read_count)) * 100.0 / @as(f64, @floatFromInt(t))
        });
        try writer.print("INSERT: {d} ({d:.1}%), ", .{
            self.insert_count,
            @as(f64, @floatFromInt(self.insert_count)) * 100.0 / @as(f64, @floatFromInt(t))
        });
        try writer.print("UPDATE: {d} ({d:.1}%), ", .{
            self.update_count,
            @as(f64, @floatFromInt(self.update_count)) * 100.0 / @as(f64, @floatFromInt(t))
        });
        try writer.print("DELETE: {d} ({d:.1}%), ", .{
            self.delete_count,
            @as(f64, @floatFromInt(self.delete_count)) * 100.0 / @as(f64, @floatFromInt(t))
        });
        try writer.print("SCAN: {d} ({d:.1}%), ", .{
            self.scan_count,
            @as(f64, @floatFromInt(self.scan_count)) * 100.0 / @as(f64, @floatFromInt(t))
        });
        try writer.print("RMW: {d} ({d:.1}%)", .{
            self.rmw_count,
            @as(f64, @floatFromInt(self.rmw_count)) * 100.0 / @as(f64, @floatFromInt(t))
        });
    }
};

test "operation chooser read only" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const mix = OperationMix.readOnly();
    var chooser = try OperationChooser.init(random, mix);

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const op = chooser.choose();
        try std.testing.expectEqual(OperationType.read, op);
    }
}

test "operation chooser balanced" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const mix = OperationMix.balanced();
    var chooser = try OperationChooser.init(random, mix);

    const stats = chooser.getStatistics(1000);
    const total = stats.total();

    try std.testing.expectEqual(@as(usize, 1000), total);
    try std.testing.expect(stats.read_count > 400 and stats.read_count < 600);
    try std.testing.expect(stats.insert_count > 400 and stats.insert_count < 600);
}

test "workload A proportions" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const mix = OperationMix.workloadA();
    var chooser = try OperationChooser.init(random, mix);

    const stats = chooser.getStatistics(10000);
    const total = stats.total();

    const read_pct = @as(f64, @floatFromInt(stats.read_count)) / @as(f64, @floatFromInt(total));
    const update_pct = @as(f64, @floatFromInt(stats.update_count)) / @as(f64, @floatFromInt(total));

    try std.testing.expect(@abs(read_pct - 0.5) < 0.05);
    try std.testing.expect(@abs(update_pct - 0.5) < 0.05);
}
