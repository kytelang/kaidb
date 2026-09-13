const std = @import("std");
const Allocator = std.mem.Allocator;
const Random = std.Random;

pub const ValueType = enum {
    fixed,
    variable,
    json,
};

pub const ValueConfig = struct {
    value_type: ValueType = .fixed,
    size: usize = 1024,
    min_size: usize = 100,
    max_size: usize = 10240,
    field_count: usize = 10,
    field_length: usize = 10,
};

pub const ValueGenerator = struct {
    allocator: Allocator,
    random: Random,
    config: ValueConfig,

    const ALPHA_NUMERIC = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    const ALPHA = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

    pub fn init(allocator: Allocator, random: Random, config: ValueConfig) ValueGenerator {
        return .{
            .allocator = allocator,
            .random = random,
            .config = config,
        };
    }

    pub fn generate(self: *ValueGenerator) ![]u8 {
        return switch (self.config.value_type) {
            .fixed => try self.generateFixed(self.config.size),
            .variable => try self.generateVariable(),
            .json => try self.generateJson(),
        };
    }

    pub fn generateFixed(self: *ValueGenerator, size: usize) ![]u8 {
        const value = try self.allocator.alloc(u8, size);
        for (value) |*c| {
            c.* = ALPHA_NUMERIC[self.random.uintLessThan(usize, ALPHA_NUMERIC.len)];
        }
        return value;
    }

    pub fn generateVariable(self: *ValueGenerator) ![]u8 {
        const size = self.config.min_size + self.random.uintLessThan(usize, self.config.max_size - self.config.min_size);
        return try self.generateFixed(size);
    }

    pub fn generateJson(self: *ValueGenerator) ![]u8 {
        var buffer = std.array_list.Managed(u8).init(self.allocator);
        errdefer buffer.deinit();

        try buffer.appendSlice("{");

        var i: usize = 0;
        while (i < self.config.field_count) : (i += 1) {
            if (i > 0) try buffer.appendSlice(",");

            try buffer.appendSlice("\"field");
            const field_name = try std.fmt.allocPrint(self.allocator, "{d}", .{i});
            defer self.allocator.free(field_name);
            try buffer.appendSlice(field_name);
            try buffer.appendSlice("\":");

            const field_type = self.random.uintLessThan(usize, 4);
            switch (field_type) {
                0 => {
                    try buffer.appendSlice("\"");
                    const rand_str = try self.generateRandomString(self.config.field_length);
                    defer self.allocator.free(rand_str);
                    try buffer.appendSlice(rand_str);
                    try buffer.appendSlice("\"");
                },
                1 => {
                    const num = try std.fmt.allocPrint(self.allocator, "{d}", .{self.random.int(i64)});
                    defer self.allocator.free(num);
                    try buffer.appendSlice(num);
                },
                2 => {
                    if (self.random.boolean()) {
                        try buffer.appendSlice("true");
                    } else {
                        try buffer.appendSlice("false");
                    }
                },
                3 => {
                    try buffer.appendSlice("null");
                },
                else => unreachable,
            }
        }

        try buffer.appendSlice("}");
        return buffer.toOwnedSlice();
    }

    pub fn generateJsonWithFields(self: *ValueGenerator, fields: []const []const u8, values: []const []const u8) ![]u8 {
        var buffer = std.array_list.Managed(u8).init(self.allocator);
        errdefer buffer.deinit();

        try buffer.appendSlice("{");

        for (fields, 0..) |field, i| {
            if (i > 0) try buffer.appendSlice(",");
            const entry = try std.fmt.allocPrint(self.allocator, "\"{s}\":\"{s}\"", .{ field, values[i] });
            defer self.allocator.free(entry);
            try buffer.appendSlice(entry);
        }

        try buffer.appendSlice("}");
        return buffer.toOwnedSlice();
    }

    pub fn generateUserDocument(self: *ValueGenerator, user_id: u64) ![]u8 {
        var buffer = std.array_list.Managed(u8).init(self.allocator);
        errdefer buffer.deinit();

        try buffer.appendSlice("{");

        const user_id_str = try std.fmt.allocPrint(self.allocator, "\"user_id\":{d},", .{user_id});
        defer self.allocator.free(user_id_str);
        try buffer.appendSlice(user_id_str);

        try buffer.appendSlice("\"name\":\"");
        const name = try self.generateRandomString(10);
        defer self.allocator.free(name);
        try buffer.appendSlice(name);
        try buffer.appendSlice("\",");

        try buffer.appendSlice("\"email\":\"");
        const email_prefix = try self.generateRandomString(8);
        defer self.allocator.free(email_prefix);
        try buffer.appendSlice(email_prefix);
        try buffer.appendSlice("@example.com\",");

        const age_str = try std.fmt.allocPrint(self.allocator, "\"age\":{d},", .{18 + self.random.uintLessThan(u32, 60)});
        defer self.allocator.free(age_str);
        try buffer.appendSlice(age_str);

        const active_str = try std.fmt.allocPrint(self.allocator, "\"active\":{s},", .{if (self.random.boolean()) "true" else "false"});
        defer self.allocator.free(active_str);
        try buffer.appendSlice(active_str);

        const ts_sec = @divTrunc(@import("metrics.zig").milliTimestamp(), 1000);
        const ts_str = try std.fmt.allocPrint(self.allocator, "\"created_at\":{d},", .{ts_sec});
        defer self.allocator.free(ts_str);
        try buffer.appendSlice(ts_str);

        try buffer.appendSlice("\"tags\":[");
        const tag_count = 1 + self.random.uintLessThan(usize, 5);
        var i: usize = 0;
        while (i < tag_count) : (i += 1) {
            if (i > 0) try buffer.appendSlice(",");
            const tag = try std.fmt.allocPrint(self.allocator, "\"tag{d}\"", .{i});
            defer self.allocator.free(tag);
            try buffer.appendSlice(tag);
        }
        try buffer.appendSlice("],");

        try buffer.appendSlice("\"metadata\":{");
        try buffer.appendSlice("\"source\":\"benchmark\",");
        try buffer.appendSlice("\"version\":1");
        try buffer.appendSlice("}");

        try buffer.appendSlice("}");
        return buffer.toOwnedSlice();
    }

    pub fn generateTimeSeriesPoint(self: *ValueGenerator, timestamp: i64, sensor_id: u32) ![]u8 {
        var buffer = std.array_list.Managed(u8).init(self.allocator);
        errdefer buffer.deinit();

        try buffer.appendSlice("{");

        const ts_str = try std.fmt.allocPrint(self.allocator, "\"timestamp\":{d},", .{timestamp});
        defer self.allocator.free(ts_str);
        try buffer.appendSlice(ts_str);

        const sensor_str = try std.fmt.allocPrint(self.allocator, "\"sensor_id\":{d},", .{sensor_id});
        defer self.allocator.free(sensor_str);
        try buffer.appendSlice(sensor_str);

        const temp_str = try std.fmt.allocPrint(self.allocator, "\"temperature\":{d:.2},", .{20.0 + self.random.float(f64) * 15.0});
        defer self.allocator.free(temp_str);
        try buffer.appendSlice(temp_str);

        const humidity_str = try std.fmt.allocPrint(self.allocator, "\"humidity\":{d:.2},", .{30.0 + self.random.float(f64) * 40.0});
        defer self.allocator.free(humidity_str);
        try buffer.appendSlice(humidity_str);

        const pressure_str = try std.fmt.allocPrint(self.allocator, "\"pressure\":{d:.2}", .{990.0 + self.random.float(f64) * 30.0});
        defer self.allocator.free(pressure_str);
        try buffer.appendSlice(pressure_str);

        try buffer.appendSlice("}");

        return buffer.toOwnedSlice();
    }

    fn generateRandomString(self: *ValueGenerator, length: usize) ![]u8 {
        const str = try self.allocator.alloc(u8, length);
        for (str) |*c| {
            c.* = ALPHA[self.random.uintLessThan(usize, ALPHA.len)];
        }
        return str;
    }

    pub fn generatePadded(self: *ValueGenerator, base_json: []const u8, target_size: usize) ![]u8 {
        if (base_json.len >= target_size) {
            return try self.allocator.dupe(u8, base_json);
        }

        const padding_needed = target_size - base_json.len - 20;
        var buffer = std.array_list.Managed(u8).init(self.allocator);
        errdefer buffer.deinit();

        try buffer.appendSlice(base_json[0 .. base_json.len - 1]);

        try buffer.appendSlice(",\"padding\":\"");

        var i: usize = 0;
        while (i < padding_needed) : (i += 1) {
            try buffer.append('x');
        }

        try buffer.appendSlice("\"}");

        return buffer.toOwnedSlice();
    }
};

test "fixed value generation" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const config = ValueConfig{ .value_type = .fixed, .size = 100 };
    var gen = ValueGenerator.init(std.testing.allocator, random, config);

    const value = try gen.generate();
    defer std.testing.allocator.free(value);

    try std.testing.expectEqual(@as(usize, 100), value.len);
}

test "variable value generation" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const config = ValueConfig{ .value_type = .variable, .min_size = 50, .max_size = 150 };
    var gen = ValueGenerator.init(std.testing.allocator, random, config);

    const value = try gen.generate();
    defer std.testing.allocator.free(value);

    try std.testing.expect(value.len >= 50 and value.len < 150);
}

test "json generation" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const config = ValueConfig{ .value_type = .json, .field_count = 5 };
    var gen = ValueGenerator.init(std.testing.allocator, random, config);

    const value = try gen.generate();
    defer std.testing.allocator.free(value);

    try std.testing.expect(value.len > 0);
    try std.testing.expect(value[0] == '{');
    try std.testing.expect(value[value.len - 1] == '}');
}

test "user document generation" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();

    const config = ValueConfig{};
    var gen = ValueGenerator.init(std.testing.allocator, random, config);

    const value = try gen.generateUserDocument(12345);
    defer std.testing.allocator.free(value);

    try std.testing.expect(value.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, value, "user_id") != null);
}
