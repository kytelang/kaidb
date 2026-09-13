const std = @import("std");
const Random = std.Random;

pub const DistributionType = enum {
    uniform,
    zipfian,
    latest,
};

pub const Distribution = union(DistributionType) {
    uniform: UniformDistribution,
    zipfian: ZipfianDistribution,
    latest: LatestDistribution,

    pub fn next(self: *Distribution) u64 {
        return switch (self.*) {
            .uniform => |*d| d.next(),
            .zipfian => |*d| d.next(),
            .latest => |*d| d.next(),
        };
    }

    pub fn nextInRange(self: *Distribution, min: u64, max: u64) u64 {
        return switch (self.*) {
            .uniform => |*d| d.nextInRange(min, max),
            .zipfian => |*d| d.nextInRange(min, max),
            .latest => |*d| d.nextInRange(min, max),
        };
    }
};

pub const UniformDistribution = struct {
    random: Random,
    min: u64,
    max: u64,

    pub fn init(random: Random, min: u64, max: u64) UniformDistribution {
        return .{
            .random = random,
            .min = min,
            .max = max,
        };
    }

    pub fn next(self: *UniformDistribution) u64 {
        if (self.max <= self.min) return self.min;
        const range = self.max - self.min;
        return self.min + (self.random.int(u64) % range);
    }

    pub fn nextInRange(self: *UniformDistribution, min: u64, max: u64) u64 {
        if (max <= min) return min;
        const range = max - min;
        return min + (self.random.int(u64) % range);
    }
};

pub const ZipfianDistribution = struct {
    random: Random,
    min: u64,
    max: u64,
    zipfian_constant: f64,
    theta: f64,
    zeta_n: f64,
    zeta_2: f64,
    alpha: f64,
    count_for_zeta: u64,
    eta: f64,

    const DEFAULT_ZIPFIAN_CONSTANT: f64 = 0.99;

    pub fn init(random: Random, min: u64, max: u64) ZipfianDistribution {
        return initWithConstant(random, min, max, DEFAULT_ZIPFIAN_CONSTANT);
    }

    pub fn initWithConstant(random: Random, min: u64, max: u64, zipfian_constant: f64) ZipfianDistribution {
        const items = max - min + 1;
        const theta = zipfian_constant;
        const zeta_2 = zeta(2, theta);
        const zeta_n = zeta(items, theta);
        const alpha = 1.0 / (1.0 - theta);
        const eta = (1.0 - std.math.pow(f64, 2.0 / @as(f64, @floatFromInt(items)), 1.0 - theta)) / (1.0 - zeta_2 / zeta_n);

        return .{
            .random = random,
            .min = min,
            .max = max,
            .zipfian_constant = zipfian_constant,
            .theta = theta,
            .zeta_n = zeta_n,
            .zeta_2 = zeta_2,
            .alpha = alpha,
            .count_for_zeta = items,
            .eta = eta,
        };
    }

    pub fn next(self: *ZipfianDistribution) u64 {
        const u = self.random.float(f64);
        const uz = u * self.zeta_n;

        if (uz < 1.0) {
            return self.min;
        }

        if (uz < 1.0 + std.math.pow(f64, 0.5, self.theta)) {
            return self.min + 1;
        }

        const ret = self.min + @as(u64, @intFromFloat(@as(f64, @floatFromInt(self.count_for_zeta)) * std.math.pow(f64, self.eta * u - self.eta + 1.0, self.alpha)));
        return @min(ret, self.max);
    }

    pub fn nextInRange(self: *ZipfianDistribution, min: u64, max: u64) u64 {
        const val = self.next();
        const range = self.max - self.min;
        const new_range = max - min;
        const offset = val - self.min;
        const scaled = (offset * new_range) / range;
        return min + scaled;
    }

    fn zeta(n: u64, theta: f64) f64 {
        var sum: f64 = 0.0;
        var i: u64 = 1;
        while (i <= n) : (i += 1) {
            sum += 1.0 / std.math.pow(f64, @as(f64, @floatFromInt(i)), theta);
        }
        return sum;
    }
};

pub const LatestDistribution = struct {
    random: Random,
    zipfian: ZipfianDistribution,
    max_key: u64,

    pub fn init(random: Random, max_key: u64) LatestDistribution {
        const zipfian = ZipfianDistribution.init(random, 0, max_key);
        return .{
            .random = random,
            .zipfian = zipfian,
            .max_key = max_key,
        };
    }

    pub fn updateMaxKey(self: *LatestDistribution, new_max: u64) void {
        if (new_max <= self.max_key) return;
        var i = self.zipfian.count_for_zeta + 1;
        while (i <= new_max + 1) : (i += 1) {
            self.zipfian.zeta_n += 1.0 / std.math.pow(f64, @as(f64, @floatFromInt(i)), self.zipfian.theta);
        }
        self.max_key = new_max;
        self.zipfian.max = new_max;
        self.zipfian.count_for_zeta = new_max + 1;
        const items_f: f64 = @as(f64, @floatFromInt(new_max + 1));
        self.zipfian.eta = (1.0 - std.math.pow(f64, 2.0 / items_f, 1.0 - self.zipfian.theta)) /
            (1.0 - self.zipfian.zeta_2 / self.zipfian.zeta_n);
    }

    pub fn next(self: *LatestDistribution) u64 {
        const zipf_val = self.zipfian.next();
        if (zipf_val > self.max_key) return 0;
        return self.max_key - zipf_val;
    }

    pub fn nextInRange(self: *LatestDistribution, min: u64, max: u64) u64 {
        const val = self.next();
        if (val < min) return min;
        if (val > max) return max;
        return val;
    }
};

pub fn createDistribution(dist_type: DistributionType, random: Random, min: u64, max: u64) Distribution {
    return switch (dist_type) {
        .uniform => .{ .uniform = UniformDistribution.init(random, min, max) },
        .zipfian => .{ .zipfian = ZipfianDistribution.init(random, min, max) },
        .latest => .{ .latest = LatestDistribution.init(random, max) },
    };
}

test "uniform distribution" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();
    
    var dist = UniformDistribution.init(random, 0, 100);

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const val = dist.next();
        try std.testing.expect(val >= 0 and val < 100);
    }
}

test "zipfian distribution" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();
    var dist = ZipfianDistribution.init(random, 0, 1000);

    var counts = [_]u64{0} ** 10;
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        const val = dist.next();
        try std.testing.expect(val >= 0 and val <= 1000);
        if (val < 10) {
            counts[val] += 1;
        }
    }

    try std.testing.expect(counts[0] > counts[5]);
}

test "latest distribution" {
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();
    var dist = LatestDistribution.init(random, 1000);

    var i: usize = 0;
    while (i < 100) : (i += 1) {
        const val = dist.next();
        try std.testing.expect(val <= 1000);
    }

    dist.updateMaxKey(2000);
    i = 0;
    while (i < 100) : (i += 1) {
        const val = dist.next();
        try std.testing.expect(val <= 2000);
    }
}
