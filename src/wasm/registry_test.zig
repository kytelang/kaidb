const std = @import("std");
const Registry = @import("registry.zig").Registry;

test "registry: register, call, version bump, drop" {
    const alloc = std.testing.allocator;
    var reg = Registry.init(alloc);
    defer reg.deinit();

    const bytes = @embedFile("testdata_add.wasm");
    try reg.register("myadd", bytes, .{});
    try std.testing.expectEqual(@as(?u32, 1), reg.versionOf("myadd"));

    // Look up and call through the registry.
    const f = reg.get("myadd").?;
    try std.testing.expectEqual(@as(i64, 42), try f.callI64("add", &.{ 40, 2 }));

    // Re-register bumps the version (a re-CREATE FUNCTION).
    try reg.register("myadd", bytes, .{});
    try std.testing.expectEqual(@as(?u32, 2), reg.versionOf("myadd"));

    // Unknown name is null; drop removes it.
    try std.testing.expect(reg.get("nope") == null);
    try std.testing.expect(reg.drop("myadd"));
    try std.testing.expect(reg.get("myadd") == null);
    try std.testing.expect(!reg.drop("myadd"));
}
