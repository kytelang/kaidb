const std = @import("std");
const zware = @import("engine/main.zig");

// Each test sets up Store/Module/Instance inline: an Instance holds self-referential state
// after instantiate(), so it must not be moved by value.

test "decode, instantiate and invoke a trivial module" {
    const alloc = std.testing.allocator;
    var store = zware.Store.init(alloc);
    defer store.deinit();
    var module = zware.Module.init(alloc, @embedFile("testdata_add.wasm"));
    defer module.deinit();
    try module.decode();
    var instance = zware.Instance.init(alloc, &store, module);
    try instance.instantiate();
    defer instance.deinit();

    var in = [2]u64{ 40, 2 };
    var out = [1]u64{0};
    try instance.invoke("add", in[0..], out[0..], .{});
    try std.testing.expectEqual(@as(u64, 42), out[0]);
}

test "fuel: a bounded budget still runs a small function" {
    const alloc = std.testing.allocator;
    var store = zware.Store.init(alloc);
    defer store.deinit();
    var module = zware.Module.init(alloc, @embedFile("testdata_add.wasm"));
    defer module.deinit();
    try module.decode();
    var instance = zware.Instance.init(alloc, &store, module);
    try instance.instantiate();
    defer instance.deinit();

    var out = [1]u64{0};
    instance.fuel_budget = 1000;
    try instance.invoke("answer", &.{}, out[0..], .{});
    try std.testing.expectEqual(@as(u64, 42), out[0]);
}

test "fuel: an infinite loop traps with FuelExhausted" {
    const alloc = std.testing.allocator;
    var store = zware.Store.init(alloc);
    defer store.deinit();
    var module = zware.Module.init(alloc, @embedFile("testdata_add.wasm"));
    defer module.deinit();
    try module.decode();
    var instance = zware.Instance.init(alloc, &store, module);
    try instance.instantiate();
    defer instance.deinit();

    // `spin` is `loop br 0` forever; a small budget must trap rather than hang.
    instance.fuel_budget = 10_000;
    const r = instance.invoke("spin", &.{}, &.{}, .{});
    try std.testing.expectError(error.FuelExhausted, r);
}

test "memory cap: growth past the imposed ceiling fails" {
    const alloc = std.testing.allocator;
    var store = zware.Store.init(alloc);
    defer store.deinit();
    var module = zware.Module.init(alloc, @embedFile("testdata_add.wasm"));
    defer module.deinit();
    try module.decode();
    var instance = zware.Instance.init(alloc, &store, module);
    try instance.instantiate();
    defer instance.deinit();

    // Module declares no maximum; impose a hard cap of 2 pages. Memory starts at 1 page.
    try instance.limitMemoryPages(2);

    var in5 = [1]u64{5};
    var out = [1]u64{0};
    try instance.invoke("grow", in5[0..], out[0..], .{});
    // memory.grow returns -1 (0xFFFFFFFF as u32) when it cannot grow.
    try std.testing.expectEqual(@as(u64, 0xFFFF_FFFF), out[0] & 0xFFFF_FFFF);

    // Growing by 1 (1 -> 2 pages) is within the cap and returns the old size (1).
    var in1 = [1]u64{1};
    var out2 = [1]u64{0};
    try instance.invoke("grow", in1[0..], out2[0..], .{});
    try std.testing.expectEqual(@as(u64, 1), out2[0]);
}

test "determinism: NaN results are canonicalised" {
    const alloc = std.testing.allocator;
    var store = zware.Store.init(alloc);
    defer store.deinit();
    var module = zware.Module.init(alloc, @embedFile("testdata_add.wasm"));
    defer module.deinit();
    try module.decode();
    var instance = zware.Instance.init(alloc, &store, module);
    try instance.instantiate();
    defer instance.deinit();

    const CANON_F64: u64 = 0x7FF8_0000_0000_0000;
    var out = [1]u64{0};
    try instance.invoke("nan_div_bits", &.{}, out[0..], .{});
    try std.testing.expectEqual(CANON_F64, out[0]);

    var out2 = [1]u64{0};
    try instance.invoke("nan_sqrt_bits", &.{}, out2[0..], .{});
    try std.testing.expectEqual(CANON_F64, out2[0]);
}
