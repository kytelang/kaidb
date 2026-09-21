const std = @import("std");
const udf = @import("udf.zig");

test "scalar UDF: numeric call returns the result" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_add.wasm"), .{});
    defer fn_.deinit();

    // add(40, 2) == 42
    const r = try fn_.callI64("add", &.{ 40, 2 });
    try std.testing.expectEqual(@as(i64, 42), r);

    // Reusing the same compiled module for another call (fresh instance each time).
    const r2 = try fn_.callI64("add", &.{ 100, 23 });
    try std.testing.expectEqual(@as(i64, 123), r2);
}

test "scalar UDF: fuel budget bounds a call" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_add.wasm"), .{ .fuel = 10_000 });
    defer fn_.deinit();

    // `spin` loops forever and returns nothing; the fuel budget must make it trap, not hang.
    var no_in = [_]u64{};
    var no_out = [_]u64{};
    const r = fn_.callRaw("spin", no_in[0..], no_out[0..]);
    try std.testing.expectError(error.FuelExhausted, r);
}

test "string UDF: byte-sum via linear-memory marshalling" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_udf_str.wasm"), .{});
    defer fn_.deinit();

    // "ABC" = 65 + 66 + 67 = 198; the host marshals the string into guest memory and the UDF
    // sums the bytes, proving the write landed correctly.
    try std.testing.expectEqual(@as(i64, 198), try fn_.callString("ABC"));
    // Empty string sums to 0.
    try std.testing.expectEqual(@as(i64, 0), try fn_.callString(""));
    // A different string to be sure it is not a fixed value.
    try std.testing.expectEqual(@as(i64, 'h' + 'i'), try fn_.callString("hi"));
}

test "hardening: malformed and hostile modules are rejected or trapped, never crash" {
    const alloc = std.testing.allocator;

    // Non-wasm garbage: rejected at decode, no crash.
    const garbage = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try std.testing.expect(std.meta.isError(udf.WasmScalarFn.init(alloc, &garbage, .{})));

    // Empty input: rejected.
    try std.testing.expect(std.meta.isError(udf.WasmScalarFn.init(alloc, "", .{})));

    // Truncated valid module (just the magic + version): rejected.
    const magic_only = [_]u8{ 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00 };
    // decode may accept an empty module; instantiate/invoke of a missing export must still fail.
    if (udf.WasmScalarFn.init(alloc, &magic_only, .{})) |*ok| {
        var m = ok.*;
        defer m.deinit();
        try std.testing.expect(std.meta.isError(m.callI64("kaidb_udf", &.{0})));
    } else |_| {}

    // Infinite recursion under a fuel budget: traps (fuel or control-stack overflow), never
    // hangs or crashes the host.
    {
        var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_recurse.wasm"), .{ .fuel = 200_000 });
        defer fn_.deinit();
        try std.testing.expect(std.meta.isError(fn_.callI64("kaidb_udf", &.{0})));
    }
}

test "string-result UDF: uppercase returns a string via packed pointer" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_udf_upper.wasm"), .{});
    defer fn_.deinit();
    fn_.returns_string = true;

    var out: [64]u8 = undefined;
    const r = try fn_.call(&.{.{ .str = "abc" }}, out[0..]);
    try std.testing.expect(r == .str_len);
    try std.testing.expectEqualStrings("ABC", out[0..r.str_len]);

    // A mixed shape works too: an int arg passed alongside would spread as one slot; here just
    // confirm a different string produces a different result.
    const r2 = try fn_.call(&.{.{ .str = "Hi!" }}, out[0..]);
    try std.testing.expectEqualStrings("HI!", out[0..r2.str_len]);
}
