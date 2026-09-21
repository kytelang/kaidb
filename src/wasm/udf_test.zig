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
