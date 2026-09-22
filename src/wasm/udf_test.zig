const std = @import("std");
const udf = @import("udf.zig");
const host = @import("host.zig");

/// A minimal in-test row the row-facing host functions read through, standing in for the
/// executor's `TableRow`. Column values are just i64s (with a parallel text form for `col_bytes`).
const MockRow = struct {
    ints: []const i64,
    texts: []const ?[]const u8 = &.{},

    fn colCount(ptr: *anyopaque) u32 {
        const self: *const MockRow = @ptrCast(@alignCast(ptr));
        return @intCast(self.ints.len);
    }
    fn colI64(ptr: *anyopaque, idx: u32) host.RowCtx.ColError!i64 {
        const self: *const MockRow = @ptrCast(@alignCast(ptr));
        if (idx >= self.ints.len) return error.BadColumnIndex;
        return self.ints[idx];
    }
    fn colF64(ptr: *anyopaque, idx: u32) host.RowCtx.ColError!f64 {
        const self: *const MockRow = @ptrCast(@alignCast(ptr));
        if (idx >= self.ints.len) return error.BadColumnIndex;
        return @floatFromInt(self.ints[idx]);
    }
    fn colIsNull(ptr: *anyopaque, idx: u32) host.RowCtx.ColError!bool {
        const self: *const MockRow = @ptrCast(@alignCast(ptr));
        if (idx >= self.ints.len) return error.BadColumnIndex;
        return false;
    }
    fn colBytes(ptr: *anyopaque, idx: u32) host.RowCtx.ColError!?[]const u8 {
        const self: *const MockRow = @ptrCast(@alignCast(ptr));
        if (idx >= self.texts.len) return error.BadColumnIndex;
        return self.texts[idx];
    }

    const vtable = host.RowCtx.VTable{
        .col_count = colCount,
        .col_i64 = colI64,
        .col_f64 = colF64,
        .col_is_null = colIsNull,
        .col_bytes = colBytes,
    };

    fn ctx(self: *const MockRow) host.RowCtx {
        return .{ .ptr = @constCast(@ptrCast(self)), .vt = &vtable };
    }
};

test "row-facing UDF: pulls columns via kaidb host imports" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_udf_row.wasm"), .{});
    defer fn_.deinit();
    // The module imports kaidb.col_i64, so it is classified as row-facing at init.
    try std.testing.expect(fn_.row_facing);

    var out: [64]u8 = undefined;

    // col0 + col1 == 42 -> keep (1).
    const keep = MockRow{ .ints = &.{ 40, 2 } };
    var rc = keep.ctx();
    const r = try fn_.callRow(&rc, out[0..]);
    try std.testing.expectEqual(udf.WasmScalarFn.Result{ .int = 1 }, r);

    // col0 + col1 != 42 -> drop (0). A fresh instance, so no state leaks between rows.
    const drop = MockRow{ .ints = &.{ 40, 3 } };
    var rc2 = drop.ctx();
    const r2 = try fn_.callRow(&rc2, out[0..]);
    try std.testing.expectEqual(udf.WasmScalarFn.Result{ .int = 0 }, r2);
}

test "row-facing UDF: a bad column index traps, never reads out of bounds" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_udf_row.wasm"), .{});
    defer fn_.deinit();

    var out: [64]u8 = undefined;
    // The module reads columns 0 and 1; a one-column row makes col_i64(1) trap.
    const short = MockRow{ .ints = &.{7} };
    var rc = short.ctx();
    try std.testing.expectError(error.Trap, fn_.callRow(&rc, out[0..]));
}

test "row-facing TEXT UDF: col_bytes into guest memory, returned as a string" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_udf_row_text.wasm"), .{});
    defer fn_.deinit();
    try std.testing.expect(fn_.row_facing);
    fn_.returns_string = true;

    var out: [64]u8 = undefined;
    const row = MockRow{ .ints = &.{0}, .texts = &.{"hello"} };
    var rc = row.ctx();
    const r = try fn_.callRow(&rc, out[0..]);
    try std.testing.expect(r == .str_len);
    try std.testing.expectEqualStrings("hello", out[0..r.str_len]);
}

test "hardening P1-7: an oversized module is rejected at init" {
    const alloc = std.testing.allocator;
    // A buffer just over the module-size cap is rejected before decode, so a hostile client cannot
    // exhaust host memory at registration time.
    const big = try alloc.alloc(u8, udf.MAX_MODULE_BYTES + 1);
    defer alloc.free(big);
    @memset(big, 0);
    try std.testing.expectError(error.ModuleTooLarge, udf.WasmScalarFn.init(alloc, big, .{}));
    try std.testing.expectError(error.ModuleTooLarge, udf.WasmAggFn.init(alloc, big, .{}));
}

test "hardening P1-6: an over-cap string result errors instead of truncating" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_view_row.wasm"), .{});
    defer fn_.deinit();
    fn_.returns_string = true;

    // The view renders ~48 bytes; a tiny output buffer must yield an explicit error, never a
    // silently truncated fragment.
    const row = MockRow{ .ints = &.{ 0, 0 }, .texts = &.{ "Hammer", "1299" } };
    var rc = row.ctx();
    var tiny: [8]u8 = undefined;
    try std.testing.expectError(error.OutputTooLarge, fn_.callRow(&rc, tiny[0..]));
}

test "KYX view: a row-facing UDF renders an HTML fragment from the row (M7)" {
    const alloc = std.testing.allocator;
    var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_view_row.wasm"), .{});
    defer fn_.deinit();
    try std.testing.expect(fn_.row_facing);
    fn_.returns_string = true;

    // The view reads column 0 (name) and column 1 (price) as text and assembles a <tr> fragment.
    const row = MockRow{ .ints = &.{ 0, 0 }, .texts = &.{ "Hammer", "1299" } };
    var rc = row.ctx();
    var out: [256]u8 = undefined;
    const r = try fn_.callRow(&rc, out[0..]);
    try std.testing.expect(r == .str_len);
    try std.testing.expectEqualStrings(
        "<tr><td>Hammer</td><td class=\"num\">1299</td></tr>",
        out[0..r.str_len],
    );
}

test "custom aggregate: accumulate across rows, finalise the group result" {
    const alloc = std.testing.allocator;
    var af = try udf.WasmAggFn.init(alloc, @embedFile("testdata_agg_sum.wasm"), .{});
    defer af.deinit();

    // One group: fold 10, 20, 12; the persistent instance carries the running sum, so finalise
    // returns 42. State lives in the guest across accumulate calls, unlike a scalar UDF.
    var aggr = try af.newAggregator();
    defer aggr.deinit();
    try aggr.accumulate(10);
    try aggr.accumulate(20);
    try aggr.accumulate(12);
    try std.testing.expectEqual(@as(i64, 42), try aggr.finalize());

    // A second aggregator is an independent group: fresh state, no leak from the first.
    var aggr2 = try af.newAggregator();
    defer aggr2.deinit();
    try aggr2.accumulate(5);
    try aggr2.accumulate(5);
    try std.testing.expectEqual(@as(i64, 10), try aggr2.finalize());

    // An empty group finalises to the aggregate's identity (0 for a sum).
    var aggr3 = try af.newAggregator();
    defer aggr3.deinit();
    try std.testing.expectEqual(@as(i64, 0), try aggr3.finalize());
}

test "custom aggregate: a module missing the required exports is rejected at init" {
    const alloc = std.testing.allocator;
    // The scalar-doubling module exports `kaidb_udf`, not the aggregate entry points.
    try std.testing.expectError(error.MissingAggregateExport, udf.WasmAggFn.init(alloc, @embedFile("testdata_add.wasm"), .{}));
}

test "row-facing classification: a non-allowlisted import is rejected at init" {
    const alloc = std.testing.allocator;
    // A module importing an unknown host function (env.foo) must be rejected: only the audited
    // kaidb.* column functions are allowed, so a UDF can never reach a clock, WASI, or randomness.
    // wasm: (module (import "env" "foo" (func (param i32))))
    const hostile = [_]u8{
        0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00, // magic + version
        0x01, 0x05, 0x01, 0x60, 0x01, 0x7f, 0x00, // type section: (func (param i32))
        0x02, 0x0b, 0x01, 0x03, 0x65, 0x6e, 0x76, 0x03, 0x66, 0x6f, 0x6f, 0x00, 0x00, // import env.foo
    };
    try std.testing.expectError(error.HostImportsNotAllowed, udf.WasmScalarFn.init(alloc, &hostile, .{}));
}

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
