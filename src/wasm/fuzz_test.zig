//! kaidb WASM engine fuzz and determinism suite (embed-wasm.md M6, sections 7 and 13).
//!
//! The sandbox's core promise is that hostile or malformed input can only be rejected or trapped,
//! never crash, hang, or leak the host, and that a UDF's output is a deterministic function of its
//! inputs so it is safe to persist and replay. This file gates both:
//!
//!   - a fuzzer over decode/validate/execute (random bytes, mutated valid modules, and a small
//!     adversarial corpus of infinite loops, deep recursion, and memory bombs), and
//!   - determinism-differential and replay tests that assert bit-identical results (including the
//!     canonical NaN) across independent instances and re-decodes.
//!
//! Determinism note for the fuzzer itself: the PRNG is seeded with a fixed constant so a failing
//! run reproduces exactly. The `std.testing.allocator` under every test is a leak-checking GPA, so
//! "never leak" is enforced by construction; the per-call fuel budget and memory-page cap bound
//! time and space, so "never hang" and "never blow memory" hold even for an adversarial input.

const std = @import("std");
const udf = @import("udf.zig");

/// A tight fuel/memory policy for fuzzing: small enough that an adversarial loop or memory bomb
/// terminates almost immediately, so thousands of iterations stay fast.
const FUZZ_POLICY = udf.Policy{ .fuel = 200_000, .memory_pages = 8 };

test "fuzz: random bytes never crash decode/validate/execute" {
    const alloc = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0xC0FFEE_D00D_F00D);
    const rand = prng.random();

    var buf: [256]u8 = undefined;
    var i: usize = 0;
    while (i < 8000) : (i += 1) {
        const n = rand.intRangeAtMost(usize, 0, buf.len);
        rand.bytes(buf[0..n]);
        // A quarter of the inputs start with a valid magic+version so the fuzzer reaches past the
        // header check and exercises the section decoders, not just the first reject.
        if (n >= 8 and (i & 3) == 0) {
            const hdr = [_]u8{ 0x00, 0x61, 0x73, 0x6d, 0x01, 0x00, 0x00, 0x00 };
            @memcpy(buf[0..8], &hdr);
        }
        // Decode + validate. Almost every input is rejected; the point is that it is rejected
        // (an error), never a crash. A rare input decodes to a trivial/empty module.
        if (udf.WasmScalarFn.init(alloc, buf[0..n], FUZZ_POLICY)) |*ok| {
            var m = ok.*;
            defer m.deinit();
            // It decoded; try to invoke the conventional entry under the tight budget. Any
            // outcome (result, trap, missing export) is acceptable; a crash/hang/leak is not.
            _ = m.callI64(udf.WasmScalarFn.ENTRY, &.{0}) catch {};
        } else |_| {}
    }
}

test "fuzz: single-bit mutations of a valid module trap or reject, never crash" {
    const alloc = std.testing.allocator;
    const base = @embedFile("testdata_add.wasm");
    var prng = std.Random.DefaultPrng.init(0xBADC0DE_1234);
    const rand = prng.random();

    var i: usize = 0;
    while (i < 8000) : (i += 1) {
        const buf = try alloc.dupe(u8, base);
        defer alloc.free(buf);
        // Apply 1..4 random single-byte mutations. This corrupts opcodes, section lengths, type
        // indices, and LEB128 fields, which is where a naive decoder crashes on a bad length or
        // an out-of-range index.
        const muts = rand.intRangeAtMost(usize, 1, 4);
        var m: usize = 0;
        while (m < muts) : (m += 1) {
            const pos = rand.intRangeLessThan(usize, 0, buf.len);
            buf[pos] = rand.int(u8);
        }
        if (udf.WasmScalarFn.init(alloc, buf, FUZZ_POLICY)) |*ok| {
            var mod = ok.*;
            defer mod.deinit();
            _ = mod.callI64("add", &.{ rand.int(i32), rand.int(i32) }) catch {};
            _ = mod.callI64(udf.WasmScalarFn.ENTRY, &.{0}) catch {};
        } else |_| {}
    }
}

test "fuzz: adversarial corpus terminates in a trap, never a hang or crash" {
    const alloc = std.testing.allocator;
    // Each of these is a legitimately-decoding module whose execution is hostile; the fuel budget
    // and memory cap must turn each into a trap, not a hang or an OOM.
    const corpus = [_][]const u8{
        @embedFile("testdata_recurse.wasm"), // unbounded recursion -> control-stack overflow / fuel
        @embedFile("testdata_membomb.wasm"), // memory.grow in an infinite loop -> mem cap / fuel
    };
    for (corpus) |bytes| {
        var fn_ = try udf.WasmScalarFn.init(alloc, bytes, FUZZ_POLICY);
        defer fn_.deinit();
        // Must error (trap / fuel / control-stack / memory), never return normally and never hang.
        try std.testing.expect(std.meta.isError(fn_.callI64(udf.WasmScalarFn.ENTRY, &.{0})));
    }

    // The `spin` export of testdata_add loops forever; a bounded budget must trap it.
    {
        var fn_ = try udf.WasmScalarFn.init(alloc, @embedFile("testdata_add.wasm"), FUZZ_POLICY);
        defer fn_.deinit();
        var no_in = [_]u64{};
        var no_out = [_]u64{};
        try std.testing.expectError(error.FuelExhausted, fn_.callRaw("spin", no_in[0..], no_out[0..]));
    }
}

test "determinism: float results are bit-identical across independent instances and re-decodes" {
    const alloc = std.testing.allocator;
    const bytes = @embedFile("testdata_float_bits.wasm");

    // The module returns the i64 bit-pattern of a computed f64: arg 0 -> 1.0/3.0, arg !=0 -> a NaN
    // (which canonical-NaN mode forces to the canonical quiet NaN). Determinism means every one of
    // these executions yields the SAME bits.
    var primary = try udf.WasmScalarFn.init(alloc, bytes, .{});
    defer primary.deinit();

    // A second module decoded from the same source stands in for a replica / a post-recovery
    // reload: it must produce bit-identical output (embed-wasm.md section 7, the replay premise).
    var replica = try udf.WasmScalarFn.init(alloc, bytes, .{});
    defer replica.deinit();

    inline for ([_]i64{ 0, 1 }) |sel| {
        const a = try primary.callI64(udf.WasmScalarFn.ENTRY, &.{sel});
        const b = try primary.callI64(udf.WasmScalarFn.ENTRY, &.{sel}); // same instance, fresh call
        const c = try replica.callI64(udf.WasmScalarFn.ENTRY, &.{sel}); // independent re-decode
        try std.testing.expectEqual(a, b);
        try std.testing.expectEqual(a, c);
    }

    // Concretely: the NaN path (sel != 0) returns exactly the canonical quiet NaN bit pattern, not
    // some implementation-defined NaN payload, so two replicas can never diverge on it.
    const nan_bits = try primary.callI64(udf.WasmScalarFn.ENTRY, &.{1});
    try std.testing.expectEqual(@as(i64, @bitCast(@as(u64, 0x7FF8_0000_0000_0000))), nan_bits);

    // And the ordinary value is the exact IEEE-754 double 1.0/3.0.
    const third_bits = try primary.callI64(udf.WasmScalarFn.ENTRY, &.{0});
    try std.testing.expectEqual(@as(i64, @bitCast(@as(f64, 1.0) / @as(f64, 3.0))), third_bits);
}

test "replay: re-decoding and re-running a module reproduces the derived value" {
    const alloc = std.testing.allocator;
    const bytes = @embedFile("testdata_udf_str.wasm"); // sums the bytes of a string argument

    // Run once and record the "derived data" (as a computed column or index key would be).
    var before = try udf.WasmScalarFn.init(alloc, bytes, .{});
    const recorded = try before.callString("kaidb");
    before.deinit();

    // Simulate a crash + recovery: the module is gone from memory and re-decoded from its stored
    // source bytes, then the same input is replayed. The result must match what was persisted.
    var after = try udf.WasmScalarFn.init(alloc, bytes, .{});
    defer after.deinit();
    const replayed = try after.callString("kaidb");

    try std.testing.expectEqual(recorded, replayed);
}
