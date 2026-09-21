//! kaidb WASM scalar UDF wrapper (embed-wasm.md M1).
//!
//! This is the seam the SQL executor calls to run a registered wasm function. It follows the
//! module/instance split from the design: a module is decoded once (the reusable "compiled"
//! form) and held immutably; every call spins up a fresh Store + Instance so the guest's
//! linear memory and stacks start clean, then runs the exported function under a per-call fuel
//! budget and memory-page cap. Numeric scalar UDFs need no linear-memory marshalling: zware
//! passes u64 arguments and results directly, so this slice covers int and float scalar UDFs.
//! Variable-length arguments (string, bytes) come with the frame codec in a later slice.

const std = @import("std");
const zware = @import("engine/main.zig");

/// Per-call resource policy (embed-wasm.md section 5). Defaults are conservative; the SQL
/// layer overrides them from the function's `WITH (...)` options and the server config.
pub const Policy = struct {
    /// Instruction budget. The VM traps with error.FuelExhausted when spent.
    fuel: u64 = 10_000_000,
    /// Hard linear-memory ceiling in 64 KiB pages, independent of what the module declares.
    memory_pages: u32 = 256, // 16 MiB
};

/// A decoded wasm module registered as a scalar function. Immutable after `init`, so one
/// `WasmScalarFn` can be shared across kaidb's query threads and reused for every row.
pub const WasmScalarFn = struct {
    alloc: std.mem.Allocator,
    /// Owned copy of the source bytes; the decoded Module borrows from these.
    bytes: []u8,
    module: zware.Module,
    policy: Policy,

    pub fn init(alloc: std.mem.Allocator, wasm_bytes: []const u8, policy: Policy) !WasmScalarFn {
        const owned = try alloc.dupe(u8, wasm_bytes);
        errdefer alloc.free(owned);

        var module = zware.Module.init(alloc, owned);
        errdefer module.deinit();
        try module.decode();

        return .{ .alloc = alloc, .bytes = owned, .module = module, .policy = policy };
    }

    pub fn deinit(self: *WasmScalarFn) void {
        self.module.deinit();
        self.alloc.free(self.bytes);
    }

    /// Call the exported function `name` with numeric arguments, returning its single result.
    /// A fresh instance is created and torn down per call (pooling comes later), so guest state
    /// never leaks between rows. The call runs under the module's fuel budget and memory cap.
    ///
    /// `in` and `out` carry raw 64-bit slots: an i32/i64 argument is its two's-complement bits,
    /// an f32/f64 argument is its IEEE bits (use @bitCast at the call site). The executor maps
    /// kaidb column values to these slots.
    pub fn callRaw(self: *WasmScalarFn, name: []const u8, in: []u64, out: []u64) !void {
        var store = zware.Store.init(self.alloc);
        defer store.deinit();

        var instance = zware.Instance.init(self.alloc, &store, self.module);
        try instance.instantiate();
        defer instance.deinit();

        // Reject modules that need host imports: a scalar UDF must be self-contained so it
        // instantiates with no host and cannot reach outside its sandbox.
        for (instance.module.imports.list.items) |_| return error.HostImportsNotAllowed;

        try instance.limitMemoryPages(self.policy.memory_pages);

        instance.fuel_budget = self.policy.fuel;
        try instance.invoke(name, in, out, .{});
    }

    /// Convenience for the common int-in / int-out UDF shape.
    pub fn callI64(self: *WasmScalarFn, name: []const u8, args: []const i64) !i64 {
        // zware caps invoke arity via fixed-size stacks; a handful of args is plenty for a
        // scalar UDF. Copy the args into a small buffer of u64 slots.
        var in_buf: [16]u64 = undefined;
        if (args.len > in_buf.len) return error.TooManyArgs;
        for (args, 0..) |a, i| in_buf[i] = @bitCast(a);
        var out = [1]u64{0};
        try self.callRaw(name, in_buf[0..args.len], out[0..]);
        return @bitCast(out[0]);
    }
};
