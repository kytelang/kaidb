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
const host = @import("host.zig");

pub const RowCtx = host.RowCtx;

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
    /// Whether this UDF returns text (`CREATE FUNCTION ... RETURNS TEXT`). When true the guest's
    /// i64 return is a packed `(ptr << 32) | len` pointing at bytes in its own linear memory;
    /// when false it is a plain numeric value.
    returns_string: bool = false,
    /// Whether this is a row-facing UDF (embed-wasm.md M3): it imports the `kaidb.*` column
    /// host functions and reads the current scan row on demand rather than taking arguments.
    /// Detected at `init` by inspecting the module's imports; a row-facing UDF is invoked via
    /// `callRow` (bound to a `RowCtx`), a scalar UDF via `call`.
    row_facing: bool = false,

    /// A single UDF argument, mapped to wasm parameters by `call`: an int becomes one i64
    /// parameter, a string becomes two i32 parameters (ptr, len) after the host writes its
    /// bytes into the guest (embed-wasm.md section 6).
    pub const Arg = union(enum) { int: i64, str: []const u8 };

    /// A UDF result: a numeric value, or a string whose bytes were copied into the caller's
    /// output buffer (the returned usize is its length; the output-size cap is the buffer size).
    pub const Result = union(enum) { int: i64, str_len: usize };

    pub fn init(alloc: std.mem.Allocator, wasm_bytes: []const u8, policy: Policy) !WasmScalarFn {
        const owned = try alloc.dupe(u8, wasm_bytes);
        errdefer alloc.free(owned);

        var module = zware.Module.init(alloc, owned);
        errdefer module.deinit();
        try module.decode();

        // Classify the module by its imports (embed-wasm.md M3). A UDF that imports nothing is a
        // self-contained scalar UDF. A UDF that imports only the allowlisted `kaidb.*` column
        // functions is row-facing. Anything else (a foreign namespace, a non-function import, or
        // a `kaidb` name outside the allowlist) is rejected here, at CREATE FUNCTION time, so a
        // module that could reach a clock, randomness, WASI, or an unaudited host call never
        // becomes registered in the first place (section 7).
        var row_facing = false;
        for (module.imports.list.items) |imp| {
            if (imp.desc_tag != .Func or
                !std.mem.eql(u8, imp.module, host.NAMESPACE) or
                !host.isAllowed(imp.name))
            {
                return error.HostImportsNotAllowed;
            }
            row_facing = true;
        }

        return .{ .alloc = alloc, .bytes = owned, .module = module, .policy = policy, .row_facing = row_facing };
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

    /// The general scalar-UDF call (embed-wasm.md section 6): mixed int/string arguments and an
    /// int or string result. Each string argument is written into the guest's linear memory
    /// (via its exported `kaidb_alloc`) and passed as a `(ptr, len)` i32 pair; ints pass
    /// directly. If `returns_string` the guest's packed `(ptr, len)` result is bounds-checked
    /// and its bytes copied into `out_buf` (capped at the buffer size); otherwise the i64 is
    /// returned as-is. A fresh metered, memory-capped, host-import-free instance per call.
    pub fn call(self: *WasmScalarFn, args: []const Arg, out_buf: []u8) !Result {
        var store = zware.Store.init(self.alloc);
        defer store.deinit();

        var instance = zware.Instance.init(self.alloc, &store, self.module);
        try instance.instantiate();
        defer instance.deinit();

        for (instance.module.imports.list.items) |_| return error.HostImportsNotAllowed;
        instance.fuel_budget = self.policy.fuel;
        try instance.limitMemoryPages(self.policy.memory_pages);

        // Build the wasm parameter slots, allocating+writing each string into guest memory.
        var in_buf: [16]u64 = undefined;
        var n: usize = 0;
        for (args) |a| switch (a) {
            .int => |v| {
                if (n >= in_buf.len) return error.TooManyArgs;
                in_buf[n] = @bitCast(v);
                n += 1;
            },
            .str => |s| {
                if (n + 2 > in_buf.len) return error.TooManyArgs;
                var ai = [1]u64{@bitCast(@as(i64, @intCast(s.len)))};
                var ao = [1]u64{0};
                try instance.invoke(ALLOC_ENTRY, ai[0..], ao[0..], .{});
                const ptr: u32 = @truncate(ao[0]);
                const mem = try instance.getMemory(0);
                const buf = mem.memory();
                if (@as(usize, ptr) + s.len > buf.len) return error.OutOfBoundsMemoryAccess;
                @memcpy(buf[ptr .. ptr + s.len], s);
                in_buf[n] = ptr;
                in_buf[n + 1] = @intCast(s.len);
                n += 2;
            },
        };

        var out = [1]u64{0};
        try instance.invoke(ENTRY, in_buf[0..n], out[0..], .{});

        if (!self.returns_string) return .{ .int = @bitCast(out[0]) };

        // Unpack the (ptr << 32) | len result and copy the bytes out, bounds-checked. The host
        // never trusts the guest-supplied pointer (section 6.4).
        const rptr: u32 = @truncate(out[0] >> 32);
        const rlen: u32 = @truncate(out[0]);
        const mem = try instance.getMemory(0);
        const buf = mem.memory();
        if (@as(usize, rptr) + rlen > buf.len) return error.OutOfBoundsMemoryAccess;
        const copy_len = @min(@as(usize, rlen), out_buf.len);
        @memcpy(out_buf[0..copy_len], buf[rptr .. rptr + copy_len]);
        return .{ .str_len = copy_len };
    }

    /// Call a row-facing UDF against the current scan row (embed-wasm.md M3, the pull model of
    /// section 6.3). Unlike `call`, the arguments are not marshalled up front: the guest reads
    /// whichever columns it needs through the `kaidb.*` host imports, which read `rc` (the bound
    /// row) read-only. The exported entry `kaidb_udf()` takes no wasm parameters and returns a
    /// single i64 (a filter predicate returns 0/non-zero; a `RETURNS TEXT` row UDF returns a
    /// packed `(ptr << 32) | len` copied into `out_buf`).
    ///
    /// The host functions are exposed on a fresh per-call store bound to `rc`, then the module is
    /// instantiated so its imports resolve. Imports were already validated to be allowlisted at
    /// `init`; instantiation would in any case fail (`ImportNotFound`) for anything not exposed
    /// here, so a non-row-facing module cannot smuggle in an import through this path. The call
    /// runs under the same fuel budget and memory-page cap as a scalar UDF.
    pub fn callRow(self: *WasmScalarFn, rc: *const RowCtx, out_buf: []u8) !Result {
        var store = zware.Store.init(self.alloc);
        defer store.deinit();

        // Expose the column host functions BEFORE instantiate so the guest's imports bind to them.
        try host.expose(&store, rc);

        var instance = zware.Instance.init(self.alloc, &store, self.module);
        try instance.instantiate();
        defer instance.deinit();

        instance.fuel_budget = self.policy.fuel;
        try instance.limitMemoryPages(self.policy.memory_pages);

        var no_args = [0]u64{};
        var out = [1]u64{0};
        try instance.invoke(ENTRY, no_args[0..], out[0..], .{});

        if (!self.returns_string) return .{ .int = @bitCast(out[0]) };

        const rptr: u32 = @truncate(out[0] >> 32);
        const rlen: u32 = @truncate(out[0]);
        const mem = try instance.getMemory(0);
        const buf = mem.memory();
        if (@as(usize, rptr) + rlen > buf.len) return error.OutOfBoundsMemoryAccess;
        const copy_len = @min(@as(usize, rlen), out_buf.len);
        @memcpy(out_buf[0..copy_len], buf[rptr .. rptr + copy_len]);
        return .{ .str_len = copy_len };
    }

    /// The exported entry points a string-taking UDF module must provide.
    pub const ENTRY = "kaidb_udf";
    pub const ALLOC_ENTRY = "kaidb_alloc";

    /// Call a UDF that takes a single string argument and returns an i64 (embed-wasm.md M2,
    /// the push-model marshalling of section 6). The guest exports `kaidb_alloc(size)->ptr`
    /// and `memory`; the host allocates space in the guest, writes the bytes there
    /// bounds-checked, then calls `kaidb_udf(ptr, len)`. A fresh metered instance per call.
    /// This is the string substrate; a full multi-arg frame codec on `src/proto/wire.zig`
    /// comes later, but a single string argument covers the common text-UDF and KYX cases.
    pub fn callString(self: *WasmScalarFn, s: []const u8) !i64 {
        var store = zware.Store.init(self.alloc);
        defer store.deinit();

        var instance = zware.Instance.init(self.alloc, &store, self.module);
        try instance.instantiate();
        defer instance.deinit();

        // A string UDF still imports nothing (it provides its own allocator and memory), so a
        // host import is rejected exactly as for the numeric path.
        for (instance.module.imports.list.items) |_| return error.HostImportsNotAllowed;

        instance.fuel_budget = self.policy.fuel;
        try instance.limitMemoryPages(self.policy.memory_pages);

        // 1. Ask the guest to allocate space for the argument bytes.
        const len_i64: i64 = @intCast(s.len);
        var alloc_in = [1]u64{@bitCast(len_i64)};
        var alloc_out = [1]u64{0};
        try instance.invoke(ALLOC_ENTRY, alloc_in[0..], alloc_out[0..], .{});
        const ptr: u32 = @truncate(alloc_out[0]);

        // 2. Write the bytes into guest linear memory, bounds-checked (host never trusts the
        //    guest-returned pointer, section 6.4).
        const mem = try instance.getMemory(0);
        const buf = mem.memory();
        if (@as(usize, ptr) + s.len > buf.len) return error.OutOfBoundsMemoryAccess;
        @memcpy(buf[ptr .. ptr + s.len], s);

        // 3. Invoke the UDF with (ptr, len); it reads the frame and returns an i64.
        var in = [2]u64{ ptr, @bitCast(len_i64) };
        var out = [1]u64{0};
        try instance.invoke(ENTRY, in[0..], out[0..], .{});
        return @bitCast(out[0]);
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
