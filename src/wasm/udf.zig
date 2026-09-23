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

/// Registration-time resource limits (embed-wasm.md hardening P1-7). A module larger than this,
/// or a registry that already holds this many entries, is rejected at `CREATE` so a hostile or
/// runaway client cannot exhaust host memory at decode time or grow a registry without bound.
pub const MAX_MODULE_BYTES: usize = 4 * 1024 * 1024; // 4 MiB of wasm is already a very large UDF
pub const MAX_REGISTERED: usize = 1024; // per registry (functions, aggregates, procedures)

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
        if (wasm_bytes.len > MAX_MODULE_BYTES) return error.ModuleTooLarge;
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
        // Never silently truncate a string result (embed-wasm.md hardening P1-6): a result that
        // does not fit the caller's buffer is an explicit error, not corrupt truncated bytes.
        if (@as(usize, rlen) > out_buf.len) return error.OutputTooLarge;
        @memcpy(out_buf[0..rlen], buf[rptr .. rptr + rlen]);
        return .{ .str_len = rlen };
    }

    /// The in-process call (embed-wasm.md section 11): like [`call`], but the guest additionally
    /// imports `kaidb.kaidb_exec` and may issue queries against the engine through `ec`, which runs
    /// them on the calling thread under the caller's transaction. Used by `CALL` so a stored
    /// procedure can read and write data. Arguments are marshalled exactly as in [`call`]; unlike
    /// `call` this path does NOT reject host imports (that is the point), but the only import
    /// exposed is `kaidb_exec`, so a module importing anything else fails to instantiate.
    pub fn callInProc(self: *WasmScalarFn, args: []const Arg, out_buf: []u8, ec: *const host.ExecCtx) !Result {
        var store = zware.Store.init(self.alloc);
        defer store.deinit();

        // Expose kaidb_exec BEFORE instantiate so the guest's import binds to it.
        try host.exposeExec(&store, ec);

        var instance = zware.Instance.init(self.alloc, &store, self.module);
        try instance.instantiate();
        defer instance.deinit();

        instance.fuel_budget = self.policy.fuel;
        try instance.limitMemoryPages(self.policy.memory_pages);

        // Build the wasm parameter slots, allocating+writing each string into guest memory (as in `call`).
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

        const rptr: u32 = @truncate(out[0] >> 32);
        const rlen: u32 = @truncate(out[0]);
        const mem = try instance.getMemory(0);
        const buf = mem.memory();
        if (@as(usize, rptr) + rlen > buf.len) return error.OutOfBoundsMemoryAccess;
        if (@as(usize, rlen) > out_buf.len) return error.OutputTooLarge;
        @memcpy(out_buf[0..rlen], buf[rptr .. rptr + rlen]);
        return .{ .str_len = rlen };
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
        // Never silently truncate a string result (embed-wasm.md hardening P1-6): a result that
        // does not fit the caller's buffer is an explicit error, not corrupt truncated bytes.
        if (@as(usize, rlen) > out_buf.len) return error.OutputTooLarge;
        @memcpy(out_buf[0..rlen], buf[rptr .. rptr + rlen]);
        return .{ .str_len = rlen };
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

/// A decoded wasm module registered as a custom aggregate (embed-wasm.md M5). Unlike a scalar
/// UDF, an aggregate keeps state across the rows of a group: the operator folds each row in with
/// `accumulate` and reads the group result once with `finalize`. That state lives in the guest's
/// own linear memory and globals, so an aggregate needs a *persistent* instance for the life of a
/// group, not the fresh-per-call instance a scalar UDF uses. `WasmAggFn` is the immutable decoded
/// module (shared across query threads); each group spins up an [`Aggregator`] over it.
///
/// The guest exports three functions (accumulate/finalise are required; init is optional):
///   (func (export "kaidb_agg_init"))                     ;; reset state (optional; a fresh
///                                                        ;; instance already starts zeroed)
///   (func (export "kaidb_agg_accumulate") (param i64))   ;; fold one row's value into state
///   (func (export "kaidb_agg_finalize") (result i64))    ;; produce the group result
/// `merge` (combining two partial states) is part of the design's triple but is only needed for
/// parallel/partitioned aggregation; the single-threaded operator here folds serially, so it is a
/// later addition. An aggregate module imports nothing: the value to fold is passed as a param.
pub const WasmAggFn = struct {
    alloc: std.mem.Allocator,
    bytes: []u8,
    module: zware.Module,
    policy: Policy,

    pub const INIT_ENTRY = "kaidb_agg_init";
    pub const ACC_ENTRY = "kaidb_agg_accumulate";
    pub const FIN_ENTRY = "kaidb_agg_finalize";

    pub fn init(alloc: std.mem.Allocator, wasm_bytes: []const u8, policy: Policy) !WasmAggFn {
        if (wasm_bytes.len > MAX_MODULE_BYTES) return error.ModuleTooLarge;
        const owned = try alloc.dupe(u8, wasm_bytes);
        errdefer alloc.free(owned);

        var module = zware.Module.init(alloc, owned);
        errdefer module.deinit();
        try module.decode();

        // An aggregate is self-contained: it folds a value passed as a parameter and keeps its
        // own state, so it imports nothing. Reject any import (a clock, WASI, randomness, or the
        // row ABI) at registration, exactly as a scalar UDF does.
        for (module.imports.list.items) |_| return error.HostImportsNotAllowed;

        // Both required exports must be present, so a bad module is a DDL error, not a surprise
        // mid-aggregation.
        _ = module.getExport(.Func, ACC_ENTRY) catch return error.MissingAggregateExport;
        _ = module.getExport(.Func, FIN_ENTRY) catch return error.MissingAggregateExport;

        return .{ .alloc = alloc, .bytes = owned, .module = module, .policy = policy };
    }

    pub fn deinit(self: *WasmAggFn) void {
        self.module.deinit();
        self.alloc.free(self.bytes);
    }

    /// Start a new aggregation over this module: a fresh persistent instance whose linear memory
    /// and globals hold the running state. Call [`Aggregator.accumulate`] once per row and
    /// [`Aggregator.finalize`] once at group end, then [`Aggregator.deinit`]. Heap-allocates the
    /// store and instance so their addresses stay stable while the instance borrows the store.
    pub fn newAggregator(self: *WasmAggFn) !*Aggregator {
        const agg = try self.alloc.create(Aggregator);
        errdefer self.alloc.destroy(agg);

        agg.alloc = self.alloc;
        agg.store = try self.alloc.create(zware.Store);
        errdefer self.alloc.destroy(agg.store);
        agg.store.* = zware.Store.init(self.alloc);
        errdefer agg.store.deinit();

        agg.instance = try self.alloc.create(zware.Instance);
        errdefer self.alloc.destroy(agg.instance);
        agg.instance.* = zware.Instance.init(self.alloc, agg.store, self.module);
        try agg.instance.instantiate();
        errdefer agg.instance.deinit();

        agg.instance.fuel_budget = self.policy.fuel;
        try agg.instance.limitMemoryPages(self.policy.memory_pages);

        // Optional explicit reset. A fresh instance already starts with zeroed memory and
        // globals, so a module that needs no custom initial state can omit `kaidb_agg_init`.
        if (self.module.getExport(.Func, INIT_ENTRY)) |_| {
            var no_in = [0]u64{};
            var no_out = [0]u64{};
            try agg.instance.invoke(INIT_ENTRY, no_in[0..], no_out[0..], .{});
        } else |_| {}

        return agg;
    }
};

/// A live aggregation over a [`WasmAggFn`] for one group. Owns a persistent instance whose state
/// survives between `accumulate` calls; `finalize` reads the group result. Each `accumulate`/
/// `finalize` invoke gets a fresh fuel budget (a single row's fold cannot exhaust the whole
/// aggregation's fuel), while the guest's accumulator state persists across them.
pub const Aggregator = struct {
    alloc: std.mem.Allocator,
    store: *zware.Store,
    instance: *zware.Instance,

    /// Fold one row's (already type-coerced) i64 value into the running state.
    pub fn accumulate(self: *Aggregator, value: i64) !void {
        var in = [1]u64{@bitCast(value)};
        var no_out = [0]u64{};
        try self.instance.invoke(WasmAggFn.ACC_ENTRY, in[0..], no_out[0..], .{});
    }

    /// Produce the group's result from the accumulated state.
    pub fn finalize(self: *Aggregator) !i64 {
        var no_in = [0]u64{};
        var out = [1]u64{0};
        try self.instance.invoke(WasmAggFn.FIN_ENTRY, no_in[0..], out[0..], .{});
        return @bitCast(out[0]);
    }

    pub fn deinit(self: *Aggregator) void {
        self.instance.deinit();
        self.alloc.destroy(self.instance);
        self.store.deinit();
        self.alloc.destroy(self.store);
        self.alloc.destroy(self);
    }
};
