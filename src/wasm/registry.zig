//! Registry of wasm scalar functions (embed-wasm.md sections 9, 10). This is the catalog map
//! that `CREATE FUNCTION ... LANGUAGE wasm` populates and the query executor's `evalFunc`
//! looks up. It owns the compiled `WasmScalarFn` for each name. A function carries a version
//! that bumps on re-registration, so a re-`CREATE` never silently reuses stale lowered code or
//! stale derived data keyed by version (section 9). In-memory here; WAL-backed persistence of
//! the source bytes is a later slice.
//!
//! Thread-safety: the registry is not internally locked. kaidb holds one behind the same
//! catalog latch it already uses for schema objects; lookups return a `*WasmScalarFn` whose
//! decoded module is immutable and safe to invoke concurrently (each call makes its own
//! instance, see udf.zig).

const std = @import("std");
const udf = @import("udf.zig");

pub const Registry = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged(Entry) = .{},

    pub const Entry = struct {
        func: udf.WasmScalarFn,
        version: u32,
    };

    pub fn init(alloc: std.mem.Allocator) Registry {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *Registry) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.func.deinit();
            self.alloc.free(kv.key_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    /// Register (or replace) a wasm scalar function. Decoding and validation happen here, so a
    /// bad module is rejected at registration, not at call time. Replacing an existing name
    /// deinits the old function and bumps the version.
    pub fn register(self: *Registry, name: []const u8, wasm_bytes: []const u8, policy: udf.Policy, returns_string: bool) !void {
        // Registry-count cap (embed-wasm.md hardening P1-7): reject a NEW name once the registry is
        // full; replacing an existing name is always allowed.
        if (self.map.getPtr(name) == null and self.map.count() >= udf.MAX_REGISTERED) return error.TooManyRegistered;

        var new_fn = try udf.WasmScalarFn.init(self.alloc, wasm_bytes, policy);
        errdefer new_fn.deinit();
        new_fn.returns_string = returns_string;

        if (self.map.getPtr(name)) |existing| {
            existing.func.deinit();
            existing.func = new_fn;
            existing.version +%= 1;
            return;
        }

        const key = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(key);
        try self.map.put(self.alloc, key, .{ .func = new_fn, .version = 1 });
    }

    /// Look up a registered function by name, or null. The returned pointer stays valid until
    /// the function is dropped or re-registered.
    pub fn get(self: *Registry, name: []const u8) ?*udf.WasmScalarFn {
        if (self.map.getPtr(name)) |e| return &e.func;
        return null;
    }

    /// The current version of a registered function, or null if absent.
    pub fn versionOf(self: *Registry, name: []const u8) ?u32 {
        if (self.map.getPtr(name)) |e| return e.version;
        return null;
    }

    /// Drop a function. Returns true if it existed.
    pub fn drop(self: *Registry, name: []const u8) bool {
        if (self.map.fetchRemove(name)) |kv| {
            var e = kv.value;
            e.func.deinit();
            self.alloc.free(kv.key);
            return true;
        }
        return false;
    }
};

/// Registry of custom wasm aggregates (embed-wasm.md M5), populated by `CREATE AGGREGATE`.
/// Parallel to [`Registry`] but holds a [`udf.WasmAggFn`]: an immutable decoded module the
/// executor spins a per-group [`udf.Aggregator`] over. Same threading contract as [`Registry`].
pub const AggRegistry = struct {
    alloc: std.mem.Allocator,
    map: std.StringHashMapUnmanaged(Entry) = .{},

    pub const Entry = struct {
        func: udf.WasmAggFn,
        version: u32,
    };

    pub fn init(alloc: std.mem.Allocator) AggRegistry {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *AggRegistry) void {
        var it = self.map.iterator();
        while (it.next()) |kv| {
            kv.value_ptr.func.deinit();
            self.alloc.free(kv.key_ptr.*);
        }
        self.map.deinit(self.alloc);
    }

    /// Register (or replace) a wasm aggregate. Decode and export validation happen here, so a
    /// module missing accumulate/finalise is rejected at registration.
    pub fn register(self: *AggRegistry, name: []const u8, wasm_bytes: []const u8, policy: udf.Policy) !void {
        if (self.map.getPtr(name) == null and self.map.count() >= udf.MAX_REGISTERED) return error.TooManyRegistered;

        var new_fn = try udf.WasmAggFn.init(self.alloc, wasm_bytes, policy);
        errdefer new_fn.deinit();

        if (self.map.getPtr(name)) |existing| {
            existing.func.deinit();
            existing.func = new_fn;
            existing.version +%= 1;
            return;
        }

        const key = try self.alloc.dupe(u8, name);
        errdefer self.alloc.free(key);
        try self.map.put(self.alloc, key, .{ .func = new_fn, .version = 1 });
    }

    /// Look up a registered aggregate by name, or null.
    pub fn get(self: *AggRegistry, name: []const u8) ?*udf.WasmAggFn {
        if (self.map.getPtr(name)) |e| return &e.func;
        return null;
    }

    /// Drop an aggregate. Returns true if it existed.
    pub fn drop(self: *AggRegistry, name: []const u8) bool {
        if (self.map.fetchRemove(name)) |kv| {
            var e = kv.value;
            e.func.deinit();
            self.alloc.free(kv.key);
            return true;
        }
        return false;
    }
};
