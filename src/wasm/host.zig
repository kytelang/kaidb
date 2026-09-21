//! kaidb WASM row-facing host ABI (embed-wasm.md M3, the pull model of section 6.3).
//!
//! A scalar UDF (M1/M2) is self-contained: it imports nothing and receives its arguments
//! marshalled into linear memory. A row-facing UDF is different. It runs as a filter or an
//! aggregate over a scan and reads the current row on demand, calling back into the host for
//! each column it actually touches. Those callbacks are the imports below, all in the "kaidb"
//! module namespace:
//!
//!   (import "kaidb" "col_i64"     (func (param i32) (result i64)))   ;; column index -> value
//!   (import "kaidb" "col_f64"     (func (param i32) (result f64)))
//!   (import "kaidb" "col_is_null" (func (param i32) (result i32)))   ;; 1 if the cell is SQL NULL
//!   (import "kaidb" "col_bytes"   (func (param i32 i32) (result i32))) ;; idx, dst_ptr -> len (-1 if NULL)
//!
//! This is the surface that must be audited hardest (section 6.3): it hands guest code a
//! read-only window onto the row the executor is currently positioned on. Two invariants make
//! it safe. First, the imports are read-only: they only ever read the bound row, never mutate
//! storage. Second, `col_bytes` writes into guest memory only after bounds-checking the
//! guest-supplied destination pointer against the live memory size (section 6.4), so a guest
//! that lies about a pointer traps rather than corrupting the host.
//!
//! The engine stays decoupled from kaidb's row types: the host functions read through a
//! `RowCtx` vtable, and the executor (`iterator.zig`) supplies the concrete implementation over
//! its `TableRow`. Columns are addressed by position (the row's schema order), matching the
//! design's index-based ABI.

const std = @import("std");
const zware = @import("engine/main.zig");

const VirtualMachine = zware.VirtualMachine;
const WasmError = zware.WasmError;

/// The read-only view a row-facing UDF sees of the current scan row. The executor builds one
/// per row over its positional cells and hands the host functions a pointer to it (as the
/// host-function context). Column access is by zero-based position in the row's schema order.
///
/// A column accessor returns `error.BadColumnIndex` for an out-of-range index, which the host
/// function turns into a wasm trap: a UDF written against the wrong arity fails loudly rather
/// than reading adjacent memory. Type coercion follows kaidb's usual scalar rules (an integer
/// cell reads as f64 for `col_f64`, etc.); a value that cannot be coerced reads as 0 / empty,
/// with `col_is_null` the reliable way to distinguish a real SQL NULL.
pub const RowCtx = struct {
    ptr: *anyopaque,
    vt: *const VTable,

    pub const ColError = error{BadColumnIndex};

    pub const VTable = struct {
        /// Number of columns in the bound row (for the guest to bound its own loops if it wants).
        col_count: *const fn (ptr: *anyopaque) u32,
        /// The column as a signed 64-bit integer (booleans as 0/1, NULL as 0).
        col_i64: *const fn (ptr: *anyopaque, idx: u32) ColError!i64,
        /// The column as an IEEE-754 double (NULL as 0).
        col_f64: *const fn (ptr: *anyopaque, idx: u32) ColError!f64,
        /// Whether the column is SQL NULL.
        col_is_null: *const fn (ptr: *anyopaque, idx: u32) ColError!bool,
        /// The column's bytes (its TEXT form), or null for SQL NULL. Borrowed for the call only.
        col_bytes: *const fn (ptr: *anyopaque, idx: u32) ColError!?[]const u8,
    };

    pub fn colCount(self: *const RowCtx) u32 {
        return self.vt.col_count(self.ptr);
    }
    pub fn colI64(self: *const RowCtx, idx: u32) ColError!i64 {
        return self.vt.col_i64(self.ptr, idx);
    }
    pub fn colF64(self: *const RowCtx, idx: u32) ColError!f64 {
        return self.vt.col_f64(self.ptr, idx);
    }
    pub fn colIsNull(self: *const RowCtx, idx: u32) ColError!bool {
        return self.vt.col_is_null(self.ptr, idx);
    }
    pub fn colBytes(self: *const RowCtx, idx: u32) ColError!?[]const u8 {
        return self.vt.col_bytes(self.ptr, idx);
    }
};

/// The module namespace every row-facing import must live in.
pub const NAMESPACE = "kaidb";

/// The allowlisted host-import names. A row-facing module may import any subset of these from
/// `NAMESPACE` and nothing else; an import outside this set is rejected at registration
/// (`udf.zig`), which is how the sandbox keeps clocks, randomness, WASI, and every other
/// nondeterministic or escape-prone host call off the table (section 7).
pub const ALLOWED = [_][]const u8{ "col_count", "col_i64", "col_f64", "col_is_null", "col_bytes" };

/// Whether `name` is an allowlisted row-facing host import.
pub fn isAllowed(name: []const u8) bool {
    for (ALLOWED) |a| {
        if (std.mem.eql(u8, a, name)) return true;
    }
    return false;
}

/// Expose the whole row-facing host-function set on `store`, bound to `rc`. Called by the
/// row-facing call path (`udf.zig`) after creating the per-call store and before instantiating,
/// so the guest's `kaidb.*` imports resolve to these. Exposing a function the guest does not
/// import is harmless: zware only wires the imports the module actually declares. The context
/// each function receives is `@intFromPtr(rc)`, recovered inside the host function.
pub fn expose(store: *zware.Store, rc: *const RowCtx) !void {
    const ctx: usize = @intFromPtr(rc);
    const i32p = [_]zware.ValType{.I32};
    const i32x2 = [_]zware.ValType{ .I32, .I32 };
    try store.exposeHostFunction(NAMESPACE, "col_count", hostColCount, ctx, &.{}, &.{.I32});
    try store.exposeHostFunction(NAMESPACE, "col_i64", hostColI64, ctx, &i32p, &.{.I64});
    try store.exposeHostFunction(NAMESPACE, "col_f64", hostColF64, ctx, &i32p, &.{.F64});
    try store.exposeHostFunction(NAMESPACE, "col_is_null", hostColIsNull, ctx, &i32p, &.{.I32});
    try store.exposeHostFunction(NAMESPACE, "col_bytes", hostColBytes, ctx, &i32x2, &.{.I32});
}

fn ctxOf(context: usize) *const RowCtx {
    return @ptrFromInt(context);
}

fn hostColCount(vm: *VirtualMachine, context: usize) WasmError!void {
    const rc = ctxOf(context);
    try vm.pushOperand(u32, rc.colCount());
}

fn hostColI64(vm: *VirtualMachine, context: usize) WasmError!void {
    const rc = ctxOf(context);
    const idx = vm.popOperand(u32);
    const v = rc.colI64(idx) catch return error.Trap;
    try vm.pushOperand(i64, v);
}

fn hostColF64(vm: *VirtualMachine, context: usize) WasmError!void {
    const rc = ctxOf(context);
    const idx = vm.popOperand(u32);
    const v = rc.colF64(idx) catch return error.Trap;
    try vm.pushOperand(f64, v);
}

fn hostColIsNull(vm: *VirtualMachine, context: usize) WasmError!void {
    const rc = ctxOf(context);
    const idx = vm.popOperand(u32);
    const is_null = rc.colIsNull(idx) catch return error.Trap;
    try vm.pushOperand(i32, if (is_null) 1 else 0);
}

fn hostColBytes(vm: *VirtualMachine, context: usize) WasmError!void {
    const rc = ctxOf(context);
    // Params were pushed left-to-right (idx, dst_ptr), so dst_ptr is on top.
    const dst_ptr = vm.popOperand(u32);
    const idx = vm.popOperand(u32);
    const maybe = rc.colBytes(idx) catch return error.Trap;
    const bytes = maybe orelse {
        // SQL NULL: nothing written, length -1 so the guest can distinguish it from an empty string.
        try vm.pushOperand(i32, -1);
        return;
    };
    const mem = try vm.inst.getMemory(0);
    const buf = mem.memory();
    // Never trust the guest-supplied destination pointer (section 6.4): bounds-check before write.
    if (@as(usize, dst_ptr) + bytes.len > buf.len) return error.OutOfBoundsMemoryAccess;
    @memcpy(buf[dst_ptr .. dst_ptr + bytes.len], bytes);
    try vm.pushOperand(i32, @intCast(bytes.len));
}
