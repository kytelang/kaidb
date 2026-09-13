//! Order-preserving, delimiter-safe binary encoding for fixed-width index-key
//! tokens.
//!
//! Background: a secondary-index key is `enc(c0):enc(c1):...:pk`, colon
//! delimited, and the scan iterators split on `:` (0x3A) and bound a value's pk
//! fan-out with the `;` (0x3B) / `\xFF` sentinels. The legacy token encoding
//! (see `types.encodeIndexValueAlloc`) wrote a fixed-width numeric column as 16
//! ASCII hex characters. Profiling (`profiling_sql_10m_findings.md`) showed
//! decoding those with `std.fmt.parseInt(_, 16)` was ~40% of an index-only GROUP
//! BY, and 16 bytes per value roughly doubled the key size (and with it the scan
//! I/O and on-disk footprint).
//!
//! This codec keeps the exact `:`-delimited framing, sentinels, and iterators
//! unchanged, and only changes the per-value token. The 64-bit order-preserving
//! transform is identical to the hex path (flip the sign bit for signed ints /
//! timestamps; IEEE-754 total-order flip for floats). The serialisation is a
//! fixed-width **10-byte base-128 big-endian** form: the 64-bit ordered value is
//! split into ten 7-bit groups, most-significant first, each stored as a byte
//! `0x80 | group`. Properties that make this a safe drop-in:
//!   - **Order-preserving:** base-128 big-endian compares lexically in value
//!     order, and adding the constant `0x80` to every digit preserves per-digit
//!     order, so a plain `memcmp` over the ten bytes reproduces numeric order.
//!   - **Delimiter-safe:** every byte is in `0x80..0xFF`, strictly above the
//!     `:`=0x3A and `;`=0x3B delimiters and the `"NULL"` sentinel (0x4E...), so a
//!     token can never collide with the key framing. The `\xFF` upper sentinel
//!     still sorts after any token byte.
//!   - **Cheap decode:** ten shifts/ORs, no `parseInt`.
//!
//! Width drops 16 -> 10 bytes per fixed-width value (37.5% smaller keys).

const std = @import("std");
const ColumnType = @import("types.zig").ColumnType;

/// Encoded width of one fixed-width numeric token, in bytes. ceil(64/7) = 10.
pub const ENC_WIDTH: usize = 10;

/// True when `ct` is a fixed-width numeric / timestamp type this codec encodes.
/// `BOOL`, `TEXT`, `BLOB` are not index-token-encoded by this path (bool is not
/// used as an ordered index token today; text/blob stay on the raw path).
pub fn isFixedNumeric(ct: ColumnType) bool {
    return switch (ct) {
        .UINT32, .UINT64, .INT32, .INT64, .TIMESTAMP, .FLOAT32, .FLOAT64 => true,
        .BOOL, .TEXT, .BLOB => false,
    };
}

/// The order-preserving 64-bit transform of a parsed integer: flip the sign bit
/// so signed order maps to unsigned byte order. Matches the legacy hex path.
fn orderedFromInt(v: i64) u64 {
    return @as(u64, @bitCast(v)) ^ (@as(u64, 1) << 63);
}
fn intFromOrdered(o: u64) i64 {
    return @bitCast(o ^ (@as(u64, 1) << 63));
}
/// IEEE-754 total order: invert all bits when negative, else set the sign bit.
fn orderedFromFloat(f: f64) u64 {
    const bits: u64 = @bitCast(f);
    return if (bits >> 63 == 1) ~bits else bits | (@as(u64, 1) << 63);
}
fn floatFromOrdered(o: u64) f64 {
    const bits: u64 = if (o >> 63 == 1) o ^ (@as(u64, 1) << 63) else ~o;
    return @bitCast(bits);
}

/// Write the 64-bit ordered value as 10 base-128 big-endian digits, each byte
/// `0x80 | digit`. `out` must be exactly `ENC_WIDTH` bytes.
fn writeOrdered(out: []u8, ordered: u64) void {
    std.debug.assert(out.len == ENC_WIDTH);
    var v = ordered;
    var i: usize = ENC_WIDTH;
    while (i > 0) {
        i -= 1;
        out[i] = 0x80 | @as(u8, @intCast(v & 0x7F));
        v >>= 7;
    }
}

/// Inverse of `writeOrdered`: read 10 base-128 digits back into the 64-bit
/// ordered value. Returns null if any byte is outside `0x80..0xFF` (not a valid
/// token, e.g. a `"NULL"` marker).
fn readOrdered(enc: []const u8) ?u64 {
    if (enc.len != ENC_WIDTH) return null;
    var v: u64 = 0;
    for (enc) |b| {
        if (b < 0x80) return null;
        v = (v << 7) | @as(u64, b & 0x7F);
    }
    return v;
}

/// Encode a fixed-width numeric column value (given as its decimal/float text,
/// as the engine carries column values) into a freshly allocated `ENC_WIDTH`
/// byte token. Returns null when `ct` is not fixed-numeric or the text does not
/// parse (the caller then falls back to duping the raw string, matching the old
/// "NULL"/non-numeric behaviour). The caller owns the returned slice.
pub fn encodeAlloc(allocator: std.mem.Allocator, ct: ColumnType, value_str: []const u8) !?[]u8 {
    if (!isFixedNumeric(ct)) return null;
    const ordered: u64 = switch (ct) {
        .UINT32, .UINT64, .INT32, .INT64, .TIMESTAMP => blk: {
            const v = std.fmt.parseInt(i64, value_str, 10) catch return null;
            break :blk orderedFromInt(v);
        },
        .FLOAT32, .FLOAT64 => blk: {
            const f = std.fmt.parseFloat(f64, value_str) catch return null;
            break :blk orderedFromFloat(f);
        },
        else => unreachable,
    };
    const out = try allocator.alloc(u8, ENC_WIDTH);
    writeOrdered(out, ordered);
    return out;
}

/// Decode a fixed-width token back to an f64 (the engine's numeric currency for
/// aggregates / comparisons), or null if `enc` is not a valid `ENC_WIDTH` token
/// for `ct`. Replaces the former `parseInt(enc, 16)` with direct base-128 reads.
pub fn decodeF64(enc: []const u8, ct: ColumnType) ?f64 {
    const ordered = readOrdered(enc) orelse return null;
    return switch (ct) {
        .UINT32, .UINT64, .INT32, .INT64, .TIMESTAMP => @floatFromInt(intFromOrdered(ordered)),
        .FLOAT32, .FLOAT64 => floatFromOrdered(ordered),
        else => null,
    };
}

// ------------------------------ tests ------------------------------

const testing = std.testing;

fn encA(a: std.mem.Allocator, ct: ColumnType, s: []const u8) ![]u8 {
    return (try encodeAlloc(a, ct, s)).?;
}

test "tokens are fixed-width, delimiter-safe, and byte-order == numeric order (int)" {
    const a = testing.allocator;
    const vals = [_]i64{ std.math.minInt(i64), -1_000_000, -1, 0, 1, 279, 1_000_000, std.math.maxInt(i64) };
    var prev: ?[]u8 = null;
    for (vals) |v| {
        var sbuf: [32]u8 = undefined;
        const enc = try encA(a, .INT64, try std.fmt.bufPrint(&sbuf, "{d}", .{v}));
        defer a.free(enc);
        try testing.expectEqual(ENC_WIDTH, enc.len);
        for (enc) |b| try testing.expect(b >= 0x80); // above ':' ';' '\xFF'-safe and "NULL"
        if (prev) |p| {
            try testing.expect(std.mem.order(u8, p, enc) == .lt);
            a.free(p);
        }
        prev = try a.dupe(u8, enc);
        try testing.expectEqual(@as(f64, @floatFromInt(v)), decodeF64(enc, .INT64).?);
    }
    if (prev) |p| a.free(p);
}

test "float tokens preserve order incl negatives and zero; round-trip" {
    const a = testing.allocator;
    const vals = [_]f64{ -1.0e30, -50000.25, -1.5, 0.0, 1.5, 10000.0, 50000.0, 1.0e30 };
    var prev: ?[]u8 = null;
    for (vals) |v| {
        var sbuf: [512]u8 = undefined;
        const enc = try encA(a, .FLOAT64, try std.fmt.bufPrint(&sbuf, "{d}", .{v}));
        defer a.free(enc);
        if (prev) |p| {
            try testing.expect(std.mem.order(u8, p, enc) == .lt);
            a.free(p);
        }
        prev = try a.dupe(u8, enc);
        try testing.expectEqual(v, decodeF64(enc, .FLOAT64).?);
    }
    if (prev) |p| a.free(p);
}

test "NULL / non-numeric and wrong width decode to null" {
    try testing.expectEqual(@as(?f64, null), decodeF64("NULL", .INT64)); // 4 bytes, not ENC_WIDTH
    try testing.expect((try encodeAlloc(testing.allocator, .TEXT, "abc")) == null);
    try testing.expect((try encodeAlloc(testing.allocator, .INT64, "NULL")) == null);
    // a 10-byte slice with a sub-0x80 byte is not a valid token
    try testing.expectEqual(@as(?f64, null), decodeF64(&[_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x3A }, .INT64));
}
