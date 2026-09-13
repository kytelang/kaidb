//! On-disk row encoding, decoding, and MVCC version-chain (de)serialisation.
//!
//! This file owns the physical byte layout of a single table row and of the
//! multi-version chain that MVCC keeps for one primary key. Everything the
//! B+Tree stores in a leaf cell for a given key ultimately passes through here,
//! so the invariants below are the contract every reader and writer of stored
//! rows must honour.
//!
//! ## Row layout: a fixed part plus a private heap
//!
//! A row is split into two contiguous byte regions:
//!
//!   * The **fixed buffer** (`row_buffer`) holds one slot per column at the
//!     column's precomputed `offset`, sized by the column's `size`. Fixed-width
//!     types (bool, the integer widths, floats, timestamp) live inline here in
//!     little-endian form. Floats are stored by their IEEE-754 bit pattern
//!     (`@bitCast` to the matching unsigned width) so the on-disk encoding is
//!     byte-stable and endianness-defined rather than depending on the C ABI.
//!
//!   * The **heap buffer** (`heap_buffer`) holds the variable-length payloads
//!     for `TEXT` and `BLOB` columns. A variable column's fixed slot does NOT
//!     hold the bytes; it holds a `u32` *offset* into the heap. At that offset
//!     the heap stores a 4-byte little-endian length immediately followed by the
//!     raw bytes (`[len:u32][data...]`). This is why a `TEXT`/`BLOB` column's
//!     [`primitiveType`](table.zig) is `u32`: the inline value is the pointer,
//!     not the string.
//!
//! The heap is a bump allocator: [`RowBuilder`] appends each payload at
//! `heap_offset` and advances it by `4 + len`. There is no free list and no
//! rewriting, so a row is built once, front to back, and never patched in place.
//! Running past `heap_buffer.len` fails with `error.HeapFull` rather than
//! growing, because these buffers are slices of a page-sized region the caller
//! owns.
//!
//! ## Two writer paths, same layout
//!
//! [`RowBuilder.write`] is the typed path: it takes a Zig value whose type must
//! match the column's [`primitiveType`] exactly, checked at *compile time* via
//! `@compileError`. [`RowBuilder.writeDynamic`] is the string path used when a
//! value arrives as text (for example from the SQL layer): it parses the text
//! into the column's type, tolerating a float spelling of an integer column by
//! parsing as `f64` and truncating. Both paths produce byte-identical rows.
//!
//! ## Reading and defaults
//!
//! [`RowReader.read`] mirrors [`RowBuilder.write`] with the same compile-time
//! type check. Because a table's schema can gain columns over time, a stored row
//! may be SHORTER than the current fixed layout: a read whose
//! `offset + size` runs past the stored `row_buffer` falls back to the column's
//! `default_value` (parsed to the requested type), treating the literal string
//! `"NULL"` as `error.NullValue` and an absent default as the type's zero. This
//! is what lets old rows survive an `ALTER TABLE ADD COLUMN` without a rewrite.
//! [`RowReader.readToString`] is the stringifying variant used for text output
//! and never fails the length check the same way (it returns a printed default).
//!
//! ## MVCC version chains: [`DecodedVersion`], [`packVersions`], [`unpackVersions`]
//!
//! One primary key can have several *versions* live at once under MVCC. A
//! [`DecodedVersion`] is the decoded form of one such version: `xmin`/`xmax` are
//! the creating and deleting transaction ids that drive visibility, `roll_ptr`
//! links to the undo record for the prior version, and `fixed`/`heap` are the
//! two row regions described above (here owned copies, freed by
//! [`DecodedVersion.deinit`]). [`packVersions`] flattens a slice of versions
//! into the single byte blob the B+Tree stores in the leaf cell for that key,
//! and [`unpackVersions`] reverses it. The wire format is:
//!
//!   `[count:u32]` then per version
//!   `[xmin:u64][xmax:u64][roll_ptr:u64][fixed_len:u32][heap_len:u32][fixed][heap]`,
//!
//! all little-endian. [`unpackVersions`] validates every length against the
//! buffer bound before reading and, on a short or malformed blob, frees the
//! versions decoded so far and returns `error.InvalidBinaryFormat`, a corrupt
//! or truncated cell must never be read past its end.

const std = @import("std");
const Allocator = std.mem.Allocator;
const table_mod = @import("table.zig");

/// Encoder that writes one row into caller-provided fixed and heap buffers.
///
/// Holds no memory of its own: `row_buffer` and `heap_buffer` are slices the
/// caller owns (typically carved from a page), and `heap_offset` is a shared
/// cursor advanced as variable-length payloads are appended. The same builder is
/// used column by column to fill a single row, so writes are expected to be
/// append-only and one-shot. See the file header for the byte layout the two
/// buffers hold.
pub const RowBuilder = struct {
    /// Schema of the table being written; supplies each column's type and its
    /// fixed-buffer `offset`/`size` via [`table.Table.getColumn`].
    table: table_mod.Table,
    /// Destination for fixed-width column values, indexed by column `offset`.
    row_buffer: []u8,
    /// Destination for `TEXT`/`BLOB` payloads, laid out as `[len:u32][data]`
    /// records; the fixed slot stores only the offset into this buffer.
    heap_buffer: []u8,
    /// Shared bump cursor into [`heap_buffer`]; advanced by `4 + len` on every
    /// variable-length write. Shared (a pointer) so successive builder calls on
    /// the same row keep appending rather than overwriting.
    heap_offset: *u32,

    /// Constructs a builder bound to the given buffers and heap cursor.
    ///
    /// Purely wires up the fields; it does not touch the buffers or reset
    /// `heap_offset`, so the caller controls where in the heap appending begins.
    pub fn init(table: table_mod.Table, row_buffer: []u8, heap_buffer: []u8, heap_offset: *u32) RowBuilder {
        return RowBuilder{
            .table = table,
            .row_buffer = row_buffer,
            .heap_buffer = heap_buffer,
            .heap_offset = heap_offset,
        };
    }

    /// Writes a strongly-typed value into the column named `col_name`.
    ///
    /// The value's Zig type must equal the column's [`primitiveType`] exactly;
    /// a mismatch is a *compile-time* `@compileError`, not a runtime error, so an
    /// ill-typed write never reaches production. Fixed-width types are stored
    /// inline little-endian at the column offset (floats via their bit pattern).
    /// For `TEXT`/`BLOB` the bytes are appended to the heap as `[len:u32][data]`
    /// and the column slot receives the heap offset.
    ///
    /// Returns `error.ColumnNotFound` if the name is not in the schema, or
    /// `error.HeapFull` if a variable-length payload would overflow
    /// [`heap_buffer`]. See [`writeDynamic`] for the text-parsing counterpart.
    pub fn write(self: *RowBuilder, col_name: []const u8, value: anytype) !void {
        const col = self.table.getColumn(col_name) orelse return error.ColumnNotFound;
        const T = @TypeOf(value);
        const expected = col.primitiveType();

        if (T != expected) {
            @compileError("RowBuilder type mismatch for column '" ++ col_name ++
                "': expected " ++ @typeName(expected) ++ ", got " ++ @typeName(T));
        }

        const col_offset = col.offset;
        const row = self.row_buffer;

        switch (col.type) {
            .BOOL => {
                row[col_offset] = if (value) 1 else 0;
            },
            .UINT32 => {
                std.mem.writeInt(u32, row[col_offset..][0..4], value, .little);
            },
            .UINT64 => {
                std.mem.writeInt(u64, row[col_offset..][0..8], value, .little);
            },
            .INT32 => {
                std.mem.writeInt(i32, row[col_offset..][0..4], value, .little);
            },
            .INT64 => {
                std.mem.writeInt(i64, row[col_offset..][0..8], value, .little);
            },
            .FLOAT32 => {
                const bits = @as(u32, @bitCast(value));
                std.mem.writeInt(u32, row[col_offset..][0..4], bits, .little);
            },
            .FLOAT64 => {
                const bits = @as(u64, @bitCast(value));
                std.mem.writeInt(u64, row[col_offset..][0..8], bits, .little);
            },
            .TIMESTAMP => {
                std.mem.writeInt(u64, row[col_offset..][0..8], value, .little);
            },
            .TEXT, .BLOB => {
                const bytes: []const u8 = value;
                const data_len = @as(u32, @intCast(bytes.len));

                const total_alloc = 4 + data_len;
                if (self.heap_offset.* + total_alloc > self.heap_buffer.len) {
                    return error.HeapFull;
                }

                const start_offset = self.heap_offset.*;
                std.mem.writeInt(u32, self.heap_buffer[start_offset..][0..4], data_len, .little);

                const data_ptr = self.heap_buffer[start_offset + 4 .. start_offset + 4 + data_len];
                @memcpy(data_ptr, bytes);

                std.mem.writeInt(u32, row[col_offset..][0..4], start_offset, .little);

                self.heap_offset.* += total_alloc;
            },
        }
    }

    /// Writes a column from its textual representation, parsing to the column's type.
    ///
    /// This is the path taken when a value arrives as a string (for example from
    /// SQL literals) and its Zig type is not known at compile time. Booleans
    /// accept `"true"`/`"1"` as true; integer and timestamp columns parse as
    /// base-10 and, on an `InvalidCharacter` (a value spelled as a float such as
    /// `"3.0"`), fall back to parsing an `f64` and truncating via `@intFromFloat`
    /// so numeric SQL that carries a decimal point still lands in an integer
    /// column. Float columns parse directly and are stored by bit pattern.
    /// `TEXT`/`BLOB` bytes are appended to the heap exactly as in [`write`].
    ///
    /// Returns `error.ColumnNotFound` for an unknown column, `error.HeapFull` on
    /// heap overflow, or a `std.fmt` parse error if the text is not a valid value
    /// for the column type.
    pub fn writeDynamic(self: *RowBuilder, col_name: []const u8, value: []const u8) !void {
        const col = self.table.getColumn(col_name) orelse return error.ColumnNotFound;
        const col_offset = col.offset;
        const row = self.row_buffer;

        switch (col.type) {
            .BOOL => {
                const b_val = std.mem.eql(u8, value, "true") or std.mem.eql(u8, value, "1");
                row[col_offset] = if (b_val) 1 else 0;
            },
            .UINT32 => {
                const val = std.fmt.parseInt(u32, value, 10) catch |err| blk: {
                    if (err == error.InvalidCharacter) {
                        const f = try std.fmt.parseFloat(f64, value);
                        break :blk @as(u32, @intFromFloat(f));
                    }
                    return err;
                };
                std.mem.writeInt(u32, row[col_offset..][0..4], val, .little);
            },
            .UINT64 => {
                const val = std.fmt.parseInt(u64, value, 10) catch |err| blk: {
                    if (err == error.InvalidCharacter) {
                        const f = try std.fmt.parseFloat(f64, value);
                        break :blk @as(u64, @intFromFloat(f));
                    }
                    return err;
                };
                std.mem.writeInt(u64, row[col_offset..][0..8], val, .little);
            },
            .INT32 => {
                const val = std.fmt.parseInt(i32, value, 10) catch |err| blk: {
                    if (err == error.InvalidCharacter) {
                        const f = try std.fmt.parseFloat(f64, value);
                        break :blk @as(i32, @intFromFloat(f));
                    }
                    return err;
                };
                std.mem.writeInt(i32, row[col_offset..][0..4], val, .little);
            },
            .INT64 => {
                const val = std.fmt.parseInt(i64, value, 10) catch |err| blk: {
                    if (err == error.InvalidCharacter) {
                        const f = try std.fmt.parseFloat(f64, value);
                        break :blk @as(i64, @intFromFloat(f));
                    }
                    return err;
                };
                std.mem.writeInt(i64, row[col_offset..][0..8], val, .little);
            },
            .FLOAT32 => {
                const val = try std.fmt.parseFloat(f32, value);
                const bits = @as(u32, @bitCast(val));
                std.mem.writeInt(u32, row[col_offset..][0..4], bits, .little);
            },
            .FLOAT64 => {
                const bits_cast = try std.fmt.parseFloat(f64, value);
                const bits = @as(u64, @bitCast(bits_cast));
                std.mem.writeInt(u64, row[col_offset..][0..8], bits, .little);
            },
            .TIMESTAMP => {
                const val = std.fmt.parseInt(u64, value, 10) catch |err| blk: {
                    if (err == error.InvalidCharacter) {
                        const f = try std.fmt.parseFloat(f64, value);
                        break :blk @as(u64, @intFromFloat(f));
                    }
                    return err;
                };
                std.mem.writeInt(u64, row[col_offset..][0..8], val, .little);
            },
            .TEXT, .BLOB => {
                const data_len = @as(u32, @intCast(value.len));
                const total_alloc = 4 + data_len;
                if (self.heap_offset.* + total_alloc > self.heap_buffer.len) {
                    return error.HeapFull;
                }

                const start_offset = self.heap_offset.*;
                std.mem.writeInt(u32, self.heap_buffer[start_offset..][0..4], data_len, .little);

                const data_ptr = self.heap_buffer[start_offset + 4 .. start_offset + 4 + data_len];
                @memcpy(data_ptr, value);

                std.mem.writeInt(u32, row[col_offset..][0..4], start_offset, .little);

                self.heap_offset.* += total_alloc;
            },
        }
    }
};

/// Decoder that reads column values back out of a stored row's two buffers.
///
/// The read-only mirror of [`RowBuilder`]: it interprets the same fixed/heap
/// layout. Both buffers are borrowed `[]const u8` slices (typically pointing
/// straight into a page), so a `RowReader` is only valid while the underlying
/// page stays pinned. It never allocates except in [`readToString`], which is
/// explicitly given an allocator.
pub const RowReader = struct {
    /// Schema used to locate each column and to know its type and offset.
    table: table_mod.Table,
    /// The stored fixed part. May be SHORTER than the current schema's fixed
    /// size for rows written before a column was added; short reads fall back to
    /// the column default (see [`read`]).
    row_buffer: []const u8,
    /// The stored heap part holding `[len:u32][data]` records that variable
    /// columns point into.
    heap_buffer: []const u8,

    /// Constructs a reader over the given fixed and heap buffers.
    ///
    /// Borrows the slices; does not copy or validate them, so they must remain
    /// alive and consistent with `table`'s layout for the reader's lifetime.
    pub fn init(table: table_mod.Table, row_buffer: []const u8, heap_buffer: []const u8) RowReader {
        return RowReader{
            .table = table,
            .row_buffer = row_buffer,
            .heap_buffer = heap_buffer,
        };
    }

    /// Reads the column named `col_name` as the strongly-typed value `T`.
    ///
    /// `T` must equal the column's [`primitiveType`] exactly; a mismatch is a
    /// compile-time `@compileError`. For `TEXT`/`BLOB`, `T` is `[]const u8` and
    /// the returned slice BORROWS directly into [`heap_buffer`] (no copy), so it
    /// is only valid while the buffer lives; use [`readToString`] for an owned
    /// copy.
    ///
    /// Schema-evolution fallback: if the column's `offset + size` runs past the
    /// stored `row_buffer` (the row predates this column), the value comes from
    /// the column's `default_value` parsed to `T`. A default of the literal
    /// `"NULL"` yields `error.NullValue`; an absent default yields the type's
    /// zero (`false`, empty string, `0`, `0.0`). A default present but for an
    /// unhandled `T` yields `error.TypeMismatch`.
    ///
    /// Returns `error.ColumnNotFound` for an unknown column.
    pub fn read(self: RowReader, col_name: []const u8, comptime T: type) !T {
        const col = self.table.getColumn(col_name) orelse return error.ColumnNotFound;
        const expected = col.primitiveType();

        if (T != expected) {
            @compileError("RowReader type mismatch for column '" ++ col_name ++
                "': expected " ++ @typeName(expected) ++ ", got " ++ @typeName(T));
        }

        const col_offset = col.offset;
        const row = self.row_buffer;

        if (col_offset + col.size > row.len) {
            if (col.default_value) |def| {
                if (std.mem.eql(u8, def, "NULL")) {
                    return error.NullValue;
                }
                if (T == bool) {
                    return @as(T, std.mem.eql(u8, def, "true"));
                } else if (T == []const u8) {
                    return @as(T, def);
                } else if (T == u32 or T == u64 or T == i32 or T == i64) {
                    const parsed_val = std.fmt.parseInt(i64, def, 10) catch 0;
                    return @as(T, @intCast(parsed_val));
                } else if (T == f32 or T == f64) {
                    const parsed_val = std.fmt.parseFloat(f64, def) catch 0.0;
                    return @as(T, @floatCast(parsed_val));
                }
            }
            if (T == bool) {
                return @as(T, false);
            } else if (T == []const u8) {
                return @as(T, "");
            } else if (T == u32 or T == u64 or T == i32 or T == i64) {
                return @as(T, @intCast(0));
            } else if (T == f32 or T == f64) {
                return @as(T, @floatCast(0.0));
            }
            return error.TypeMismatch;
        }

        return switch (col.type) {
            .BOOL => row[col_offset] != 0,
            .UINT32 => std.mem.readInt(u32, row[col_offset..][0..4], .little),
            .UINT64 => std.mem.readInt(u64, row[col_offset..][0..8], .little),
            .INT32 => std.mem.readInt(i32, row[col_offset..][0..4], .little),
            .INT64 => std.mem.readInt(i64, row[col_offset..][0..8], .little),
            .FLOAT32 => @bitCast(std.mem.readInt(u32, row[col_offset..][0..4], .little)),
            .FLOAT64 => @bitCast(std.mem.readInt(u64, row[col_offset..][0..8], .little)),
            .TIMESTAMP => std.mem.readInt(u64, row[col_offset..][0..8], .little),
            .TEXT, .BLOB => {
                const heap_offset = std.mem.readInt(u32, row[col_offset..][0..4], .little);

                const len_ptr = self.heap_buffer[heap_offset..][0..4];
                const data_len = std.mem.readInt(u32, len_ptr, .little);

                const data_start = heap_offset + 4;
                return self.heap_buffer[data_start .. data_start + data_len];
            },
        };
    }

    /// Convenience wrapper around [`read`] for an integer column type `T`.
    ///
    /// Adds no behaviour beyond [`read`]; it exists to make the call site read as
    /// an integer fetch and to keep the same compile-time type check.
    pub fn readInt(self: RowReader, col_name: []const u8, comptime T: type) !T {
        return try self.read(col_name, T);
    }

    /// Reads a `TEXT`/`BLOB` column as a borrowed `[]const u8`.
    ///
    /// Thin alias for `read(col_name, []const u8)`; the returned slice points
    /// into [`heap_buffer`] and is not owned (see [`read`]).
    pub fn readText(self: RowReader, col_name: []const u8) ![]const u8 {
        return try self.read(col_name, []const u8);
    }

    /// Reads a `BOOL` column. Thin alias for `read(col_name, bool)`.
    pub fn readBool(self: RowReader, col_name: []const u8) !bool {
        return try self.read(col_name, bool);
    }

    /// Reads any column and returns a freshly allocated, human-readable string.
    ///
    /// The general-purpose stringifier used for text output where the column's
    /// type is not known at the call site. Unlike [`read`], it does no
    /// compile-time type check: it formats each type to its decimal/`true`/`false`
    /// spelling (`{d}` for numbers, the raw bytes for `TEXT`/`BLOB`) and always
    /// returns an OWNED allocation the caller must free. The same
    /// schema-evolution fallback applies: a row too short for the column yields
    /// the column's `default_value` if set, else a per-type printed zero
    /// (`"false"`, `"0"`, `""`).
    ///
    /// Returns `error.ColumnNotFound` for an unknown column, or an allocator
    /// error on out-of-memory.
    pub fn readToString(self: RowReader, allocator: std.mem.Allocator, col_name: []const u8) ![]const u8 {
        const col = self.table.getColumn(col_name) orelse return error.ColumnNotFound;
        const col_offset = col.offset;
        const row = self.row_buffer;

        if (col_offset + col.size > row.len) {
            if (col.default_value) |def| {
                return try allocator.dupe(u8, def);
            }
            return try allocator.dupe(u8, switch (col.type) {
                .BOOL => "false",
                .UINT32, .UINT64, .INT32, .INT64, .TIMESTAMP => "0",
                .FLOAT32, .FLOAT64 => "0",
                .TEXT, .BLOB => "",
            });
        }

        switch (col.type) {
            .BOOL => {
                const val = row[col_offset] != 0;
                return try allocator.dupe(u8, if (val) "true" else "false");
            },
            .UINT32 => {
                const val = std.mem.readInt(u32, row[col_offset..][0..4], .little);
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .UINT64 => {
                const val = std.mem.readInt(u64, row[col_offset..][0..8], .little);
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .INT32 => {
                const val = std.mem.readInt(i32, row[col_offset..][0..4], .little);
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .INT64 => {
                const val = std.mem.readInt(i64, row[col_offset..][0..8], .little);
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .FLOAT32 => {
                const bits = std.mem.readInt(u32, row[col_offset..][0..4], .little);
                const val = @as(f32, @bitCast(bits));
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .FLOAT64 => {
                const bits = std.mem.readInt(u64, row[col_offset..][0..8], .little);
                const val = @as(f64, @bitCast(bits));
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .TIMESTAMP => {
                const val = std.mem.readInt(u64, row[col_offset..][0..8], .little);
                return try std.fmt.allocPrint(allocator, "{d}", .{val});
            },
            .TEXT, .BLOB => {
                const heap_offset = std.mem.readInt(u32, row[col_offset..][0..4], .little);
                const len_ptr = self.heap_buffer[heap_offset..][0..4];
                const data_len = std.mem.readInt(u32, len_ptr, .little);
                const data_start = heap_offset + 4;
                return try allocator.dupe(u8, self.heap_buffer[data_start .. data_start + data_len]);
            },
        }
    }
};

/// One decoded MVCC version of a row: its visibility ids, undo link, and bytes.
///
/// A single primary key can carry several versions at once under MVCC; this is
/// the in-memory form of one of them. `fixed`/`heap` are the two row regions
/// described in the file header. When produced by [`unpackVersions`] the two
/// slices are OWNED heap allocations and must be released with [`deinit`]; the
/// same struct is also built with borrowed slices when packing.
pub const DecodedVersion = struct {
    /// Id of the transaction that created this version. Drives read visibility:
    /// a snapshot sees the version only if `xmin` committed before it.
    xmin: u64,
    /// Id of the transaction that deleted or superseded this version, or `0`
    /// (and effectively "live") if none. A snapshot stops seeing the version
    /// once `xmax` is committed and visible to it.
    xmax: u64,
    /// Pointer to the undo record for the PRIOR version in the chain, linking
    /// this version to what it replaced; `0` when there is no earlier version.
    roll_ptr: u64 = 0,
    /// The fixed-width row bytes for this version (see file header layout).
    fixed: []const u8,
    /// The variable-length heap bytes (`[len:u32][data]` records) for this
    /// version, referenced by offsets stored in [`fixed`].
    heap: []const u8,

    /// Frees the owned `fixed` and `heap` slices of a version.
    ///
    /// Call only on versions whose slices are owned (those returned by
    /// [`unpackVersions`]); calling it on a version built from borrowed slices
    /// would free memory this struct does not own.
    pub fn deinit(self: *DecodedVersion, allocator: Allocator) void {
        allocator.free(self.fixed);
        allocator.free(self.heap);
    }
};

/// Serialises a version chain into the single byte blob stored in a leaf cell.
///
/// Produces `[count:u32]` followed, per version, by
/// `[xmin:u64][xmax:u64][roll_ptr:u64][fixed_len:u32][heap_len:u32][fixed][heap]`,
/// all little-endian. This is the exact format [`unpackVersions`] reads back.
/// The returned slice is owned by the caller. On any allocation failure the
/// partially built buffer is freed via `errdefer` before the error propagates.
pub fn packVersions(allocator: Allocator, versions: []const DecodedVersion) ![]const u8 {
    var list = std.ArrayList(u8).empty;
    errdefer list.deinit(allocator);

    var count_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &count_bytes, @intCast(versions.len), .little);
    try list.appendSlice(allocator, &count_bytes);

    for (versions) |v| {
        var xmin_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &xmin_bytes, v.xmin, .little);
        try list.appendSlice(allocator, &xmin_bytes);

        var xmax_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &xmax_bytes, v.xmax, .little);
        try list.appendSlice(allocator, &xmax_bytes);

        var roll_ptr_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &roll_ptr_bytes, v.roll_ptr, .little);
        try list.appendSlice(allocator, &roll_ptr_bytes);

        var fixed_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &fixed_len_bytes, @intCast(v.fixed.len), .little);
        try list.appendSlice(allocator, &fixed_len_bytes);

        var heap_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &heap_len_bytes, @intCast(v.heap.len), .little);
        try list.appendSlice(allocator, &heap_len_bytes);

        try list.appendSlice(allocator, v.fixed);
        try list.appendSlice(allocator, v.heap);
    }

    return try list.toOwnedSlice(allocator);
}

/// Deserialises a leaf-cell blob back into an owned array of versions.
///
/// The inverse of [`packVersions`]. Every length is checked against the buffer
/// bound before it is read: a header shorter than 4 bytes, a per-version fixed
/// span shorter than the 32-byte record head, or a `fixed_len`/`heap_len` that
/// would read past `bytes` all yield `error.InvalidBinaryFormat` (and log the
/// offending sizes), so a truncated or corrupt cell is rejected rather than read
/// out of bounds. Each version's `fixed`/`heap` slices are freshly duplicated,
/// so the result is fully owned and independent of `bytes`.
///
/// On a mid-stream failure the versions decoded so far are freed via `errdefer`
/// (tracked by `allocated_count`) before the error propagates, so no partial
/// allocation leaks. The caller owns the returned slice and each element's
/// buffers, and must [`DecodedVersion.deinit`] every element and free the slice.
pub fn unpackVersions(allocator: Allocator, bytes: []const u8) ![]DecodedVersion {
    if (bytes.len < 4) {
        std.log.err("unpackVersions error: bytes.len = {d} is less than 4! bytes: {any}", .{bytes.len, bytes});
        return error.InvalidBinaryFormat;
    }
    const count = std.mem.readInt(u32, bytes[0..4], .little);

    var versions = try allocator.alloc(DecodedVersion, count);
    var allocated_count: usize = 0;
    errdefer {
        var j: usize = 0;
        while (j < allocated_count) : (j += 1) {
            versions[j].deinit(allocator);
        }
        allocator.free(versions);
    }

    var offset: usize = 4;
    while (allocated_count < count) : (allocated_count += 1) {
        if (offset + 32 > bytes.len) {
            std.log.err("unpackVersions error: offset + 32 ({d}) > bytes.len ({d}). bytes: {any}", .{offset + 32, bytes.len, bytes});
            return error.InvalidBinaryFormat;
        }
        const xmin = std.mem.readInt(u64, bytes[offset..][0..8], .little);
        offset += 8;
        const xmax = std.mem.readInt(u64, bytes[offset..][0..8], .little);
        offset += 8;
        const roll_ptr = std.mem.readInt(u64, bytes[offset..][0..8], .little);
        offset += 8;
        const fixed_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const heap_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;

        if (offset + fixed_len + heap_len > bytes.len) {
            std.log.err("unpackVersions error: offset + fixed_len + heap_len ({d} + {d} + {d} = {d}) > bytes.len ({d}). bytes: {any}", .{offset, fixed_len, heap_len, offset + fixed_len + heap_len, bytes.len, bytes});
            return error.InvalidBinaryFormat;
        }

        const fixed = try allocator.dupe(u8, bytes[offset .. offset + fixed_len]);
        errdefer allocator.free(fixed);

        const heap = try allocator.dupe(u8, bytes[offset + fixed_len .. offset + fixed_len + heap_len]);

        versions[allocated_count] = DecodedVersion{
            .xmin = xmin,
            .xmax = xmax,
            .roll_ptr = roll_ptr,
            .fixed = fixed,
            .heap = heap,
        };
        offset += fixed_len + heap_len;
    }

    return versions;
}
