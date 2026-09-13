//! In-memory catalog descriptors for tables and secondary indexes, plus the
//! byte-level key encoder the B+Tree uses.
//!
//! This module is the runtime, allocator-owning half of the schema layer. The
//! sibling `types.zig` holds the *serialisable* forms ([`ColumnMetadata`],
//! [`TableMetadata`], [`IndexMetadata`]) that get written into and read back
//! from the on-disk catalog; the types here ([`Table`], [`Index`]) are the
//! live descriptors the query executor consults on every statement. A catalog
//! load turns metadata into these; a DDL statement builds these first and then
//! persists the matching metadata.
//!
//! Row layout is FIXED-WIDTH. [`Table.init`] walks the columns once and assigns
//! each a byte `offset` into the row image, accumulating a running total into
//! [`Table.fixed_size`]. Every value type occupies a constant number of bytes
//! (see [`TableBuilder.addColumn`]): scalars are stored inline, but a `TEXT` or
//! `BLOB` column takes only 4 bytes in the row, holding a reference (an overflow
//! locator) rather than the payload, so the row stride stays constant and a
//! column is addressable as `row[col.offset..][0..col.size]` without scanning.
//! This is why the fixed_size is knowable up front and why offsets never shift
//! once a table is built.
//!
//! Index keys are encoded BIG-ENDIAN by [`Index.packKey`]. That is deliberate:
//! the B+Tree compares keys as raw byte strings (`memcmp` order), and big-endian
//! is the byte order under which an unsigned integer's lexicographic byte order
//! matches its numeric order, so range scans and ordered iteration fall out of a
//! plain bytewise comparison. Keys are also FIXED-WIDTH per column (a `TEXT`
//! key column is truncated or zero-padded to `col.size`), so a composite key is
//! just the concatenation of its parts with no separators or length prefixes.
//!
//! The two `*Builder` types exist so callers can assemble a descriptor
//! incrementally (column by column, index-key set by index-key set) and only pay
//! the offset-assignment / column-resolution cost once at [`TableBuilder.build`]
//! / [`IndexBuilder.build`]. Every descriptor owns heap copies of its strings and
//! column slices via the stored [`std.mem.Allocator`]; the matching `deinit`
//! frees them, so a descriptor's lifetime is independent of the text it was
//! built from.

const std = @import("std");
/// Convenience alias for the standard allocator interface every descriptor
/// stores so it can free its owned strings and column slices at `deinit`.
const Allocator = std.mem.Allocator;
/// The serialisable schema vocabulary: [`types.Column`], [`types.ColumnType`],
/// [`types.IndexKind`] and [`types.IndexValue`]. See `types.zig`.
const types = @import("types.zig");

/// A live, in-memory descriptor of a base table.
///
/// Produced from catalog metadata on load or from a [`TableBuilder`] on DDL, and
/// consulted by the executor to locate and decode columns within a fixed-width
/// row image. It owns heap copies of [`name`] and, transitively, of the strings
/// inside [`columns`]; call [`deinit`] to release them. The [`columns`] slice
/// itself is taken over (not copied) by [`init`], which stamps each column's
/// byte `offset` in place.
pub const Table = struct {
    /// Catalog-assigned numeric identity of the table, stable across restarts
    /// and used as the key into the schema catalog.
    id: u32,
    /// Heap-owned copy of the table name (duplicated by [`init`]).
    name: []const u8,
    /// The table's columns, in declaration order. Each column's `offset` field
    /// has been filled in by [`init`] to point at its slot in the fixed-width
    /// row image. Owned by this table and freed in [`deinit`].
    columns: []types.Column,
    /// Total width in bytes of one row image, i.e. the sum of every column's
    /// `size`. `TEXT`/`BLOB` contribute 4 bytes each (an overflow reference),
    /// not their payload length, so this stays constant for the table.
    fixed_size: u16,
    /// Live count of rows, maintained by the executor as rows are inserted and
    /// deleted; not persisted here.
    row_count: u64,
    /// Allocator that owns [`name`] and [`columns`]; the same one must back
    /// every string inside those columns so [`deinit`] can free them.
    allocator: Allocator,

    /// Builds a table descriptor, taking ownership of `columns` and assigning
    /// each column its byte offset in the row image.
    ///
    /// Iterates the columns once, packing them contiguously: column `i` is placed
    /// at the running offset and the offset advances by its `size`, so the final
    /// running total becomes [`fixed_size`]. The `columns` slice is stored
    /// directly (the caller must not free it or mutate offsets afterwards); the
    /// name is duplicated with `allocator`. Returns `error.OutOfMemory` if the
    /// name copy fails.
    pub fn init(allocator: Allocator, id: u32, name: []const u8, columns: []types.Column) !Table {
        var offset: u16 = 0;
        for (columns) |*col| {
            col.offset = offset;
            offset += col.size;
        }
        return Table{
            .id = id,
            .name = try allocator.dupe(u8, name),
            .columns = columns,
            .fixed_size = offset,
            .row_count = 0,
            .allocator = allocator,
        };
    }

    /// Looks up a column by name, returning a mutable pointer into [`columns`]
    /// or `null` if no column matches.
    ///
    /// The returned pointer aliases the table's own slice, so it stays valid
    /// only while the table does. Comparison is exact and case-sensitive. This
    /// is a linear scan, which is acceptable because column counts are small and
    /// the result is typically resolved once per statement and cached.
    pub fn getColumn(self: Table, name: []const u8) ?*types.Column {
        for (self.columns) |*col| {
            if (std.mem.eql(u8, col.name, name)) return col;
        }
        return null;
    }

    /// Frees everything this table owns: each column's inner strings, the
    /// columns slice, and the table name.
    ///
    /// Must be called with the descriptor no longer in use. Each column's own
    /// [`types.Column.deinit`] runs first (releasing its name and default value)
    /// before the backing slices are freed, so there is no use-after-free of the
    /// column strings.
    pub fn deinit(self: *Table) void {
        for (self.columns) |*col| col.deinit(self.allocator);
        self.allocator.free(self.columns);
        self.allocator.free(self.name);
    }
};

/// Incremental constructor for a [`Table`].
///
/// Accumulates columns one at a time via [`addColumn`], tracking the running row
/// width so each column gets a correct offset as it is added, then hands the
/// collected columns to [`Table.init`] in [`build`]. Exists so DDL code can
/// append columns as it parses them without knowing the final count. If the
/// builder is abandoned before [`build`], call [`deinit`] to release what has
/// been accumulated.
pub const TableBuilder = struct {
    /// Allocator used for every duplicated string and for the column list;
    /// passed straight through to the finished [`Table`].
    allocator: Allocator,
    /// Table id to stamp onto the built [`Table`].
    id: u32,
    /// Table name. Note this is stored as given (not duplicated by [`init`]);
    /// [`Table.init`] duplicates it at [`build`] time, and [`deinit`] frees this
    /// original, so the caller passes ownership of an allocator-owned name in.
    name: []const u8,
    /// Growable list of columns collected so far, each already offset-stamped by
    /// [`addColumn`].
    columns: std.ArrayList(types.Column),
    /// Running total of column sizes, i.e. the offset the NEXT column will take.
    /// Becomes the table's `fixed_size` once building is done.
    current_size: u16,

    /// Creates an empty builder for a table with the given id and name.
    ///
    /// Does not copy `name`; the builder holds the caller's slice and later
    /// frees it in [`deinit`] (or, on success, [`Table.init`] duplicates it and
    /// [`build`]'s caller is responsible for the original). The column list
    /// starts empty and `current_size` at zero.
    pub fn init(allocator: Allocator, id: u32, name: []const u8) TableBuilder {
        return TableBuilder{
            .allocator = allocator,
            .id = id,
            .name = name,
            .columns = std.ArrayList(types.Column).empty,
            .current_size = 0,
        };
    }

    /// Appends one column definition, sizing and offset-placing it.
    ///
    /// The byte `size` is decided purely by `col_type`: `BOOL` is 1 byte;
    /// `UINT32`/`INT32`/`FLOAT32` are 4; `UINT64`/`INT64`/`FLOAT64`/`TIMESTAMP`
    /// are 8; and crucially `TEXT`/`BLOB` are 4, because a variable-length value
    /// is stored out of line and the row holds only a 4-byte reference. The
    /// column takes the current running offset, then `current_size` advances by
    /// `size`. Both `name` and `default_value` (if present) are duplicated with
    /// the builder's allocator, so the caller keeps ownership of its inputs.
    /// Returns `error.OutOfMemory` on a failed duplication or list append.
    pub fn addColumn(
        self: *TableBuilder,
        name: []const u8,
        col_type: types.ColumnType,
        is_primary_key: bool,
        is_auto_increment: bool,
        is_nullable: bool,
        default_value: ?[]const u8,
    ) !void {
        const size = switch (col_type) {
            .BOOL => 1,
            .UINT32, .INT32, .FLOAT32 => 4,
            .UINT64, .INT64, .FLOAT64, .TIMESTAMP => 8,
            .TEXT, .BLOB => 4,
        };
        try self.columns.append(types.Column{
            .name = try self.allocator.dupe(u8, name),
            .type = col_type,
            .size = size,
            .offset = self.current_size,
            .is_primary_key = is_primary_key,
            .is_auto_increment = is_auto_increment,
            .is_nullable = is_nullable,
            .default_value = if (default_value) |dv| try self.allocator.dupe(u8, dv) else null,
        });
        self.current_size += size;
    }

    /// Finalises the builder into a [`Table`], transferring the accumulated
    /// columns to it.
    ///
    /// The column list is released from the [`std.ArrayList`] as an owned slice
    /// and handed to [`Table.init`], which re-walks it to assign offsets (a
    /// second pass that reproduces the offsets [`addColumn`] already computed).
    /// After a successful build the builder no longer owns the columns, so do
    /// NOT call [`deinit`] on it; the returned [`Table`] owns them now. Returns
    /// `error.OutOfMemory` on failure.
    pub fn build(self: *TableBuilder) !Table {
        const cols = try self.columns.toOwnedSlice();
        return Table.init(self.allocator, self.id, self.name, cols);
    }

    /// Releases a builder that will NOT be built, freeing every column
    /// accumulated so far, the column list, and the held name.
    ///
    /// Use this only on the abandon path. After a successful [`build`] the
    /// columns have moved into the [`Table`], so calling this would double-free
    /// them.
    pub fn deinit(self: *TableBuilder) void {
        for (self.columns.items) |*col| col.deinit(self.allocator);
        self.columns.deinit();
        self.allocator.free(self.name);
    }
};



/// A live descriptor of a secondary index over a [`Table`].
///
/// Holds copies of the key columns (and, for a covering index, the value
/// columns) resolved from the parent table, so the index knows its own byte
/// layout independently of the table. Its central job is [`packKey`], which
/// serialises a set of column values into the fixed-width, big-endian key blob
/// the B+Tree stores and compares. Owned strings and column slices are released
/// by [`deinit`].
pub const Index = struct {
    /// Catalog-assigned numeric identity of the index.
    id: u32,
    /// Heap-owned index name.
    name: []const u8,
    /// Id of the [`Table`] this index is defined over.
    table_id: u32,
    /// Index flavour ([`types.IndexKind`]): `UNIQUE`, `COMPOSITE`, or
    /// `COVERING_COMPOSITE`. A covering index is the case where
    /// [`value_columns`] is populated so the index can answer a projection
    /// without touching the base table.
    kind: types.IndexKind,
    /// The columns that make up the key, in key order. Their concatenated
    /// big-endian encodings form the stored key; owned and freed in [`deinit`].
    key_columns: []types.Column,
    /// For a covering index, the extra columns stored alongside the key so an
    /// index-only scan can return them; `null` for a non-covering index. Owned
    /// and freed in [`deinit`] when present.
    value_columns: ?[]types.Column,
    /// Mirrors [`types.IndexMetadata.exact`]: `true` when every entry is known to
    /// map one-to-one to a live row (fresh build, no delete/update on the table
    /// since), so an index-only COUNT may trust the entry count. Loaded from the
    /// catalog; kept in sync by the executor on delete/update.
    exact: bool = false,
    /// Allocator owning [`name`], the column slices, and their inner strings.
    allocator: Allocator,

    /// Returns the total key width in bytes: the sum of every key column's
    /// `size`.
    ///
    /// This is the exact length [`packKey`] writes and the minimum buffer it
    /// requires. Because column sizes are fixed, the key width is constant for
    /// the index.
    pub fn keySize(self: Index) u16 {
        var total: u16 = 0;
        for (self.key_columns) |col| total += col.size;
        return total;
    }

    /// Returns the total width in bytes of the stored value columns, or `null`
    /// if this is not a covering index.
    ///
    /// Mirrors [`keySize`] for the [`value_columns`] side, used to size the
    /// value slot of a covering-index entry.
    pub fn valueSize(self: Index) ?u16 {
        if (self.value_columns) |vcols| {
            var total: u16 = 0;
            for (vcols) |col| total += col.size;
            return total;
        }
        return null;
    }

    /// Reports whether this index can satisfy `projected_cols` from its stored
    /// value columns alone (i.e. serve as a covering index for that projection).
    ///
    /// Requires that the index HAS value columns and that they match
    /// `projected_cols` exactly: same count and same names in the same order.
    /// A non-covering index, a length mismatch, or any name mismatch returns
    /// `false`. The strict positional match means the executor must present the
    /// projection in the index's stored order to get an index-only scan.
    pub fn isCoveringFor(self: Index, projected_cols: []const []const u8) bool {
        if (self.value_columns == null) return false;
        const vcols = self.value_columns.?;
        if (vcols.len != projected_cols.len) return false;
        for (vcols, projected_cols) |vcol, pname| {
            if (!std.mem.eql(u8, vcol.name, pname)) return false;
        }
        return true;
    }

    /// Encodes a set of column values into `buffer` as the fixed-width,
    /// big-endian B+Tree key.
    ///
    /// For each key column in key order it finds the matching entry in `values`
    /// (matched by column name) and appends its encoding at the running offset.
    /// Integers, floats and timestamps are written big-endian so a bytewise
    /// comparison of two keys reproduces numeric order for the unsigned/timestamp
    /// cases; floats and signed integers are bit-cast to their unsigned width
    /// first, so their encoding round-trips but is not itself order-preserving.
    /// `TEXT`/`BLOB` values are copied up to `col.size` bytes and zero-padded if
    /// shorter, keeping every key column fixed-width and separator-free.
    ///
    /// Errors: `error.BufferTooSmall` if `buffer` is shorter than [`keySize`];
    /// `error.MissingColumnValue` if any key column has no corresponding entry
    /// in `values`. The caller owns `buffer` and must size it to at least
    /// [`keySize`].
    pub fn packKey(self: Index, buffer: []u8, values: []const struct { col: []const u8, val: types.IndexValue }) !void {
        if (buffer.len < self.keySize()) return error.BufferTooSmall;

        var offset: usize = 0;
        for (self.key_columns) |key_col| {
            const match = for (values) |v| {
                if (std.mem.eql(u8, v.col, key_col.name)) break v;
            } else return error.MissingColumnValue;

            switch (key_col.type) {
                .BOOL => {
                    const val: u8 = if (match.val.bool) 1 else 0;
                    buffer[offset] = val;
                    offset += 1;
                },
                .UINT32 => {
                    const val = match.val.uint32;
                    std.mem.writeInt(u32, buffer[offset..][0..4], val, .big);
                    offset += 4;
                },
                .INT32 => {
                    const val = match.val.int32;
                    std.mem.writeInt(i32, buffer[offset..][0..4], @as(u32, @bitCast(val)), .big);
                    offset += 4;
                },
                .UINT64 => {
                    const val = match.val.uint64;
                    std.mem.writeInt(u64, buffer[offset..][0..8], val, .big);
                    offset += 8;
                },
                .INT64 => {
                    const val = match.val.int64;
                    std.mem.writeInt(i64, buffer[offset..][0..8], @as(u64, @bitCast(val)), .big);
                    offset += 8;
                },
                .FLOAT32 => {
                    const val = match.val.float32;
                    std.mem.writeInt(u32, buffer[offset..][0..4], @as(u32, @bitCast(val)), .big);
                    offset += 4;
                },
                .FLOAT64 => {
                    const val = match.val.float64;
                    std.mem.writeInt(u64, buffer[offset..][0..8], @as(u64, @bitCast(val)), .big);
                    offset += 8;
                },
                .TIMESTAMP => {
                    const val = match.val.timestamp;
                    std.mem.writeInt(u64, buffer[offset..][0..8], val, .big);
                    offset += 8;
                },
                .TEXT, .BLOB => {
                    const val = match.val.text;
                    const max_copy = @min(val.len, key_col.size);
                    @memcpy(buffer[offset..][0..max_copy], val[0..max_copy]);
                    if (max_copy < key_col.size) {
                        @memset(buffer[offset + max_copy .. offset + key_col.size], 0);
                    }
                    offset += key_col.size;
                },
            }
        }
    }

    /// Frees everything this index owns: the name, each key column's inner
    /// strings and the key column slice, and, when present, the value columns.
    ///
    /// Each [`types.Column.deinit`] runs before its backing slice is freed so no
    /// column string is used after free. Safe whether or not [`value_columns`]
    /// is set.
    pub fn deinit(self: *Index) void {
        self.allocator.free(self.name);
        for (self.key_columns) |*col| {
            col.deinit(self.allocator);
        }
        if (self.value_columns) |vcols| {
            for (vcols) |*col| {
                col.deinit(self.allocator);
            }
            self.allocator.free(vcols);
        }
        self.allocator.free(self.key_columns);
    }
};

/// Incremental constructor for an [`Index`].
///
/// Unlike [`Index`], which stores full column copies, the builder holds only the
/// POSITIONAL indices of the key (and optional value) columns within the parent
/// table. [`build`] resolves those indices against the actual [`Table`] to copy
/// out the real columns, so the caller describes an index purely by which of the
/// table's columns participate. Abandon it with [`deinit`].
pub const IndexBuilder = struct {
    /// Allocator for the resolved column slices and duplicated name.
    allocator: Allocator,
    /// Index id to stamp onto the built [`Index`].
    id: u32,
    /// Index name; stored as given and duplicated by [`build`], with this
    /// original freed by [`deinit`].
    name: []const u8,
    /// Id of the parent table the index belongs to.
    table_id: u32,
    /// Index flavour to record on the built [`Index`]; see [`types.IndexKind`].
    kind: types.IndexKind,
    /// Positions (into the parent table's `columns`) of the key columns, in key
    /// order. Owned once [`setKeyColumns`] has duplicated the caller's slice;
    /// starts as an empty slice.
    key_column_indices: []usize,
    /// Positions of the covering value columns, or `null` for a non-covering
    /// index. Populated by [`setValueColumns`].
    value_column_indices: ?[]usize,

    /// Creates an empty index builder with no key or value columns selected yet.
    ///
    /// `name` is held (not copied) and later freed by [`deinit`]; the key index
    /// slice starts empty and the value indices `null` until
    /// [`setKeyColumns`]/[`setValueColumns`] are called.
    pub fn init(allocator: Allocator, id: u32, name: []const u8, table_id: u32, kind: types.IndexKind) IndexBuilder {
        return IndexBuilder{
            .allocator = allocator,
            .id = id,
            .name = name,
            .table_id = table_id,
            .kind = kind,
            .key_column_indices = &.{},
            .value_column_indices = null,
        };
    }

    /// Records which parent-table columns form the key, by position.
    ///
    /// Duplicates `indices` with the builder's allocator so the caller keeps
    /// ownership of its input. The order given is the key order. Returns
    /// `error.OutOfMemory` on failure.
    pub fn setKeyColumns(self: *IndexBuilder, indices: []const usize) !void {
        self.key_column_indices = try self.allocator.dupe(usize, indices);
    }

    /// Records which parent-table columns are stored as covering values, by
    /// position, making this a covering index.
    ///
    /// Duplicates `indices`; the caller retains ownership of its slice. Leaving
    /// this unset keeps the index non-covering. Returns `error.OutOfMemory` on
    /// failure.
    pub fn setValueColumns(self: *IndexBuilder, indices: []const usize) !void {
        self.value_column_indices = try self.allocator.dupe(usize, indices);
    }

    /// Resolves the recorded column positions against `parent_table` and
    /// produces the finished [`Index`].
    ///
    /// Allocates a key-column slice and copies `parent_table.columns[idx]` for
    /// each recorded key index (in order); does the same for the value columns
    /// when [`value_column_indices`] is set. The copies are struct copies of the
    /// table's columns, so their inner strings are shared references at this
    /// point, but note [`Index.deinit`] frees them, so the caller must ensure
    /// the copies are the owning ones (the parent table's own columns are
    /// duplicated at their own construction). The name is duplicated with the
    /// allocator. Returns `error.OutOfMemory` on any allocation failure. An
    /// index position out of range for `parent_table.columns` is a caller error
    /// and will trip Zig's bounds checking.
    pub fn build(self: *IndexBuilder, parent_table: Table) !Index {
        var key_cols = try self.allocator.alloc(types.Column, self.key_column_indices.len);
        for (self.key_column_indices, 0..) |idx, i| {
            key_cols[i] = parent_table.columns[idx];
        }

        var value_cols: ?[]types.Column = null;
        if (self.value_column_indices) |v_indices| {
            var vcols = try self.allocator.alloc(types.Column, v_indices.len);
            for (v_indices, 0..) |idx, i| {
                vcols[i] = parent_table.columns[idx];
            }
            value_cols = vcols;
        }

        return Index{
            .id = self.id,
            .name = try self.allocator.dupe(u8, self.name),
            .table_id = self.table_id,
            .kind = self.kind,
            .key_columns = key_cols,
            .value_columns = value_cols,
            .allocator = self.allocator,
        };
    }

    /// Releases a builder that will not be built: frees the held name and the
    /// duplicated key/value index slices.
    ///
    /// The position slices are plain `usize` arrays with no inner ownership, so
    /// only the slices themselves are freed. Does not touch any [`Index`]
    /// produced by [`build`], which owns its own copies.
    pub fn deinit(self: *IndexBuilder) void {
        self.allocator.free(self.name);
        self.allocator.free(self.key_column_indices);
        if (self.value_column_indices) |v| self.allocator.free(v);
    }
};
