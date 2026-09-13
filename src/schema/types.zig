//! Catalog value types: the on-disk description of tables, columns, indexes,
//! foreign keys, sequences, and generic objects.
//!
//! This module is the vocabulary the schema/catalog layer uses to describe
//! *what* is stored, as opposed to the storage layer (`storage/`) which owns
//! *how* bytes live in pages. Every persistent object in NovaDB, a table, an
//! index, a sequence, an FK constraint, is ultimately one of the structs here,
//! and the catalog persists them by calling the `serialize`/`deserialize`
//! methods defined alongside each type.
//!
//! Two design decisions run through the whole file:
//!
//!   1. **A fixed, ordered enum of column types.** [`ColumnType`] is an
//!      `enum(u8)`, so its numeric tag is the byte written to disk. The variants
//!      must never be reordered or renumbered, doing so would silently
//!      reinterpret every previously written catalog page. [`IndexValue`] is the
//!      runtime counterpart: an actual value carried in an index key, tagged by
//!      the same conceptual type set.
//!
//!   2. **A hand-rolled little-endian wire format for metadata.** The `Metadata`
//!      structs ([`ColumnMetadata`], [`TableMetadata`], [`ObjectMetadata`],
//!      [`IndexMetadata`]) each own a matched `serialize`/`deserialize` pair that
//!      reads and writes a compact byte layout with explicit length prefixes.
//!      The layouts are format contracts: the field order, the little-endian
//!      integer encoding, and the flag-bit assignments in
//!      [`ColumnMetadata.serialize`] must stay in lockstep across the two
//!      directions or a round-trip corrupts. All strings are length-prefixed
//!      `u32` then raw bytes, and every deserialised string is `dupe`d into the
//!      caller's allocator, so the returned metadata owns its heap and must be
//!      freed by the caller.
//!
//! Ownership note: the plain `Column`/`ForeignKey`/`Sequence` structs (which
//! carry an `allocator` or take one in `deinit`) are the *live* catalog objects
//! held in memory; the `*Metadata` structs are their serialisable projections.
//! Freeing is manual throughout, each type documents exactly what it owns.

const std = @import("std");
const index_key = @import("index_key.zig");
/// Shorthand for the standard allocator interface, used by every `init`,
/// `deinit`, `serialize`, and `deserialize` in this file to own or release the
/// duplicated names and payloads that make a catalog object self-contained.
const Allocator = std.mem.Allocator;

/// Classifies an index by how many columns it spans and whether it also stores
/// non-key payload columns.
///
/// The tag is an explicit `u8` because [`IndexMetadata.serialize`] writes
/// `@intFromEnum(self.kind)` as a single byte and
/// [`IndexMetadata.deserialize`] reads it back with `@enumFromInt`; the numeric
/// order is therefore part of the on-disk format and must not be reshuffled.
pub const IndexKind = enum(u8) {
    /// Single-column unique index: the key alone is enough to enforce
    /// uniqueness and locate a row.
    UNIQUE,
    /// Multi-column (compound) index keyed on several columns in order, with no
    /// extra stored payload beyond the key.
    COMPOSITE,
    /// Composite index that additionally stores a set of value columns inline
    /// (a covering index), so index-only scans can answer a query without
    /// touching the base table. Its payload is the `value_columns` of
    /// [`IndexMetadata`].
    COVERING_COMPOSITE,
};

/// A concrete typed value as it appears inside an index key or entry.
///
/// This is the runtime, in-memory counterpart to [`ColumnType`]: the enum names
/// which kind is present, and the payload carries the value. `text` and `blob`
/// borrow their bytes (`[]const u8`), they do not own the underlying buffer, so
/// the referenced storage must outlive the [`IndexValue`].
pub const IndexValue = union(enum) {
    /// A boolean key component.
    bool: bool,
    /// An unsigned 32-bit integer key component.
    uint32: u32,
    /// An unsigned 64-bit integer key component.
    uint64: u64,
    /// A signed 32-bit integer key component.
    int32: i32,
    /// A signed 64-bit integer key component.
    int64: i64,
    /// A 32-bit IEEE float key component.
    float32: f32,
    /// A 64-bit IEEE float key component.
    float64: f64,
    /// A timestamp stored as an unsigned 64-bit count (epoch units).
    timestamp: u64,
    /// A UTF-8 text value; the slice is borrowed, not owned.
    text: []const u8,
    /// An opaque byte blob; the slice is borrowed, not owned.
    blob: []const u8,
};

/// The set of column data types NovaDB understands.
///
/// Backed by `u8`: the tag value is written verbatim to disk by
/// [`ColumnMetadata.serialize`] and mapped to a Zig primitive at comptime by
/// [`Column.primitiveType`]. Because the numeric tag is persisted, variants are
/// append-only, reordering or removing one reinterprets existing catalog bytes.
/// `TEXT` and `BLOB` are variable-length: their fixed on-page footprint is a
/// `u32` (an offset/handle into overflow storage), which is why
/// [`Column.primitiveType`] maps both to `u32`.
pub const ColumnType = enum(u8) {
    /// One-byte boolean.
    BOOL,
    /// Unsigned 32-bit integer.
    UINT32,
    /// Unsigned 64-bit integer.
    UINT64,
    /// Signed 32-bit integer.
    INT32,
    /// Signed 64-bit integer.
    INT64,
    /// 32-bit IEEE floating point.
    FLOAT32,
    /// 64-bit IEEE floating point.
    FLOAT64,
    /// Timestamp stored as an unsigned 64-bit value.
    TIMESTAMP,
    /// Variable-length UTF-8 text; represented in a fixed row slot by a `u32`
    /// handle into overflow/heap storage.
    TEXT,
    /// Variable-length opaque bytes; like `TEXT`, a fixed-slot `u32` handle.
    BLOB,
};

/// Encodes a column value into an order-preserving, colon-free token for the
/// leading part of a secondary-index key.
///
/// Secondary-index keys are `value:value:...:pk`. When the leading column is an
/// integer type its value must sort in numeric order, but the values are stored
/// as decimal text and plain text sorts lexically (so `"9"` sorts after `"10"`).
/// This maps integer/timestamp types to a fixed-width 16-char uppercase-hex of
/// the 64-bit value with its sign bit flipped, so signed numeric order maps
/// exactly to unsigned lexical order, and float types to the IEEE-754
/// total-order transform (flip all bits if negative, else set the sign bit),
/// likewise as 16-char hex. The token never contains a `:` (the key delimiter).
/// Every other type, and any value that does not parse numerically (including
/// the `"NULL"` sentinel), is returned unchanged, matching the old text-key
/// behaviour. The 16 hex chars are a fixed width, so a range bound over the
/// encoded value can be expressed purely by how the seek/end keys are built.
/// The caller owns the returned slice.
///
/// The SAME encoding must be applied at every index-key write site and at the
/// equality/range read sites, or the index and the query will disagree. Getting
/// it wrong on a float column would let a range scan skip in-range rows (which
/// the residual WHERE filter cannot add back), so floats must be handled here
/// rather than falling through to lexical text order.
pub fn encodeIndexValueAlloc(allocator: std.mem.Allocator, col_type: ColumnType, value_str: []const u8) ![]u8 {
    // Fixed-width numeric / timestamp / float columns encode to a compact,
    // delimiter-safe, order-preserving 10-byte token (see `index_key.zig`).
    if (try index_key.encodeAlloc(allocator, col_type, value_str)) |enc| return enc;
    // Non-numeric type, or a value that does not parse (e.g. the "NULL"
    // sentinel): fall back to the raw string, matching the legacy behaviour.
    return allocator.dupe(u8, value_str);
}

/// A live, in-memory column definition owned by the catalog.
///
/// Distinct from [`ColumnMetadata`]: `Column` is the working definition used to
/// build tables and constraints and it owns its `name` (and `default_value`) on
/// the heap, freed by [`Column.deinit`]. `size` and `offset` place the column
/// within a fixed-width row image, so they are set once the row layout is
/// finalised.
pub const Column = struct {
    /// Heap-owned column name; released by [`Column.deinit`].
    name: []const u8,
    /// The column's declared data type; see [`ColumnType`].
    type: ColumnType,
    /// Fixed byte width this column occupies in a row image (for `TEXT`/`BLOB`
    /// this is the size of the handle, not the payload).
    size: u16,
    /// Byte offset of this column within the fixed-width row image.
    offset: u16,

    /// True if this column participates in the table's primary key.
    is_primary_key: bool = false,
    /// True if the engine assigns this column's value from a sequence on insert.
    is_auto_increment: bool = false,
    /// True if `NULL` is permitted; defaults to nullable.
    is_nullable: bool = true,
    /// Optional heap-owned default value bytes, or `null` for no default;
    /// released by [`Column.deinit`] when present.
    default_value: ?[]const u8 = null,

    /// Maps this column's [`ColumnType`] to the Zig primitive that represents it
    /// in a row image, evaluated at comptime.
    ///
    /// `TEXT` and `BLOB` intentionally return `u32`: their slot holds a
    /// fixed-width handle into overflow storage, not the variable-length bytes
    /// themselves. Must be called on a comptime-known `Column` because the
    /// result is a `type`.
    pub fn primitiveType(comptime self: Column) type {
        return switch (self.type) {
            .BOOL => bool,
            .UINT32 => u32,
            .UINT64 => u64,
            .INT32 => i32,
            .INT64 => i64,
            .FLOAT32 => f32,
            .FLOAT64 => f64,
            .TIMESTAMP => u64,
            .TEXT => u32,
            .BLOB => u32,
        };
    }

    /// Frees the heap-owned `name` and, if present, `default_value`.
    ///
    /// Takes the allocator explicitly (unlike [`ForeignKey`]/[`Sequence`], which
    /// store one) because a `Column` is often held by value inside a larger
    /// owning structure that already knows the allocator.
    pub fn deinit(self: *Column, allocator: Allocator) void {
        allocator.free(self.name);
        if (self.default_value) |dv| allocator.free(dv);
    }
};

/// SQL referential action taken on the child rows when a referenced parent row
/// is deleted. `u8`-tagged so it can be persisted as a single byte.
pub const OnDeleteAction = enum(u8) {
    /// Defer checking; standard-SQL default (no immediate enforcement action).
    NO_ACTION,
    /// Reject the delete if any child row references the parent.
    RESTRICT,
    /// Delete the referencing child rows as well.
    CASCADE,
    /// Set the referencing columns in child rows to `NULL`.
    SET_NULL,
    /// Set the referencing columns in child rows to their column default.
    SET_DEFAULT,
};
/// SQL referential action taken on the child rows when a referenced parent key
/// is updated. Mirrors [`OnDeleteAction`]; `u8`-tagged for persistence.
pub const OnUpdateAction = enum(u8) {
    /// Defer checking; standard-SQL default.
    NO_ACTION,
    /// Reject the update if any child row references the old key.
    RESTRICT,
    /// Propagate the new key value to the referencing child rows.
    CASCADE,
    /// Set the referencing columns in child rows to `NULL`.
    SET_NULL,
    /// Set the referencing columns in child rows to their column default.
    SET_DEFAULT,
};

/// A resolved foreign-key constraint linking a child table's columns to a
/// parent table's columns.
///
/// "Resolved" means the referenced table and columns are already bound to
/// concrete ids/[`Column`] definitions (contrast [`ForeignKeyDefinition`],
/// which holds only names, before resolution). The struct owns its `name` and
/// both column slices, including each contained [`Column`]'s heap, and frees
/// them via [`ForeignKey.deinit`] using the stored `allocator`.
pub const ForeignKey = struct {
    /// Catalog-assigned constraint id.
    id: u32,
    /// Heap-owned constraint name (duplicated in [`ForeignKey.init`]).
    name: []const u8,
    /// Id of the child (referencing) table.
    table_id: u32,
    /// Id of the parent (referenced) table.
    referenced_table_id: u32,
    /// Child-side columns that form the foreign key; owned, freed in
    /// [`ForeignKey.deinit`].
    columns: []Column,
    /// Parent-side columns the key references, positionally matched to
    /// `columns`; owned, freed in [`ForeignKey.deinit`].
    referenced_columns: []Column,
    /// Action applied to child rows on parent delete; see [`OnDeleteAction`].
    on_delete: OnDeleteAction,
    /// Action applied to child rows on parent-key update; see [`OnUpdateAction`].
    on_update: OnUpdateAction,
    /// Allocator that owns this constraint's heap; used by [`ForeignKey.deinit`].
    allocator: Allocator,

    /// Constructs a [`ForeignKey`], taking ownership of the passed column
    /// slices and duplicating `name` into `allocator`.
    ///
    /// The `columns` and `referenced_columns` slices are stored as-is (the
    /// constraint now owns them and will free each contained [`Column`]); only
    /// `name` is copied. Returns an error if the name duplication fails.
    pub fn init(
        allocator: Allocator,
        id: u32,
        name: []const u8,
        table_id: u32,
        referenced_table_id: u32,
        columns: []Column,
        referenced_columns: []Column,
        on_delete: OnDeleteAction,
        on_update: OnUpdateAction,
    ) !ForeignKey {
        return ForeignKey{
            .id = id,
            .name = try allocator.dupe(u8, name),
            .table_id = table_id,
            .referenced_table_id = referenced_table_id,
            .columns = columns,
            .referenced_columns = referenced_columns,
            .on_delete = on_delete,
            .on_update = on_update,
            .allocator = allocator,
        };
    }

    /// Releases everything the constraint owns: its `name`, each [`Column`] in
    /// both slices, and the two slice allocations, all via the stored
    /// `allocator`.
    pub fn deinit(self: *ForeignKey) void {
        self.allocator.free(self.name);
        for (self.columns) |*col| {
            col.deinit(self.allocator);
        }
        self.allocator.free(self.columns);
        for (self.referenced_columns) |*col| {
            col.deinit(self.allocator);
        }
        self.allocator.free(self.referenced_columns);
    }
};

/// An unresolved foreign-key declaration expressed purely by names.
///
/// This is what a `CREATE TABLE ... FOREIGN KEY` clause parses to before the
/// catalog binds the names to ids and [`Column`] definitions to produce a
/// [`ForeignKey`]. All slices borrow their strings from the parse input; this
/// struct owns nothing.
pub const ForeignKeyDefinition = struct {
    /// Name of the child (referencing) table.
    table_name: []const u8,
    /// Name of the parent (referenced) table.
    referenced_table_name: []const u8,
    /// Child-side column names, in key order.
    columns: []const []const u8,
    /// Parent-side column names, positionally matched to `columns`.
    referenced_columns: []const []const u8,
};

/// A monotonic counter backing an auto-increment column.
///
/// The live counter itself lives in a shared memory region (so multiple
/// processes/threads can bump it atomically); this struct records the sequence's
/// identity plus the `offset` into that region where its `u64` word sits. The
/// bump happens in [`Sequence.next`]. `name` and `column_name` are heap-owned
/// and freed by [`Sequence.deinit`].
pub const Sequence = struct {
    /// Catalog-assigned sequence id.
    id: u32,
    /// Heap-owned sequence name.
    name: []const u8,
    /// Id of the table this sequence feeds.
    table_id: u32,
    /// Heap-owned name of the auto-increment column served.
    column_name: []const u8,
    /// Byte offset of this sequence's `u64` counter within the shared memory
    /// region passed to [`Sequence.next`].
    offset: u32,
    /// Last-known counter value (advisory snapshot; the authoritative value is
    /// the shared-memory word).
    current_value: u64,
    /// Step added on each allocation. Defaults to 1.
    increment_by: u64 = 1,
    /// Lower bound of the sequence range. Defaults to 1.
    min_value: u64 = 1,
    /// Upper bound of the sequence range. Defaults to the `u64` maximum.
    max_value: u64 = std.math.maxInt(u64),
    /// Whether the sequence wraps back to `min_value` on overflow. Defaults off.
    cycle: bool = false,
    /// Allocator owning `name`/`column_name`; used by [`Sequence.deinit`].
    allocator: Allocator,

    /// Atomically fetches the current counter and increments it by one,
    /// returning the pre-increment value.
    ///
    /// The counter is read from `shared_mem_ptr + self.offset`, so the caller
    /// must pass the base of the same shared region the sequence was placed in.
    /// Uses a `.Monotonic` fetch-add of `increment_by`: correct for handing out
    /// unique numbers concurrently, but it imposes no ordering on surrounding
    /// memory. The returned value is the pre-increment counter (the number this
    /// call allocates). `min_value`/`max_value`/`cycle` are not enforced here;
    /// they remain advisory bounds until a wrapping CAS path is added.
    pub fn next(self: *Sequence, shared_mem_ptr: [*]u8) u64 {
        const ptr: *u64 = @ptrCast(shared_mem_ptr + self.offset);
        const step = if (self.increment_by == 0) 1 else self.increment_by;
        return @atomicRmw(u64, ptr, .Add, step, .Monotonic);
    }

    /// Frees the heap-owned `name` and `column_name` via the stored allocator.
    /// Does not touch the shared-memory counter, which the sequence does not own.
    pub fn deinit(self: *Sequence) void {
        self.allocator.free(self.name);
        self.allocator.free(self.column_name);
    }
};

/// The serialisable projection of a [`Column`], with a compact on-disk byte
/// layout.
///
/// Field-for-field identical to [`Column`] minus the working conveniences; it
/// exists so a column can be written to and read back from a catalog page.
/// [`ColumnMetadata.serialize`] and [`ColumnMetadata.deserialize`] define a
/// format contract that must stay symmetric. A deserialised value owns its
/// `name` (and `default_value`) on the heap.
pub const ColumnMetadata = struct {
    /// Column name; heap-owned after [`ColumnMetadata.deserialize`].
    name: []const u8,
    /// Declared data type; see [`ColumnType`].
    type: ColumnType,
    /// Fixed byte width in a row image.
    size: u16,
    /// Byte offset within a row image.
    offset: u16,
    /// True if part of the primary key.
    is_primary_key: bool = false,
    /// True if fed from a [`Sequence`] on insert.
    is_auto_increment: bool = false,
    /// True if `NULL` is permitted.
    is_nullable: bool = true,
    /// Optional heap-owned default value bytes, or `null`.
    default_value: ?[]const u8 = null,

    /// Serialises this column into a freshly allocated little-endian byte
    /// buffer that [`ColumnMetadata.deserialize`] can reverse.
    ///
    /// Layout, in order: `type` (1 byte tag), `size` (`u16`), `offset` (`u16`),
    /// a `flags` byte, `name_len` (`u32`), the name bytes, and, only when a
    /// default is present, `def_len` (`u32`) plus the default bytes. The flag
    /// bits are fixed: `1` primary key, `2` auto-increment, `4` nullable, `8`
    /// default-present; these bit values are part of the format and must match
    /// the reader. Returns the owned buffer (caller frees) or an allocation
    /// error.
    pub fn serialize(self: ColumnMetadata, allocator: Allocator) ![]const u8 {
        const name_len = @as(u32, @intCast(self.name.len));
        var def_len: u32 = 0;
        if (self.default_value) |dv| {
            def_len = @as(u32, @intCast(dv.len));
        }

        var flags: u8 = 0;
        if (self.is_primary_key) flags |= 1;
        if (self.is_auto_increment) flags |= 2;
        if (self.is_nullable) flags |= 4;
        if (self.default_value != null) flags |= 8;

        const total_size = 1 + 2 + 2 + 1 + 4 + name_len + (if (self.default_value != null) 4 + def_len else 0);
        const bytes = try allocator.alloc(u8, total_size);
        errdefer allocator.free(bytes);

        var offset: usize = 0;
        bytes[offset] = @intFromEnum(self.type);
        offset += 1;
        std.mem.writeInt(u16, bytes[offset..][0..2], self.size, .little);
        offset += 2;
        std.mem.writeInt(u16, bytes[offset..][0..2], self.offset, .little);
        offset += 2;
        bytes[offset] = flags;
        offset += 1;
        std.mem.writeInt(u32, bytes[offset..][0..4], name_len, .little);
        offset += 4;
        @memcpy(bytes[offset .. offset + name_len], self.name);
        offset += name_len;

        if (self.default_value) |dv| {
            std.mem.writeInt(u32, bytes[offset..][0..4], def_len, .little);
            offset += 4;
            @memcpy(bytes[offset .. offset + def_len], dv);
            offset += def_len;
        }

        return bytes;
    }

    /// Reads one column back from a byte buffer at `offset.*`, advancing
    /// `offset` past the bytes consumed.
    ///
    /// The `offset` is an in/out cursor precisely so this can be called in a
    /// loop to parse a run of columns from a larger buffer (see
    /// [`TableMetadata.deserialize`] and [`IndexMetadata.deserialize`]). It
    /// reverses [`ColumnMetadata.serialize`] exactly, decoding the same flag
    /// bits, and `dupe`s both the `name` and any `default_value` into
    /// `allocator`, so the returned metadata owns that heap. Returns an
    /// allocation error on failure. It trusts the buffer to be well-formed:
    /// lengths are read directly and used to slice, so truncated or corrupt
    /// input is not validated here.
    pub fn deserialize(allocator: Allocator, bytes: []const u8, offset: *usize) !ColumnMetadata {
        const t_val = bytes[offset.*];
        offset.* += 1;
        const col_type = @as(ColumnType, @enumFromInt(t_val));

        const size = std.mem.readInt(u16, bytes[offset.*..][0..2], .little);
        offset.* += 2;

        const col_offset = std.mem.readInt(u16, bytes[offset.*..][0..2], .little);
        offset.* += 2;

        const flags = bytes[offset.*];
        offset.* += 1;

        const name_len = std.mem.readInt(u32, bytes[offset.*..][0..4], .little);
        offset.* += 4;

        const name = try allocator.dupe(u8, bytes[offset.* .. offset.* + name_len]);
        offset.* += name_len;

        const is_primary_key = (flags & 1) != 0;
        const is_auto_increment = (flags & 2) != 0;
        const is_nullable = (flags & 4) != 0;

        var default_value: ?[]const u8 = null;
        if ((flags & 8) != 0) {
            const def_len = std.mem.readInt(u32, bytes[offset.*..][0..4], .little);
            offset.* += 4;
            default_value = try allocator.dupe(u8, bytes[offset.* .. offset.* + def_len]);
            offset.* += def_len;
        }

        return ColumnMetadata{
            .name = name,
            .type = col_type,
            .size = size,
            .offset = col_offset,
            .is_primary_key = is_primary_key,
            .is_auto_increment = is_auto_increment,
            .is_nullable = is_nullable,
            .default_value = default_value,
        };
    }
};

/// The persisted description of a table: its identity, its columns, and the
/// page where its B+Tree root lives.
///
/// This is the unit the catalog writes per user table. Its `serialize`/
/// `deserialize` pair frames the whole record (id, root page, name, then a
/// counted run of [`ColumnMetadata`]). A deserialised value owns its `name` and
/// the `columns` array (including each column's heap).
pub const TableMetadata = struct {
    /// Catalog-assigned table id.
    id: u32,
    /// Table name; heap-owned after [`TableMetadata.deserialize`].
    name: []const u8,
    /// The table's columns in declaration order; owned after deserialisation.
    columns: []const ColumnMetadata,
    /// Page id of the root of this table's B+Tree in the storage layer.
    root_page_id: u64,

    /// Serialises the table record into a freshly allocated little-endian
    /// buffer, returning the owned slice (caller frees) or an allocation error.
    ///
    /// Layout: `id` (`u32`), `root_page_id` (`u64`), `name_len` (`u32`) + name
    /// bytes, `col_count` (`u32`), then each column's
    /// [`ColumnMetadata.serialize`] output concatenated. Note the field order
    /// here (root page before name) differs from [`IndexMetadata.serialize`];
    /// each metadata type owns its own independent layout and its reader must
    /// match it exactly.
    pub fn serialize(self: TableMetadata, allocator: Allocator) ![]const u8 {
        var list = std.ArrayList(u8).empty;
        errdefer list.deinit(allocator);

        var id_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &id_bytes, self.id, .little);
        try list.appendSlice(allocator, &id_bytes);

        var root_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &root_bytes, self.root_page_id, .little);
        try list.appendSlice(allocator, &root_bytes);

        var name_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &name_len_bytes, @intCast(self.name.len), .little);
        try list.appendSlice(allocator, &name_len_bytes);
        try list.appendSlice(allocator, self.name);

        var col_count_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &col_count_bytes, @intCast(self.columns.len), .little);
        try list.appendSlice(allocator, &col_count_bytes);

        for (self.columns) |col| {
            const col_bytes = try col.serialize(allocator);
            defer allocator.free(col_bytes);
            try list.appendSlice(allocator, col_bytes);
        }

        return try list.toOwnedSlice(allocator);
    }

    /// Reverses [`TableMetadata.serialize`], reconstructing a table record from
    /// bytes and duplicating all owned data into `allocator`.
    ///
    /// Uses a local cursor, reading id, root page, name, and column count, then
    /// looping `col_count` times through [`ColumnMetadata.deserialize`] (which
    /// shares the cursor). The returned value owns `name` and the `columns`
    /// array. The `errdefer` here iterates `columns[0..0]`, an empty slice, so
    /// on a mid-loop allocation failure the already-parsed columns are not
    /// individually freed; callers should treat a failed deserialise as leaving
    /// nothing to clean up beyond what the allocator reclaims. Returns an
    /// allocation error on failure.
    pub fn deserialize(allocator: Allocator, bytes: []const u8) !TableMetadata {
        var offset: usize = 0;
        const id = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const root_page_id = std.mem.readInt(u64, bytes[offset..][0..8], .little);
        offset += 8;
        const name_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const name = try allocator.dupe(u8, bytes[offset .. offset + name_len]);
        offset += name_len;
        const col_count = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;

        var columns = try allocator.alloc(ColumnMetadata, col_count);
        errdefer {
            for (columns[0..0]) |*col| {
                allocator.free(col.name);
                if (col.default_value) |dv| allocator.free(dv);
            }
            allocator.free(columns);
        }

        var i: usize = 0;
        while (i < col_count) : (i += 1) {
            columns[i] = try ColumnMetadata.deserialize(allocator, bytes, &offset);
        }

        return TableMetadata{
            .id = id,
            .name = name,
            .columns = columns,
            .root_page_id = root_page_id,
        };
    }
};

/// A generic catalog object descriptor: an id, a name, a free-form `type`
/// string, and a root page.
///
/// Where [`TableMetadata`] and [`IndexMetadata`] describe specific kinds of
/// object with full structure, `ObjectMetadata` is the lowest-common-denominator
/// entry used for anything the catalog tracks by name and root page, with its
/// kind carried as a string (for example `"table"`, `"index"`, `"sequence"`). A
/// deserialised value owns both `name` and `type`.
pub const ObjectMetadata = struct {
    /// Catalog-assigned object id.
    id: u32,
    /// Object name; heap-owned after [`ObjectMetadata.deserialize`].
    name: []const u8,
    /// Free-form object kind string; heap-owned after deserialisation.
    type: []const u8,
    /// Page id of the object's root in the storage layer.
    root_page_id: u64,

    /// Serialises the object descriptor into a freshly allocated little-endian
    /// buffer, returning the owned slice (caller frees) or an allocation error.
    ///
    /// Layout: `id` (`u32`), `root_page_id` (`u64`), `type_len` (`u32`) + type
    /// bytes, `name_len` (`u32`) + name bytes. Note that `type` is written
    /// before `name` here; [`ObjectMetadata.deserialize`] reads them in the same
    /// order.
    pub fn serialize(self: ObjectMetadata, allocator: Allocator) ![]const u8 {
        var list = std.ArrayList(u8).empty;
        errdefer list.deinit(allocator);

        var id_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &id_bytes, self.id, .little);
        try list.appendSlice(allocator, &id_bytes);

        var root_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &root_bytes, self.root_page_id, .little);
        try list.appendSlice(allocator, &root_bytes);

        var type_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &type_len_bytes, @intCast(self.type.len), .little);
        try list.appendSlice(allocator, &type_len_bytes);
        try list.appendSlice(allocator, self.type);

        var name_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &name_len_bytes, @intCast(self.name.len), .little);
        try list.appendSlice(allocator, &name_len_bytes);
        try list.appendSlice(allocator, self.name);

        return try list.toOwnedSlice(allocator);
    }

    /// Reverses [`ObjectMetadata.serialize`], reconstructing the descriptor and
    /// duplicating `type` and `name` into `allocator`.
    ///
    /// The `errdefer` frees the already-duplicated `type_str` if the subsequent
    /// `name` duplication fails, so no leak occurs on partial construction. The
    /// returned value owns both strings. Returns an allocation error on failure.
    pub fn deserialize(allocator: Allocator, bytes: []const u8) !ObjectMetadata {
        var offset: usize = 0;
        const id = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const root_page_id = std.mem.readInt(u64, bytes[offset..][0..8], .little);
        offset += 8;
        const type_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const type_str = try allocator.dupe(u8, bytes[offset .. offset + type_len]);
        errdefer allocator.free(type_str);
        offset += type_len;
        const name_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const name = try allocator.dupe(u8, bytes[offset .. offset + name_len]);
        return ObjectMetadata{
            .id = id,
            .name = name,
            .type = type_str,
            .root_page_id = root_page_id,
        };
    }
};

/// The persisted description of an index: its identity, the table it covers,
/// its kind, its key columns, optional covering value columns, and its B+Tree
/// root page.
///
/// The presence of `value_columns` corresponds to a
/// [`IndexKind.COVERING_COMPOSITE`] index; for plain indexes it is `null`. The
/// serialise/deserialise pair encodes this optionality with a one-byte presence
/// flag. A deserialised value owns `name`, the `key_columns` array, and, when
/// present, the `value_columns` array (each including its columns' heap).
pub const IndexMetadata = struct {
    /// Catalog-assigned index id.
    id: u32,
    /// Index name; heap-owned after [`IndexMetadata.deserialize`].
    name: []const u8,
    /// Id of the table this index is built on.
    table_id: u32,
    /// The index's shape/kind; see [`IndexKind`].
    kind: IndexKind,
    /// Columns forming the index key, in key order; owned after deserialisation.
    key_columns: []const ColumnMetadata,
    /// For a covering index, the extra columns stored inline; `null` otherwise.
    /// Owned after deserialisation when present.
    value_columns: ?[]const ColumnMetadata,
    /// Page id of the root of this index's B+Tree.
    root_page_id: u64,
    /// `true` when every index entry is known to correspond one-to-one to a live
    /// row: the index was built fresh (so it indexes only currently-visible rows)
    /// and the table has had NO delete/update since (which would leave a stale
    /// entry behind under MVCC). Serialised as a trailing byte, so records written
    /// by older versions (which lack it) deserialise to `false` (the safe value:
    /// treat as possibly-inexact). Lets an index-only COUNT trust the entry count
    /// without fetching each base row. Set `true` at CREATE INDEX; flipped to
    /// `false` (pessimistically, so a rollback that leaves it false is still safe)
    /// on the first delete/update to the table.
    exact: bool = false,

    /// Serialises the index record into a freshly allocated little-endian
    /// buffer, returning the owned slice (caller frees) or an allocation error.
    ///
    /// Layout: `id` (`u32`), `root_page_id` (`u64`), `table_id` (`u32`), `kind`
    /// (1 byte), `name_len` (`u32`) + name, `key_col_count` (`u32`) + each key
    /// column's [`ColumnMetadata.serialize`] output, then a one-byte
    /// value-columns presence flag; if `1`, `val_col_count` (`u32`) followed by
    /// each value column, if `0`, nothing more. The presence flag is what lets
    /// [`IndexMetadata.deserialize`] distinguish a covering index with zero
    /// value columns from a non-covering one.
    pub fn serialize(self: IndexMetadata, allocator: Allocator) ![]const u8 {
        var list = std.ArrayList(u8).empty;
        errdefer list.deinit(allocator);

        var id_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &id_bytes, self.id, .little);
        try list.appendSlice(allocator, &id_bytes);

        var root_bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &root_bytes, self.root_page_id, .little);
        try list.appendSlice(allocator, &root_bytes);

        var table_id_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &table_id_bytes, self.table_id, .little);
        try list.appendSlice(allocator, &table_id_bytes);

        try list.append(allocator, @intFromEnum(self.kind));

        var name_len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &name_len_bytes, @intCast(self.name.len), .little);
        try list.appendSlice(allocator, &name_len_bytes);
        try list.appendSlice(allocator, self.name);

        var key_col_count_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &key_col_count_bytes, @intCast(self.key_columns.len), .little);
        try list.appendSlice(allocator, &key_col_count_bytes);

        for (self.key_columns) |col| {
            const col_bytes = try col.serialize(allocator);
            defer allocator.free(col_bytes);
            try list.appendSlice(allocator, col_bytes);
        }

        if (self.value_columns) |vcols| {
            try list.append(allocator, 1);
            var val_col_count_bytes: [4]u8 = undefined;
            std.mem.writeInt(u32, &val_col_count_bytes, @intCast(vcols.len), .little);
            try list.appendSlice(allocator, &val_col_count_bytes);
            for (vcols) |col| {
                const col_bytes = try col.serialize(allocator);
                defer allocator.free(col_bytes);
                try list.appendSlice(allocator, col_bytes);
            }
        } else {
            try list.append(allocator, 0);
        }

        // Trailing exactness byte (see `exact`). Appended last so older readers
        // that stop after the value-columns block simply ignore it, and older
        // writers' records (which lack it) read back as `false`.
        try list.append(allocator, @intFromBool(self.exact));

        return try list.toOwnedSlice(allocator);
    }

    /// Reverses [`IndexMetadata.serialize`], reconstructing the index record and
    /// duplicating all owned data into `allocator`.
    ///
    /// Reads the fixed header, then loops through the key columns, then consumes
    /// the presence flag; only if it is `1` does it allocate and parse the
    /// value-columns run, leaving `value_columns` as `null` otherwise. Key and
    /// value columns are decoded via [`ColumnMetadata.deserialize`] sharing the
    /// cursor. As in the sibling deserialisers, the `errdefer` loops iterate an
    /// empty slice, so a mid-loop failure does not free the partially parsed
    /// columns individually. Returns an allocation error on failure.
    pub fn deserialize(allocator: Allocator, bytes: []const u8) !IndexMetadata {
        var offset: usize = 0;
        const id = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const root_page_id = std.mem.readInt(u64, bytes[offset..][0..8], .little);
        offset += 8;
        const table_id = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const kind_val = bytes[offset];
        offset += 1;
        const kind = @as(IndexKind, @enumFromInt(kind_val));

        const name_len = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;
        const name = try allocator.dupe(u8, bytes[offset .. offset + name_len]);
        offset += name_len;

        const key_col_count = std.mem.readInt(u32, bytes[offset..][0..4], .little);
        offset += 4;

        var key_columns = try allocator.alloc(ColumnMetadata, key_col_count);
        errdefer {
            for (key_columns[0..0]) |*col| {
                allocator.free(col.name);
                if (col.default_value) |dv| allocator.free(dv);
            }
            allocator.free(key_columns);
        }

        var i: usize = 0;
        while (i < key_col_count) : (i += 1) {
            key_columns[i] = try ColumnMetadata.deserialize(allocator, bytes, &offset);
        }

        const has_val_cols = bytes[offset];
        offset += 1;

        var value_columns: ?[]ColumnMetadata = null;
        if (has_val_cols == 1) {
            const val_col_count = std.mem.readInt(u32, bytes[offset..][0..4], .little);
            offset += 4;
            var val_cols = try allocator.alloc(ColumnMetadata, val_col_count);
            errdefer {
                for (val_cols[0..0]) |*col| {
                    allocator.free(col.name);
                    if (col.default_value) |dv| allocator.free(dv);
                }
                allocator.free(val_cols);
            }
            var j: usize = 0;
            while (j < val_col_count) : (j += 1) {
                val_cols[j] = try ColumnMetadata.deserialize(allocator, bytes, &offset);
            }
            value_columns = val_cols;
        }

        // Optional trailing exactness byte; absent in records written by older
        // versions, in which case the index is treated as possibly-inexact.
        const exact = offset < bytes.len and bytes[offset] != 0;

        return IndexMetadata{
            .id = id,
            .name = name,
            .table_id = table_id,
            .kind = kind,
            .key_columns = key_columns,
            .value_columns = value_columns,
            .root_page_id = root_page_id,
            .exact = exact,
        };
    }
};
