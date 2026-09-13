//! Maps NovaDB's internal column types onto the binary wire protocol's type
//! identifiers (OIDs) and on-wire size hints.
//!
//! The wire protocol describes every result column to a client with a
//! `RowDescription` message, and each column in it carries three pieces of
//! type metadata: a numeric type OID (see [`wire.oid`]), the column position,
//! and a fixed on-wire size (or `-1` for variable-length values). NovaDB stores
//! and reasons about columns using the richer [`ColumnType`] enum from the
//! schema layer; this file is the single, small translation seam that converts
//! a schema-side [`ColumnType`] into the protocol-side facts a client needs.
//!
//! Keeping the mapping isolated here matters for two reasons. First, the OID
//! numbers are part of the on-the-wire contract: a client decoder keys off them
//! to pick a value decoder, so they must stay stable and must never diverge
//! between the row description and the actual row bytes. Second, the mapping is
//! total by construction. Every arm of [`oidForColumn`] and [`typeSize`]
//! switches exhaustively over [`ColumnType`] with no `else`, so adding a new
//! column type is a compile error until it is given an OID and a size here. The
//! accompanying test walks every enum value to guard that totality at test time
//! as well.
//!
//! Design note on sizes: the `type_size` is a hint, not a framing rule. Fixed
//! types report their exact byte width (`BOOL` = 1, the 32-bit types = 4, the
//! 64-bit types = 8), while `TEXT` and `BLOB` report `-1` because their length
//! is carried per value on the wire rather than being known from the type
//! alone. `TIMESTAMP` is encoded as an 8-byte integer, hence size 8.

const wire = @import("wire.zig");
const ColumnType = @import("../schema/types.zig").ColumnType;

/// Returns the wire-protocol type OID that names a given schema [`ColumnType`].
///
/// The returned OID is the on-the-wire type tag a client uses to select a value
/// decoder, so this mapping is a stable part of the binary protocol contract:
/// the OID emitted here for a column in the `RowDescription` must match the
/// encoding actually used for that column's row values. The switch is
/// exhaustive with no `else`, so a newly added [`ColumnType`] will fail to
/// compile until it is assigned an OID here. See [`wire.oid`] for the numeric
/// values and [`fieldDesc`] for where this is combined with [`typeSize`].
pub fn oidForColumn(t: ColumnType) wire.Oid {
    return switch (t) {
        .BOOL => wire.oid.bool_v,
        .INT32 => wire.oid.int4,
        .INT64 => wire.oid.int8,
        .UINT32 => wire.oid.uint4,
        .UINT64 => wire.oid.uint8,
        .FLOAT32 => wire.oid.float4,
        .FLOAT64 => wire.oid.float8,
        .TIMESTAMP => wire.oid.timestamp,
        .TEXT => wire.oid.text,
        .BLOB => wire.oid.blob,
    };
}

/// True for the fixed-width numeric/bool column types that can be shipped as
/// big-endian binary cells (int4/int8/uint4/uint8/float4/float8/timestamp/bool).
/// TEXT/BLOB are always text on the wire.
pub fn isBinaryType(t: ColumnType) bool {
    return switch (t) {
        .TEXT, .BLOB => false,
        else => true,
    };
}

/// Returns the fixed on-wire byte width for a [`ColumnType`], or `-1` when the
/// type is variable-length.
///
/// This is the `type_size` field a client sees in the `RowDescription`. Fixed
/// types report their exact width so a decoder can frame them by width alone:
/// `BOOL` is a single byte, the 32-bit numeric types (`INT32`, `UINT32`,
/// `FLOAT32`) are 4, and the 64-bit types (`INT64`, `UINT64`, `FLOAT64`, plus
/// `TIMESTAMP`, which is encoded as an 8-byte integer) are 8. `TEXT` and `BLOB`
/// return `-1` because their length is not fixed by the type and is instead
/// carried per value on the wire. Exhaustive with no `else`, so a new
/// [`ColumnType`] must be given a size here to compile. Paired with
/// [`oidForColumn`] by [`fieldDesc`].
pub fn typeSize(t: ColumnType) i16 {
    return switch (t) {
        .BOOL => 1,
        .INT32, .UINT32, .FLOAT32 => 4,
        .INT64, .UINT64, .FLOAT64, .TIMESTAMP => 8,
        .TEXT, .BLOB => -1,
    };
}

/// Builds a complete [`wire.FieldDesc`] for one result column from its name,
/// schema type, and zero-based column position.
///
/// This is the convenience seam that assembles the three type facts a client
/// needs into a single description entry: it fills `type_oid` from
/// [`oidForColumn`] and `type_size` from [`typeSize`] so callers cannot pair a
/// column's OID with the wrong size. The `name` slice is borrowed, not copied,
/// so it must outlive the returned [`wire.FieldDesc`]. A slice of these is what
/// `wire.encodeRowDescription` serialises into the `RowDescription` message.
pub fn fieldDesc(name: []const u8, t: ColumnType, col_no: u16) wire.FieldDesc {
    return .{
        .name = name,
        .col_no = col_no,
        .type_oid = oidForColumn(t),
        .type_size = typeSize(t),
    };
}

/// Standard library import, used only by the totality test below.
const std = @import("std");
test "oid map covers every ColumnType" {
    inline for (@typeInfo(ColumnType).@"enum".fields) |f| {
        const t: ColumnType = @enumFromInt(f.value);
        _ = oidForColumn(t);
        _ = typeSize(t);
    }
}
