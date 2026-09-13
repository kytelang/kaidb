//! PostgreSQL-style binary wire-protocol codec for the NovaDB server.
//!
//! This module is the pure, transport-agnostic encode/decode layer that sits
//! between raw bytes on a socket and the typed messages the server loop in
//! `session.zig` acts on. It knows nothing about sockets, allocation policy, or
//! query execution: it only turns Zig values into protocol frames and back.
//! That separation is deliberate, so the framing rules can be unit-tested in
//! isolation (see the `test` blocks at the bottom) without standing up a
//! server.
//!
//! ## Framing
//!
//! Every message except the initial startup packet is a length-prefixed frame:
//!
//! ```text
//!   +--------+------------------+-------------------------+
//!   | type   | length (u32, BE) | payload (length-4 bytes)|
//!   | 1 byte | 4 bytes          |                         |
//!   +--------+------------------+-------------------------+
//! ```
//!
//! The length field is big-endian and, following the PostgreSQL convention,
//! COUNTS ITSELF: it is `4 + payload_len`, so the total bytes on the wire are
//! `1 + length`. All multi-byte integers in this protocol are big-endian
//! (network byte order); [`Builder`] and [`Cursor`] are the only two places
//! that endianness is spelled out, everything else goes through them.
//!
//! [`parseFrame`] is written for a streaming reader: given whatever bytes have
//! arrived so far it returns `null` (rather than an error) when the buffer does
//! not yet hold a complete frame, so the caller can read more and retry. It
//! only errors on frames that can never be valid: a length below the 4-byte
//! minimum, or one exceeding [`max_frame_len`] (a denial-of-service guard so a
//! hostile or corrupt client cannot make the server pre-allocate gigabytes).
//!
//! ## The two directions
//!
//! [`Frontend`] tags are messages a CLIENT sends (query, parse, bind, execute,
//! ...); [`Backend`] tags are messages the SERVER sends (row description, data
//! row, ready-for-query, ...). The tag byte values match PostgreSQL's, which is
//! what lets an off-the-shelf Postgres client talk to NovaDB.
//!
//! ## NULL handling
//!
//! A field value is length-prefixed with a SIGNED i32; a length of -1 means SQL
//! NULL, distinct from an empty (length-0) string. [`Builder.putValue`] and
//! [`Cursor.value`] are the two ends of that convention, and the "data row with
//! NULL round-trips" test pins it so a future refactor cannot accidentally
//! collapse NULL and empty.
//!
//! ## Ownership
//!
//! Decoders come in two flavours. The simple ones ([`decodeStartup`],
//! [`decodeQuery`], [`decodeExecute`], and the [`Cursor`] readers) return
//! slices that BORROW the caller's payload buffer, so the result is only valid
//! while that buffer lives; they allocate nothing. The variable-arity ones
//! ([`decodeParse`], [`decodeBind`]) must allocate arrays for their counted
//! fields and hand back a struct with a `deinit` that frees them; those use
//! `errdefer` so a truncated message frees any partial allocation before
//! returning the error. Encoders always allocate the finished frame with the
//! caller's allocator and transfer ownership of it to the caller.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

/// Type tag byte for a message the CLIENT sends to the server.
///
/// The values are the PostgreSQL frontend message-type bytes, chosen so a
/// standard Postgres client interoperates with NovaDB. The non-exhaustive `_`
/// member lets [`parseFrame`] surface an unknown tag as data rather than a
/// decode error, leaving the policy decision (reject vs ignore) to the server
/// loop in `session.zig`.
pub const Frontend = enum(u8) {
    /// Initial connection request (startup packet). Note this member is only a
    /// symbolic label: the real startup packet is UNTAGGED on the wire, so this
    /// byte is used to frame startup in tests, not to recognise it on a socket.
    startup = 'H',
    /// Reply to an authentication challenge (e.g. the password/SASL response).
    auth_response = 'p',
    /// Simple-query protocol: an SQL string to parse, execute, and return in one round trip.
    query = 'Q',
    /// Extended-query: create a (possibly named) prepared statement from SQL plus parameter type OIDs.
    parse = 'P',
    /// Extended-query: bind parameter values to a prepared statement, producing a portal to execute.
    bind = 'B',
    /// Extended-query: request the row description (or parameter description) of a statement/portal.
    describe = 'D',
    /// Extended-query: run a bound portal, optionally capped to `max_rows` rows.
    execute = 'E',
    /// Extended-query: end of a request batch; flush and reply ready-for-query.
    sync = 'S',
    /// Close a named prepared statement or portal, releasing its server-side resources.
    close = 'C',
    /// Client is disconnecting; the server should tear the session down cleanly.
    terminate = 'X',
    /// NovaDB document-model operation (insert/find/...). This is NOT a
    /// PostgreSQL frontend byte, so a standard PG client never sends it; the
    /// NovaDB driver uses it to carry BSON document commands over the same
    /// negotiated connection. Payload shape: see [`decodeDocRequest`].
    doc_op = 'J',
    /// Non-exhaustive catch-all so an unrecognised tag byte does not make the
    /// enum conversion illegal; unknown frontend messages are handled as data.
    _,
};

/// Type tag byte for a message the SERVER sends to the client.
///
/// Values match PostgreSQL's backend message-type bytes. Unlike [`Frontend`]
/// this enum is exhaustive: the server only ever emits these, so an out-of-set
/// value would be a bug on our own encoding side, not untrusted input.
pub const Backend = enum(u8) {
    /// Authentication status/challenge; the payload's first u32 is an [`AuthMethod`].
    auth = 'R',
    /// A single server runtime parameter (key/value), e.g. `server_version`, sent after auth.
    parameter_status = 'S',
    /// Backend process id + secret key, used by the client to issue cancel requests.
    backend_key = 'K',
    /// Ready-for-query; payload carries a [`TxnStatus`]. Marks the end of a response cycle.
    ready = 'Z',
    /// Describes the columns of a forthcoming result set (a sequence of [`FieldDesc`]).
    row_description = 'T',
    /// One row of result values, NULLs encoded as -1 lengths (see [`Builder.putValue`]).
    data_row = 'D',
    /// A command finished; payload is the completion tag (e.g. `INSERT 0 1`).
    command_complete = 'C',
    /// Acknowledges a [`Frontend.parse`] succeeded.
    parse_complete = '1',
    /// Acknowledges a [`Frontend.bind`] succeeded.
    bind_complete = '2',
    /// Acknowledges a [`Frontend.close`] succeeded.
    close_complete = '3',
    /// Execution stopped early because the row cap was hit; the portal is still open.
    portal_suspended = 's',
    /// The submitted query string was empty.
    empty_query = 'I',
    /// The described statement/portal returns no rows.
    no_data = 'n',
    /// An error terminating the current command; payload is severity/code/message.
    error_response = 'E',
    /// A non-fatal notice; same payload shape as [`Backend.error_response`] but advisory.
    notice_response = 'N',
};

/// Transaction status reported in a [`Backend.ready`] message.
///
/// Tells the client whether the next command runs autocommit (`idle`), inside
/// an open transaction (`in_txn`), or inside a transaction that has already
/// errored and will roll back (`failed`, in which only ROLLBACK is accepted).
pub const TxnStatus = enum(u8) {
    /// Not in a transaction block; each statement autocommits.
    idle = 'I',
    /// Inside an open transaction block.
    in_txn = 'T',
    /// Inside a failed transaction; commands are rejected until it is rolled back.
    failed = 'E',
};

/// Authentication method/result code carried in the first u32 of a [`Backend.auth`] payload.
pub const AuthMethod = enum(u32) {
    /// Authentication succeeded; no further challenge follows.
    ok = 0,
    /// Server requests a cleartext password in the next [`Frontend.auth_response`].
    cleartext_password = 3,
    /// Server requests a SASL (SCRAM) authentication exchange.
    sasl = 10,
};

/// Encoding of a parameter or result value: `text` (human-readable) or `binary` (raw typed bytes).
///
/// Chosen per column at [`Frontend.bind`] time; [`ParsedBind.paramFormat`]
/// resolves the effective format for a given parameter index.
pub const Format = enum(u16) {
    /// Value is the SQL literal's textual representation.
    text = 0,
    /// Value is the type's raw binary representation.
    binary = 1,
};

/// A NovaDB object identifier for a data type, mirroring PostgreSQL's `Oid` concept.
pub const Oid = u32;
/// The fixed set of built-in type OIDs NovaDB advertises in row descriptions and parameter lists.
///
/// These are NovaDB's own small, dense numbering (not PostgreSQL's system-catalog
/// OIDs); the driver maps them to Nova types. Referenced as `oid.int8` etc. from
/// [`FieldDesc.type_oid`] and [`decodeParse`].
pub const oid = struct {
    /// Boolean.
    pub const bool_v: Oid = 1;
    /// 32-bit signed integer.
    pub const int4: Oid = 2;
    /// 64-bit signed integer.
    pub const int8: Oid = 3;
    /// 32-bit unsigned integer.
    pub const uint4: Oid = 4;
    /// 64-bit unsigned integer.
    pub const uint8: Oid = 5;
    /// 32-bit IEEE-754 float.
    pub const float4: Oid = 6;
    /// 64-bit IEEE-754 float.
    pub const float8: Oid = 7;
    /// Timestamp.
    pub const timestamp: Oid = 8;
    /// UTF-8 text string.
    pub const text: Oid = 9;
    /// Arbitrary binary blob.
    pub const blob: Oid = 10;
    /// Exact fixed-point decimal.
    pub const decimal: Oid = 11;
};

/// Metadata for one result column, emitted inside a [`Backend.row_description`] message.
///
/// The defaults describe an unattached expression column (no source table); the
/// negative `type_size`/`type_mod` sentinels mean "variable length / no
/// modifier", matching the PostgreSQL row-description semantics.
pub const FieldDesc = struct {
    /// Column name as it should appear to the client.
    name: []const u8,
    /// Source table's object id, or 0 if the column is not a plain table column.
    table_id: u32 = 0,
    /// 1-based column number within the source table, or 0 if not applicable.
    col_no: u16 = 0,
    /// Data type of the column, one of the [`oid`] constants.
    type_oid: Oid,
    /// Fixed byte size of the type, or -1 for variable-length types.
    type_size: i16 = -1, // -1 = variable length
    /// Type-specific modifier (e.g. varchar length), or -1 for none.
    type_mod: i32 = -1,
    /// Wire encoding of the column's values in the following data rows.
    format: Format = .text,
};

/// Upper bound on a single frame's declared length, rejecting anything larger in [`parseFrame`].
///
/// 64 MiB. This is a denial-of-service guard: without it a client could send a
/// frame header claiming a multi-gigabyte length and force the server to buffer
/// (or attempt to allocate) that much before noticing the body never arrives.
pub const max_frame_len: u32 = 64 * 1024 * 1024;


/// Accumulates a message PAYLOAD, then frames it with a type byte and length header.
///
/// The builder writes payload fields into a growable buffer using big-endian
/// (network) byte order, and [`finish`] prepends the 1-byte type tag and the
/// self-counting u32 length to produce the complete on-wire frame. All the
/// `encode*` free functions in this module are thin wrappers over this type.
///
/// After [`finish`] the internal buffer is cleared but its capacity is
/// retained, so one `Builder` can be reused to emit several frames; call
/// [`deinit`] to release that capacity.
pub const Builder = struct {
    /// Growable payload accumulator; holds the bytes AFTER the frame header.
    buf: std.ArrayList(u8),
    /// Allocator backing [`buf`] and the frame returned by [`finish`].
    allocator: Allocator,

    /// Creates an empty builder that will allocate through `allocator`.
    pub fn init(allocator: Allocator) Builder {
        return .{ .buf = .empty, .allocator = allocator };
    }
    /// Releases the payload buffer's memory. Does not free frames already handed out by [`finish`].
    pub fn deinit(self: *Builder) void {
        self.buf.deinit(self.allocator);
    }

    /// Appends a single raw byte to the payload.
    pub fn putU8(self: *Builder, v: u8) !void {
        try self.buf.append(self.allocator, v);
    }
    /// Appends a u16 in big-endian order.
    pub fn putU16(self: *Builder, v: u16) !void {
        var tmp: [2]u8 = undefined;
        mem.writeInt(u16, &tmp, v, .big);
        try self.buf.appendSlice(self.allocator, &tmp);
    }
    /// Appends a u32 in big-endian order.
    pub fn putU32(self: *Builder, v: u32) !void {
        var tmp: [4]u8 = undefined;
        mem.writeInt(u32, &tmp, v, .big);
        try self.buf.appendSlice(self.allocator, &tmp);
    }
    /// Appends a signed i16 in big-endian order via its two's-complement bit pattern.
    pub fn putI16(self: *Builder, v: i16) !void {
        try self.putU16(@bitCast(v));
    }
    /// Appends a signed i32 in big-endian order via its two's-complement bit pattern.
    pub fn putI32(self: *Builder, v: i32) !void {
        try self.putU32(@bitCast(v));
    }
    /// Appends raw bytes verbatim, with no length prefix.
    pub fn putBytes(self: *Builder, b: []const u8) !void {
        try self.buf.appendSlice(self.allocator, b);
    }
    /// Appends a u16 length prefix followed by the string bytes.
    ///
    /// Used for identifiers and short strings (names, tags, parameter keys).
    /// The length is truncated with `@intCast`, so `s` must be at most 65535
    /// bytes; a longer string is a programming error, not a runtime-handled case.
    pub fn putStr16(self: *Builder, s: []const u8) !void {
        try self.putU16(@intCast(s.len));
        try self.putBytes(s);
    }
    /// Appends a nullable field value: an i32 length then the bytes, or a length of -1 for NULL.
    ///
    /// This is the encoding half of the NULL-versus-empty distinction: a present
    /// value writes its (non-negative) length even when zero, while `null`
    /// writes -1. [`Cursor.value`] decodes the same convention.
    pub fn putValue(self: *Builder, v: ?[]const u8) !void {
        if (v) |bytes| {
            try self.putI32(@intCast(bytes.len));
            try self.putBytes(bytes);
        } else {
            try self.putI32(-1);
        }
    }

    /// Frames the accumulated payload with `type_byte` and a length header, returning the owned frame.
    ///
    /// Allocates a fresh buffer of `1 + 4 + payload_len` bytes: the type tag,
    /// the big-endian self-counting length (`4 + payload_len`, so it includes
    /// its own four bytes but not the tag), then the payload. The caller owns
    /// the returned slice and must free it. The builder's payload buffer is
    /// cleared (capacity retained) so the same `Builder` can emit the next frame.
    pub fn finish(self: *Builder, type_byte: u8) ![]u8 {
        const payload_len: u32 = @intCast(self.buf.items.len);
        const total = 1 + 4 + payload_len;
        const out = try self.allocator.alloc(u8, total);
        out[0] = type_byte;
        mem.writeInt(u32, out[1..5], 4 + payload_len, .big); // len includes itself
        @memcpy(out[5..], self.buf.items);
        self.buf.clearRetainingCapacity();
        return out;
    }
};


/// Builds an [`Backend.auth`] frame signalling authentication succeeded ([`AuthMethod.ok`]).
///
/// Returns an owned frame the caller must free.
pub fn encodeAuthOk(a: Allocator) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU32(@intFromEnum(AuthMethod.ok));
    return b.finish(@intFromEnum(Backend.auth));
}

/// Builds an [`Backend.auth`] frame requesting a cleartext password from the client.
///
/// The client replies with a [`Frontend.auth_response`] carrying the password;
/// decode it with [`decodeAuthResponse`]. Returns an owned frame the caller must free.
pub fn encodeAuthCleartext(a: Allocator) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU32(@intFromEnum(AuthMethod.cleartext_password));
    return b.finish(@intFromEnum(Backend.auth));
}

/// Extracts the password/response string from a [`Frontend.auth_response`] payload.
///
/// The returned slice borrows `payload` and is only valid while it lives.
/// Errors `Truncated` if the length-prefixed string runs past the buffer.
pub fn decodeAuthResponse(payload: []const u8) DecodeError![]const u8 {
    var c = Cursor{ .data = payload };
    return c.str16();
}

/// Builds a [`Backend.ready`] frame reporting the current [`TxnStatus`].
///
/// Sent to close each response cycle so the client knows it may send the next
/// command. Returns an owned frame the caller must free.
pub fn encodeReady(a: Allocator, status: TxnStatus) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU8(@intFromEnum(status));
    return b.finish(@intFromEnum(Backend.ready));
}

/// Builds a [`Backend.parameter_status`] frame advertising one server runtime parameter.
///
/// Sent during connection setup for keys the client expects (e.g.
/// `server_version`, `client_encoding`). Returns an owned frame the caller must free.
pub fn encodeParameterStatus(a: Allocator, key: []const u8, value: []const u8) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putStr16(key);
    try b.putStr16(value);
    return b.finish(@intFromEnum(Backend.parameter_status));
}

/// Builds a [`Backend.backend_key`] frame carrying the session's process id and cancel secret.
///
/// The client stores both and presents them on a separate connection to cancel
/// an in-flight query. Returns an owned frame the caller must free.
pub fn encodeBackendKey(a: Allocator, pid: u32, secret: u32) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU32(pid);
    try b.putU32(secret);
    return b.finish(@intFromEnum(Backend.backend_key));
}

/// Builds a [`Backend.row_description`] frame describing the columns of a result set.
///
/// Writes a u16 column count then each [`FieldDesc`] in order (name, table id,
/// column number, type OID, type size, type modifier, format). Emitted once
/// before the [`Backend.data_row`] stream. Returns an owned frame the caller must free.
pub fn encodeRowDescription(a: Allocator, fields: []const FieldDesc) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU16(@intCast(fields.len));
    for (fields) |f| {
        try b.putStr16(f.name);
        try b.putU32(f.table_id);
        try b.putU16(f.col_no);
        try b.putU32(f.type_oid);
        try b.putI16(f.type_size);
        try b.putI32(f.type_mod);
        try b.putU16(@intFromEnum(f.format));
    }
    return b.finish(@intFromEnum(Backend.row_description));
}

/// Builds a [`Backend.data_row`] frame holding one result row's column values.
///
/// Writes a u16 value count then each value via [`Builder.putValue`], so a
/// `null` element encodes as a -1 length (SQL NULL) distinct from an empty
/// string. Column order must match the preceding [`encodeRowDescription`].
/// Returns an owned frame the caller must free.
pub fn encodeDataRow(a: Allocator, values: []const ?[]const u8) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU16(@intCast(values.len));
    for (values) |v| try b.putValue(v);
    return b.finish(@intFromEnum(Backend.data_row));
}

/// Builds a [`Backend.command_complete`] frame carrying the completion tag (e.g. `SELECT 3`).
///
/// Returns an owned frame the caller must free.
pub fn encodeCommandComplete(a: Allocator, tag: []const u8) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putStr16(tag);
    return b.finish(@intFromEnum(Backend.command_complete));
}

/// Builds an empty-payload backend frame of tag `t`.
///
/// Covers the acknowledgement messages that carry no data of their own
/// ([`Backend.parse_complete`], [`Backend.bind_complete`],
/// [`Backend.close_complete`], [`Backend.no_data`], [`Backend.empty_query`]).
/// Returns an owned frame the caller must free.
pub fn encodeSimple(a: Allocator, t: Backend) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    return b.finish(@intFromEnum(t));
}

/// Builds a [`Backend.error_response`] frame from a severity, SQLSTATE code, and message.
///
/// This is NovaDB's compact three-field error layout (severity, code, message
/// each as a str16), not PostgreSQL's tagged field-list form. Returns an owned
/// frame the caller must free.
pub fn encodeError(a: Allocator, severity: []const u8, code: []const u8, message: []const u8) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putStr16(severity);
    try b.putStr16(code);
    try b.putStr16(message);
    return b.finish(@intFromEnum(Backend.error_response));
}


/// Errors the decoders and [`parseFrame`] can return.
///
/// [`Cursor`] reads yield `Truncated` when a field runs past the buffer;
/// [`parseFrame`] yields `ShortBuffer` for a sub-minimal length header and
/// `FrameTooLarge` when the declared length exceeds [`max_frame_len`].
/// `BadString` is reserved for malformed string content.
pub const DecodeError = error{ ShortBuffer, FrameTooLarge, BadString, Truncated };

/// A forward-only reader over a message payload, decoding big-endian fields in place.
///
/// Every read is bounds-checked against [`remaining`] and advances [`pos`];
/// running short returns `error.Truncated` rather than reading out of bounds.
/// Slice-returning reads ([`str16`], [`value`]) BORROW the underlying `data`,
/// so their results are only valid while the payload buffer lives. The cursor
/// never allocates and never mutates the buffer.
pub const Cursor = struct {
    /// The payload bytes being decoded. Not owned; must outlive any slice returned.
    data: []const u8,
    /// Byte offset of the next unread field; advanced by each read.
    pos: usize = 0,

    /// Number of bytes left between [`pos`] and the end of [`data`].
    pub fn remaining(self: *const Cursor) usize {
        return self.data.len - self.pos;
    }
    /// Reads one byte, or `Truncated` if the buffer is exhausted.
    pub fn u8v(self: *Cursor) DecodeError!u8 {
        if (self.remaining() < 1) return error.Truncated;
        const v = self.data[self.pos];
        self.pos += 1;
        return v;
    }
    /// Reads a big-endian u16, or `Truncated` if fewer than 2 bytes remain.
    pub fn u16v(self: *Cursor) DecodeError!u16 {
        if (self.remaining() < 2) return error.Truncated;
        const v = mem.readInt(u16, self.data[self.pos..][0..2], .big);
        self.pos += 2;
        return v;
    }
    /// Reads a big-endian u32, or `Truncated` if fewer than 4 bytes remain.
    pub fn u32v(self: *Cursor) DecodeError!u32 {
        if (self.remaining() < 4) return error.Truncated;
        const v = mem.readInt(u32, self.data[self.pos..][0..4], .big);
        self.pos += 4;
        return v;
    }
    /// Reads a big-endian i32, reinterpreting the u32 bit pattern; used for signed lengths.
    pub fn i32v(self: *Cursor) DecodeError!i32 {
        return @bitCast(try self.u32v());
    }
    /// Reads a big-endian i16, reinterpreting the u16 bit pattern.
    pub fn i16v(self: *Cursor) DecodeError!i16 {
        return @bitCast(try self.u16v());
    }
    /// Reads a u16-length-prefixed string, returning a borrowed slice of the payload.
    ///
    /// Errors `Truncated` if the declared length exceeds the bytes remaining.
    pub fn str16(self: *Cursor) DecodeError![]const u8 {
        const n = try self.u16v();
        if (self.remaining() < n) return error.Truncated;
        const s = self.data[self.pos .. self.pos + n];
        self.pos += n;
        return s;
    }
    /// Reads a nullable i32-length-prefixed value, returning `null` for a -1 length (SQL NULL).
    ///
    /// The decode counterpart of [`Builder.putValue`]: a non-negative length
    /// yields a borrowed slice (empty when the length is 0, distinct from NULL),
    /// while -1 yields `null`. Errors `Truncated` if a present value's length
    /// runs past the buffer.
    pub fn value(self: *Cursor) DecodeError!?[]const u8 {
        const n = try self.i32v();
        if (n < 0) return null;
        const un: usize = @intCast(n);
        if (self.remaining() < un) return error.Truncated;
        const s = self.data[self.pos .. self.pos + un];
        self.pos += un;
        return s;
    }
};

/// A parsed frame: its type tag byte and a borrowed slice of its payload.
pub const Frame = struct {
    /// The message type tag (a [`Frontend`] or [`Backend`] byte).
    type: u8,
    /// The payload bytes, borrowed from the buffer passed to [`parseFrame`].
    payload: []const u8,
};

/// Attempts to peel one complete frame off the front of a streaming read buffer.
///
/// Returns `null` when `buf` does not yet contain a whole frame (fewer than the
/// 5-byte header, or the declared body has not fully arrived), which signals the
/// caller to read more bytes and retry rather than treating it as an error. On
/// success it returns the [`Frame`] (payload borrowing `buf`) and `consumed`,
/// the total byte count to advance past, so the caller can process further
/// frames already in the buffer.
///
/// Errors `ShortBuffer` if the length header is below the 4-byte minimum (it
/// counts itself), and `FrameTooLarge` if it exceeds [`max_frame_len`]; both are
/// unrecoverable, unlike the `null` "need more data" case.
pub fn parseFrame(buf: []const u8) DecodeError!?struct { frame: Frame, consumed: usize } {
    if (buf.len < 5) return null; // need at least type + len
    const len = mem.readInt(u32, buf[1..5], .big);
    if (len < 4) return error.ShortBuffer;
    if (len > max_frame_len) return error.FrameTooLarge;
    const total = 1 + len; // type byte + (len includes the 4 len bytes)
    if (buf.len < total) return null; // incomplete
    const payload = buf[5..total];
    return .{ .frame = .{ .type = buf[0], .payload = payload }, .consumed = total };
}


/// The decoded startup packet: negotiated protocol version and connection parameters.
///
/// All string fields borrow the payload buffer. Produced by [`decodeStartup`].
pub const Startup = struct {
    /// Requested protocol major version.
    proto_major: u16,
    /// Requested protocol minor version.
    proto_minor: u16,
    /// Login user name.
    user: []const u8,
    /// Target database name.
    database: []const u8,
    /// Client application name (for logging/identification).
    application: []const u8,
};

/// Decodes a startup packet payload into a [`Startup`].
///
/// Reads the two version u16s then three str16 fields in order. The returned
/// strings borrow `payload`. Errors `Truncated` on a short buffer.
pub fn decodeStartup(payload: []const u8) DecodeError!Startup {
    var c = Cursor{ .data = payload };
    return .{
        .proto_major = try c.u16v(),
        .proto_minor = try c.u16v(),
        .user = try c.str16(),
        .database = try c.str16(),
        .application = try c.str16(),
    };
}

/// Extracts the SQL string from a [`Frontend.query`] (simple-query) payload.
///
/// The returned slice borrows `payload`. Errors `Truncated` on a short buffer.
pub fn decodeQuery(payload: []const u8) DecodeError![]const u8 {
    var c = Cursor{ .data = payload };
    return c.str16();
}

/// A parse-message shape carrying the raw (undecoded) parameter-OID bytes.
///
/// Unlike [`ParsedParse`], this keeps `param_oids` as the borrowed raw byte
/// slice plus an explicit `param_count`, i.e. an allocation-free view; callers
/// that need the OIDs as a `[]Oid` use [`decodeParse`] instead.
pub const ParseMsg = struct {
    /// Prepared-statement name (empty string for the unnamed statement).
    stmt_name: []const u8,
    /// The SQL text to prepare.
    sql: []const u8,
    /// Raw, still-encoded parameter type OID bytes.
    param_oids: []const u8, // raw; caller iterates u32s (count-prefixed below via a Cursor)
    /// Number of parameter OIDs encoded in [`param_oids`].
    param_count: u16,
};

/// A decoded [`Frontend.execute`] message: which portal to run and its row cap.
pub const ExecuteMsg = struct {
    /// Name of the bound portal to execute (empty for the unnamed portal).
    portal: []const u8,
    /// Maximum rows to return; 0 means unlimited.
    max_rows: u32,
};

/// Decodes a [`Frontend.execute`] payload into an [`ExecuteMsg`].
///
/// The `portal` slice borrows `payload`. Errors `Truncated` on a short buffer.
pub fn decodeExecute(payload: []const u8) DecodeError!ExecuteMsg {
    var c = Cursor{ .data = payload };
    const portal = try c.str16();
    const max_rows = try c.u32v();
    return .{ .portal = portal, .max_rows = max_rows };
}

// -- Document-model wire messages (slice D-5) ---------------------------------

/// Sub-operation carried in a [`Frontend.doc_op`] request.
pub const DocOp = enum(u8) {
    /// Insert one BSON document (the payload) into the collection.
    insert = 1,
    /// Return every document matching the payload BSON filter (`{}` = all).
    find = 2,
    /// Return the first document matching the payload BSON filter, or none.
    find_one = 3,
    /// Create the named collection if it does not already exist (payload ignored).
    create_collection = 4,
    /// Open a document transaction on this connection (collection/payload ignored).
    begin = 5,
    /// Commit the open document transaction, making its writes visible atomically.
    commit = 6,
    /// Roll back the open document transaction, discarding its writes.
    rollback = 7,
    /// Delete every document matching the payload BSON filter.
    delete = 8,
    /// Update every document matching a filter with an update spec. The payload is
    /// a `u32` filter length, the filter BSON, then the update-spec BSON (see
    /// [`splitUpdatePayload`]).
    update = 9,
    /// Create a secondary index on the collection; the payload is the field path
    /// (a plain UTF-8 string, e.g. "price" or "address.city").
    create_index = 10,
    /// Find with result shaping. The payload is a BSON "query envelope" document
    /// with optional fields `filter`, `projection`, `sort` (all sub-documents),
    /// and `skip`/`limit` (integers). Returns a document result set.
    find_query = 11,
    /// Count the documents matching the payload BSON filter. Returns a one-document
    /// result set whose single document is `{ n: <count> }`.
    count = 12,
    /// Insert many documents in one request. The payload is a BSON document with
    /// a `docs` array field; each element is inserted (server-generating an `_id`
    /// when absent). Returns an `INSERT-DOC <n>` command-complete.
    insert_many = 13,
    /// One page of a cursor scan. The payload is a BSON envelope with an optional
    /// `filter` sub-document, an optional `after` (a 12-byte binary `_id` to
    /// resume past), and an optional `batch` integer. Returns a cursor result
    /// (see [`encodeDocCursorResult`]): the page of documents plus the next
    /// resume id, or none when the collection is exhausted.
    find_cursor = 14,
    /// Update matching documents, or insert one when none match (upsert). The
    /// payload has the same `u32 filter_len` + filter + update-spec layout as
    /// `update`. Returns an `UPSERT-DOC <n>` command-complete.
    upsert = 15,
    /// Apply a mixed batch of writes in one request. The payload is a BSON
    /// document with an `ops` array; each element is a document describing one
    /// operation: `{op:"insert", doc:{...}}`, `{op:"update", filter, update}`,
    /// `{op:"upsert", filter, update}`, or `{op:"delete", filter}`. Returns a
    /// `BULK-DOC <n>` command-complete where n is the number of affected docs.
    bulk_write = 16,
    /// Run an aggregation pipeline over the collection. The payload is a BSON
    /// document `{stages: [ ... ]}` where each stage is a one-key document
    /// (`$match`/`$group`/`$sort`/`$skip`/`$limit`/`$project`/`$count`). Returns
    /// a document result set of the pipeline output.
    aggregate = 17,
    /// Insert one document, FAILING if a document with the same `_id` already
    /// exists (insert-if-absent). The payload is the BSON document. Returns an
    /// `INSERT-DOC 1 <hex>` command-complete, or a `23505` duplicate-key error.
    insert_unique = 18,
    /// Read the document-path observability counters (gap G17). The payload is
    /// ignored; returns a one-document result set whose single document is
    /// `{finds, inserts, deletes, updates, docsScanned, docsMatched}`.
    doc_stats = 19,
    /// Create a UNIQUE secondary index on the collection; the payload is the field
    /// path. A write that would give a second document the same value then fails
    /// with a `23505` duplicate-key error. Building over an existing duplicate
    /// value fails the same way.
    create_unique_index = 20,
    /// Create a COMPOUND secondary index over several fields. The payload is the
    /// comma-separated field paths in index order (e.g. "EmployeeID,TotalDue").
    /// A find whose filter pins the leading fields by equality can then be
    /// answered - and ordered by the trailing field - straight from this index.
    create_compound_index = 21,
    /// Non-exhaustive: an unknown op byte still decodes (no crash on untrusted
    /// input); the handler rejects it with an error response.
    _,
};

/// A decoded [`Frontend.doc_op`] request: the operation, the target collection,
/// and a BSON payload (a document for `insert`; a filter for `find`/`find_one`;
/// empty for `create_collection`). The `collection` and `payload` slices borrow
/// the frame payload.
pub const DocRequest = struct {
    op: DocOp,
    collection: []const u8,
    payload: []const u8,
};

/// Encodes a [`DocRequest`] into a full [`Frontend.doc_op`] frame (owned). Layout
/// after the frame header: `u8 op`, `u16-prefixed collection`, then the BSON
/// payload as the remainder of the frame.
pub fn encodeDocRequest(a: Allocator, req: DocRequest) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU8(@intFromEnum(req.op));
    try b.putStr16(req.collection);
    try b.putBytes(req.payload);
    return b.finish(@intFromEnum(Frontend.doc_op));
}

/// Decodes a [`Frontend.doc_op`] payload. The `collection`/`payload` slices
/// borrow `payload`. Errors `Truncated` on a short buffer.
pub fn decodeDocRequest(payload: []const u8) DecodeError!DocRequest {
    var c = Cursor{ .data = payload };
    const op: DocOp = @enumFromInt(try c.u8v());
    const collection = try c.str16();
    return .{ .op = op, .collection = collection, .payload = c.data[c.pos..] };
}

/// Split an `update` op payload into its filter and update-spec BSON slices.
/// Layout: `u32 filter_len` (big-endian) then the filter, then the spec (the
/// remainder). Both slices borrow `payload`. Errors `Truncated` on a short buffer.
pub fn splitUpdatePayload(payload: []const u8) DecodeError!struct { filter: []const u8, spec: []const u8 } {
    if (payload.len < 4) return error.Truncated;
    const flen = mem.readInt(u32, payload[0..4], .big);
    if (4 + @as(usize, flen) > payload.len) return error.Truncated;
    return .{ .filter = payload[4 .. 4 + flen], .spec = payload[4 + flen ..] };
}

/// Build an `update` op payload (`u32 filter_len` + filter + spec). Owned by the
/// caller; convenient for tests and the driver.
pub fn encodeUpdatePayload(a: Allocator, filter: []const u8, spec: []const u8) ![]u8 {
    const out = try a.alloc(u8, 4 + filter.len + spec.len);
    mem.writeInt(u32, out[0..4], @intCast(filter.len), .big);
    @memcpy(out[4 .. 4 + filter.len], filter);
    @memcpy(out[4 + filter.len ..], spec);
    return out;
}

/// Result-set tag byte for a document response (server -> client). Lowercase so
/// it does not collide with any exhaustive [`Backend`] message byte; only the
/// NovaDB driver reads it.
pub const doc_result_tag: u8 = 'j';

/// Encodes a document result set: `u32 count` then, per document, `i32 length`
/// and its BSON bytes. Returns an owned frame tagged [`doc_result_tag`].
pub fn encodeDocResult(a: Allocator, docs: []const []const u8) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU32(@intCast(docs.len));
    for (docs) |d| {
        try b.putI32(@intCast(d.len));
        try b.putBytes(d);
    }
    return b.finish(doc_result_tag);
}

/// Decodes a document result set into borrowed BSON slices. The outer slice is
/// owned by the caller (free with `allocator.free`); each inner slice borrows
/// `payload`. Crash-safe on garbage: a declared count larger than the buffer, or
/// a length running past the end, errors rather than over-reading.
pub fn decodeDocResult(payload: []const u8, a: Allocator) (DecodeError || Allocator.Error)![][]const u8 {
    var c = Cursor{ .data = payload };
    const count = try c.u32v();
    // A document needs at least a 4-byte length, so count can never exceed the
    // remaining bytes; this bounds the allocation on hostile input.
    if (count > c.remaining()) return error.Truncated;
    const out = try a.alloc([]const u8, count);
    errdefer a.free(out);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const n = try c.i32v();
        if (n < 0) return error.BadString;
        const un: usize = @intCast(n);
        if (c.remaining() < un) return error.Truncated;
        out[i] = c.data[c.pos .. c.pos + un];
        c.pos += un;
    }
    return out;
}

/// A decoded cursor result: a page of borrowed BSON document slices plus the
/// optional resume id. The `docs` outer slice is owned by the caller; each inner
/// slice borrows the payload.
pub const DocCursorResult = struct {
    docs: [][]const u8,
    next: ?[12]u8,
};

/// Encode a cursor page: `u8 has_next`, then `[12]u8 next` when present, then the
/// standard document-result body (`u32 count` then per-doc `i32 len` + bytes).
/// Tagged [`doc_result_tag`]; the client picks the decoder from the op it sent.
pub fn encodeDocCursorResult(a: Allocator, docs: []const []const u8, next: ?[12]u8) ![]u8 {
    var b = Builder.init(a);
    defer b.deinit();
    if (next) |id| {
        try b.putU8(1);
        try b.putBytes(&id);
    } else {
        try b.putU8(0);
    }
    try b.putU32(@intCast(docs.len));
    for (docs) |d| {
        try b.putI32(@intCast(d.len));
        try b.putBytes(d);
    }
    return b.finish(doc_result_tag);
}

/// Decode a cursor page produced by [`encodeDocCursorResult`]. The `docs` outer
/// slice is owned by the caller (free with `allocator.free`); inner slices borrow
/// `payload`. Bounds-checked against hostile input.
pub fn decodeDocCursorResult(payload: []const u8, a: Allocator) (DecodeError || Allocator.Error)!DocCursorResult {
    var c = Cursor{ .data = payload };
    const has_next = try c.u8v();
    var next: ?[12]u8 = null;
    if (has_next != 0) {
        if (c.remaining() < 12) return error.Truncated;
        var id: [12]u8 = undefined;
        @memcpy(&id, c.data[c.pos .. c.pos + 12]);
        c.pos += 12;
        next = id;
    }
    const count = try c.u32v();
    if (count > c.remaining()) return error.Truncated;
    const out = try a.alloc([]const u8, count);
    errdefer a.free(out);
    var i: usize = 0;
    while (i < count) : (i += 1) {
        const n = try c.i32v();
        if (n < 0) return error.BadString;
        const un: usize = @intCast(n);
        if (c.remaining() < un) return error.Truncated;
        out[i] = c.data[c.pos .. c.pos + un];
        c.pos += un;
    }
    return .{ .docs = out, .next = next };
}

/// A fully decoded [`Frontend.parse`] message with parameter OIDs as an owned `[]Oid`.
///
/// The string fields borrow the payload buffer, but `param_oids` is heap
/// allocated by [`decodeParse`]; call [`deinit`] to free it.
pub const ParsedParse = struct {
    /// Prepared-statement name (empty string for the unnamed statement).
    stmt_name: []const u8,
    /// The SQL text to prepare.
    sql: []const u8,
    /// Declared parameter type OIDs; owned, freed by [`deinit`].
    param_oids: []Oid,
    /// Frees the allocated [`param_oids`] array using the same allocator `decodeParse` used.
    pub fn deinit(self: *ParsedParse, a: Allocator) void {
        a.free(self.param_oids);
    }
};

/// Decodes a [`Frontend.parse`] payload, allocating the parameter-OID array.
///
/// Reads the statement name, SQL, a u16 OID count, then that many u32 OIDs into
/// a freshly allocated `[]Oid`. On any truncation after the allocation the
/// `errdefer` frees it, so the caller never leaks on error. On success the
/// caller owns the result and must call [`ParsedParse.deinit`]. The string
/// fields still borrow `payload`.
pub fn decodeParse(payload: []const u8, a: Allocator) (DecodeError || Allocator.Error)!ParsedParse {
    var c = Cursor{ .data = payload };
    const stmt_name = try c.str16();
    const sql = try c.str16();
    const n = try c.u16v();
    const oids = try a.alloc(Oid, n);
    errdefer a.free(oids);
    for (oids) |*o| o.* = try c.u32v();
    return .{ .stmt_name = stmt_name, .sql = sql, .param_oids = oids };
}

/// A fully decoded [`Frontend.bind`] message: parameter values and format codes for a portal.
///
/// The three slices ([`param_formats`], [`params`], [`result_formats`]) are all
/// heap allocated by [`decodeBind`] and freed by [`deinit`]; the string fields
/// and each parameter's bytes borrow the payload buffer.
pub const ParsedBind = struct {
    /// Destination portal name (empty for the unnamed portal).
    portal: []const u8,
    /// Source prepared-statement name (empty for the unnamed statement).
    stmt: []const u8,
    /// Per-parameter format codes; see [`paramFormat`] for the 0/1/N convention.
    param_formats: []Format,
    /// Parameter values in order; a `null` element is SQL NULL. Bytes borrow the payload.
    params: []?[]const u8,
    /// Requested format for each result column.
    result_formats: []Format,
    /// Frees the three allocated arrays using the allocator `decodeBind` used.
    pub fn deinit(self: *ParsedBind, a: Allocator) void {
        a.free(self.param_formats);
        a.free(self.params);
        a.free(self.result_formats);
    }
    /// Resolves the wire format of parameter `i`, honouring the protocol's 0/1/N shorthand.
    ///
    /// The bind message may send zero format codes (all parameters are `text`),
    /// exactly one (it applies to EVERY parameter), or one per parameter. This
    /// collapses those three cases so callers can index by parameter without
    /// re-checking the count.
    pub fn paramFormat(self: *const ParsedBind, i: usize) Format {
        if (self.param_formats.len == 0) return .text;
        if (self.param_formats.len == 1) return self.param_formats[0];
        return self.param_formats[i];
    }
};

/// Decodes a [`Frontend.bind`] payload, allocating the format and parameter arrays.
///
/// Reads, in order: portal name, statement name, a counted list of parameter
/// format codes, a counted list of parameter values (each nullable via
/// [`Cursor.value`]), and a counted list of result format codes. Each of the
/// three arrays is allocated separately, and an `errdefer` guards every one so a
/// truncation partway through frees what was already allocated. On success the
/// caller owns the result and must call [`ParsedBind.deinit`].
pub fn decodeBind(payload: []const u8, a: Allocator) (DecodeError || Allocator.Error)!ParsedBind {
    var c = Cursor{ .data = payload };
    const portal = try c.str16();
    const stmt = try c.str16();

    const pf_count = try c.u16v();
    const pfmts = try a.alloc(Format, pf_count);
    errdefer a.free(pfmts);
    for (pfmts) |*f| f.* = @enumFromInt(try c.u16v());

    const p_count = try c.u16v();
    const params = try a.alloc(?[]const u8, p_count);
    errdefer a.free(params);
    for (params) |*p| p.* = try c.value();

    const rf_count = try c.u16v();
    const rfmts = try a.alloc(Format, rf_count);
    errdefer a.free(rfmts);
    for (rfmts) |*f| f.* = @enumFromInt(try c.u16v());

    return .{ .portal = portal, .stmt = stmt, .param_formats = pfmts, .params = params, .result_formats = rfmts };
}

/// Test-only alias for the standard testing helpers used by the round-trip tests below.
const testing = std.testing;

test "framing round-trip: ready" {
    const a = testing.allocator;
    const msg = try encodeReady(a, .idle);
    defer a.free(msg);
    const parsed = (try parseFrame(msg)).?;
    try testing.expectEqual(@as(u8, @intFromEnum(Backend.ready)), parsed.frame.type);
    try testing.expectEqual(msg.len, parsed.consumed);
    var c = Cursor{ .data = parsed.frame.payload };
    try testing.expectEqual(@as(u8, @intFromEnum(TxnStatus.idle)), try c.u8v());
}

test "data row with NULL round-trips (-1 length, not a sentinel)" {
    const a = testing.allocator;
    const vals = [_]?[]const u8{ "Nova", null, "42" };
    const msg = try encodeDataRow(a, &vals);
    defer a.free(msg);
    const parsed = (try parseFrame(msg)).?;
    try testing.expectEqual(@as(u8, @intFromEnum(Backend.data_row)), parsed.frame.type);
    var c = Cursor{ .data = parsed.frame.payload };
    try testing.expectEqual(@as(u16, 3), try c.u16v());
    try testing.expectEqualStrings("Nova", (try c.value()).?);
    try testing.expect((try c.value()) == null); // real NULL
    try testing.expectEqualStrings("42", (try c.value()).?);
}

test "row description round-trip" {
    const a = testing.allocator;
    const fields = [_]FieldDesc{
        .{ .name = "id", .type_oid = oid.int8, .type_size = 8 },
        .{ .name = "name", .type_oid = oid.text },
    };
    const msg = try encodeRowDescription(a, &fields);
    defer a.free(msg);
    const parsed = (try parseFrame(msg)).?;
    var c = Cursor{ .data = parsed.frame.payload };
    try testing.expectEqual(@as(u16, 2), try c.u16v());
    try testing.expectEqualStrings("id", try c.str16());
    _ = try c.u32v(); // table_id
    _ = try c.u16v(); // col_no
    try testing.expectEqual(oid.int8, try c.u32v());
}

test "decode startup + query + execute" {
    const a = testing.allocator;
    var b = Builder.init(a);
    defer b.deinit();
    try b.putU16(1); // major
    try b.putU16(0); // minor
    try b.putStr16("admin");
    try b.putStr16("nova");
    try b.putStr16("workbench");
    const framed = try b.finish(@intFromEnum(Frontend.startup));
    defer a.free(framed);
    const parsed = (try parseFrame(framed)).?;
    try testing.expectEqual(@as(u8, @intFromEnum(Frontend.startup)), parsed.frame.type);
    const s = try decodeStartup(parsed.frame.payload);
    try testing.expectEqualStrings("admin", s.user);
    try testing.expectEqualStrings("nova", s.database);

    var b2 = Builder.init(a);
    defer b2.deinit();
    try b2.putStr16("portal1");
    try b2.putU32(100);
    const ef = try b2.finish(@intFromEnum(Frontend.execute));
    defer a.free(ef);
    const ep = (try parseFrame(ef)).?;
    const em = try decodeExecute(ep.frame.payload);
    try testing.expectEqualStrings("portal1", em.portal);
    try testing.expectEqual(@as(u32, 100), em.max_rows);
}

test "parse + bind round-trip (parameter binding)" {
    const a = testing.allocator;
    var b = Builder.init(a);
    defer b.deinit();
    try b.putStr16("stmt1");
    try b.putStr16("SELECT * FROM t WHERE id = $1");
    try b.putU16(1);
    try b.putU32(oid.int8);
    const pf = try b.finish(@intFromEnum(Frontend.parse));
    defer a.free(pf);
    var parse = try decodeParse((try parseFrame(pf)).?.frame.payload, a);
    defer parse.deinit(a);
    try testing.expectEqualStrings("stmt1", parse.stmt_name);
    try testing.expectEqual(@as(usize, 1), parse.param_oids.len);
    try testing.expectEqual(oid.int8, parse.param_oids[0]);

    var b2 = Builder.init(a);
    defer b2.deinit();
    try b2.putStr16("p1");
    try b2.putStr16("stmt1");
    try b2.putU16(1); // 1 param format = applies to all
    try b2.putU16(@intFromEnum(Format.text));
    try b2.putU16(2); // 2 params
    try b2.putValue("42");
    try b2.putValue(null);
    try b2.putU16(0); // 0 result formats
    const bf = try b2.finish(@intFromEnum(Frontend.bind));
    defer a.free(bf);
    var bind = try decodeBind((try parseFrame(bf)).?.frame.payload, a);
    defer bind.deinit(a);
    try testing.expectEqualStrings("p1", bind.portal);
    try testing.expectEqualStrings("stmt1", bind.stmt);
    try testing.expectEqual(@as(usize, 2), bind.params.len);
    try testing.expectEqualStrings("42", bind.params[0].?);
    try testing.expect(bind.params[1] == null);
    try testing.expectEqual(Format.text, bind.paramFormat(0));
    try testing.expectEqual(Format.text, bind.paramFormat(1)); // 1-format rule applies to all
}

test "incomplete frame returns null (need more data)" {
    const partial = [_]u8{ 'Z', 0, 0, 0, 5 }; // header says 5 bytes but payload missing
    try testing.expect((try parseFrame(&partial)) == null);
}

test "oversized frame rejected" {
    var hdr = [_]u8{ 'Q', 0, 0, 0, 0 };
    mem.writeInt(u32, hdr[1..5], max_frame_len + 10, .big);
    try testing.expectError(error.FrameTooLarge, parseFrame(&hdr));
}
