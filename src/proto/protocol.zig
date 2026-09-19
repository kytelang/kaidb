//! Binary wire-protocol primitives shared by the kaidb server and its driver.
//!
//! This module defines the on-the-wire encoding that a client (Kyte's
//! `packages/nova-kaidb` driver) and the `btree` server speak to each other.
//! It is deliberately a low-level, POD-oriented layer: the framing types are
//! `extern struct`s laid out with a fixed C ABI so both ends can memcpy them
//! directly to and from a socket buffer, and the read/write helpers do nothing
//! more than bounds-checked cursor arithmetic over a byte slice. The higher
//! layers (`wire.zig`, `session.zig`, `command.zig`) build request/response
//! semantics on top of the message framing declared here.
//!
//! ## Message framing
//!
//! Every message on the wire is a fixed-size [`MessageHeader`] followed by
//! `payload_len` bytes of body. The header carries a magic tag ("NOVA" as a
//! little-endian [`MessageHeader.MAGIC_VALUE`]) so a receiver can cheaply reject
//! garbage or a desynchronised stream, a [`MessageType`] discriminant naming the
//! body's shape, a `stream_id` so several logical requests can be multiplexed
//! over one connection, and the payload length so the framing layer knows how
//! many bytes to read before handing the body up. Because the header is an
//! `extern struct`, its layout is stable across compilations and both peers rely
//! on that: fields are read back with a raw `@memcpy`, not a field-by-field
//! decode.
//!
//! ## Endianness and portability
//!
//! Struct-based (`readStruct`/`writeStruct`) transfers copy the host byte order
//! verbatim, so the integer fields inside the `extern struct`s are host-endian
//! on the wire. The one place this file commits to an explicit encoding is the
//! row layout in [`serializeQueryRow`], whose offset table is written and read
//! as little-endian via [`std.mem.writeInt`]/`readInt`. Keep that in mind when
//! extending the protocol: mixing the two conventions is the classic wire bug.
//!
//! ## Version negotiation
//!
//! [`PROTOCOL_VERSION`] and [`MIN_SUPPORTED_VERSION`] bound the range this build
//! understands; [`isCompatible`] and [`negotiate`] implement the handshake that
//! picks the highest mutually-supported version (or refuses the connection).
//!
//! ## Row encoding
//!
//! A query result row is encoded with a split fixed/heap representation
//! ([`RowWriter`], [`serializeQueryRow`]): a fixed-width array of 4-byte offsets
//! (one per cell) points into a heap section that holds each cell's length-
//! prefixed bytes. This lets a reader locate any cell in O(1) without decoding
//! the ones before it, and keeps variable-length payloads out of the fixed part
//! so the offset table stays a flat, indexable array.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

/// Discriminant tagging what a framed message's body means.
///
/// Stored as the `u8` [`MessageHeader.msg_type`] field on the wire. The values
/// are assigned explicitly and must stay stable across versions, since an older
/// peer decodes them by number. Requests and their matching responses are
/// paired (`connect`/`connect_resp`, `query`/`query_resp`, `fetch`/`fetch_resp`);
/// [`err`] is the generic failure reply for any request, and [`close`] tears a
/// session down.
pub const MessageType = enum(u8) {
    /// Client → server: open a session (the protocol handshake).
    connect = 1,
    /// Server → client: handshake result, body is a [`ConnectRespPayload`].
    connect_resp = 2,
    /// Client → server: submit a SQL statement for execution.
    query = 3,
    /// Server → client: query outcome / start of a result stream.
    query_resp = 4,
    /// Client → server: pull the next batch of rows from an open cursor,
    /// body is a [`FetchPayload`].
    fetch = 5,
    /// Server → client: a batch of rows in response to a [`fetch`].
    fetch_resp = 6,
    /// Either direction: close a session and release its server-side state.
    close = 7,
    /// Server → client: an error reply for any request, prefixed by an
    /// [`ErrorPayloadHeader`].
    err = 8,
};

/// Highest wire-protocol version this build implements and advertises.
///
/// Bump this only alongside a wire change; a peer at a higher version than this
/// is refused by [`isCompatible`] because we cannot decode its framing.
pub const PROTOCOL_VERSION: u16 = 1;

/// Oldest peer version this build can still talk to.
///
/// Anything below this is rejected by [`isCompatible`]. Raise it when support
/// for an obsolete framing is dropped so old clients fail the handshake cleanly
/// instead of exchanging bytes neither side agrees on.
pub const MIN_SUPPORTED_VERSION: u16 = 1;

/// Returns whether `peer_version` falls within the range this build supports.
///
/// The window is the closed interval
/// [[`MIN_SUPPORTED_VERSION`], [`PROTOCOL_VERSION`]]. Used by [`negotiate`] and
/// during the connect handshake to decide, before any payload is exchanged,
/// whether the two ends can proceed at all.
pub fn isCompatible(peer_version: u16) bool {
    return peer_version >= MIN_SUPPORTED_VERSION and peer_version <= PROTOCOL_VERSION;
}

/// Picks the effective version for a connection, or refuses it.
///
/// Returns the highest version both ends understand, which is
/// `min(peer_version, PROTOCOL_VERSION)` once compatibility is established, or
/// `null` when [`isCompatible`] rejects the peer. Returning `null` (rather than
/// clamping) is deliberate: a peer outside the supported window must be turned
/// away, not silently downgraded to a version it never offered.
pub fn negotiate(peer_version: u16) ?u16 {
    if (!isCompatible(peer_version)) return null;
    return @min(peer_version, PROTOCOL_VERSION);
}

/// Fixed-size frame header prefixing every message on the wire.
///
/// Declared `extern` so its layout is a stable C ABI: both the server and the
/// driver memcpy the raw bytes in and out of the socket buffer instead of
/// decoding field by field, which only works because the field order, sizes,
/// and padding are guaranteed. The header is followed on the wire by exactly
/// [`MessageHeader.payload_len`] body bytes.
pub const MessageHeader = extern struct {
    /// Sentinel identifying a kaidb frame; must equal [`MAGIC_VALUE`].
    /// A mismatch means the stream is corrupt or misaligned, see [`isValid`].
    magic: u32,
    /// The body's [`MessageType`], stored as its raw `u8` value.
    msg_type: u8,
    /// Reserved per-message flag bits; currently always initialised to `0`.
    flags: u8,
    /// Logical request identifier, letting multiple in-flight requests share
    /// one connection and letting a response be matched to its request.
    stream_id: u16,
    /// Number of body bytes that follow this header, so the framing layer
    /// knows how much to read before the next header.
    payload_len: u64,

    /// The magic tag, ASCII "NOVA" as a little-endian `u32` (`0x4e4f5641`).
    pub const MAGIC_VALUE: u32 = 0x4e4f5641;

    /// Builds a header with the magic tag set and `flags` cleared.
    ///
    /// The caller supplies the semantic fields; `magic` is always
    /// [`MAGIC_VALUE`] and `flags` always `0`, so a freshly built header
    /// passes [`isValid`] by construction.
    pub fn init(msg_type: MessageType, stream_id: u16, payload_len: u64) MessageHeader {
        return .{
            .magic = MAGIC_VALUE,
            .msg_type = @intFromEnum(msg_type),
            .flags = 0,
            .stream_id = stream_id,
            .payload_len = payload_len,
        };
    }

    /// Returns whether this header carries the expected [`MAGIC_VALUE`].
    ///
    /// The first check a receiver runs after reading a header off the wire: a
    /// false result signals a desynchronised or corrupt stream and the
    /// connection should be dropped rather than trusted.
    pub fn isValid(self: MessageHeader) bool {
        return self.magic == MAGIC_VALUE;
    }
};

/// Body of a [`MessageType.connect_resp`] frame.
///
/// `extern` for the same memcpy-on-the-wire reason as [`MessageHeader`]. Sent
/// once per successful handshake to hand the client its server-assigned
/// [`session_id`].
pub const ConnectRespPayload = extern struct {
    /// Handshake result code (`0` conventionally meaning success); the concrete
    /// codes are defined by the connect handler, not this framing layer.
    status: u8,
    /// Server-assigned session identifier the client echoes on later requests
    /// (for example in a [`FetchPayload`]).
    session_id: u64,
};

/// Body of a [`MessageType.fetch`] frame, requesting the next batch of rows.
///
/// `extern` for stable wire layout. Names the cursor to advance and how many
/// rows the client is willing to receive in the reply.
pub const FetchPayload = extern struct {
    /// Session (and thereby open cursor) to pull rows from, as handed out in a
    /// [`ConnectRespPayload`].
    session_id: u64,
    /// Maximum number of rows the server should return in this batch; the
    /// server may return fewer when the cursor is near exhaustion.
    batch_size: u32,
};

/// Column type used for a bound parameter, re-exported from the schema layer.
///
/// Aliases `schema/types.zig`'s `ColumnType` so protocol code can refer to
/// parameter types without importing the schema module directly, keeping the
/// wire layer's type vocabulary in one place.
pub const ParamType = @import("../schema/types.zig").ColumnType;
/// Column type of a result value, re-exported from the schema layer.
///
/// Same underlying `ColumnType` as [`ParamType`]; the two aliases document
/// intent (parameter vs result column) at each use site.
pub const ColumnType = @import("../schema/types.zig").ColumnType;

/// Fixed prefix of a [`MessageType.err`] frame's body.
///
/// `extern` for stable wire layout. Precedes the human-readable message text on
/// the wire; the reader consumes this header, then reads [`message_len`] bytes
/// of UTF-8 message that follow it.
pub const ErrorPayloadHeader = extern struct {
    /// Machine-readable error classifier; the code space is owned by the error
    /// handling layer, not this framing type.
    error_code: u32,
    /// Byte length of the message string that follows this header on the wire.
    message_len: u16,
};

/// Bounds-checked forward cursor for decoding a message body.
///
/// Wraps a borrowed, immutable byte slice and an advancing [`offset`]; it does
/// not own or copy the buffer, so it is only valid while that buffer outlives
/// it. Every read validates against the buffer end and returns
/// `error.EndOfStream` on a short read rather than reading past it, which is the
/// module's defence against a truncated or malicious frame.
pub const ProtocolReader = struct {
    /// The bytes being decoded; borrowed, never mutated or freed here.
    buffer: []const u8,
    /// Cursor position: index of the next unread byte in [`buffer`].
    offset: usize = 0,

    /// Creates a reader positioned at the start of `buffer`.
    pub fn init(buffer: []const u8) ProtocolReader {
        return .{ .buffer = buffer };
    }

    /// Reads and returns a value of POD type `T` by raw byte copy.
    ///
    /// Copies `@sizeOf(T)` bytes out of the buffer with `@memcpy` and advances
    /// [`offset`], mirroring how the sender laid the value down with
    /// [`ProtocolWriter.writeStruct`]. This reproduces the host byte order and
    /// padding, so it is correct only between peers of matching ABI (see the
    /// module endianness note). Returns `error.EndOfStream` if fewer than
    /// `@sizeOf(T)` bytes remain, leaving [`offset`] unchanged.
    pub fn readStruct(self: *ProtocolReader, comptime T: type) !T {
        const size = @sizeOf(T);
        if (self.offset + size > self.buffer.len) return error.EndOfStream;
        const slice = self.buffer[self.offset .. self.offset + size];
        var value: T = undefined;
        @memcpy(mem.asBytes(&value), slice);
        self.offset += size;
        return value;
    }

    /// Borrows the next `len` bytes as a sub-slice and advances past them.
    ///
    /// The returned slice aliases [`buffer`]; it is not copied, so it stays
    /// valid only as long as the underlying buffer does. Returns
    /// `error.EndOfStream` if fewer than `len` bytes remain, leaving [`offset`]
    /// unchanged.
    pub fn readString(self: *ProtocolReader, len: usize) ![]const u8 {
        if (self.offset + len > self.buffer.len) return error.EndOfStream;
        const slice = self.buffer[self.offset .. self.offset + len];
        self.offset += len;
        return slice;
    }

    /// Reads a host-endian `u32`; convenience over [`readStruct`].
    pub fn readU32(self: *ProtocolReader) !u32 {
        return self.readStruct(u32);
    }

    /// Reads a host-endian `u16`; convenience over [`readStruct`].
    pub fn readU16(self: *ProtocolReader) !u16 {
        return self.readStruct(u16);
    }

    /// Reads a single `u8`; convenience over [`readStruct`].
    pub fn readU8(self: *ProtocolReader) !u8 {
        return self.readStruct(u8);
    }
};

/// Growable byte-buffer builder for encoding a message body.
///
/// The write-side counterpart to [`ProtocolReader`]: it appends POD values and
/// raw bytes into an owned [`std.ArrayList`], then hands ownership of the packed
/// bytes to the caller via [`toOwnedSlice`]. The caller must either call
/// [`toOwnedSlice`] (which drains the buffer) or [`deinit`] to avoid leaking the
/// backing allocation.
pub const ProtocolWriter = struct {
    /// Allocator backing [`buffer`]; also used by [`toOwnedSlice`] to dupe the
    /// final slice.
    allocator: Allocator,
    /// Accumulating output bytes in wire order.
    buffer: std.ArrayList(u8),

    /// Creates an empty writer bound to `allocator`.
    ///
    /// No allocation happens until the first append; [`buffer`] starts as the
    /// empty `ArrayList`.
    pub fn init(allocator: Allocator) ProtocolWriter {
        return .{
            .allocator = allocator,
            .buffer = .empty,
        };
    }

    /// Frees the backing buffer.
    ///
    /// Safe to call after [`toOwnedSlice`], which resets [`buffer`] to empty; in
    /// that case this is a no-op on an empty list.
    pub fn deinit(self: *ProtocolWriter) void {
        self.buffer.deinit(self.allocator);
    }

    /// Appends the raw bytes of any POD value in host byte order.
    ///
    /// Reads `@sizeOf(@TypeOf(value))` bytes from `value` via `mem.asBytes` and
    /// appends them, producing exactly what [`ProtocolReader.readStruct`] will
    /// consume on the other end. `value` is taken by `anytype`, so pass an
    /// already-sized integer (e.g. `@as(u32, n)`) to control the width on the
    /// wire.
    pub fn writeStruct(self: *ProtocolWriter, value: anytype) !void {
        const T = @TypeOf(value);
        const bytes = mem.asBytes(&value);
        try self.buffer.appendSlice(self.allocator, bytes[0..@sizeOf(T)]);
    }

    /// Appends `str` verbatim, with no length prefix or terminator.
    ///
    /// The caller is responsible for framing (writing a length first, as
    /// [`RowWriter.serialize`] does) so the reader knows how many bytes to take
    /// back with [`ProtocolReader.readString`].
    pub fn writeString(self: *ProtocolWriter, str: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, str);
    }

    /// Detaches the accumulated bytes as a caller-owned slice and resets self.
    ///
    /// Returns a freshly `dupe`d copy of the current contents, then frees the
    /// internal buffer and re-arms it as empty so the writer can be reused. The
    /// caller owns the returned slice and must free it with the same allocator.
    /// The dupe (rather than handing back the ArrayList's own storage) keeps the
    /// writer's lifecycle self-contained.
    pub fn toOwnedSlice(self: *ProtocolWriter) ![]u8 {
        const slice = try self.allocator.dupe(u8, self.buffer.items);
        self.buffer.deinit(self.allocator);
        self.buffer = .empty;
        return slice;
    }
};

/// Encoder for a single result row's split fixed/heap byte layout.
///
/// A row is serialised as two length-prefixed sections: a `fixed` part (the
/// per-cell offset table) and a `heap` part (the length-prefixed cell bytes the
/// offsets point into). This type just pairs the two borrowed sections with an
/// allocator and frames them; the actual fixed/heap contents are built by
/// [`serializeQueryRow`]. Both slices are borrowed and must outlive the call to
/// [`serialize`].
pub const RowWriter = struct {
    /// Allocator used to build the framed output in [`serialize`].
    allocator: Allocator,
    /// The fixed-width section (the offset table); borrowed, not owned.
    fixed: []const u8,
    /// The variable-length heap section (length-prefixed cells); borrowed.
    heap: []const u8,

    /// Pairs the two row sections with `allocator` for later framing.
    pub fn init(allocator: Allocator, fixed: []const u8, heap: []const u8) RowWriter {
        return .{
            .allocator = allocator,
            .fixed = fixed,
            .heap = heap,
        };
    }

    /// Frames the row as `[u32 fixed_len][fixed][u32 heap_len][heap]`.
    ///
    /// Each section is prefixed by its `u32` length so a reader can split the
    /// two back apart with two [`ProtocolReader.readU32`] + `readString` pairs.
    /// Returns a caller-owned slice (via [`ProtocolWriter.toOwnedSlice`]); the
    /// caller frees it with [`allocator`]. The internal writer is cleaned up on
    /// every path by the `defer`.
    pub fn serialize(self: RowWriter) ![]u8 {
        var writer = ProtocolWriter.init(self.allocator);
        defer writer.deinit();

        try writer.writeStruct(@as(u32, @intCast(self.fixed.len)));
        try writer.writeString(self.fixed);

        try writer.writeStruct(@as(u32, @intCast(self.heap.len)));
        try writer.writeString(self.heap);

        return writer.toOwnedSlice();
    }
};

/// Serialises one result row from its per-cell byte slices.
///
/// Builds the split fixed/heap layout that [`RowWriter`] frames: the `fixed`
/// section is a flat array of `cells.len` little-endian `u32` offsets (4 bytes
/// each, zero-filled up front, then patched in place), and the `heap` section
/// holds each cell as a `u32` little-endian length followed by its raw bytes.
/// Cell `i`'s offset points at the start of its length prefix within the heap,
/// so a reader indexes any cell in O(1) via `fixed[i*4]` without scanning the
/// preceding cells.
///
/// The offset table is deliberately little-endian here (via
/// [`std.mem.writeInt`]) even though the struct helpers are host-endian, so
/// decoders must read it back with `readInt(..., .little)` to match. Returns a
/// caller-owned slice; the two scratch lists are freed on every path by their
/// `defer`s.
pub fn serializeQueryRow(allocator: Allocator, cells: []const []const u8) ![]u8 {
    var fixed_list = std.ArrayList(u8).empty;
    defer fixed_list.deinit(allocator);
    var heap_list = std.ArrayList(u8).empty;
    defer heap_list.deinit(allocator);

    try fixed_list.appendNTimes(allocator, 0, cells.len * 4);

    for (cells, 0..) |cell, i| {
        const offset: u32 = @intCast(heap_list.items.len);
        std.mem.writeInt(u32, fixed_list.items[i * 4 ..][0..4], offset, .little);

        const str_len: u32 = @intCast(cell.len);
        try heap_list.appendSlice(allocator, std.mem.asBytes(&str_len));
        try heap_list.appendSlice(allocator, cell);
    }

    var writer = RowWriter.init(allocator, fixed_list.items, heap_list.items);
    return try writer.serialize();
}

test "Protocol header and fixed/heap row serialization" {
    const allocator = std.testing.allocator;

    const header = MessageHeader.init(.query, 12, 100);
    try std.testing.expect(header.isValid());
    try std.testing.expectEqual(@intFromEnum(MessageType.query), header.msg_type);
    try std.testing.expectEqual(@as(u16, 12), header.stream_id);
    try std.testing.expectEqual(@as(u64, 100), header.payload_len);

    const mock_fixed = "FIXED_DATA";
    const mock_heap = "HEAP_DATA";

    const row_writer = RowWriter.init(allocator, mock_fixed, mock_heap);
    const row_bytes = try row_writer.serialize();
    defer allocator.free(row_bytes);

    var reader = ProtocolReader.init(row_bytes);
    const fixed_len = try reader.readU32();
    try std.testing.expectEqual(@as(u32, @intCast(mock_fixed.len)), fixed_len);

    const fixed = try reader.readString(fixed_len);
    try std.testing.expectEqualStrings(mock_fixed, fixed);

    const heap_len = try reader.readU32();
    try std.testing.expectEqual(@as(u32, @intCast(mock_heap.len)), heap_len);

    const heap = try reader.readString(heap_len);
    try std.testing.expectEqualStrings(mock_heap, heap);

    const cells = [_][]const u8{ "val1", "val2" };
    const query_row_bytes = try serializeQueryRow(allocator, &cells);
    defer allocator.free(query_row_bytes);

    var qr_reader = ProtocolReader.init(query_row_bytes);
    const q_fixed_len = try qr_reader.readU32();
    try std.testing.expectEqual(@as(u32, 8), q_fixed_len);
    const q_fixed = try qr_reader.readString(q_fixed_len);

    const offset0 = std.mem.readInt(u32, q_fixed[0..4], .little);
    const offset1 = std.mem.readInt(u32, q_fixed[4..8], .little);
    try std.testing.expectEqual(@as(u32, 0), offset0);
    try std.testing.expectEqual(@as(u32, 8), offset1);

    const q_heap_len = try qr_reader.readU32();
    const q_heap = try qr_reader.readString(q_heap_len);

    const len0 = std.mem.readInt(u32, q_heap[offset0..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 4), len0);
    try std.testing.expectEqualStrings("val1", q_heap[offset0 + 4 .. offset0 + 4 + len0]);

    const len1 = std.mem.readInt(u32, q_heap[offset1..][0..4], .little);
    try std.testing.expectEqual(@as(u32, 4), len1);
    try std.testing.expectEqualStrings("val2", q_heap[offset1 + 4 .. offset1 + 4 + len1]);
}

test "D10: protocol version negotiation" {
    try std.testing.expect(isCompatible(1));
    try std.testing.expect(!isCompatible(0));
    try std.testing.expect(!isCompatible(2));
    try std.testing.expectEqual(@as(?u16, 1), negotiate(1));
    try std.testing.expectEqual(@as(?u16, null), negotiate(0));
    try std.testing.expectEqual(@as(?u16, null), negotiate(99));
}
