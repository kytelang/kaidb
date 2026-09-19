//! Wire-protocol message types for the kaidb server.
//!
//! This module defines the on-the-wire shapes exchanged between a client and
//! the kaidb server, and between a primary and its replicas. It is the shared
//! vocabulary that both ends serialise to and parse from, so it lives in
//! `common/` and is deliberately free of any storage, executor, or networking
//! logic: it only knows how to turn a message into bytes and back.
//!
//! There are TWO distinct framing conventions here, kept separate on purpose:
//!
//!   1. The **request/response protocol** ([`Operation`] carried inside a
//!      [`Packet`]). This is the client-facing path a driver uses to run SQL
//!      and receive replies, and it is also how WAL records are shipped to a
//!      replica ([`ShipWalRequest`]). Its body is encoded as JSON: [`Packet`]
//!      length-prefixes a JSON document of the tagged [`Operation`] union, which
//!      keeps the message schema self-describing and easy to evolve at the cost
//!      of density. (The engine's high-throughput driver path uses the purely
//!      binary protocol under `src/proto/`; this JSON packet form is the
//!      simpler control/legacy path.)
//!
//!   2. The **replication streaming protocol** ([`ReplFrames`] / [`ReplAck`]).
//!      This is a compact, hand-rolled little-endian binary format used on the
//!      primary→replica log-shipping channel, where volume is high and every
//!      byte counts. Each frame carries a CRC32 so a replica can detect a
//!      corrupted or truncated stream, and the batch is tagged with an `epoch`
//!      and `base_seq` so the receiver can order frames and reject stale batches
//!      from a superseded primary (see [`ReplAck`], the flow-control reply).
//!
//! Ownership convention: the parse routines here (`deserialize`,
//! [`Packet.deserialize`]) allocate the memory they hand back, and the matching
//! `deinit` / [`Packet.free`] release it. String fields on the request structs
//! are borrowed slices while a message is being built for sending, but become
//! owned allocations once parsed from bytes, which is why the free paths differ
//! by how the value was produced.

const std = @import("std");
/// Convenience alias for the standard allocator interface used by every
/// serialise/parse routine in this module.
const Allocator = std.mem.Allocator;

/// Result code carried in a [`ReplyResponse`], encoded as a single byte on the
/// wire.
///
/// A bare success/failure discriminator: the human-readable detail (an error
/// message, or the query result set) rides in [`ReplyResponse.data`] alongside
/// it, so this only distinguishes "the request succeeded" from "it did not".
pub const Status = enum(u8) {
    /// The request completed successfully; any payload is a valid result.
    Ok = 0,
    /// The request failed; [`ReplyResponse.data`] holds the error detail.
    Error = 1,
};

/// A client's request to execute a SQL statement.
pub const QueryRequest = struct {
    /// The SQL text to execute, verbatim.
    sql: []const u8,
    /// Optional opaque session/authentication token identifying the caller, or
    /// `null` for an unauthenticated or session-less request.
    session_token: ?[]const u8 = null,
};

/// The server's reply to a [`QueryRequest`].
pub const ReplyResponse = struct {
    /// Whether the request succeeded; see [`Status`].
    status: Status,
    /// The payload: the serialised result set on success, or the error detail
    /// on failure. `null` when there is nothing to return (e.g. a statement
    /// that produced no rows and no error text).
    data: ?[]const u8 = null,
};

/// A client's request to authenticate a connection before issuing queries.
pub const AuthenticateRequest = struct {
    /// The user identity presented by the caller.
    uid: []const u8,
    /// The credential (key/secret) proving that identity.
    key: []const u8,
};

/// A single write-ahead-log record shipped from the primary to a replica.
///
/// This carries one logical mutation extracted from the primary's WAL so the
/// replica can replay it against its own copy of the tree. The `lsn`/`tx_id`
/// let the replica apply records in log order and group them by transaction,
/// and `kind` selects the mutation type (insert/update/delete) that
/// `table_name`/`key`/`value` then describe.
pub const ShipWalRequest = struct {
    /// Log sequence number: the record's monotonic position in the primary's
    /// WAL, used by the replica to apply in order and to track how far it has
    /// caught up.
    lsn: u64,
    /// Identifier of the transaction this record belongs to, so the replica can
    /// group and commit records atomically per transaction.
    tx_id: u64,
    /// Wall-clock time the record was produced on the primary (used for
    /// replication lag reporting and diagnostics).
    timestamp: i64,
    /// The mutation type discriminator (e.g. insert vs update vs delete),
    /// interpreted by the replay path.
    kind: u8,
    /// Name of the table the mutation applies to.
    table_name: []const u8,
    /// The row/index key affected by the mutation.
    key: []const u8,
    /// The new value payload (empty for a delete, per the `kind`).
    value: []const u8,
};

/// The tagged set of messages that can travel inside a [`Packet`].
///
/// This union IS the protocol's message schema: [`Packet.serialize`] emits it as
/// JSON and [`Packet.deserialize`] parses it back, so the active tag names the
/// operation and its payload is the matching request/response struct above.
pub const Operation = union(enum) {
    /// A client SQL request.
    Query: QueryRequest,
    /// A server reply to a request.
    Reply: ReplyResponse,
    /// A client authentication request.
    Authenticate: AuthenticateRequest,
    /// A shipped WAL record for a replica to replay.
    ShipWal: ShipWalRequest,
};


/// A batch of replication log frames in the compact binary streaming format.
///
/// Wire layout (all integers little-endian): `epoch` (u64), `base_seq` (u64),
/// frame count (u32), then for each frame its length (u32), a CRC32 of its
/// bytes (u32), and the raw bytes. The per-frame CRC lets the receiver reject a
/// corrupted or truncated stream ([`deserialize`] returns
/// `error.ChecksumMismatch`), and the leading `epoch`/`base_seq` let it order
/// frames and discard batches from a stale primary. The [`ReplAck`] reply closes
/// the loop by confirming how far the receiver has durably accepted.
pub const ReplFrames = struct {
    /// Primary term/generation number. A replica ignores batches from an epoch
    /// older than the one it has already accepted, which is how a superseded
    /// primary's late frames are fenced off.
    epoch: u64,
    /// Sequence number of the FIRST frame in this batch; the Nth frame's
    /// sequence is `base_seq + N`. Lets the receiver detect gaps and place the
    /// batch in the overall stream.
    base_seq: u64,
    /// The raw frame payloads, in order; each is one serialised replication
    /// record whose meaning is opaque to this codec.
    frames: []const []const u8,

    /// Serialises this batch into a freshly allocated buffer in the binary wire
    /// format described on [`ReplFrames`].
    ///
    /// Writes the header, then each frame prefixed by its length and CRC32. The
    /// caller owns the returned buffer and must free it with `allocator`. The
    /// intermediate allocating writer is torn down before returning, so the
    /// result is an independent `dupe` and not a view into scratch memory.
    pub fn serialize(self: ReplFrames, allocator: Allocator) ![]u8 {
        var out = std.Io.Writer.Allocating.init(allocator);
        defer out.deinit();
        const w = &out.writer;
        try w.writeInt(u64, self.epoch, .little);
        try w.writeInt(u64, self.base_seq, .little);
        try w.writeInt(u32, @intCast(self.frames.len), .little);
        for (self.frames) |f| {
            try w.writeInt(u32, @intCast(f.len), .little);
            try w.writeInt(u32, std.hash.Crc32.hash(f), .little);
            try w.writeAll(f);
        }
        return allocator.dupe(u8, out.written());
    }

    /// Parses a binary batch produced by [`serialize`] back into a
    /// [`ReplFrames`], allocating a fresh copy of every frame.
    ///
    /// Validates as it goes: `error.InvalidFrame` if the buffer is shorter than
    /// the 20-byte header or if any declared frame length runs past the end of
    /// the buffer, and `error.ChecksumMismatch` if a frame's bytes do not match
    /// its stored CRC32. On any error the frames allocated so far are freed via
    /// the `errdefer`, so no memory leaks on a partial parse. On success the
    /// returned value owns its `frames`; release it with [`deinit`].
    pub fn deserialize(allocator: Allocator, bytes: []const u8) !ReplFrames {
        var off: usize = 0;
        if (bytes.len < 20) return error.InvalidFrame;
        const epoch = std.mem.readInt(u64, bytes[off..][0..8], .little);
        off += 8;
        const base_seq = std.mem.readInt(u64, bytes[off..][0..8], .little);
        off += 8;
        const count = std.mem.readInt(u32, bytes[off..][0..4], .little);
        off += 4;

        const frames = try allocator.alloc([]const u8, count);
        var filled: usize = 0;
        errdefer {
            for (frames[0..filled]) |f| allocator.free(f);
            allocator.free(frames);
        }
        var i: usize = 0;
        while (i < count) : (i += 1) {
            if (off + 8 > bytes.len) return error.InvalidFrame;
            const flen = std.mem.readInt(u32, bytes[off..][0..4], .little);
            off += 4;
            const fcrc = std.mem.readInt(u32, bytes[off..][0..4], .little);
            off += 4;
            if (off + flen > bytes.len) return error.InvalidFrame;
            const fbytes = bytes[off..][0..flen];
            off += flen;
            if (std.hash.Crc32.hash(fbytes) != fcrc) return error.ChecksumMismatch;
            frames[i] = try allocator.dupe(u8, fbytes);
            filled = i + 1;
        }
        return ReplFrames{ .epoch = epoch, .base_seq = base_seq, .frames = frames };
    }

    /// Frees the owned frame storage of a [`ReplFrames`] returned by
    /// [`deserialize`].
    ///
    /// Releases each frame's bytes and then the frame pointer array. Only call
    /// this on a value whose `frames` were allocated by [`deserialize`], not on
    /// one built from borrowed slices for sending.
    pub fn deinit(self: ReplFrames, allocator: Allocator) void {
        for (self.frames) |f| allocator.free(f);
        allocator.free(self.frames);
    }
};

/// The replica's acknowledgement reply on the replication channel.
///
/// Fixed 16-byte little-endian message reporting how far the replica has
/// durably accepted, which the primary uses for flow control and to advance its
/// commit/truncation watermark. Fixed-size and allocation-free: [`serialize`]
/// returns a stack array, so acks are cheap to send frequently.
pub const ReplAck = struct {
    /// The primary epoch this ack pertains to; must match the batch's
    /// [`ReplFrames.epoch`] so a stale primary cannot misread a newer replica's
    /// ack as progress on its own term.
    epoch: u64,
    /// The highest contiguous sequence number the replica has durably accepted;
    /// the primary may consider everything up to and including this confirmed.
    confirmed_seq: u64,

    /// Serialises this ack into a fixed 16-byte little-endian buffer.
    ///
    /// Returns the bytes by value on the stack (`epoch` then `confirmed_seq`),
    /// so there is nothing to free.
    pub fn serialize(self: ReplAck) [16]u8 {
        var buf: [16]u8 = undefined;
        std.mem.writeInt(u64, buf[0..8], self.epoch, .little);
        std.mem.writeInt(u64, buf[8..16], self.confirmed_seq, .little);
        return buf;
    }

    /// Parses a 16-byte little-endian buffer back into a [`ReplAck`].
    ///
    /// Returns `error.InvalidAck` if fewer than 16 bytes are supplied. Reads
    /// only the first 16 bytes; any trailing bytes are ignored. No allocation,
    /// so there is nothing to free.
    pub fn deserialize(bytes: []const u8) !ReplAck {
        if (bytes.len < 16) return error.InvalidAck;
        return ReplAck{
            .epoch = std.mem.readInt(u64, bytes[0..8], .little),
            .confirmed_seq = std.mem.readInt(u64, bytes[8..16], .little),
        };
    }
};

/// The client/server request-response envelope carrying an [`Operation`].
///
/// The header fields below (`checksum`/`packet_length`/`packet_id`/`timestamp`)
/// describe a full framed packet, but note that [`serialize`] currently emits
/// only a length-prefixed JSON body of `op`; the header fields default to zero
/// and are not written on the wire by the current codec. The `op` field is the
/// actual message. `_parsed` is bookkeeping that ties the packet's lifetime to
/// the JSON arena its strings were parsed from.
pub const Packet = struct {
    /// Integrity checksum of the packet (header field; not populated by the
    /// current JSON [`serialize`] path).
    checksum: u64 = 0,
    /// Total packet length (header field; not populated by the current JSON
    /// [`serialize`] path, which writes its own u32 body-length prefix).
    packet_length: u32 = 0,
    /// Correlation id linking a reply to its request (header field; not
    /// populated by the current JSON [`serialize`] path).
    packet_id: u32 = 0,
    /// Packet creation time (header field; not populated by the current JSON
    /// [`serialize`] path).
    timestamp: i64 = 0,
    /// The message this packet carries; the only field the current codec
    /// actually transmits.
    op: Operation,
    /// The JSON parse arena that backs `op`'s string slices when this packet
    /// came from [`deserialize`], or `null` when `op` was constructed directly.
    ///
    /// [`free`] uses its presence to decide how to reclaim `op`: if set, one
    /// `p.deinit()` frees every borrowed slice at once (they all point into this
    /// arena); if `null`, the individual owned strings are freed one by one.
    /// This is what keeps freeing correct across both construction paths.
    _parsed: ?std.json.Parsed(Operation) = null,

    /// Serialises this packet's [`Operation`] as a length-prefixed JSON body.
    ///
    /// Renders `op` to JSON in a temporary page-allocator buffer, writes its
    /// byte length as a little-endian u32, then writes the JSON bytes. The
    /// header fields are NOT emitted. The scratch buffer is freed before
    /// returning. Any writer error propagates.
    pub fn serialize(self: Packet, writer: anytype) !void {
        var allocating = std.Io.Writer.Allocating.init(std.heap.page_allocator);
        defer allocating.deinit();
        try allocating.writer.print("{f}", .{std.json.fmt(self.op, .{})});
        try writer.writeInt(u32, @intCast(allocating.written().len), .little);
        try writer.writeAll(allocating.written());
    }

    /// Parses a length-prefixed JSON packet produced by [`serialize`] into a
    /// [`Packet`].
    ///
    /// Reads the leading little-endian u32 body length, bounds-checks it against
    /// the buffer (`error.InvalidPacket` if the buffer is too short for either
    /// the prefix or the declared body), then JSON-parses the [`Operation`]. The
    /// resulting [`std.json.Parsed`] is retained in `_parsed` so the packet owns
    /// the arena backing its strings; release everything with [`free`]. The
    /// `errdefer` cleans up the parse arena if construction of the return value
    /// fails.
    pub fn deserialize(allocator: Allocator, bytes: []const u8) !Packet {
        if (bytes.len < 4) return error.InvalidPacket;
        const len = std.mem.readInt(u32, bytes[0..4], .little);
        if (bytes.len < 4 + len) return error.InvalidPacket;
        const payload = bytes[4..][0..len];

        const parsed = try std.json.parseFromSlice(Operation, allocator, payload, .{});
        errdefer parsed.deinit();

        return Packet{
            .op = parsed.value,
            ._parsed = parsed,
        };
    }

    /// Frees the memory owned by a [`Packet`], handling both construction paths.
    ///
    /// If the packet was produced by [`deserialize`], `_parsed` is set and a
    /// single `deinit` on the JSON arena reclaims all of `op`'s strings at once.
    /// Otherwise `op` was built from individually allocated strings, so each
    /// active-variant field is freed separately with `allocator`. Calling this
    /// on a packet whose `op` holds borrowed (non-owned) slices and no `_parsed`
    /// would wrongly free the caller's memory, so match the free path to how the
    /// packet was made.
    pub fn free(allocator: Allocator, self: Packet) void {
        if (self._parsed) |p| {
            p.deinit();
        } else {
            switch (self.op) {
                .Query => |q| allocator.free(q.sql),
                .Reply => |r| {
                    if (r.data) |d| allocator.free(d);
                },
                .Authenticate => |auth| {
                    allocator.free(auth.uid);
                    allocator.free(auth.key);
                },
                .ShipWal => |ship| {
                    allocator.free(ship.table_name);
                    allocator.free(ship.key);
                    allocator.free(ship.value);
                },
            }
        }
    }
};
