//! On-disk record format for the write-ahead log (WAL).
//!
//! This module defines the single unit of durability in kaidb: a
//! [`LogRecord`], the length-prefixed, checksummed entry that the WAL appends
//! for every transaction boundary and every row mutation. Everything the
//! recovery path replays after a crash is a stream of these records read back
//! in append order, so the on-wire encoding here is effectively the durability
//! contract between the running engine and its own future self.
//!
//! Wire layout (all integers little-endian):
//!
//! ```text
//!   u32  payload_len   -- byte count of everything AFTER this field
//!   u64  lsn           -- monotonic log-sequence number
//!   u64  tx_id         -- owning transaction
//!   i64  timestamp     -- wall-clock at append
//!   u8   kind           -- OpKind discriminant
//!   u32  table_name.len
//!   ...  table_name bytes
//!   u32  key.len
//!   ...  key bytes
//!   u32  value.len
//!   ...  value bytes
//!   u64  checksum      -- Wyhash over every logical field (NOT the framing)
//! ```
//!
//! Two design decisions matter for correctness and recovery:
//!
//!   1. The `payload_len` prefix frames each record so the reader knows how far
//!      to advance without trusting any inner length, and so a torn tail write
//!      (the last record only partly flushed before a crash) is detected: a
//!      short read of the fixed fields surfaces as [`LogRecord.deserialize`]
//!      returning `error.InvalidRecordLength`, and a clean end-of-file returns
//!      `null` (normal end of log) rather than an error.
//!
//!   2. The trailing `checksum` is a Wyhash over the *logical* field values
//!      ([`LogRecord.hash`]), independent of the framing bytes. On replay,
//!      [`LogRecord.deserialize`] recomputes it and rejects a record whose
//!      contents were corrupted on disk with `error.ChecksumMismatch`, so a
//!      bit-rotted or half-written record cannot be silently replayed.
//!
//! The record borrows nothing on the write side (fields are plain slices the
//! caller owns), but on the read side [`LogRecord.deserialize`] allocates the
//! three variable-length slices from the supplied allocator; the caller owns
//! and must free `table_name`, `key`, and `value`.

const std = @import("std");

/// The kind of operation a [`LogRecord`] describes.
///
/// The discriminant values (0..5) are the stable on-disk encoding written by
/// [`LogRecord.serialize`] and decoded by [`LogRecord.deserialize`]; they must
/// never be renumbered, because existing WAL files on disk carry these exact
/// bytes. `begin`/`commit`/`rollback` mark transaction boundaries and carry no
/// row payload; `insert`/`update`/`delete` carry the affected `key` and (for
/// insert/update) the new `value`.
pub const OpKind = enum(u8) {
    /// Start of a transaction. Bounds the group of mutations that a matching
    /// `commit` makes durable or a `rollback`/crash discards.
    begin = 0,
    /// Successful end of a transaction. Records at or before this LSN for the
    /// same `tx_id` are durable once this record is flushed.
    commit = 1,
    /// Explicit abort of a transaction; its mutations are not to be applied on
    /// replay.
    rollback = 2,
    /// A row was inserted. `key` and `value` hold the new tuple.
    insert = 3,
    /// A row was updated. `key` identifies the row; `value` holds the new image.
    update = 4,
    /// A row was deleted. `key` identifies the removed row; `value` is empty.
    delete = 5,
};

/// One durable write-ahead-log entry: a transaction boundary or a row mutation.
///
/// A record is self-describing and self-verifying on disk: it is framed by a
/// leading `payload_len` and validated by a trailing checksum (see the module
/// header for the exact layout). Instances constructed in memory borrow their
/// slice fields from the caller; instances produced by
/// [`LogRecord.deserialize`] own freshly allocated `table_name`/`key`/`value`
/// that the caller must free.
pub const LogRecord = struct {
    /// Log-sequence number: a monotonically increasing id assigned when the
    /// record is appended, used to order the log and to reason about what is
    /// durable up to a given point.
    lsn: u64,
    /// Identifier of the transaction this record belongs to. Ties row mutations
    /// to their `begin`/`commit`/`rollback` boundaries during replay.
    tx_id: u64,
    /// Wall-clock timestamp captured at append time. Diagnostic/audit only; the
    /// authoritative ordering comes from [`LogRecord.lsn`], not this value.
    timestamp: i64,
    /// Which operation this record encodes; see [`OpKind`].
    kind: OpKind,
    /// Name of the table the operation targets. Empty for pure transaction
    /// boundary records.
    table_name: []const u8,
    /// The affected row's key. Empty for `begin`/`commit`/`rollback`.
    key: []const u8,
    /// The row payload: the new tuple image for `insert`/`update`, empty for
    /// `delete` and for transaction boundaries.
    value: []const u8,

    /// Computes the Wyhash checksum stored in (and verified from) the record's
    /// trailer.
    ///
    /// Hashes the logical field values in a fixed order, deliberately excluding
    /// the framing `payload_len` and the checksum itself, so the same content
    /// hashes identically whether it is being written or read back. Any change
    /// to a field, including a corrupted length or byte on disk, changes the
    /// result, which is how [`LogRecord.deserialize`] detects tampering or
    /// bit-rot.
    pub fn hash(self: LogRecord) u64 {
        const seed: u64 = 0;
        var hasher = std.hash.Wyhash.init(seed);
        hasher.update(std.mem.asBytes(&self.lsn));
        hasher.update(std.mem.asBytes(&self.tx_id));
        hasher.update(std.mem.asBytes(&self.timestamp));
        hasher.update(std.mem.asBytes(&self.kind));
        hasher.update(self.table_name);
        hasher.update(self.key);
        hasher.update(self.value);
        return hasher.final();
    }

    /// Returns the exact number of bytes this record occupies on disk once
    /// serialized, including the leading `payload_len` prefix and the trailing
    /// checksum.
    ///
    /// [`LogRecord.serialize`] uses this to compute `payload_len` as
    /// `size() - @sizeOf(u32)` (everything after the length prefix), so this
    /// function and the serializer must agree field-for-field or framing
    /// breaks. The four `@sizeOf(u32)` terms account for the length prefix plus
    /// the three slice-length words; the slice `.len` terms account for the
    /// variable payloads.
    pub fn size(self: LogRecord) usize {
        return @sizeOf(u32) +
            @sizeOf(u64) +
            @sizeOf(u64) +
            @sizeOf(i64) +
            @sizeOf(u8) +
            @sizeOf(u32) +
            self.table_name.len +
            @sizeOf(u32) +
            self.key.len +
            @sizeOf(u32) +
            self.value.len +
            @sizeOf(u64);
    }

    /// Writes the record to `writer` in the canonical WAL wire format.
    ///
    /// Emits the `payload_len` frame first, then every fixed field, then each
    /// variable field prefixed by its own `u32` length, then the checksum
    /// trailer, all little-endian. The checksum is computed with
    /// [`LogRecord.hash`] before writing so it covers the exact bytes that
    /// follow. Propagates any error the underlying `writer` raises; a partial
    /// write on failure leaves a torn record that [`LogRecord.deserialize`] will
    /// reject via short-read or checksum failure on the next read.
    pub fn serialize(record: LogRecord, writer: anytype) !void {
        const checksum = record.hash();
        const payload_len: u32 = @intCast(record.size() - @sizeOf(u32));
        try writer.writeInt(u32, payload_len, .little);
        try writer.writeInt(u64, record.lsn, .little);
        try writer.writeInt(u64, record.tx_id, .little);
        try writer.writeInt(i64, record.timestamp, .little);
        try writer.writeInt(u8, @intFromEnum(record.kind), .little);
        
        try writer.writeInt(u32, @intCast(record.table_name.len), .little);
        try writer.writeAll(record.table_name);
        
        try writer.writeInt(u32, @intCast(record.key.len), .little);
        try writer.writeAll(record.key);
        
        try writer.writeInt(u32, @intCast(record.value.len), .little);
        try writer.writeAll(record.value);
        
        try writer.writeInt(u64, checksum, .little);
    }

    /// Reads and validates one record from `reader`, allocating its variable
    /// fields from `allocator`.
    ///
    /// Returns `null` on a clean end-of-stream at a record boundary (the reader
    /// is out of records: the normal end of a WAL scan). This is distinct from
    /// the error cases:
    ///
    ///   - `error.RecordTooLarge` if the framed `payload_len` exceeds a 1 GB
    ///     sanity cap, which guards against a corrupt length triggering a huge
    ///     allocation.
    ///   - `error.InvalidRecordLength` if the stream ends partway through the
    ///     fixed fields, the `kind` byte is not a valid [`OpKind`], or the
    ///     bytes actually consumed do not match the declared `payload_len` (a
    ///     torn or misframed record).
    ///   - `error.ChecksumMismatch` if the recomputed [`LogRecord.hash`] does
    ///     not equal the stored trailer, i.e. the content was corrupted.
    ///
    /// On any of the post-allocation failures the three owned slices are freed
    /// before returning, so a rejected record never leaks. On success the caller
    /// owns `table_name`, `key`, and `value` and must free them. The `errdefer`
    /// guards cover only failures raised between each allocation and the next
    /// read; the explicit frees before the length/checksum returns cover the
    /// case where all three are already allocated.
    pub fn deserialize(allocator: std.mem.Allocator, reader: anytype) !?LogRecord {
        const payload_len = reader.readInt(u32, .little) catch |err| {
            if (err == error.EndOfStream) return null;
            return err;
        };
        if (payload_len > 1_000_000_000) return error.RecordTooLarge;

        const lsn = reader.readInt(u64, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const tx_id = reader.readInt(u64, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const timestamp = reader.readInt(i64, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const kind_int = reader.readInt(u8, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };
        const kind: OpKind = switch (kind_int) {
            0 => OpKind.begin,
            1 => OpKind.commit,
            2 => OpKind.rollback,
            3 => OpKind.insert,
            4 => OpKind.update,
            5 => OpKind.delete,
            else => return error.InvalidRecordLength,
        };

        const table_name_len = reader.readInt(u32, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };
        const table_name = try allocator.alloc(u8, table_name_len);
        errdefer allocator.free(table_name);
        _ = reader.readAll(table_name) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const key_len = reader.readInt(u32, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };
        const key = try allocator.alloc(u8, key_len);
        errdefer allocator.free(key);
        _ = reader.readAll(key) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const value_len = reader.readInt(u32, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };
        const value = try allocator.alloc(u8, value_len);
        errdefer allocator.free(value);
        _ = reader.readAll(value) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const checksum = reader.readInt(u64, .little) catch |err| {
            if (err == error.EndOfStream) return error.InvalidRecordLength;
            return err;
        };

        const fixed_fields_len: u32 = 8 + 8 + 8 + 1 + 4 + table_name_len + 4 + key_len + 4 + value_len + 8;

        if (payload_len != fixed_fields_len) {
            allocator.free(table_name);
            allocator.free(key);
            allocator.free(value);
            return error.InvalidRecordLength;
        }

        const record = LogRecord{
            .lsn = lsn,
            .tx_id = tx_id,
            .timestamp = timestamp,
            .kind = kind,
            .table_name = table_name,
            .key = key,
            .value = value,
        };

        if (record.hash() != checksum) {
            allocator.free(table_name);
            allocator.free(key);
            allocator.free(value);
            return error.ChecksumMismatch;
        }
        return record;
    }
};
