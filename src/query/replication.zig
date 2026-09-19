//! Streaming primary/follower replication for kaidb.
//!
//! This module carries committed writes from the primary (leader) to one or more
//! followers over a length-framed binary stream, so a follower can stand in if the
//! primary fails. It is the counterpart to the durability layer: the WAL makes a
//! single node crash-safe, and this module makes the *cluster* fault-tolerant by
//! shipping the same logical change stream to replicas.
//!
//! ## What flows over the wire
//!
//! The unit of replication is a *batch* of serialised [`LogRecord`] frames wrapped
//! in a [`proto.ReplFrames`] envelope: an `epoch`, a `base_seq` (the sequence number
//! of the batch's first frame), and the frames themselves. Sequence numbers are a
//! contiguous per-primary counter ([`DurableReplicator.next_seq`]); a follower acks
//! back the highest `confirmed_seq` it has durably applied plus the `epoch` it has
//! seen, in a fixed 16-byte [`proto.ReplAck`]. Everything is little-endian and each
//! message is prefixed by a `u32` length, so a reader always knows how much to pull
//! before deserialising.
//!
//! ## Ordering and gap invariants
//!
//! Replication is strictly in-order and gap-intolerant on the apply side. A follower
//! ([`Follower.recvFrames`]) only applies a batch whose `base_seq` is exactly
//! `confirmed_seq + 1`; anything ahead is reported as `.gap` and the primary must
//! resend from the follower's confirmed point. This is why the primary keeps history:
//! an in-memory ring of recently sent batches ([`DurableReplicator.sent`], capped at
//! [`DurableReplicator.MAX_RETAINED`]) and an optional on-disk [`BackfillLog`] that
//! survives further back. On reconnect the primary sends an empty heartbeat batch to
//! learn the follower's `confirmed_seq`, then replays every retained batch above it
//! ([`DurableReplicator.reconnectAndCatchUp`]). If neither the ring nor the backfill
//! reaches back far enough, catch-up fails with `error.SnapshotRequired` and an
//! operator must restore the follower from a full snapshot.
//!
//! ## Epochs and fencing
//!
//! `epoch` is the leader-generation number bumped on every failover/leader election.
//! A follower records the highest epoch it has ever seen and *fences* any batch from
//! a lower epoch ([`Follower.recvFrames`] `.fenced`), so a deposed old primary that
//! comes back cannot corrupt a follower that has already accepted a newer leader.
//! Epoch and `confirmed_seq` are persisted together in [`FollowerState`] via an
//! atomic write-temp-then-rename, so a follower recovers its fencing point across a
//! restart.
//!
//! ## Durability coupling: ship-on-commit
//!
//! [`DurableReplicator`] subscribes to the local commit stream through
//! [`DurableReplicator.onRecord`]: it buffers serialised records as they are produced,
//! discards the buffer on `rollback`, and only when a `commit` record arrives does
//! [`DurableReplicator.shipPending`] assign sequence numbers, retain the batch, append
//! it to the backfill log, and send it. `await_quorum` selects synchronous replication
//! (block until a majority of replicas confirm, via [`QuorumTracker`]) versus
//! asynchronous (commit locally, catch the follower up on a later write).
//!
//! ## Security
//!
//! Two independent layers guard the link. Transport is optional mutual-TLS
//! ([`TlsConfig`]): when a CA, cert, and key are all configured the follower's
//! [`ReplServer`] *requires* a client certificate and the primary presents one.
//! Above (or instead of) TLS, a shared-key HMAC-SHA256 challenge-response
//! ([`serverAuthenticate`]/[`clientAuthenticate`]) proves both peers hold the same
//! secret; the tag comparison is constant-time ([`ctEq`]) to avoid a timing oracle.
//!
//! ## Roles in this file
//!
//! * [`DurableReplicator`] + [`ReplClient`] run on the **primary** and push batches.
//! * [`ReplServer`] + [`Follower`] run on a **follower** and apply them.
//! * [`ReplWal`], [`CheckpointRecord`], [`BackfillLog`] are the on-disk retention
//!   stores that let a lagging follower catch up without a fresh snapshot.
//! * [`QuorumTracker`] turns per-follower ack progress into a durability decision.

const std = @import("std");
const builtin = @import("builtin");
/// Zig's evented-I/O namespace; every blocking op here takes an [`Io`] token so the
/// caller's scheduler drives the syscall rather than the OS thread parking.
const Io = std.Io;
/// TCP/socket types (`net.Stream`, `net.Server`, `net.IpAddress`) used for the wire.
const net = std.Io.net;
/// Convenience alias for the standard allocator interface threaded through this module.
const Allocator = std.mem.Allocator;
/// Server configuration; supplies `replica.address/port/uid/key` for the primary's
/// outbound replication client (see [`ReplClient.connect`]).
const Config = @import("../common/config.zig").Config;
/// The binary wire-protocol namespace. Re-exported `pub` so callers of this module
/// can name [`proto.ReplFrames`]/[`proto.ReplAck`] without a second import.
pub const proto = @import("../common/proto.zig");
/// Framed request/response envelope used for the initial authentication handshake
/// on the primary's raw (`ship`/`readReply`) path.
const Packet = proto.Packet;
/// Operation tag carried inside a [`Packet`] (e.g. `Authenticate`, `Reply`).
const Operation = proto.Operation;
/// One logical change (insert/update/delete/commit/rollback), the atom that is
/// serialised into a replication frame and re-applied on the follower.
const LogRecord = @import("../common/common.zig").LogRecord;
/// The discriminant of a [`LogRecord`] (`insert`, `commit`, `rollback`, ...); read
/// in [`DurableReplicator.onRecord`] to decide when a batch is buffered vs shippable.
const OpKind = @import("../common/common.zig").OpKind;
/// The live database a follower applies received records into via `applyStream`.
const Database = @import("../schema.zig").Database;
/// The pure-Kyte/Zig TLS stack used for the optional mutual-TLS transport.
const tls = @import("tls");

/// Scoped logger; all replication diagnostics carry the `.replication` scope so
/// they can be filtered from the rest of the engine's output.
const log = std.log.scoped(.replication);

/// Paths to the certificate material for the optional mutual-TLS replication link.
///
/// TLS is engaged only when *all three* of CA, cert and key are present
/// ([`TlsConfig.isEnabled`]); a partially-filled config falls back to a plaintext
/// stream (still HMAC-authenticated). The same struct is used by both peers: on the
/// follower's [`ReplServer`] it drives a client-cert-required handshake, on the
/// primary's [`ReplClient`] it supplies the client identity presented.
pub const TlsConfig = struct {
    /// Path to the trusted CA bundle used to verify the peer's certificate. Empty
    /// disables TLS.
    ca_path: []const u8 = "",
    /// Path to this node's own certificate presented to the peer. Empty disables TLS.
    cert_path: []const u8 = "",
    /// Path to the private key matching [`TlsConfig.cert_path`]. Empty disables TLS.
    key_path: []const u8 = "",
    /// Expected server name for verification (SNI/hostname). When empty the primary
    /// falls back to the address it dialled (see [`ReplClient.upgradeTls`]).
    host: []const u8 = "",

    /// Reports whether TLS should be used, i.e. CA, cert and key are all configured.
    ///
    /// Deliberately all-or-nothing: a half-configured TLS block is treated as "off"
    /// rather than erroring, so an operator who leaves one path blank gets a working
    /// (plaintext, HMAC-only) link instead of a start-up failure.
    pub fn isEnabled(self: TlsConfig) bool {
        return self.ca_path.len > 0 and self.cert_path.len > 0 and self.key_path.len > 0;
    }
};

/// Seeds a fresh CSPRNG from the OS entropy source and returns a [`std.Random`] over it.
///
/// The TLS stack needs a random source for nonces and key material. `csprng` is an
/// out-parameter the caller must keep alive for as long as the returned `Random` is
/// used, because the `Random` borrows it; seeding uses [`std.Io.random`] so entropy
/// is drawn through the caller's I/O token rather than a global.
fn tlsRng(io: Io, csprng: *std.Random.DefaultCsprng) std.Random {
    var seed: [32]u8 = undefined;
    std.Io.random(io, &seed);
    csprng.* = std.Random.DefaultCsprng.init(seed);
    return csprng.random();
}

/// A bounds-checked cursor over an already-received byte slice.
///
/// Frames arrive fully buffered in memory, so decoding a [`LogRecord`] does not
/// need a streaming reader; this adapts a `[]const u8` to the minimal read interface
/// [`LogRecord.deserialize`] expects. It never allocates and never reads past
/// `buffer`, returning `error.EndOfStream` on a short read so a truncated or
/// corrupt frame is rejected rather than read out of bounds.
const FrameReader = struct {
    /// The complete frame payload being decoded.
    buffer: []const u8,
    /// Read offset into [`FrameReader.buffer`]; advances as fields are consumed.
    pos: usize = 0,

    /// Reads a fixed-width integer of type `T` in the given byte order, advancing `pos`.
    ///
    /// Returns `error.EndOfStream` if fewer than `@sizeOf(T)` bytes remain, which is
    /// how a truncated frame is caught mid-decode.
    pub fn readInt(r: *FrameReader, comptime T: type, endian: std.builtin.Endian) !T {
        const size = @sizeOf(T);
        if (r.pos + size > r.buffer.len) return error.EndOfStream;
        const value = std.mem.readInt(T, r.buffer[r.pos..][0..size], endian);
        r.pos += size;
        return value;
    }

    /// Copies exactly `buf.len` bytes out of the frame into `buf`, advancing `pos`.
    ///
    /// Returns `error.EndOfStream` if the remaining input is shorter than `buf`.
    pub fn readAll(r: *FrameReader, buf: []u8) !void {
        if (r.pos + buf.len > r.buffer.len) return error.EndOfStream;
        @memcpy(buf, r.buffer[r.pos..][0..buf.len]);
        r.pos += buf.len;
    }
};

/// The persisted "how far replication is confirmed" marker for the primary's WAL.
///
/// Written as a tiny JSON document next to the replication WAL, it records the point
/// up to which followers have confirmed receipt, so the primary knows which
/// [`ReplWal`] segments are safe to prune. Kept as JSON (not a binary frame) because
/// it is a single small control record read once at start-up, and human-inspectable
/// state is worth more here than a few bytes.
pub const CheckpointRecord = struct {
    /// The WAL file sequence number confirmed durable by followers; segments at or
    /// below this are eligible for deletion.
    file_seq: u64,
    /// The highest log-sequence number flushed at the time of the checkpoint,
    /// carried so recovery can resume the LSN counter.
    last_flushed_lsn: u64 = 0,
    /// On-disk filename of the checkpoint document within the WAL directory.
    const CHECKPOINT_FILENAME = "CHECKPOINT";

    /// Loads the checkpoint from `dir_path`, defaulting to a zeroed record if absent.
    ///
    /// A missing file (fresh node) and a path too long to format both yield
    /// `{ file_seq = 0, last_flushed_lsn = 0 }` rather than an error, so a first
    /// start is not treated as a failure. Any other I/O error propagates. The parsed
    /// value owns no borrowed slices, so it is safe to return after freeing `data`.
    pub fn load(allocator: Allocator, io: Io, dir_path: []const u8) !CheckpointRecord {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const checkpoint_path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, CHECKPOINT_FILENAME }) catch return CheckpointRecord{ .file_seq = 0, .last_flushed_lsn = 0 };
        const data = Io.Dir.readFileAlloc(.cwd(), io, checkpoint_path, allocator, .unlimited) catch |err| {
            if (err == error.FileNotFound) return CheckpointRecord{ .file_seq = 0, .last_flushed_lsn = 0 };
            return err;
        };
        defer allocator.free(data);
        const parsed = try std.json.parseFromSlice(CheckpointRecord, allocator, data, .{});
        defer parsed.deinit();
        return parsed.value;
    }

    /// Atomically persists this checkpoint by writing a temp file, `fsync`ing it,
    /// then renaming it over the live `CHECKPOINT`.
    ///
    /// The write-temp-then-rename dance guarantees a reader never observes a
    /// half-written checkpoint: a crash mid-write leaves either the old complete
    /// file or the new complete file, never a torn one. The explicit
    /// [`Io.File.sync`] before the rename is what makes that guarantee hold across a
    /// power loss, not just a process crash.
    pub fn save(self: CheckpointRecord, io: Io, dir_path: []const u8) !void {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&path_buf, "{s}/CHECKPOINT.tmp", .{dir_path});

        var buf: [128]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "{{\"file_seq\":{}, \"last_flushed_lsn\":{}}}", .{ self.file_seq, self.last_flushed_lsn });

        var file = try Io.Dir.createFile(.cwd(), io, tmp_path, .{});
        defer file.close(io);
        try file.writeStreamingAll(io, data);
        try file.sync(io);

        var final_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const final_path = try std.fmt.bufPrint(&final_path_buf, "{s}/{s}", .{ dir_path, CHECKPOINT_FILENAME });
        try Io.Dir.rename(.cwd(), tmp_path, .cwd(), final_path, io);
    }
};

/// A segmented, self-rotating write-ahead log dedicated to replication frames.
///
/// Separate from the storage engine's own durability WAL, this log records the
/// serialised change stream so followers can be re-fed after a disconnect. Records
/// are length-framed (`u32` little-endian length + payload) and appended into
/// numbered segment files (`NNNNNN.rwal`); a segment rotates when it hits
/// [`ReplWal.max_file_size`] or [`ReplWal.sync_interval_ms`] elapses. Appends are
/// staged in an in-memory [`ReplWal.buffer`] and flushed (write + `fsync`) in bulk,
/// trading a small window of un-synced data for far fewer syscalls; the caller is
/// responsible for calling [`ReplWal.flush`] at the durability points it needs.
pub const ReplWal = struct {
    /// Allocator owning the heap `ReplWal`, the duped `dir_path`, and the buffer.
    allocator: Allocator,
    /// I/O token for all file operations.
    io: Io,
    /// Owned copy of the directory holding the `.rwal` segments and checkpoint.
    dir_path: []const u8,

    /// Sequence number of the segment currently being written; also the number
    /// stamped into its filename.
    current_seq: u64,
    /// The open segment file, or `null` before the first append / after a rotate.
    current_file: ?Io.File,
    /// Logical size of the current segment including buffered-but-unflushed bytes;
    /// used both for rotation thresholds and to compute the flush write offset.
    file_size: u64,
    /// Rotation threshold: once [`ReplWal.file_size`] reaches this, the next
    /// [`ReplWal.shouldRotate`] returns true.
    max_file_size: u64,
    /// Time-based rotation threshold in milliseconds since the last rotate.
    sync_interval_ms: u64,
    /// Wall-clock time (ms) of the last rotate, the baseline for the time threshold.
    last_rotate_time: i64 = 0,
    /// In-memory staging buffer for appended frames, flushed to disk in bulk.
    buffer: std.ArrayList(u8),
    /// Count of records written into the current segment (reset on rotate).
    record_count: u32,
    /// Highest LSN observed, propagated into the [`CheckpointRecord`] on checkpoint.
    last_lsn: u64 = 0,

    /// Opens (or creates) the replication WAL in `dir_path`, resuming the newest
    /// existing segment.
    ///
    /// Scans the directory for `*.rwal` files, parses their numeric names, and
    /// continues appending to the highest-numbered segment (re-opening it read-write
    /// and adopting its on-disk size) so a restart does not orphan an in-progress
    /// segment. A fresh directory starts at sequence 1. Returns a heap-allocated
    /// `ReplWal` the caller must release with [`ReplWal.deinit`]. Directory-scan
    /// failures are swallowed (treated as "no existing segments") so a transient
    /// listing error degrades to starting fresh rather than refusing to start.
    pub fn init(allocator: Allocator, io: Io, dir_path: []const u8, max_file_size: u64, sync_interval_ms: u64) !*ReplWal {
        Io.Dir.createDirPath(.cwd(), io, dir_path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        var max_seq: u64 = 0;
        var found_file = false;

        if (Io.Dir.openDir(.cwd(), io, dir_path, .{ .iterate = true })) |dir| {
            var wal_dir = dir;
            defer wal_dir.close(io);
            var dir_iter = wal_dir.iterate();
            while (dir_iter.next(io) catch null) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".rwal")) {
                    const seq_str = entry.name[0 .. entry.name.len - 5];
                    const seq = std.fmt.parseUnsigned(u64, seq_str, 10) catch continue;
                    if (!found_file or seq > max_seq) {
                        max_seq = seq;
                    }
                    found_file = true;
                }
            }
        } else |_| {}

        const self = try allocator.create(ReplWal);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .dir_path = try allocator.dupe(u8, dir_path),
            .current_seq = if (found_file) max_seq else 1,
            .current_file = null,
            .file_size = 0,
            .max_file_size = max_file_size,
            .sync_interval_ms = sync_interval_ms,
            .buffer = std.ArrayList(u8).empty,
            .record_count = 0,
        };

        self.last_rotate_time = std.Io.Clock.now(.real, io).toMilliseconds();

        if (found_file) {
            const file_path = try self.getFilePath(max_seq);
            defer allocator.free(file_path);
            if (Io.Dir.openFile(.cwd(), io, file_path, .{ .mode = .read_write })) |file| {
                if (file.stat(io)) |stat| {
                    self.current_file = file;
                    self.file_size = stat.size;
                } else |_| {
                    file.close(io);
                }
            } else |_| {}
        }

        return self;
    }

    /// Flushes any buffered records, closes the segment, and frees the `ReplWal`.
    ///
    /// The final [`ReplWal.flush`] is best-effort (`catch {}`): during teardown a
    /// flush failure cannot be usefully handled, and a lost tail is recoverable
    /// because followers re-request gaps. After this call the pointer is invalid.
    pub fn deinit(self: *ReplWal) void {
        self.flush() catch {};
        if (self.current_file) |file| {
            file.close(self.io);
            self.current_file = null;
        }
        self.buffer.deinit(self.allocator);
        self.allocator.free(self.dir_path);
        self.allocator.destroy(self);
    }

    /// Records that followers have confirmed up to `confirmed_seq` by writing a
    /// durable [`CheckpointRecord`].
    ///
    /// This is what later authorises pruning of segments below `confirmed_seq`;
    /// the current [`ReplWal.last_lsn`] is folded in so recovery can restore the
    /// LSN counter.
    pub fn checkpoint(self: *ReplWal, confirmed_seq: u64) !void {
        const cp = CheckpointRecord{
            .file_seq = confirmed_seq,
            .last_flushed_lsn = self.last_lsn,
        };
        try cp.save(self.io, self.dir_path);
    }

    /// Reads back the persisted [`CheckpointRecord`], or a zeroed one if none exists.
    pub fn loadCheckpoint(self: *ReplWal) !CheckpointRecord {
        return CheckpointRecord.load(self.allocator, self.io, self.dir_path);
    }

    /// Appends one length-framed record into the current segment's staging buffer.
    ///
    /// Opens a segment lazily on the first append. The frame is `u32` length +
    /// payload. To bound memory the buffer is flushed to disk when it would exceed
    /// 256 KiB, so a burst of appends cannot grow the buffer without limit.
    /// [`ReplWal.file_size`] is advanced by the whole frame immediately (before the
    /// flush) because it also drives rotation decisions, which must account for
    /// buffered bytes.
    pub fn append(self: *ReplWal, data: []const u8) !void {
        if (self.current_file == null) {
            try self.openNewFile();
        }

        const frame_size: u64 = 4 + data.len;

        if (self.buffer.items.len + frame_size > 256 * 1024) {
            try self.flush();
        }

        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, @intCast(data.len), .little);
        try self.buffer.appendSlice(self.allocator, &len_buf);
        try self.buffer.appendSlice(self.allocator, data);

        self.file_size += frame_size;
        self.record_count += 1;
    }

    /// Writes the staged buffer to disk at the correct offset and `fsync`s it.
    ///
    /// The write offset is `file_size - buffer.len`: because [`ReplWal.file_size`]
    /// was advanced eagerly on each [`ReplWal.append`], the buffered bytes belong at
    /// the position just before the current logical end. A no-op when the buffer is
    /// empty or no segment is open. After the `fsync` the buffer's capacity is kept
    /// for reuse so steady-state appends do not re-allocate.
    pub fn flush(self: *ReplWal) !void {
        if (self.buffer.items.len == 0) return;
        const file = self.current_file orelse return;

        const write_offset = self.file_size - self.buffer.items.len;
        try file.writePositionalAll(self.io, self.buffer.items, write_offset);
        try file.sync(self.io);
        self.buffer.clearRetainingCapacity();
    }

    /// Decides whether the current segment should be rotated now.
    ///
    /// True when the segment has reached [`ReplWal.max_file_size`], or when at least
    /// [`ReplWal.sync_interval_ms`] has elapsed since the last rotate *and* the
    /// segment holds data. The time-based arm bounds how long a low-traffic primary
    /// leaves a segment open, so retention/pruning granularity does not stall.
    /// Always false when no segment is open.
    pub fn shouldRotate(self: *ReplWal) bool {
        if (self.current_file == null) return false;
        if (self.file_size >= self.max_file_size) return true;
        const elapsed = @max(0, std.Io.Clock.now(.real, self.io).toMilliseconds() - self.last_rotate_time);
        return elapsed >= self.sync_interval_ms and self.file_size > 0;
    }

    /// Closes the active segment and advances to the next, returning the sequence
    /// number of the segment just closed.
    ///
    /// Flushes first so no buffered records are lost across the boundary, then bumps
    /// [`ReplWal.current_seq`] and resets the per-segment counters. The returned
    /// sealed-segment number is what a caller checkpoints/prunes against; the next
    /// [`ReplWal.append`] lazily opens the new segment file.
    pub fn rotate(self: *ReplWal) !u64 {
        try self.flush();

        const rotated_seq = self.current_seq;

        if (self.current_file) |file| {
            file.close(self.io);
            self.current_file = null;
        }

        self.current_seq += 1;
        self.file_size = 0;
        self.record_count = 0;
        self.last_rotate_time = std.Io.Clock.now(.real, self.io).toMilliseconds();

        return rotated_seq;
    }

    /// Reads a sealed segment back and splits it into its constituent frames.
    ///
    /// Returns a freshly allocated slice of frame slices (each duped from the file
    /// data) owned by `allocator`, the caller frees each frame and the outer slice.
    /// A trailing partial frame (length header present but body truncated, e.g. a
    /// crash mid-flush) is silently ignored via the `break`, so a torn tail does not
    /// poison replay of the intact records before it.
    pub fn readFile(self: *ReplWal, seq: u64, allocator: Allocator) ![][]u8 {
        const file_path = try self.getFilePath(seq);
        defer self.allocator.free(file_path);

        const file = try Io.Dir.openFile(.cwd(), self.io, file_path, .{});
        defer file.close(self.io);

        const stat = try file.stat(self.io);
        if (stat.size == 0) return try allocator.alloc([]u8, 0);

        const file_data = try allocator.alloc(u8, stat.size);
        defer allocator.free(file_data);
        _ = try file.readPositionalAll(self.io, file_data, 0);

        var frames = std.ArrayList([]u8).empty;
        defer frames.deinit(allocator);
        var pos: usize = 0;

        while (pos + 4 <= file_data.len) {
            const frame_len = std.mem.readInt(u32, file_data[pos..][0..4], .little);
            pos += 4;
            if (pos + frame_len > file_data.len) break;
            const frame = try allocator.dupe(u8, file_data[pos..][0..frame_len]);
            try frames.append(allocator, frame);
            pos += frame_len;
        }

        return try frames.toOwnedSlice(allocator);
    }

    /// Deletes the segment file for `seq`, used to prune history confirmed durable.
    pub fn deleteFile(self: *ReplWal, seq: u64) !void {
        const file_path = try self.getFilePath(seq);
        defer self.allocator.free(file_path);
        try Io.Dir.deleteFile(.cwd(), self.io, file_path);
    }

    /// Creates and opens the segment file for the current sequence number.
    ///
    /// Opened with `truncate = false` so re-opening an existing partially-written
    /// segment (on resume) preserves its content; the size is reset to 0 here and
    /// the caller's resume path adjusts it. Called lazily from [`ReplWal.append`].
    fn openNewFile(self: *ReplWal) !void {
        const file_path = try self.getFilePath(self.current_seq);
        defer self.allocator.free(file_path);
        self.current_file = try Io.Dir.createFile(.cwd(), self.io, file_path, .{ .read = true, .truncate = false });
        self.file_size = 0;
        self.last_rotate_time = std.Io.Clock.now(.real, self.io).toMilliseconds();
    }

    /// Formats the path of segment `seq` as `<dir>/NNNNNN.rwal` (6-digit zero-padded).
    ///
    /// The zero-padding keeps lexical order equal to numeric order, which the
    /// directory scan in [`ReplWal.init`] relies on. Caller owns and frees the result.
    fn getFilePath(self: *ReplWal, seq: u64) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}/{d:0>6}.rwal", .{ self.dir_path, seq });
    }
};

/// A durable, batch-granular history of shipped replication batches, used to catch
/// up a follower that has fallen further behind than the in-memory ring retains.
///
/// Where [`DurableReplicator.sent`] is a bounded RAM ring, this is the on-disk
/// backstop: every batch is appended as a self-describing record (magic + `base` +
/// `last` + frame count + framed frames) into a single append-only file, with an
/// in-memory [`BackfillLog.index`] of `(base, last, offset, size)` so a
/// catch-up read can seek straight to the batches above a follower's confirmed
/// point without rescanning. Old records are compacted away in [`BackfillLog.checkpoint`]
/// once every follower has confirmed past them. The magic word per record lets
/// [`BackfillLog.scan`] validate framing on recovery and stop at the first torn
/// record rather than misreading a truncated tail.
pub const BackfillLog = struct {
    /// Allocator owning the struct, the duped `dir_path`, and the index.
    allocator: Allocator,
    /// I/O token for file operations.
    io: Io,
    /// Owned copy of the directory that holds the backfill file.
    dir_path: []const u8,
    /// The single append-only backfill file.
    file: Io.File,
    /// Byte offset one past the last valid record; where the next append lands.
    end_offset: u64,
    /// In-memory directory of records (one [`BackfillLog.Entry`] per batch), kept in
    /// append order so index 0 is the oldest retained batch.
    index: std.ArrayList(Entry),

    /// Record magic (`"BLOG"` little-endian) prefixing every batch on disk; a
    /// mismatch during [`BackfillLog.scan`] marks the end of valid data.
    const MAGIC: u32 = 0x424C4F47;
    /// Fixed filename of the backfill log within [`BackfillLog.dir_path`].
    const FILENAME = "backfill.blog";
    /// Byte length of a record header: magic(4) + base(8) + last(8) + nframes(4).
    const HEADER_LEN: usize = 4 + 8 + 8 + 4;

    /// One entry in the in-memory index locating a batch record in the file.
    const Entry = struct {
        /// Sequence number of the batch's first frame.
        base: u64,
        /// Sequence number of the batch's last frame.
        last: u64,
        /// Byte offset of the record within [`BackfillLog.file`].
        offset: u64,
        /// Total on-disk byte length of the record (header + all framed frames).
        size: u64,
    };

    /// A batch decoded out of the backfill log, owning its frame buffers.
    ///
    /// Returned by [`BackfillLog.batchesFrom`]; the caller must call
    /// [`OwnedBatch.deinit`] to release the frames and the outer slice.
    pub const OwnedBatch = struct {
        /// Sequence number of the first frame in [`OwnedBatch.frames`].
        base: u64,
        /// Sequence number of the last frame.
        last: u64,
        /// The decoded frame payloads, each individually allocated.
        frames: [][]u8,

        /// Frees every frame buffer and the containing slice.
        pub fn deinit(self: OwnedBatch, allocator: Allocator) void {
            for (self.frames) |fr| allocator.free(fr);
            allocator.free(self.frames);
        }
    };

    /// Opens (creating if needed) the backfill log in `dir_path` and rebuilds its
    /// index by scanning existing records.
    ///
    /// The file is opened append-preserving (`truncate = false`) so a restart keeps
    /// prior history. [`BackfillLog.scan`] populates the index and sets
    /// `end_offset` to just past the last intact record, so a torn tail from a crash
    /// mid-append is effectively discarded on the next write. Returns a heap pointer
    /// released by [`BackfillLog.deinit`]; the `errdefer`s unwind partial state if
    /// the scan fails.
    pub fn init(allocator: Allocator, io: Io, dir_path: []const u8) !*BackfillLog {
        Io.Dir.createDirPath(.cwd(), io, dir_path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };
        const self = try allocator.create(BackfillLog);
        errdefer allocator.destroy(self);
        const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir_path, FILENAME });
        defer allocator.free(path);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .dir_path = try allocator.dupe(u8, dir_path),
            .file = try Io.Dir.createFile(.cwd(), io, path, .{ .read = true, .truncate = false }),
            .end_offset = 0,
            .index = std.ArrayList(Entry).empty,
        };
        errdefer {
            self.file.close(io);
            allocator.free(self.dir_path);
        }
        try self.scan();
        return self;
    }

    /// Closes the file and frees the log and its index.
    pub fn deinit(self: *BackfillLog) void {
        self.file.close(self.io);
        self.index.deinit(self.allocator);
        self.allocator.free(self.dir_path);
        self.allocator.destroy(self);
    }

    /// Rebuilds [`BackfillLog.index`] by walking the file record-by-record.
    ///
    /// Reads the whole file once, then for each record validates the [`MAGIC`],
    /// reads the header, and walks the declared frame count to compute the record's
    /// end. The loop `break`s (rather than errors) at the first record whose magic is
    /// wrong or whose frames run past the buffer, so a partially-written trailing
    /// record left by a crash is treated as "not there": `end_offset` is set to the
    /// end of the last *complete* record, and the next append overwrites the torn tail.
    fn scan(self: *BackfillLog) !void {
        const stat = try self.file.stat(self.io);
        if (stat.size == 0) return;
        const data = try self.allocator.alloc(u8, stat.size);
        defer self.allocator.free(data);
        _ = try self.file.readPositionalAll(self.io, data, 0);
        var pos: usize = 0;
        while (pos + HEADER_LEN <= data.len) {
            const magic = std.mem.readInt(u32, data[pos..][0..4], .little);
            if (magic != MAGIC) break;
            const base = std.mem.readInt(u64, data[pos + 4 ..][0..8], .little);
            const last = std.mem.readInt(u64, data[pos + 12 ..][0..8], .little);
            const nframes = std.mem.readInt(u32, data[pos + 20 ..][0..4], .little);
            var p = pos + HEADER_LEN;
            var ok = true;
            var i: u32 = 0;
            while (i < nframes) : (i += 1) {
                if (p + 4 > data.len) {
                    ok = false;
                    break;
                }
                const flen = std.mem.readInt(u32, data[p..][0..4], .little);
                p += 4;
                if (p + flen > data.len) {
                    ok = false;
                    break;
                }
                p += flen;
            }
            if (!ok) break;
            try self.index.append(self.allocator, .{ .base = base, .last = last, .offset = pos, .size = p - pos });
            pos = p;
        }
        self.end_offset = pos;
    }

    /// Serialises and appends one batch (frames spanning `base..last`) to the log.
    ///
    /// Builds the whole record in memory (magic, base, last, frame count, then each
    /// framed frame), writes it at `end_offset`, `fsync`s, and records its
    /// [`BackfillLog.Entry`]. `fsync` here is what makes the backfill genuinely
    /// durable, which is the whole point of having it in addition to the RAM ring.
    pub fn append(self: *BackfillLog, base: u64, last: u64, frames: []const []const u8) !void {
        var out = std.Io.Writer.Allocating.init(self.allocator);
        defer out.deinit();
        const w = &out.writer;
        try w.writeInt(u32, MAGIC, .little);
        try w.writeInt(u64, base, .little);
        try w.writeInt(u64, last, .little);
        try w.writeInt(u32, @intCast(frames.len), .little);
        for (frames) |fr| {
            try w.writeInt(u32, @intCast(fr.len), .little);
            try w.writeAll(fr);
        }
        const bytes = out.written();
        try self.file.writePositionalAll(self.io, bytes, self.end_offset);
        try self.file.sync(self.io);
        try self.index.append(self.allocator, .{ .base = base, .last = last, .offset = self.end_offset, .size = bytes.len });
        self.end_offset += bytes.len;
    }

    /// Returns the `base` sequence of the oldest retained batch, or `null` if empty.
    ///
    /// Used by [`DurableReplicator.reconnectAndCatchUp`] to decide whether the
    /// backfill reaches back far enough to cover a follower's confirmed point.
    pub fn earliestBase(self: *BackfillLog) ?u64 {
        if (self.index.items.len == 0) return null;
        return self.index.items[0].base;
    }

    /// Number of batches currently retained on disk.
    pub fn batchCount(self: *BackfillLog) usize {
        return self.index.items.len;
    }

    /// Decodes every retained batch whose `last > confirmed`, i.e. everything a
    /// follower at `confirmed` still needs.
    ///
    /// Reads each qualifying record from disk and parses it via
    /// [`BackfillLog.parseRecord`]. Returns an owned slice of [`OwnedBatch`]; on any
    /// error the `errdefer` releases the batches decoded so far so nothing leaks.
    /// The caller frees each batch and the slice.
    pub fn batchesFrom(self: *BackfillLog, confirmed: u64, allocator: Allocator) ![]OwnedBatch {
        var list = std.ArrayList(OwnedBatch).empty;
        errdefer {
            for (list.items) |b| b.deinit(allocator);
            list.deinit(allocator);
        }
        for (self.index.items) |e| {
            if (e.last <= confirmed) continue;
            const buf = try self.allocator.alloc(u8, e.size);
            defer self.allocator.free(buf);
            _ = try self.file.readPositionalAll(self.io, buf, e.offset);
            const batch = try parseRecord(buf, e.base, allocator);
            try list.append(allocator, batch);
        }
        return list.toOwnedSlice(allocator);
    }

    /// Decodes a single on-disk backfill record into an [`OwnedBatch`].
    ///
    /// Validates the magic and that the record's stored `base` equals `expect_base`
    /// (the index entry it was located by), guarding against a corrupt or misindexed
    /// record; any framing overrun yields `error.CorruptBackfill`. The `errdefer`
    /// frees the frames decoded before a mid-record failure so a corrupt tail frame
    /// does not leak the good ones already allocated.
    fn parseRecord(buf: []const u8, expect_base: u64, allocator: Allocator) !OwnedBatch {
        if (buf.len < HEADER_LEN) return error.CorruptBackfill;
        if (std.mem.readInt(u32, buf[0..4], .little) != MAGIC) return error.CorruptBackfill;
        const base = std.mem.readInt(u64, buf[4..12], .little);
        const last = std.mem.readInt(u64, buf[12..20], .little);
        if (base != expect_base) return error.CorruptBackfill;
        const nframes = std.mem.readInt(u32, buf[20..24], .little);
        const frames = try allocator.alloc([]u8, nframes);
        var filled: usize = 0;
        errdefer {
            for (frames[0..filled]) |fr| allocator.free(fr);
            allocator.free(frames);
        }
        var p: usize = HEADER_LEN;
        var i: u32 = 0;
        while (i < nframes) : (i += 1) {
            if (p + 4 > buf.len) return error.CorruptBackfill;
            const flen = std.mem.readInt(u32, buf[p..][0..4], .little);
            p += 4;
            if (p + flen > buf.len) return error.CorruptBackfill;
            frames[i] = try allocator.dupe(u8, buf[p..][0..flen]);
            filled = i + 1;
            p += flen;
        }
        return OwnedBatch{ .base = base, .last = last, .frames = frames };
    }

    /// Compacts the log, dropping every batch fully confirmed at or below `seq`.
    ///
    /// Finds the first batch to keep (the first whose `last > seq`). If nothing can
    /// be dropped it returns immediately. If *everything* can be dropped it truncates
    /// the file to empty. Otherwise it copies the surviving tail to the front by
    /// rewriting the file (truncate + write tail + `fsync`) and rebuilds the index
    /// with offsets shifted by the removed prefix. Rewriting whole rather than
    /// in-place keeps the on-disk layout contiguous from offset 0, which
    /// [`BackfillLog.scan`] and [`BackfillLog.batchesFrom`] assume. The old index is
    /// only freed after the new one is fully built, so a mid-build failure leaves the
    /// original index intact.
    pub fn checkpoint(self: *BackfillLog, seq: u64) !void {
        var keep_from: usize = 0;
        while (keep_from < self.index.items.len and self.index.items[keep_from].last <= seq) keep_from += 1;
        if (keep_from == 0) return;

        const path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.dir_path, FILENAME });
        defer self.allocator.free(path);

        if (keep_from >= self.index.items.len) {
            self.file.close(self.io);
            self.file = try Io.Dir.createFile(.cwd(), self.io, path, .{ .read = true, .truncate = true });
            self.index.clearRetainingCapacity();
            self.end_offset = 0;
            return;
        }

        const shift = self.index.items[keep_from].offset;
        const tail_len = self.end_offset - shift;
        const tail = try self.allocator.alloc(u8, tail_len);
        defer self.allocator.free(tail);
        _ = try self.file.readPositionalAll(self.io, tail, shift);

        self.file.close(self.io);
        self.file = try Io.Dir.createFile(.cwd(), self.io, path, .{ .read = true, .truncate = true });
        try self.file.writePositionalAll(self.io, tail, 0);
        try self.file.sync(self.io);

        var new_index = std.ArrayList(Entry).empty;
        errdefer new_index.deinit(self.allocator);
        for (self.index.items[keep_from..]) |e| {
            try new_index.append(self.allocator, .{ .base = e.base, .last = e.last, .offset = e.offset - shift, .size = e.size });
        }
        self.index.deinit(self.allocator);
        self.index = new_index;
        self.end_offset = tail_len;
    }
};

/// Owns all the long-lived TLS state for one client-side connection.
///
/// Heap-allocated as a unit (rather than living inline in [`ReplClient`]) because
/// the TLS reader/writer and connection hold *pointers* into their own buffers and
/// into the cert/CA structures: a move would dangle them, so they must sit at a
/// stable address. Every `undefined` field is fully initialised in
/// [`ReplClient.upgradeTls`] before use and torn down in [`ReplClient.disconnect`].
const TlsClientHolder = struct {
    /// Reader over the raw TCP socket, feeding ciphertext to the TLS engine.
    raw_r: net.Stream.Reader,
    /// Writer over the raw TCP socket, draining ciphertext from the TLS engine.
    raw_w: net.Stream.Writer,
    /// Input buffer backing [`TlsClientHolder.raw_r`]; sized by the TLS record limit.
    raw_rbuf: [tls.input_buffer_len]u8 = undefined,
    /// Output buffer backing [`TlsClientHolder.raw_w`].
    raw_wbuf: [tls.output_buffer_len]u8 = undefined,
    /// The established TLS connection carrying application data.
    conn: tls.Connection = undefined,
    /// This client's certificate + key presented for mutual authentication.
    client_ck: tls.config.CertKeyPair = undefined,
    /// The trusted CA bundle used to verify the follower's server certificate.
    ca_bundle: tls.config.cert.Bundle = undefined,
    /// Per-connection CSPRNG seeded in [`ReplClient.upgradeTls`]; must outlive the
    /// [`std.Random`] handed to the TLS config.
    csprng: std.Random.DefaultCsprng = undefined,
};

/// The primary side of the replication link: dials a follower, optionally upgrades
/// to TLS, authenticates, and ships framed batches.
///
/// Wraps a single outbound connection. Two framing styles coexist for historical
/// reasons: the [`Packet`]-based `ship`/`readReply` path used for the legacy
/// authenticate handshake, and the length-prefixed [`proto.ReplFrames`] path
/// ([`ReplClient.shipFrames`]) used for the actual change stream. When TLS is active
/// all traffic goes through [`ReplClient.tls`]; otherwise through the plaintext
/// [`ReplClient.stream`]. This type is normally driven by [`DurableReplicator`],
/// which owns retry/catch-up policy on top of it.
pub const ReplClient = struct {
    /// Allocator for connection state and transient buffers.
    allocator: Allocator,
    /// I/O token for socket and TLS operations.
    io: Io,
    /// The connected TCP socket, or `null` when disconnected.
    stream: ?net.Stream = null,
    /// TLS material; when [`TlsConfig.isEnabled`] the link is upgraded to mutual-TLS.
    tls_config: TlsConfig = .{},
    /// Reserved hostname for verification; currently unused by the active paths.
    tls_host: []const u8 = "",
    /// The active TLS session, or `null` on a plaintext (or not-yet-upgraded) link.
    tls: ?*TlsClientHolder = null,

    /// Constructs a disconnected client bound to `allocator` and `io`.
    ///
    /// No socket is opened until [`ReplClient.connect`]/[`ReplClient.connectRaw`];
    /// set [`ReplClient.tls_config`] before connecting to enable TLS.
    pub fn init(allocator: Allocator, io: Io) ReplClient {
        return .{
            .allocator = allocator,
            .io = io,
        };
    }

    /// Tears down any live connection and TLS state.
    pub fn deinit(self: *ReplClient) void {
        self.disconnect();
    }

    /// Upgrades the already-connected plaintext socket to mutual-TLS in place.
    ///
    /// A no-op when TLS is not configured, so callers can invoke it unconditionally.
    /// Loads this node's cert/key and the CA bundle, seeds a per-connection RNG, and
    /// performs the client handshake, verifying the server against `host` (or
    /// [`TlsConfig.host`] when set). On success [`ReplClient.tls`] is populated; the
    /// cascade of `errdefer`s frees the holder, cert and CA bundle if any step fails
    /// so a failed upgrade leaks nothing. Requires an established
    /// [`ReplClient.stream`] (`error.NotConnected` otherwise).
    pub fn upgradeTls(self: *ReplClient, host: []const u8) !void {
        if (!self.tls_config.isEnabled()) return;
        const stream = self.stream orelse return error.NotConnected;
        const h = try self.allocator.create(TlsClientHolder);
        errdefer self.allocator.destroy(h);
        h.raw_r = stream.reader(self.io, &h.raw_rbuf);
        h.raw_w = stream.writer(self.io, &h.raw_wbuf);
        h.client_ck = try tls.config.CertKeyPair.fromFilePath(self.allocator, self.io, Io.Dir.cwd(), self.tls_config.cert_path, self.tls_config.key_path);
        errdefer h.client_ck.deinit(self.allocator);
        h.ca_bundle = try tls.config.cert.fromFilePath(self.allocator, self.io, Io.Dir.cwd(), self.tls_config.ca_path);
        errdefer h.ca_bundle.deinit(self.allocator);
        const rng = tlsRng(self.io, &h.csprng);
        const vhost = if (self.tls_config.host.len > 0) self.tls_config.host else host;
        const opts = tls.config.Client{
            .rng = rng,
            .now = Io.Clock.real.now(self.io),
            .host = vhost,
            .root_ca = h.ca_bundle,
            .auth = &h.client_ck,
        };
        h.conn = try tls.client(&h.raw_r.interface, &h.raw_w.interface, opts);
        self.tls = h;
    }

    /// Connects if needed, then writes each frame and waits for its reply, one at a
    /// time.
    ///
    /// This is the legacy [`Packet`]-framed shipping path (each frame is a full
    /// request answered by a [`ReplClient.readReply`]). Any write or read failure
    /// disconnects the client (so the next call reconnects cleanly) and propagates
    /// the error. Honours cooperative cancellation: if `is_running` goes false it
    /// returns `error.Canceled` instead of blocking on a dead peer.
    pub fn ship(self: *ReplClient, config: *const Config, frames: [][]const u8, is_running: *const bool) !void {
        try self.ensureConnected(config, is_running);
        if (!is_running.*) return error.Canceled;
        for (frames) |frame| {
            self.writeFramed(frame) catch |err| {
                log.err("ship: write failed: {any}. Disconnecting...", .{err});
                self.disconnect();
                return err;
            };
            self.readReply() catch |err| {
                log.err("ship: read reply failed: {any}. Disconnecting...", .{err});
                self.disconnect();
                return err;
            };
        }
    }

    /// Opens a bare TCP connection to `host:port` with bounded connect retries, no
    /// auth and no TLS.
    ///
    /// Retries a refused/failed connect up to 100 times with a 20 ms pause (the
    /// follower may still be binding its listener during a coordinated restart),
    /// then surfaces the last error. On POSIX it sets 3-second send/receive socket
    /// timeouts so a wedged peer cannot block a shipping coroutine forever; those
    /// `setsockopt`s are best-effort and skipped on Windows (see the module note in
    /// `CLAUDE.md`). Idempotent: returns immediately if already connected. Callers
    /// layer [`ReplClient.upgradeTls`] + [`ReplClient.authenticateStream`] on top.
    pub fn connectRaw(self: *ReplClient, host: []const u8, port: u16) !void {
        if (self.stream != null) return;
        const addr = try net.IpAddress.parse(host, port);
        var attempts: u32 = 0;
        while (true) {
            const stream = addr.connect(self.io, .{ .mode = .stream, .protocol = .tcp }) catch |err| {
                attempts += 1;
                if (attempts >= 100) return err;
                _ = self.io.sleep(std.Io.Duration.fromMilliseconds(20), .real) catch {};
                continue;
            };
            if (builtin.os.tag != .windows) {
                const tv = std.posix.timeval{ .sec = 3, .usec = 0 };
                std.posix.setsockopt(stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.RCVTIMEO, std.mem.asBytes(&tv)) catch {};
                std.posix.setsockopt(stream.socket.handle, std.posix.SOL.SOCKET, std.posix.SO.SNDTIMEO, std.mem.asBytes(&tv)) catch {};
            }
            self.stream = stream;
            return;
        }
    }

    /// Ships one [`proto.ReplFrames`] batch and reads back the follower's fixed
    /// 16-byte [`proto.ReplAck`].
    ///
    /// This is the primary change-stream path: it writes a `u32` little-endian length
    /// prefix, the serialised payload, then blocks for exactly the ack. Two transport
    /// branches share the framing: over TLS through [`ReplClient.tls`], otherwise
    /// over the plaintext stream with a flushed buffered writer. A short ack read is
    /// `error.EndOfStream`. Unlike [`ReplClient.ship`] this does not auto-reconnect;
    /// the caller ([`DurableReplicator`]) decides how to react to a failure.
    pub fn shipFrames(self: *ReplClient, frames: proto.ReplFrames) !proto.ReplAck {
        const payload = try frames.serialize(self.allocator);
        defer self.allocator.free(payload);
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, @intCast(payload.len), .little);
        var ack_bytes: [16]u8 = undefined;

        if (self.tls) |h| {
            try h.conn.writeAll(&len_bytes);
            try h.conn.writeAll(payload);
            const n = try h.conn.readAll(&ack_bytes);
            if (n < ack_bytes.len) return error.EndOfStream;
            return proto.ReplAck.deserialize(&ack_bytes);
        }

        const stream = self.stream orelse return error.NotConnected;
        var write_buf: [8192]u8 = undefined;
        var w = stream.writer(self.io, &write_buf);
        try w.interface.writeAll(&len_bytes);
        try w.interface.writeAll(payload);
        try w.interface.flush();

        var read_buf: [64]u8 = undefined;
        var r = stream.reader(self.io, &read_buf);
        try r.interface.readSliceAll(&ack_bytes);
        return proto.ReplAck.deserialize(&ack_bytes);
    }

    /// Performs the client half of the HMAC-SHA256 challenge-response over the
    /// current connection.
    ///
    /// A no-op when `key` is empty (auth disabled). Reads the server's 32-byte nonce,
    /// replies with `HMAC(nonce, key)`, and expects a single `0x01` accept byte;
    /// anything else is `error.AuthFailed`. The TLS branch runs the same protocol
    /// directly over the encrypted [`ReplClient.tls`] connection, the plaintext
    /// branch delegates to [`clientAuthenticate`]. Proves shared-secret possession
    /// without ever sending the key.
    pub fn authenticateStream(self: *ReplClient, key: []const u8) !void {
        if (key.len == 0) return;
        if (self.tls) |h| {
            var nonce: [32]u8 = undefined;
            if (try h.conn.readAll(&nonce) < nonce.len) return error.AuthFailed;
            var tag: [32]u8 = undefined;
            HmacSha256.create(&tag, &nonce, key);
            try h.conn.writeAll(&tag);
            var ackb: [1]u8 = undefined;
            if (try h.conn.readAll(&ackb) < 1 or ackb[0] != 0x01) return error.AuthFailed;
            return;
        }
        const stream = self.stream orelse return error.NotConnected;
        var read_buf: [64]u8 = undefined;
        var write_buf: [64]u8 = undefined;
        var rr = stream.reader(self.io, &read_buf);
        var ww = stream.writer(self.io, &write_buf);
        try clientAuthenticate(&rr.interface, &ww.interface, key);
    }

    /// Ships a batch and folds the returned ack's `confirmed_seq` into `tracker`
    /// under `follower_id`.
    ///
    /// The convenience seam that keeps [`QuorumTracker`] progress in step with every
    /// send, so a later [`QuorumTracker.awaitQuorum`] sees this follower's advance.
    pub fn shipAndRecord(self: *ReplClient, frames: proto.ReplFrames, tracker: *QuorumTracker, follower_id: u64) !proto.ReplAck {
        const ack = try self.shipFrames(frames);
        try tracker.recordAck(follower_id, ack.confirmed_seq);
        return ack;
    }

    /// Blocks until connected (with exponential backoff) or cancellation.
    ///
    /// Idempotent when already connected. Retries [`ReplClient.connect`] with backoff
    /// doubling from 100 ms to a 5 s cap, logging each failure, until either a
    /// connection is made or `is_running` goes false (then `error.Canceled`). This is
    /// the legacy `config`-driven connect path (address/uid/key from [`Config`]).
    pub fn ensureConnected(self: *ReplClient, config: *const Config, is_running: *const bool) !void {
        if (self.stream != null) return;
        var backoff_ms: u64 = 100;
        while (self.stream == null and is_running.*) {
            self.connect(config) catch |err| {
                if (!is_running.*) return error.Canceled;
                log.warn("Replication client failed to connect to {s}:{d}: {any}. Retrying in {d}ms...", .{
                    config.replica.address, config.replica.port, err, backoff_ms,
                });
                const delay = std.Io.Duration.fromMilliseconds(@intCast(backoff_ms));
                _ = self.io.sleep(delay, .real) catch {};
                backoff_ms = @min(backoff_ms * 2, 5000);
                continue;
            };
            log.info("Replication client successfully connected to replica at {s}:{d}", .{
                config.replica.address, config.replica.port,
            });
            break;
        }
        if (self.stream == null and !is_running.*) return error.Canceled;
    }

    /// Dials the configured replica and runs the [`Packet`]-based authenticate
    /// handshake.
    ///
    /// Refuses to proceed without both a replica uid and key
    /// (`error.NoReplicationCredentials`) so an unconfigured node never connects
    /// anonymously. On any failure after the socket opens, the `errdefer` closes it
    /// and clears [`ReplClient.stream`] so the client is left cleanly disconnected.
    fn connect(self: *ReplClient, config: *const Config) !void {
        if (config.replica.uid.len == 0 or config.replica.key.len == 0) {
            log.err("repl: replica.uid/replica.key not configured", .{});
            return error.NoReplicationCredentials;
        }
        const address = try net.IpAddress.parse(config.replica.address, config.replica.port);
        const stream = try address.connect(self.io, .{ .mode = .stream, .protocol = .tcp });
        self.stream = stream;
        errdefer {
            stream.close(self.io);
            self.stream = null;
        }

        try self.authenticate(config);
    }

    /// Closes the TLS session (if any) and the underlying socket, resetting to a
    /// disconnected state.
    ///
    /// Frees the [`TlsClientHolder`] and its cert/CA material, then closes the raw
    /// stream. Safe to call when already disconnected and safe to call twice. Close
    /// errors are ignored because there is nothing actionable during teardown.
    fn disconnect(self: *ReplClient) void {
        if (self.tls) |h| {
            h.conn.close() catch {};
            h.client_ck.deinit(self.allocator);
            h.ca_bundle.deinit(self.allocator);
            self.allocator.destroy(h);
            self.tls = null;
        }
        if (self.stream) |*s| {
            s.close(self.io);
            self.stream = null;
        }
    }

    /// Sends an `Authenticate` [`Packet`] carrying the replica uid/key and waits for
    /// the server's reply.
    ///
    /// The legacy credential handshake (distinct from the nonce-based
    /// [`ReplClient.authenticateStream`]): it serialises the packet, ships it framed,
    /// and validates the reply via [`ReplClient.readReply`], which turns a rejection
    /// into an error.
    fn authenticate(self: *ReplClient, config: *const Config) !void {
        log.debug("client authenticating...", .{});
        const auth = Packet{
            .op = Operation{
                .Authenticate = .{
                    .uid = config.replica.uid,
                    .key = config.replica.key,
                },
            },
        };

        var serialize_allocating = std.Io.Writer.Allocating.init(self.allocator);
        defer serialize_allocating.deinit();
        try auth.serialize(&serialize_allocating.writer);

        log.debug("client sending auth packet size={d}", .{serialize_allocating.written().len});
        try self.writeFramed(serialize_allocating.written());
        log.debug("client waiting for auth reply...", .{});
        try self.readReply();
        log.debug("client authenticated successfully!", .{});
    }

    /// Writes a pre-framed [`Packet`] payload over the plaintext stream and flushes.
    ///
    /// The payload is expected to already carry its own length prefix (the [`Packet`]
    /// serialiser emits it), so this just streams the bytes. `error.NotConnected` if
    /// there is no socket.
    fn writeFramed(self: *ReplClient, payload: []const u8) !void {
        const stream = self.stream orelse return error.NotConnected;
        var write_buf: [4096]u8 = undefined;
        var w = stream.writer(self.io, &write_buf);

        log.debug("client writeFramed payload_len={d}", .{payload.len});
        try w.interface.writeAll(payload);
        try w.interface.flush();
    }

    /// Reads one length-prefixed [`Packet`] reply and validates it is an `Ok` reply.
    ///
    /// Reads the 4-byte length, then the body, reconstructs the framed bytes and
    /// deserialises them. A non-`Reply` op is `error.InvalidReply`; a `Reply` whose
    /// status is not `Ok` is `error.ReplicationRejected` (the server's rejection
    /// message is logged). The deserialised packet is freed before returning.
    fn readReply(self: *ReplClient) !void {
        const stream = self.stream orelse return error.NotConnected;
        var read_buf: [4096]u8 = undefined;
        var r = stream.reader(self.io, &read_buf);

        var len_bytes: [4]u8 = undefined;
        r.interface.readSliceAll(&len_bytes) catch |err| {
            log.debug("client readSliceAll prefix failed: {any}", .{err});
            return err;
        };
        const payload_len = std.mem.readInt(u32, &len_bytes, .little);
        log.debug("client read prefix payload_len={d}", .{payload_len});

        const resp_bytes = try self.allocator.alloc(u8, 4 + payload_len);
        defer self.allocator.free(resp_bytes);
        @memcpy(resp_bytes[0..4], &len_bytes);
        r.interface.readSliceAll(resp_bytes[4..]) catch |err| {
            log.debug("client readSliceAll payload failed: {any}", .{err});
            return err;
        };

        const reply = Packet.deserialize(self.allocator, resp_bytes) catch |err| {
            log.debug("client deserialize reply failed: {any}", .{err});
            return err;
        };
        defer Packet.free(self.allocator, reply);

        switch (reply.op) {
            .Reply => |rep| {
                log.debug("client received reply status={any}", .{rep.status});
                if (rep.status != .Ok) {
                    if (rep.data) |msg| {
                        log.debug("client rejection message: {s}", .{msg});
                    }
                    return error.ReplicationRejected;
                }
            },
            else => {
                log.debug("client invalid reply op type", .{});
                return error.InvalidReply;
            },
        }
    }
};


/// The durable position of a follower: the newest leader epoch it has accepted and
/// how far it has applied.
///
/// Persisted as a small JSON document so a follower survives a restart with its
/// fencing point ([`FollowerState.max_epoch_seen`]) and its apply cursor
/// ([`FollowerState.confirmed_seq`]) intact. These two numbers together are the
/// entire replication safety state on the follower: the epoch prevents accepting a
/// deposed leader's writes, the seq prevents applying out of order or twice.
pub const FollowerState = struct {
    /// Highest leader epoch ever accepted; batches from a lower epoch are fenced.
    max_epoch_seen: u64 = 0,
    /// Sequence number of the last frame durably applied; the next expected batch
    /// must have `base_seq == confirmed_seq + 1`.
    confirmed_seq: u64 = 0,
    /// On-disk filename for the persisted follower state.
    const STATE_FILENAME = "REPL_FOLLOWER_STATE";

    /// Loads the follower state from `dir_path`, defaulting to zeros on any problem.
    ///
    /// A missing file, unreadable file, or malformed JSON all yield the zero state
    /// (fresh follower) rather than erroring: a follower with no prior state is a
    /// valid starting condition, and a corrupt state file is safest treated as
    /// "start over" because the primary will re-drive it from its confirmed point.
    pub fn load(allocator: Allocator, io: Io, dir_path: []const u8) FollowerState {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const p = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, STATE_FILENAME }) catch return .{};
        const data = Io.Dir.readFileAlloc(.cwd(), io, p, allocator, .unlimited) catch return .{};
        defer allocator.free(data);
        const parsed = std.json.parseFromSlice(FollowerState, allocator, data, .{}) catch return .{};
        defer parsed.deinit();
        return parsed.value;
    }

    /// Atomically persists the follower state via write-temp, `fsync`, rename.
    ///
    /// Same crash-safe pattern as [`CheckpointRecord.save`]: because
    /// [`Follower.recvFrames`] calls this on the apply path, the `fsync` is what makes
    /// "I have applied up to seq N" genuinely durable before the follower acks N, so
    /// a follower cannot ack a position it might lose on a crash.
    pub fn save(self: FollowerState, io: Io, dir_path: []const u8) !void {
        var tmp_buf: [std.fs.max_path_bytes]u8 = undefined;
        const tmp_path = try std.fmt.bufPrint(&tmp_buf, "{s}/{s}.tmp", .{ dir_path, STATE_FILENAME });
        var buf: [128]u8 = undefined;
        const data = try std.fmt.bufPrint(&buf, "{{\"max_epoch_seen\":{}, \"confirmed_seq\":{}}}", .{ self.max_epoch_seen, self.confirmed_seq });
        var file = try Io.Dir.createFile(.cwd(), io, tmp_path, .{});
        defer file.close(io);
        try file.writeStreamingAll(io, data);
        try file.sync(io);
        var final_buf: [std.fs.max_path_bytes]u8 = undefined;
        const final_path = try std.fmt.bufPrint(&final_buf, "{s}/{s}", .{ dir_path, STATE_FILENAME });
        try Io.Dir.rename(.cwd(), tmp_path, .cwd(), final_path, io);
    }
};

/// The disposition of a batch offered to [`Follower.recvFrames`].
///
/// Reported alongside the ack so the primary can distinguish "keep going"
/// (`applied`/`empty`) from "you sent from a stale epoch" (`fenced`) and "I'm behind,
/// resend" (`gap`), and react accordingly (e.g. trigger catch-up on `gap`).
pub const RecvOutcome = enum {
    /// The batch was decoded and durably applied; `confirmed_seq` advanced.
    applied,
    /// The batch's epoch was older than [`FollowerState.max_epoch_seen`] and was
    /// rejected to protect against a deposed leader.
    fenced,
    /// The batch's `base_seq` was ahead of `confirmed_seq + 1`; nothing was applied
    /// and the primary must resend from the follower's confirmed point.
    gap,
    /// The batch carried no frames (e.g. a heartbeat used to learn `confirmed_seq`).
    empty,
};

/// A follower's response to one received batch: the ack to send back plus the
/// classification of what happened.
pub const RecvResult = struct {
    /// The ack (epoch + confirmed_seq) to return to the primary.
    ack: proto.ReplAck,
    /// How the batch was handled; see [`RecvOutcome`].
    outcome: RecvOutcome,
};

/// The follower-side apply engine: validates, decodes and applies received batches
/// against the local [`Database`], persisting progress as it goes.
///
/// Holds the durable [`FollowerState`] and enforces the two replication invariants on
/// the receive side: epoch fencing (reject older leaders) and strict in-order,
/// no-gap application. It is driven by [`ReplServer`]'s connection loop, which hands
/// it one deserialised [`proto.ReplFrames`] at a time.
pub const Follower = struct {
    /// Allocator for the transient decode buffers.
    allocator: Allocator,
    /// I/O token used when persisting [`Follower.state`].
    io: Io,
    /// The live database batches are applied into via `applyStream`/`observeEpoch`.
    db: *Database,
    /// Directory where [`FollowerState`] is persisted.
    dir_path: []const u8,
    /// The durable follower position (epoch + confirmed seq), loaded at init.
    state: FollowerState,

    /// Builds a follower over `db`, loading any persisted [`FollowerState`] from
    /// `dir_path`.
    ///
    /// A fresh directory yields a zeroed state, so a brand-new follower starts ready
    /// to accept from `base_seq == 1`.
    pub fn init(allocator: Allocator, io: Io, db: *Database, dir_path: []const u8) Follower {
        return .{
            .allocator = allocator,
            .io = io,
            .db = db,
            .dir_path = dir_path,
            .state = FollowerState.load(allocator, io, dir_path),
        };
    }

    /// Snapshots the current durable position as an ack to return to the primary.
    fn currentAck(self: *Follower) proto.ReplAck {
        return .{ .epoch = self.state.max_epoch_seen, .confirmed_seq = self.state.confirmed_seq };
    }

    /// Validates, decodes and applies one received batch, returning the ack and
    /// outcome.
    ///
    /// The core follower-side state machine, in order:
    ///  1. **Fence**: if `frames.epoch < max_epoch_seen`, reject as `.fenced` without
    ///     applying, a stale leader must not overwrite a newer one's data.
    ///  2. **Adopt epoch**: if the batch's epoch is newer, persist it and notify the
    ///     database ([`Database.observeEpoch`]) before applying, so the fencing point
    ///     is durable even if the apply below fails.
    ///  3. **Empty**: a frameless batch (heartbeat) just returns the current ack.
    ///  4. **Gap check**: require `base_seq == confirmed_seq + 1`; otherwise `.gap`,
    ///     leaving state untouched so the primary resends.
    ///  5. **Decode + apply**: deserialise every frame to a [`LogRecord`] and hand the
    ///     whole run to [`Database.applyStream`] atomically, then advance and persist
    ///     `confirmed_seq`. The `defer` frees every decoded record's owned buffers
    ///     (only the `decoded` prefix, so a mid-batch `error.InvalidFrame` does not
    ///     touch un-filled slots).
    ///
    /// `confirmed_seq` is only advanced and saved *after* a successful apply, so a
    /// crash mid-apply cannot leave the follower claiming to have applied records it
    /// did not.
    pub fn recvFrames(self: *Follower, frames: proto.ReplFrames) !RecvResult {
        if (frames.epoch < self.state.max_epoch_seen) {
            log.warn("follower: FENCED frame epoch={d} < max_epoch_seen={d}; not applying", .{ frames.epoch, self.state.max_epoch_seen });
            return .{ .ack = self.currentAck(), .outcome = .fenced };
        }
        if (frames.epoch > self.state.max_epoch_seen) {
            self.state.max_epoch_seen = frames.epoch;
            try self.state.save(self.io, self.dir_path);
            try self.db.observeEpoch(frames.epoch);
        }

        if (frames.frames.len == 0) return .{ .ack = self.currentAck(), .outcome = .empty };

        if (frames.base_seq != self.state.confirmed_seq + 1) {
            log.warn("follower: GAP base_seq={d} expected={d}; requesting resync", .{ frames.base_seq, self.state.confirmed_seq + 1 });
            return .{ .ack = self.currentAck(), .outcome = .gap };
        }

        const records = try self.allocator.alloc(LogRecord, frames.frames.len);
        var decoded: usize = 0;
        defer {
            for (records[0..decoded]) |r| {
                self.allocator.free(r.table_name);
                self.allocator.free(r.key);
                self.allocator.free(r.value);
            }
            self.allocator.free(records);
        }
        for (frames.frames) |fbytes| {
            var reader = FrameReader{ .buffer = fbytes };
            const maybe = try LogRecord.deserialize(self.allocator, &reader);
            records[decoded] = maybe orelse return error.InvalidFrame;
            decoded += 1;
        }

        _ = try self.db.applyStream(records[0..decoded]);

        self.state.confirmed_seq = frames.base_seq + frames.frames.len - 1;
        try self.state.save(self.io, self.dir_path);
        return .{ .ack = self.currentAck(), .outcome = .applied };
    }
};

/// The MAC primitive for the challenge-response handshake: HMAC-SHA256.
const HmacSha256 = std.crypto.auth.hmac.sha2.HmacSha256;

/// Constant-time equality over two byte slices.
///
/// Used to compare the received HMAC tag against the expected one. It accumulates
/// the XOR of every byte pair and only branches on the aggregate, so the running
/// time does not depend on *where* the first mismatch is, closing the timing side
/// channel a naive early-return comparison would open. Length mismatch short-circuits
/// (the lengths are not secret).
fn ctEq(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var diff: u8 = 0;
    for (a, b) |x, y| diff |= x ^ y;
    return diff == 0;
}

/// Runs the challenger (server) side of the HMAC-SHA256 handshake.
///
/// A no-op when `key` is empty (auth disabled). Generates a fresh 32-byte random
/// nonce, sends it, reads back the peer's tag, and accepts only if it equals
/// `HMAC(nonce, key)` compared in constant time ([`ctEq`]); a mismatch is
/// `error.AuthFailed`. On success it sends a single `0x01` accept byte. The random
/// nonce per handshake defeats replay. Generic over `reader`/`writer` so it serves
/// both the plaintext and TLS transports.
fn serverAuthenticate(io: Io, reader: anytype, writer: anytype, key: []const u8) !void {
    if (key.len == 0) return;
    var nonce: [32]u8 = undefined;
    std.Io.random(io, &nonce);
    try writer.writeAll(&nonce);
    try writer.flush();
    var tag: [32]u8 = undefined;
    try reader.readSliceAll(&tag);
    var expected: [32]u8 = undefined;
    HmacSha256.create(&expected, &nonce, key);
    if (!ctEq(&tag, &expected)) return error.AuthFailed;
    try writer.writeAll(&[_]u8{0x01});
    try writer.flush();
}

/// Runs the responder (client) side of the HMAC-SHA256 handshake.
///
/// A no-op when `key` is empty. Reads the server's 32-byte nonce, replies with
/// `HMAC(nonce, key)`, and waits for the `0x01` accept byte; a read failure or any
/// other byte is `error.AuthFailed`. The mirror of [`serverAuthenticate`]; the key
/// itself is never transmitted.
fn clientAuthenticate(reader: anytype, writer: anytype, key: []const u8) !void {
    if (key.len == 0) return;
    var nonce: [32]u8 = undefined;
    try reader.readSliceAll(&nonce);
    var tag: [32]u8 = undefined;
    HmacSha256.create(&tag, &nonce, key);
    try writer.writeAll(&tag);
    try writer.flush();
    var ackb: [1]u8 = undefined;
    reader.readSliceAll(&ackb) catch return error.AuthFailed;
    if (ackb[0] != 0x01) return error.AuthFailed;
}

/// The follower-side network server: accepts primary connections and feeds each
/// batch into its [`Follower`].
///
/// Listens on `host:port`, authenticates and (optionally) TLS-wraps each inbound
/// connection, then loops receiving length-prefixed [`proto.ReplFrames`] and acking
/// them. Each accepted connection is handled on its own task in [`ReplServer.group`],
/// so multiple primaries/retries do not serialise. The atomic flags make lifecycle
/// and fault-injection thread-safe: [`ReplServer.running`] gates the accept loop,
/// [`ReplServer.partitioned`] simulates a network partition for tests
/// ([`ReplServer.partition`]/[`ReplServer.heal`]) by dropping traffic without
/// closing the listener.
pub const ReplServer = struct {
    /// Allocator for per-connection buffers.
    allocator: Allocator,
    /// I/O token for listen/accept and per-connection I/O.
    io: Io,
    /// The apply engine every received batch is routed to.
    follower: *Follower,
    /// Bind address.
    host: []const u8,
    /// Bind port.
    port: u16,
    /// Accept-loop gate; cleared by [`ReplServer.stop`] to wind the server down.
    running: std.atomic.Value(bool),
    /// Set once the listener has successfully bound; lets a starter observe readiness.
    bound: std.atomic.Value(bool),
    /// Test/fault-injection flag: when set, connections are refused mid-stream to
    /// emulate a partition, without tearing the listener down.
    partitioned: std.atomic.Value(bool),
    /// The bound listening socket, or `null` before [`ReplServer.listen`].
    listen_server: ?net.Server = null,
    /// Task group owning the per-connection handlers; cancelled on stop.
    group: Io.Group = .init,
    /// Shared HMAC key required of every connecting primary (empty = no auth).
    auth_key: []const u8 = "",
    /// Mutual-TLS material; when enabled the server *requires* a client certificate.
    tls_config: TlsConfig = .{},

    /// Constructs a not-yet-listening server bound to `follower` and the given
    /// address/key.
    ///
    /// Set [`ReplServer.tls_config`] before [`ReplServer.listen`] to require TLS.
    pub fn init(allocator: Allocator, io: Io, follower: *Follower, host: []const u8, port: u16, auth_key: []const u8) ReplServer {
        return .{
            .allocator = allocator,
            .io = io,
            .follower = follower,
            .host = host,
            .port = port,
            .running = std.atomic.Value(bool).init(false),
            .bound = std.atomic.Value(bool).init(false),
            .partitioned = std.atomic.Value(bool).init(false),
            .auth_key = auth_key,
        };
    }

    /// Simulates a network partition: subsequently accepted (and in-flight)
    /// connections drop traffic instead of applying it.
    ///
    /// A test hook for exercising failover/catch-up without real network faults; the
    /// listener stays bound so healing is instant.
    pub fn partition(self: *ReplServer) void {
        self.partitioned.store(true, .seq_cst);
    }

    /// Ends a simulated partition, resuming normal apply behaviour.
    pub fn heal(self: *ReplServer) void {
        self.partitioned.store(false, .seq_cst);
    }

    /// Binds the listener and runs the accept loop until [`ReplServer.stop`].
    ///
    /// Sets [`ReplServer.bound`] once bound (with `reuse_address` so a quick restart
    /// does not hit `TIME_WAIT`) and spawns each accepted connection onto
    /// [`ReplServer.group`] via [`handleReplConn`]. Accept errors are tolerated: a
    /// `Canceled` (from stop) or a cleared `running` breaks the loop, any other
    /// transient error just retries the accept, so one bad connection cannot kill the
    /// server.
    pub fn listen(self: *ReplServer) !void {
        const addr = try net.IpAddress.parse(self.host, self.port);
        var s = try addr.listen(self.io, .{ .reuse_address = true });
        self.listen_server = s;
        self.running.store(true, .seq_cst);
        self.bound.store(true, .seq_cst);
        while (self.running.load(.seq_cst)) {
            const stream = s.accept(self.io) catch |err| {
                if (err == error.Canceled) break;
                if (!self.running.load(.seq_cst)) break;
                continue;
            };
            self.group.async(self.io, handleReplConn, .{ self, stream });
        }
    }

    /// Thread/task entry point that runs [`ReplServer.listen`] and logs a fatal bind
    /// failure.
    ///
    /// A `void`-returning wrapper suitable for spawning; the server is expected to
    /// run for the process lifetime, so a listen error is logged rather than
    /// propagated.
    pub fn listenEntry(self: *ReplServer) void {
        self.listen() catch |err| log.err("repl server: listen failed: {any}", .{err});
    }

    /// Signals the accept loop to stop, closes the listener, and cancels all
    /// in-flight connection handlers.
    ///
    /// Clearing [`ReplServer.running`] first ensures a handler observing it after the
    /// cancel still exits; closing the socket unblocks a parked `accept`.
    pub fn stop(self: *ReplServer) void {
        self.running.store(false, .seq_cst);
        if (self.listen_server) |*s| s.socket.close(self.io);
        self.group.cancel(self.io);
    }
};

/// Per-connection task body: runs the connection to completion and always closes the
/// socket.
///
/// Delegates to [`handleReplConnInner`] and downgrades any error to a warning
/// (a dropped/failed peer is routine), guaranteeing the socket is closed on every
/// exit path so a failed handshake cannot leak a descriptor.
fn handleReplConn(self: *ReplServer, stream: net.Stream) void {
    handleReplConnInner(self, stream) catch |err| {
        log.warn("repl server: connection ended: {any}", .{err});
    };
    stream.close(self.io);
}

/// Sets up transport (TLS or plaintext) for one connection, then enters the receive
/// loop.
///
/// If partitioned, returns immediately (dropping the connection). When TLS is
/// enabled it loads the server cert/key and CA bundle and performs a *client-cert-
/// required* handshake: a handshake failure is logged and the connection refused, so
/// an unauthenticated or wrong-CA peer never reaches the apply loop. Either branch
/// ends in [`replConnLoop`] over the appropriate reader/writer interface. The buffers
/// and TLS state are stack-local to this connection's task, hence the fixed-size
/// arrays.
fn handleReplConnInner(self: *ReplServer, stream: net.Stream) !void {
    if (self.partitioned.load(.seq_cst)) return;
    if (self.tls_config.isEnabled()) {
        var csprng: std.Random.DefaultCsprng = undefined;
        const rng = tlsRng(self.io, &csprng);
        var server_ck = tls.config.CertKeyPair.fromFilePath(self.allocator, self.io, Io.Dir.cwd(), self.tls_config.cert_path, self.tls_config.key_path) catch |err| {
            log.err("repl server: loading server cert/key failed: {any}", .{err});
            return;
        };
        defer server_ck.deinit(self.allocator);
        var ca_bundle = tls.config.cert.fromFilePath(self.allocator, self.io, Io.Dir.cwd(), self.tls_config.ca_path) catch |err| {
            log.err("repl server: loading CA bundle failed: {any}", .{err});
            return;
        };
        defer ca_bundle.deinit(self.allocator);
        const opts = tls.config.Server{
            .rng = rng,
            .auth = &server_ck,
            .client_auth = .{ .root_ca = ca_bundle, .auth_type = .require },
            .now = Io.Clock.real.now(self.io),
        };
        var conn = tls.serverFromStream(self.io, stream, opts) catch |err| {
            log.warn("repl server: mutual-TLS handshake FAILED ({any}); refusing connection", .{err});
            return;
        };
        defer conn.close() catch {};
        var trbuf: [tls.input_buffer_len]u8 = undefined;
        var twbuf: [tls.output_buffer_len]u8 = undefined;
        var tr = conn.reader(&trbuf);
        var tw = conn.writer(&twbuf);
        return replConnLoop(self, &tr.interface, &tw.interface);
    }
    var read_buf: [8192]u8 = undefined;
    var write_buf: [64]u8 = undefined;
    var r = stream.reader(self.io, &read_buf);
    var w = stream.writer(self.io, &write_buf);
    return replConnLoop(self, &r.interface, &w.interface);
}

/// The follower's per-connection receive loop: authenticate, then apply batches and
/// ack, transport-agnostic.
///
/// First runs [`serverAuthenticate`]; a failure refuses the connection. Then, while
/// running, it reads a `u32` length prefix and the payload, guarding against absurd
/// sizes (`0` or `> 64 MiB` is `error.InvalidFrame`) so a malformed length cannot
/// trigger a huge allocation. A clean `EndOfStream` ends the loop. A payload that
/// fails to deserialise is not fatal: the follower's *current* ack is sent back and
/// the loop continues, letting the primary re-drive from its confirmed point. A
/// well-formed batch is routed to [`Follower.recvFrames`] and its ack returned.
/// Re-checks [`ReplServer.partitioned`] after each read so a partition injected
/// mid-stream takes effect immediately.
fn replConnLoop(self: *ReplServer, reader: *Io.Reader, writer: *Io.Writer) !void {
    serverAuthenticate(self.io, reader, writer, self.auth_key) catch |err| {
        log.warn("repl server: peer authentication FAILED ({any}); refusing connection", .{err});
        return;
    };

    while (self.running.load(.seq_cst)) {
        var len_bytes: [4]u8 = undefined;
        reader.readSliceAll(&len_bytes) catch |err| {
            if (err == error.EndOfStream) return;
            return err;
        };
        if (self.partitioned.load(.seq_cst)) return;
        const payload_len = std.mem.readInt(u32, &len_bytes, .little);
        if (payload_len == 0 or payload_len > 64 * 1024 * 1024) return error.InvalidFrame;

        const payload = try self.allocator.alloc(u8, payload_len);
        defer self.allocator.free(payload);
        try reader.readSliceAll(payload);

        var frames = proto.ReplFrames.deserialize(self.allocator, payload) catch |err| {
            log.warn("repl server: rejecting batch: {any}", .{err});
            var ackbuf = self.follower.currentAck().serialize();
            try writer.writeAll(&ackbuf);
            try writer.flush();
            continue;
        };
        defer frames.deinit(self.allocator);

        const result = try self.follower.recvFrames(frames);
        var ackbuf = result.ack.serialize();
        try writer.writeAll(&ackbuf);
        try writer.flush();
    }
}

/// Tracks each follower's confirmed sequence and answers "is this write durable on a
/// majority yet?".
///
/// The primary's synchronous-replication gate: it maps `follower_id -> confirmed_seq`
/// and derives coverage/quorum from it. The primary itself always counts as one vote
/// (its own copy is durable), so counts start at 1. Access is serialised by a small
/// spin lock ([`QuorumTracker.lockSpin`]) rather than a mutex because the critical
/// sections are tiny (a hash lookup or a short iteration) and are touched from
/// shipping coroutines that must not block a whole thread.
pub const QuorumTracker = struct {
    /// Allocator backing [`QuorumTracker.follower_seqs`].
    allocator: Allocator,
    /// Total replica count including the primary; forced to at least 1 so quorum math
    /// is well-defined.
    total_replicas: u32,
    /// Highest confirmed sequence reported by each follower, keyed by follower id.
    follower_seqs: std.AutoHashMap(u64, u64),
    /// Spin-lock flag guarding [`QuorumTracker.follower_seqs`] against concurrent
    /// shipping tasks.
    lock_flag: std.atomic.Value(bool) = .{ .raw = false },

    /// Creates an empty tracker for `total_replicas` (clamped to a minimum of 1).
    pub fn init(allocator: Allocator, total_replicas: u32) QuorumTracker {
        return .{
            .allocator = allocator,
            .total_replicas = if (total_replicas == 0) 1 else total_replicas,
            .follower_seqs = std.AutoHashMap(u64, u64).init(allocator),
        };
    }

    /// Frees the follower-sequence map.
    pub fn deinit(self: *QuorumTracker) void {
        self.follower_seqs.deinit();
    }

    /// Acquires the spin lock, busy-waiting until the flag flips false→true with
    /// acquire ordering.
    ///
    /// Appropriate only because every critical section here is O(1) or a short map
    /// walk; it would be the wrong choice for anything that can block.
    fn lockSpin(self: *QuorumTracker) void {
        while (self.lock_flag.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {}
    }
    /// Releases the spin lock with release ordering, publishing the critical
    /// section's writes.
    fn unlockSpin(self: *QuorumTracker) void {
        self.lock_flag.store(false, .release);
    }

    /// Returns the majority threshold `floor(total/2) + 1`.
    ///
    /// A write is durable once this many replicas (primary included) hold it; see
    /// [`QuorumTracker.isCovered`].
    pub fn quorum(self: *QuorumTracker) u32 {
        return self.total_replicas / 2 + 1;
    }

    /// Records that `follower_id` has confirmed up to `confirmed_seq`, monotonically.
    ///
    /// Only advances the stored value (a stale/duplicate ack with a lower seq is
    /// ignored), so out-of-order acks cannot regress a follower's recorded progress.
    /// Takes the spin lock for the duration.
    pub fn recordAck(self: *QuorumTracker, follower_id: u64, confirmed_seq: u64) !void {
        self.lockSpin();
        defer self.unlockSpin();
        const cur = self.follower_seqs.get(follower_id) orelse 0;
        if (confirmed_seq > cur) try self.follower_seqs.put(follower_id, confirmed_seq);
    }

    /// Counts how many replicas hold `seq`, including the primary.
    ///
    /// Starts at 1 for the primary's own durable copy, then adds every follower whose
    /// confirmed seq is `>= seq`. Feeds [`QuorumTracker.isCovered`].
    pub fn coverage(self: *QuorumTracker, seq: u64) u32 {
        self.lockSpin();
        defer self.unlockSpin();
        var n: u32 = 1;
        var it = self.follower_seqs.valueIterator();
        while (it.next()) |v| {
            if (v.* >= seq) n += 1;
        }
        return n;
    }

    /// Reports whether `seq` is held by a quorum, i.e. it is durable enough to
    /// acknowledge to a client.
    pub fn isCovered(self: *QuorumTracker, seq: u64) bool {
        return self.coverage(seq) >= self.quorum();
    }

    /// Returns the lowest confirmed sequence across all `expected_followers`, or 0 if
    /// they have not all reported.
    ///
    /// The safe-to-prune watermark: history below this is confirmed on *every*
    /// expected follower, so [`DurableReplicator.checkpointBackfill`] can compact the
    /// backfill up to it. Returns 0 (prune nothing) if fewer than `expected_followers`
    /// have acked, so a follower that has not yet reported is never assumed caught up.
    pub fn minConfirmed(self: *QuorumTracker, expected_followers: u32) u64 {
        self.lockSpin();
        defer self.unlockSpin();
        if (self.follower_seqs.count() < expected_followers) return 0;
        var m: u64 = std.math.maxInt(u64);
        var it = self.follower_seqs.valueIterator();
        while (it.next()) |v| {
            if (v.* < m) m = v.*;
        }
        return if (m == std.math.maxInt(u64)) 0 else m;
    }

    /// Blocks until `seq` is covered by a quorum, or `error.QuorumTimeout` after
    /// `timeout_ms`.
    ///
    /// Polls [`QuorumTracker.isCovered`] with a 2 ms sleep between checks (yielding to
    /// the scheduler via `io.sleep` rather than spinning), so a synchronous write
    /// waits here for follower acks. The bounded timeout means a lost/slow follower
    /// degrades the write to an error rather than hanging forever.
    pub fn awaitQuorum(self: *QuorumTracker, io: Io, seq: u64, timeout_ms: u64) !void {
        const start = std.Io.Clock.now(.real, io).toMilliseconds();
        while (!self.isCovered(seq)) {
            const elapsed = std.Io.Clock.now(.real, io).toMilliseconds() - start;
            if (elapsed >= 0 and @as(u64, @intCast(elapsed)) >= timeout_ms) return error.QuorumTimeout;
            _ = io.sleep(std.Io.Duration.fromMilliseconds(2), .real) catch {};
        }
    }
};

/// The primary-side replication driver: turns the local commit stream into shipped,
/// quorum-tracked, catch-up-capable batches.
///
/// This is the orchestration layer above [`ReplClient`]. It subscribes to committed
/// records ([`DurableReplicator.onRecord`]), assembles per-transaction batches on
/// commit ([`DurableReplicator.shipPending`]), assigns them contiguous sequence
/// numbers, retains them (RAM ring + optional [`BackfillLog`]) so a lagging follower
/// can be replayed, ships them, and (in synchronous mode) waits for quorum via
/// [`DurableReplicator.tracker`]. It transparently reconnects and catches a follower
/// up after a drop ([`DurableReplicator.reconnectAndCatchUp`]), and prunes retained
/// history once every follower has confirmed past it
/// ([`DurableReplicator.checkpointBackfill`]).
///
/// Currently single-follower on the wire (`follower_id` defaults to 1) though the
/// quorum machinery is written for N.
pub const DurableReplicator = struct {
    /// One follower's transport: everything per-follower lives here, so the
    /// replicator can fan the (single, shared) sequence stream out to a set of
    /// followers. The seq stream itself (`next_seq`, `sent`, `backfill`, the
    /// `tracker`) is shared and not duplicated per link.
    pub const FollowerLink = struct {
        /// Stable follower id; the key into [`QuorumTracker.follower_seqs`].
        id: u64,
        /// Owned copy of the follower host, for reconnects.
        host: []const u8,
        /// Follower port.
        port: u16,
        /// The outbound connection to this follower.
        client: ReplClient,
        /// Whether this link currently believes it holds a live connection.
        connected: bool = false,
    };

    /// Allocator owning the buffers, retained batches, and duped hosts.
    allocator: Allocator,
    /// I/O token for network and sleep operations.
    io: Io,
    /// Per-follower ack progress and the quorum-durability decision. Shared
    /// across all links (keyed by [`FollowerLink.id`]).
    tracker: QuorumTracker,
    /// Current leader epoch stamped into every shipped batch; bumped on failover.
    epoch: u64,
    /// How long a synchronous write waits for quorum before `error.QuorumTimeout`.
    timeout_ms: u64,
    /// Shared HMAC key presented on each connect via [`ReplClient.authenticateStream`].
    auth_key: []const u8 = "",
    /// TLS config applied to every follower link's client on connect.
    tls_config: TlsConfig = .{},
    /// Next sequence number to assign to a shipped frame; the primary's monotonic
    /// counter. One contiguous stream shared by every follower.
    next_seq: u64 = 1,
    /// Frames accumulated for the in-progress transaction, flushed on commit.
    buf: std.ArrayList([]const u8) = .empty,
    /// Set when a `commit` record has arrived, i.e. the buffered batch is shippable.
    pending_ready: bool = false,
    /// Set when the buffer contains at least one data mutation; a commit with no
    /// writes ships nothing.
    pending_write: bool = false,

    /// The followers this primary ships to. Each is fed the same shared stream
    /// from its own cursor. Populated by [`DurableReplicator.connect`] /
    /// [`DurableReplicator.addFollower`].
    links: std.ArrayList(FollowerLink) = .empty,
    /// Bounded in-memory ring of recently shipped batches for fast catch-up.
    sent: std.ArrayList(SentBatch) = .empty,
    /// Optional durable catch-up store reaching further back than the ring.
    backfill: ?*BackfillLog = null,

    /// Maximum batches kept in the in-memory [`DurableReplicator.sent`] ring before
    /// the oldest is evicted; deeper catch-up relies on the [`BackfillLog`].
    const MAX_RETAINED = 1024;

    /// One retained batch in the in-memory ring: its sequence span and duped frames.
    const SentBatch = struct {
        /// Sequence number of the batch's first frame.
        base: u64,
        /// Sequence number of the batch's last frame.
        last: u64,
        /// Owned copies of the batch's frame payloads, replayable on reconnect.
        frames: [][]const u8,
    };

    /// Builds a replicator for `total_replicas` at leader `epoch`, wiring the client's
    /// TLS config.
    ///
    /// No connection is made here; call [`DurableReplicator.connect`] (and optionally
    /// [`DurableReplicator.enableBackfill`]) afterwards.
    pub fn init(allocator: Allocator, io: Io, total_replicas: u32, epoch: u64, timeout_ms: u64, auth_key: []const u8, tls_config: TlsConfig) DurableReplicator {
        return .{
            .allocator = allocator,
            .io = io,
            .tracker = QuorumTracker.init(allocator, total_replicas),
            .epoch = epoch,
            .timeout_ms = timeout_ms,
            .auth_key = auth_key,
            .tls_config = tls_config,
        };
    }

    /// The highest sequence number the primary has assigned to a shipped frame
    /// (`next_seq - 1`); 0 before anything is shipped. Read racily for metrics.
    pub fn producedSeq(self: *const DurableReplicator) u64 {
        return if (self.next_seq == 0) 0 else self.next_seq - 1;
    }

    /// The sequence number confirmed durable by every expected follower (the
    /// prune watermark); 0 until all expected followers have reported. For the
    /// single-follower deployment this is that follower's confirmed seq.
    pub fn confirmedSeq(self: *DurableReplicator) u64 {
        const expected: u32 = if (self.tracker.total_replicas > 1) self.tracker.total_replicas - 1 else 1;
        return self.tracker.minConfirmed(expected);
    }

    /// Replication lag in frames: how many shipped frames the follower(s) have
    /// not yet confirmed. Saturating so it can never underflow.
    pub fn lagFrames(self: *DurableReplicator) u64 {
        return self.producedSeq() -| self.confirmedSeq();
    }

    /// Releases all owned state: pending buffer, retained ring, backfill, host, client
    /// and tracker.
    pub fn deinit(self: *DurableReplicator) void {
        self.clearBuf();
        self.buf.deinit(self.allocator);
        for (self.sent.items) |b| self.freeBatch(b);
        self.sent.deinit(self.allocator);
        if (self.backfill) |bl| bl.deinit();
        for (self.links.items) |*link| {
            link.client.deinit();
            if (link.host.len > 0) self.allocator.free(link.host);
        }
        self.links.deinit(self.allocator);
        self.tracker.deinit();
    }

    /// Enables durable on-disk backfill in `dir`, replacing any existing backfill log.
    ///
    /// With backfill on, [`DurableReplicator.reconnectAndCatchUp`] can replay from
    /// disk when a follower has fallen past the in-memory ring, avoiding a full
    /// snapshot restore in far more cases.
    pub fn enableBackfill(self: *DurableReplicator, dir: []const u8) !void {
        if (self.backfill) |old| old.deinit();
        self.backfill = try BackfillLog.init(self.allocator, self.io, dir);
    }

    /// Frees one retained batch's frames and frame slice.
    fn freeBatch(self: *DurableReplicator, b: SentBatch) void {
        for (b.frames) |f| self.allocator.free(f);
        self.allocator.free(b.frames);
    }

    /// Establishes the initial connection to `host:port`: TCP, TLS upgrade, then auth.
    ///
    /// Remembers `host`/`port` (duping the host once) so later reconnects need no
    /// arguments, and marks the replicator connected. The three steps mirror
    /// [`DurableReplicator.reconnectAndCatchUp`]'s except that this path does not
    /// replay history (there is none yet on a first connect).
    /// Adds a follower link with stable id `id` and connects it (TCP + TLS +
    /// auth). The link is appended only on success; a failed connect frees the
    /// half-built client and duped host so nothing leaks. Fan-out ships the same
    /// shared stream to every link added here.
    pub fn addFollower(self: *DurableReplicator, id: u64, host: []const u8, port: u16) !void {
        const host_dup = try self.allocator.dupe(u8, host);
        errdefer self.allocator.free(host_dup);
        var client = ReplClient.init(self.allocator, self.io);
        client.tls_config = self.tls_config;
        errdefer client.deinit();
        try client.connectRaw(host, port);
        try client.upgradeTls(host);
        try client.authenticateStream(self.auth_key);
        try self.links.append(self.allocator, .{
            .id = id,
            .host = host_dup,
            .port = port,
            .client = client,
            .connected = true,
        });
    }

    /// Back-compatible single-follower connect: adds one link with id 1.
    pub fn connect(self: *DurableReplicator, host: []const u8, port: u16) !void {
        return self.addFollower(1, host, port);
    }

    /// Reconnects to the remembered follower and replays every batch it is missing.
    ///
    /// The catch-up protocol after a disconnect:
    ///  1. Reconnect (TCP + TLS + auth).
    ///  2. Send an *empty* heartbeat batch and read the ack to learn the follower's
    ///     current `confirmed_seq`.
    ///  3. Decide whether the in-memory ring reaches back far enough (its oldest
    ///     `base <= confirmed + 1`). If not, fall back to the on-disk [`BackfillLog`]
    ///     if it reaches back far enough, replaying its batches above `confirmed`.
    ///  4. If neither store covers the gap, return `error.SnapshotRequired`, the
    ///     follower is too far behind and needs a fresh snapshot restore.
    ///  5. Otherwise replay every retained ring batch with `base > confirmed`.
    ///
    /// Each replayed batch goes through [`ReplClient.shipAndRecord`] so the tracker
    /// stays current, and `confirmed` advances as acks come back so replay stops at
    /// the right point. The `errdefer` marks the replicator disconnected if any step
    /// fails, so a partial catch-up does not leave a false "connected" belief.
    fn reconnectAndCatchUpLink(self: *DurableReplicator, link: *FollowerLink) !void {
        link.client.disconnect();
        link.connected = false;
        errdefer {
            link.client.disconnect();
            link.connected = false;
        }
        try link.client.connectRaw(link.host, link.port);
        try link.client.upgradeTls(link.host);
        try link.client.authenticateStream(self.auth_key);
        link.connected = true;

        const hb = try link.client.shipAndRecord(
            proto.ReplFrames{ .epoch = self.epoch, .base_seq = 0, .frames = &[_][]const u8{} },
            &self.tracker,
            link.id,
        );
        var confirmed = hb.confirmed_seq;

        const ring_covers = self.sent.items.len == 0 or self.sent.items[0].base <= confirmed + 1;
        if (!ring_covers) {
            if (self.backfill) |bl| {
                if (bl.earliestBase()) |eb| {
                    if (eb <= confirmed + 1) {
                        const batches = try bl.batchesFrom(confirmed, self.allocator);
                        defer {
                            for (batches) |b| b.deinit(self.allocator);
                            self.allocator.free(batches);
                        }
                        for (batches) |b| {
                            const ack = try link.client.shipAndRecord(
                                proto.ReplFrames{ .epoch = self.epoch, .base_seq = b.base, .frames = b.frames },
                                &self.tracker,
                                link.id,
                            );
                            confirmed = ack.confirmed_seq;
                        }
                        log.info("replica {d} catch-up (on-disk backfill) complete: re-synced to seq {d}", .{ link.id, confirmed });
                        return;
                    }
                }
            }
            log.warn("replica {d} catch-up: at seq {d}; no retained batch reaches back to {d}; a full snapshot restore is required", .{ link.id, confirmed, confirmed + 1 });
            return error.SnapshotRequired;
        }
        for (self.sent.items) |b| {
            if (b.base <= confirmed) continue;
            const ack = try link.client.shipAndRecord(
                proto.ReplFrames{ .epoch = self.epoch, .base_seq = b.base, .frames = b.frames },
                &self.tracker,
                link.id,
            );
            confirmed = ack.confirmed_seq;
        }
        log.info("replica {d} catch-up complete: re-synced to seq {d}", .{ link.id, confirmed });
    }

    /// Deep-copies the current pending buffer into the retained ring as one batch
    /// spanning `base..last`.
    ///
    /// The frames must be duped (not aliased) because [`DurableReplicator.buf`] is
    /// cleared after shipping, but the ring must survive to replay later. Enforces the
    /// [`DurableReplicator.MAX_RETAINED`] bound by evicting the oldest batch. The
    /// `errdefer` frees the partial copy if a dup mid-way fails so nothing leaks.
    fn retain(self: *DurableReplicator, base: u64, last: u64) !void {
        const frames = try self.allocator.alloc([]const u8, self.buf.items.len);
        var filled: usize = 0;
        errdefer {
            for (frames[0..filled]) |f| self.allocator.free(f);
            self.allocator.free(frames);
        }
        for (self.buf.items, 0..) |f, i| {
            frames[i] = try self.allocator.dupe(u8, f);
            filled = i + 1;
        }
        try self.sent.append(self.allocator, .{ .base = base, .last = last, .frames = frames });
        if (self.sent.items.len > MAX_RETAINED) {
            const old = self.sent.orderedRemove(0);
            self.freeBatch(old);
        }
    }

    /// Updates the leader epoch stamped into future batches, e.g. after this node is
    /// elected/re-elected leader.
    pub fn setEpoch(self: *DurableReplicator, epoch: u64) void {
        self.epoch = epoch;
    }

    /// Returns the highest sequence number assigned so far (`next_seq - 1`), i.e. the
    /// point a snapshot taken now would correspond to.
    pub fn snapshotSeq(self: *DurableReplicator) u64 {
        return self.next_seq - 1;
    }

    /// Drops the entire in-memory retained ring, freeing its batches.
    ///
    /// Used when the ring is known to be superseded (e.g. after a snapshot is taken),
    /// so future catch-up relies on the [`BackfillLog`] rather than stale ring entries.
    pub fn evictRing(self: *DurableReplicator) void {
        for (self.sent.items) |b| self.freeBatch(b);
        self.sent.clearRetainingCapacity();
    }

    /// Frees and clears the pending-transaction buffer and resets its ready/write
    /// flags.
    ///
    /// Called on rollback and after a successful ship; leaves the buffer's capacity
    /// for reuse.
    fn clearBuf(self: *DurableReplicator) void {
        for (self.buf.items) |f| self.allocator.free(f);
        self.buf.clearRetainingCapacity();
        self.pending_ready = false;
        self.pending_write = false;
    }

    /// Commit-stream callback: buffers each committed record and flags when a batch
    /// is shippable.
    ///
    /// Registered with the database as an opaque-context hook, hence the `?*anyopaque`
    /// first parameter cast back to `*DurableReplicator`. A `rollback` record discards
    /// the whole pending buffer (an aborted transaction ships nothing). Other records
    /// are serialised and appended; `insert`/`update`/`delete` set
    /// [`DurableReplicator.pending_write`] (there is real data to ship) and `commit`
    /// sets [`DurableReplicator.pending_ready`] (the batch is complete). Serialisation
    /// or append failures are swallowed (`return`) because this runs on the commit
    /// path and must not fail the local write; the follower will resync via a later
    /// batch. [`DurableReplicator.shipPending`] consumes these flags.
    pub fn onRecord(ctx: ?*anyopaque, record: LogRecord) void {
        const self: *DurableReplicator = @ptrCast(@alignCast(ctx.?));
        if (record.kind == .rollback) {
            self.clearBuf();
            return;
        }
        var out = std.Io.Writer.Allocating.init(self.allocator);
        defer out.deinit();
        record.serialize(&out.writer) catch return;
        const frame = self.allocator.dupe(u8, out.written()) catch return;
        self.buf.append(self.allocator, frame) catch {
            self.allocator.free(frame);
            return;
        };
        switch (record.kind) {
            .insert, .update, .delete => self.pending_write = true,
            .commit => self.pending_ready = true,
            else => {},
        }
    }

    /// Ships the buffered committed batch, optionally blocking until it reaches quorum.
    ///
    /// The heart of the primary's send path. Does nothing unless a `commit` has been
    /// seen ([`DurableReplicator.pending_ready`]); a commit with no data mutations
    /// clears the buffer and returns. Otherwise it assigns the batch its sequence span
    /// (`base..last`, advancing [`DurableReplicator.next_seq`]), retains it in the ring
    /// and appends it to the backfill (a backfill failure is logged and tolerated,
    /// falling back to ring-only retention), then sends it.
    ///
    /// Two delivery modes, selected by `await_quorum`:
    ///  * **Synchronous** (`true`): a ship or reconnect failure propagates, and after a
    ///    successful ship it waits on [`QuorumTracker.awaitQuorum`] so the caller only
    ///    proceeds once the write is durable on a majority.
    ///  * **Asynchronous** (`false`): a failure is logged and *swallowed*, the write
    ///    is already committed locally and the follower catches up on a later batch,
    ///    so replication never blocks or fails a local commit.
    ///
    /// If disconnected it first runs [`DurableReplicator.reconnectAndCatchUp`]. On
    /// success it opportunistically prunes the backfill via
    /// [`DurableReplicator.checkpointBackfill`]. The `defer clearBuf` guarantees the
    /// pending buffer is reset on every path.
    pub fn shipPending(self: *DurableReplicator, await_quorum: bool) !void {
        if (!self.pending_ready) return;
        defer self.clearBuf();
        if (!self.pending_write or self.buf.items.len == 0) return;
        const base = self.next_seq;
        const last = base + self.buf.items.len - 1;
        self.next_seq = last + 1;
        try self.retain(base, last);
        if (self.backfill) |bl| bl.append(base, last, self.buf.items) catch |err| {
            log.warn("backfill append failed: {any}; ring-only backfill for this batch", .{err});
        };

        // Fan out the (single, shared) batch to every follower link. Each link
        // ships independently: a slow or dead follower marks itself disconnected
        // and is skipped, so it can never block the others or the primary. Under
        // synchronous replication the quorum wait below is what enforces
        // durability across the fleet; a per-link failure is surfaced immediately
        // (fail-fast) to preserve the original single-follower semantics.
        const frames = proto.ReplFrames{ .epoch = self.epoch, .base_seq = base, .frames = self.buf.items };
        for (self.links.items) |*link| {
            if (!link.connected) {
                self.reconnectAndCatchUpLink(link) catch |err| {
                    if (await_quorum) return err;
                    log.warn("replica {d}: reconnect failed: {any}; committed locally, will catch up", .{ link.id, err });
                    continue;
                };
            } else {
                _ = link.client.shipAndRecord(frames, &self.tracker, link.id) catch |err| {
                    link.connected = false;
                    if (await_quorum) return err;
                    log.warn("replica {d}: async ship failed (follower unreachable?): {any}; committed locally, will catch up", .{ link.id, err });
                    continue;
                };
            }
        }
        if (await_quorum) try self.tracker.awaitQuorum(self.io, last, self.timeout_ms);
        self.checkpointBackfill();
    }

    /// Compacts the backfill log up to the point confirmed by every follower.
    ///
    /// A no-op without a backfill or without followers. Computes the safe watermark as
    /// [`QuorumTracker.minConfirmed`] over the expected follower count
    /// (`total_replicas - 1`, excluding the primary), and only prunes if that is
    /// non-zero (every follower has reported past some point). Pruning to the *minimum*
    /// confirmed seq, not the quorum point, guarantees no follower can still need a
    /// batch that was dropped. Prune failures are logged and tolerated.
    fn checkpointBackfill(self: *DurableReplicator) void {
        const bl = self.backfill orelse return;
        const expected: u32 = if (self.tracker.total_replicas > 0) self.tracker.total_replicas - 1 else 0;
        if (expected == 0) return;
        const safe = self.tracker.minConfirmed(expected);
        if (safe == 0) return;
        bl.checkpoint(safe) catch |err| log.warn("backfill checkpoint failed: {any}", .{err});
    }

    /// Drops the current pending batch without shipping it, e.g. on a transaction
    /// abort observed outside [`DurableReplicator.onRecord`].
    pub fn discardPending(self: *DurableReplicator) void {
        self.clearBuf();
    }
};
