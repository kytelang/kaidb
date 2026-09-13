//! Durable checkpoint marker for the write-ahead log.
//!
//! A checkpoint is the small piece of persistent state that tells recovery
//! *where to start*. On its own the WAL is an ever-growing stream of records;
//! without a marker, restart would have to replay from the very beginning of
//! log history. The checkpoint pins the boundary between "already made durable
//! on the data pages" and "must be replayed": everything at or before
//! [`CheckpointRecord.last_flushed_lsn`] is known to be safely on disk, so
//! recovery reopens the log segment named by [`CheckpointRecord.file_seq`] and
//! replays forward from there.
//!
//! The record is deliberately tiny (two `u64`s) and is stored as a single JSON
//! object in a file literally named `CHECKPOINT` alongside the WAL segments in
//! the database directory. JSON is used rather than a packed binary layout
//! because the payload is trivial, written rarely (once per checkpoint, not per
//! transaction), and being human-readable makes on-disk state easy to inspect
//! during recovery debugging.
//!
//! The one invariant that matters here is **atomic replacement**. A checkpoint
//! file that is torn (half-written) during a crash would be worse than no
//! checkpoint at all, because recovery would trust a bogus start point.
//! [`CheckpointRecord.save`] therefore never writes the live file in place: it
//! writes `CHECKPOINT.tmp`, `fsync`s it, and then `rename`s it over the real
//! name. `rename` within a directory is atomic on POSIX filesystems, so a
//! reader (or a crash) sees either the complete old record or the complete new
//! one, never a mixture.
//!
//! This module sits under `durability/` next to `write_ahead_log.zig`: the WAL
//! advances `last_flushed_lsn` as pages are flushed, and a checkpoint captures
//! that watermark into this record so the next startup can prune replay.

const std = @import("std");
/// Alias for `std.mem`, used for the caller-supplied allocator type.
const mem = std.mem;
/// Alias for `std.fmt`, used to format paths and serialise the JSON payload.
const fmt = std.fmt;
/// Alias for `std.Io`, the I/O context threaded through load/save so the
/// filesystem calls run on the engine's chosen I/O backend rather than blocking
/// globals.
const Io = std.Io;
/// Alias for `std.Io.Dir`, the directory-relative file API used for reading,
/// creating, syncing, and renaming the checkpoint file.
const Dir = Io.Dir;

/// The persisted "where to start recovery" marker for the write-ahead log.
///
/// Holds just enough state to resume: which log segment to reopen
/// ([`file_seq`]) and how far replay may safely be skipped ([`last_flushed_lsn`]).
/// Instances are cheap value types; the durable form is a JSON object in the
/// [`CHECKPOINT_FILENAME`] file. See [`load`] to read it (absence is treated as
/// a fresh database) and [`save`] to write it atomically.
pub const CheckpointRecord = struct {
    /// Sequence number of the WAL segment file recovery should reopen and
    /// replay forward from. Corresponds to the log segment that was current
    /// when this checkpoint was taken.
    file_seq: u64,
    /// Highest log sequence number known to be durably applied to the data
    /// pages at checkpoint time. Recovery may skip everything at or below this
    /// LSN; anything above it must be replayed. Defaults to `0`, meaning "no
    /// progress yet, replay from the start of the segment".
    last_flushed_lsn: u64 = 0,
    /// Fixed on-disk filename for the checkpoint record, stored in the database
    /// directory beside the WAL segments. The temporary staging file used by
    /// [`save`] is this name with a `.tmp` suffix.
    const CHECKPOINT_FILENAME = "CHECKPOINT";

    /// Reads the checkpoint record from `dir_path`, returning a zeroed record
    /// when none exists yet.
    ///
    /// A missing file is not an error: a brand-new database has never
    /// checkpointed, so both `error.FileNotFound` and a path-formatting failure
    /// yield `CheckpointRecord{ .file_seq = 0, .last_flushed_lsn = 0 }`, which
    /// tells recovery to start from the beginning. Any other I/O error (a
    /// present-but-unreadable file) propagates to the caller.
    ///
    /// The file contents are parsed as the JSON written by [`save`]. Parsing
    /// uses a throwaway arena that is torn down before returning, and the
    /// resulting value is copied out by value, so the returned record borrows
    /// nothing from the arena or the read buffer.
    pub fn load(allocator: mem.Allocator, io: Io, dir_path: []const u8) !CheckpointRecord {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const checkpoint_path = fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir_path, CHECKPOINT_FILENAME }) catch return CheckpointRecord{ .file_seq = 0, .last_flushed_lsn = 0 };
        const data = Dir.readFileAlloc(.cwd(), io, checkpoint_path, allocator, @enumFromInt(100)) catch |err| {
            if (err == error.FileNotFound) return CheckpointRecord{ .file_seq = 0, .last_flushed_lsn = 0 };
            return err;
        };
        defer allocator.free(data);
        const gpa = std.heap.page_allocator;
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        const parsed = try std.json.parseFromSlice(CheckpointRecord, arena.allocator(), data, .{});
        return parsed.value;
    }

    /// Writes this record durably into `dir_path`, replacing any prior
    /// checkpoint atomically.
    ///
    /// The write is crash-safe by construction and must stay that way: the
    /// payload is serialised into `CHECKPOINT.tmp`, `fsync`ed via
    /// [`Dir.createFile`]'s handle `sync`, and only then `rename`d over
    /// [`CHECKPOINT_FILENAME`]. Because a same-directory rename is atomic, a
    /// crash at any point leaves either the complete previous record or the
    /// complete new one on disk, never a torn file that recovery would
    /// misread. The temporary file is `fsync`ed *before* the rename so its
    /// bytes are on the platter when the rename makes it visible.
    ///
    /// The JSON is hand-formatted into a fixed 128-byte stack buffer (two
    /// `u64`s and their keys always fit), so no allocation occurs. Returns any
    /// filesystem error from creating, writing, syncing, or renaming.
    pub fn save(self: CheckpointRecord, io: Io, dir_path: []const u8) !void {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const tmp_path = try fmt.bufPrint(&path_buf, "{s}/CHECKPOINT.tmp", .{dir_path});

        var buf: [128]u8 = undefined;
        const data = try fmt.bufPrint(&buf, "{{\"file_seq\":{}, \"last_flushed_lsn\":{}}}", .{ self.file_seq, self.last_flushed_lsn });

        var file = try Dir.createFile(.cwd(), io, tmp_path, .{});
        defer file.close(io);
        try file.writeStreamingAll(io, data);
        try file.sync(io);

        var final_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const final_path = try fmt.bufPrint(&final_path_buf, "{s}/{s}", .{ dir_path, CHECKPOINT_FILENAME });
        try Dir.rename(.cwd(), tmp_path, .cwd(), final_path, io);
    }
};
