//! Write-ahead log (WAL) for NovaDB: the durability backbone of the storage
//! engine.
//!
//! Every mutation the engine intends to make is first serialised as a
//! [`LogRecord`] and appended here, before the corresponding page is written
//! back to the data file. This is the write-ahead rule: the log record for a
//! change reaches stable storage no later than the page it describes, so a
//! crash can always be repaired by replaying the log forward from the last
//! checkpoint. Recovery is what [`WriteAheadLog.replay`] performs; it is the
//! reason the engine can promise durability and atomicity across a `kill -9`.
//!
//! ## On-disk shape
//!
//! The log is a sequence of numbered segment files named `NNNNNN.wal`
//! (zero-padded six digits, see [`WriteAheadLog.getFilePath`]). Only the
//! highest-numbered file is "active" and appended to; when it grows past
//! `max_file_size` the log rotates to a fresh segment
//! ([`WriteAheadLog.rotateLocked`]). Each segment begins with a fixed 16-byte
//! header ([`WalHeader`]) holding the segment's start and last LSN; records
//! follow immediately after byte 16, which is why `file_size` is initialised to
//! 16 everywhere and every offset computation reserves that prefix.
//!
//! ## LSNs and the flush frontier
//!
//! An LSN (log sequence number) is a monotonically increasing 64-bit stamp.
//! `lsn` is the highest LSN assigned to any appended record; `flushed_lsn` is
//! the highest LSN that has actually reached the OS and been `fsync`'d. The gap
//! between the two is exactly the set of records still buffered in memory. A
//! caller that needs a record to survive a crash must wait until
//! `flushed_lsn >= its LSN`, which is what an explicit [`WriteAheadLog.sync`]
//! (or the `sync`-suffixed append path) guarantees.
//!
//! ## Buffering vs. durability
//!
//! Appends normally accumulate in an in-memory [`Buffer`] and are flushed to the
//! file lazily: when the buffer would overflow `max_buffer_size`, when
//! `flush_interval_in_ms` has elapsed, or on an explicit flush/rotate/checkpoint.
//! This batches many small records into few write syscalls. Setting
//! `skip_buffers` degrades to flush-and-sync on every append, trading throughput
//! for the tightest possible durability window (used where a record must never
//! be lost). [`WriteAheadLog.appendAndSync`] is the per-call version of that
//! promise.
//!
//! ## Concurrency
//!
//! A single `wal_mutex` serialises all mutating operations. Every public entry
//! point (`append`, `flush`, `sync`, `rotate`, `checkpoint`, ...) takes it and
//! delegates to a `*Locked` helper that assumes the lock is already held, so the
//! helpers can call one another without re-entering the mutex. The LSN counters
//! are `std.atomic.Value` so a reader that only needs the current frontier can
//! observe it without taking the mutex.
//!
//! ## Checkpoints and truncation
//!
//! [`WriteAheadLog.checkpoint`] flushes, records the current segment sequence and
//! flushed LSN in a [`CheckpointRecord`] on disk, rotates, and drops now-obsolete
//! segments. Recovery starts from the checkpoint's segment rather than segment 1,
//! so the log does not have to be replayed from the beginning of time. A separate
//! age-based [`WriteAheadLog.truncate`] garbage-collects segments older than
//! `retain_logs_days`.
//!
//! ## Corruption tolerance on replay
//!
//! A crash can leave a torn tail: a half-written record, or one whose CRC no
//! longer matches. [`WriteAheadLog.replayFile`] treats these defensively. A
//! checksum-corrupt record in the middle is logged and skipped (using its length
//! prefix to step over it); a torn or implausibly-sized record is taken as the
//! end of valid data and replay stops there. This is safe because the WAL is
//! append-only, so damage can only live at the tail of the last segment.
//!
//! ## Replication hook
//!
//! An optional `ship_callback` is invoked with each appended record, which is how
//! a leader ships log records to replicas. It is a plain function pointer plus an
//! opaque context so this module does not depend on the replication subsystem.

/// The Zig standard library.
const std = @import("std");
/// The async/blocking I/O abstraction used throughout the engine; every file
/// and directory operation here takes an `Io` so the WAL can run on either a
/// blocking or an event-loop backend.
const Io = std.Io;
/// Directory handle type, used to create the WAL directory and enumerate,
/// open, and delete `.wal` segment files.
const Dir = Io.Dir;
/// File handle type for reading and writing individual WAL segments.
const File = Io.File;
/// Alias for `std.mem`, the slice/byte utilities.
const mem = std.mem;
/// Alias for `std.fmt`, used to format segment paths and parse segment
/// sequence numbers out of filenames.
const fmt = std.fmt;
/// Alias for `std.testing`, the unit-test helpers.
const testing = std.testing;
/// CRC-32 implementation. Record-level checksums live in [`LogRecord`]'s own
/// serialisation; this alias is kept alongside it for the same purpose.
const crc32 = std.hash.Crc32;
/// Alias for `std.json`.
const json = std.json;
/// The WAL-specific error set (write failed, header write failed, and so on),
/// returned by the durability-critical operations so callers can distinguish a
/// genuine I/O failure from a benign no-op.
const WalError = @import("wal_error.zig").WalError;

/// On-disk checkpoint marker: records which segment and flushed LSN recovery
/// should resume from. Written by [`WriteAheadLog.checkpoint`] and read back by
/// [`WriteAheadLog.replay`].
const CheckpointRecord = @import("checkpoint.zig").CheckpointRecord;

/// Timing/metrics sink for WAL operations.
///
/// This is a stand-in shape whose `wal.start`/`wal.stop` methods are no-ops:
/// they exist so the append/flush/sync/rotate/replay paths can be instrumented
/// uniformly (`start` at entry, `stop` on `defer`) without the real metrics
/// engine having to be linked in here. A production build can substitute a type
/// with the same method surface.
const EngineMetrics = struct {
    /// Namespace grouping the per-operation timers. Its methods are deliberately
    /// empty so metrics collection compiles to nothing when disabled.
    wal: struct {
        /// Begins timing operation `op` and returns a [`StopWatch`] handle. A
        /// no-op that returns a default-initialised watch.
        pub fn start(self: anytype, io: anytype, op: anytype) StopWatch {
            _ = self; _ = io; _ = op;
            return .{};
        }
        /// Ends the timing started by [`EngineMetrics.wal.start`]. A no-op.
        pub fn stop(self: anytype, io: anytype, sw: *StopWatch, op: anytype) void {
            _ = self; _ = io; _ = sw; _ = op;
        }
    } = .{},
};
/// Monotonic stopwatch handle from the shared `utils` module, threaded through
/// the metrics start/stop calls.
const StopWatch = @import("utils").StopWatch;
/// Enumeration of log-record kinds (insert, update, delete, commit, ...); the
/// discriminant carried inside each serialised [`LogRecord`].
const LogRecordKind = @import("../common/common.zig").LogRecordKind;
/// The unit of the log: one serialisable mutation. It owns its own
/// serialise/deserialise/size/checksum logic; the WAL only frames and orders
/// these records, it does not interpret their payloads.
const LogRecord = @import("../common/common.zig").LogRecord;
/// A growable, position-tracked byte buffer that stages appended records in
/// memory before they are flushed to the active segment file.
///
/// It is a thin arena over a single `[]u8`: [`Buffer.writeBytes`] doubles the
/// backing allocation on overflow, [`Buffer.reset`] rewinds the write position
/// to zero without freeing (so the allocation is reused across flush cycles),
/// and [`Buffer.slice`] exposes only the written prefix. The nested
/// [`Buffer.Writer`] adapts it to the `writer`-style interface that
/// [`LogRecord.serialize`] expects.
const Buffer = struct {
    /// Allocator backing [`Buffer.data`]; used for the initial allocation, the
    /// growth `realloc`, and the `free` in [`Buffer.deinit`].
    allocator: std.mem.Allocator,
    /// The backing storage. Only the first `pos` bytes are meaningful; the
    /// remainder is spare capacity.
    data: []u8,
    /// Current write position, i.e. the number of valid bytes in [`Buffer.data`]
    /// and the offset at which the next write lands.
    pos: usize,

    /// Allocates a buffer with `capacity` bytes of initial storage and an empty
    /// write position.
    pub fn init(allocator: std.mem.Allocator, capacity: usize) !Buffer {
        const data = try allocator.alloc(u8, capacity);
        return Buffer{
            .allocator = allocator,
            .data = data,
            .pos = 0,
        };
    }

    /// Frees the backing storage and poisons the buffer. Using it afterwards is
    /// illegal.
    pub fn deinit(self: *Buffer) void {
        self.allocator.free(self.data);
        self.* = undefined;
    }

    /// Rewinds the write position to zero, discarding buffered bytes without
    /// releasing capacity. Called after a successful flush so the same
    /// allocation serves the next batch.
    pub fn reset(self: *Buffer) void {
        self.pos = 0;
    }

    /// Returns a read-only view of the bytes written so far (`data[0..pos]`),
    /// the payload handed to the file on flush.
    pub fn slice(self: Buffer) []const u8 {
        return self.data[0..self.pos];
    }

    /// Appends `bytes`, growing the backing allocation if it would not fit.
    ///
    /// On overflow the capacity is at least doubled (and at least enough for the
    /// incoming write, whichever is larger), so amortised append is O(1).
    /// Returns the number of bytes written (always `bytes.len`), or the
    /// allocator error if the `realloc` fails.
    pub fn writeBytes(self: *Buffer, bytes: []const u8) std.mem.Allocator.Error!usize {
        if (self.pos + bytes.len > self.data.len) {
            const new_cap = @max(self.data.len * 2, self.pos + bytes.len);
            self.data = try self.allocator.realloc(self.data, new_cap);
        }
        @memcpy(self.data[self.pos .. self.pos + bytes.len], bytes);
        self.pos += bytes.len;
        return bytes.len;
    }

    /// Returns a [`Buffer.Writer`] adapter bound to this buffer, so record
    /// serialisation can target it through a generic writer interface.
    pub fn writer(self: *Buffer) Writer {
        return .{ .buffer = self };
    }

    /// A minimal writer front-end over a [`Buffer`].
    ///
    /// It supplies exactly the three methods [`LogRecord.serialize`] needs, each
    /// forwarding to [`Buffer.writeBytes`]. It is a value that borrows the
    /// buffer pointer, so copies are cheap and all share the same underlying
    /// storage.
    pub const Writer = struct {
        /// The buffer every write is forwarded to.
        buffer: *Buffer,

        /// Appends the whole slice.
        pub fn writeAll(self: Writer, bytes: []const u8) !void {
            _ = try self.buffer.writeBytes(bytes);
        }

        /// Appends a single byte.
        pub fn writeByte(self: Writer, byte: u8) !void {
            _ = try self.buffer.writeBytes(&[_]u8{byte});
        }

        /// Appends `value` as a `T`-sized integer in the given byte order.
        ///
        /// Encodes into a stack scratch buffer with `std.mem.writeInt` and then
        /// writes it, so the caller controls endianness explicitly (WAL headers
        /// and record fields are little-endian on disk).
        pub fn writeInt(self: Writer, comptime T: type, value: T, endian: std.builtin.Endian) !void {
            var bytes: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(T, &bytes, value, endian);
            try self.writeAll(&bytes);
        }
    };
};
/// Wall-clock source from the shared `utils` module. It is `Io`-bound so time
/// reads (used for the flush-interval timer and last-sync bookkeeping) go
/// through the same I/O backend as everything else.
const Now = @import("utils").Now;
/// Scoped logger for the WAL subsystem; every diagnostic in this file tags its
/// output with `.wal`.
const log = std.log.scoped(.wal);

/// Immutable configuration passed to [`WriteAheadLog.init`].
///
/// It fixes the log directory, the rotation and buffering thresholds, the
/// retention window, and the I/O backend for the lifetime of the log. Fields
/// with defaults are optional at the call site.
const WalConfigData = struct {
    /// Directory that holds the `.wal` segments and the checkpoint marker.
    /// Created on init if it does not exist. The string is duped into the log's
    /// own allocator, so the caller need not keep it alive.
    dir_path: []const u8,
    /// Rotate to a new segment once the active file reaches this many bytes.
    max_file_size: usize,
    /// In-memory staging buffer capacity; also the threshold at which a pending
    /// append triggers a flush before it is written.
    max_buffer_size: usize,
    /// Maximum time an append may sit unflushed. An append that finds this many
    /// milliseconds have elapsed since the last sync flushes first, bounding the
    /// durability lag even under a light write rate.
    flush_interval_in_ms: i64,
    /// The I/O backend used for every file/directory/clock operation.
    io: Io,
    /// Age in days beyond which [`WriteAheadLog.truncate`] deletes retired
    /// segments. Defaults to 15.
    retain_logs_days: u32 = 15,
    /// Whether retired segments are archived rather than simply removed. Part of
    /// the configuration surface consumed by the wider durability subsystem.
    log_archive_enabled: bool,
    /// Destination directory for archived segments when
    /// [`WalConfigData.log_archive_enabled`] is set.
    log_archive_dest_path: []const u8,
    /// If set, every append flushes and syncs immediately (no in-memory
    /// batching), giving the tightest durability window at the cost of
    /// throughput. Defaults to false. See [`WriteAheadLog.appendAndSync`] for
    /// the per-call equivalent.
    skip_buffers: bool = false,
};

/// The fixed 16-byte prefix at the start of every WAL segment file.
///
/// It stores two little-endian `u64`s: the LSN the segment starts at and the
/// last LSN written to it. On recovery the last LSN of the active segment
/// re-seeds the log's in-memory counters ([`WriteAheadLog.init`]), so the LSN
/// space is continuous across restarts. The fields are atomic because the
/// header is updated on the append path while readers may inspect it. On disk
/// only the raw 16 bytes are stored, in the layout `[startLSN:8][lastLSN:8]`.
pub const WalHeader = struct {
    /// The first LSN belonging to this segment. Set to the current last LSN when
    /// a fresh segment is opened (see [`WalHeader.update`]).
    startLSN: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// The highest LSN written into this segment, persisted so recovery knows
    /// where the log left off.
    lastLSN: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    /// Reads and decodes the 16-byte header from offset 0 of `file`.
    ///
    /// Used both when reopening an existing active segment on startup and at the
    /// start of replaying each segment. Errors if the positional read fails
    /// (e.g. a file shorter than 16 bytes).
    pub fn read(io: std.Io, file: File) !WalHeader {
        var header_buf: [16]u8 = undefined;
        _ = try file.readPositionalAll(io, header_buf[0..], 0);
        const slsn = std.mem.readInt(u64, header_buf[0..8], .little);
        const llsn = std.mem.readInt(u64, header_buf[8..16], .little);

        return WalHeader{
            .startLSN = std.atomic.Value(u64).init(slsn),
            .lastLSN = std.atomic.Value(u64).init(llsn),
        };
    }
    /// Encodes and writes the header to offset 0 of `file`.
    ///
    /// Snapshots both LSN counters atomically, then writes the fixed 16 bytes.
    /// Called when finalising a segment before it is closed and when stamping a
    /// freshly rotated segment, so the on-disk frontier stays current.
    pub fn write(self: *WalHeader, io: Io, file: File) !void {
        const slsn = self.startLSN.load(.monotonic);
        const llsn = self.lastLSN.load(.monotonic);

        var header_buf: [16]u8 = undefined;
        std.mem.writeInt(u64, header_buf[0..8], slsn, .little);
        std.mem.writeInt(u64, header_buf[8..16], llsn, .little);
        try file.writePositionalAll(io, header_buf[0..], 0);
    }

    /// Reseeds both LSN fields to the current last LSN, marking the start of a
    /// new segment.
    ///
    /// Invoked during rotation so the new segment's `startLSN` equals the LSN it
    /// begins recording at, giving each segment a self-describing LSN range.
    pub fn update(self: *WalHeader) void {
        const llsn = self.lastLSN.load(.monotonic);
        self.startLSN.store(llsn, .monotonic);
        self.lastLSN.store(llsn, .monotonic);
    }
};

/// The result of replaying the log: the recovered records plus the arena that
/// owns them.
///
/// The records and every allocation they point into live in `arena`; the caller
/// takes ownership and must `deinit`+`destroy` the arena once it has finished
/// applying them. Bundling the arena with the slice keeps recovered records
/// alive as one unit rather than requiring per-record frees.
pub const ReplayResult = struct {
    /// The arena backing every recovered [`LogRecord`]. Caller-owned; free it to
    /// release the whole result at once.
    arena: *std.heap.ArenaAllocator,
    /// The recovered records in log order, ready to be re-applied to the engine.
    records: []const LogRecord,
};

/// The write-ahead log itself: an append-only, segmented, crash-recoverable
/// record of every intended mutation.
///
/// Instances are heap-allocated ([`WriteAheadLog.init`] returns `*WriteAheadLog`)
/// and single-owner. All mutating methods serialise on [`WriteAheadLog.wal_mutex`];
/// the atomic LSN counters may be read without it. See the file-level
/// documentation for the durability rule, LSN model, buffering policy, and
/// recovery behaviour this type implements.
pub const WriteAheadLog = struct {
    /// Public alias so callers can name the config type as
    /// `WriteAheadLog.WalConfig`.
    pub const WalConfig = WalConfigData;
    /// Allocator for the log's own bookkeeping: the struct itself, the duped
    /// `dir_path`, the staging buffer, and transient path strings.
    allocator: mem.Allocator,
    /// The I/O backend for all file, directory, and clock operations.
    io: Io,
    /// Owned copy of the WAL directory path. Freed in [`WriteAheadLog.deinit`].
    dir_path: []const u8,
    /// Sequence number of the active segment; the file is `dir_path/NNNNNN.wal`.
    /// Increments on every rotation and is recovered as the highest existing
    /// segment on startup.
    current_seq: u64,
    /// The open active segment, or null before the first append (a lazily
    /// created log has no file until it has something to write).
    current_file: ?File,
    /// Optional metrics sink; when present, each operation is timed. Null
    /// disables instrumentation with no branch cost beyond the null check.
    engine_metrics: ?*EngineMetrics,
    /// In-memory staging buffer holding serialised records not yet flushed.
    buffer: Buffer,
    /// Buffer-overflow / flush threshold in bytes. An append whose record would
    /// push past this flushes first.
    max_buffer_size: usize,
    /// Segment-size limit in bytes; crossing it forces a rotation.
    max_file_size: usize,
    /// Logical size of the active segment on disk, in bytes, INCLUDING the
    /// 16-byte header. Initialised to 16 (header only) and advanced by each
    /// record's serialised size. Used both to place buffer writes and to decide
    /// when to rotate.
    file_size: usize = 16,
    /// In-memory copy of the active segment's [`WalHeader`], kept current so it
    /// can be written back on rotation without a re-read.
    header: WalHeader = .{},
    /// Highest LSN durably on disk (flushed AND fsync'd). The durability
    /// frontier: a record is safe once this reaches its LSN.
    flushed_lsn: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Highest LSN assigned to any appended record, durable or not. Advanced by
    /// [`WriteAheadLog.incrementLSN`].
    lsn: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    /// Count of records buffered since the last successful sync. Reset to zero on
    /// sync; informational bookkeeping for the flush policy.
    pending_writes: u32 = 0,
    /// Wall-clock time (ms) of the last successful sync, used to evaluate the
    /// `flush_interval_in_ms` deadline on the append path.
    last_sync_time: i64 = 0,
    /// Wall-clock source used for the flush-interval timer and sync timestamps.
    now: Now,
    /// True when data has been written to the file but not yet fsync'd, so
    /// [`WriteAheadLog.syncLocked`] knows an fsync is actually required. Avoids
    /// syncing when nothing changed.
    needs_sync: bool = false,
    /// Test-only hook: when set, the next flush returns this error once (then
    /// clears itself), so failure paths can be exercised deterministically.
    test_inject_write_error: ?WalError = null,
    /// The mutex serialising every mutating operation. Public methods take it;
    /// `*Locked` helpers assume it is already held.
    wal_mutex: std.Io.Mutex = .init,
    /// Retention window in days for [`WriteAheadLog.truncate`]'s age-based GC.
    retain_logs_days: u32,

    /// When true, every append flushes and syncs immediately (no batching). Set
    /// from [`WalConfigData.skip_buffers`].
    skip_buffers: bool = false,
    /// Maximum time (ms) a record may sit unflushed before an append forces a
    /// flush; bounds durability lag under a slow write rate.
    flush_interval_in_ms: i64,
    /// Opaque context passed back to [`WriteAheadLog.ship_callback`]; typically
    /// the replication manager. Kept as `?*anyopaque` so this module does not
    /// depend on the replication types.
    replication_manager: ?*anyopaque = null,
    /// Optional per-record replication hook, invoked with each appended record
    /// so a leader can ship it to replicas. Null on a standalone node.
    ship_callback: ?*const fn (ctx: ?*anyopaque, record: LogRecord) void = null,

    /// Constructs a log over `config.dir_path`, recovering any existing state.
    ///
    /// Creates the directory if absent, then scans it for the highest-numbered
    /// `.wal` segment. If one exists it becomes the active segment: its size and
    /// [`WalHeader`] are read back and the in-memory `lsn`/`flushed_lsn` counters
    /// are re-seeded from the header's last LSN, so appends continue from where
    /// the previous run stopped. If none exists the log starts empty and defers
    /// creating a file until the first flush. Returns a heap-allocated instance
    /// the caller owns and must release with [`WriteAheadLog.deinit`].
    pub fn init(allocator: mem.Allocator, engine_metrics: ?*EngineMetrics, config: WalConfigData) !*WriteAheadLog {
        const io = config.io;

        Dir.createDirPath(.cwd(), io, config.dir_path) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };

        const buf_size = if (config.skip_buffers) 4096 else config.max_buffer_size;

        var self = try allocator.create(WriteAheadLog);
        self.* = .{
            .allocator = allocator,
            .io = io,
            .dir_path = try allocator.dupe(u8, config.dir_path),
            .current_seq = 0,
            .current_file = null,
            .buffer = try Buffer.init(allocator, buf_size),
            .max_buffer_size = buf_size,
            .max_file_size = config.max_file_size,
            .retain_logs_days = config.retain_logs_days,
            .header = .{},
            .now = Now{ .io = io },
            .flush_interval_in_ms = config.flush_interval_in_ms,
            .engine_metrics = engine_metrics,
            .skip_buffers = config.skip_buffers,
            .replication_manager = null,
            .ship_callback = null,
        };

        self.last_sync_time = self.now.toMilliSeconds();
        


        var max_seq: u64 = 0;
        var found_file = false;

        if (Dir.openDir(.cwd(), io, self.dir_path, .{ .iterate = true })) |dir| {
            var wal_dir = dir;
            defer wal_dir.close(io);
            var dir_iter = wal_dir.iterate();
            while (dir_iter.next(io) catch null) |entry| {
                if (entry.kind == .file and std.mem.endsWith(u8, entry.name, ".wal")) {
                    const seq_str = entry.name[0 .. entry.name.len - 4];
                    const seq = fmt.parseUnsigned(u64, seq_str, 10) catch continue;
                    if (!found_file or seq > max_seq) {
                        max_seq = seq;
                    }
                    found_file = true;
                }
            }
        } else |_| {}

        if (found_file) {
            self.current_seq = max_seq;
            const file_path = try self.getFilePath(self.current_seq);
            defer self.allocator.free(file_path);
            self.current_file = Dir.openFile(.cwd(), io, file_path, .{ .mode = .read_write }) catch null;
            if (self.current_file) |file| {
                const stat = file.stat(io) catch null;
                if (stat) |s| {
                    self.file_size = s.size;
                }
                self.header = try WalHeader.read(self.io, file);
                const persisted_lsn = self.header.lastLSN.load(.monotonic);
                self.lsn.store(persisted_lsn, .monotonic);
                self.flushed_lsn.store(persisted_lsn, .monotonic);
            }
        } else {
            self.current_seq = 0;
            self.current_file = null;
        }
        return self;
    }

    /// Flushes any buffered records, closes the active segment, and frees the
    /// instance.
    ///
    /// The final [`WriteAheadLog.flush`] ensures nothing buffered is lost on a
    /// clean shutdown. After this returns the pointer is invalid. Propagates a
    /// [`WalError`] if the closing flush fails.
    pub fn deinit(self: *WriteAheadLog) WalError!void {
        try self.flush();
        if (self.current_file) |file| {
            file.close(self.io);
        }
        self.buffer.deinit();
        self.allocator.free(self.dir_path);
        self.allocator.destroy(self);
    }

    /// Builds the on-disk path for segment `seq` as
    /// `dir_path/NNNNNN.wal` (six-digit zero-padded).
    ///
    /// The caller owns the returned string and must free it with
    /// [`WriteAheadLog.allocator`]. The fixed-width naming is what makes segments
    /// sort and parse back into sequence numbers reliably.
    fn getFilePath(self: *WriteAheadLog, seq: u64) ![]u8 {
        return try fmt.allocPrint(self.allocator, "{s}/{d:0>6}.wal", .{ self.dir_path, seq });
    }

    /// Atomically allocates and returns the next LSN.
    ///
    /// A monotonic fetch-add: callers stamp a record with the value they get
    /// back before appending it. Lock-free, so LSN assignment does not require
    /// [`WriteAheadLog.wal_mutex`].
    pub fn incrementLSN(self: *WriteAheadLog) u64 {
        return self.lsn.fetchAdd(1, .monotonic) + 1;
    }

    /// Publishes an externally determined flushed frontier.
    ///
    /// Lets a caller that manages durability outside the normal sync path (for
    /// example after confirming a group of records reached disk) advance
    /// [`WriteAheadLog.flushed_lsn`].
    pub fn flushedLSN(self: *WriteAheadLog, flushed_lsn: u64) void {
        self.flushed_lsn.store(flushed_lsn, .monotonic);
    }

    /// Writes the staged buffer out to the active segment (no fsync).
    ///
    /// Assumes [`WriteAheadLog.wal_mutex`] is held. Creates the first segment
    /// lazily if none is open yet. The write lands at
    /// `file_size - buffer.pos`, i.e. it back-fills the region the records were
    /// already accounted for in `file_size` when they were appended, keeping the
    /// on-disk size and the logical size in step. A buffer larger than the
    /// tracked file size can only mean stale/inconsistent state, so it is dropped
    /// with a warning rather than corrupting the file. Sets `needs_sync` on a
    /// successful write and clears the buffer; a raw write failure is mapped to
    /// [`WalError`]`.WriteFailed`. The `test_inject_write_error` hook can force a
    /// one-shot failure here.
    fn flushBuffer(self: *WriteAheadLog) WalError!void {
        var sw: StopWatch = if (self.engine_metrics) |em| em.wal.start(self.io, .Flush) else .{};
        defer if (self.engine_metrics) |em| em.wal.stop(self.io, &sw, .Flush);

        if (self.buffer.pos == 0) {
            return;
        }
        if (self.current_file == null) {
            try self.rotateLocked();

            self.file_size = 16 + self.buffer.pos;
        }
        var file = self.current_file.?;

        if (self.test_inject_write_error) |e| {
            self.test_inject_write_error = null;
            return e;
        }

        if (self.buffer.pos == 0) return;
        if (self.buffer.pos > self.file_size) {
            log.warn("WAL flushBuffer: buffer.pos={d} > file_size={d}; dropping stale buffer", .{ self.buffer.pos, self.file_size });
            self.buffer.reset();
            return;
        }
        const write_offset = self.file_size - self.buffer.pos;
        file.writePositionalAll(self.io, self.buffer.slice(), write_offset) catch return WalError.WriteFailed;
        self.buffer.reset();
        self.needs_sync = true;
    }

    /// fsyncs the active segment, making everything written durable.
    ///
    /// Assumes [`WriteAheadLog.wal_mutex`] is held. No-ops when `needs_sync` is
    /// false, so redundant syncs are free. On success it advances
    /// [`WriteAheadLog.flushed_lsn`] to the current `lsn` (the just-synced writes
    /// are now durable), resets `pending_writes`, and stamps `last_sync_time` for
    /// the flush-interval timer. This is the step that actually satisfies the
    /// write-ahead durability guarantee.
    fn syncLocked(self: *WriteAheadLog) WalError!void {
        if (!self.needs_sync) return;

        var sw: StopWatch = if (self.engine_metrics) |em| em.wal.start(self.io, .Fsync) else .{};
        defer if (self.engine_metrics) |em| em.wal.stop(self.io, &sw, .Fsync);

        if (self.current_file) |file| {
            try file.sync(self.io);
            self.needs_sync = false;
            self.pending_writes = 0;
            self.last_sync_time = self.now.toMilliSeconds();
            self.flushed_lsn.store(self.lsn.load(.monotonic), .monotonic);
        }
    }

    /// Public fsync: takes the mutex and calls [`WriteAheadLog.syncLocked`].
    ///
    /// Note this only fsyncs; it does NOT first flush the in-memory buffer, so it
    /// makes durable what has already been written to the file. Use
    /// [`WriteAheadLog.flush`] to push the buffer out and sync in one step.
    pub fn sync(self: *WriteAheadLog) WalError!void {
        self.wal_mutex.lockUncancelable(self.io);
        defer self.wal_mutex.unlock(self.io);
        try self.syncLocked();
    }

    /// Buffer-to-disk-to-durable in one step, assuming the mutex is held.
    ///
    /// Writes the staged buffer ([`WriteAheadLog.flushBuffer`]) then fsyncs
    /// ([`WriteAheadLog.syncLocked`]); the shared inner routine behind
    /// [`WriteAheadLog.flush`] and the internal flush points on the append path.
    fn flushLocked(self: *WriteAheadLog) WalError!void {
        try self.flushBuffer();
        try self.syncLocked();
    }

    /// Public flush: takes the mutex and calls [`WriteAheadLog.flushLocked`],
    /// making every appended-but-buffered record durable.
    pub fn flush(self: *WriteAheadLog) WalError!void {
        self.wal_mutex.lockUncancelable(self.io);
        defer self.wal_mutex.unlock(self.io);
        try self.flushLocked();
    }

    /// Appends one record, honouring the buffering and rotation policy.
    ///
    /// Under the mutex it may flush first for three reasons: the buffer would
    /// overflow `max_buffer_size`, the `flush_interval_in_ms` deadline has passed
    /// with data pending, or the segment has reached `max_file_size` (which also
    /// rotates). It then serialises the record into the buffer, advances
    /// `file_size` and `pending_writes`, and updates the header's last LSN.
    /// When `skip_buffers` is set it flushes-and-syncs immediately. Finally, if a
    /// [`WriteAheadLog.ship_callback`] is registered the record is handed to it
    /// for replication. Note the record is not guaranteed durable on return
    /// unless a flush was triggered; use [`WriteAheadLog.appendAndSync`] when it
    /// must be.
    pub fn append(self: *WriteAheadLog, record: LogRecord) !void {
        self.wal_mutex.lockUncancelable(self.io);
        defer self.wal_mutex.unlock(self.io);

        var sw: StopWatch = if (self.engine_metrics) |em| em.wal.start(self.io, .Append) else .{};
        defer if (self.engine_metrics) |em| em.wal.stop(self.io, &sw, .Append);

        if (self.buffer.pos + record.size() >= self.max_buffer_size) {
            try self.flushLocked();
        }

        const now_ms = self.now.toMilliSeconds();
        if (now_ms - self.last_sync_time >= self.flush_interval_in_ms and self.buffer.pos > 0) {
            try self.flushLocked();
        }

        if (self.file_size >= self.max_file_size) {
            try self.flushLocked();
            try self.rotateLocked();
        }

        try LogRecord.serialize(record, self.buffer.writer());

        self.file_size += record.size();
        self.pending_writes += 1;
        self.header.lastLSN.store(self.lsn.load(.monotonic), .monotonic);

        if (self.skip_buffers) {
            try self.flushLocked();
        }

        if (self.ship_callback) |callback| {
            callback(self.replication_manager, record);
        }
    }

    /// Appends a record and forces it durable before returning.
    ///
    /// Like [`WriteAheadLog.append`] but always ends in a flush-and-sync, so on
    /// return the record has reached stable storage (`flushed_lsn` covers it).
    /// It skips the buffer-overflow and interval checks because it flushes
    /// unconditionally, but still rotates when the segment is full. Use for
    /// commit records and any mutation that must survive an immediate crash.
    pub fn appendAndSync(self: *WriteAheadLog, record: LogRecord) !void {
        self.wal_mutex.lockUncancelable(self.io);
        defer self.wal_mutex.unlock(self.io);

        var sw: StopWatch = if (self.engine_metrics) |em| em.wal.start(self.io, .Append) else .{};
        defer if (self.engine_metrics) |em| em.wal.stop(self.io, &sw, .Append);

        if (self.file_size >= self.max_file_size) {
            try self.flushLocked();
            try self.rotateLocked();
        }

        try LogRecord.serialize(record, self.buffer.writer());
        self.file_size += record.size();
        self.pending_writes += 1;
        self.header.lastLSN.store(self.lsn.load(.monotonic), .monotonic);
        try self.flushLocked();

        if (self.ship_callback) |callback| {
            callback(self.replication_manager, record);
        }
    }

    /// Public rotation: takes the mutex and calls
    /// [`WriteAheadLog.rotateLocked`].
    pub fn rotate(self: *WriteAheadLog) WalError!void {
        self.wal_mutex.lockUncancelable(self.io);
        defer self.wal_mutex.unlock(self.io);
        try self.rotateLocked();
    }

    /// Closes the current segment and opens a fresh one, assuming the mutex is
    /// held.
    ///
    /// Ordering matters for crash safety: it first syncs the outgoing segment,
    /// then writes its final [`WalHeader`] and closes it, so a segment is never
    /// left with a stale header. It then bumps `current_seq`, creates the next
    /// `NNNNNN.wal`, calls [`WalHeader.update`] so the new segment's LSN range
    /// starts correctly, and stamps its initial header. `file_size` resets to 16
    /// (header only) and `needs_sync` clears. A failure to write either header is
    /// surfaced as [`WalError`]`.FailedToWriteHeader` after closing the file.
    fn rotateLocked(self: *WriteAheadLog) WalError!void {
        try self.syncLocked();
        if (self.current_file) |file| {
            self.header.write(self.io, file) catch |err| {
                file.close(self.io);
                log.err("Failed to write WAL header during rotation: {s}", .{@errorName(err)});
                return WalError.FailedToWriteHeader;
            };
            file.close(self.io);

        }
        self.current_seq += 1;
        const file_path = try self.getFilePath(self.current_seq);
        defer self.allocator.free(file_path);
        self.current_file = try Dir.createFile(.cwd(), self.io, file_path, .{ .read = true, .truncate = false });
        if (self.current_file) |file| {
            self.header.update();
            self.header.write(self.io, file) catch |err| {
                file.close(self.io);
                log.err("Failed to write WAL header during rotation: {s}", .{@errorName(err)});
                return WalError.FailedToWriteHeader;
            };
        }
        self.file_size = 16;
        self.needs_sync = false;
    }

    /// Establishes a checkpoint: a recovery starting point that lets earlier
    /// segments be discarded.
    ///
    /// Under the mutex it flushes everything durable, writes a
    /// [`CheckpointRecord`] naming the current segment sequence and flushed LSN,
    /// rotates so the checkpoint sits on a clean segment boundary, and then
    /// deletes every segment older than the checkpoint via
    /// [`WriteAheadLog.truncateActiveLogs`] (whose failure is intentionally
    /// swallowed, since the checkpoint is already durable and stale segments are
    /// merely wasted space). After this, replay begins from the checkpoint's
    /// segment rather than segment 1.
    pub fn checkpoint(self: *WriteAheadLog) WalError!void {
        self.wal_mutex.lockUncancelable(self.io);
        defer self.wal_mutex.unlock(self.io);
        try self.flushLocked();
        const seq = if (self.current_file == null) 0 else self.current_seq;
        const cp = CheckpointRecord{ .file_seq = seq, .last_flushed_lsn = self.flushed_lsn.load(.monotonic) };
        try cp.save(self.io, self.dir_path);
        try self.rotateLocked();
        self.truncateActiveLogs(seq) catch {};
    }

    /// Deletes every segment strictly older than `checkpoint_seq`.
    ///
    /// Called right after a checkpoint: those segments describe changes that the
    /// checkpoint already covers, so they can never be needed by recovery.
    /// Iterates the directory, parses each `.wal` filename back to a sequence
    /// number, and unlinks the older ones. Individual delete failures are logged
    /// and skipped rather than aborting the sweep, and a directory it cannot open
    /// is treated as nothing to do.
    pub fn truncateActiveLogs(self: *WriteAheadLog, checkpoint_seq: u64) !void {
        var wal_dir = Dir.openDir(.cwd(), self.io, self.dir_path, .{ .iterate = true }) catch return;
        defer wal_dir.close(self.io);

        var dir_iter = wal_dir.iterate();
        while (dir_iter.next(self.io) catch null) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".wal")) continue;

            const seq_str = entry.name[0 .. entry.name.len - 4];
            const seq = fmt.parseUnsigned(u64, seq_str, 10) catch continue;

            if (seq < checkpoint_seq) {
                const file_path = self.getFilePath(seq) catch continue;
                defer self.allocator.free(file_path);
                Dir.deleteFile(.cwd(), self.io, file_path) catch |err| {
                    log.warn("truncateActiveLogs: failed to delete {s}: {}", .{ entry.name, err });
                };
            }
        }
    }

    /// Age-based garbage collection: deletes retired segments older than
    /// `retain_logs_days`.
    ///
    /// Distinct from [`WriteAheadLog.truncateActiveLogs`], which is
    /// checkpoint-relative; this one is time-relative. It computes a cutoff
    /// timestamp `now - retain_logs_days`, then for each `.wal` segment BELOW the
    /// active `current_seq` it stats the file and deletes it only if its mtime is
    /// older than the cutoff. The active segment is always kept. Per-file errors
    /// (open/stat/delete) are logged and skipped so one bad file does not stop
    /// the sweep; a directory it cannot open is a no-op.
    pub fn truncate(self: *WriteAheadLog) WalError!void {
        var sw: StopWatch = if (self.engine_metrics) |em| em.wal.start(self.io, .Truncate) else .{};
        defer if (self.engine_metrics) |em| em.wal.stop(self.io, &sw, .Truncate);

        const ns_per_day: i128 = 24 * 3600 * std.time.ns_per_s;
        const retain_ns: i128 = @as(i128, self.retain_logs_days) * ns_per_day;
        const now_ns: i128 = std.Io.Clock.now(.real, self.io).toNanoseconds();
        const cutoff_ns: i128 = now_ns - retain_ns;

        var wal_dir = Dir.openDir(.cwd(), self.io, self.dir_path, .{ .iterate = true }) catch return;
        defer wal_dir.close(self.io);

        var dir_iter = wal_dir.iterate();
        while (dir_iter.next(self.io) catch null) |entry| {
            if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".wal")) continue;

            const seq_str = entry.name[0 .. entry.name.len - 4];
            const seq = fmt.parseUnsigned(u64, seq_str, 10) catch continue;

            if (seq >= self.current_seq) continue;

            const file_path = self.getFilePath(seq) catch continue;
            defer self.allocator.free(file_path);

            const file = Dir.openFile(.cwd(), self.io, file_path, .{}) catch continue;
            const stat = file.stat(self.io) catch {
                file.close(self.io);
                continue;
            };
            file.close(self.io);

            const file_mtime: i128 = stat.mtime.toNanoseconds();
            if (file_mtime >= cutoff_ns) continue;

            Dir.deleteFile(.cwd(), self.io, file_path) catch |err| {
                log.warn("truncate: failed to delete {s}: {}", .{ entry.name, err });
                continue;
            };
            log.info("truncate: deleted {s} (seq {d}, age > {d} days)", .{
                entry.name, seq, self.retain_logs_days,
            });
        }
    }

    /// Reports whether the log holds any records.
    ///
    /// True if the active segment has grown past its 16-byte header (so it
    /// contains at least one record) or if any rotation has occurred
    /// (`current_seq > 0` implies earlier segments existed). Lets the engine
    /// decide whether recovery has anything to replay.
    pub fn hasData(self: *WriteAheadLog) bool {
        return (self.current_file != null and self.file_size > 16) or self.current_seq > 0;
    }

    /// Wipes the log back to an empty state: flushes, closes, and deletes all
    /// segments and the checkpoint marker.
    ///
    /// Destructive. Used when the log's contents are no longer wanted (for
    /// example a fresh database or after a full snapshot supersedes the log). It
    /// resets every in-memory counter (`current_seq`, `file_size`, buffer,
    /// header, `pending_writes`, `needs_sync`) so the instance behaves like a
    /// freshly initialised, empty log. A directory it cannot open is treated as
    /// already empty; individual delete failures are ignored.
    pub fn reset(self: *WriteAheadLog) WalError!void {
        try self.flush();

        if (self.current_file) |file| {
            file.close(self.io);
            self.current_file = null;
        }

        var wal_dir = Dir.openDir(.cwd(), self.io, self.dir_path, .{ .iterate = true }) catch return;
        defer wal_dir.close(self.io);

        var dir_iter = wal_dir.iterate();
        while (dir_iter.next(self.io) catch null) |entry| {
            if (entry.kind != .file) continue;
            if (!std.mem.endsWith(u8, entry.name, ".wal") and !std.mem.eql(u8, entry.name, "CHECKPOINT")) continue;

            const file_path = fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.dir_path, entry.name }) catch continue;
            defer self.allocator.free(file_path);
            Dir.deleteFile(.cwd(), self.io, file_path) catch {};
        }

        self.current_seq = 0;
        self.file_size = 16;
        self.buffer.reset();
        self.header = .{};
        self.pending_writes = 0;
        self.needs_sync = false;
    }

    /// Reads segment `seq` and appends its recovered records to `list`.
    ///
    /// The segment is read whole into memory and decoded from offset 16 (past
    /// the header) forward, one [`LogRecord`] at a time, each allocated from
    /// `allocator`. It is deliberately tolerant of a torn tail, since a crash can
    /// only damage the end of the last segment:
    ///
    /// - A `ChecksumMismatch` record is logged and skipped using its length
    ///   prefix to step over it, then decoding continues (mid-file corruption
    ///   should not lose valid records that follow).
    /// - An `InvalidRecordLength` or `RecordTooLarge` is taken as the boundary of
    ///   valid data and replay of this file stops cleanly.
    /// - A null deserialise result likewise ends the file.
    /// - Any other error is genuine and propagates.
    ///
    /// Called in order by [`WriteAheadLog.replay`]; `list` accumulates across
    /// segments.
    pub fn replayFile(self: *WriteAheadLog, seq: u64, list: *std.ArrayList(LogRecord), allocator: mem.Allocator) !void {
        const file_path = try self.getFilePath(seq);
        defer self.allocator.free(file_path);
        var file = try Dir.openFile(.cwd(), self.io, file_path, .{});
        defer file.close(self.io);

        const stat = try file.stat(self.io);
        const content = try allocator.alloc(u8, stat.size);
        defer allocator.free(content);
        self.header = try WalHeader.read(self.io, file);
        _ = try file.readPositionalAll(self.io, content, 0);

        var offset: usize = 16;
        while (offset < content.len) {
            const BufferReader = struct {
                buffer: []const u8,
                pos: usize,

                pub fn readInt(r: *@This(), comptime T: type, endian: std.builtin.Endian) !T {
                    const size = @sizeOf(T);
                    if (r.pos + size > r.buffer.len) return error.EndOfStream;
                    const value = std.mem.readInt(T, r.buffer[r.pos..][0..size], endian);
                    r.pos += size;
                    return value;
                }

                pub fn readAll(r: *@This(), buf: []u8) !void {
                    if (r.pos + buf.len > r.buffer.len) return error.EndOfStream;
                    @memcpy(buf, r.buffer[r.pos..][0..buf.len]);
                    r.pos += buf.len;
                }
            };

            var reader = BufferReader{ .buffer = content[offset..], .pos = 0 };
            const record_result = LogRecord.deserialize(allocator, &reader) catch |err| switch (err) {
                error.ChecksumMismatch => {
                    const payload_len = std.mem.readInt(u32, content[offset..][0..4], .little);
                    log.warn("WAL replay: skipping checksum-corrupt record at offset {d}", .{offset});
                    offset += @sizeOf(u32) + payload_len;
                    continue;
                },
                error.InvalidRecordLength, error.RecordTooLarge => {
                    log.warn("WAL replay stopped at torn record: {s}", .{@errorName(err)});
                    break;
                },
                else => return err,
            };
            if (record_result) |record| {
                try list.append(allocator, record);
                offset += record.size();
            } else {
                break;
            }
        }
    }

    /// Recovers the log: replays every segment from the last checkpoint forward
    /// and returns the ordered records.
    ///
    /// This is the crash-recovery entry point. It first flushes so any buffered
    /// records are on disk and replayed too. It reads the [`CheckpointRecord`] to
    /// learn the starting segment (defaulting to segment 1 when there is no
    /// checkpoint), then calls [`WriteAheadLog.replayFile`] for each segment up
    /// to `current_seq`, stopping early on a missing file (a gap means the log
    /// ends there). All recovered records are allocated into a fresh
    /// [`std.heap.ArenaAllocator`] that is returned inside the
    /// [`ReplayResult`]; the caller owns that arena and must free it once the
    /// records have been re-applied to the engine.
    pub fn replay(self: *WriteAheadLog) !ReplayResult {
        try self.flush();

        var sw: StopWatch = if (self.engine_metrics) |em| em.wal.start(self.io, .Replay) else .{};
        const gpa = std.heap.page_allocator;
        var arena = try gpa.create(std.heap.ArenaAllocator);
        arena.* = std.heap.ArenaAllocator.init(gpa);
        var records_list: std.ArrayList(LogRecord) = .empty;
        const cp = try CheckpointRecord.load(self.allocator, self.io, self.dir_path);
        var seq = cp.file_seq;
        if (seq == 0) seq = 1;
        while (seq <= self.current_seq) : (seq += 1) {
            self.replayFile(seq, &records_list, arena.allocator()) catch |err| {
                if (err == error.FileNotFound) {
                    break;
                }
                return err;
            };
        }
        if (self.engine_metrics) |em| em.wal.stop(self.io, &sw, .Replay);
        return ReplayResult{
            .arena = arena,
            .records = try records_list.toOwnedSlice(arena.allocator()),
        };
    }
};
