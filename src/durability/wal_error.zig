//! The single error set for the write-ahead log subsystem.
//!
//! Every fallible entry point of the WAL ([`WriteAheadLog.flush`],
//! [`WriteAheadLog.sync`], [`WriteAheadLog.rotate`], [`WriteAheadLog.truncate`],
//! and so on) declares its return type as `WalError!void`. Collapsing all of the
//! WAL's failure modes into one explicit, named error set is a deliberate design
//! choice rather than letting Zig infer per-function error unions:
//!
//!   * It lets callers `catch` the WAL as a whole and map any failure to a single
//!     durability policy (typically: refuse the commit, mark the log poisoned, and
//!     surface the error up the transaction stack) without having to enumerate a
//!     different inferred set at every call site.
//!   * It fixes a stable ABI-like contract for the fault-injection hook
//!     `test_inject_write_error: ?WalError` on [`WriteAheadLog`]: a test can force
//!     any one of these errors (for example `WalError.NoSpaceLeft`, as
//!     `root.zig` does) at the exact point the real I/O would have failed, which
//!     is how the disk-full and truncated-write recovery paths are exercised
//!     without an actual full disk.
//!
//! The set is intentionally a superset of the errors that can genuinely arise
//! here. It contains two layers:
//!
//!   1. **WAL-semantic errors**, conditions the WAL itself detects and names,
//!      such as [`WalError.ChecksumMismatch`] and [`WalError.RecordTooLarge`]
//!      during replay, or [`WalError.FailedToWriteHeader`] when rotating a segment.
//!      These carry meaning specific to the log format.
//!
//!   2. **Flattened OS / filesystem errors**, the raw errors that Zig's
//!      `std.fs`/`std.posix` file operations can return (`NoSpaceLeft`,
//!      `WouldBlock`, `InputOutput`, `AccessDenied`, the Windows sharing/lock
//!      variants, and so on). The WAL's own helpers translate the common cases
//!      into the semantic errors above (a failed positional write becomes
//!      [`WalError.WriteFailed`]), but the OS names are kept in the set so that a
//!      lower-level error which is passed through, or injected verbatim, still
//!      unifies into `WalError` instead of forcing a wider inferred type.
//!
//! This file is pure declaration: it holds no state and no logic, so it can be
//! imported by both the WAL implementation (`write_ahead_log.zig`) and the
//! recovery/test code without creating a dependency cycle.

const std = @import("std");

/// The unified error set returned by every fallible write-ahead-log operation.
///
/// Members fall into two groups: WAL-semantic errors named by the log itself,
/// and the flattened set of OS/filesystem errors the underlying file I/O can
/// raise (see the file-level docs above for why both are kept). Callers should
/// treat the whole set as "the WAL could not guarantee durability" and fail the
/// enclosing commit rather than branching on most individual members.
pub const WalError = error{
    /// Opening (or creating) a WAL segment file failed. Raised when the
    /// underlying `open`/`openat` on a log segment does not succeed.
    OpenFileFailed,
    /// A positional write of buffered log bytes to a segment failed. The WAL
    /// maps a failed `writePositionalAll` to this so callers see one WAL-level
    /// error instead of the raw OS write error.
    WriteFailed,
    /// `fsync`/`fdatasync` of a segment failed, so the previously written bytes
    /// are not known to be durable. A commit that depended on this sync must be
    /// treated as not durable and aborted.
    SyncFailed,
    /// Creating the directory that holds WAL segments failed.
    CreateDirFailed,
    /// A WAL segment file name did not match the expected naming scheme, so its
    /// sequence number could not be derived during discovery/recovery.
    InvalidFileName,
    /// Truncating a segment (for example when resetting or discarding the tail
    /// after a checkpoint) failed.
    TruncateFailed,
    /// Repositioning the file offset within a segment failed.
    SeekFailed,
    /// The absolute path of a segment file could not be resolved.
    GetPathFailed,
    /// A segment, or a length field read from it, exceeds the size the WAL is
    /// willing to handle. Distinct from [`WalError.RecordTooLarge`], which is
    /// about a single record rather than the whole file.
    FileTooLarge,
    /// A record's stored checksum did not match the checksum recomputed over its
    /// bytes during replay, indicating a torn or corrupt write. Recovery stops at
    /// this record: everything before it is durable, this record and anything
    /// after it is discarded.
    ChecksumMismatch,
    /// Replaying the log against the pager/B+Tree failed while re-applying a
    /// record, so the database could not be brought to a consistent state.
    ReplayFailed,
    /// A single log record's length exceeds the maximum record size, so it cannot
    /// be a valid record and is rejected during replay.
    RecordTooLarge,
    /// A record's length field is structurally invalid (for example zero, or
    /// larger than the bytes remaining in the segment), so the record framing is
    /// broken and replay cannot continue past it.
    InvalidRecordLength,
    /// A referenced WAL file does not exist. Passed through from the filesystem.
    FileNotFound,
    /// A file/directory that the WAL tried to create already exists.
    PathAlreadyExists,
    /// The process lacks permission for the requested file operation.
    AccessDenied,
    /// Memory allocation failed while buffering or replaying log data.
    OutOfMemory,
    /// A read hit end of file before the expected number of bytes were available,
    /// which during replay usually means a partially written trailing record.
    EndOfStream,
    /// An OS error with no more specific mapping. Passed through from the standard
    /// library as a catch-all.
    Unexpected,
    /// The file is larger than the host can address (the `std` counterpart to the
    /// WAL's own [`WalError.FileTooLarge`]). Kept so a passed-through OS error
    /// still unifies into this set.
    FileTooBig,
    /// The target device is missing/absent.
    NoDevice,
    /// The kernel is out of the resources (file handles, memory) needed to
    /// complete the operation.
    SystemResources,
    /// The device is busy and could not service the operation.
    DeviceBusy,
    /// The write would exceed the user's disk quota.
    DiskQuota,
    /// The filesystem has no free space for the write. Commonly the injected
    /// error used to exercise the WAL's disk-full handling.
    NoSpaceLeft,
    /// A non-blocking operation would have blocked. Passed through for WAL files
    /// opened in non-blocking mode.
    WouldBlock,
    /// A low-level I/O error from the storage device.
    InputOutput,
    /// An argument to a file syscall was invalid.
    InvalidArgument,
    /// The write end of a pipe/socket has no reader.
    BrokenPipe,
    /// The operation was aborted (typically a Windows overlapped-I/O cancellation).
    OperationAborted,
    /// A write was attempted on a handle that was not opened for writing.
    NotOpenForWriting,
    /// A byte-range or file lock could not be acquired because it conflicts with
    /// another lock (Windows).
    LockViolation,
    /// The peer reset the connection (surfaces when WAL I/O runs over a
    /// network/pipe file object).
    ConnectionResetByPeer,
    /// The referenced process no longer exists.
    ProcessNotFound,
    /// Too many symbolic links were encountered resolving the WAL path.
    SymLinkLoop,
    /// The process hit its per-process open-file-descriptor limit.
    ProcessFdQuotaExceeded,
    /// A path or file-name component is longer than the OS allows.
    NameTooLong,
    /// The system-wide open-file limit was reached.
    SystemFdQuotaExceeded,
    /// The file is in use in a way that blocks the operation.
    FileBusy,
    /// A path component that was expected to be a directory is not one.
    NotDir,
    /// A path or name contained invalid UTF-8.
    InvalidUtf8,
    /// A path or name contained invalid WTF-8 (the Windows path encoding).
    InvalidWtf8,
    /// A path name was rejected by the OS as malformed.
    BadPathName,
    /// The network path/host for the file could not be found (Windows/UNC paths).
    NetworkNotFound,
    /// The file could not be opened because another handle holds an incompatible
    /// sharing mode (Windows).
    SharingViolation,
    /// The operation was cancelled.
    Canceled,
    /// The operation was denied by permissions.
    PermissionDenied,
    /// The filesystem does not support the requested file locking.
    FileLocksUnsupported,
    /// A named-pipe endpoint is busy (Windows).
    PipeBusy,
    /// Antivirus/endpoint software interfered with the file operation (Windows).
    AntivirusInterference,
    /// The path names a directory where a file was required.
    IsDir,
    /// The platform does not support file locks (alternate spelling of the
    /// unsupported-locks condition surfaced by some `std` paths).
    FileLocksNotSupported,
    /// The filesystem is mounted read-only, so writes/creates cannot proceed.
    ReadOnlyFileSystem,
    /// Creating a hard link would exceed the link quota.
    LinkQuotaExceeded,
    /// A rename would cross a mount point, which the OS cannot do atomically.
    RenameAcrossMountPoints,
    /// A generic filesystem error with no finer classification.
    FileSystem,
    /// A directory expected to be empty (for removal) is not.
    DirNotEmpty,
    /// An operation (typically rename/link) would cross device boundaries.
    CrossDevice,
    /// The storage hardware reported a failure.
    HardwareFailure,
    /// Writing a fresh segment's header failed during rotation, so the new
    /// segment is unusable. Named separately from [`WalError.WriteFailed`]
    /// because it identifies the header write specifically (see
    /// [`WriteAheadLog.rotate`]).
    FailedToWriteHeader,
};
