//! NovaDB server entry point: bootstraps the storage engine and serves it over
//! two network fronts.
//!
//! This is the top-level `main` for the standalone `btree`/`novadb` executable.
//! Its whole job is orchestration, not storage logic: it reads configuration,
//! opens (and on first run creates) the on-disk [`Database`], wires up the
//! query layer, optionally joins a replication topology, and then runs two
//! listeners concurrently until a shutdown signal arrives.
//!
//! The engine exposes two independent front ends over the same live
//! [`Database`]:
//!
//!   1. A binary/TCP protocol server ([`TcpServer`]) on `config.address:config.port`
//!     , the intended fast path that Nova's driver speaks (see `src/proto/`).
//!   2. An HTTP server ([`schnell.Server`]) on `config.http.*` that serves static
//!      content and a JSON `POST /query` convenience endpoint via [`handleHttp`].
//!
//! Both listeners run as async tasks on a single shared [`std.Io.Threaded`]
//! executor; the main thread then parks in a 100ms poll loop watching
//! [`shutdown_triggered`], which the POSIX signal handler flips. The whole
//! design is intentionally "do all fallible setup up front, then serve": every
//! resource is opened with a matching `defer` for orderly teardown, so a failure
//! during bootstrap unwinds cleanly rather than leaving half-open files or ports.
//!
//! Configuration precedence is layered: values come from the on-disk config
//! ([`Config.load`]) and are then OVERRIDDEN by environment variables
//! (`PRIMARY`, `REPLICA_*`, `HTTP_PORT`, `SYNCHRONOUS_COMMIT`). This lets an
//! orchestrator inject role and topology at launch without rewriting the config
//! file, which is how the same binary boots as either a primary or a follower.
//!
//! Replication role is decided here, not in the storage layer: if `config.primary`
//! is false the node becomes a follower ([`Database.becomeFollower`]) and starts
//! a fence-guarded replication listener; a primary with replication enabled
//! becomes a durable leader that ships WAL to its replica. A leader that cannot
//! reach its replica logs and serves anyway (availability over strict
//! durability at startup), whereas a follower that fails to start is only logged
//!, the process still comes up read-serving from its local file.

const std = @import("std");
const Io = std.Io;
const Dir = Io.Dir;
const builtin = @import("builtin");

/// The parsed server configuration type (data dir, ports, TLS, durability,
/// replication, security). Loaded from disk by [`Config.load`] and then patched
/// from the environment before any resource is opened.
const Config = @import("common/config.zig").Config;
/// The live storage engine handle: the opened B+Tree file, buffer pool, WAL,
/// MVCC state, security manager and replication role live behind this. Opened
/// once in [`main`] via [`Database.open`] and shared read/write by both listeners.
const Database = @import("schema.zig").Database;
/// Executes a decoded query against a [`Database`] and returns columns/rows. One
/// shared instance ([`global_executor`]) is fronted by both the TCP and HTTP paths.
const QueryExecutor = @import("query/query_executor.zig").QueryExecutor;
/// The JSON-decodable request shape accepted by the HTTP `POST /query` endpoint;
/// parsed from the request body in [`handleHttp`].
const QueryRequest = @import("query/query_executor.zig").QueryRequest;
/// The binary/wire-protocol listener, the primary path Nova's driver speaks. Run
/// as an async task by [`startTcpServer`].
const TcpServer = @import("query/tcp_server.zig").TcpServer;

/// The lightweight HTTP(S) server used for the static-file and JSON-query front
/// end; distinct from the binary [`TcpServer`] and driven by [`startHttpServer`].
const schnell = @import("common/schnell.zig");

/// Scoped logger for this module; all bootstrap and lifecycle messages are tagged
/// `.main` so server startup can be filtered from storage/protocol logs.
const log = std.log.scoped(.main);

/// In-memory cache of static assets served over HTTP (path → bytes + MIME + ETag).
/// Populated from `config.http.static_dir` and looked up by [`handleHttp`] on `GET`.
const StaticContentStore = @import("utils").StaticContentStore;

/// Shared, opaque context threaded through the HTTP raw handler.
///
/// [`schnell.Server`] hands the handler a `?*anyopaque`; this struct is what that
/// pointer actually is, cast back in [`handleHttp`]. It bundles the two things a
/// request needs, the query engine and the optional static store, so the
/// handler stays a free function with no captured state.
const ServerContext = struct {
    /// The shared query engine used to service `POST /query`. Borrowed, not owned
    /// (it points at [`global_executor`], whose lifetime spans the whole server).
    executor: *QueryExecutor,
    /// Optional static-asset store for `GET` requests; `null` (or an empty store)
    /// means no static content is served and every `GET` falls through to 404.
    static_store: ?*StaticContentStore,
};

/// Process-wide query executor, stored as an optional so it can be constructed
/// after the [`Database`] is open yet still referenced by the module-level
/// [`ServerContext`]. Set once in [`main`]; `null` before bootstrap completes.
var global_executor: ?QueryExecutor = null;
/// Set to `true` by [`shutdownSignalHandler`] on SIGINT/SIGTERM. The main loop
/// in [`main`] polls this to leave its serve loop; atomic + seq_cst because it is
/// written from an async signal context and read from the main thread.
var shutdown_triggered: std.atomic.Value(bool) = .init(false);
/// Weak, module-level pointer to the running HTTP server so a signal handler
/// could reach it. Published in [`main`] after the server is constructed; may be
/// `null` before then. Only pointers safe to touch from a signal handler belong here.
var shutdown_server: ?*schnell.Server = null;

/// Async-signal handler for SIGINT/SIGTERM that requests a graceful shutdown.
///
/// It does the minimum legal in signal context: a single atomic swap on
/// [`shutdown_triggered`]. The swap doubles as an idempotency guard, a second
/// signal returns immediately rather than re-running teardown. The actual
/// stopping of servers happens back on the main thread once its poll loop
/// observes the flag, because tearing sockets down from a signal handler is not
/// async-signal-safe. The `shutdown_server` deref is intentionally a no-op here
/// (the pointer is only read, not acted on) for that same reason.
fn shutdownSignalHandler(sig: std.c.SIG) callconv(.c) void {
    _ = sig;
    if (shutdown_triggered.swap(true, .seq_cst)) return;
    if (shutdown_server) |s| {
        _ = s;
    }
}

/// Installs [`shutdownSignalHandler`] for SIGINT and SIGTERM on POSIX hosts.
///
/// No-op on Windows (guarded at comptime), which has no `sigaction`; the Windows
/// server would need a console control handler instead. The mask is empty and
/// flags are 0, so the handler runs without blocking other signals, acceptable
/// because it does only an atomic swap and returns.
fn installShutdownHandlers() void {
    if (comptime builtin.os.tag != .windows) {
        const action = std.posix.Sigaction{
            .handler = .{ .handler = shutdownSignalHandler },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };
        std.posix.sigaction(std.posix.SIG.INT, &action, null);
        std.posix.sigaction(std.posix.SIG.TERM, &action, null);
    }
}

/// Resolves the directory to load configuration from.
///
/// Currently this always returns the current working directory: it probes for a
/// `db.json` there and returns `.cwd()` whether or not the probe succeeds (the
/// error branch falls through to the same result). The probe is retained as the
/// hook where an alternative search path would be added; today it has no
/// behavioural effect and the caller ([`main`]) treats a missing config as a
/// hard error via [`Config.load`].
fn configDir(io: Io) Io.Dir {
    if (Io.Dir.access(.cwd(), io, "db.json", .{})) {
        return .cwd();
    } else |_| {}
    return .cwd();
}

/// Async task body that runs the HTTP listener to completion.
///
/// Spawned into an [`Io.Group`] by [`main`]. It calls the blocking
/// [`schnell.Server.listen`] and swallows any listen error into a log line rather
/// than propagating it, so a failed HTTP front end does not abort the process
/// while the TCP front end may still be serving. Returns `Io.Cancelable!void`
/// because the group cancels it during shutdown.
fn startHttpServer(server: *schnell.Server, thr_io: Io) Io.Cancelable!void {
    server.listen(thr_io) catch |err| {
        log.err("HTTP server error: {}", .{err});
    };
}

/// Async task body that runs the binary/TCP protocol listener to completion.
///
/// The mirror of [`startHttpServer`] for [`TcpServer`]: spawned into its own
/// [`Io.Group`], it runs [`TcpServer.listen`] and logs (rather than propagates)
/// any error so one front end failing does not take down the other. Cancelled by
/// the group on shutdown.
fn startTcpServer(server: *TcpServer, thr_io: Io) Io.Cancelable!void {
    server.listen(thr_io) catch |err| {
        log.err("TCP server error: {}", .{err});
    };
}

/// Raw HTTP request handler: turns a raw request buffer into a full HTTP
/// response string.
///
/// Registered with [`schnell.Server.setRawHandler`], so it sees the unparsed
/// request bytes and must emit the entire response (status line, headers, body).
/// It handles exactly two routes and 404s everything else:
///
///   - `GET <path>`: if a [`StaticContentStore`] is present and has the file, it
///     is returned with `Content-Type`, `Content-Length`, `ETag` and a one-hour
///     `Cache-Control`. A miss falls through to 404.
///   - `POST /query`: the body is parsed as a [`QueryRequest`]; a malformed body
///     yields `400 Bad Request` with a JSON error, otherwise the query runs on
///     the shared [`QueryExecutor`] and the result is serialised to JSON with `200 OK`.
///
/// Every allocation the response is built from is freed via `defer` before
/// return, including the deep free of the executor result's columns and rows.
/// Returns `error.MissingServerContext` if the opaque context pointer is null,
/// and propagates allocation/serialisation errors as `anyerror`; the caller (the
/// server) owns the returned slice.
fn handleHttp(allocator: std.mem.Allocator, raw_request: []const u8, ctx: ?*anyopaque) anyerror![]const u8 {
    const s_ctx: *ServerContext = @ptrCast(@alignCast(ctx orelse return error.MissingServerContext));

    if (std.mem.startsWith(u8, raw_request, "GET ")) {
        const path_end = std.mem.indexOf(u8, raw_request, " HTTP/") orelse raw_request.len;
        const path = if (path_end > 4) raw_request[4..path_end] else "/";

        // Operational endpoints (liveness, readiness, Prometheus metrics). Checked
        // before the static store so they cannot be shadowed by a served file.
        if (std.mem.eql(u8, path, "/healthz")) {
            const body = "ok\n";
            return try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\n\r\n{s}", .{ body.len, body });
        }
        if (std.mem.eql(u8, path, "/readyz")) {
            const ready = !@atomicLoad(bool, &s_ctx.executor.db.is_closed, .seq_cst);
            const body: []const u8 = if (ready) "ready\n" else "not ready\n";
            const status: []const u8 = if (ready) "200 OK" else "503 Service Unavailable";
            return try std.fmt.allocPrint(allocator, "HTTP/1.1 {s}\r\nContent-Type: text/plain\r\nContent-Length: {d}\r\n\r\n{s}", .{ status, body.len, body });
        }
        if (std.mem.eql(u8, path, "/metrics")) {
            const pool = s_ctx.executor.db.pool;
            const body = try std.fmt.allocPrint(allocator,
                \\# HELP kaidb_up 1 when the server is serving.
                \\# TYPE kaidb_up gauge
                \\kaidb_up 1
                \\# HELP kaidb_buffer_pool_size_pages Configured buffer-pool size in pages.
                \\# TYPE kaidb_buffer_pool_size_pages gauge
                \\kaidb_buffer_pool_size_pages {d}
                \\# HELP kaidb_buffer_pool_resident_pages Pages currently backed by the file.
                \\# TYPE kaidb_buffer_pool_resident_pages gauge
                \\kaidb_buffer_pool_resident_pages {d}
                \\# HELP kaidb_buffer_pool_fetches_total Total fetchPage calls (hits + misses).
                \\# TYPE kaidb_buffer_pool_fetches_total counter
                \\kaidb_buffer_pool_fetches_total {d}
                \\# HELP kaidb_buffer_pool_hits_total fetchPage calls served from a resident frame (cache hits). Miss ratio = 1 - hits/fetches.
                \\# TYPE kaidb_buffer_pool_hits_total counter
                \\kaidb_buffer_pool_hits_total {d}
                \\# HELP kaidb_buffer_pool_evictions_total Total pages evicted.
                \\# TYPE kaidb_buffer_pool_evictions_total counter
                \\kaidb_buffer_pool_evictions_total {d}
                \\# HELP kaidb_mmap_borrow_serves_total Read misses served from the mmap borrow.
                \\# TYPE kaidb_mmap_borrow_serves_total counter
                \\kaidb_mmap_borrow_serves_total {d}
                \\# HELP kaidb_pread_serves_total Read misses served by a pread copy.
                \\# TYPE kaidb_pread_serves_total counter
                \\kaidb_pread_serves_total {d}
                \\# HELP kaidb_checksum_failures_total Page checksum validation failures.
                \\# TYPE kaidb_checksum_failures_total counter
                \\kaidb_checksum_failures_total {d}
                \\# HELP kaidb_next_tx_id Next transaction id (monotonic; proxy for txns started).
                \\# TYPE kaidb_next_tx_id counter
                \\kaidb_next_tx_id {d}
                \\# HELP kaidb_current_lsn Current log sequence number / durability watermark.
                \\# TYPE kaidb_current_lsn counter
                \\kaidb_current_lsn {d}
                \\
            , .{
                pool.pool_size,
                pool.pager.num_pages,
                pool.fetch_count.load(.monotonic),
                pool.hit_count.load(.monotonic),
                pool.evict_count.load(.monotonic),
                pool.borrow_serves.load(.monotonic),
                pool.pread_serves.load(.monotonic),
                pool.checksum_failed,
                s_ctx.executor.db.txn_manager.next_tx_id,
                pool.current_lsn.load(.monotonic),
            });
            defer allocator.free(body);

            // Latency histogram (query duration). Rendered into a bounded stack
            // buffer; ~18 short lines fit comfortably.
            var hbuf: [4096]u8 = undefined;
            var hw = std.Io.Writer.fixed(&hbuf);
            s_ctx.executor.db.query_latency.writeProm(&hw, "kaidb_query_duration_seconds", "End-to-end query execution latency in seconds.") catch {};
            const hist = hw.buffered();

            // Replication lag, only when this node is a shipping primary. Emitting
            // 0 when no replica is configured would falsely read as "fully caught
            // up", so these series are absent unless replication is active.
            var rbuf: [768]u8 = undefined;
            var rw = std.Io.Writer.fixed(&rbuf);
            if (s_ctx.executor.db.durable_repl) |dr| {
                rw.print(
                    \\# HELP kaidb_replication_produced_seq Highest replication frame seq produced by the primary.
                    \\# TYPE kaidb_replication_produced_seq gauge
                    \\kaidb_replication_produced_seq {d}
                    \\# HELP kaidb_replication_confirmed_seq Highest replication frame seq confirmed durable by followers.
                    \\# TYPE kaidb_replication_confirmed_seq gauge
                    \\kaidb_replication_confirmed_seq {d}
                    \\# HELP kaidb_replication_lag_frames Frames produced but not yet confirmed by followers.
                    \\# TYPE kaidb_replication_lag_frames gauge
                    \\kaidb_replication_lag_frames {d}
                    \\
                , .{ dr.producedSeq(), dr.confirmedSeq(), dr.lagFrames() }) catch {};
            }
            const repl = rw.buffered();

            // WAL size + checkpoint lag, only when durability is on (WAL present).
            var wbuf: [768]u8 = undefined;
            var ww = std.Io.Writer.fixed(&wbuf);
            if (s_ctx.executor.db.wal) |wal| {
                ww.print(
                    \\# HELP kaidb_wal_bytes Total size of the on-disk WAL segments.
                    \\# TYPE kaidb_wal_bytes gauge
                    \\kaidb_wal_bytes {d}
                    \\# HELP kaidb_wal_checkpoint_lag_lsn LSNs written since the last checkpoint (0 = caught up).
                    \\# TYPE kaidb_wal_checkpoint_lag_lsn gauge
                    \\kaidb_wal_checkpoint_lag_lsn {d}
                    \\
                , .{ wal.walBytesOnDisk(), wal.checkpointLagLsn() }) catch {};
            }
            const walm = ww.buffered();

            return try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: text/plain; version=0.0.4\r\nContent-Length: {d}\r\n\r\n{s}{s}{s}{s}", .{ body.len + hist.len + repl.len + walm.len, body, hist, repl, walm });
        }

        if (s_ctx.static_store) |store| {
            if (path_end > 4) {
                if (store.lookup(path)) |file| {
                    return try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\n" ++
                        "Content-Type: {s}\r\n" ++
                        "Content-Length: {d}\r\n" ++
                        "ETag: {s}\r\n" ++
                        "Cache-Control: public, max-age=3600\r\n\r\n{s}", .{ file.mime_type, file.data.len, file.etag, file.data });
                }
            }
        }
    }

    if (std.mem.startsWith(u8, raw_request, "POST /query") or std.mem.indexOf(u8, raw_request, "POST /query ") != null) {
        // The JSON /query endpoint is the SQL surface; reject it when the instance
        // runs in document-only mode (matches the wire-protocol gate).
        if (s_ctx.executor.db.mode == .document) {
            const msg = "{\"error_message\":\"SQL is disabled: this server runs in document mode\"}";
            return try std.fmt.allocPrint(allocator, "HTTP/1.1 403 Forbidden\r\nContent-Type: application/json\r\nContent-Length: {d}\r\n\r\n{s}", .{ msg.len, msg });
        }
        const body = getHttpBody(raw_request);
        const req = std.json.parseFromSlice(QueryRequest, allocator, body, .{}) catch |err| {
            const err_resp = try std.fmt.allocPrint(allocator, "{{\"error_message\":\"Invalid JSON payload: {any}\"}}", .{err});
            defer allocator.free(err_resp);
            return try std.fmt.allocPrint(allocator, "HTTP/1.1 400 Bad Request\r\nContent-Type: application/json\r\nContent-Length: {d}\r\n\r\n{s}", .{ err_resp.len, err_resp });
        };
        defer req.deinit();

        const executor = s_ctx.executor;
        const res = try executor.execute(req.value);
        defer {
            if (res.error_message) |m| allocator.free(m);
            for (res.columns) |c| allocator.free(c);
            allocator.free(res.columns);
            for (res.rows) |r| {
                for (r) |c| allocator.free(c);
                allocator.free(r);
            }
            allocator.free(res.rows);
        }

        var allocating = std.Io.Writer.Allocating.init(allocator);
        defer allocating.deinit();
        try allocating.writer.print("{f}", .{std.json.fmt(res, .{})});

        return try std.fmt.allocPrint(allocator, "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {d}\r\n\r\n{s}", .{ allocating.written().len, allocating.written() });
    }

    return try allocator.dupe(u8, "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n");
}

/// Extracts the body of an HTTP request by finding the header/body separator.
///
/// Splits on the first `\r\n\r\n` (proper HTTP) and, failing that, a bare `\n\n`
/// (tolerating lax clients / test traffic). If no blank-line separator is found
/// at all it returns the whole buffer unchanged, on the assumption the caller
/// passed a body-only payload. The result borrows into `raw`; it does not copy.
fn getHttpBody(raw: []const u8) []const u8 {
    if (std.mem.indexOf(u8, raw, "\r\n\r\n")) |idx| {
        return raw[idx + 4 ..];
    }
    if (std.mem.indexOf(u8, raw, "\n\n")) |idx| {
        return raw[idx + 2 ..];
    }
    return raw;
}

/// Reads the optional `providers.yaml` from a directory into a freshly allocated
/// buffer.
///
/// A missing file is not an error: `error.FileNotFound` is mapped to an empty
/// slice so an absent providers config is treated as "no providers" rather than
/// a failure. Any other I/O error propagates. On success the caller owns the
/// returned bytes and must free them (the `errdefer` only covers the failure
/// path within this function).
fn loadProviders(allocator: std.mem.Allocator, io: std.Io, dir: Io.Dir) ![]u8 {
    const content = Io.Dir.readFileAlloc(dir, io, "providers.yaml", allocator, .unlimited) catch |err| switch (err) {
        error.FileNotFound => return &.{},
        else => return err,
    };
    errdefer allocator.free(content);
    return content;
}

/// Process entry point: bootstrap the engine, start both listeners, serve until
/// shutdown.
///
/// The body is a strict "acquire, then serve" sequence, each resource paired with
/// a `defer` for teardown in reverse order:
///
///   1. Choose an allocator, a leak-detecting [`DebugAllocator`] in Debug builds
///      (the deferred `detectLeaks` exits non-zero on any leak, making tests fail
///      loudly), the libc allocator otherwise.
///   2. Build the [`std.Io.Threaded`] executor both listeners share, load and
///      validate [`Config`], and create the data directory.
///   3. Open the [`Database`] (WAL dir only when durability is enabled) and apply
///      environment overrides for role/ports/durability over the file config.
///   4. Establish replication role: follower vs durable leader (see the module
///      header for the availability trade-offs on failure).
///   5. Construct the shared [`QueryExecutor`], the [`StaticContentStore`], and
///      the [`ServerContext`], then start the HTTP and TCP servers as async tasks
///      in separate [`Io.Group`]s.
///   6. Install signal handlers and park in a 100ms poll loop on
///      [`shutdown_triggered`]; on shutdown, stop and cancel both servers.
///
/// Returns any bootstrap error (config, directory creation, DB open, server init)
/// to the runtime, which aborts startup. Runtime listen errors, by contrast, are
/// logged inside the task bodies and do not propagate here.
pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const allocator = if (builtin.mode == .Debug) gpa.allocator() else std.heap.c_allocator;

    defer if (builtin.mode == .Debug) {
        if (gpa.detectLeaks() > 0) {
            std.process.exit(1);
        }
    };

    var threaded: std.Io.Threaded = .init(allocator, .{
        .environ = init.minimal.environ,
        .async_limit = .unlimited,
        .concurrent_limit = .unlimited,
    });

    defer threaded.deinit();
    const io = threaded.io();

    // Offline maintenance: `novadb compact <src_dir> <dst_dir>` rebuilds the
    // database into a fresh, fully packed file and exits (see compact.zig). Runs
    // before any server/config setup so it is a pure one-shot tool.
    {
        const args = try init.minimal.args.toSlice(allocator);
        defer allocator.free(args);
        if (args.len >= 4 and std.mem.eql(u8, args[1], "compact")) {
            log.info("compacting {s} -> {s} ...", .{ args[2], args[3] });
            try @import("compact.zig").compact(allocator, io, args[2], args[3]);
            return;
        }
        // Backup: `novadb backup <base_dir> <dest_dir>` writes a consistent snapshot
        // (checkpoint + fsync + full page copy + WAL) to <dest_dir>/snapshot.db(+wal).
        // Run against a STOPPED server (this opens its own handle); the underlying
        // exportSnapshot primitive is the same one the replication path uses live.
        if (args.len >= 4 and std.mem.eql(u8, args[1], "backup")) {
            const base = args[2];
            const dest = args[3];
            const dbf = try std.fmt.allocPrint(allocator, "{s}/nova.db", .{base});
            defer allocator.free(dbf);
            const wdir = try std.fmt.allocPrint(allocator, "{s}/wal", .{base});
            defer allocator.free(wdir);
            log.info("backing up {s} -> {s} ...", .{ base, dest });
            var db = try Database.open(allocator, io, dbf, 8192, wdir);
            defer db.close();
            try db.exportSnapshot(dest);
            log.info("backup complete: {s}/snapshot.db (+ wal/)", .{dest});
            return;
        }
        // Restore: `novadb restore <snapshot_dir> <dest_base_dir>` reconstructs a data
        // directory from a snapshot (copies snapshot.db -> nova.db and the WAL back);
        // the next server start on <dest_base_dir> runs recovery over it.
        //
        // Point-in-time recovery (PITR): add
        //   --archive=<dir>       merge archived WAL segments into the restored log
        //   --target-lsn=<N>      replay only up to LSN N, then materialise that state
        // Together these reconstruct the database as it was at LSN N: the base
        // snapshot supplies pages up to its checkpoint, the archived segments
        // supply the history past it, and the replay stops at the target.
        if (args.len >= 4 and std.mem.eql(u8, args[1], "restore")) {
            const snap = args[2];
            const dest_base = args[3];
            var target_lsn: u64 = 0;
            var archive_dir: ?[]const u8 = null;
            for (args[4..]) |a| {
                if (std.mem.startsWith(u8, a, "--target-lsn=")) {
                    target_lsn = std.fmt.parseUnsigned(u64, a["--target-lsn=".len..], 10) catch {
                        log.err("invalid --target-lsn value: {s}", .{a});
                        return error.InvalidArgument;
                    };
                } else if (std.mem.startsWith(u8, a, "--archive=")) {
                    archive_dir = a["--archive=".len..];
                }
            }
            try Io.Dir.createDirPath(.cwd(), io, dest_base);
            const dbf = try std.fmt.allocPrint(allocator, "{s}/nova.db", .{dest_base});
            defer allocator.free(dbf);
            const wdir = try std.fmt.allocPrint(allocator, "{s}/wal", .{dest_base});
            defer allocator.free(wdir);
            log.info("restoring {s} -> {s} ...", .{ snap, dest_base });
            try Database.restoreSnapshot(allocator, io, snap, dbf, wdir);

            // Merge archived segments into the restored WAL, filling the history
            // between the snapshot's checkpoint and the target. The archive copy
            // wins on overlap: an archived segment was sealed at truncation, so it
            // is the final, complete version, whereas the snapshot may have caught
            // that same segment mid-write. Segments only the snapshot carries (the
            // tail segment that was never retired) are left in place.
            if (archive_dir) |adir| {
                var copied: usize = 0;
                var asrc = Io.Dir.openDir(.cwd(), io, adir, .{ .iterate = true }) catch {
                    log.err("could not open archive dir {s}", .{adir});
                    return error.InvalidArgument;
                };
                defer asrc.close(io);
                var ait = asrc.iterate();
                while (ait.next(io) catch null) |entry| {
                    if (entry.kind != .file or !std.mem.endsWith(u8, entry.name, ".wal")) continue;
                    const sp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ adir, entry.name });
                    defer allocator.free(sp);
                    const b = Io.Dir.readFileAlloc(.cwd(), io, sp, allocator, .unlimited) catch continue;
                    defer allocator.free(b);
                    const dp = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ wdir, entry.name });
                    defer allocator.free(dp);
                    const of = try Io.Dir.createFile(.cwd(), io, dp, .{ .truncate = true });
                    defer of.close(io);
                    try of.writeStreamingAll(io, b);
                    try of.sync(io);
                    copied += 1;
                }
                log.info("merged {d} archived WAL segment(s) from {s}", .{ copied, adir });
            }

            if (target_lsn != 0) {
                // Open at the target LSN so recovery replays only up to N, then
                // close so the recovered-to-N state is checkpointed into nova.db.
                // A later plain open sees a clean database as of the target.
                var rdb = try Database.openAt(allocator, io, dbf, 8192, wdir, target_lsn);
                rdb.close();
                log.info("restore complete: {s} (point-in-time: LSN <= {d})", .{ dest_base, target_lsn });
            } else {
                log.info("restore complete: {s}", .{dest_base});
            }
            return;
        }
        // Offline password rotation: `novadb passwd <base_dir> <user> <newpassword>`
        // opens its own handle (server may be stopped), so it can rotate the seeded
        // admin/admin before the server is ever exposed. This is the escape hatch
        // for `security.require_admin_password_change`.
        if (args.len >= 5 and std.mem.eql(u8, args[1], "passwd")) {
            const base = args[2];
            const user = args[3];
            const newpw = args[4];
            const dbf = try std.fmt.allocPrint(allocator, "{s}/nova.db", .{base});
            defer allocator.free(dbf);
            const wdir = try std.fmt.allocPrint(allocator, "{s}/wal", .{base});
            defer allocator.free(wdir);
            var db = try Database.open(allocator, io, dbf, 8192, wdir);
            defer db.close();

            var salt: [32]u8 = undefined;
            std.Io.random(io, &salt);
            const hash = try db.security_manager.hashKey(newpw, salt);
            const hex_hash = std.fmt.bytesToHex(hash, .lower);
            const hex_salt = std.fmt.bytesToHex(salt, .lower);
            const password_hash = try std.fmt.allocPrint(allocator, "{s}:{s}", .{ hex_hash, hex_salt });
            defer allocator.free(password_hash);

            db.updateUserPassword(user, password_hash, 1) catch |err| {
                if (err == error.UserNotFound) {
                    log.err("passwd: user '{s}' does not exist", .{user});
                    return error.UserNotFound;
                }
                return err;
            };
            log.info("passwd: password for '{s}' updated", .{user});
            return;
        }
    }

    log.info("Bootstrapping B+Tree database engine...", .{});

    const config_dir = configDir(io);
    var parsed_config = Config.load(allocator, io, config_dir) catch |err| {
        log.err("configuration error: {any}", .{err});
        return err;
    };
    defer parsed_config.deinit();
    const config = &parsed_config.value;

    Io.Dir.createDirPath(.cwd(), io, config.base_dir) catch |err| {
        log.err("could not create data directory {s}: {any}", .{ config.base_dir, err });
        return err;
    };

    const db_file_path = try std.fmt.allocPrint(allocator, "{s}/nova.db", .{config.base_dir});
    defer allocator.free(db_file_path);

    const wal_dir = if (config.durability.enabled)
        try std.fmt.allocPrint(allocator, "{s}/wal", .{config.base_dir})
    else
        null;
    defer if (wal_dir) |d| allocator.free(d);

    // Page-mgmt (mmap reads): opt-in, enabled INSIDE open (before recovery and
    // tree-cache population read any page) so startup reads borrow the read-only
    // map instead of materialising into slab windows; see `Database.openWithMmap`
    // and `match-page-mgmt-with-sqlite.md`.
    const pool_pages = config.effectivePoolPages();
    log.info("buffer pool: {d} pages ({d} MiB){s}", .{
        pool_pages,
        (@as(u64, pool_pages) * 16384) / (1024 * 1024),
        if (config.pool_size == 0) " [auto]" else "",
    });
    var db = if (config.mmap_reads)
        try Database.openWithMmap(allocator, io, db_file_path, pool_pages, wal_dir)
    else
        try Database.open(allocator, io, db_file_path, pool_pages, wal_dir);
    defer db.close();
    db.security_manager.enabled = config.security.enabled;

    // Point-in-time-recovery retention: when an archive directory is configured,
    // enable WAL archiving so checkpoints copy each retired segment aside instead
    // of deleting it. A later `restore --target-lsn` replays the base backup
    // forward over these segments to any point in the history.
    if (config.durability.wal_archive_dir.len > 0) {
        if (db.wal) |w| {
            w.setArchive(config.durability.wal_archive_dir) catch |err| {
                log.err("could not enable WAL archiving at {s}: {any}", .{ config.durability.wal_archive_dir, err });
                return err;
            };
            log.info("WAL archiving enabled -> {s}", .{config.durability.wal_archive_dir});
        }
    }

    if (init.environ_map.get("PRIMARY")) |v| config.primary = std.mem.eql(u8, v, "true");
    if (init.environ_map.get("REPLICA_ENABLED")) |v| config.replica.enabled = std.mem.eql(u8, v, "true");
    if (init.environ_map.get("REPLICA_PORT")) |v| {
        if (std.fmt.parseInt(u16, v, 10)) |p| config.replica.port = p else |_| {}
    }
    if (init.environ_map.get("HTTP_PORT")) |v| {
        if (std.fmt.parseInt(u16, v, 10)) |p| config.http.port = p else |_| {}
    }

    const repl_addr = blk: {
        if (init.environ_map.get("REPLICA_ADDRESS")) |v| {
            if (v.len > 0) break :blk v;
        }
        break :blk if (config.replica.address.len > 0) config.replica.address else "127.0.0.1";
    };
    if (!config.primary) {
        const fence_dir = wal_dir orelse config.base_dir;
        db.becomeFollower(repl_addr, config.replica.port, fence_dir, config.replica.key, .{}) catch |err| {
            log.err("failed to start follower replication listener: {any}", .{err});
        };
    } else if (config.replica.enabled) {
        db.becomeDurableLeader(repl_addr, config.replica.port, 2, 1, 5000, config.replica.key, .{}) catch |err| {
            log.err("leader could not connect to replica {s}:{d} for shipping: {any} (serving without replication)", .{ repl_addr, config.replica.port, err });
        };
    }

    // Single data-model surface: SQL-only or document-only (see ServerMode). The
    // wire session and HTTP query handler reject the other surface's requests.
    db.mode = config.mode;
    log.info("data model: {s}", .{@tagName(config.mode)});

    db.synchronous_commit = config.durability.synchronous_commit;
    if (init.environ_map.get("SYNCHRONOUS_COMMIT")) |val| {
        db.synchronous_commit = std.mem.eql(u8, val, "true");
    }

    // Authentication enforcement. `openInner` constructs the security manager
    // disabled (embedded-friendly default); apply the configured policy here now
    // that config is known. `require_auth` implies `enabled`. When auth is active
    // the startup path challenges every connection and rejects bad credentials;
    // with `require_auth` the wire session additionally fail-closes any data-plane
    // frame on an unauthenticated connection.
    // `require_tls_for_auth` implies `require_auth`, which implies `enabled`.
    const require_tls_for_auth = config.security.require_tls_for_auth;
    const tls_configured = config.tls.enabled and config.tls.cert_file.len > 0 and config.tls.key_file.len > 0;
    if (require_tls_for_auth and !tls_configured) {
        // Fail closed at startup rather than silently rejecting every login: with
        // this flag on, password auth is only offered over TLS, so a server with
        // no TLS configured would accept no client at all.
        log.err("security.require_tls_for_auth is on but TLS is not configured (need tls.enabled + tls.cert_file + tls.key_file); refusing to start", .{});
        return error.TlsRequiredButNotConfigured;
    }
    // Force rotation of the seeded admin credential when asked. Checked with a
    // lockout-free comparison (never `authenticate`, which would feed the
    // brute-force counter). Fail closed BEFORE opening the listeners. The
    // bootstrap paradox (a fresh DB seeds admin/admin, and rotating needs an admin
    // login) is resolved by the offline `novadb passwd admin '<newpw>'` command,
    // which rotates without the server running; the error points operators to it.
    if (config.security.require_admin_password_change and
        db.security_manager.passwordMatches("admin", "admin"))
    {
        log.err("security.require_admin_password_change is on but 'admin' still has the default password; rotate it first with `novadb passwd admin '<newpassword>'` (server may be stopped), then start. Refusing to start.", .{});
        return error.DefaultAdminPasswordNotRotated;
    }

    db.security_manager.enabled = config.security.enabled or config.security.require_auth or require_tls_for_auth;
    db.security_manager.require_auth = config.security.require_auth or require_tls_for_auth;
    db.security_manager.require_tls_for_auth = require_tls_for_auth;
    if (db.security_manager.enabled) {
        log.info("authentication ENABLED (require_auth={}, require_tls_for_auth={})", .{ db.security_manager.require_auth, require_tls_for_auth });
        // A fresh database seeds `admin`/`admin`. We deliberately do NOT probe the
        // credential here (a failed probe would feed the brute-force lockout and
        // could lock the real admin out across restarts); instead advise operators
        // unconditionally to rotate it before exposing the server.
        log.warn("SECURITY: ensure the bootstrap 'admin' account's default password ('admin') has been changed (ALTER USER admin IDENTIFIED BY '...') before exposing this server", .{});
    }

    // Apply the configured result-materialisation cap before any connection is
    // accepted. It bounds how many bytes one query may buffer while building its
    // result set, so a runaway unindexed sort/scan fails cleanly with a "Query
    // Memory Limit Exceeded" response instead of OOM-killing the whole server.
    // Read by every executed query on both the wire and HTTP paths. 0 in config
    // means unlimited.
    QueryExecutor.result_bytes_limit_default =
        if (config.query_memory_limit_bytes == 0) null else config.query_memory_limit_bytes;

    global_executor = QueryExecutor.init(allocator, db);
    defer global_executor.?.deinit();

    var static_store = StaticContentStore.init(allocator);
    defer static_store.deinit();

    const static_dir = config.http.static_dir orelse "public";
    static_store.loadDir(io, static_dir) catch |err| {
        log.warn("Failed to load static content from '{s}': {any}", .{ static_dir, err });
    };

    var server_context: ServerContext = .{
        .executor = &global_executor.?,
        .static_store = &static_store,
    };

    var http_cfg = config.http;
    if (config.tls.enabled) {
        http_cfg.tls_cert_file = config.tls.cert_file;
        http_cfg.tls_key_file = config.tls.key_file;
    }

    var http_server = try schnell.Server.init(allocator, http_cfg);
    defer http_server.deinit();

    http_server.setRawHandler(handleHttp, &server_context);
    shutdown_server = &http_server;

    var http_group: Io.Group = .init;
    http_group.async(io, startHttpServer, .{ &http_server, io });

    log.info("Database HTTPS Server listening on {s}:{d}...", .{ http_cfg.host, http_cfg.port });

    var tcp_server = TcpServer.init(allocator, config.address, config.port, db, config.max_sessions, config);
    defer tcp_server.deinit();
    if (tls_configured) {
        tcp_server.enableTlsFiles(config.tls.cert_file, config.tls.key_file);
        log.info("data-plane TLS enabled (cert {s})", .{config.tls.cert_file});
    }

    var tcp_group: Io.Group = .init;
    tcp_group.async(io, startTcpServer, .{ &tcp_server, io });

    log.info("Database TCP Server listening on {s}:{d}...", .{ config.address, config.port });

    installShutdownHandlers();

    while (!shutdown_triggered.load(.seq_cst)) {
        io.sleep(Io.Duration.fromMilliseconds(100), .awake) catch {};
    }

    log.info("Shutdown signal received. Shutting down...", .{});
    http_server.stop(io);
    http_group.cancel(io);
    tcp_server.stop(io);
    tcp_group.cancel(io);
}
