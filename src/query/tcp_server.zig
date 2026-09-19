//! Front-door TCP listener for the kaidb server: it accepts client sockets,
//! optionally wraps them in TLS, and demultiplexes each connection onto one of
//! three wire dialects that this one port speaks.
//!
//! ## Why one listener, three protocols
//!
//! A kaidb deployment has to serve heterogeneous clients over a single port,
//! so the very first byte (or first 4 bytes) of a connection decides which
//! dialect the peer is speaking. [`runLoop`] is the demux point and never
//! guesses twice: once a connection commits to a dialect it stays there for its
//! lifetime.
//!
//!   1. **PostgreSQL-style startup protocol**, if the first byte equals
//!      [`wire.Frontend.startup`], the whole connection is handed to
//!      [`session.run`] with its own [`Session`]. This is the psql/`libpq`
//!      compatible path.
//!
//!   2. **Length-prefixed JSON packet protocol**, the legacy/simple path. Each
//!      message is a little-endian `u32` payload length followed by a serialised
//!      [`proto.Packet`]. Query results are marshalled to JSON by
//!      [`writeQueryResponse`]. This path also carries the replication verbs
//!      ([`proto.Operation`] `Authenticate` and `ShipWal`).
//!
//!   3. **Binary wire protocol**, if the first 4 bytes equal
//!      [`binary_proto.MessageHeader.MAGIC_VALUE`], the connection upgrades to
//!      the compact framed protocol handled by [`runBinaryProtocolStream`]. This
//!      is the intended fast path for Kyte's own driver.
//!
//! The trick that makes the demux free is that the JSON path's `u32` length and
//! the binary path's magic occupy the same first 4 bytes: [`runLoop`] reads
//! those 4 bytes once and compares against the magic before treating them as a
//! length, so no byte is ever consumed speculatively.
//!
//! ## Concurrency and back-pressure
//!
//! Each accepted socket runs as an independent async task on [`TcpServer.group`]
//! (an [`Io.Group`]), so slow clients cannot block the accept loop. A
//! [`TcpServer.max_connections`] cap is enforced at accept time by an atomic
//! counter ([`TcpServer.active_connections`]); over the cap, the socket is
//! closed immediately rather than queued. The counter is incremented before the
//! task is spawned and decremented in [`handleConnection`]'s `defer`, so it
//! tracks live handlers even on error paths.
//!
//! ## Replication ordering invariant
//!
//! The `ShipWal` verb applies write-ahead-log records shipped from a primary.
//! [`runLoop`] enforces strict LSN ordering: a record whose LSN is at or below
//! `db.last_applied_lsn` is idempotently acked, a record exactly one past it is
//! applied and checkpointed, and any gap is rejected so the primary must resend
//! in order. This keeps a replica's log contiguous.
//!
//! Buffers for the JSON path are recycled through a [`MessageBufferPool`] to
//! keep per-request allocation off the hot path.

const std = @import("std");
const builtin = @import("builtin");
/// Async I/O namespace: the reader/writer/group/mutex primitives this server is
/// built on. Alias for `std.Io`.
const Io = std.Io;
/// Networking namespace ([`net.Server`], [`net.Stream`], [`net.IpAddress`]).
const net = std.Io.net;
/// The general-purpose allocator interface used for per-request scratch and the
/// buffer pool.
const Allocator = std.mem.Allocator;
/// The live database this server queries and mutates (schema/catalog, storage
/// engine, replication state such as `last_applied_lsn`).
const Database = @import("../schema.zig").Database;
/// Per-connection SQL execution engine; one instance is created per connection
/// so that connection-scoped state (e.g. authenticated-replica flag) is isolated.
const QueryExecutor = @import("query_executor.zig").QueryExecutor;
/// A parsed query request (SQL text plus an optional session token) handed to
/// [`QueryExecutor.execute`].
const QueryRequest = @import("query_executor.zig").QueryRequest;
/// The columns/rows/rows-affected/error result returned by
/// [`QueryExecutor.execute`], owned by the executor and freed by the caller.
const QueryResponse = @import("query_executor.zig").QueryResponse;
/// The legacy length-prefixed packet protocol (packet framing + operations).
const proto = @import("../common/proto.zig");
/// The compact binary wire protocol (magic-framed headers, typed payloads).
const binary_proto = @import("../proto/protocol.zig");
/// A single request/response message in the legacy [`proto`] dialect.
const Packet = proto.Packet;
/// The operation-code union carried inside a [`Packet`] (Query, Authenticate,
/// ShipWal, Reply, ...).
const Operation = proto.Operation;
/// The PostgreSQL-style frontend protocol constants; `wire.Frontend.startup` is
/// the first-byte sentinel that routes a connection to [`session.run`].
const wire = @import("../proto/wire.zig");
/// The PostgreSQL-style session state machine used for the startup dialect.
const session = @import("../proto/session.zig");
/// Per-connection session object for the startup dialect.
const Session = session.Session;
/// TLS library used to wrap accepted sockets when [`TcpServer.tls_opts`] is set.
const tls = @import("tls");
/// Server configuration, notably the `replica` credentials/toggle consulted by
/// the `Authenticate` and `ShipWal` verbs in [`runLoop`].
const Config = @import("../common/config.zig").Config;

/// Scoped logger for this module; connection-handler errors that are not benign
/// disconnects are surfaced here.
const log = std.log.scoped(.tcp_server);

/// The kaidb network front door: an accept loop that spawns one async handler
/// per connection and demultiplexes it onto the startup / JSON-packet / binary
/// dialects described in the module header.
///
/// A `TcpServer` owns no long-lived per-connection state itself; the shared
/// resources it holds are the [`Database`], the connection-count cap, the
/// [`MessageBufferPool`], and the [`Io.Group`] that tracks in-flight handlers so
/// [`stop`] can cancel them.
pub const TcpServer = struct {
    /// Allocator for scratch buffers, response marshalling, and the buffer pool.
    allocator: Allocator,
    /// Bind host (numeric IP or resolvable name) passed to [`net.IpAddress.parse`].
    host: []const u8,
    /// Bind TCP port.
    port: u16,
    /// The database served by every connection; shared, not per-connection.
    db: *Database,
    /// Hard cap on concurrent live connections, enforced at accept time against
    /// [`active_connections`]. Excess sockets are closed immediately.
    max_connections: usize,
    /// Optional server config; when absent the replication verbs
    /// (`Authenticate`/`ShipWal`) reply "Config not configured".
    config: ?*const Config,
    /// Live handler count. Incremented before a handler task is spawned and
    /// decremented in [`handleConnection`]'s `defer`; read at accept time to
    /// enforce [`max_connections`]. Atomic because handlers run concurrently.
    active_connections: std.atomic.Value(usize) = .init(0),
    /// Accept-loop run flag. [`listen`] sets it true; [`stop`] clears it so the
    /// loop exits after the in-flight `accept` returns or is cancelled.
    running: std.atomic.Value(bool) = .init(false),
    /// The bound listening socket, retained so [`stop`] can close it and unblock
    /// a parked `accept`. Null until [`listen`] binds.
    listen_server: ?net.Server = null,
    /// Async task group holding every spawned connection handler; [`stop`]
    /// cancels the group to tear all of them down.
    group: Io.Group = .init,
    /// TLS server options; when set, [`handleConnectionInner`] wraps the raw
    /// stream before running the protocol loop. Null means plaintext.
    tls_opts: ?tls.config.Server = null,
    /// PEM cert/key paths for TLS. When both are set, [`handleConnectionInner`]
    /// builds a fresh [`tls.config.Server`] per connection (fresh rng + `now`, so
    /// certificate validity is judged against the current time rather than a
    /// timestamp frozen at startup). Empty means no file-based TLS.
    tls_cert_path: []const u8 = "",
    tls_key_path: []const u8 = "",
    /// Recycled buffers for the JSON-packet read path; initialised in [`listen`],
    /// hence `undefined` until then. See [`MessageBufferPool`].
    buffer_pool: MessageBufferPool = undefined,

    /// Constructs a server value without binding a socket or allocating the
    /// buffer pool; the actual bind and pool init happen in [`listen`].
    ///
    /// The atomic/nullable fields keep their in-struct defaults, so the returned
    /// value is inert until [`listen`] runs.
    pub fn init(allocator: Allocator, host: []const u8, port: u16, db: *Database, max_connections: usize, config: ?*const Config) TcpServer {
        return .{
            .allocator = allocator,
            .host = host,
            .port = port,
            .db = db,
            .max_connections = max_connections,
            .config = config,
        };
    }

    /// Enables TLS for subsequently accepted connections by storing the server
    /// options; must be called before [`listen`] to affect that session.
    pub fn enableTls(self: *TcpServer, opts: tls.config.Server) void {
        self.tls_opts = opts;
    }

    /// Enables file-based TLS: stores the PEM cert/key paths so each accepted
    /// connection is TLS-wrapped (options built per connection in
    /// [`handleConnectionInner`]). Must be called before [`listen`].
    pub fn enableTlsFiles(self: *TcpServer, cert_path: []const u8, key_path: []const u8) void {
        self.tls_cert_path = cert_path;
        self.tls_key_path = key_path;
    }

    /// True when any TLS mode is configured (explicit options or cert/key files).
    pub fn tlsEnabled(self: *const TcpServer) bool {
        return self.tls_opts != null or (self.tls_cert_path.len > 0 and self.tls_key_path.len > 0);
    }

    /// Releases server-owned resources, i.e. the [`MessageBufferPool`]. Safe
    /// only after [`listen`] has initialised the pool.
    pub fn deinit(self: *TcpServer) void {
        self.buffer_pool.deinit();
    }

    /// Binds the socket and runs the accept loop until [`stop`] is called.
    ///
    /// Initialises the [`MessageBufferPool`], binds with `reuse_address` so a
    /// quick restart is not blocked by TIME_WAIT, then loops accepting sockets.
    /// For each accepted socket it: enforces [`max_connections`] (closing excess
    /// immediately), bumps [`active_connections`], sets `TCP_NODELAY` +
    /// `SO_KEEPALIVE` best-effort on non-Windows hosts (both are latency/liveness
    /// tunables whose failure is ignored), and spawns [`handleConnection`] onto
    /// [`group`]. An `accept` returning `error.Canceled` (from [`stop`]) breaks
    /// the loop; any other accept error is skipped so one bad accept cannot kill
    /// the listener. Returns when `running` is cleared or the accept is cancelled.
    pub fn listen(self: *TcpServer, io: Io) !void {
        self.buffer_pool = MessageBufferPool.init(self.allocator, io, 8192, 1000);
        const addr = try net.IpAddress.parse(self.host, self.port);
        var s = try addr.listen(io, .{ .reuse_address = true });
        self.listen_server = s;
        self.running.store(true, .seq_cst);

        while (self.running.load(.seq_cst)) {
            const stream = s.accept(io) catch |err| {
                if (err == error.Canceled) break;
                continue;
            };

            const active = self.active_connections.load(.acquire);
            if (active >= self.max_connections) {
                stream.close(io);
                continue;
            }

            _ = self.active_connections.fetchAdd(1, .release);

            if (builtin.os.tag != .windows) {
                const value: c_int = 1;
                std.posix.setsockopt(
                    stream.socket.handle,
                    std.posix.IPPROTO.TCP,
                    std.posix.TCP.NODELAY,
                    std.mem.asBytes(&value),
                ) catch {};

                const keepalive: c_int = 1;
                std.posix.setsockopt(
                    stream.socket.handle,
                    std.posix.SOL.SOCKET,
                    std.posix.SO.KEEPALIVE,
                    std.mem.asBytes(&keepalive),
                ) catch {};
            }

            self.group.async(io, handleConnection, .{ self, io, stream });
        }
    }

    /// Signals the accept loop to exit and tears down live connections.
    ///
    /// Clears `running`, closes the listening socket (which unblocks a parked
    /// `accept` with `error.Canceled`), and cancels [`group`] so every in-flight
    /// [`handleConnection`] task is cancelled. Idempotent-safe against a null
    /// `listen_server` (a server that never bound).
    pub fn stop(self: *TcpServer, io: Io) void {
        self.running.store(false, .seq_cst);
        if (self.listen_server) |*s| {
            s.socket.close(io);
        }
        self.group.cancel(io);
    }
};

/// A mutex-guarded free list of fixed-size byte buffers, reused across
/// JSON-packet reads so the hot path avoids an allocate/free per request.
///
/// Only buffers of exactly [`buffer_size`] are pooled: a request needing more
/// gets a one-off oversized allocation that is freed on [`release`] rather than
/// retained (see [`acquire`]/[`release`]). The pool is bounded by [`max_size`]
/// so a burst cannot grow it without limit.
pub const MessageBufferPool = struct {
    /// Backing allocator for pooled and oversized buffers.
    allocator: Allocator,
    /// The free list of available, standard-sized buffers.
    pool: std.ArrayList([]u8),
    /// Guards [`pool`] against concurrent handler access.
    mutex: std.Io.Mutex,
    /// I/O context the mutex locks/unlocks against.
    io: Io,
    /// The standard buffer length; only buffers of exactly this size are pooled.
    buffer_size: usize,
    /// Maximum number of buffers retained in [`pool`]; releases beyond this free
    /// the buffer instead of pooling it.
    max_size: usize,

    /// Creates an empty pool with the given standard buffer size and capacity;
    /// no buffers are pre-allocated (they accrue lazily via [`release`]).
    pub fn init(allocator: Allocator, io: Io, buffer_size: usize, pool_size: usize) MessageBufferPool {
        return MessageBufferPool{
            .allocator = allocator,
            .pool = .empty,
            .mutex = .init,
            .io = io,
            .buffer_size = buffer_size,
            .max_size = pool_size,
        };
    }

    /// Frees every pooled buffer and the backing list. Takes the lock so it is
    /// safe against a concurrent release, though callers should quiesce handlers
    /// first.
    pub fn deinit(self: *MessageBufferPool) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        for (self.pool.items) |buffer| {
            self.allocator.free(buffer);
        }
        self.pool.deinit(self.allocator);
    }

    /// Returns a buffer of at least `needed_size` bytes.
    ///
    /// If the request fits the standard [`buffer_size`] and a pooled buffer is
    /// available, that buffer is popped and returned (fast path, no allocation).
    /// Otherwise a fresh buffer is allocated at `max(buffer_size, needed_size)`.
    /// A buffer larger than [`buffer_size`] will not be re-pooled by [`release`].
    /// Every returned buffer must be handed back via [`release`].
    pub fn acquire(self: *MessageBufferPool, needed_size: usize) ![]u8 {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (needed_size <= self.buffer_size and self.pool.items.len > 0) {
            return self.pool.pop().?;
        }

        const size = if (needed_size < self.buffer_size) self.buffer_size else needed_size;
        return try self.allocator.alloc(u8, size);
    }

    /// Returns a buffer to the pool, or frees it.
    ///
    /// The buffer is re-pooled only if it is exactly [`buffer_size`] and the pool
    /// is below [`max_size`]; oversized or excess buffers are freed. If the
    /// bookkeeping append itself fails to allocate, the buffer is freed rather
    /// than leaked. Do not use a buffer after releasing it.
    pub fn release(self: *MessageBufferPool, buffer: []u8) void {
        self.mutex.lockUncancelable(self.io);
        defer self.mutex.unlock(self.io);

        if (buffer.len == self.buffer_size and self.pool.items.len < self.max_size) {
            self.pool.append(self.allocator, buffer) catch {
                self.allocator.free(buffer);
            };
        } else {
            self.allocator.free(buffer);
        }
    }
};

/// Marshals a [`QueryResponse`] to JSON on `writer` for the legacy packet path.
///
/// Emits `{"columns":[...],"rows":[[...],...],"rows_affected":N,"error_message":
/// ...}`. Every cell and column name is wrapped in quotes verbatim: this is a
/// deliberately minimal encoder that does NOT escape embedded quotes or control
/// characters, so it assumes cell text is already safe. `error_message` is the
/// JSON literal `null` when absent. The binary path uses a different encoder
/// (see [`runBinaryProtocolStream`]).
fn writeQueryResponse(writer: anytype, res: QueryResponse) !void {
    try writer.writeAll("{\"columns\":[");
    for (res.columns, 0..) |col, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.writeAll("\"");
        try writer.writeAll(col);
        try writer.writeAll("\"");
    }
    try writer.writeAll("],\"rows\":[");
    for (res.rows, 0..) |row, idx| {
        if (idx > 0) try writer.writeAll(",");
        try writer.writeAll("[");
        for (row, 0..) |cell, j| {
            if (j > 0) try writer.writeAll(",");
            try writer.writeAll("\"");
            try writer.writeAll(cell);
            try writer.writeAll("\"");
        }
        try writer.writeAll("]");
    }
    try writer.writeAll("],\"rows_affected\":");
    try writer.print("{d}", .{res.rows_affected});
    try writer.writeAll(",\"error_message\":");
    if (res.error_message) |msg| {
        try writer.writeAll("\"");
        try writer.writeAll(msg);
        try writer.writeAll("\"");
    } else {
        try writer.writeAll("null");
    }
    try writer.writeAll("}");
}

/// Top-level per-connection task spawned onto [`TcpServer.group`].
///
/// Owns the connection lifetime: on any exit it closes the socket and decrements
/// [`TcpServer.active_connections`] (the `defer`), so the count stays accurate
/// even when the handler errors. Delegates the actual protocol work to
/// [`handleConnectionInner`] and swallows the benign disconnect errors
/// (`EndOfStream`, `ReadFailed`, `Canceled`, `ConnectionResetByPeer`) silently;
/// anything else is logged. Returns `Io.Cancelable!void` so the group can cancel
/// it during [`TcpServer.stop`].
fn handleConnection(server: *TcpServer, io: Io, stream: net.Stream) Io.Cancelable!void {
    defer {
        stream.close(io);
        _ = server.active_connections.fetchSub(1, .release);
    }
    handleConnectionInner(server, io, stream) catch |err| {
        if (err != error.EndOfStream and err != error.ReadFailed and err != error.Canceled and err != error.ConnectionResetByPeer) {
            log.err("Connection handler error: {}", .{err});
        }
    };
}

/// Sets up per-connection state and the reader/writer, then enters [`runLoop`].
///
/// Creates a connection-scoped [`QueryExecutor`] (so replica-auth state and any
/// executor caches are isolated per connection). If [`TcpServer.tls_opts`] is
/// set the raw stream is wrapped with [`tls.serverFromStream`] and TLS-sized
/// read/write buffers are used; otherwise 4 KiB stack buffers back a plain
/// stream reader/writer. Either way the same [`runLoop`] drives the dialect
/// demux. Buffers live on the stack for the duration of the call.
fn handleConnectionInner(server: *TcpServer, io: Io, stream: net.Stream) !void {
    var connection_executor = QueryExecutor.init(server.allocator, server.db);
    defer connection_executor.deinit();

    if (server.tls_opts) |tls_cfg| {
        var tls_conn = try tls.serverFromStream(io, stream, tls_cfg);
        defer tls_conn.close() catch {};

        var tls_read_buf: [tls.input_buffer_len]u8 = undefined;
        var tls_write_buf: [tls.output_buffer_len]u8 = undefined;
        var r = tls_conn.reader(&tls_read_buf);
        var w = tls_conn.writer(&tls_write_buf);

        try runLoop(server, io, &r.interface, &w.interface, &connection_executor, true);
    } else if (server.tls_cert_path.len > 0 and server.tls_key_path.len > 0) {
        // File-based TLS: build options per connection so `now` (the cert-validity
        // reference) and the CSPRNG are fresh, and the CertKeyPair lifetime is
        // bounded to this connection. Mirrors the replication server path.
        var csprng: std.Random.DefaultCsprng = undefined;
        const rng = tlsRng(io, &csprng);
        var server_ck = tls.config.CertKeyPair.fromFilePath(server.allocator, io, Io.Dir.cwd(), server.tls_cert_path, server.tls_key_path) catch |err| {
            log.err("tcp server: loading TLS cert/key ({s}, {s}) failed: {any}", .{ server.tls_cert_path, server.tls_key_path, err });
            return;
        };
        defer server_ck.deinit(server.allocator);
        const opts = tls.config.Server{
            .rng = rng,
            .auth = &server_ck,
            .now = Io.Clock.real.now(io),
        };
        var tls_conn = tls.serverFromStream(io, stream, opts) catch |err| {
            log.warn("tcp server: TLS handshake failed ({any}); refusing connection", .{err});
            return;
        };
        defer tls_conn.close() catch {};

        var tls_read_buf: [tls.input_buffer_len]u8 = undefined;
        var tls_write_buf: [tls.output_buffer_len]u8 = undefined;
        var r = tls_conn.reader(&tls_read_buf);
        var w = tls_conn.writer(&tls_write_buf);

        try runLoop(server, io, &r.interface, &w.interface, &connection_executor, true);
    } else {
        var read_buf: [4096]u8 = undefined;
        var write_buf: [4096]u8 = undefined;
        var r = stream.reader(io, &read_buf);
        var w = stream.writer(io, &write_buf);

        try runLoop(server, io, &r.interface, &w.interface, &connection_executor, false);
    }
}

/// Seeds a per-connection CSPRNG from the platform RNG and returns a
/// `std.Random` for the TLS handshake. A fresh instance per connection keeps
/// handshake nonces independent.
fn tlsRng(io: Io, csprng: *std.Random.DefaultCsprng) std.Random {
    var seed: [32]u8 = undefined;
    std.Io.random(io, &seed);
    csprng.* = std.Random.DefaultCsprng.init(seed);
    return csprng.random();
}

/// The protocol demultiplexer and request loop for one connection.
///
/// First it peeks the leading byte (without consuming it): if it is
/// [`wire.Frontend.startup`] the whole connection is a PostgreSQL-style session
/// and is handed to [`session.run`], which owns it until close. A clean
/// `EndOfStream` before any byte is a silent, empty connection and returns.
///
/// Otherwise the connection is length-prefixed. Each iteration reads a 4-byte
/// little-endian header. Because the binary protocol's
/// [`binary_proto.MessageHeader.MAGIC_VALUE`] and the JSON path's payload length
/// share those 4 bytes, the value is compared against the magic first: a match
/// upgrades the connection to [`runBinaryProtocolStream`] (and ends this loop);
/// otherwise the value IS the payload length and a [`proto.Packet`] is read into
/// a pooled buffer and dispatched by op:
///
///   - `Query`, executes via `connection_executor`, JSON-encodes the result
///     with [`writeQueryResponse`], and replies with a `Reply` packet. All
///     executor-owned result memory is freed on the way out.
///   - `Authenticate`, checks the presented uid/key against
///     `config.replica`; on success marks the executor an authenticated replica.
///   - `ShipWal`, replication apply, guarded by the LSN-ordering invariant
///     (idempotent ack at/below `last_applied_lsn`, apply+checkpoint at exactly
///     +1, reject any gap). Requires replica auth when `config.replica.enabled`.
///   - anything else, a "Unsupported operation" error reply.
///
/// A malformed packet produces an error reply and the loop continues rather than
/// dropping the connection. The loop ends on `EndOfStream` or a binary upgrade.
fn runLoop(
    server: *TcpServer,
    io: Io,
    reader: *Io.Reader,
    writer: *Io.Writer,
    connection_executor: *QueryExecutor,
    secure: bool,
) !void {

    {
        const first = reader.peekByte() catch |err| {
            if (err == error.EndOfStream) return;
            return err;
        };
        if (first == @intFromEnum(wire.Frontend.startup)) {
            var sess = Session.init(server.allocator, io, server.db, 0, null);
            sess.secure = secure;
            defer sess.deinit();
            return session.run(&sess, reader, writer);
        }
    }

    while (true) {
        var len_bytes: [4]u8 = undefined;
        reader.readSliceAll(&len_bytes) catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };
        const magic = std.mem.readInt(u32, &len_bytes, .little);
        if (magic == binary_proto.MessageHeader.MAGIC_VALUE) {
            try runBinaryProtocolStream(server, io, reader, writer, connection_executor, len_bytes);
            break;
        }
        const payload_len = magic;

        const packet_bytes = try server.buffer_pool.acquire(4 + payload_len);
        defer server.buffer_pool.release(packet_bytes);
        @memcpy(packet_bytes[0..4], &len_bytes);

        try reader.readSliceAll(packet_bytes[4..][0..payload_len]);

        const packet = Packet.deserialize(server.allocator, packet_bytes[0 .. 4 + payload_len]) catch |err| {
            const err_msg = try std.fmt.allocPrint(server.allocator, "Invalid packet payload: {any}", .{err});
            defer server.allocator.free(err_msg);
            try sendErrorReply(server, writer, err_msg);
            continue;
        };
        defer Packet.free(server.allocator, packet);

        switch (packet.op) {
            .Query => |q| {
                const query_req = QueryRequest{
                    .sql = q.sql,
                    .session_token = q.session_token,
                };

                const res = connection_executor.execute(query_req) catch |err| {
                    const err_msg = try std.fmt.allocPrint(server.allocator, "Execution Error: {any}", .{err});
                    defer server.allocator.free(err_msg);
                    try sendErrorReply(server, writer, err_msg);
                    continue;
                };
                defer {
                    if (res.error_message) |m| server.allocator.free(m);
                    for (res.columns) |c| server.allocator.free(c);
                    server.allocator.free(res.columns);
                    for (res.rows) |rows_list| {
                        for (rows_list) |c| server.allocator.free(c);
                        server.allocator.free(rows_list);
                    }
                    server.allocator.free(res.rows);
                }

                var response_allocating = std.Io.Writer.Allocating.init(server.allocator);
                defer response_allocating.deinit();
                try writeQueryResponse(&response_allocating.writer, res);
                const response_bytes = response_allocating.written();

                const reply_packet = Packet{
                    .op = .{
                        .Reply = .{
                            .status = .Ok,
                            .data = response_bytes,
                        },
                    },
                };

                var serialize_allocating = std.Io.Writer.Allocating.init(server.allocator);
                defer serialize_allocating.deinit();
                try reply_packet.serialize(&serialize_allocating.writer);
                const serialized_bytes = serialize_allocating.written();

                try writer.writeAll(serialized_bytes);
                try writer.flush();
            },
            .Authenticate => |auth| {
                const cfg = server.config orelse {
                    try sendErrorReply(server, writer, "Config not configured");
                    continue;
                };
                if (std.mem.eql(u8, auth.uid, cfg.replica.uid) and
                    std.mem.eql(u8, auth.key, cfg.replica.key))
                {
                    connection_executor.is_authenticated_replica = true;
                    try sendOkReply(server, writer);
                } else {
                    try sendErrorReply(server, writer, "Authentication failed");
                }
            },
            .ShipWal => |ship| {
                const cfg = server.config orelse {
                    try sendErrorReply(server, writer, "Config not configured");
                    continue;
                };
                if (cfg.replica.enabled and !connection_executor.is_authenticated_replica) {
                    try sendErrorReply(server, writer, "Not authenticated");
                    continue;
                }

                if (ship.lsn <= server.db.last_applied_lsn) {
                    try sendOkReply(server, writer);
                    continue;
                }

                if (ship.lsn != server.db.last_applied_lsn + 1) {
                    try sendErrorReply(server, writer, "LSN gap detected");
                    continue;
                }

                server.db.post(ship) catch |err| {
                    const err_msg = try std.fmt.allocPrint(server.allocator, "Failed to apply WAL: {any}", .{err});
                    defer server.allocator.free(err_msg);
                    try sendErrorReply(server, writer, err_msg);
                    continue;
                };

                server.db.last_applied_lsn = ship.lsn;
                server.db.saveReplCheckpoint(ship.lsn) catch |err| {
                    log.err("Failed to save replica checkpoint: {any}", .{err});
                };

                try sendOkReply(server, writer);
            },
            else => {
                try sendErrorReply(server, writer, "Unsupported operation");
            },
        }
    }
}

/// Serialises and flushes a legacy `Reply` packet with `Error` status carrying
/// `err_msg` as its payload. Used on the JSON-packet path for malformed packets,
/// execution failures, and auth/replication rejections.
fn sendErrorReply(server: *TcpServer, writer: *Io.Writer, err_msg: []const u8) !void {
    const reply_packet = Packet{
        .op = .{
            .Reply = .{
                .status = .Error,
                .data = err_msg,
            },
        },
    };

    var serialize_allocating = std.Io.Writer.Allocating.init(server.allocator);
    defer serialize_allocating.deinit();
    try reply_packet.serialize(&serialize_allocating.writer);
    const serialized_bytes = serialize_allocating.written();

    try writer.writeAll(serialized_bytes);
    try writer.flush();
}

/// Serialises and flushes a legacy `Reply` packet with `Ok` status and no
/// payload; the success ack for `Authenticate` and `ShipWal` on the JSON path.
fn sendOkReply(server: *TcpServer, writer: *Io.Writer) !void {
    const reply_packet = Packet{
        .op = .{
            .Reply = .{
                .status = .Ok,
                .data = null,
            },
        },
    };

    var serialize_allocating = std.Io.Writer.Allocating.init(server.allocator);
    defer serialize_allocating.deinit();
    try reply_packet.serialize(&serialize_allocating.writer);
    const serialized_bytes = serialize_allocating.written();

    try writer.writeAll(serialized_bytes);
    try writer.flush();
}

/// Frames and flushes a binary-protocol message whose body is a POD struct.
///
/// Writes a [`binary_proto.MessageHeader`] (magic, type, stream id, length)
/// followed by the raw little-endian bytes of `payload` (a pointer, per
/// `std.mem.asBytes`). Use [`sendBinaryPayload`] when the body is already a byte
/// slice rather than a struct.
fn sendBinaryResponse(writer: *Io.Writer, stream_id: u16, msg_type: binary_proto.MessageType, payload: anytype) !void {
    const payload_bytes = std.mem.asBytes(payload);
    const header = binary_proto.MessageHeader.init(msg_type, stream_id, payload_bytes.len);
    try writer.writeAll(std.mem.asBytes(&header));
    try writer.writeAll(payload_bytes);
    try writer.flush();
}

/// Frames and flushes a binary-protocol message whose body is a ready byte
/// slice (e.g. an already-serialised query result). Counterpart to
/// [`sendBinaryResponse`], which takes a struct pointer.
fn sendBinaryPayload(writer: *Io.Writer, stream_id: u16, msg_type: binary_proto.MessageType, payload_bytes: []const u8) !void {
    const header = binary_proto.MessageHeader.init(msg_type, stream_id, payload_bytes.len);
    try writer.writeAll(std.mem.asBytes(&header));
    try writer.writeAll(payload_bytes);
    try writer.flush();
}

/// Frames and flushes a binary-protocol `err` message for `stream_id`.
///
/// The body is an [`binary_proto.ErrorPayloadHeader`] (numeric `error_code` plus
/// message length) followed by the message text, so this writes the frame in
/// three parts (message header, error header, text) rather than reusing the
/// single-blob helpers. The codes used here mirror HTTP-ish semantics: 401 auth
/// failure, 426 protocol-too-new, 400 unsupported message, 500 execution error.
fn sendBinaryErrorReply(writer: *Io.Writer, stream_id: u16, error_code: u32, message: []const u8) !void {
    const msg_hdr = binary_proto.ErrorPayloadHeader{
        .error_code = error_code,
        .message_len = @intCast(message.len),
    };
    const payload_len = @sizeOf(binary_proto.ErrorPayloadHeader) + message.len;
    const header = binary_proto.MessageHeader.init(.err, stream_id, payload_len);
    try writer.writeAll(std.mem.asBytes(&header));
    try writer.writeAll(std.mem.asBytes(&msg_hdr));
    try writer.writeAll(message);
    try writer.flush();
}

/// Runs the compact binary wire protocol for a connection that opened with the
/// magic header, up to `close` or disconnect.
///
/// `initial_bytes` are the 4 magic bytes [`runLoop`] already consumed; they are
/// prepended to the remaining 12 header bytes to reconstruct the full 16-byte
/// [`binary_proto.MessageHeader`]. The handshake is strict: the first message
/// MUST be `connect` (else 401), the peer's protocol version must not exceed
/// [`binary_proto.PROTOCOL_VERSION`] (else 426; a version above `u16` range is
/// clamped for the comparison), and credentials are checked via
/// `db.security_manager.authenticate` (failure → 401). On success a
/// `connect_resp` with a session id is sent.
///
/// After the handshake it loops per message: read a 16-byte header + its
/// payload, then dispatch by [`binary_proto.MessageType`]:
///   - `query`, reads the tx-mode/SQL/param-count preamble, executes, and on
///     success serialises a column-descriptor + row block via
///     [`binary_proto.ProtocolWriter`]/[`binary_proto.serializeQueryRow`]; on
///     error sends a 500. Every column is described as `TEXT`. Executor-owned
///     result memory is freed on the way out.
///   - `fetch`, replies with an empty result block (cursor streaming is a stub
///     here: zero rows, no-more flag).
///   - `close`, ends the loop, closing the connection.
///   - anything else, a 400 "Unsupported message type" reply.
///
/// The `io` parameter is currently unused on this path (framing is synchronous
/// over the already-established reader/writer).
fn runBinaryProtocolStream(
    server: *TcpServer,
    io: Io,
    reader: *Io.Reader,
    writer: *Io.Writer,
    connection_executor: *QueryExecutor,
    initial_bytes: [4]u8,
) !void {
    _ = io;
    var header_bytes: [16]u8 = undefined;
    @memcpy(header_bytes[0..4], &initial_bytes);
    
    try reader.readSliceAll(header_bytes[4..16]);
    
    var hdr_reader = binary_proto.ProtocolReader.init(&header_bytes);
    const header = try hdr_reader.readStruct(binary_proto.MessageHeader);
    
    if (!header.isValid()) return error.InvalidMagic;
    
    const payload = try server.allocator.alloc(u8, @intCast(header.payload_len));
    defer server.allocator.free(payload);
    try reader.readSliceAll(payload);
    
    var payload_reader = binary_proto.ProtocolReader.init(payload);
    
    if (header.msg_type != @intFromEnum(binary_proto.MessageType.connect)) {
        try sendBinaryErrorReply(writer, header.stream_id, 401, "Expected CONNECT message first");
        return;
    }
    
    const protocol_version = try payload_reader.readU32();
    const peer_version: u16 = if (protocol_version > std.math.maxInt(u16)) std.math.maxInt(u16) else @intCast(protocol_version);
    if (peer_version > binary_proto.PROTOCOL_VERSION) {
        try sendBinaryErrorReply(writer, header.stream_id, 426, "Protocol version too new; upgrade the server");
        return;
    }
    const username_len = try payload_reader.readU16();
    const username = try payload_reader.readString(username_len);
    const password_len = try payload_reader.readU16();
    const password = try payload_reader.readString(password_len);
    const db_name_len = try payload_reader.readU16();
    const db_name = try payload_reader.readString(db_name_len);
    
    _ = db_name;
    
    _ = server.db.security_manager.authenticate(username, password, null) catch {
        try sendBinaryErrorReply(writer, header.stream_id, 401, "Authentication failed");
        return;
    };
    
    const resp_payload = binary_proto.ConnectRespPayload{
        .status = 0,
        .session_id = 12345,
    };
    try sendBinaryResponse(writer, header.stream_id, .connect_resp, &resp_payload);
    
    while (true) {
        var msg_hdr_bytes: [16]u8 = undefined;
        reader.readSliceAll(&msg_hdr_bytes) catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };
        
        var m_reader = binary_proto.ProtocolReader.init(&msg_hdr_bytes);
        const m_header = try m_reader.readStruct(binary_proto.MessageHeader);
        if (!m_header.isValid()) return error.InvalidMagic;
        
        const m_payload = try server.allocator.alloc(u8, @intCast(m_header.payload_len));
        defer server.allocator.free(m_payload);
        try reader.readSliceAll(m_payload);
        
        var m_payload_reader = binary_proto.ProtocolReader.init(m_payload);
        
        switch (@as(binary_proto.MessageType, @enumFromInt(m_header.msg_type))) {
            .query => {
                const tx_mode = try m_payload_reader.readU8();
                _ = tx_mode;
                const sql_len = try m_payload_reader.readU32();
                const sql = try m_payload_reader.readString(sql_len);
                
                const param_count = try m_payload_reader.readU16();
                _ = param_count;
                
                const res = connection_executor.execute(.{ .sql = sql }) catch |err| {
                    const err_msg = try std.fmt.allocPrint(server.allocator, "Execution Error: {any}", .{err});
                    defer server.allocator.free(err_msg);
                    try sendBinaryErrorReply(writer, m_header.stream_id, 500, err_msg);
                    continue;
                };
                defer {
                    if (res.error_message) |m| server.allocator.free(m);
                    for (res.columns) |c| server.allocator.free(c);
                    server.allocator.free(res.columns);
                    for (res.rows) |r| {
                        for (r) |c| server.allocator.free(c);
                        server.allocator.free(r);
                    }
                    server.allocator.free(res.rows);
                }
                
                if (res.error_message) |msg| {
                    try sendBinaryErrorReply(writer, m_header.stream_id, 500, msg);
                    continue;
                }
                
                var writer_payload = binary_proto.ProtocolWriter.init(server.allocator);
                defer writer_payload.deinit();
                
                const is_select = res.columns.len > 0;
                try writer_payload.writeStruct(@as(u8, if (is_select) 0 else 1));
                try writer_payload.writeStruct(res.rows_affected);
                try writer_payload.writeStruct(@as(u16, @intCast(res.columns.len)));
                
                var offset: u32 = 0;
                for (res.columns) |col_name| {
                    const name_len: u16 = @intCast(col_name.len);
                    try writer_payload.writeStruct(name_len);
                    try writer_payload.writeString(col_name);
                    
                    try writer_payload.writeStruct(@as(u8, @intFromEnum(binary_proto.ColumnType.TEXT)));
                    try writer_payload.writeStruct(offset);
                    try writer_payload.writeStruct(@as(u32, 4));
                    offset += 4;
                }
                
                try writer_payload.writeStruct(@as(u8, if (res.rows.len > 0) 1 else 0));
                
                if (res.rows.len > 0) {
                    try writer_payload.writeStruct(@as(u32, @intCast(res.rows.len)));
                    for (res.rows) |row| {
                        const row_bytes = try binary_proto.serializeQueryRow(server.allocator, row);
                        defer server.allocator.free(row_bytes);
                        try writer_payload.writeString(row_bytes);
                    }
                }
                
                const payload_bytes = try writer_payload.toOwnedSlice();
                defer server.allocator.free(payload_bytes);
                
                try sendBinaryPayload(writer, m_header.stream_id, .query_resp, payload_bytes);
            },
            .fetch => {
                var writer_payload = binary_proto.ProtocolWriter.init(server.allocator);
                defer writer_payload.deinit();
                try writer_payload.writeStruct(@as(u32, 0));
                try writer_payload.writeStruct(@as(u8, 0));
                
                const payload_bytes = try writer_payload.toOwnedSlice();
                defer server.allocator.free(payload_bytes);
                try sendBinaryPayload(writer, m_header.stream_id, .fetch_resp, payload_bytes);
            },
            .close => {
                break;
            },
            else => {
                try sendBinaryErrorReply(writer, m_header.stream_id, 400, "Unsupported message type");
            }
        }
    }
}
