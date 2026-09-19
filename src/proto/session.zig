//! Per-connection session state machine for the kaidb binary wire protocol.
//!
//! One [`Session`] exists for the lifetime of a single client connection. It
//! owns the client's per-connection state (authentication, prepared statements,
//! open portals) and drives the request/response loop in [`run`], which reads
//! framed frontend messages off the socket and writes framed backend messages
//! back. The frame format and the message encoders/decoders live in `wire.zig`;
//! this file is purely the protocol *logic* that sequences them.
//!
//! ## Protocol shape
//!
//! The wire protocol is modelled closely on PostgreSQL's, so the message types
//! and SQLSTATE codes used here (`08P01`, `28P01`, `42601`, ...) are the same
//! ones a Postgres client would expect. Two request styles are supported:
//!
//!   - **Simple query** ([`wire.Frontend.query`]): one SQL string in, a full
//!     result set out, terminated by a `ReadyForQuery`. Handled by [`runSql`].
//!   - **Extended query** (Parse / Bind / Describe / Execute / Sync): the SQL is
//!     first parsed into a named prepared statement ([`handleParse`]), then a
//!     Bind ([`handleBind`]) substitutes parameter values to produce a portal,
//!     and Execute ([`streamPortal`]) streams rows from that portal, optionally
//!     in `max_rows`-sized chunks with a `PortalSuspended` in between. Because
//!     kaidb substitutes parameters textually rather than binding them into a
//!     plan, a "portal" here just carries the fully substituted SQL string plus
//!     a cached result and a cursor over its rows.
//!
//! ## Ownership and buffers
//!
//! Backend messages are built into freshly allocated byte slices by the
//! `wire.encode*` helpers and are freed immediately after being written; see
//! [`sendOwned`], which owns-then-frees. Inbound frame payloads are read into
//! buffers taken from an optional [`MessageBufferPool`] (falling back to the raw
//! allocator) via [`Session.acquireBuf`] / [`Session.releaseBuf`], so that the
//! hot request path can reuse fixed-size buffers instead of churning the
//! allocator. [`freeResponse`] is the counterpart that releases the deep,
//! per-cell allocations a [`qe.QueryResponse`] holds.
//!
//! ## Authentication
//!
//! Auth is negotiated once, at startup. If the database's security manager is
//! enabled and has users, [`run`] issues a cleartext-password challenge, reads
//! the password message, and authenticates; on success it stashes the session
//! token, which is later passed (hex-encoded by [`Session.tokenHex`]) into the
//! executor so row-level/permission checks can see the authenticated principal.

const std = @import("std");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const mem = std.mem;

const wire = @import("wire.zig");
const oidmap = @import("oidmap.zig");
const command = @import("command.zig");
const qe = @import("../query/query_executor.zig");
const ColumnType = @import("../schema/types.zig").ColumnType;
const Database = @import("../schema.zig").Database;
const StopWatch = @import("utils").StopWatch;
/// Re-export of the fixed-size buffer pool used for inbound frame payloads.
///
/// Re-exported here so callers that construct a [`Session`] can name the pool
/// type they pass to [`Session.init`] without importing the pool module
/// directly.
pub const MessageBufferPool = @import("message_buffer_pool.zig").MessageBufferPool;

/// A parsed, named prepared statement in the extended-query protocol.
///
/// Created by [`handleParse`] and looked up by name during [`handleBind`]. Both
/// fields are heap-owned by the [`Session`] and are freed in
/// [`Session.clearState`] (or when a same-named statement is replaced).
const Prepared = struct {
    /// The statement's SQL text, with `$1`/`?` placeholders left intact for
    /// [`command.substituteParams`] to fill in at Bind time. Owned copy.
    sql: []u8,
    /// One [`command.ParamClass`] per placeholder, derived from the Parse
    /// message's declared parameter OIDs via [`classForOid`]. Tells the
    /// substitution step whether each argument should be quoted as text,
    /// rendered as a boolean, or emitted as a bare numeric literal.
    classes: []command.ParamClass,
};

/// An open portal: a prepared statement with its parameters already bound.
///
/// In this engine a portal carries the fully substituted SQL (parameters are
/// textually inlined rather than bound into a plan), a lazily materialised
/// result set, and a cursor for chunked delivery. Created by [`handleBind`],
/// executed and streamed by [`streamPortal`], and torn down in
/// [`Session.clearState`].
const Portal = struct {
    /// The final SQL to execute, with all bind parameters already substituted
    /// in. Owned copy.
    sql: []u8,
    /// The cached result of executing [`Portal.sql`], or `null` until the first
    /// Execute/Describe forces execution via [`ensureExecuted`]. Caching lets a
    /// Describe followed by one or more chunked Executes run the query exactly
    /// once. Freed with [`freeResponse`].
    result: ?qe.QueryResponse = null,
    /// Index of the next unsent row in [`Portal.result`]. Advanced by
    /// [`streamPortal`] across successive Execute messages so that a
    /// `max_rows`-limited Execute resumes where the previous one stopped.
    cursor: usize = 0,
    /// Whether a `RowDescription` has already been sent for this portal. Guards
    /// against emitting the column header twice when a Describe precedes an
    /// Execute (both would otherwise send it).
    described: bool = false,
};

/// All state for one client connection over the binary protocol.
///
/// A `Session` is created per accepted connection, driven by [`run`] until the
/// client disconnects or sends Terminate, and then torn down with
/// [`Session.deinit`]. It is single-threaded with respect to its own socket:
/// the request loop reads one frame, fully handles it, flushes the reply, and
/// only then reads the next.
pub const Session = struct {
    /// Allocator used for all per-session heap: statement/portal maps, owned SQL
    /// copies, and the transient encode buffers freed by [`sendOwned`].
    allocator: Allocator,
    /// Async I/O context handle, carried for use by I/O the session initiates.
    io: Io,
    /// The SQL query executor bound to this session's database. Runs both simple
    /// queries and portal executions; re-initialised on [`Session.reset`].
    executor: qe.QueryExecutor,
    /// Named prepared statements, keyed by statement name (the empty string is
    /// the unnamed statement). Keys and values are session-owned.
    prepared: std.StringHashMapUnmanaged(Prepared) = .{},
    /// Open portals, keyed by portal name (empty string = unnamed portal). Keys
    /// and values are session-owned.
    portals: std.StringHashMapUnmanaged(Portal) = .{},

    /// Scratch read buffer reserved with the session (fixed 16 KiB). Present for
    /// callers that wire the session to a buffered reader.
    read_buffer: [16 * 1024]u8 = undefined,
    /// Scratch write buffer reserved with the session (fixed 16 KiB). Present for
    /// callers that wire the session to a buffered writer.
    write_buffer: [16 * 1024]u8 = undefined,
    /// True once the client has successfully authenticated (or once auth was
    /// skipped because the security manager is disabled/has no users).
    authenticated: bool = false,
    /// The 32-byte token minted by the security manager on successful auth, or
    /// `null` when unauthenticated. Passed to the executor (hex-encoded by
    /// [`Session.tokenHex`]) to identify the principal for access checks.
    session_token: ?[32]u8 = null,
    /// Timestamp of the last activity, in milliseconds. Reserved for idle
    /// enforcement against [`Session.idle_timeout_ms`].
    last_activity_ms: i64 = 0,
    /// Idle timeout in milliseconds after which an inactive connection may be
    /// reaped. Defaults to 60 seconds; overridden via [`Session.init`].
    idle_timeout_ms: u64 = 60_000,
    /// Optional shared pool for inbound frame payload buffers. When set, frame
    /// reads borrow from it (see [`Session.acquireBuf`]); when `null`, the raw
    /// allocator is used instead.
    msg_pool: ?*MessageBufferPool = null,
    /// True when the underlying transport is TLS-encrypted. Set by the connection
    /// handler before [`run`]. Consulted by the startup handler to refuse the
    /// cleartext-password exchange on a plaintext link when
    /// `SecurityManager.require_tls_for_auth` is on.
    secure: bool = false,

    /// Constructs a session bound to `db` with the given idle timeout and
    /// optional buffer pool.
    ///
    /// Does not perform any I/O or authentication; the connection is unauthenticated
    /// until [`run`] negotiates auth. The `read_buffer`/`write_buffer` and the
    /// statement/portal maps are left at their `undefined`/empty defaults.
    pub fn init(allocator: Allocator, io: Io, db: *Database, idle_timeout_ms: u64, msg_pool: ?*MessageBufferPool) Session {
        return .{
            .allocator = allocator,
            .io = io,
            .executor = qe.QueryExecutor.init(allocator, db),
            .idle_timeout_ms = idle_timeout_ms,
            .msg_pool = msg_pool,
        };
    }

    /// Resets the session for reuse by a fresh connection.
    ///
    /// Frees all prepared statements and portals via [`Session.clearState`],
    /// rebuilds the executor on the same database, and clears authentication
    /// (token and `authenticated` flag). Used when a pooled session object is
    /// handed to a new client so no state leaks across connections.
    pub fn reset(self: *Session) void {
        self.clearState();
        self.executor = qe.QueryExecutor.init(self.allocator, self.executor.db);
        self.authenticated = false;
        self.session_token = null;
        self.last_activity_ms = 0;
    }

    /// Hex-encodes the session token into `buf`, returning the 64-char slice.
    ///
    /// Returns `null` when the session is unauthenticated (no token). The caller
    /// supplies the 64-byte buffer so no allocation is needed on the hot query
    /// path; the returned slice borrows it. The executor takes this hex string as
    /// its `session_token` to attribute the query to the authenticated user.
    fn tokenHex(self: *Session, buf: *[64]u8) ?[]const u8 {
        const tok = self.session_token orelse return null;
        const hexchars = "0123456789abcdef";
        for (tok, 0..) |b, i| {
            buf[i * 2] = hexchars[b >> 4];
            buf[i * 2 + 1] = hexchars[b & 0x0f];
        }
        return buf[0..64];
    }

    /// Frees every prepared statement and portal and empties both maps.
    ///
    /// For each prepared statement it frees the owned key, SQL, and param-class
    /// array; for each portal it frees the key, SQL, and any cached result (via
    /// [`freeResponse`]). Retains the maps' capacity so a subsequent reuse does
    /// not reallocate. Shared by [`Session.reset`] and [`Session.deinit`].
    fn clearState(self: *Session) void {
        var pit = self.prepared.iterator();
        while (pit.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.sql);
            self.allocator.free(e.value_ptr.classes);
        }
        self.prepared.clearRetainingCapacity();
        var oit = self.portals.iterator();
        while (oit.next()) |e| {
            self.allocator.free(e.key_ptr.*);
            self.allocator.free(e.value_ptr.sql);
            if (e.value_ptr.result) |r| freeResponse(self.allocator, r);
        }
        self.portals.clearRetainingCapacity();
    }

    /// Tears the session down completely.
    ///
    /// Frees all statements/portals ([`Session.clearState`]), then releases the
    /// two hash maps' backing storage and the executor. After this the session
    /// object must not be used again.
    pub fn deinit(self: *Session) void {
        self.clearState();
        self.prepared.deinit(self.allocator);
        self.portals.deinit(self.allocator);
        self.executor.deinit();
    }

    /// Obtains a payload buffer of at least `size` bytes.
    ///
    /// Borrows from the [`MessageBufferPool`] when one is configured (avoiding an
    /// allocation on the frame-read hot path), otherwise falls back to the raw
    /// allocator. Every buffer returned here must be handed back to
    /// [`Session.releaseBuf`], which routes to the matching source.
    pub fn acquireBuf(self: *Session, size: usize) ![]u8 {
        if (self.msg_pool) |mp| return mp.acquire(size);
        return self.allocator.alloc(u8, size);
    }
    /// Returns a buffer obtained from [`Session.acquireBuf`] to its source.
    ///
    /// Routes back to the pool when one is configured, else frees via the raw
    /// allocator. Must be called with the same pool-vs-allocator configuration
    /// that produced the buffer.
    pub fn releaseBuf(self: *Session, buf: []u8) void {
        if (self.msg_pool) |mp| mp.release(buf) else self.allocator.free(buf);
    }
};

/// A single decoded protocol frame: a one-byte type tag plus its payload.
///
/// `payload` is the slice the handlers read from; `buf` is the underlying pooled
/// buffer that must be returned to the session via [`releaseFrame`]. They differ
/// for a zero-length payload, where `buf` is empty and nothing needs freeing.
const Frame = struct {
    /// The frontend message type byte (see [`wire.Frontend`]).
    type: u8,
    /// The message body, `buf[0..plen]`, or an empty slice for a bodyless frame.
    payload: []u8,
    /// The owning buffer to release, or empty when no buffer was allocated.
    buf: []u8,
};

/// Reads one framed message from `reader`, or `null` at a clean end of stream.
///
/// The frame header is a one-byte type followed by a big-endian `u32` length
/// that counts itself (so the payload is `len - 4` bytes). Rejects a length
/// below 4 or above [`wire.max_frame_len`] with `error.BadFrame` to bound the
/// per-message allocation. A zero-length payload returns a frame with empty
/// slices and no pooled buffer. On any other short read the error propagates;
/// `error.EndOfStream` on the *header* is the normal disconnect and maps to
/// `null`. The returned frame owns a pooled buffer that the caller must free
/// with [`releaseFrame`]; `errdefer` releases it if the payload read fails.
fn readFrame(sess: *Session, reader: *Io.Reader) !?Frame {
    var hdr: [5]u8 = undefined;
    reader.readSliceAll(&hdr) catch |err| {
        if (err == error.EndOfStream) return null;
        return err;
    };
    const len = mem.readInt(u32, hdr[1..5], .big);
    if (len < 4 or len > wire.max_frame_len) return error.BadFrame;
    const plen = len - 4;
    if (plen == 0) return .{ .type = hdr[0], .payload = &.{}, .buf = &.{} };
    const buf = try sess.acquireBuf(plen);
    errdefer sess.releaseBuf(buf);
    try reader.readSliceAll(buf[0..plen]);
    return .{ .type = hdr[0], .payload = buf[0..plen], .buf = buf };
}

/// Returns a frame's pooled payload buffer to the session.
///
/// A no-op for a bodyless frame (empty `buf`), so it is safe to `defer` over any
/// frame returned by [`readFrame`].
fn releaseFrame(sess: *Session, frame: Frame) void {
    if (frame.buf.len > 0) sess.releaseBuf(frame.buf);
}

/// Writes a pre-encoded backend message to `writer`.
///
/// A thin seam over `writeAll` so all outbound writes funnel through one place;
/// the buffer is owned by the caller (freed by [`sendOwned`]) and this only
/// copies it into the writer.
fn writeMsg(writer: *Io.Writer, owned: []const u8) !void {
    try writer.writeAll(owned);
}

/// The request/response loop: reads frames and dispatches by message type.
///
/// Runs until the client disconnects (a `null` frame from [`readFrame`]), sends
/// a Terminate, or an unrecoverable I/O error propagates. Each iteration reads
/// one frame, `defer`-releases its buffer, dispatches on [`wire.Frontend`], and
/// flushes the writer before looping so the client sees a complete reply.
///
/// The `startup` case additionally performs the one-time auth handshake inline:
/// when the security manager is enabled with users it challenges for a cleartext
/// password, reads the follow-up message, and authenticates, replying with a
/// fatal error and returning on any failure. Malformed control messages are
/// answered with the appropriate Postgres SQLSTATE error rather than crashing.
/// Data-plane errors (from [`runSql`] / [`streamPortal`]) are reported as error
/// responses inside those handlers, so the loop keeps the connection alive.
pub fn run(sess: *Session, reader: *Io.Reader, writer: *Io.Writer) !void {
    const allocator = sess.allocator;
    // A connection that drops mid-transaction must not leave the tx pinned.

    while (true) {
        const maybe = readFrame(sess, reader) catch |err| {
            return err;
        };
        const frame = maybe orelse break;
        defer releaseFrame(sess, frame);

        const t: wire.Frontend = @enumFromInt(frame.type);

        // Single-surface gate: this instance serves SQL only (relational) or
        // documents only (document). Reject the other surface's data-plane frames
        // with SQLSTATE 0A000 (feature not supported), keeping the connection
        // alive. Auth (startup/auth_response) and protocol control (sync/close/
        // terminate) are allowed in both modes.
        const blocked: ?[]const u8 = if (t == .doc_op) "document operations are not supported: this server is relational (SQL) only" else null;
        if (blocked) |msg| {
            try sendOwned(writer, allocator, try wire.encodeError(allocator, "ERROR", "0A000", msg));
            try sendOwned(writer, allocator, try wire.encodeReady(allocator, .idle));
            try writer.flush();
            continue;
        }

        // Auth enforcement: with `require_auth` on, every data-plane frame requires an
        // authenticated session (SQLSTATE 28000). The startup challenge already blocks
        // unauthenticated connects when auth is enabled; this is defence-in-depth that
        // also fail-closes any data frame arriving before/without authentication. Auth
        // frames (startup/auth_response) and protocol control (sync/close/terminate)
        // pass through so a client can still authenticate and disconnect cleanly.
        const data_plane = switch (t) {
            .query, .parse, .bind, .describe, .execute => true,
            else => false,
        };
        if (data_plane and sess.executor.db.security_manager.require_auth and !sess.authenticated) {
            try sendOwned(writer, allocator, try wire.encodeError(allocator, "ERROR", "28000", "authentication required"));
            try sendOwned(writer, allocator, try wire.encodeReady(allocator, .idle));
            try writer.flush();
            continue;
        }

        switch (t) {
            .startup => {
                const su = wire.decodeStartup(frame.payload) catch {
                    try sendOwned(writer, allocator, try wire.encodeError(allocator, "FATAL", "08P01", "invalid startup packet"));
                    try writer.flush();
                    return;
                };
                const sec = sess.executor.db.security_manager;
                if (sec.enabled and sec.users.count() > 0) {
                    // TLS gate: never send a cleartext-password challenge (nor read
                    // a password) over a plaintext link when the operator requires
                    // TLS for auth. Refuse before any secret crosses the wire.
                    if (sec.require_tls_for_auth and !sess.secure) {
                        try sendOwned(writer, allocator, try wire.encodeError(allocator, "FATAL", "28000", "TLS required for password authentication"));
                        try writer.flush();
                        return;
                    }
                    try sendOwned(writer, allocator, try wire.encodeAuthCleartext(allocator));
                    try writer.flush();

                    const pmaybe = readFrame(sess, reader) catch return;
                    const pframe = pmaybe orelse return;
                    defer releaseFrame(sess, pframe);
                    if (@as(wire.Frontend, @enumFromInt(pframe.type)) != .auth_response) {
                        try sendOwned(writer, allocator, try wire.encodeError(allocator, "FATAL", "08P01", "expected password message"));
                        try writer.flush();
                        return;
                    }
                    const password = wire.decodeAuthResponse(pframe.payload) catch "";

                    const auth_session = sec.authenticate(su.user, password, null) catch {
                        try sendOwned(writer, allocator, try wire.encodeError(allocator, "FATAL", "28P01", "authentication failed"));
                        try writer.flush();
                        return;
                    };
                    sess.session_token = auth_session.token;
                    sess.authenticated = true;
                }
                try sendOwned(writer, allocator, try wire.encodeAuthOk(allocator));
                try sendOwned(writer, allocator, try wire.encodeParameterStatus(allocator, "server", "kaidb"));
                try sendOwned(writer, allocator, try wire.encodeReady(allocator, .idle));
                try writer.flush();
            },
            .query => {
                const sql = wire.decodeQuery(frame.payload) catch "";
                try runSql(sess, writer, sql);
                try sendOwned(writer, allocator, try wire.encodeReady(allocator, .idle));
                try writer.flush();
            },
            .parse => {
                try handleParse(sess, writer, frame.payload);
                try sendOwned(writer, allocator, try wire.encodeSimple(allocator, .parse_complete));
                try writer.flush();
            },
            .bind => {
                try handleBind(sess, writer, frame.payload);
                try sendOwned(writer, allocator, try wire.encodeSimple(allocator, .bind_complete));
                try writer.flush();
            },
            .execute => {
                const em = wire.decodeExecute(frame.payload) catch wire.ExecuteMsg{ .portal = "", .max_rows = 0 };
                if (sess.portals.getPtr(em.portal)) |portal| {
                    try streamPortal(sess, writer, portal, em.max_rows);
                } else {
                    try sendOwned(writer, allocator, try wire.encodeError(allocator, "ERROR", "34000", "no such portal"));
                }
                try writer.flush();
            },
            .describe => {
                try handleDescribe(sess, writer, frame.payload);
                try writer.flush();
            },
            .close => {
                try sendOwned(writer, allocator, try wire.encodeSimple(allocator, .close_complete));
                try writer.flush();
            },
            .sync => {
                try sendOwned(writer, allocator, try wire.encodeReady(allocator, .idle));
                try writer.flush();
            },
            .terminate => break,
            .doc_op => {
                try sendOwned(writer, allocator, try wire.encodeError(allocator, "ERROR", "0A000", "document operations are not supported: this server is relational (SQL) only"));
                try sendOwned(writer, allocator, try wire.encodeReady(allocator, .idle));
                try writer.flush();
            },
            else => {
                try sendOwned(writer, allocator, try wire.encodeError(allocator, "ERROR", "08P01", "unknown message type"));
                try writer.flush();
            },
        }
    }
}

/// Writes an owned, freshly encoded message and then frees it.
///
/// The `wire.encode*` helpers allocate their output; this pairs the write with a
/// `defer`-free so callers can pass `try wire.encode...(...)` inline without
/// leaking. The free happens even if the write errors.
fn sendOwned(writer: *Io.Writer, a: Allocator, owned: []u8) !void {
    defer a.free(owned);
    try writeMsg(writer, owned);
}

/// Handles a Parse message: registers a named prepared statement.
///
/// Decodes the statement name, SQL, and declared parameter OIDs. Each OID is
/// mapped to a [`command.ParamClass`] via [`classForOid`] so Bind knows how to
/// render each argument. The SQL and name are duped into session-owned copies; a
/// pre-existing statement of the same name is removed and freed first, so
/// re-preparing a name replaces it. A malformed Parse yields a `42601` error
/// response and returns without registering anything.
fn handleParse(sess: *Session, writer: *Io.Writer, payload: []const u8) !void {
    const a = sess.allocator;
    var parsed = wire.decodeParse(payload, a) catch {
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "42601", "invalid Parse message"));
        return;
    };
    defer parsed.deinit(a);

    const classes = try a.alloc(command.ParamClass, parsed.param_oids.len);
    errdefer a.free(classes);
    for (parsed.param_oids, 0..) |o, i| classes[i] = classForOid(o);

    const sql_copy = try a.dupe(u8, parsed.sql);
    errdefer a.free(sql_copy);
    const name_copy = try a.dupe(u8, parsed.stmt_name);

    if (sess.prepared.fetchRemove(name_copy)) |old| {
        a.free(old.key);
        a.free(old.value.sql);
        a.free(old.value.classes);
    }
    try sess.prepared.put(a, name_copy, .{ .sql = sql_copy, .classes = classes });
}

/// Handles a Bind message: substitutes parameters and opens a portal.
///
/// Looks up the referenced prepared statement (a `26000` error if absent), then
/// uses [`command.substituteParams`] to inline the supplied parameter values
/// into the statement's SQL according to its stored [`command.ParamClass`]es,
/// producing the portal's final SQL. A same-named existing portal is removed and
/// freed first. Bad parameter values yield `22P02`; a malformed Bind yields
/// `42601`. Unlike a real planning engine, binding here is pure text
/// substitution, so the portal holds a ready-to-execute SQL string.
fn handleBind(sess: *Session, writer: *Io.Writer, payload: []const u8) !void {
    const a = sess.allocator;
    var bind = wire.decodeBind(payload, a) catch {
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "42601", "invalid Bind message"));
        return;
    };
    defer bind.deinit(a);

    const prep = sess.prepared.get(bind.stmt) orelse {
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "26000", "no such prepared statement"));
        return;
    };

    const final_sql = command.substituteParams(a, prep.sql, bind.params, prep.classes) catch {
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "22P02", "invalid parameter value"));
        return;
    };
    errdefer a.free(final_sql);

    const pname = try a.dupe(u8, bind.portal);
    if (sess.portals.fetchRemove(pname)) |old| {
        a.free(old.key);
        a.free(old.value.sql);
    }
    try sess.portals.put(a, pname, .{ .sql = final_sql });
}

/// Executes one simple-query SQL string and streams its full result.
///
/// Runs the SQL through the executor (passing the hex session token for access
/// checks), then serialises the response: a `RowDescription` plus a `DataRow`
/// per row and a `SELECT n` command tag for result-bearing statements, or an
/// `OK n` tag reporting rows affected for non-result statements. A missing cell
/// (`i >= row.len`) is sent as SQL NULL. Executor errors and result-carried
/// error messages both become `XX000` error responses; the caller is expected
/// to follow with a `ReadyForQuery`. The response is deep-freed by
/// [`freeResponse`] on the way out.
/// Context for the streaming row sink: the live writer + allocator. Its address is
/// handed to the executor as `qe.RowSink.ctx` for the duration of one runSql call.
const StreamCtx = struct { writer: *Io.Writer, a: Allocator };

/// RowSink.begin: send the RowDescription once, before any streamed DataRow.
fn streamBegin(ctx: *anyopaque, names: []const []const u8, types: []const ColumnType, binary: bool) anyerror!void {
    const s: *StreamCtx = @alignCast(@ptrCast(ctx));
    const fields = try s.a.alloc(wire.FieldDesc, names.len);
    defer s.a.free(fields);
    for (names, 0..) |name, i| {
        const ct: ColumnType = if (i < types.len) types[i] else .TEXT;
        fields[i] = oidmap.fieldDesc(name, ct, @intCast(i));
        if (binary and oidmap.isBinaryType(ct)) fields[i].format = .binary;
    }
    try sendOwned(s.writer, s.a, try wire.encodeRowDescription(s.a, fields));
}

/// RowSink.row: encode and send one DataRow. Cells are non-null text/binary bytes
/// parallel to the columns; they are owned by the executor and freed after return.
fn streamRow(ctx: *anyopaque, cells: []const []const u8) anyerror!void {
    const s: *StreamCtx = @alignCast(@ptrCast(ctx));
    const vals = try s.a.alloc(?[]const u8, cells.len);
    defer s.a.free(vals);
    for (cells, 0..) |c, i| vals[i] = c;
    try sendOwned(s.writer, s.a, try wire.encodeDataRow(s.a, vals));
}

fn runSql(sess: *Session, writer: *Io.Writer, sql: []const u8) !void {
    const a = sess.allocator;
    var tokbuf: [64]u8 = undefined;
    // Stream SELECT rows to the client during the scan (overlaps the client's row
    // decode with the server scan) — the default; KAIDB_NOSTREAM forces the old
    // buffer-then-send path. The sink is attached only for the duration of this call
    // and used solely by the scan-order SELECT * fast path (a sorted/offset/distinct
    // query ignores it and still buffers); every other statement ignores it too.
    var sctx = StreamCtx{ .writer = writer, .a = a };
    if (std.c.getenv("KAIDB_NOSTREAM") == null) {
        sess.executor.row_sink = .{ .ctx = @ptrCast(&sctx), .begin = streamBegin, .row = streamRow };
    }
    defer sess.executor.row_sink = null;
    // Coarse per-query wire profiling, gated by env KAIDB_QEXEC. Splits the
    // server's time into execute() (planner + storage) versus encode+socket
    // send, so a client-observed round trip can be decomposed into
    // server-execute / server-encode+send / (network + client decode).
    const qwire = std.c.getenv("KAIDB_QEXEC") != null;
    const qio = sess.executor.db.pool.pager.io;
    var sw_exec = StopWatch{};
    if (qwire) sw_exec.start(qio);
    const resp = sess.executor.execute(.{ .sql = sql, .session_token = sess.tokenHex(&tokbuf) }) catch |err| {
        const msg = try std.fmt.allocPrint(a, "execution error: {s}", .{@errorName(err)});
        defer a.free(msg);
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "XX000", msg));
        return;
    };
    if (qwire) sw_exec.stop(qio);
    defer freeResponse(a, resp);

    if (resp.error_message) |m| {
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "XX000", m));
        return;
    }

    // Streamed result: the sink already sent RowDescription + every DataRow during
    // the scan, so only the final command tag remains.
    if (resp.streamed) {
        const tag = try std.fmt.allocPrint(a, "SELECT {d}", .{resp.rows_affected});
        defer a.free(tag);
        try sendOwned(writer, a, try wire.encodeCommandComplete(a, tag));
        return;
    }

    var sw_wire = StopWatch{};
    if (qwire) sw_wire.start(qio);
    if (resp.columns.len > 0) {
        const fields = try a.alloc(wire.FieldDesc, resp.columns.len);
        defer a.free(fields);
        for (resp.columns, 0..) |name, i| {
            const ct: ColumnType = if (i < resp.column_types.len) resp.column_types[i] else .TEXT;
            fields[i] = oidmap.fieldDesc(name, ct, @intCast(i));
            if (resp.result_binary and oidmap.isBinaryType(ct)) fields[i].format = .binary;
        }
        try sendOwned(writer, a, try wire.encodeRowDescription(a, fields));

        const vals = try a.alloc(?[]const u8, resp.columns.len);
        defer a.free(vals);
        for (resp.rows) |row| {
            for (0..resp.columns.len) |i| {
                vals[i] = if (i < row.len) row[i] else null;
            }
            try sendOwned(writer, a, try wire.encodeDataRow(a, vals));
        }
        const tag = try std.fmt.allocPrint(a, "SELECT {d}", .{resp.rows.len});
        defer a.free(tag);
        try sendOwned(writer, a, try wire.encodeCommandComplete(a, tag));
        if (qwire) {
            sw_wire.stop(qio);
            const to_ms = struct {
                fn f(sw: StopWatch) f64 {
                    return @as(f64, @floatFromInt(sw.elapsedNs())) / 1_000_000.0;
                }
            };
            const n = @min(sql.len, 52);
            std.debug.print("[QWIRE] exec={d:.2}ms encode+send={d:.2}ms rows={d} binary={} sql=\"{s}\"\n", .{
                to_ms.f(sw_exec), to_ms.f(sw_wire), resp.rows.len, resp.result_binary, sql[0..n],
            });
        }
    } else {
        if (qwire) sw_wire.start(qio);
        const tag = try std.fmt.allocPrint(a, "OK {d}", .{resp.rows_affected});
        defer a.free(tag);
        try sendOwned(writer, a, try wire.encodeCommandComplete(a, tag));
        if (qwire) {
            sw_wire.stop(qio);
            const to_ms = struct {
                fn f(sw: StopWatch) f64 {
                    return @as(f64, @floatFromInt(sw.elapsedNs())) / 1_000_000.0;
                }
            };
            const n = @min(sql.len, 40);
            std.debug.print("[QDML] exec={d:.2}ms reply={d:.2}ms affected={d} sql=\"{s}\"\n", .{
                to_ms.f(sw_exec), to_ms.f(sw_wire), resp.rows_affected, sql[0..n],
            });
        }
    }
}

/// Executes a portal's SQL once and caches the result on the portal.
///
/// Idempotent: returns immediately if [`Portal.result`] is already populated, so
/// a Describe followed by one or more Executes only runs the query a single
/// time. An execution error is *not* raised to the caller here; it is captured
/// as the portal result's `error_message` so that [`streamPortal`] and
/// [`handleDescribe`] can report it as a normal error response. The message
/// string is allocated from the session allocator and later freed by
/// [`freeResponse`].
fn ensureExecuted(sess: *Session, portal: *Portal) !void {
    if (portal.result != null) return;
    var tokbuf: [64]u8 = undefined;
    const resp = sess.executor.execute(.{ .sql = portal.sql, .session_token = sess.tokenHex(&tokbuf) }) catch |err| {
        const msg = try std.fmt.allocPrint(sess.allocator, "execution error: {s}", .{@errorName(err)});
        portal.result = .{ .error_message = msg };
        return;
    };
    portal.result = resp;
}

/// Builds the `RowDescription` field array for a query response.
///
/// Maps each column name and [`ColumnType`] to a [`wire.FieldDesc`] via
/// [`oidmap.fieldDesc`], defaulting a column whose type is missing from
/// `column_types` to `TEXT`. The returned slice is caller-owned and must be
/// freed by the caller. Shared by [`streamPortal`] and [`handleDescribe`].
fn portalFields(sess: *Session, resp: qe.QueryResponse) ![]wire.FieldDesc {
    const a = sess.allocator;
    const fields = try a.alloc(wire.FieldDesc, resp.columns.len);
    for (resp.columns, 0..) |name, i| {
        const ct: ColumnType = if (i < resp.column_types.len) resp.column_types[i] else .TEXT;
        fields[i] = oidmap.fieldDesc(name, ct, @intCast(i));
        if (resp.result_binary and oidmap.isBinaryType(ct)) fields[i].format = .binary;
    }
    return fields;
}

/// Streams up to `max_rows` rows from a portal, resuming across Executes.
///
/// Forces execution ([`ensureExecuted`]), reports a captured error as `XX000`,
/// and for a non-result statement emits an `OK n` command tag. For a result set
/// it sends the `RowDescription` once (guarded by [`Portal.described`], since a
/// prior Describe may have sent it), then `DataRow`s from [`Portal.cursor`] up
/// to `cursor + max_rows` (or all remaining rows when `max_rows == 0`),
/// advancing the cursor as it goes. When the cursor reaches the end it sends a
/// `SELECT total` command tag; otherwise it sends `PortalSuspended`, and a later
/// Execute resumes from the same cursor. Missing cells are sent as SQL NULL.
fn streamPortal(sess: *Session, writer: *Io.Writer, portal: *Portal, max_rows: u32) !void {
    const a = sess.allocator;
    try ensureExecuted(sess, portal);
    const resp = portal.result.?;

    if (resp.error_message) |m| {
        try sendOwned(writer, a, try wire.encodeError(a, "ERROR", "XX000", m));
        return;
    }

    if (resp.columns.len == 0) {
        const tag = try std.fmt.allocPrint(a, "OK {d}", .{resp.rows_affected});
        defer a.free(tag);
        try sendOwned(writer, a, try wire.encodeCommandComplete(a, tag));
        return;
    }

    if (!portal.described) {
        const fields = try portalFields(sess, resp);
        defer a.free(fields);
        try sendOwned(writer, a, try wire.encodeRowDescription(a, fields));
        portal.described = true;
    }

    const total = resp.rows.len;
    const limit: usize = if (max_rows == 0) total else @min(portal.cursor + max_rows, total);
    const vals = try a.alloc(?[]const u8, resp.columns.len);
    defer a.free(vals);
    while (portal.cursor < limit) : (portal.cursor += 1) {
        const row = resp.rows[portal.cursor];
        for (0..resp.columns.len) |i| vals[i] = if (i < row.len) row[i] else null;
        try sendOwned(writer, a, try wire.encodeDataRow(a, vals));
    }

    if (portal.cursor >= total) {
        const tag = try std.fmt.allocPrint(a, "SELECT {d}", .{total});
        defer a.free(tag);
        try sendOwned(writer, a, try wire.encodeCommandComplete(a, tag));
    } else {
        try sendOwned(writer, a, try wire.encodeSimple(a, .portal_suspended));
    }
}

/// Handles a Describe message for a portal, replying with its row shape.
///
/// Only the portal form (`'P'`) is described here: it reads the portal name,
/// forces execution to learn the result shape, and if the portal produced a
/// result set with no error it sends the `RowDescription` and marks the portal
/// `described` so [`streamPortal`] will not resend it. In every other case (a
/// statement Describe `'S'`, an unknown portal, an errored or non-result query)
/// it replies `NoData`. Statement-form parameter description is not emitted.
fn handleDescribe(sess: *Session, writer: *Io.Writer, payload: []const u8) !void {
    const a = sess.allocator;
    if (payload.len >= 1 and payload[0] == 'P') {
        var c = wire.Cursor{ .data = payload[1..] };
        const name = c.str16() catch "";
        if (sess.portals.getPtr(name)) |portal| {
            try ensureExecuted(sess, portal);
            const resp = portal.result.?;
            if (resp.error_message == null and resp.columns.len > 0) {
                const fields = try portalFields(sess, resp);
                defer a.free(fields);
                try sendOwned(writer, a, try wire.encodeRowDescription(a, fields));
                portal.described = true;
                return;
            }
        }
    }
    try sendOwned(writer, a, try wire.encodeSimple(a, .no_data));
}

/// Deep-frees every allocation owned by a [`qe.QueryResponse`].
///
/// Releases the optional error message, each column-name string and the column
/// array, each cell and each row array, and the outer rows array. The
/// `len > 0` guards mirror the executor, which leaves the slice pointers dangling
/// (not freeable) when a dimension is empty. Called wherever a response goes out
/// of scope: [`runSql`], [`Session.clearState`], and portal teardown.
fn freeResponse(a: Allocator, resp: qe.QueryResponse) void {
    if (resp.error_message) |m| a.free(m);
    for (resp.columns) |c| a.free(c);
    if (resp.columns.len > 0) a.free(resp.columns);
    for (resp.rows) |row| {
        for (row) |cell| a.free(cell);
        a.free(row);
    }
    if (resp.rows.len > 0) a.free(resp.rows);
}

/// Classifies a parameter OID into the substitution class Bind should use.
///
/// Text and blob OIDs render as quoted text, the boolean OID as a boolean
/// literal, and everything else as a bare numeric literal. Used by
/// [`handleParse`] to precompute a [`Prepared.classes`] entry per placeholder so
/// [`command.substituteParams`] can format each argument correctly at Bind time.
fn classForOid(o: wire.Oid) command.ParamClass {
    return switch (o) {
        wire.oid.text, wire.oid.blob => .text,
        wire.oid.bool_v => .boolean,
        else => .numeric,
    };
}
