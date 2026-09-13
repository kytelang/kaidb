//! `schnell`, a small, self-contained HTTP/1.1 server and client for NovaDB.
//!
//! "Schnell" is German for "fast", and this module is deliberately tiny: it is
//! the HTTP transport NovaDB uses for its non-wire-protocol surface (health
//! checks, the admin/observability endpoints, and outbound HTTP the engine
//! needs to make) without pulling in a full web framework. The parser is a
//! blocking, byte-at-a-time HTTP/1.1 implementation, sufficient for small
//! request/response bodies rather than large streaming transfers.
//!
//! It is built directly on Zig's new async I/O ([`std.Io`]) instead of on
//! blocking sockets, so the same source runs on whatever reactor the host has
//! wired up. Everything takes an `Io` handle explicitly rather than reaching for
//! a global, which keeps the module testable and lets the caller own the event
//! loop.
//!
//! The two halves are independent:
//!
//!   1. [`Server`]: a one-goroutine-per-connection accept loop. It exposes a
//!      single "raw handler" seam ([`Server.setRawHandler`]) that receives the
//!      full request bytes and returns the full response bytes, so the routing
//!      and body-shaping policy lives in the caller, not here. The server only
//!      does the framing: read headers up to `\r\n\r\n`, honour
//!      `Content-Length` for the body, hand the bytes off, write the reply.
//!
//!   2. [`Client`]: a blocking request/response HTTP client with TLS support via
//!      the `tls` module. It resolves the host itself ([`resolveHost`]), speaks
//!      HTTP/1.1 with `Connection: close`, and understands both
//!      `Content-Length` and `Transfer-Encoding: chunked` response bodies.
//!
//! Supporting types cover the incidental HTTP concerns the engine needs:
//! [`Cookie`] formatting, [`Csrf`] token generation/constant-time comparison,
//! and a token-bucket [`RateLimiter`]. The system-root-CA bundle is cached
//! process-wide behind a mutex ([`getSystemRootCa`]) with a 24h TTL, because
//! loading it from the OS trust store is expensive and rarely changes.
//!
//! Design notes worth knowing:
//!   - The server framing caps the header section at 8192 bytes (it bails out
//!     rather than growing unbounded) and reads exactly `Content-Length` body
//!     bytes; there is no keep-alive on the server path.
//!   - Slices returned in parsed structures ([`Cookie`], [`ParsedUrl`],
//!     [`ClientResponse.Header`]) borrow the caller's / response buffer's
//!     memory; only [`ClientResponse`] owns heap and must be `deinit`'d.

const std = @import("std");
/// Alias for Zig's async I/O namespace; every socket, clock, and mutex here
/// flows through an `Io` handle so the module is reactor-agnostic.
const Io = std.Io;
/// Networking primitives (addresses, streams, listeners) layered on [`Io`].
const net = std.Io.net;
/// TLS implementation used by [`Client`] for `https://` requests and by
/// [`getSystemRootCa`] to load the OS trust store.
const tls = @import("tls");
/// Debug-only invariant check; compiled out in release builds.
const assert = std.debug.assert;
/// Shorthand for the standard allocator interface.
const Allocator = std.mem.Allocator;

/// A minimal HTTP/1.1 server with a single "raw bytes in, raw bytes out" seam.
///
/// The server owns no routing logic: it frames the request (headers to
/// `\r\n\r\n`, then a `Content-Length` body), passes the concatenated bytes to
/// the caller-supplied [`Server.raw_handler`], and writes back whatever the
/// handler returns. Each accepted connection is handled on its own async task in
/// [`Server.group`], so slow handlers do not block the accept loop. There is no
/// keep-alive: one request/response per connection, then close.
pub const Server = struct {
    /// Allocator used for per-request scratch (the request buffer and body).
    allocator: std.mem.Allocator,
    /// Listener configuration (bind address, limits, TLS paths); see
    /// [`ServerConfig`].
    cfg: ServerConfig,
    /// Application callback: `(allocator, request_bytes, ctx) -> response_bytes`.
    /// Null until [`Server.setRawHandler`] installs one; if it stays null the
    /// server reads the request and closes without replying. The returned slice
    /// is freed by the server with `allocator` after it is written.
    raw_handler: ?*const fn (std.mem.Allocator, []const u8, ?*anyopaque) anyerror![]const u8 = null,
    /// Opaque context pointer passed unchanged to [`Server.raw_handler`], so the
    /// handler can recover its owning object without a global.
    raw_handler_ctx: ?*anyopaque = null,
    /// Accept-loop run flag. Set true in [`Server.listen`], flipped false by
    /// [`Server.stop`]; loaded with `.seq_cst` so a stop from another task is
    /// observed promptly.
    running: std.atomic.Value(bool) = .init(false),
    /// The bound listening socket, populated once [`Server.listen`] succeeds so
    /// [`Server.stop`] can close it and unblock `accept`.
    listen_server: ?net.Server = null,
    /// Async task group tracking every in-flight connection handler, so
    /// [`Server.stop`] can cancel them all at once.
    group: Io.Group = .init,

    /// Constructs a server bound to `cfg` but does not open any socket yet.
    ///
    /// Call [`Server.listen`] to actually bind and serve. Returns an error type
    /// for forward-compatibility; the current body cannot fail.
    pub fn init(allocator: std.mem.Allocator, cfg: ServerConfig) !Server {
        return .{
            .allocator = allocator,
            .cfg = cfg,
        };
    }

    /// Tears down the server. Currently a no-op: the server holds no owned heap;
    /// the listening socket is closed by [`Server.stop`] and connection tasks by
    /// [`Server.group`]. Present so callers can `defer server.deinit()` safely.
    pub fn deinit(self: *Server) void {
        _ = self;
    }

    /// Installs the application request handler and its context pointer.
    ///
    /// Must be called before [`Server.listen`] for the server to produce
    /// replies; without it every connection is read and dropped. `ctx` is handed
    /// back to `handler` verbatim on each request.
    pub fn setRawHandler(self: *Server, handler: *const fn (std.mem.Allocator, []const u8, ?*anyopaque) anyerror![]const u8, ctx: ?*anyopaque) void {
        self.raw_handler = handler;
        self.raw_handler_ctx = ctx;
    }

    /// Binds the socket and runs the accept loop until [`Server.stop`] is called.
    ///
    /// Parses `cfg.host:cfg.port`, listens with `reuse_address`, then loops:
    /// each accepted stream is dispatched to [`handleConnection`] on
    /// [`Server.group`] so the loop keeps accepting. A single `accept` failure is
    /// logged and retried; `error.Canceled` (from [`Server.stop`] closing the
    /// socket) breaks the loop cleanly. Blocks the calling task for the server's
    /// whole lifetime.
    pub fn listen(self: *Server, io: Io) !void {
        const addr = try net.IpAddress.parse(self.cfg.host, self.cfg.port);
        var s = try addr.listen(io, .{ .reuse_address = true });
        self.listen_server = s;
        self.running.store(true, .seq_cst);

        while (self.running.load(.seq_cst)) {
            const stream = s.accept(io) catch |err| {
                std.debug.print("SERVER: accept error: {}\n", .{err});
                if (err == error.Canceled) break;
                continue;
            };
            std.debug.print("SERVER: Accepted connection!\n", .{});
            self.group.async(io, handleConnection, .{ self, io, stream });
        }
    }

    /// Signals the accept loop to exit and cancels all in-flight connections.
    ///
    /// Clears [`Server.running`], closes the listening socket (which surfaces as
    /// `error.Canceled` inside [`Server.listen`]'s `accept`), and cancels every
    /// task in [`Server.group`]. Safe to call from a task other than the one
    /// running `listen`.
    pub fn stop(self: *Server, io: Io) void {
        self.running.store(false, .seq_cst);
        if (self.listen_server) |*s| {
            s.socket.close(io);
        }
        self.group.cancel(io);
    }
};

/// Per-connection task entry point: ensures the stream is closed and funnels
/// errors from [`handleConnectionInner`] into logs.
///
/// Runs on [`Server.group`]. It closes `stream` on exit no matter what, and
/// propagates `error.Canceled` (so [`Server.group.cancel`] tears the task down)
/// while swallowing all other handler errors into a scoped log line, so one bad
/// connection never crashes the server.
fn handleConnection(server: *Server, io: Io, stream: net.Stream) Io.Cancelable!void {
    defer stream.close(io);
    std.debug.print("SERVER: Inside handleConnection!\n", .{});
    handleConnectionInner(server, io, stream) catch |err| {
        std.debug.print("SERVER: handleConnection error: {}\n", .{err});
        if (err == error.Canceled) return error.Canceled;
        std.log.scoped(.http).err("Connection handler error: {}", .{err});
    };
}

/// Reads one HTTP/1.1 request off `stream`, invokes the handler, writes the reply.
///
/// The framing is deliberate and worth understanding:
///   - Headers are read one byte at a time until the `\r\n\r\n` terminator is
///     seen, with a hard 8192-byte cap (it silently returns if exceeded, to
///     bound memory against a client that never sends the blank line).
///   - It scans the header lines only for `Content-Length`; if present and
///     non-zero, exactly that many body bytes are read with `readSliceAll` and
///     appended, so the handler receives headers+body contiguously.
///   - If [`Server.raw_handler`] is set, its response bytes are written and
///     freed. A handler error is reported to the client as a bare
///     `500 Internal Server Error` (best-effort; write failures are ignored)
///     and then re-raised so [`handleConnection`] logs it.
///
/// Note the writer reuses `read_buf` as its scratch buffer, which is safe here
/// because the request has already been fully consumed before any reply is
/// written.
fn handleConnectionInner(server: *Server, io: Io, stream: net.Stream) !void {
    // Read the request headers + body
    var read_buf: [4096]u8 = undefined;
    var r = stream.reader(io, &read_buf);

    var request_list = std.ArrayList(u8).empty;
    defer request_list.deinit(server.allocator);

    while (true) {
        const byte = (&r.interface).takeByte() catch |err| {
            std.debug.print("SERVER: takeByte err: {}\n", .{err});
            if (err == error.EndOfStream) break;
            return err;
        };
        std.debug.print("{c}", .{byte});
        try request_list.append(server.allocator, byte);
        if (std.mem.endsWith(u8, request_list.items, "\r\n\r\n")) {
            break;
        }
        if (request_list.items.len >= 8192) {
            return;
        }
    }

    // Parse Content-Length header to read body
    var content_len: usize = 0;
    var lines_it = std.mem.tokenizeSequence(u8, request_list.items, "\r\n");
    while (lines_it.next()) |line| {
        if (std.mem.startsWith(u8, line, "Content-Length: ")) {
            const len_str = line["Content-Length: ".len..];
            content_len = std.fmt.parseInt(usize, len_str, 10) catch 0;
        }
    }

    if (content_len > 0) {
        std.debug.print("SERVER: Reading body of length {d}...\n", .{content_len});
        const body_buf = try server.allocator.alloc(u8, content_len);
        defer server.allocator.free(body_buf);

        try (&r.interface).readSliceAll(body_buf);
        std.debug.print("SERVER: Body read successfully: '{s}'\n", .{body_buf});
        try request_list.appendSlice(server.allocator, body_buf);
    }

    // Call the handler
    if (server.raw_handler) |handler| {
        std.debug.print("SERVER: Calling raw handler...\n", .{});
        const response_bytes = handler(server.allocator, request_list.items, server.raw_handler_ctx) catch |err| {
            std.debug.print("SERVER: Handler returned error: {}\n", .{err});
            const err_msg = "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\n\r\n";
            var w = stream.writer(io, &read_buf);
            _ = (&w.interface).write(err_msg) catch {};
            _ = (&w.interface).flush() catch {};
            return err;
        };
        defer server.allocator.free(response_bytes);

        std.debug.print("SERVER: Handler returned response! Len={d}\n", .{response_bytes.len});
        var w = stream.writer(io, &read_buf);
        try (&w.interface).writeAll(response_bytes);
        try (&w.interface).flush();
        std.debug.print("SERVER: Response written successfully!\n", .{});
    }
}

/// An HTTP `Set-Cookie` attribute set, with a formatter that renders the header
/// value.
///
/// All string fields borrow the caller's memory; a `Cookie` copies nothing and
/// is only valid while those slices live. Optional fields are omitted from the
/// output when null/false, matching how a `Set-Cookie` header is normally
/// written.
pub const Cookie = struct {
    /// Cookie name (the part before `=`).
    name: []const u8,
    /// Cookie value (the part after `=`); not URL-encoded by this type.
    value: []const u8,
    /// `Path` attribute; defaults to `/` so the cookie applies site-wide.
    path: []const u8 = "/",
    /// Optional `Domain` attribute; null omits it (host-only cookie).
    domain: ?[]const u8 = null,
    /// Optional `Max-Age` in seconds; null omits it (session cookie).
    max_age: ?i32 = null,
    /// When true, appends `; Secure` (send only over HTTPS).
    secure: bool = false,
    /// When true, appends `; HttpOnly` (hide from `document.cookie`).
    http_only: bool = false,

    /// Serialises the cookie to its `Set-Cookie` header-value form.
    ///
    /// Always emits `name=value; Path=path`, then appends `Domain`, `Max-Age`,
    /// `Secure`, and `HttpOnly` only for the fields that are set. Intended to be
    /// used with Zig's formatting machinery via any `writer`.
    pub fn format(self: Cookie, writer: anytype) !void {
        try writer.print("{s}={s}; Path={s}", .{ self.name, self.value, self.path });
        if (self.domain) |d| try writer.print("; Domain={s}", .{d});
        if (self.max_age) |ma| try writer.print("; Max-Age={d}", .{ma});
        if (self.secure) try writer.writeAll("; Secure");
        if (self.http_only) try writer.writeAll("; HttpOnly");
    }
};

/// CSRF token helpers: generation and constant-time comparison.
///
/// A namespace (no fields), grouping the two operations a CSRF defence needs.
/// Generation uses a cryptographic RNG and validation is timing-safe, so the
/// token cannot be leaked bit-by-bit by measuring comparison time.
pub const Csrf = struct {
    /// Generates a fresh CSRF token: 32 random bytes as a 64-char lowercase hex
    /// string.
    ///
    /// Draws from `std.crypto.random` (a CSPRNG). The returned slice is owned by
    /// `allocator` and must be freed by the caller. 256 bits of entropy makes
    /// the token infeasible to guess.
    pub fn generateToken(allocator: std.mem.Allocator) ![]const u8 {
        var bytes: [32]u8 = undefined;
        std.crypto.random.bytes(&bytes);
        const hex = std.fmt.bytesToHex(bytes, .lower);
        return try allocator.dupe(u8, &hex);
    }

    /// Compares two tokens for equality in constant time.
    ///
    /// Returns false immediately on a length mismatch (lengths are not secret),
    /// then uses `timingSafeEql` so the comparison time does not depend on where
    /// the first differing byte is, closing a timing side channel that a naive
    /// `mem.eql` would open.
    pub fn validateToken(token1: []const u8, token2: []const u8) bool {
        if (token1.len != token2.len) return false;
        return std.crypto.utils.timingSafeEql(u8, token1, token2);
    }
};

/// A token-bucket rate limiter driven by wall-clock time.
///
/// The bucket holds up to [`RateLimiter.capacity`] tokens and refills
/// continuously at [`RateLimiter.refill_rate`] tokens per millisecond. Each
/// permitted event consumes one token. Refill is lazy (computed from elapsed
/// time on each [`RateLimiter.allow`] call) rather than on a timer, so the
/// limiter costs nothing when idle. Not internally synchronised: guard it
/// externally if shared across tasks.
pub const RateLimiter = struct {
    /// Maximum number of tokens the bucket can hold (the burst ceiling).
    capacity: usize,
    /// Current token count, fractional so partial refills accumulate across
    /// calls instead of being lost to rounding.
    tokens: f64,
    /// Refill rate in tokens per millisecond (the per-second rate passed to
    /// [`RateLimiter.init`] divided by 1000).
    refill_rate: f64, // tokens per millisecond
    /// Timestamp (ms) of the last refill computation; the delta from now drives
    /// how many tokens to add on the next [`RateLimiter.allow`].
    last_refill_time: i64,
    /// I/O handle used to read the real-time clock for elapsed-time refills.
    io: Io,

    /// Creates a full bucket (`tokens == capacity`) refilling at
    /// `refill_rate_per_sec`.
    ///
    /// The per-second rate is converted to per-millisecond internally to match
    /// the millisecond clock used in [`RateLimiter.allow`]. Starting full lets an
    /// initial burst up to `capacity` through immediately.
    pub fn init(io: Io, capacity: usize, refill_rate_per_sec: f64) RateLimiter {
        return .{
            .capacity = capacity,
            .tokens = @floatFromInt(capacity),
            .refill_rate = refill_rate_per_sec / 1000.0,
            .last_refill_time = Io.Clock.now(.real, io).toMilliseconds(),
            .io = io,
        };
    }

    /// Attempts to consume one token; returns whether the event is allowed.
    ///
    /// First refills the bucket by `elapsed_ms * refill_rate`, capped at
    /// `capacity`, then consumes one token if at least one is available. Returns
    /// true (and decrements) when allowed, false when the bucket is empty.
    /// Advances `last_refill_time` to now on every call.
    pub fn allow(self: *RateLimiter) bool {
        const now = Io.Clock.now(.real, self.io).toMilliseconds();
        const elapsed = @as(f64, @floatFromInt(now - self.last_refill_time));
        self.last_refill_time = now;

        self.tokens = @min(@as(f64, @floatFromInt(self.capacity)), self.tokens + elapsed * self.refill_rate);

        if (self.tokens >= 1.0) {
            self.tokens -= 1.0;
            return true;
        }
        return false;
    }
};

/// Configuration for a [`Server`]; every field has a sensible default so
/// `.{}` gives a working local server.
pub const ServerConfig = struct {
    /// Bind address; defaults to loopback so a misconfigured server is not
    /// reachable off-box.
    host: []const u8 = "127.0.0.1",
    /// TCP port to listen on.
    port: u16 = 3008,
    /// Advisory cap on concurrent connections (not currently enforced by the
    /// accept loop; reserved for callers/limiters).
    max_connections: u32 = 1024,
    /// Advisory maximum request body size in bytes (1 MiB default).
    max_body_size: u32 = 1024 * 1024,
    /// Advisory idle timeout in milliseconds before a connection is dropped.
    idle_timeout_ms: u32 = 60000,
    /// Path to a TLS certificate (PEM); empty means plaintext HTTP.
    tls_cert_file: []const u8 = "",
    /// Path to the matching TLS private key (PEM); empty means plaintext HTTP.
    tls_key_file: []const u8 = "",
    /// Optional directory to serve static files from; null disables static
    /// serving.
    static_dir: ?[]const u8 = null,
};

/// Scoped logger for the [`Client`] path (DNS, connection, response errors).
const log = std.log.scoped(.client);

/// A fully-read HTTP response returned by [`Client`].
///
/// This is the one type in the module that OWNS heap: [`ClientResponse.headers`]
/// and [`ClientResponse.body`] are allocated with [`ClientResponse.allocator`]
/// and must be released by [`ClientResponse.deinit`]. Individual header
/// name/value slices point into the response's own buffers.
pub const ClientResponse = struct {
    /// HTTP status code parsed from the response line (e.g. 200, 404).
    status: u16,
    /// Parsed response headers, owned; freed by [`ClientResponse.deinit`].
    headers: []Header,
    /// Response body bytes (dechunked if the response was chunked); owned.
    body: []const u8,
    /// Allocator that owns [`ClientResponse.headers`] and
    /// [`ClientResponse.body`].
    allocator: Allocator,

    /// A single `Name: Value` response header.
    pub const Header = struct {
        /// Header field name, as received (case is preserved).
        name: []const u8,
        /// Header field value, trimmed of surrounding spaces.
        value: []const u8,
    };

    /// Case-insensitively looks up a header value by name.
    ///
    /// Returns the first matching value, or null if absent. Comparison ignores
    /// ASCII case, per HTTP header semantics.
    pub fn header(self: *const ClientResponse, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }

    /// Frees the owned header and body allocations.
    ///
    /// Guards each free on a non-empty length so it is safe even when the body
    /// or header slice was never allocated. Must be called exactly once per
    /// response.
    pub fn deinit(self: *ClientResponse) void {
        if (self.headers.len > 0) self.allocator.free(self.headers);
        if (self.body.len > 0) self.allocator.free(self.body);
    }
};

/// HTTP request methods supported by [`Client`].
pub const RequestMethod = enum {
    /// HTTP GET.
    get,
    /// HTTP POST.
    post,
    /// HTTP PUT.
    put,
    /// HTTP DELETE.
    delete,
    /// HTTP PATCH.
    patch,
    /// HTTP HEAD.
    head,
    /// HTTP OPTIONS.
    options,
};

/// Options for a single [`Client.request`] call.
///
/// Only [`RequestOptions.url`] is required; the rest default to a simple GET.
/// All slices borrow the caller's memory for the duration of the call.
pub const RequestOptions = struct {
    /// Request method; defaults to GET.
    method: RequestMethod = .get,
    /// Absolute URL (`http://` or `https://`); parsed by [`parseUrl`].
    url: []const u8,
    /// Extra request headers as `(name, value)` tuples, emitted verbatim.
    headers: []const struct { []const u8, []const u8 } = &.{},
    /// Optional request body; when present, a `Content-Length` header is added.
    body: ?[]const u8 = null,
    /// Request timeout in milliseconds (currently advisory; not enforced on the
    /// blocking path).
    timeout_ms: u32 = 10_000,
    /// When true, skips TLS certificate verification (an empty CA bundle and
    /// `insecure_skip_verify`). Use only for testing.
    insecure: bool = false,
    /// Optional value for an `X-Request-Id` header, for request tracing; empty
    /// or null omits it.
    request_id: ?[]const u8 = null,
};

/// The pieces of a URL that [`Client`] needs, extracted by [`parseUrl`].
///
/// `host` and `path` borrow slices of the original URL string, so a `ParsedUrl`
/// is only valid while that string lives.
pub const ParsedUrl = struct {
    /// True for `https://`, false for `http://`.
    is_https: bool,
    /// Host portion (no scheme, no port).
    host: []const u8,
    /// Port, either explicit or the scheme default (443 for https, 80 for http).
    port: u16,
    /// Request path including the leading `/` (defaults to `/` when absent).
    path: []const u8,
};

/// Process-wide cached system-root-CA bundle; null until first loaded by
/// [`getSystemRootCa`].
var system_ca_cache: ?tls.config.cert.Bundle = null;
/// Wall-clock time (ms) the cached bundle was loaded, used to expire it against
/// [`system_ca_ttl_ms`].
var system_ca_cache_loaded_ms: i64 = 0;
/// Guards [`system_ca_cache`] and [`system_ca_cache_loaded_ms`] against
/// concurrent first-load from multiple tasks.
var system_ca_cache_mutex: Io.Mutex = .init;
/// Time-to-live for the cached CA bundle: 24 hours, after which it is reloaded.
const system_ca_ttl_ms: i64 = 24 * 60 * 60 * 1000;

/// Returns the OS trust-store CA bundle, loading and caching it on first use.
///
/// Loading from the system trust store is expensive, so the bundle is cached
/// process-wide for [`system_ca_ttl_ms`] behind [`system_ca_cache_mutex`]. A
/// fresh cached bundle is returned directly; otherwise it is reloaded via
/// `tls.config.cert.fromSystem` and the cache refreshed. The cache uses the
/// long-lived `smp_allocator`, matching the bundle's process lifetime.
pub fn getSystemRootCa(io: Io) !tls.config.cert.Bundle {
    try system_ca_cache_mutex.lock(io);
    defer system_ca_cache_mutex.unlock(io);

    const now = Io.Clock.now(.real, io).toMilliseconds();
    if (system_ca_cache) |b| {
        if (now - system_ca_cache_loaded_ms < system_ca_ttl_ms) return b;
    }
    const bundle = try tls.config.cert.fromSystem(std.heap.smp_allocator, io);
    system_ca_cache = bundle;
    system_ca_cache_loaded_ms = now;
    return bundle;
}

/// Resolves `host:port` to an [`net.IpAddress`], accepting a literal IP or a name.
///
/// Fast path: if `host` parses as an IPv4 literal it is returned without any DNS.
/// Otherwise it performs an async DNS lookup and returns the first `.address`
/// result (canonical-name entries are skipped). Returns `error.InvalidHost` for
/// a malformed name and `error.HostResolutionFailed` when the lookup fails,
/// is cancelled, or yields no addresses.
pub fn resolveHost(io: Io, host: []const u8, port: u16) !net.IpAddress {
    if (net.IpAddress.parseIp4(host, port)) |addr| {
        return addr;
    } else |_| {}

    const HostName = net.HostName;
    const hn = HostName.init(host) catch return error.InvalidHost;

    var backing: [16]HostName.LookupResult = undefined;
    var resolved: Io.Queue(HostName.LookupResult) = .init(&backing);
    var name_buf: [HostName.max_len]u8 = undefined;

    log.debug("dns lookup start: {s}:{d}", .{ host, port });
    hn.lookup(io, &resolved, .{
        .port = port,
        .canonical_name_buffer = &name_buf,
    }) catch |err| {
        log.err("DNS lookup failed for {s}: {}", .{ host, err });
        return error.HostResolutionFailed;
    };

    while (resolved.getOne(io)) |result| {
        switch (result) {
            .address => |addr| {
                log.debug("dns resolved: {s} -> {f}", .{ host, addr });
                return addr;
            },
            .canonical_name => continue,
        }
    } else |err| switch (err) {
        error.Closed => {
            log.err("DNS lookup for {s}: no addresses returned", .{host});
            return error.HostResolutionFailed;
        },
        error.Canceled => return error.HostResolutionFailed,
    }
    return error.HostResolutionFailed;
}

/// Parses an absolute HTTP(S) URL into a [`ParsedUrl`].
///
/// Requires a `http://` or `https://` scheme (returns `error.InvalidUrl`
/// otherwise), splits off the path at the first `/` (defaulting to `/`), and
/// reads an optional `:port`, falling back to 443/80 by scheme. A non-numeric
/// port yields `error.InvalidUrl`. The returned host and path borrow slices of
/// `url`.
pub fn parseUrl(url: []const u8) !ParsedUrl {
    var rest = url;
    var is_https = false;

    if (std.mem.startsWith(u8, rest, "https://")) {
        is_https = true;
        rest = rest[8..];
    } else if (std.mem.startsWith(u8, rest, "http://")) {
        rest = rest[7..];
    } else {
        return error.InvalidUrl;
    }

    const path_start = std.mem.indexOfScalar(u8, rest, '/') orelse rest.len;
    const host_port = rest[0..path_start];
    const path = if (path_start < rest.len) rest[path_start..] else "/";

    var host: []const u8 = host_port;
    var port: u16 = if (is_https) 443 else 80;

    if (std.mem.indexOfScalar(u8, host_port, ':')) |colon| {
        host = host_port[0..colon];
        port = std.fmt.parseInt(u16, host_port[colon + 1 ..], 10) catch return error.InvalidUrl;
    }

    return .{ .is_https = is_https, .host = host, .port = port, .path = path };
}

/// A stateless HTTP/1.1 client namespace (no instance state; all methods static).
///
/// Each [`Client.request`] opens a fresh TCP (and, for `https`, TLS) connection,
/// sends the request with `Connection: close`, reads the whole response into a
/// [`ClientResponse`], and closes. There is no connection pooling or keep-alive;
/// this is for the occasional request the engine makes, not a high-throughput
/// path.
pub const Client = struct {
    /// Convenience wrapper: performs a GET of `url` with default options.
    ///
    /// Equivalent to [`Client.request`] with just the URL set. The returned
    /// [`ClientResponse`] must be `deinit`'d by the caller.
    pub fn get(allocator: Allocator, io: Io, url: []const u8) !ClientResponse {
        return request(allocator, io, .{ .url = url });
    }

    /// Performs a single HTTP/1.1 request and returns the full response.
    ///
    /// Parses the URL, resolves the host, connects TCP, and for `https` wraps the
    /// stream in a TLS session (using the cached system CA bundle, or an empty
    /// bundle with verification skipped when [`RequestOptions.insecure`]). The
    /// request is then written and the response read by [`Client.doRequest`].
    /// The connection is closed on return; the [`ClientResponse`] owns its heap
    /// and must be `deinit`'d.
    pub fn request(allocator: Allocator, io: Io, opts: RequestOptions) !ClientResponse {
        const parsed = try parseUrl(opts.url);

        const address = try resolveHost(io, parsed.host, parsed.port);
        var stream = try address.connect(io, .{
            .mode = .stream,
            .protocol = .tcp,
        });
        defer stream.close(io);

        if (parsed.is_https) {
            const root_ca = if (opts.insecure) tls.config.cert.Bundle.empty else try getSystemRootCa(io);
            const rng_impl: std.Random.IoSource = .{ .io = io };
            var tls_conn = try tls.clientFromStream(io, stream, .{
                .host = parsed.host,
                .rng = rng_impl.interface(),
                .now = Io.Clock.now(.real, io),
                .root_ca = root_ca,
                .insecure_skip_verify = opts.insecure,
            });
            defer tls_conn.close() catch {};

            var tls_read_buf: [tls.input_buffer_len]u8 = undefined;
            var tls_write_buf: [tls.output_buffer_len]u8 = undefined;
            var reader = tls_conn.reader(&tls_read_buf);
            var writer = tls_conn.writer(&tls_write_buf);

            return doRequest(allocator, &reader.interface, &writer.interface, parsed, opts);
        } else {
            var read_buf: [4096]u8 = undefined;
            var write_buf: [4096]u8 = undefined;
            var reader = stream.reader(io, &read_buf);
            var writer = stream.writer(io, &write_buf);

            return doRequest(allocator, &reader.interface, &writer.interface, parsed, opts);
        }
    }

    /// Writes the request line, headers, and body to `writer`, then reads the
    /// response.
    ///
    /// Generic over `reader`/`writer` so it serves both the plaintext and TLS
    /// paths in [`Client.request`]. Builds the whole request head into an 8192-
    /// byte stack buffer (request line, `Host`, optional `Content-Length`, a
    /// forced `Connection: close`, an optional `X-Request-Id`, then any
    /// caller headers), writes it, streams the body if present, flushes, and
    /// delegates to [`Client.readResponse`]. The fixed head buffer means very
    /// large header sets will error from `bufPrint`.
    fn doRequest(
        allocator: Allocator,
        reader: anytype,
        writer: anytype,
        parsed: ParsedUrl,
        opts: RequestOptions
    ) !ClientResponse {
        var req_buf: [8192]u8 = undefined;
        var pos: usize = 0;

        const method_str = switch (opts.method) {
            .get => "GET",
            .post => "POST",
            .put => "PUT",
            .delete => "DELETE",
            .patch => "PATCH",
            .head => "HEAD",
            .options => "OPTIONS",
        };

        const line = try std.fmt.bufPrint(req_buf[pos..], "{s} {s} HTTP/1.1\r\n", .{ method_str, parsed.path });
        pos += line.len;

        const host_hdr = try std.fmt.bufPrint(req_buf[pos..], "Host: {s}\r\n", .{parsed.host});
        pos += host_hdr.len;

        if (opts.body) |body| {
            const cl = try std.fmt.bufPrint(req_buf[pos..], "Content-Length: {d}\r\n", .{body.len});
            pos += cl.len;
        }

        const conn_hdr = "Connection: close\r\n";
        @memcpy(req_buf[pos..][0..conn_hdr.len], conn_hdr);
        pos += conn_hdr.len;

        if (opts.request_id) |rid| {
            if (rid.len > 0) {
                const rid_hdr = try std.fmt.bufPrint(req_buf[pos..], "X-Request-Id: {s}\r\n", .{rid});
                pos += rid_hdr.len;
            }
        }

        for (opts.headers) |h| {
            const hdr = try std.fmt.bufPrint(req_buf[pos..], "{s}: {s}\r\n", .{ h[0], h[1] });
            pos += hdr.len;
        }

        req_buf[pos] = '\r';
        req_buf[pos + 1] = '\n';
        pos += 2;

        try writer.writeAll(req_buf[0..pos]);

        if (opts.body) |body| {
            try writer.writeAll(body);
        }
        try writer.flush();

        return readResponse(allocator, reader);
    }

    /// Reads and parses a complete HTTP/1.1 response from `reader`.
    ///
    /// Reads the header block byte-by-byte into an 8192-byte buffer until the
    /// `\r\n\r\n` terminator, parses the status code off the first line, then
    /// collects each `Name: Value` header (values trimmed of spaces). The body
    /// is read according to the framing headers: dechunked via
    /// [`Client.readChunkedBody`] when `Transfer-Encoding: chunked`, else exactly
    /// `Content-Length` bytes, else empty. Returns `error.InvalidResponse` on a
    /// malformed status line and `error.ConnectionClosed` if the stream ends
    /// mid-response. The returned [`ClientResponse`] owns its headers and body.
    fn readResponse(allocator: Allocator, reader: anytype) !ClientResponse {
        var header_buf: [8192]u8 = undefined;
        var header_len: usize = 0;

        while (header_len < header_buf.len) {
            const b = reader.peekByte() catch return error.ConnectionClosed;
            reader.seek += 1;
            header_buf[header_len] = b;
            header_len += 1;

            if (b == '\n' and header_len >= 4 and
                header_buf[header_len - 4] == '\r' and
                header_buf[header_len - 3] == '\n' and
                header_buf[header_len - 2] == '\r')
            {
                break;
            }
        }

        const header_str = header_buf[0..header_len];
        const first_line_end = std.mem.indexOf(u8, header_str, "\r\n") orelse return error.InvalidResponse;
        const first_line = header_str[0..first_line_end];

        const sp1 = std.mem.indexOfScalar(u8, first_line, ' ') orelse return error.InvalidResponse;
        const rest = first_line[sp1 + 1 ..];
        const sp2 = std.mem.indexOfScalar(u8, rest, ' ') orelse rest.len;
        const status = std.fmt.parseInt(u16, rest[0..sp2], 10) catch return error.InvalidResponse;

        var headers = std.ArrayList(ClientResponse.Header).empty;
        var lines = std.mem.splitSequence(u8, header_str[first_line_end + 2 ..], "\r\n");
        while (lines.next()) |line| {
            if (line.len == 0) break;
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            try headers.append(allocator, .{
                .name = line[0..colon],
                .value = std.mem.trim(u8, line[colon + 1 ..], " "),
            });
        }

        var content_length: usize = 0;
        var chunked = false;
        for (headers.items) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, "Content-Length")) {
                content_length = std.fmt.parseInt(usize, h.value, 10) catch 0;
            } else if (std.ascii.eqlIgnoreCase(h.name, "Transfer-Encoding")) {
                if (std.ascii.indexOfIgnoreCase(h.value, "chunked") != null) chunked = true;
            }
        }

        var body: []const u8 = "";
        if (chunked) {
            body = try readChunkedBody(allocator, reader);
        } else if (content_length > 0) {
            const body_buf = try allocator.alloc(u8, content_length);
            reader.readSliceAll(body_buf) catch {
                allocator.free(body_buf);
                return error.ConnectionClosed;
            };
            body = body_buf;
        }

        return .{
            .status = status,
            .headers = try headers.toOwnedSlice(allocator),
            .body = body,
            .allocator = allocator,
        };
    }

    /// Decodes a `Transfer-Encoding: chunked` response body into a single buffer.
    ///
    /// Loops reading each chunk: a hex size line (any `;`-delimited chunk
    /// extension is ignored), then that many payload bytes, then the trailing
    /// CRLF. A zero-size chunk ends the body; any trailer headers after it are
    /// read and discarded up to the blank line. Returns `error.InvalidResponse`
    /// on a bad size line and `error.ConnectionClosed` on a truncated stream.
    /// The accumulated body is returned as an owned slice (freed on error via
    /// the `errdefer`).
    fn readChunkedBody(allocator: Allocator, reader: anytype) ![]const u8 {
        var body = std.ArrayList(u8).empty;
        errdefer body.deinit(allocator);

        var line_buf: [64]u8 = undefined;
        while (true) {
            const line = readLine(reader, &line_buf) catch return error.ConnectionClosed;
            const semi = std.mem.indexOfScalar(u8, line, ';');
            const size_str = std.mem.trim(u8, if (semi) |s| line[0..s] else line, " \t\r");
            const chunk_size = std.fmt.parseInt(usize, size_str, 16) catch return error.InvalidResponse;

            if (chunk_size == 0) {
                while (true) {
                    const trailer = readLine(reader, &line_buf) catch break;
                    if (trailer.len == 0) break;
                }
                break;
            }

            try body.ensureUnusedCapacity(allocator, chunk_size);
            const start = body.items.len;
            body.items.len = start + chunk_size;
            reader.readSliceAll(body.items[start..]) catch return error.ConnectionClosed;

            var crlf: [2]u8 = undefined;
            reader.readSliceAll(&crlf) catch return error.ConnectionClosed;
        }
        return try body.toOwnedSlice(allocator);
    }

    /// Reads a single CRLF-terminated line into `buf`, returning it without the
    /// CRLF.
    ///
    /// Used by [`Client.readChunkedBody`] for chunk-size and trailer lines. Reads
    /// byte-by-byte and returns when it sees `\n` preceded by `\r`. Returns
    /// `error.LineTooLong` if the line exceeds `buf`, or `error.ConnectionClosed`
    /// if the stream ends first.
    fn readLine(reader: anytype, buf: []u8) ![]const u8 {
        var i: usize = 0;
        while (i < buf.len) {
            const b = reader.peekByte() catch return error.ConnectionClosed;
            reader.seek += 1;
            if (b == '\n' and i > 0 and buf[i - 1] == '\r') return buf[0 .. i - 1];
            buf[i] = b;
            i += 1;
        }
        return error.LineTooLong;
    }
};

