const std = @import("std");
const Allocator = std.mem.Allocator;
const Io = std.Io;
const btree = @import("btree");
const tls = btree.tls;
const wire = btree.proto_wire;

pub const Client = struct {
    allocator: Allocator,
    io: Io,
    stream: std.Io.net.Stream,
    tls_client: ?tls.Connection = null,
    reader: *Io.Reader,
    writer: *Io.Writer,
    session_token: ?[]const u8 = null,

    tls_read_buf: [tls.input_buffer_len]u8 = undefined,
    tls_write_buf: [tls.output_buffer_len]u8 = undefined,
    read_buf: [4096]u8 = undefined,
    write_buf: [4096]u8 = undefined,

    r_tls: tls.Connection.Reader = undefined,
    w_tls: tls.Connection.Writer = undefined,
    r_tcp: std.Io.net.Stream.Reader = undefined,
    w_tcp: std.Io.net.Stream.Writer = undefined,

    pub fn init(allocator: Allocator, io: Io, host: []const u8, port: u16, tls_enabled: bool) !*Client {
        const address = try std.Io.net.IpAddress.parse(host, port);
        var stream = try address.connect(io, .{ .mode = .stream, .protocol = .tcp });
        errdefer stream.close(io);

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

        const self = try allocator.create(Client);
        errdefer allocator.destroy(self);

        self.* = .{
            .allocator = allocator,
            .io = io,
            .stream = stream,
            .tls_client = null,
            .reader = undefined,
            .writer = undefined,
            .session_token = null,
        };

        if (tls_enabled) {
            const rng_impl: std.Random.IoSource = .{ .io = io };
            self.tls_client = tls.clientFromStream(io, stream, .{
                .host = host,
                .rng = rng_impl.interface(),
                .now = Io.Clock.now(.real, io),
                .root_ca = tls.config.cert.Bundle.empty,
                .insecure_skip_verify = true,
            }) catch |err| {
                std.debug.print("TLS Handshake failed: {}\n", .{err});
                return err;
            };
            self.r_tls = self.tls_client.?.reader(&self.tls_read_buf);
            self.w_tls = self.tls_client.?.writer(&self.tls_write_buf);
            self.reader = &self.r_tls.interface;
            self.writer = &self.w_tls.interface;
        } else {
            self.r_tcp = stream.reader(io, &self.read_buf);
            self.w_tcp = stream.writer(io, &self.write_buf);
            self.reader = &self.r_tcp.interface;
            self.writer = &self.w_tcp.interface;
        }

        return self;
    }

    pub fn deinit(self: *Client) void {
        if (self.tls_client) |*tc| tc.close() catch {};
        self.stream.close(self.io);
        if (self.session_token) |t| self.allocator.free(t);
        self.allocator.destroy(self);
    }

    pub fn disconnect(self: *Client) void {
        _ = self;
    }

    /// Read one framed reply (`[type:u8][len:u32 BE][payload]`). Payload owned.
    fn readFrame(self: *Client) !struct { tag: u8, payload: []u8 } {
        var hdr: [5]u8 = undefined;
        try self.reader.readSliceAll(&hdr);
        const len = std.mem.readInt(u32, hdr[1..5], .big);
        const plen = len - 4;
        const buf = try self.allocator.alloc(u8, plen);
        errdefer self.allocator.free(buf);
        if (plen > 0) try self.reader.readSliceAll(buf[0..plen]);
        return .{ .tag = hdr[0], .payload = buf };
    }

    /// Run a SQL statement over the binary wire protocol (a `Frontend.query`
    /// frame, the same negotiated connection the document client uses), draining
    /// the typed reply frames to ReadyForQuery. Rows come back as binary
    /// `data_row` frames rather than a JSON packet, so this avoids the JSON
    /// serialise/parse the legacy packet path pays. YCSB only needs success or
    /// failure, so on success it returns a small owned marker (the caller frees
    /// it), and surfaces a server error as `error.QueryFailed`.
    pub fn execute(self: *Client, sql: []const u8) ![]const u8 {
        var b = wire.Builder.init(self.allocator);
        defer b.deinit();
        try b.putStr16(sql);
        const frame = try b.finish(@intFromEnum(wire.Frontend.query));
        defer self.allocator.free(frame);
        try self.writer.writeAll(frame);
        try self.writer.flush();

        var had_error = false;
        while (true) {
            const f = try self.readFrame();
            defer self.allocator.free(f.payload);
            if (f.tag == @intFromEnum(wire.Backend.ready)) break;
            if (f.tag == @intFromEnum(wire.Backend.error_response)) {
                had_error = true;
            }
            // row_description / data_row / command_complete are drained and ignored.
        }
        if (had_error) return error.QueryFailed;
        return self.allocator.dupe(u8, "OK");
    }

    /// Run a SQL query and return the number of result rows (counts `data_row`
    /// frames). For an aggregate/`COUNT(*)` this is the number of result rows, not
    /// the aggregate value. Used by the orders benchmark to report result sizes.
    pub fn queryCount(self: *Client, sql: []const u8) !usize {
        var b = wire.Builder.init(self.allocator);
        defer b.deinit();
        try b.putStr16(sql);
        const frame = try b.finish(@intFromEnum(wire.Frontend.query));
        defer self.allocator.free(frame);
        try self.writer.writeAll(frame);
        try self.writer.flush();

        var rows: usize = 0;
        var had_error = false;
        while (true) {
            const f = try self.readFrame();
            defer self.allocator.free(f.payload);
            if (f.tag == @intFromEnum(wire.Backend.ready)) break;
            if (f.tag == @intFromEnum(wire.Backend.data_row)) rows += 1;
            if (f.tag == @intFromEnum(wire.Backend.error_response)) had_error = true;
        }
        if (had_error) return error.QueryFailed;
        return rows;
    }

    /// Perform the startup handshake and authenticate. This is the FIRST thing
    /// sent on the connection (its leading `Frontend.startup` byte routes the
    /// server into the PostgreSQL-style session dialect), so `execute` afterwards
    /// speaks the binary query protocol. Answers a cleartext-password challenge
    /// when the server (with security enabled) issues one.
    pub fn login(self: *Client, uid: []const u8, password: []const u8) !void {
        var b = wire.Builder.init(self.allocator);
        defer b.deinit();
        try b.putU16(1); // proto major
        try b.putU16(0); // proto minor
        try b.putStr16(uid);
        try b.putStr16(""); // database (default)
        try b.putStr16("ycsb"); // application
        const su = try b.finish(@intFromEnum(wire.Frontend.startup));
        defer self.allocator.free(su);
        try self.writer.writeAll(su);
        try self.writer.flush();

        while (true) {
            const f = try self.readFrame();
            defer self.allocator.free(f.payload);
            if (f.tag == @intFromEnum(wire.Backend.ready)) return;
            if (f.tag == @intFromEnum(wire.Backend.auth) and f.payload.len >= 4) {
                const method = std.mem.readInt(u32, f.payload[0..4], .big);
                if (method == @intFromEnum(wire.AuthMethod.cleartext_password)) {
                    var pb = wire.Builder.init(self.allocator);
                    defer pb.deinit();
                    try pb.putStr16(password);
                    const pr = try pb.finish(@intFromEnum(wire.Frontend.auth_response));
                    defer self.allocator.free(pr);
                    try self.writer.writeAll(pr);
                    try self.writer.flush();
                }
            }
            if (f.tag == @intFromEnum(wire.Backend.error_response)) return error.LoginFailed;
        }
    }
};
