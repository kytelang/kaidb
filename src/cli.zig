//! CLI client for the kaidb server (`kaidb-cli`).
//!
//! A thin, standalone SQL client: it owns no storage and never interprets SQL.
//! It opens a socket to a running server, speaks the **pure binary wire
//! protocol** in `proto/protocol.zig` (the "NOVA"-magic framed protocol,
//! MessageType `connect`/`query`/`query_resp`/`err`), and pretty-prints the
//! binary result. There is NO HTTP and NO JSON anywhere on this path: results
//! arrive as the split fixed/heap row encoding and are decoded field by field.
//!
//! Modes
//! -----
//!   * `-c "SQL"`   run one statement, print it, exit (scripting / CI).
//!   * piped stdin  read all input, split on `;`, run each statement in order
//!                  (e.g. `kaidb-cli < script.sql`, `echo "SELECT 1" | kaidb-cli`).
//!   * a TTY        interactive prompt: read a line, run it, until EOF or `exit`.
//!
//! Usage: `kaidb-cli [host[:port]] [--tls|--no-tls] [-u user] [-p pass]
//!         [-d db] [-c "SQL"]`. Host/port/TLS default from `db.json`; auth
//! defaults to `admin`/`admin` on database `default`.

const std = @import("std");
const Io = std.Io;
const tls = @import("tls");
const Config = @import("common/config.zig").Config;
/// The pure binary wire protocol: "NOVA"-magic framing, message types, and the
/// split fixed/heap row encoding the server emits for a result set.
const proto = @import("proto/protocol.zig");

const MessageType = proto.MessageType;
const MessageHeader = proto.MessageHeader;

/// One decoded protocol frame: the message type and its owned payload bytes.
const Frame = struct {
    msg_type: u8,
    payload: []u8,
};

/// Writes a 16-byte binary header (`magic`, `msg_type`, `flags=0`, `stream_id`,
/// `payload_len`) followed by `payload`, then flushes. Little-endian on the
/// wire, matching the server's host-endian encode on this (arm64/x86) ABI.
fn writeFrame(writer: *Io.Writer, msg_type: MessageType, stream_id: u16, payload: []const u8) !void {
    var hdr: [16]u8 = undefined;
    std.mem.writeInt(u32, hdr[0..4], MessageHeader.MAGIC_VALUE, .little);
    hdr[4] = @intFromEnum(msg_type);
    hdr[5] = 0; // flags
    std.mem.writeInt(u16, hdr[6..8], stream_id, .little);
    std.mem.writeInt(u64, hdr[8..16], payload.len, .little);
    try writer.writeAll(&hdr);
    try writer.writeAll(payload);
    try writer.flush();
}

/// Reads one frame: the 16-byte header (validated against the magic) and its
/// payload. The payload is owned by `allocator`; the caller frees it.
fn readFrame(reader: *Io.Reader, allocator: std.mem.Allocator) !Frame {
    var hdr: [16]u8 = undefined;
    try reader.readSliceAll(&hdr);
    if (std.mem.readInt(u32, hdr[0..4], .little) != MessageHeader.MAGIC_VALUE) return error.InvalidMagic;
    const msg_type = hdr[4];
    const payload_len = std.mem.readInt(u64, hdr[8..16], .little);
    const payload = try allocator.alloc(u8, @intCast(payload_len));
    errdefer allocator.free(payload);
    try reader.readSliceAll(payload);
    return .{ .msg_type = msg_type, .payload = payload };
}

/// Little-endian cursor over a payload with bounds checks (short read → error).
const Cursor = struct {
    b: []const u8,
    o: usize = 0,
    fn u8v(self: *Cursor) !u8 {
        if (self.o + 1 > self.b.len) return error.Truncated;
        const v = self.b[self.o];
        self.o += 1;
        return v;
    }
    fn u16v(self: *Cursor) !u16 {
        if (self.o + 2 > self.b.len) return error.Truncated;
        const v = std.mem.readInt(u16, self.b[self.o..][0..2], .little);
        self.o += 2;
        return v;
    }
    fn u32v(self: *Cursor) !u32 {
        if (self.o + 4 > self.b.len) return error.Truncated;
        const v = std.mem.readInt(u32, self.b[self.o..][0..4], .little);
        self.o += 4;
        return v;
    }
    fn u64v(self: *Cursor) !u64 {
        if (self.o + 8 > self.b.len) return error.Truncated;
        const v = std.mem.readInt(u64, self.b[self.o..][0..8], .little);
        self.o += 8;
        return v;
    }
    fn bytes(self: *Cursor, n: usize) ![]const u8 {
        if (self.o + n > self.b.len) return error.Truncated;
        const s = self.b[self.o .. self.o + n];
        self.o += n;
        return s;
    }
};

/// Prints an `err` frame's `[error_code u32][message_len u16][message]` body.
fn printError(out: *Io.Writer, payload: []const u8) !void {
    var c = Cursor{ .b = payload };
    const code = c.u32v() catch 0;
    const mlen = c.u16v() catch 0;
    const msg = c.bytes(mlen) catch "";
    try out.print("Error [{d}]: {s}\n", .{ code, msg });
}

/// Sends one SQL statement as a `query` frame and renders the reply.
///
/// Reads back either an `err` frame (printed as a diagnostic) or a `query_resp`
/// frame, which is decoded field by field: a result-type byte, `rows_affected`,
/// the column descriptors, then (if present) the row block in the split
/// fixed/heap layout. A SELECT prints an ASCII table; a DML prints the affected
/// row count. Returns false on a transport error so the caller can stop.
fn runStatement(reader: *Io.Reader, writer: *Io.Writer, out: *Io.Writer, allocator: std.mem.Allocator, sql: []const u8) !bool {
    // query payload: [tx_mode u8][sql_len u32][sql][param_count u16]
    var qp = std.ArrayList(u8).empty;
    defer qp.deinit(allocator);
    try qp.append(allocator, 0); // tx_mode
    var lenbuf: [4]u8 = undefined;
    std.mem.writeInt(u32, &lenbuf, @intCast(sql.len), .little);
    try qp.appendSlice(allocator, &lenbuf);
    try qp.appendSlice(allocator, sql);
    try qp.appendSlice(allocator, &[_]u8{ 0, 0 }); // param_count u16 = 0

    writeFrame(writer, .query, 1, qp.items) catch |err| {
        try out.print("send failed: {}\n", .{err});
        return false;
    };

    const frame = readFrame(reader, allocator) catch |err| {
        try out.print("connection closed: {}\n", .{err});
        return false;
    };
    defer allocator.free(frame.payload);

    if (frame.msg_type == @intFromEnum(MessageType.err)) {
        try printError(out, frame.payload);
        return true;
    }
    if (frame.msg_type != @intFromEnum(MessageType.query_resp)) {
        try out.print("unexpected reply type {d}\n", .{frame.msg_type});
        return true;
    }

    var c = Cursor{ .b = frame.payload };
    const result_type = try c.u8v(); // 0 = select, 1 = non-select
    const rows_affected = try c.u64v();
    const num_cols = try c.u16v();

    var cols = try allocator.alloc([]const u8, num_cols);
    defer allocator.free(cols);
    var i: usize = 0;
    while (i < num_cols) : (i += 1) {
        const nlen = try c.u16v();
        cols[i] = try c.bytes(nlen);
        _ = try c.u8v(); // col_type (server describes every column as TEXT)
        _ = try c.u32v(); // offset (unused by this reader)
        _ = try c.u32v(); // width  (unused)
    }

    const has_rows = try c.u8v();
    if (has_rows == 0 or result_type == 1) {
        if (result_type == 1) {
            try out.print("Query OK, {d} row(s) affected\n\n", .{rows_affected});
        } else {
            try printTable(out, cols, &[_][]const []const u8{});
        }
        return true;
    }

    const num_rows = try c.u32v();
    var rows = try allocator.alloc([]const []const u8, num_rows);
    defer {
        for (rows) |r| allocator.free(r);
        allocator.free(rows);
    }
    var r: usize = 0;
    while (r < num_rows) : (r += 1) {
        // Each row is self-delimiting: [fixed_len u32][fixed][heap_len u32][heap].
        const fixed_len = try c.u32v();
        const fixed = try c.bytes(fixed_len);
        const heap_len = try c.u32v();
        const heap = try c.bytes(heap_len);

        var cells = try allocator.alloc([]const u8, num_cols);
        var k: usize = 0;
        while (k < num_cols) : (k += 1) {
            if (k * 4 + 4 > fixed.len) {
                cells[k] = "";
                continue;
            }
            const off = std.mem.readInt(u32, fixed[k * 4 ..][0..4], .little);
            if (off + 4 > heap.len) {
                cells[k] = "";
                continue;
            }
            const slen = std.mem.readInt(u32, heap[off..][0..4], .little);
            if (off + 4 + slen > heap.len) {
                cells[k] = "";
                continue;
            }
            cells[k] = heap[off + 4 .. off + 4 + slen];
        }
        rows[r] = cells;
    }

    try printTable(out, cols, rows);
    return true;
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.arena.allocator();
    const io = init.io;

    const args = try init.minimal.args.toSlice(allocator);

    const config_dir = Io.Dir.cwd();
    var parsed_config = Config.load(allocator, io, config_dir) catch |err| {
        std.debug.print("configuration error: {any}\n", .{err});
        return err;
    };
    defer parsed_config.deinit();
    const config = &parsed_config.value;

    var host: []const u8 = config.address;
    var port: u16 = config.port;
    var tls_enabled: bool = config.tls.enabled;
    var user: []const u8 = "admin";
    var pass: []const u8 = "admin";
    var db_name: []const u8 = "default";
    var one_shot: ?[]const u8 = null;

    // Parse: a bare `host[:port]` positional, plus flags. `-c/-u/-p/-d` take the
    // next argument; `--tls/--no-tls` toggle transport.
    var ai: usize = 1;
    while (ai < args.len) : (ai += 1) {
        const a = args[ai];
        if (std.mem.eql(u8, a, "--tls")) {
            tls_enabled = true;
        } else if (std.mem.eql(u8, a, "--no-tls")) {
            tls_enabled = false;
        } else if (std.mem.eql(u8, a, "-c")) {
            ai += 1;
            if (ai < args.len) one_shot = args[ai];
        } else if (std.mem.eql(u8, a, "-u")) {
            ai += 1;
            if (ai < args.len) user = args[ai];
        } else if (std.mem.eql(u8, a, "-p")) {
            ai += 1;
            if (ai < args.len) pass = args[ai];
        } else if (std.mem.eql(u8, a, "-d")) {
            ai += 1;
            if (ai < args.len) db_name = args[ai];
        } else if (std.mem.startsWith(u8, a, "-")) {
            // Unknown flag: ignore rather than abort a scripted run.
        } else if (std.mem.indexOf(u8, a, ":")) |colon| {
            host = a[0..colon];
            port = std.fmt.parseInt(u16, a[colon + 1 ..], 10) catch port;
        } else {
            host = a;
        }
    }

    var stdout_file = std.Io.File.stdout();
    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = stdout_file.writer(io, &stdout_buf);
    const out = &stdout_w.interface;

    const interactive = one_shot == null and (std.Io.File.stdin().isTty(io) catch false);

    const address = std.Io.net.IpAddress.parse(host, port) catch |err| {
        std.debug.print("Failed to parse address '{s}:{d}': {}\n", .{ host, port, err });
        return err;
    };
    var stream = address.connect(io, .{ .mode = .stream, .protocol = .tcp }) catch |err| {
        std.debug.print("Failed to connect to database server at {s}:{d}: {}\n", .{ host, port, err });
        return err;
    };
    defer stream.close(io);

    var tls_client: ?tls.Connection = null;
    defer if (tls_client) |*tc| tc.close() catch {};

    var reader: *Io.Reader = undefined;
    var writer: *Io.Writer = undefined;

    var tls_read_buf: [tls.input_buffer_len]u8 = undefined;
    var tls_write_buf: [tls.output_buffer_len]u8 = undefined;
    var read_buf: [4096]u8 = undefined;
    var write_buf: [4096]u8 = undefined;

    var r_tls: tls.Connection.Reader = undefined;
    var w_tls: tls.Connection.Writer = undefined;
    var r_tcp: std.Io.net.Stream.Reader = undefined;
    var w_tcp: std.Io.net.Stream.Writer = undefined;

    if (tls_enabled) {
        const rng_impl: std.Random.IoSource = .{ .io = io };
        tls_client = tls.clientFromStream(io, stream, .{
            .host = host,
            .rng = rng_impl.interface(),
            .now = Io.Clock.now(.real, io),
            .root_ca = tls.config.cert.Bundle.empty,
            .insecure_skip_verify = true,
        }) catch |err| {
            std.debug.print("TLS Handshake failed: {}\n", .{err});
            return err;
        };
        r_tls = tls_client.?.reader(&tls_read_buf);
        w_tls = tls_client.?.writer(&tls_write_buf);
        reader = &r_tls.interface;
        writer = &w_tls.interface;
    } else {
        r_tcp = stream.reader(io, &read_buf);
        w_tcp = stream.writer(io, &write_buf);
        reader = &r_tcp.interface;
        writer = &w_tcp.interface;
    }

    // Handshake: connect payload is
    // [version u32][user_len u16][user][pass_len u16][pass][db_len u16][db].
    {
        var cp = std.ArrayList(u8).empty;
        defer cp.deinit(allocator);
        var b4: [4]u8 = undefined;
        std.mem.writeInt(u32, &b4, proto.PROTOCOL_VERSION, .little);
        try cp.appendSlice(allocator, &b4);
        try appendLenPrefixed(&cp, allocator, user);
        try appendLenPrefixed(&cp, allocator, pass);
        try appendLenPrefixed(&cp, allocator, db_name);
        try writeFrame(writer, .connect, 1, cp.items);

        const frame = readFrame(reader, allocator) catch |err| {
            std.debug.print("Failed to connect (handshake): {}\n", .{err});
            return err;
        };
        defer allocator.free(frame.payload);
        if (frame.msg_type == @intFromEnum(MessageType.err)) {
            try printError(out, frame.payload);
            try out.flush();
            return;
        }
        if (frame.msg_type != @intFromEnum(MessageType.connect_resp) or frame.payload.len < 1 or frame.payload[0] != 0) {
            std.debug.print("Handshake rejected by server\n", .{});
            return error.HandshakeFailed;
        }
    }

    // One-shot: run the single statement and exit.
    if (one_shot) |sql| {
        _ = try runStatement(reader, writer, out, allocator, std.mem.trim(u8, sql, " \r\t\n;"));
        writeFrame(writer, .close, 1, &[_]u8{}) catch {};
        try out.flush();
        return;
    }

    if (interactive) {
        try out.writeAll("kaidb CLI (binary protocol). Type 'exit' or 'quit', or Ctrl-D.\n\n");
        try out.flush();
        var stdin_file = std.Io.File.stdin();
        var stdin_buf: [8192]u8 = undefined;
        var stdin_r = stdin_file.reader(io, &stdin_buf);
        const stdin = &stdin_r.interface;
        while (true) {
            try out.writeAll("kaidb> ");
            try out.flush();
            const line = stdin.takeDelimiterExclusive('\n') catch |err| {
                if (err == error.EndOfStream) break;
                return err;
            };
            const t = std.mem.trim(u8, line, " \r\t;");
            if (t.len == 0) continue;
            if (std.mem.eql(u8, t, "exit") or std.mem.eql(u8, t, "quit")) break;
            if (!try runStatement(reader, writer, out, allocator, t)) break;
            try out.flush();
        }
    } else {
        // Piped / redirected input: read it all, split into `;`-terminated
        // statements, run each. Exits cleanly at EOF (no interactive prompt).
        var stdin_file = std.Io.File.stdin();
        var stdin_buf: [8192]u8 = undefined;
        var stdin_r = stdin_file.reader(io, &stdin_buf);
        const stdin = &stdin_r.interface;
        // Read the whole input to EOF (allocRemaining handles EOF correctly,
        // unlike a takeDelimiter loop which can block/spin on a closed pipe).
        const input = stdin.allocRemaining(allocator, .unlimited) catch |err| blk: {
            if (err == error.EndOfStream) break :blk try allocator.alloc(u8, 0);
            return err;
        };
        defer allocator.free(input);
        var it = std.mem.splitScalar(u8, input, ';');
        while (it.next()) |stmt| {
            const t = std.mem.trim(u8, stmt, " \r\t\n");
            if (t.len == 0) continue;
            if (!try runStatement(reader, writer, out, allocator, t)) break;
        }
    }

    writeFrame(writer, .close, 1, &[_]u8{}) catch {};
    try out.flush();
}

/// Appends a `u16` length-prefixed string (little-endian length) to `list`.
fn appendLenPrefixed(list: *std.ArrayList(u8), allocator: std.mem.Allocator, s: []const u8) !void {
    var b2: [2]u8 = undefined;
    std.mem.writeInt(u16, &b2, @intCast(s.len), .little);
    try list.appendSlice(allocator, &b2);
    try list.appendSlice(allocator, s);
}

/// Renders a result set as an ASCII box table (MySQL-CLI style) to `out`
/// (stdout, so results are pipeable). Column widths are the max of the header
/// and every cell in that column. Draws nothing for zero columns.
fn printTable(out: *Io.Writer, columns: []const []const u8, rows: []const []const []const u8) !void {
    if (columns.len == 0) {
        try out.writeAll("(no columns)\n\n");
        return;
    }
    var widths = std.heap.page_allocator.alloc(usize, columns.len) catch return;
    defer std.heap.page_allocator.free(widths);
    for (columns, 0..) |col, i| widths[i] = col.len;
    for (rows) |row| {
        for (row, 0..) |cell, i| {
            if (i < widths.len) widths[i] = @max(widths[i], cell.len);
        }
    }

    try printSeparator(out, widths);
    try out.writeAll("|");
    for (columns, 0..) |col, i| try out.print(" {s:<[1]} |", .{ col, widths[i] });
    try out.writeAll("\n");
    try printSeparator(out, widths);
    for (rows) |row| {
        try out.writeAll("|");
        for (row, 0..) |cell, i| {
            if (i < widths.len) try out.print(" {s:<[1]} |", .{ cell, widths[i] });
        }
        try out.writeAll("\n");
    }
    try printSeparator(out, widths);
    try out.print("{d} row(s) in set\n\n", .{rows.len});
}

/// Prints a `+----+----+` rule sized to `widths` (each column `width + 2`).
fn printSeparator(out: *Io.Writer, widths: []const usize) !void {
    try out.writeAll("+");
    for (widths) |w| {
        var j: usize = 0;
        while (j < w + 2) : (j += 1) try out.writeAll("-");
        try out.writeAll("+");
    }
    try out.writeAll("\n");
}
