//! Interactive REPL client for the NovaDB server (`novadb-cli`).
//!
//! This is the human-facing SQL shell that ships alongside the `novadb`
//! server binary. It is a thin, standalone client: it owns no storage, no
//! B+Tree, and no catalog. All it does is open a socket to a running server,
//! speak the binary wire protocol defined in `common/proto.zig`, and pretty
//! print whatever comes back. Every query the user types is forwarded verbatim
//! to the server, which parses and executes the SQL; the CLI never interprets
//! SQL itself.
//!
//! Wire protocol and framing
//! --------------------------
//! Requests and responses are [`Packet`] values (see `common/proto.zig`). Each
//! serialised packet is length-prefixed with a little-endian `u32` payload
//! length, so the read path here is deliberately two-step: read the 4-byte
//! length header, allocate `4 + payload_len` bytes, copy the header back in,
//! then read exactly the remaining payload before handing the whole frame to
//! [`Packet.deserialize`]. This matches how the server writes replies and is
//! why a short read on the header is treated as "connection closed" rather than
//! a protocol error.
//!
//! A query is sent as an [`Operation`] `.Query` packet carrying the raw SQL and
//! the current session token; the reply arrives as a `.Reply` packet whose
//! `data` field is a JSON document (columns/rows for result sets,
//! `rows_affected` for DML, or `error_message` for a server-side error). The
//! JSON body is parsed with `std.json` and rendered as an ASCII box table by
//! [`printTable`].
//!
//! Sessions and authentication
//! ---------------------------
//! Login is not a separate protocol message: it is an ordinary SQL statement
//! whose result set is a single `session_token` column. The REPL special-cases
//! that shape, when a reply has exactly one column named `session_token` and at
//! least one row, it captures the token, stores it (freeing any previous one),
//! prints "Login successful!", and thereafter attaches the token to every
//! outgoing query. The token is heap-owned by this process and freed on exit.
//!
//! Transport
//! ---------
//! The connection is either plain TCP or TLS, chosen by config and overridable
//! per-run with a `--tls` / `--no-tls` argument. When TLS is on, the client uses
//! `insecure_skip_verify` (it does not validate the server certificate), which
//! is acceptable for a local admin shell but is NOT a secure client for
//! untrusted networks. In both cases the code funnels down to a single
//! `*Io.Reader` / `*Io.Writer` pair so the request/response loop is transport
//! agnostic.
//!
//! Error handling philosophy
//! -------------------------
//! The REPL is designed to survive bad input without dying: per-query failures
//! (send failure, malformed JSON, server error) print a diagnostic and `continue`
//! the loop, whereas failures that mean the connection is gone (a short read on
//! the length header or payload) `break` out of the loop and end the session.

const std = @import("std");
/// Shorthand for the standard library's I/O namespace (`std.Io`).
///
/// Used throughout for the reader/writer interfaces, `Io.Dir`, `Io.File`,
/// `Io.Clock`, and `Io.net`, the async-capable I/O surface the server and CLI
/// share.
const Io = std.Io;
/// The TLS library module, providing [`tls.Connection`] and the handshake
/// entry point [`tls.clientFromStream`] used for encrypted transport.
const tls = @import("tls");
/// The server/client configuration record loaded from `db.json`, supplying the
/// default host, port, and TLS settings before any command-line overrides.
const Config = @import("common/config.zig").Config;
/// The binary wire-protocol module: packet framing, serialisation, and the
/// operation/reply types the CLI exchanges with the server.
const proto = @import("common/proto.zig");
/// A single length-prefixed protocol message. See [`proto`]; the CLI both
/// serialises `.Query` packets and deserialises `.Reply` packets through it.
const Packet = proto.Packet;
/// The tagged union of protocol operations (`.Query`, `.Reply`, ...). Aliased
/// for readability at the packet-construction sites.
const Operation = proto.Operation;

/// Entry point: run the interactive REPL until the user exits or the connection
/// drops.
///
/// The flow is: load [`Config`] from the current directory, apply optional
/// `host[:port]` and `--tls`/`--no-tls` overrides from argv, open a TCP (or TLS)
/// stream to the server, then loop reading a line of SQL, sending it as a
/// [`Packet`] `.Query`, and rendering the `.Reply`.
///
/// The single request/response iteration is the subtle part. It writes the
/// serialised query and flushes, then reads the 4-byte little-endian length
/// header, allocates the full `4 + payload_len` frame, copies the header back,
/// and reads the payload before deserialising, see the file header for why the
/// length prefix is reconstructed rather than skipped. A failure to read the
/// header or payload means the peer is gone and ends the loop; a failure to
/// send, parse JSON, or a server-reported error only skips the current line.
///
/// Login is detected structurally: a reply that is a single `session_token`
/// column with rows captures the token into `session_token` (freeing the prior
/// one) so subsequent queries authenticate. All heap allocations use the
/// process arena or the general allocator and are freed via `defer`; the
/// captured session token is freed on exit.
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

    if (args.len > 1) {
        const arg = args[1];
        if (std.mem.indexOf(u8, arg, ":")) |colon_idx| {
            host = arg[0..colon_idx];
            port = std.fmt.parseInt(u16, arg[colon_idx + 1 ..], 10) catch 3009;
        } else {
            host = arg;
        }
    }

    if (args.len > 2) {
        if (std.mem.eql(u8, args[2], "--tls")) {
            tls_enabled = true;
        } else if (std.mem.eql(u8, args[2], "--no-tls")) {
            tls_enabled = false;
        }
    }

    const stdout_file = std.Io.File.stdout();
    var stdout_buf: [1024]u8 = undefined;
    var stdout_w = stdout_file.writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;

    const stdin_file = std.Io.File.stdin();
    var stdin_buf: [4096]u8 = undefined;
    var stdin_r = stdin_file.reader(io, &stdin_buf);
    const stdin = &stdin_r.interface;

    try stdout.writeAll("B+Tree Relational Database CLI REPL\n");
    try stdout.writeAll("Type 'exit' or 'quit' to exit.\n\n");
    try stdout_w.interface.flush();

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

    var session_token: ?[]const u8 = null;
    defer if (session_token) |tok| allocator.free(tok);

    while (true) {
        try stdout.writeAll("nova> ");
        try stdout_w.interface.flush();
        const line = stdin.takeDelimiterExclusive('\n') catch |err| {
            if (err == error.EndOfStream) break;
            return err;
        };

        const trimmed = std.mem.trim(u8, line, " \r\t");
        if (trimmed.len == 0) continue;
        if (std.mem.eql(u8, trimmed, "exit") or std.mem.eql(u8, trimmed, "quit")) break;

        const query_pkt = Packet{
            .op = .{
                .Query = .{
                    .sql = trimmed,
                    .session_token = session_token,
                },
            },
        };

        query_pkt.serialize(writer) catch |err| {
            std.debug.print("Failed to send query: {}\n", .{err});
            continue;
        };
        writer.flush() catch |err| {
            std.debug.print("Failed to flush connection: {}\n", .{err});
            continue;
        };

        var len_bytes: [4]u8 = undefined;
        reader.readSliceAll(&len_bytes) catch |err| {
            std.debug.print("Connection closed by server: {}\n", .{err});
            break;
        };
        const payload_len = std.mem.readInt(u32, &len_bytes, .little);
        const buf = allocator.alloc(u8, 4 + payload_len) catch |err| {
            std.debug.print("OOM allocating payload buffer: {}\n", .{err});
            continue;
        };
        defer allocator.free(buf);
        @memcpy(buf[0..4], &len_bytes);

        reader.readSliceAll(buf[4..]) catch |err| {
            std.debug.print("Failed to read response payload: {}\n", .{err});
            break;
        };

        const resp_packet = Packet.deserialize(allocator, buf) catch |err| {
            std.debug.print("Failed to deserialize response: {}\n", .{err});
            continue;
        };
        defer Packet.free(allocator, resp_packet);

        switch (resp_packet.op) {
            .Reply => |reply| {
                if (reply.status == .Error) {
                    const err_str = reply.data orelse "Unknown Error";
                    std.debug.print("Error: {s}\n", .{err_str});
                    continue;
                }
                const body = reply.data orelse "{}";

                const parsed = std.json.parseFromSlice(std.json.Value, allocator, body, .{}) catch |err| {
                    std.debug.print("Failed to parse JSON response: {} (Raw: {s})\n", .{ err, body });
                    continue;
                };
                defer parsed.deinit();

                if (parsed.value == .object) {
                    const err_msg = parsed.value.object.get("error_message");
                    if (err_msg != null and err_msg.? != .null) {
                        std.debug.print("Error: {s}\n", .{err_msg.?.string});
                    } else {
                        const cols_val = parsed.value.object.get("columns");
                        const rows_val = parsed.value.object.get("rows");

                        if (cols_val != null and rows_val != null and cols_val.? == .array and rows_val.? == .array) {
                            const cols = cols_val.?.array;
                            const rows = rows_val.?.array;

                            if (cols.items.len == 1 and std.mem.eql(u8, cols.items[0].string, "session_token") and rows.items.len > 0) {
                                const token = rows.items[0].array.items[0].string;
                                if (session_token) |tok| allocator.free(tok);
                                session_token = try allocator.dupe(u8, token);
                                try stdout.writeAll("Login successful!\n");
                                try stdout_w.interface.flush();
                                continue;
                            }

                            var col_slice = try allocator.alloc([]const u8, cols.items.len);
                            defer allocator.free(col_slice);
                            for (cols.items, 0..) |c, i| {
                                col_slice[i] = c.string;
                            }

                            var row_slice = try allocator.alloc([]const []const u8, rows.items.len);
                            defer allocator.free(row_slice);
                            for (rows.items, 0..) |r, i| {
                                var row_cells = try allocator.alloc([]const u8, r.array.items.len);
                                for (r.array.items, 0..) |cell, j| {
                                    row_cells[j] = cell.string;
                                }
                                row_slice[i] = row_cells;
                            }
                            defer {
                                for (row_slice) |r| allocator.free(r);
                            }

                            printTable(col_slice, row_slice);
                        } else {
                            const rows_affected = parsed.value.object.get("rows_affected");
                            const count = if (rows_affected) |ra| ra.integer else 0;
                            std.debug.print("Query OK, {d} rows affected\n\n", .{count});
                        }
                    }
                }
            },
            else => {
                std.debug.print("Received unexpected operation response type\n", .{});
            },
        }
    }
}

/// Render a result set as an ASCII box table on stderr, MySQL-CLI style.
///
/// Column widths are computed in a first pass as the maximum of the header
/// length and every cell length in that column, so the table is aligned to the
/// widest value. It then prints a `+---+` separator, the header row, another
/// separator, one left-justified row per record, a closing separator, and a
/// trailing "N rows in set" line.
///
/// Returns immediately (drawing nothing) if there are no columns. The width
/// array is taken from `std.heap.page_allocator` rather than the caller's
/// allocator because this is a fire-and-forget rendering helper with no
/// allocator parameter; on allocation failure it silently returns rather than
/// erroring, since failing to draw a table must not abort the REPL. Rendering
/// goes through `std.debug.print` (stderr), matching the diagnostic output the
/// rest of the loop uses. See [`printSeparator`] for the rule lines.
fn printTable(columns: []const []const u8, rows: []const []const []const u8) void {
    if (columns.len == 0) return;

    var widths = std.heap.page_allocator.alloc(usize, columns.len) catch return;
    defer std.heap.page_allocator.free(widths);
    for (columns, 0..) |col, i| {
        widths[i] = col.len;
    }
    for (rows) |row| {
        for (row, 0..) |cell, i| {
            if (i < widths.len) {
                widths[i] = @max(widths[i], cell.len);
            }
        }
    }

    printSeparator(widths);
    std.debug.print("|", .{});
    for (columns, 0..) |col, i| {
        std.debug.print(" {s:<[1]} |", .{ col, widths[i] });
    }
    std.debug.print("\n", .{});
    printSeparator(widths);

    for (rows) |row| {
        std.debug.print("|", .{});
        for (row, 0..) |cell, i| {
            if (i < widths.len) {
                std.debug.print(" {s:<[1]} |", .{ cell, widths[i] });
            }
        }
        std.debug.print("\n", .{});
    }
    printSeparator(widths);
    std.debug.print("{d} rows in set\n\n", .{rows.len});
}

/// Print a horizontal rule line (`+----+----+`) sized to the given column
/// widths.
///
/// Each column contributes `width + 2` dashes (the extra two account for the
/// single-space padding [`printTable`] puts on either side of every cell),
/// bracketed by `+` characters. Written to stderr via `std.debug.print` to stay
/// consistent with [`printTable`].
fn printSeparator(widths: []const usize) void {
    std.debug.print("+", .{});
    for (widths) |w| {
        var j: usize = 0;
        while (j < w + 2) : (j += 1) {
            std.debug.print("-", .{});
        }
        std.debug.print("+", .{});
    }
    std.debug.print("\n", .{});
}
