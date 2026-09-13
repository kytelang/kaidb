const std = @import("std");
const assert = std.debug.assert;
const Io = std.Io;

pub const max_ciphertext_record_len = @import("cipher.zig").max_ciphertext_record_len;

pub const input_buffer_len = max_ciphertext_record_len;

pub const output_buffer_len = @import("cipher.zig").max_encrypted_record_len;

pub const Connection = @import("connection.zig").Connection;

const handshake = struct {
    const Client = @import("handshake_client.zig").Handshake;
    const Server = @import("handshake_server.zig").Handshake;
};

pub inline fn clientFromStream(io: std.Io, stream: anytype, opt: config.Client) !Connection {
    const input, const output = streamToRaderWriter(io, stream);
    return try client(input, output, opt);
}

pub fn client(input: *Io.Reader, output: *Io.Writer, opt: config.Client) !Connection {
    assert(input.buffer.len >= input_buffer_len);
    assert(output.buffer.len >= 2048);

    var hc: handshake.Client = .{ .input = input, .output = output };
    const cipher, const session_resumption_secret_idx = try hc.handshake(opt);
    return .{
        .cipher = cipher,
        .input = input,
        .output = output,
        .session_resumption_secret_idx = session_resumption_secret_idx,
        .session_resumption = opt.session_resumption,
    };
}

pub inline fn serverFromStream(io: Io, stream: anytype, opt: config.Server) !Connection {
    const input, const output = streamToRaderWriter(io, stream);
    return try server(input, output, opt);
}

pub fn server(input: *Io.Reader, output: *Io.Writer, opt: config.Server) !Connection {
    var hs: handshake.Server = .{ .input = input, .output = output };
    const cipher = try hs.handshake(opt);
    return .{
        .cipher = cipher,
        .input = input,
        .output = output,
    };
}

inline fn streamToRaderWriter(io: std.Io, stream: anytype) struct { *Io.Reader, *Io.Writer } {
    var input_buf: [input_buffer_len]u8 = undefined;
    var output_buf: [output_buffer_len]u8 = undefined;
    var reader = stream.reader(io, &input_buf);
    var writer = stream.writer(io, &output_buf);
    const input = if (@hasField(@TypeOf(reader), "interface")) &reader.interface else reader.interface();
    const output = &writer.interface;
    return .{ input, output };
}

pub const Cipher = @import("cipher.zig").Cipher;
pub const config = struct {
    const proto = @import("protocol.zig");
    const common = @import("handshake_common.zig");

    pub const CipherSuite = @import("cipher.zig").CipherSuite;
    pub const PrivateKey = @import("PrivateKey.zig");
    pub const NamedGroup = proto.NamedGroup;
    pub const Version = proto.Version;
    pub const cert = common.cert;
    pub const CertKeyPair = common.CertKeyPair;

    pub const cipher_suites = @import("cipher.zig").cipher_suites;
    pub const key_log = @import("key_log.zig");

    pub const Client = @import("handshake_client.zig").Options;
    pub const Server = @import("handshake_server.zig").Options;
};

pub const nonblock = struct {
    pub const Client = @import("handshake_client.zig").NonBlock;
    pub const Server = @import("handshake_server.zig").NonBlock;
    pub const Connection = @import("connection.zig").NonBlock;
};

pub const Ktls = @import("Ktls.zig");

test "nonblock handshake and connection" {
    const testing = @import("std").testing;
    const rng_impl: std.Random.IoSource = .{ .io = testing.io };
    const rng = rng_impl.interface();

    var sc_buf: [max_ciphertext_record_len]u8 = undefined;
    var cs_buf: [max_ciphertext_record_len]u8 = undefined;

    const cli_cipher, const srv_cipher = brk: {
        var cli = nonblock.Client.init(.{
            .rng = rng,
            .root_ca = .{ .map = .empty, .bytes = .empty },
            .host = &.{},
            .insecure_skip_verify = true,
            .now = .zero,
        });
        var srv = nonblock.Server.init(.{
            .rng = rng,
            .auth = null,
            .now = .zero,
        });

        var cr = try cli.run(&sc_buf, &cs_buf);
        try testing.expectEqual(0, cr.recv_pos);
        try testing.expect(cr.send.len > 0);
        try testing.expect(!cli.done());

        {
            for (0..cr.send_pos) |i| {
                const sr = try srv.run(cs_buf[0..i], &sc_buf);
                try testing.expectEqual(0, sr.recv_pos);
                try testing.expectEqual(0, sr.send_pos);
            }
        }

        var sr = try srv.run(&cs_buf, &sc_buf);
        try testing.expectEqual(sr.recv_pos, cr.send_pos);
        try testing.expect(sr.send.len > 0);
        try testing.expect(!srv.done());

        {
            for (0..sr.send_pos) |i| {
                cr = try cli.run(sc_buf[0..i], &cs_buf);
                try testing.expectEqual(0, cr.recv_pos);
                try testing.expectEqual(0, cr.send_pos);
            }
        }

        cr = try cli.run(&sc_buf, &cs_buf);
        try testing.expectEqual(sr.send_pos, cr.recv_pos);
        try testing.expect(cr.send.len > 0);
        try testing.expect(cli.done());
        try testing.expect(cli.cipher() != null);

        sr = try srv.run(&cs_buf, &sc_buf);
        try testing.expectEqual(sr.recv_pos, cr.send_pos);
        try testing.expectEqual(0, sr.send.len);
        try testing.expect(srv.done());
        try testing.expect(srv.cipher() != null);

        break :brk .{ cli.cipher().?, srv.cipher().? };
    };
    {
        var cli = nonblock.Connection.init(cli_cipher);
        var srv = nonblock.Connection.init(srv_cipher);

        const cleartext = "Lorem ipsum dolor sit amet";
        {
            const e = try cli.encrypt(cleartext, &cs_buf);
            try testing.expectEqual(cleartext.len, e.cleartext_pos);
            try testing.expect(e.ciphertext.len > cleartext.len);
            try testing.expect(e.unused_cleartext.len == 0);

            const d = try srv.decrypt(e.ciphertext, &sc_buf);
            try testing.expectEqualSlices(u8, cleartext, d.cleartext);
            try testing.expectEqual(e.ciphertext.len, d.ciphertext_pos);
            try testing.expectEqual(0, d.unused_ciphertext.len);
        }
        {
            const e = try srv.encrypt(cleartext, &sc_buf);
            const d = try cli.decrypt(e.ciphertext, &cs_buf);
            try testing.expectEqualSlices(u8, cleartext, d.cleartext);
        }
        {
            const close_buf = try srv.close(&sc_buf);
            const d = try cli.decrypt(close_buf, &cs_buf);
            try testing.expectEqual(close_buf.len, d.ciphertext_pos);
            try testing.expectEqual(0, d.unused_ciphertext.len);
            try testing.expect(d.closed);
        }
    }
}

test {
    _ = @import("handshake_common.zig");
    _ = @import("handshake_server.zig");
    _ = @import("handshake_client.zig");

    _ = @import("connection.zig");
    _ = @import("cipher.zig");
    _ = @import("record.zig");
    _ = @import("transcript.zig");
    _ = @import("PrivateKey.zig");
}
