const std = @import("std");
const builtin = @import("builtin");
const mem = std.mem;
const Io = std.Io;
const assert = std.debug.assert;

const proto = @import("protocol.zig");
const record = @import("record.zig");
const Record = record.Record;
const cipher = @import("cipher.zig");
const Cipher = cipher.Cipher;
const SessionResumption = @import("handshake_client.zig").Options.SessionResumption;

const log = std.log.scoped(.tls);

pub const Connection = struct {
    input: *Io.Reader,
    output: *Io.Writer,
    cipher: Cipher,

    max_encrypt_seq: u64 = std.math.maxInt(u64) - 1,
    key_update_requested: bool = false,
    received_close_notify: bool = false,
    cleartext_buf: []const u8 = &.{},

    session_resumption: ?*SessionResumption = null,
    session_resumption_secret_idx: ?usize = null,

    const Self = @This();

    fn writeRecord(c: *Self, content_type: proto.ContentType, bytes: []const u8) !void {
        assert(bytes.len <= cipher.max_cleartext_len);
        if (c.cipher.encryptSeq() >= c.max_encrypt_seq or @atomicLoad(bool, &c.key_update_requested, .monotonic)) {
            @atomicStore(bool, &c.key_update_requested, false, .monotonic);

            const key_update = &record.handshakeHeader(.key_update, 1) ++ [_]u8{0};
            try c.encryptWrite(.handshake, key_update);
            try c.cipher.keyUpdateEncrypt();
        }
        try c.encryptWrite(content_type, bytes);
    }

    fn encryptWrite(c: *Self, content_type: proto.ContentType, bytes: []const u8) !void {
        const encrypted_len = c.cipher.recordLen(bytes.len);
        const writable = try c.output.writableSliceGreedy(encrypted_len);
        const rec = try c.cipher.encrypt(writable, content_type, bytes);
        c.output.advance(rec.len);
        try c.output.flush();
    }

    pub fn next(c: *Self) anyerror!?[]const u8 {
        return c.nextRecord(&.{}) catch |err| {
            if (err == error.EndOfStream) return null;
            if (mem.startsWith(u8, @errorName(err), "Tls"))
                try c.encryptWrite(.alert, &proto.alertFromError(err));
            return err;
        };
    }

    fn nextRecord(c: *Self, buffer: []u8) ![]const u8 {
        assert(c.cleartext_buf.len == 0);
        if (c.received_close_notify) return error.EndOfStream;
        while (true) {
            const rec = try Record.read(c.input);
            if (rec.protocol_version != .tls_1_2) return error.TlsBadVersion;

            const cleartext_buf = if (buffer.len >= rec.payload.len) buffer else @constCast(rec.buffer);
            const content_type, const cleartext = try c.cipher.decrypt(cleartext_buf, rec);

            switch (content_type) {
                .application_data => {},
                .handshake => {
                    const handshake_type: proto.Handshake = @enumFromInt(cleartext[0]);
                    switch (handshake_type) {
                        .new_session_ticket => {
                            if (c.session_resumption) |r| {
                                r.pushTicket(cleartext, c.session_resumption_secret_idx.?) catch {};
                            }
                            continue;
                        },
                        .key_update => {
                            if (cleartext.len != 5) return error.TlsDecodeError;
                            try c.cipher.keyUpdateDecrypt();
                            const key: proto.KeyUpdateRequest = @enumFromInt(cleartext[4]);
                            switch (key) {
                                .update_requested => {
                                    @atomicStore(bool, &c.key_update_requested, true, .monotonic);
                                },
                                .update_not_requested => {},
                                else => return error.TlsIllegalParameter,
                            }
                            continue;
                        },
                        else => {},
                    }
                },
                .alert => {
                    if (cleartext.len < 2) return error.TlsUnexpectedMessage;
                    try proto.Alert.parse(cleartext[0..2].*).toError();
                    c.received_close_notify = true;
                    return error.EndOfStream;
                },
                else => return error.TlsUnexpectedMessage,
            }
            return cleartext;
        }
    }

    pub fn eof(c: *Self) bool {
        return c.received_close_notify and c.cleartext_buf.len == 0;
    }

    pub fn close(c: *Self) anyerror!void {
        if (c.received_close_notify) return;
        try c.writeRecord(.alert, &proto.Alert.closeNotify());
    }


    pub fn write(c: *Self, bytes: []const u8) !usize {
        const encrypt_overhead = c.cipher.encryptOverhead();
        assert(c.output.buffer.len > encrypt_overhead);
        const n = @min(bytes.len, cipher.max_cleartext_len, c.output.buffer.len - encrypt_overhead);
        try c.writeRecord(.application_data, bytes[0..n]);
        return n;
    }

    pub fn writeAll(c: *Self, bytes: []const u8) !void {
        var index: usize = 0;
        while (index < bytes.len) {
            index += try c.write(bytes[index..]);
        }
    }

    pub fn read(c: *Self, buffer: []u8) !usize {
        if (c.cleartext_buf.len == 0) {
            const cleartext = c.nextRecord(buffer) catch |err| {
                if (err == error.EndOfStream) return 0;
                if (mem.startsWith(u8, @errorName(err), "Tls"))
                    try c.encryptWrite(.alert, &proto.alertFromError(err));
                return err;
            };
            if (cleartext.ptr == buffer.ptr) {
                return cleartext.len;
            }
            c.cleartext_buf = cleartext;
        }
        const n = @min(c.cleartext_buf.len, buffer.len);
        @memcpy(buffer[0..n], c.cleartext_buf[0..n]);
        c.cleartext_buf = c.cleartext_buf[n..];
        return n;
    }

    pub fn readAll(c: *Self, buffer: []u8) !usize {
        return c.readAtLeast(buffer, buffer.len);
    }

    pub fn readAtLeast(c: *Self, buffer: []u8, len: usize) !usize {
        assert(len <= buffer.len);
        var index: usize = 0;
        while (index < len) {
            const amt = try c.read(buffer[index..]);
            if (amt == 0) break;
            index += amt;
        }
        return index;
    }

    pub fn readv(c: *Self, iovecs: []std.posix.iovec) !usize {
        var vp: VecPut = .{ .iovecs = iovecs };
        while (true) {
            if (c.cleartext_buf.len == 0) {
                c.cleartext_buf = try c.next() orelse break;
            }
            const n = vp.put(c.cleartext_buf);
            const read_buf_len = c.cleartext_buf.len;
            c.cleartext_buf = c.cleartext_buf[n..];
            if (n < read_buf_len) break;
        }
        return vp.total;
    }


    pub const Reader = struct {
        conn: *Connection,
        interface: Io.Reader,
        err: ?anyerror = null,

        pub fn init(c: *Connection, buffer: []u8) Reader {
            return .{
                .conn = c,
                .interface = .{
                    .vtable = &.{
                        .stream = stream,
                    },
                    .buffer = buffer,
                    .seek = 0,
                    .end = 0,
                },
            };
        }

        fn stream(r: *Io.Reader, w: *Io.Writer, limit: Io.Limit) Io.Reader.StreamError!usize {
            const self: *Reader = @fieldParentPtr("interface", r);
            const buf = limit.slice(try w.writableSliceGreedy(1));
            const n = self.conn.read(buf) catch |err| {
                self.err = err;
                if (err == error.EndOfStream) return error.EndOfStream;
                return error.ReadFailed;
            };
            if (n == 0) return error.EndOfStream;
            w.advance(n);
            return n;
        }
    };

    pub fn reader(c: *Self, buffer: []u8) Reader {
        return .init(c, buffer);
    }


    pub const Writer = struct {
        conn: *Connection,
        interface: Io.Writer,
        err: ?anyerror = null,

        pub fn init(c: *Connection, buffer: []u8) Writer {
            return .{
                .conn = c,
                .interface = .{
                    .vtable = &.{
                        .drain = drain,
                    },
                    .buffer = buffer,
                    .end = 0,
                },
            };
        }

        fn drain(w: *Io.Writer, data: []const []const u8, splat: usize) Io.Writer.Error!usize {
            const self: *Writer = @fieldParentPtr("interface", w);
            try self.writeAll(w.buffered());
            w.end = 0;

            if (data.len == 0) return 0;
            var n: usize = 0;
            for (data[0 .. data.len - 1]) |bytes| {
                try self.writeAll(bytes);
                n += bytes.len;
            }

            const pattern = data[data.len - 1];
            for (0..splat) |_| {
                try self.writeAll(pattern);
                n += pattern.len;
            }

            return n;
        }

        fn writeAll(self: *Writer, bytes: []const u8) Io.Writer.Error!void {
            self.conn.writeAll(bytes) catch |err| {
                self.err = err;
                return error.WriteFailed;
            };
        }
    };

    pub fn writer(c: *Self, buffer: []u8) Writer {
        return .init(c, buffer);
    }
};

const testing = std.testing;
const data12 = @import("testdata/tls12.zig");
const testu = @import("testu.zig");

test "encrypt decrypt" {
    const rng_impl: std.Random.IoSource = .{ .io = testing.io };
    const rng = rng_impl.interface();
    var output_buf: [1024]u8 = undefined;
    var source_buf = data12.server_pong ** 4;
    var stream_reader: Io.Reader = .fixed(&source_buf);
    var stream_writer: Io.Writer = .fixed(&output_buf);
    var conn: Connection = .{
        .input = &stream_reader,
        .output = &stream_writer,
        .cipher = try Cipher.initTls12(.ECDHE_RSA_WITH_AES_128_CBC_SHA, &data12.key_material, .client, rng),
    };
    conn.cipher.ECDHE_RSA_WITH_AES_128_CBC_SHA.rng = testu.random(0);

    {
        _ = testu.random(0x40);
        try conn.writeRecord(.handshake, &data12.client_finished);
        try testing.expectEqualSlices(u8, &data12.verify_data_encrypted_msg, conn.output.buffered());
    }
    _ = conn.output.consumeAll();
    {
        const cleartext = "ping";
        _ = testu.random(0);

        try conn.writeAll(cleartext);
        try testing.expectEqualSlices(u8, &data12.encrypted_ping_msg, conn.output.buffered());
    }
    _ = conn.output.consumeAll();
    {
        const cleartext = "ping";
        _ = testu.random(0);
        conn.cipher.ECDHE_RSA_WITH_AES_128_CBC_SHA.encrypt_seq = 1;

        var writer = conn.writer(&.{});
        var w = &writer.interface;
        try w.writeAll(cleartext);
        try testing.expectEqualSlices(u8, &data12.encrypted_ping_msg, conn.output.buffered());
    }
    {
        conn.cipher.ECDHE_RSA_WITH_AES_128_CBC_SHA.decrypt_seq = 1;
        try testing.expectEqualStrings("pong", (try conn.next()).?);
    }
    {
        conn.cipher.ECDHE_RSA_WITH_AES_128_CBC_SHA.decrypt_seq = 1;
        var buffer: [2]u8 = undefined;
        var reader = conn.reader(&buffer);
        var rdr = &reader.interface;
        try testing.expectEqualStrings("", conn.cleartext_buf);
        try testing.expectEqualStrings("po", try rdr.take(rdr.buffer.len));
        try testing.expectEqualStrings("ng", conn.cleartext_buf);
        try testing.expectEqualStrings("n", try rdr.take(1));
        try testing.expectEqualStrings("", conn.cleartext_buf);
        try testing.expectEqualStrings("g", try rdr.take(1));
    }
    {
        conn.cipher.ECDHE_RSA_WITH_AES_128_CBC_SHA.decrypt_seq = 1;
        var buffer: [5]u8 = undefined;
        var reader = conn.reader(&buffer);
        var rdr = &reader.interface;
        try testing.expectEqual(1, try rdr.discard(.limited(1)));
        try testing.expectEqualStrings("ong", conn.cleartext_buf);
        try testing.expectEqual(3, try rdr.discard(.limited(3)));
    }
    {
        conn.cipher.ECDHE_RSA_WITH_AES_128_CBC_SHA.decrypt_seq = 1;
        var buffer: [9]u8 = undefined;
        var iovecs = [_]std.posix.iovec{
            .{ .base = &buffer, .len = 3 },
            .{ .base = buffer[3..], .len = 3 },
            .{ .base = buffer[6..], .len = 3 },
        };
        const n = try conn.readv(iovecs[0..]);
        try testing.expectEqual(4, n);
        try testing.expectEqualStrings("pong", buffer[0..n]);
    }
}

pub const VecPut = struct {
    iovecs: []const std.posix.iovec,
    idx: usize = 0,
    off: usize = 0,
    total: usize = 0,

    pub fn put(vp: *VecPut, bytes: []const u8) usize {
        if (vp.idx >= vp.iovecs.len) return 0;
        var bytes_i: usize = 0;
        while (true) {
            const v = vp.iovecs[vp.idx];
            const dest = v.base[vp.off..v.len];
            const src = bytes[bytes_i..][0..@min(dest.len, bytes.len - bytes_i)];
            @memcpy(dest[0..src.len], src);
            bytes_i += src.len;
            vp.off += src.len;
            if (vp.off >= v.len) {
                vp.off = 0;
                vp.idx += 1;
                if (vp.idx >= vp.iovecs.len) {
                    vp.total += bytes_i;
                    return bytes_i;
                }
            }
            if (bytes_i >= bytes.len) {
                vp.total += bytes_i;
                return bytes_i;
            }
        }
    }
};

test "client/server connection" {
    const rng_impl: std.Random.IoSource = .{ .io = testing.io };
    const rng = rng_impl.interface();

    const cipher_client, const cipher_server = brk: {
        const Transcript = @import("transcript.zig").Transcript;
        const CipherSuite = @import("cipher.zig").CipherSuite;
        const cipher_suite: CipherSuite = .AES_256_GCM_SHA384;

        var rnd_buf: [128]u8 = undefined;
        rng.bytes(&rnd_buf);
        const secret = Transcript.Secret{
            .client = rnd_buf[0..64],
            .server = rnd_buf[64..],
        };

        break :brk .{
            try Cipher.initTls13(cipher_suite, secret, .client),
            try Cipher.initTls13(cipher_suite, secret, .server),
        };
    };

    var client_conn: Connection = .{
        .input = undefined,
        .output = undefined,
        .cipher = cipher_client,
    };
    var server_conn: Connection = .{
        .input = undefined,
        .output = undefined,
        .cipher = cipher_server,
    };

    var cleartext_buf: [cipher.max_cleartext_len * 5]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0);
    const random = prng.random();
    random.bytes(&cleartext_buf);

    for (0..16) |_| {
        const n = random.intRangeAtMost(usize, cipher.max_cleartext_len + 1, cleartext_buf.len);
        const client_cleartext = cleartext_buf[0..n];

        var ciphertext_buf: [cleartext_buf.len]u8 = undefined;
        var w: Io.Writer = .fixed(&ciphertext_buf);
        client_conn.output = &w;

        try client_conn.writeAll(client_cleartext);
        const ciphertext_len = NonBlock.init(cipher_client).encryptedLength(n);
        try testing.expectEqual(ciphertext_len, w.buffered().len);

        var r: Io.Reader = .fixed(w.buffered());
        server_conn.input = &r;
        var server_cleartext_buf: [cleartext_buf.len]u8 = undefined;
        const nr = try server_conn.readAll(&server_cleartext_buf);
        const server_cleartext = server_cleartext_buf[0..nr];

        try testing.expectEqual(n, nr);
        try testing.expectEqualSlices(u8, client_cleartext, server_cleartext);
    }
}

pub const NonBlock = struct {
    const Self = @This();

    inner: Connection,

    pub fn init(c: Cipher) Self {
        return .{ .inner = .{ .cipher = c, .input = undefined, .output = undefined } };
    }

    pub fn encryptedLength(self: Self, cleartext_len: usize) usize {
        const c = self.inner.cipher;
        const records_count = cleartext_len / cipher.max_cleartext_len;
        if (records_count == 0) return c.recordLen(cleartext_len);
        const last_chunk_len = cleartext_len - cipher.max_cleartext_len * records_count;
        return c.recordLen(cipher.max_cleartext_len) * records_count +
            if (last_chunk_len == 0) 0 else c.recordLen(last_chunk_len);
    }

    fn reset(self: *Self) void {
        self.inner.input = undefined;
        self.inner.output = undefined;
    }

    pub fn encrypt(
        self: *Self,
        cleartext: []const u8,
        ciphertext: []u8,
    ) !struct {
        cleartext_pos: usize = 0,
        unused_cleartext: []const u8,
        ciphertext: []u8,
    } {
        defer self.reset();
        var output: Io.Writer = .fixed(ciphertext);
        self.inner.output = &output;

        var n: usize = 0;
        while (n < cleartext.len and output.end < output.buffer.len) {
            n += try self.inner.write(cleartext[n..]);
        }
        return .{
            .cleartext_pos = n,
            .unused_cleartext = cleartext[n..],
            .ciphertext = output.buffered(),
        };
    }

    pub fn decrypt(
        self: *Self,
        ciphertext: []const u8,
        cleartext: []u8,
    ) !struct {
        ciphertext_pos: usize,
        unused_ciphertext: []const u8,
        cleartext: []u8,
        closed: bool = false,
    } {
        defer self.reset();
        var input: Io.Reader = .fixed(ciphertext);
        self.inner.input = &input;

        var n: usize = 0;
        while (n < cleartext.len) {
            const nn = self.inner.read(cleartext[n..]) catch |err| switch (err) {
                error.InputBufferUndersize => break,
                else => return err,
            };
            if (nn == 0) break;
            n += nn;
        }

        return .{
            .ciphertext_pos = input.seek,
            .unused_ciphertext = input.buffered(),
            .cleartext = cleartext[0..n],
            .closed = self.inner.received_close_notify,
        };
    }

    pub fn close(self: *Self, ciphertext: []u8) ![]const u8 {
        return try self.inner.cipher.encrypt(ciphertext, .alert, &proto.Alert.closeNotify());
    }
};

test "nonblock encrypt" {
    const data13 = @import("testdata/tls13.zig");
    const client_cipher, _ = cipher.testCiphers();
    var conn = NonBlock.init(client_cipher);

    const cleartext = "ping";
    try testing.expectEqual(26, conn.encryptedLength(cleartext.len));
    var ciphertext: [32]u8 = undefined;

    {
        const res = try conn.encrypt(cleartext, &ciphertext);
        try testing.expectEqual(cleartext.len, res.cleartext_pos);
        try testing.expectEqual(26, res.ciphertext.len);
        try testing.expectEqualSlices(u8, &data13.client_ping_wrapped, res.ciphertext);
    }

    for (conn.inner.cipher.encryptOverhead() + 1..ciphertext.len) |i| {
        const res = try conn.encrypt(cleartext, ciphertext[0..i]);
        const expected_used_cleartext = @min(cleartext.len, i - conn.inner.cipher.encryptOverhead());
        try testing.expectEqual(expected_used_cleartext, res.cleartext_pos);
        try testing.expectEqual(expected_used_cleartext + conn.inner.cipher.encryptOverhead(), res.ciphertext.len);
    }
}

test "nonblock decrypt" {
    const data13 = @import("testdata/tls13.zig");
    _, const server_cipher = cipher.testCiphers();
    var conn = NonBlock.init(server_cipher);

    const ciphertext = data13.client_ping_wrapped;
    var cleartext_buf: [32]u8 = undefined;

    for (1..ciphertext.len - 1) |i| {
        const res = try conn.decrypt(ciphertext[0..i], &cleartext_buf);
        try testing.expectEqual(0, res.ciphertext_pos);
        try testing.expectEqual(0, res.cleartext.len);
        try testing.expectEqual(i, res.unused_ciphertext.len);
    }

    const res = try conn.decrypt(&ciphertext, &cleartext_buf);
    try testing.expectEqualSlices(u8, "ping", res.cleartext);
}
