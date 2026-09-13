const std = @import("std");
const aes = std.crypto.core.aes;
const mem = std.mem;
const debug = std.debug;

pub fn CBC(comptime BlockCipher: anytype) type {
    const EncryptCtx = aes.AesEncryptCtx(BlockCipher);
    const DecryptCtx = aes.AesDecryptCtx(BlockCipher);

    return struct {
        const Self = @This();

        enc_ctx: EncryptCtx,
        dec_ctx: DecryptCtx,

        pub fn init(key: [BlockCipher.key_bits / 8]u8) Self {
            const enc_ctx = BlockCipher.initEnc(key);
            const dec_ctx = DecryptCtx.initFromEnc(enc_ctx);

            return Self{ .enc_ctx = enc_ctx, .dec_ctx = dec_ctx };
        }

        pub fn paddedLength(length: usize) usize {
            return (std.math.divCeil(usize, length + 1, EncryptCtx.block_length) catch unreachable) * EncryptCtx.block_length;
        }

        pub fn encrypt(self: Self, dst: []u8, src: []const u8, iv: [EncryptCtx.block_length]u8) void {
            const block_length = EncryptCtx.block_length;
            const padded_length = paddedLength(src.len);
            debug.assert(dst.len == padded_length);
            var cv = iv;
            var i: usize = 0;
            while (i + block_length <= src.len) : (i += block_length) {
                const in = src[i..][0..block_length];
                for (cv[0..], in) |*x, y| x.* ^= y;
                self.enc_ctx.encrypt(&cv, &cv);
                @memcpy(dst[i..][0..block_length], &cv);
            }
            var in = [_]u8{0} ** block_length;
            const padding_length: u8 = @intCast(padded_length - src.len - 1);
            @memset(&in, padding_length);
            @memcpy(in[0 .. src.len - i], src[i..]);
            for (cv[0..], in) |*x, y| x.* ^= y;
            self.enc_ctx.encrypt(&cv, &cv);
            @memcpy(dst[i..], cv[0 .. dst.len - i]);
        }

        pub fn decrypt(self: Self, dst: []u8, src: []const u8, iv: [DecryptCtx.block_length]u8) !void {
            const block_length = DecryptCtx.block_length;
            if (src.len != dst.len) {
                return error.EncodingError;
            }
            debug.assert(src.len % block_length == 0);
            var i: usize = 0;
            var cv = iv;
            var out: [block_length]u8 = undefined;
            while (i + block_length <= dst.len) : (i += block_length) {
                const in = src[i..][0..block_length];
                self.dec_ctx.decrypt(&out, in);
                for (&out, cv) |*x, y| x.* ^= y;
                cv = in.*;
                @memcpy(dst[i..][0..block_length], &out);
            }
            if (i < dst.len) {
                const in = src[i..][0..block_length];
                @memset(&out, 0);
                self.dec_ctx.decrypt(&out, in);
                for (&out, cv) |*x, y| x.* ^= y;
                @memcpy(dst[i..], out[0 .. dst.len - i]);
            }
        }
    };
}

test "CBC mode" {
    const M = CBC(aes.Aes128);
    const key = [_]u8{ 0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c };
    const iv = [_]u8{ 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f };
    const src_ = "This is a test of AES-CBC that goes on longer than a couple blocks. It is a somewhat long test case to type out!";
    const expected = "\xA0\x8C\x09\x7D\xFF\x42\xB6\x65\x4D\x4B\xC6\x90\x90\x39\xDE\x3D\xC7\xCA\xEB\xF6\x9A\x4F\x09\x97\xC9\x32\xAB\x75\x88\xB7\x57\x17";
    var res: [32]u8 = undefined;

    try comptime std.testing.expect(src_.len / M.paddedLength(1) >= 3);

    const z = M.init(key);

    var h = std.crypto.hash.sha2.Sha256.init(.{});
    inline for (0..src_.len) |len| {
        const src = src_[0..len];
        var dst = [_]u8{0} ** M.paddedLength(src.len);

        z.encrypt(&dst, src, iv);
        h.update(&dst);

        var decrypted = [_]u8{0} ** dst.len;
        try z.decrypt(&decrypted, &dst, iv);

        const padding = decrypted[decrypted.len - 1] + 1;
        try std.testing.expectEqualSlices(u8, src, decrypted[0 .. decrypted.len - padding]);
    }
    h.final(&res);
    try std.testing.expectEqualSlices(u8, expected, &res);

    h = std.crypto.hash.sha2.Sha256.init(.{});
    inline for (0..src_.len) |len| {
        var buf = [_]u8{0} ** M.paddedLength(len);
        @memcpy(buf[0..len], src_[0..len]);
        z.encrypt(&buf, buf[0..len], iv);
        h.update(&buf);

        try z.decrypt(&buf, &buf, iv);

        try std.testing.expectEqualSlices(u8, src_[0..len], buf[0..len]);
    }
    h.final(&res);
    try std.testing.expectEqualSlices(u8, expected, &res);
}
