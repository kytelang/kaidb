const std = @import("std");
const crypto = std.crypto;
const hkdfExpandLabel = crypto.tls.hkdfExpandLabel;

const Sha1 = crypto.hash.Sha1;
const Sha256 = crypto.hash.sha2.Sha256;
const Sha384 = crypto.hash.sha2.Sha384;

const record = @import("record.zig");
const Record = record.Record;
const Transcript = @import("transcript.zig").Transcript;
const proto = @import("protocol.zig");

const CbcAes128Sha1 = CbcType(crypto.core.aes.Aes128, Sha1);
const CbcAes128Sha256 = CbcType(crypto.core.aes.Aes128, Sha256);
const CbcAes256Sha384 = CbcType(crypto.core.aes.Aes256, Sha384);
const Aead12Aes128Gcm = Aead12Type(crypto.aead.aes_gcm.Aes128Gcm);
const Aead12Aes256Gcm = Aead12Type(crypto.aead.aes_gcm.Aes256Gcm);
const Aead12ChaCha = Aead12ChaChaType(crypto.aead.chacha_poly.ChaCha20Poly1305);
const Aes128GcmSha256 = Aead13Type(crypto.aead.aes_gcm.Aes128Gcm, Sha256);
const Aes256GcmSha384 = Aead13Type(crypto.aead.aes_gcm.Aes256Gcm, Sha384);
const ChaChaSha256 = Aead13Type(crypto.aead.chacha_poly.ChaCha20Poly1305, Sha256);
const Aegis128Sha256 = Aead13Type(crypto.aead.aegis.Aegis128L, Sha256);

pub const encrypt_overhead_tls_12: comptime_int = @max(
    CbcAes128Sha1.encrypt_overhead,
    CbcAes128Sha256.encrypt_overhead,
    CbcAes256Sha384.encrypt_overhead,
    Aead12Aes128Gcm.encrypt_overhead,
    Aead12Aes256Gcm.encrypt_overhead,
    Aead12ChaCha.encrypt_overhead,
);
pub const encrypt_overhead_tls_13: comptime_int = @max(
    Aes128GcmSha256.encrypt_overhead,
    Aes256GcmSha384.encrypt_overhead,
    ChaChaSha256.encrypt_overhead,
    Aegis128Sha256.encrypt_overhead,
);

pub const max_cleartext_len = 1 << 14;
pub const max_ciphertext_len = max_cleartext_len + 256;
pub const max_ciphertext_record_len = record.header_len + max_ciphertext_len;
pub const max_encrypted_record_len = max_cleartext_len + @max(encrypt_overhead_tls_13, encrypt_overhead_tls_12);

test {
    try testing.expectEqual(16645, max_ciphertext_record_len);
    try testing.expectEqual(85, encrypt_overhead_tls_12);
    try testing.expectEqual(22, encrypt_overhead_tls_13);
    try testing.expectEqual(16469, max_encrypted_record_len);
}

fn CipherType(comptime tag: CipherSuite) type {
    return switch (tag) {
        .ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
        .ECDHE_RSA_WITH_AES_128_CBC_SHA,
        .RSA_WITH_AES_128_CBC_SHA,
        => CbcAes128Sha1,
        .ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
        .ECDHE_RSA_WITH_AES_128_CBC_SHA256,
        .RSA_WITH_AES_128_CBC_SHA256,
        => CbcAes128Sha256,
        .ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
        .ECDHE_RSA_WITH_AES_256_CBC_SHA384,
        => CbcAes256Sha384,

        .ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        .ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        => Aead12Aes128Gcm,
        .ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        .ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        => Aead12Aes256Gcm,

        .ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
        .ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
        => Aead12ChaCha,

        .AES_128_GCM_SHA256 => Aes128GcmSha256,
        .AES_256_GCM_SHA384 => Aes256GcmSha384,
        .CHACHA20_POLY1305_SHA256 => ChaChaSha256,
        .AEGIS_128L_SHA256 => Aegis128Sha256,

        else => unreachable,
    };
}

pub const Cipher = union(CipherSuite) {
    ECDHE_ECDSA_WITH_AES_128_CBC_SHA: CipherType(.ECDHE_ECDSA_WITH_AES_128_CBC_SHA),
    ECDHE_RSA_WITH_AES_128_CBC_SHA: CipherType(.ECDHE_RSA_WITH_AES_128_CBC_SHA),
    RSA_WITH_AES_128_CBC_SHA: CipherType(.RSA_WITH_AES_128_CBC_SHA),

    ECDHE_ECDSA_WITH_AES_128_CBC_SHA256: CipherType(.ECDHE_ECDSA_WITH_AES_128_CBC_SHA256),
    ECDHE_RSA_WITH_AES_128_CBC_SHA256: CipherType(.ECDHE_RSA_WITH_AES_128_CBC_SHA256),
    RSA_WITH_AES_128_CBC_SHA256: CipherType(.RSA_WITH_AES_128_CBC_SHA256),

    ECDHE_ECDSA_WITH_AES_256_CBC_SHA384: CipherType(.ECDHE_ECDSA_WITH_AES_256_CBC_SHA384),
    ECDHE_RSA_WITH_AES_256_CBC_SHA384: CipherType(.ECDHE_RSA_WITH_AES_256_CBC_SHA384),
    ECDHE_ECDSA_WITH_AES_128_GCM_SHA256: CipherType(.ECDHE_ECDSA_WITH_AES_128_GCM_SHA256),
    ECDHE_ECDSA_WITH_AES_256_GCM_SHA384: CipherType(.ECDHE_RSA_WITH_AES_256_GCM_SHA384),
    ECDHE_RSA_WITH_AES_128_GCM_SHA256: CipherType(.ECDHE_RSA_WITH_AES_128_GCM_SHA256),
    ECDHE_RSA_WITH_AES_256_GCM_SHA384: CipherType(.ECDHE_RSA_WITH_AES_256_GCM_SHA384),
    ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256: CipherType(.ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256),
    ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256: CipherType(.ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256),
    AES_128_GCM_SHA256: CipherType(.AES_128_GCM_SHA256),
    AES_256_GCM_SHA384: CipherType(.AES_256_GCM_SHA384),
    CHACHA20_POLY1305_SHA256: CipherType(.CHACHA20_POLY1305_SHA256),
    AEGIS_128L_SHA256: CipherType(.AEGIS_128L_SHA256),

    pub fn initTls12(tag: CipherSuite, key_material: []const u8, side: proto.Side, rng: std.Random) !Cipher {
        switch (tag) {
            inline .ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
            .ECDHE_RSA_WITH_AES_128_CBC_SHA,
            .RSA_WITH_AES_128_CBC_SHA,
            .ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
            .ECDHE_RSA_WITH_AES_128_CBC_SHA256,
            .RSA_WITH_AES_128_CBC_SHA256,
            .ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
            .ECDHE_RSA_WITH_AES_256_CBC_SHA384,
            .ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
            .ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            .ECDHE_RSA_WITH_AES_128_GCM_SHA256,
            .ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            .ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
            .ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
            => |comptime_tag| {
                return @unionInit(Cipher, @tagName(comptime_tag), CipherType(comptime_tag).init(key_material, side, rng));
            },
            else => return error.TlsIllegalParameter,
        }
    }

    pub fn initTls13(tag: CipherSuite, secret: Transcript.Secret, side: proto.Side) !Cipher {
        return switch (tag) {
            inline .AES_128_GCM_SHA256,
            .AES_256_GCM_SHA384,
            .CHACHA20_POLY1305_SHA256,
            .AEGIS_128L_SHA256,
            => |comptime_tag| {
                return @unionInit(Cipher, @tagName(comptime_tag), CipherType(comptime_tag).init(secret, side));
            },
            else => return error.TlsIllegalParameter,
        };
    }

    pub fn encrypt(
        c: *Cipher,
        buf: []u8,
        content_type: proto.ContentType,
        cleartext: []const u8,
    ) ![]const u8 {
        return switch (c.*) {
            inline else => |*f| try f.encrypt(buf, content_type, cleartext),
        };
    }

    pub fn decrypt(
        c: *Cipher,
        buf: []u8,
        rec: Record,
    ) !struct { proto.ContentType, []u8 } {
        return switch (c.*) {
            inline else => |*f| {
                const content_type, const cleartext = try f.decrypt(buf, rec);
                if (cleartext.len > max_cleartext_len) return error.TlsRecordOverflow;
                return .{ content_type, cleartext };
            },
        };
    }

    pub fn recordLen(c: Cipher, cleartext_len: usize) usize {
        return switch (c) {
            inline else => |f| f.recordLen(cleartext_len),
        };
    }

    pub fn encryptOverhead(c: Cipher) usize {
        return switch (c) {
            inline else => |f| return @TypeOf(f).encrypt_overhead,
        };
    }

    pub fn encryptSeq(c: Cipher) u64 {
        return switch (c) {
            inline else => |f| f.encrypt_seq,
        };
    }

    pub fn keyUpdateEncrypt(c: *Cipher) !void {
        return switch (c.*) {
            inline .AES_128_GCM_SHA256,
            .AES_256_GCM_SHA384,
            .CHACHA20_POLY1305_SHA256,
            .AEGIS_128L_SHA256,
            => |*f| f.keyUpdateEncrypt(),
            else => return error.TlsUnexpectedMessage,
        };
    }
    pub fn keyUpdateDecrypt(c: *Cipher) !void {
        return switch (c.*) {
            inline .AES_128_GCM_SHA256,
            .AES_256_GCM_SHA384,
            .CHACHA20_POLY1305_SHA256,
            .AEGIS_128L_SHA256,
            => |*f| f.keyUpdateDecrypt(),
            else => return error.TlsUnexpectedMessage,
        };
    }
};

fn Aead12Type(comptime AeadType: type) type {
    return struct {
        const explicit_iv_len = 8;
        const key_len = AeadType.key_length;
        const auth_tag_len = AeadType.tag_length;
        const nonce_len = AeadType.nonce_length;
        const iv_len = AeadType.nonce_length - explicit_iv_len;
        const encrypt_overhead = record.header_len + explicit_iv_len + auth_tag_len;

        encrypt_key: [key_len]u8,
        decrypt_key: [key_len]u8,
        encrypt_iv: [iv_len]u8,
        decrypt_iv: [iv_len]u8,
        encrypt_seq: u64 = 0,
        decrypt_seq: u64 = 0,
        rng: std.Random,

        const Self = @This();

        fn init(key_material: []const u8, side: proto.Side, rng: std.Random) Self {
            const client_key = key_material[0..key_len].*;
            const server_key = key_material[key_len..][0..key_len].*;
            const client_iv = key_material[2 * key_len ..][0..iv_len].*;
            const server_iv = key_material[2 * key_len + iv_len ..][0..iv_len].*;
            return .{
                .encrypt_key = if (side == .client) client_key else server_key,
                .decrypt_key = if (side == .client) server_key else client_key,
                .encrypt_iv = if (side == .client) client_iv else server_iv,
                .decrypt_iv = if (side == .client) server_iv else client_iv,
                .rng = rng,
            };
        }

        pub fn encrypt(
            self: *Self,
            buf: []u8,
            content_type: proto.ContentType,
            cleartext: []const u8,
        ) ![]const u8 {
            const record_len = record.header_len + explicit_iv_len + cleartext.len + auth_tag_len;
            if (buf.len < record_len) return error.TlsCipherNoSpaceLeft;

            const header = buf[0..record.header_len];
            const explicit_iv = buf[record.header_len..][0..explicit_iv_len];
            const ciphertext = buf[record.header_len + explicit_iv_len ..][0..cleartext.len];
            const auth_tag = buf[record.header_len + explicit_iv_len + cleartext.len ..][0..auth_tag_len];

            header.* = record.header(content_type, explicit_iv_len + cleartext.len + auth_tag_len);
            self.rng.bytes(explicit_iv);
            const iv = self.encrypt_iv ++ explicit_iv.*;
            const ad = additionalData(self.encrypt_seq, content_type, cleartext.len);
            AeadType.encrypt(ciphertext, auth_tag, cleartext, &ad, iv, self.encrypt_key);
            self.encrypt_seq +%= 1;

            return buf[0..record_len];
        }

        pub fn recordLen(_: Self, cleartext_len: usize) usize {
            return record.header_len + explicit_iv_len + cleartext_len + auth_tag_len;
        }

        pub fn decrypt(
            self: *Self,
            buf: []u8,
            rec: Record,
        ) !struct { proto.ContentType, []u8 } {
            const overhead = explicit_iv_len + auth_tag_len;
            if (rec.payload.len < overhead) return error.TlsDecryptError;
            const cleartext_len = rec.payload.len - overhead;
            if (buf.len < cleartext_len) return error.TlsCipherNoSpaceLeft;

            const explicit_iv = rec.payload[0..explicit_iv_len];
            const ciphertext = rec.payload[explicit_iv_len..][0..cleartext_len];
            const auth_tag = rec.payload[explicit_iv_len + cleartext_len ..][0..auth_tag_len];

            const iv = self.decrypt_iv ++ explicit_iv.*;
            const ad = additionalData(self.decrypt_seq, rec.content_type, cleartext_len);
            const cleartext = buf[0..cleartext_len];
            AeadType.decrypt(cleartext, ciphertext, auth_tag.*, &ad, iv, self.decrypt_key) catch return error.TlsDecryptError;
            self.decrypt_seq +%= 1;
            return .{ rec.content_type, cleartext };
        }
    };
}

fn Aead12ChaChaType(comptime AeadType: type) type {
    return struct {
        const key_len = AeadType.key_length;
        const auth_tag_len = AeadType.tag_length;
        const nonce_len = AeadType.nonce_length;
        const encrypt_overhead = record.header_len + auth_tag_len;

        encrypt_key: [key_len]u8,
        decrypt_key: [key_len]u8,
        encrypt_iv: [nonce_len]u8,
        decrypt_iv: [nonce_len]u8,
        encrypt_seq: u64 = 0,
        decrypt_seq: u64 = 0,

        const Self = @This();

        fn init(key_material: []const u8, side: proto.Side, _: std.Random) Self {
            const client_key = key_material[0..key_len].*;
            const server_key = key_material[key_len..][0..key_len].*;
            const client_iv = key_material[2 * key_len ..][0..nonce_len].*;
            const server_iv = key_material[2 * key_len + nonce_len ..][0..nonce_len].*;
            return .{
                .encrypt_key = if (side == .client) client_key else server_key,
                .decrypt_key = if (side == .client) server_key else client_key,
                .encrypt_iv = if (side == .client) client_iv else server_iv,
                .decrypt_iv = if (side == .client) server_iv else client_iv,
            };
        }

        pub fn encrypt(
            self: *Self,
            buf: []u8,
            content_type: proto.ContentType,
            cleartext: []const u8,
        ) ![]const u8 {
            const record_len = record.header_len + cleartext.len + auth_tag_len;
            if (buf.len < record_len) return error.TlsCipherNoSpaceLeft;

            const ciphertext = buf[record.header_len..][0..cleartext.len];
            const auth_tag = buf[record.header_len + ciphertext.len ..][0..auth_tag_len];

            const ad = additionalData(self.encrypt_seq, content_type, cleartext.len);
            const iv = ivWithSeq(nonce_len, self.encrypt_iv, self.encrypt_seq);
            AeadType.encrypt(ciphertext, auth_tag, cleartext, &ad, iv, self.encrypt_key);
            self.encrypt_seq +%= 1;

            buf[0..record.header_len].* = record.header(content_type, ciphertext.len + auth_tag.len);
            return buf[0..record_len];
        }

        pub fn recordLen(_: Self, cleartext_len: usize) usize {
            return record.header_len + cleartext_len + auth_tag_len;
        }

        pub fn decrypt(
            self: *Self,
            buf: []u8,
            rec: Record,
        ) !struct { proto.ContentType, []u8 } {
            const overhead = auth_tag_len;
            if (rec.payload.len < overhead) return error.TlsDecryptError;
            const cleartext_len = rec.payload.len - overhead;
            if (buf.len < cleartext_len) return error.TlsCipherNoSpaceLeft;

            const ciphertext = rec.payload[0..cleartext_len];
            const auth_tag = rec.payload[cleartext_len..][0..auth_tag_len];
            const cleartext = buf[0..cleartext_len];

            const ad = additionalData(self.decrypt_seq, rec.content_type, cleartext_len);
            const iv = ivWithSeq(nonce_len, self.decrypt_iv, self.decrypt_seq);
            AeadType.decrypt(cleartext, ciphertext, auth_tag.*, &ad, iv, self.decrypt_key) catch return error.TlsDecryptError;
            self.decrypt_seq +%= 1;
            return .{ rec.content_type, cleartext };
        }
    };
}

fn Aead13Type(comptime AeadType: type, comptime Hash: type) type {
    return struct {
        const Hmac = crypto.auth.hmac.Hmac(Hash);
        const Hkdf = crypto.kdf.hkdf.Hkdf(Hmac);

        const key_len = AeadType.key_length;
        const auth_tag_len = AeadType.tag_length;
        const nonce_len = AeadType.nonce_length;
        const digest_len = Hash.digest_length;
        const encrypt_overhead = record.header_len + 1 + auth_tag_len;

        encrypt_secret: [digest_len]u8,
        decrypt_secret: [digest_len]u8,
        encrypt_key: [key_len]u8,
        decrypt_key: [key_len]u8,
        encrypt_iv: [nonce_len]u8,
        decrypt_iv: [nonce_len]u8,
        encrypt_seq: u64 = 0,
        decrypt_seq: u64 = 0,

        const Self = @This();

        pub fn init(secret: Transcript.Secret, side: proto.Side) Self {
            var self = Self{
                .encrypt_secret = if (side == .client) secret.client[0..digest_len].* else secret.server[0..digest_len].*,
                .decrypt_secret = if (side == .server) secret.client[0..digest_len].* else secret.server[0..digest_len].*,
                .encrypt_key = undefined,
                .decrypt_key = undefined,
                .encrypt_iv = undefined,
                .decrypt_iv = undefined,
            };
            self.keyGenerate();
            return self;
        }

        fn keyGenerate(self: *Self) void {
            self.encrypt_key = hkdfExpandLabel(Hkdf, self.encrypt_secret, "key", "", key_len);
            self.decrypt_key = hkdfExpandLabel(Hkdf, self.decrypt_secret, "key", "", key_len);
            self.encrypt_iv = hkdfExpandLabel(Hkdf, self.encrypt_secret, "iv", "", nonce_len);
            self.decrypt_iv = hkdfExpandLabel(Hkdf, self.decrypt_secret, "iv", "", nonce_len);
        }

        pub fn keyUpdateEncrypt(self: *Self) void {
            self.encrypt_secret = hkdfExpandLabel(Hkdf, self.encrypt_secret, "traffic upd", "", digest_len);
            self.encrypt_seq = 0;
            self.keyGenerate();
        }

        pub fn keyUpdateDecrypt(self: *Self) void {
            self.decrypt_secret = hkdfExpandLabel(Hkdf, self.decrypt_secret, "traffic upd", "", digest_len);
            self.decrypt_seq = 0;
            self.keyGenerate();
        }

        pub fn encrypt(
            self: *Self,
            buf: []u8,
            content_type: proto.ContentType,
            cleartext: []const u8,
        ) ![]const u8 {
            const payload_len = cleartext.len + 1 + auth_tag_len;
            const record_len = record.header_len + payload_len;
            if (buf.len < record_len) return error.TlsCipherNoSpaceLeft;

            const header = buf[0..record.header_len];
            header.* = record.header(.application_data, payload_len);

            if (&cleartext[0] != &buf[record.header_len]) {
                @memcpy(buf[record.header_len..][0..cleartext.len], cleartext);
            }
            buf[record.header_len + cleartext.len] = @intFromEnum(content_type);
            const ciphertext = buf[record.header_len..][0 .. cleartext.len + 1];
            const auth_tag = buf[record.header_len + ciphertext.len ..][0..auth_tag_len];

            const iv = ivWithSeq(nonce_len, self.encrypt_iv, self.encrypt_seq);
            AeadType.encrypt(ciphertext, auth_tag, ciphertext, header, iv, self.encrypt_key);
            self.encrypt_seq +%= 1;
            return buf[0..record_len];
        }

        pub fn recordLen(_: Self, cleartext_len: usize) usize {
            const payload_len = cleartext_len + 1 + auth_tag_len;
            return record.header_len + payload_len;
        }

        pub fn decrypt(
            self: *Self,
            buf: []u8,
            rec: Record,
        ) !struct { proto.ContentType, []u8 } {
            const overhead = auth_tag_len + 1;
            if (rec.payload.len < overhead) return error.TlsDecryptError;
            const ciphertext_len = rec.payload.len - auth_tag_len;
            if (ciphertext_len > max_cleartext_len + 1) return error.TlsRecordOverflow;
            if (buf.len < ciphertext_len) return error.TlsCipherNoSpaceLeft;

            const ciphertext = rec.payload[0..ciphertext_len];
            const auth_tag = rec.payload[ciphertext_len..][0..auth_tag_len];

            const iv = ivWithSeq(nonce_len, self.decrypt_iv, self.decrypt_seq);
            AeadType.decrypt(buf[0..ciphertext_len], ciphertext, auth_tag.*, rec.header, iv, self.decrypt_key) catch return error.TlsBadRecordMac;

            var content_type_idx: usize = ciphertext_len - 1;
            while (buf[content_type_idx] == 0 and content_type_idx > 0) : (content_type_idx -= 1) {}

            const cleartext = buf[0..content_type_idx];
            const content_type: proto.ContentType = @enumFromInt(buf[content_type_idx]);
            self.decrypt_seq +%= 1;
            return .{ content_type, cleartext };
        }
    };
}

fn CbcType(comptime BlockCipher: type, comptime HashType: type) type {
    const CBC = @import("cbc/main.zig").CBC(BlockCipher);
    return struct {
        const mac_len = HashType.digest_length;
        const key_len = BlockCipher.key_bits / 8;
        const iv_len = 16;
        const encrypt_overhead = record.header_len + iv_len + mac_len + max_padding;

        const Hmac = crypto.auth.hmac.Hmac(HashType);
        const paddedLength = CBC.paddedLength;
        const max_padding = 16;

        encrypt_secret: [mac_len]u8,
        decrypt_secret: [mac_len]u8,
        encrypt_key: [key_len]u8,
        decrypt_key: [key_len]u8,
        encrypt_seq: u64 = 0,
        decrypt_seq: u64 = 0,
        rng: std.Random,

        const Self = @This();

        fn init(key_material: []const u8, side: proto.Side, rng: std.Random) Self {
            const client_secret = key_material[0..mac_len].*;
            const server_secret = key_material[mac_len..][0..mac_len].*;
            const client_key = key_material[2 * mac_len ..][0..key_len].*;
            const server_key = key_material[2 * mac_len + key_len ..][0..key_len].*;
            return .{
                .encrypt_secret = if (side == .client) client_secret else server_secret,
                .decrypt_secret = if (side == .client) server_secret else client_secret,
                .encrypt_key = if (side == .client) client_key else server_key,
                .decrypt_key = if (side == .client) server_key else client_key,
                .rng = rng,
            };
        }

        pub fn encrypt(
            self: *Self,
            buf: []u8,
            content_type: proto.ContentType,
            cleartext: []const u8,
        ) ![]const u8 {
            if (buf.len < self.recordLen(cleartext.len)) return error.TlsCipherNoSpaceLeft;
            const cleartext_idx = record.header_len + iv_len;
            @memcpy(buf[cleartext_idx..][0..cleartext.len], cleartext);

            {
                const ad = additionalData(self.encrypt_seq, content_type, cleartext.len);
                const mac_msg = buf[cleartext_idx - ad.len ..][0 .. ad.len + cleartext.len];
                @memcpy(mac_msg[0..ad.len], &ad);
                const mac = buf[cleartext_idx + cleartext.len ..][0..mac_len];
                Hmac.create(mac, mac_msg, &self.encrypt_secret);
                self.encrypt_seq +%= 1;
            }

            const unpadded_len = cleartext.len + mac_len;
            const padded_len = paddedLength(unpadded_len);
            const plaintext = buf[cleartext_idx..][0..unpadded_len];
            const ciphertext = buf[cleartext_idx..][0..padded_len];

            buf[0..record.header_len].* = record.header(content_type, iv_len + ciphertext.len);
            const iv = buf[record.header_len..][0..iv_len];
            self.rng.bytes(iv);

            CBC.init(self.encrypt_key).encrypt(ciphertext, plaintext, iv[0..iv_len].*);

            return buf[0 .. record.header_len + iv_len + ciphertext.len];
        }

        pub fn recordLen(_: Self, cleartext_len: usize) usize {
            const unpadded_len = cleartext_len + mac_len;
            const padded_len = paddedLength(unpadded_len);
            return record.header_len + iv_len + padded_len;
        }

        pub fn decrypt(
            self: *Self,
            buf: []u8,
            rec: Record,
        ) !struct { proto.ContentType, []u8 } {
            if (rec.payload.len < iv_len + mac_len + 1) return error.TlsDecryptError;

            const iv = rec.payload[0..iv_len];
            const ciphertext = rec.payload[iv_len..];

            if (buf.len < ciphertext.len + additional_data_len) return error.TlsCipherNoSpaceLeft;
            const plaintext = buf[additional_data_len..][0..ciphertext.len];
            CBC.init(self.decrypt_key).decrypt(plaintext, ciphertext, iv[0..iv_len].*) catch return error.TlsDecryptError;

            const padding_len = plaintext[plaintext.len - 1] + 1;
            if (plaintext.len < mac_len + padding_len) return error.TlsDecryptError;
            const cleartext_len = plaintext.len - mac_len - padding_len;
            const cleartext = plaintext[0..cleartext_len];
            const mac = plaintext[cleartext_len..][0..mac_len];

            var ad = additionalData(self.decrypt_seq, rec.content_type, cleartext_len);
            @memcpy(buf[0..ad.len], &ad);
            const mac_msg = buf[0 .. ad.len + cleartext_len];
            self.decrypt_seq +%= 1;

            var expected_mac: [mac_len]u8 = undefined;
            Hmac.create(&expected_mac, mac_msg, &self.decrypt_secret);
            if (!std.mem.eql(u8, &expected_mac, mac))
                return error.TlsBadRecordMac;

            return .{ rec.content_type, cleartext };
        }
    };
}

fn ivWithSeq(comptime nonce_len: usize, iv: [nonce_len]u8, seq: u64) [nonce_len]u8 {
    var res = iv;
    const buf = res[nonce_len - 8 ..];
    const operand = std.mem.readInt(u64, buf, .big);
    std.mem.writeInt(u64, buf, operand ^ seq, .big);
    return res;
}

pub const additional_data_len = record.header_len + @sizeOf(u64);

fn additionalData(seq: u64, content_type: proto.ContentType, payload_len: usize) [additional_data_len]u8 {
    const header = record.header(content_type, payload_len);
    var seq_buf: [8]u8 = undefined;
    std.mem.writeInt(u64, &seq_buf, seq, .big);
    return seq_buf ++ header;
}

pub const cipher_suites = struct {
    pub const tls12_secure = if (crypto.core.aes.has_hardware_support) [_]CipherSuite{
        .ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        .ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
        .ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
        .ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        .ECDHE_RSA_WITH_AES_256_GCM_SHA384,
        .ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
    } else [_]CipherSuite{
        .ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256,
        .ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
        .ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,

        .ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256,
        .ECDHE_RSA_WITH_AES_128_GCM_SHA256,
        .ECDHE_RSA_WITH_AES_256_GCM_SHA384,
    };
    pub const tls12_weak = [_]CipherSuite{
        .ECDHE_ECDSA_WITH_AES_128_CBC_SHA256,
        .ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
        .ECDHE_ECDSA_WITH_AES_128_CBC_SHA,
        .ECDHE_RSA_WITH_AES_128_CBC_SHA256,
        .ECDHE_RSA_WITH_AES_256_CBC_SHA384,
        .ECDHE_RSA_WITH_AES_128_CBC_SHA,

        .RSA_WITH_AES_128_CBC_SHA256,
        .RSA_WITH_AES_128_CBC_SHA,
    };
    pub const tls13_ = if (crypto.core.aes.has_hardware_support) [_]CipherSuite{
        .AES_128_GCM_SHA256,
        .AES_256_GCM_SHA384,
        .CHACHA20_POLY1305_SHA256,
    } else [_]CipherSuite{
        .CHACHA20_POLY1305_SHA256,
        .AES_128_GCM_SHA256,
        .AES_256_GCM_SHA384,
    };

    pub const tls13 = &tls13_;
    pub const tls12 = &(tls12_secure ++ tls12_weak);
    pub const secure = &(tls13_ ++ tls12_secure);
    pub const all = &(tls13_ ++ tls12_secure ++ tls12_weak);

    pub fn includes(list: []const CipherSuite, cs: CipherSuite) bool {
        for (list) |s| {
            if (cs == s) return true;
        }
        return false;
    }
};

pub const CipherSuite = enum(u16) {
    ECDHE_ECDSA_WITH_AES_128_CBC_SHA = 0xc009,
    ECDHE_RSA_WITH_AES_128_CBC_SHA = 0xc013,
    RSA_WITH_AES_128_CBC_SHA = 0x002F,
    ECDHE_ECDSA_WITH_AES_128_CBC_SHA256 = 0xc023,
    ECDHE_RSA_WITH_AES_128_CBC_SHA256 = 0xc027,
    RSA_WITH_AES_128_CBC_SHA256 = 0x003c,
    ECDHE_ECDSA_WITH_AES_256_CBC_SHA384 = 0xc024,
    ECDHE_RSA_WITH_AES_256_CBC_SHA384 = 0xc028,
    ECDHE_ECDSA_WITH_AES_128_GCM_SHA256 = 0xc02b,
    ECDHE_ECDSA_WITH_AES_256_GCM_SHA384 = 0xc02c,
    ECDHE_RSA_WITH_AES_128_GCM_SHA256 = 0xc02f,
    ECDHE_RSA_WITH_AES_256_GCM_SHA384 = 0xc030,
    ECDHE_ECDSA_WITH_CHACHA20_POLY1305_SHA256 = 0xcca9,
    ECDHE_RSA_WITH_CHACHA20_POLY1305_SHA256 = 0xcca8,
    AES_128_GCM_SHA256 = 0x1301,
    AES_256_GCM_SHA384 = 0x1302,
    CHACHA20_POLY1305_SHA256 = 0x1303,
    AEGIS_128L_SHA256 = 0x1307,
    _,

    pub fn validate(cs: CipherSuite) !void {
        if (cipher_suites.includes(cipher_suites.tls12, cs)) return;
        if (cipher_suites.includes(cipher_suites.tls13, cs)) return;
        return error.TlsIllegalParameter;
    }

    pub const Versions = enum {
        both,
        tls_1_3,
        tls_1_2,
    };

    pub fn versions(list: []const CipherSuite) !Versions {
        var has_12 = false;
        var has_13 = false;
        for (list) |cs| {
            if (cipher_suites.includes(cipher_suites.tls12, cs)) {
                has_12 = true;
            } else {
                if (cipher_suites.includes(cipher_suites.tls13, cs)) has_13 = true;
            }
        }
        if (has_12 and has_13) return .both;
        if (has_12) return .tls_1_2;
        if (has_13) return .tls_1_3;
        return error.TlsIllegalParameter;
    }

    pub const KeyExchangeAlgorithm = enum {
        ecdhe,
        rsa,
    };

    pub fn keyExchange(s: CipherSuite) KeyExchangeAlgorithm {
        return switch (s) {
            .RSA_WITH_AES_128_CBC_SHA,
            .RSA_WITH_AES_128_CBC_SHA256,
            => .rsa,
            else => .ecdhe,
        };
    }

    pub const HashTag = enum {
        sha256,
        sha384,
        sha512,
    };

    pub fn hash(cs: CipherSuite) HashTag {
        return switch (cs) {
            .ECDHE_RSA_WITH_AES_256_CBC_SHA384,
            .ECDHE_RSA_WITH_AES_256_GCM_SHA384,
            .ECDHE_ECDSA_WITH_AES_256_CBC_SHA384,
            .ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
            .AES_256_GCM_SHA384,
            => .sha384,
            else => .sha256,
        };
    }
};

const testing = std.testing;
const testu = @import("testu.zig");

test "CipherSuite validate" {
    {
        const cs: CipherSuite = .AES_256_GCM_SHA384;
        try cs.validate();
        try testing.expectEqual(cs.hash(), .sha384);
        try testing.expectEqual(cs.keyExchange(), .ecdhe);
    }
    {
        const cs: CipherSuite = .AES_128_GCM_SHA256;
        try cs.validate();
        try testing.expectEqual(.sha256, cs.hash());
        try testing.expectEqual(.ecdhe, cs.keyExchange());
    }
    for (cipher_suites.tls12) |cs| {
        try cs.validate();
        _ = cs.hash();
        _ = cs.keyExchange();
    }
}

test "CipherSuite versions" {
    try testing.expectEqual(.tls_1_3, CipherSuite.versions(&[_]CipherSuite{.AES_128_GCM_SHA256}));
    try testing.expectEqual(.both, CipherSuite.versions(&[_]CipherSuite{ .AES_128_GCM_SHA256, .ECDHE_ECDSA_WITH_AES_128_CBC_SHA }));
    try testing.expectEqual(.tls_1_2, CipherSuite.versions(&[_]CipherSuite{.RSA_WITH_AES_128_CBC_SHA}));
}

test "gcm 1.2 encrypt overhead" {
    inline for ([_]type{
        Aead12Aes128Gcm,
        Aead12Aes256Gcm,
    }) |T| {
        {
            const expected_key_len = switch (T) {
                Aead12Aes128Gcm => 16,
                Aead12Aes256Gcm => 32,
                else => unreachable,
            };
            try testing.expectEqual(expected_key_len, T.key_len);
            try testing.expectEqual(16, T.auth_tag_len);
            try testing.expectEqual(12, T.nonce_len);
            try testing.expectEqual(4, T.iv_len);
            try testing.expectEqual(29, T.encrypt_overhead);
        }
    }
}

test "cbc 1.2 encrypt overhead" {
    try testing.expectEqual(85, encrypt_overhead_tls_12);

    inline for ([_]type{
        CbcAes128Sha1,
        CbcAes128Sha256,
        CbcAes256Sha384,
    }) |T| {
        switch (T) {
            CbcAes128Sha1 => {
                try testing.expectEqual(20, T.mac_len);
                try testing.expectEqual(16, T.key_len);
                try testing.expectEqual(57, T.encrypt_overhead);
            },
            CbcAes128Sha256 => {
                try testing.expectEqual(32, T.mac_len);
                try testing.expectEqual(16, T.key_len);
                try testing.expectEqual(69, T.encrypt_overhead);
            },
            CbcAes256Sha384 => {
                try testing.expectEqual(48, T.mac_len);
                try testing.expectEqual(32, T.key_len);
                try testing.expectEqual(85, T.encrypt_overhead);
            },
            else => unreachable,
        }
        try testing.expectEqual(16, T.paddedLength(1));
        try testing.expectEqual(16, T.iv_len);
    }
}

test "overhead tls 1.3" {
    try testing.expectEqual(22, encrypt_overhead_tls_13);
    try expectOverhead(Aes128GcmSha256, 16, 16, 12, 22);
    try expectOverhead(Aes256GcmSha384, 32, 16, 12, 22);
    try expectOverhead(ChaChaSha256, 32, 16, 12, 22);
    try expectOverhead(Aegis128Sha256, 16, 16, 16, 22);
    try expectOverhead(Aead12ChaCha, 32, 16, 12, 21);
}

fn expectOverhead(T: type, key_len: usize, auth_tag_len: usize, nonce_len: usize, overhead: usize) !void {
    try testing.expectEqual(key_len, T.key_len);
    try testing.expectEqual(auth_tag_len, T.auth_tag_len);
    try testing.expectEqual(nonce_len, T.nonce_len);
    try testing.expectEqual(overhead, T.encrypt_overhead);
}

test "client/server encryption tls 1.3" {
    inline for (cipher_suites.tls13) |cs| {
        var buf: [256]u8 = undefined;
        testu.fill(&buf);
        const secret = Transcript.Secret{
            .client = buf[0..128],
            .server = buf[128..],
        };
        var client_cipher = try Cipher.initTls13(cs, secret, .client);
        var server_cipher = try Cipher.initTls13(cs, secret, .server);
        try encryptDecrypt(&client_cipher, &server_cipher);

        try client_cipher.keyUpdateEncrypt();
        try server_cipher.keyUpdateDecrypt();
        try encryptDecrypt(&client_cipher, &server_cipher);

        try client_cipher.keyUpdateDecrypt();
        try server_cipher.keyUpdateEncrypt();
        try encryptDecrypt(&client_cipher, &server_cipher);
    }
}

test "client/server encryption tls 1.2" {
    const rng_impl: std.Random.IoSource = .{ .io = testing.io };
    const rng = rng_impl.interface();
    inline for (cipher_suites.tls12) |cs| {
        var key_material: [256]u8 = undefined;
        testu.fill(&key_material);
        var client_cipher = try Cipher.initTls12(cs, &key_material, .client, rng);
        var server_cipher = try Cipher.initTls12(cs, &key_material, .server, rng);
        try encryptDecrypt(&client_cipher, &server_cipher);
    }
}

fn encryptDecrypt(client_cipher: *Cipher, server_cipher: *Cipher) !void {
    const cleartext =
        \\ Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed do
        \\ eiusmod tempor incididunt ut labore et dolore magna aliqua.
    ;
    var buf: [256]u8 = undefined;

    {
        const encrypted = try client_cipher.encrypt(&buf, .application_data, cleartext);
        const expected_encrypted_len = switch (client_cipher.*) {
            inline else => |f| brk: {
                const T = @TypeOf(f);
                break :brk switch (T) {
                    CbcAes128Sha1,
                    CbcAes128Sha256,
                    CbcAes256Sha384,
                    => record.header_len + T.paddedLength(T.iv_len + cleartext.len + T.mac_len),
                    Aead12Aes128Gcm,
                    Aead12Aes256Gcm,
                    Aead12ChaCha,
                    Aes128GcmSha256,
                    Aes256GcmSha384,
                    ChaChaSha256,
                    Aegis128Sha256,
                    => cleartext.len + T.encrypt_overhead,
                    else => unreachable,
                };
            },
        };
        try testing.expectEqual(client_cipher.recordLen(cleartext.len), encrypted.len);
        try testing.expectEqual(expected_encrypted_len, encrypted.len);
        const content_type, const decrypted = try server_cipher.decrypt(&buf, Record.init(encrypted));
        try testing.expectEqualSlices(u8, cleartext, decrypted);
        try testing.expectEqual(.application_data, content_type);
    }
    {
        const encrypted = try server_cipher.encrypt(&buf, .application_data, cleartext);
        const content_type, const decrypted = try client_cipher.decrypt(&buf, Record.init(encrypted));
        try testing.expectEqualSlices(u8, cleartext, decrypted);
        try testing.expectEqual(.application_data, content_type);
    }
}

const data13 = @import("testdata/tls13.zig");

pub fn testCiphers() struct { Cipher, Cipher } {
    const client_cipher: Cipher = .{ .AES_256_GCM_SHA384 = .{
        .encrypt_secret = undefined,
        .decrypt_secret = undefined,
        .encrypt_key = data13.client_application_key,
        .decrypt_key = data13.server_application_key,
        .encrypt_iv = data13.client_application_iv,
        .decrypt_iv = data13.server_application_iv,
    } };
    const server_cipher: Cipher = .{ .AES_256_GCM_SHA384 = .{
        .encrypt_secret = undefined,
        .decrypt_secret = undefined,
        .encrypt_key = data13.server_application_key,
        .decrypt_key = data13.client_application_key,
        .encrypt_iv = data13.server_application_iv,
        .decrypt_iv = data13.client_application_iv,
    } };
    return .{ client_cipher, server_cipher };
}

test testCiphers {
    var client_cipher, var server_cipher = testCiphers();
    var buffer: [1024]u8 = undefined;
    {
        const payload = "ping";
        const ciphertext = try client_cipher.encrypt(&buffer, .application_data, payload);
        try testing.expectEqualSlices(u8, &data13.client_ping_wrapped, ciphertext);

        const content_type, const cleartext = try server_cipher.decrypt(&buffer, record.Record.init(ciphertext));
        try testing.expectEqualStrings(payload, cleartext);
        try testing.expectEqual(.application_data, content_type);
    }
    {
        const close_notify = &proto.Alert.closeNotify();
        const ciphertext = try client_cipher.encrypt(&buffer, .alert, close_notify);

        const content_type, const cleartext = try server_cipher.decrypt(&buffer, record.Record.init(ciphertext));
        try testing.expectEqualStrings(close_notify, cleartext);
        try testing.expectEqual(.alert, content_type);
    }
}
