//! Fast CRC-32 (IEEE 802.3, reflected) via slicing-by-8.
//!
//! `std.hash.Crc32` (Crc32IsoHdlc) is correct but processes ONE byte per
//! iteration through a single 256-entry table, which clocks in around
//! 400 MB/s. Validating a 16 KiB page on every buffer-pool miss made that the
//! single largest per-miss cost (profiled at ~42 microseconds per page, ~92% of
//! a miss). This computes the exact same CRC-32 value - same polynomial, same
//! reflection, same init/xor - but consumes 8 bytes per iteration using eight
//! precomputed tables (the classic Intel slicing-by-8 method), which is roughly
//! 8-10x faster. Because the output is byte-identical to `std.hash.Crc32`, the
//! on-disk checksum format is unchanged: a page written with the old slow CRC
//! still validates, and vice versa. A unit test pins that equivalence.

const std = @import("std");

/// The reflected IEEE polynomial (0x04C11DB7 reflected = 0xEDB88320), the same
/// one `Crc32IsoHdlc` uses.
const POLY: u32 = 0xEDB88320;

/// Eight 256-entry tables, generated at comptime. `tables[0]` is the ordinary
/// byte table; `tables[k]` folds in a byte that is `k` positions further from
/// the end, so eight can be combined per 8-byte block.
const tables: [8][256]u32 = blk: {
    @setEvalBranchQuota(200000);
    var t: [8][256]u32 = undefined;
    var i: usize = 0;
    while (i < 256) : (i += 1) {
        var crc: u32 = @intCast(i);
        var j: usize = 0;
        while (j < 8) : (j += 1) {
            crc = if (crc & 1 != 0) (crc >> 1) ^ POLY else crc >> 1;
        }
        t[0][i] = crc;
    }
    var k: usize = 1;
    while (k < 8) : (k += 1) {
        i = 0;
        while (i < 256) : (i += 1) {
            const prev = t[k - 1][i];
            t[k][i] = (prev >> 8) ^ t[0][prev & 0xff];
        }
    }
    break :blk t;
};

/// CRC-32 of `data`, identical in value to `std.hash.Crc32.hash(0, data)`.
pub fn hash(data: []const u8) u32 {
    var crc: u32 = 0xFFFFFFFF;
    var i: usize = 0;
    // Slice-by-8: fold eight input bytes per iteration.
    while (i + 8 <= data.len) : (i += 8) {
        const lo = std.mem.readInt(u32, data[i..][0..4], .little) ^ crc;
        const hi = std.mem.readInt(u32, data[i + 4 ..][0..4], .little);
        crc = tables[7][lo & 0xff] ^
            tables[6][(lo >> 8) & 0xff] ^
            tables[5][(lo >> 16) & 0xff] ^
            tables[4][(lo >> 24) & 0xff] ^
            tables[3][hi & 0xff] ^
            tables[2][(hi >> 8) & 0xff] ^
            tables[1][(hi >> 16) & 0xff] ^
            tables[0][(hi >> 24) & 0xff];
    }
    // Tail: the ordinary byte-at-a-time loop.
    while (i < data.len) : (i += 1) {
        crc = (crc >> 8) ^ tables[0][(crc & 0xff) ^ data[i]];
    }
    return ~crc;
}

test "fast CRC-32 matches std.hash.Crc32 across sizes and content" {
    var prng = std.Random.DefaultPrng.init(0xC0FFEE);
    const rnd = prng.random();
    var buf: [17000]u8 = undefined;
    // Exercise every length class: sub-8 tails, exact 8-multiples, 16 KiB pages.
    const lens = [_]usize{ 0, 1, 7, 8, 9, 15, 16, 31, 255, 256, 4096, 16376, 16384, 17000 };
    for (lens) |n| {
        rnd.bytes(buf[0..n]);
        const want = std.hash.Crc32.hash(buf[0..n]);
        const got = hash(buf[0..n]);
        try std.testing.expectEqual(want, got);
    }
    // A few fully random lengths for good measure.
    var r: usize = 0;
    while (r < 200) : (r += 1) {
        const n = rnd.uintLessThan(usize, buf.len + 1);
        rnd.bytes(buf[0..n]);
        try std.testing.expectEqual(std.hash.Crc32.hash(buf[0..n]), hash(buf[0..n]));
    }
}
