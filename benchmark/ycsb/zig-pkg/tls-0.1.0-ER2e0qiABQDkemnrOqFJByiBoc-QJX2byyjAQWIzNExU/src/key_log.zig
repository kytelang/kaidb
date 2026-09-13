
const std = @import("std");

const key_log_file_env = "SSLKEYLOGFILE";

pub const label = struct {
    pub const client_handshake_traffic_secret: []const u8 = "CLIENT_HANDSHAKE_TRAFFIC_SECRET";
    pub const server_handshake_traffic_secret: []const u8 = "SERVER_HANDSHAKE_TRAFFIC_SECRET";
    pub const client_traffic_secret_0: []const u8 = "CLIENT_TRAFFIC_SECRET_0";
    pub const server_traffic_secret_0: []const u8 = "SERVER_TRAFFIC_SECRET_0";
    pub const client_random: []const u8 = "CLIENT_RANDOM";
};

var environ: std.process.Environ = .empty;

pub const Callback = *const fn (label: []const u8, client_random: []const u8, secret: []const u8) void;

pub fn init(env: std.process.Environ) Callback {
    environ = env;
    return callback;
}

fn callback(label_: []const u8, client_random: []const u8, secret: []const u8) void {
    const builtin = @import("builtin");
    const native_os = builtin.os.tag;
    const file_name: []const u8 = if (native_os == .windows)
        return
    else if (native_os == .wasi and !builtin.link_libc)
        return
    else
        environ.getPosix(key_log_file_env) orelse return;
    fileAppend(file_name, label_, client_random, secret) catch return;
}

fn fileAppend(file_name: []const u8, label_: []const u8, client_random: []const u8, secret: []const u8) !void {
    var buf: [1024]u8 = undefined;
    const line = try formatLine(&buf, label_, client_random, secret);
    try fileWrite(file_name, line);
}

fn fileWrite(file_name: []const u8, line: []const u8) !void {
    var threaded: std.Io.Threaded = .init_single_threaded;
    const io = threaded.io();

    var file = try std.Io.Dir.createFileAbsolute(io, file_name, .{ .truncate = false });
    defer file.close(io);
    const stat = try file.stat(io);
    try file.writePositionalAll(io, line, stat.size);
}

fn formatLine(buf: []u8, label_: []const u8, client_random: []const u8, secret: []const u8) ![]const u8 {
    var w = std.Io.Writer.fixed(buf);
    try w.print("{s} ", .{label_});
    for (client_random) |b| {
        try w.print("{x:0>2}", .{b});
    }
    try w.writeByte(' ');
    for (secret) |b| {
        try w.print("{x:0>2}", .{b});
    }
    try w.writeByte('\n');
    return w.buffered();
}
