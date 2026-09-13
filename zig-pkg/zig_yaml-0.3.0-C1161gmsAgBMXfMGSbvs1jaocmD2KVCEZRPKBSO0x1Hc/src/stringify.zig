const std = @import("std");

const Yaml = @import("Yaml.zig");

pub fn stringify(gpa: std.mem.Allocator, input: anytype, writer: *std.Io.Writer) Yaml.StringifyError!void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();

    const maybe_value = try Yaml.Value.encode(arena.allocator(), input);

    if (maybe_value) |value| {
        try value.stringify(writer, .{});
    }
}
