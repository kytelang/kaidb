const std = @import("std");
const assert = std.debug.assert;
const mem = std.mem;

const Allocator = mem.Allocator;
const Token = @import("Tokenizer.zig").Token;
const Tree = @This();

source: []const u8,
tokens: std.MultiArrayList(TokenWithLineCol).Slice,
docs: []const Node.Index,
nodes: std.MultiArrayList(Node).Slice,
extra: []const u32,
string_bytes: []const u8,

pub fn deinit(self: *Tree, gpa: Allocator) void {
    self.tokens.deinit(gpa);
    self.nodes.deinit(gpa);
    gpa.free(self.docs);
    gpa.free(self.extra);
    gpa.free(self.string_bytes);
    self.* = undefined;
}

pub fn nodeTag(tree: Tree, node: Node.Index) Node.Tag {
    return tree.nodes.items(.tag)[@intFromEnum(node)];
}

pub fn nodeData(tree: Tree, node: Node.Index) Node.Data {
    return tree.nodes.items(.data)[@intFromEnum(node)];
}

pub fn nodeScope(tree: Tree, node: Node.Index) Node.Scope {
    return tree.nodes.items(.scope)[@intFromEnum(node)];
}

pub fn extraData(tree: Tree, comptime T: type, index: Extra) struct { data: T, end: Extra } {
    const fields = std.meta.fields(T);
    var i = @intFromEnum(index);
    var result: T = undefined;
    inline for (fields) |field| {
        @field(result, field.name) = switch (field.type) {
            u32 => tree.extra[i],
            i32 => @bitCast(tree.extra[i]),
            Node.Index, Node.OptionalIndex, Token.Index => @enumFromInt(tree.extra[i]),
            else => @compileError("bad field type: " ++ @typeName(field.type)),
        };
        i += 1;
    }
    return .{
        .data = result,
        .end = @enumFromInt(i),
    };
}

pub fn directive(self: Tree, node_index: Node.Index) ?[]const u8 {
    const tag = self.nodeTag(node_index);
    switch (tag) {
        .doc => return null,
        .doc_with_directive => {
            const data = self.nodeData(node_index).doc_with_directive;
            return self.rawString(data.directive, data.directive);
        },
        else => unreachable,
    }
}

pub fn rawString(self: Tree, start: Token.Index, end: Token.Index) []const u8 {
    const start_token = self.token(start);
    const end_token = self.token(end);
    return self.source[start_token.loc.start..end_token.loc.end];
}

pub fn token(self: Tree, index: Token.Index) Token {
    return self.tokens.items(.token)[@intFromEnum(index)];
}

pub const Node = struct {
    tag: Tag,
    scope: Scope,
    data: Data,

    pub const Tag = enum(u8) {
        doc,

        doc_with_directive,

        map_single,

        map_many,

        list_empty,

        list_one,

        list_two,

        list_many,

        value,

        string_value,
    };

    pub const Scope = struct {
        start: Token.Index,
        end: Token.Index,

        pub fn rawString(scope: Scope, tree: Tree) []const u8 {
            return tree.rawString(scope.start, scope.end);
        }
    };

    pub const Data = union {
        node: Index,

        maybe_node: OptionalIndex,

        doc_with_directive: struct {
            maybe_node: OptionalIndex,
            directive: Token.Index,
        },

        map: struct {
            key: Token.Index,
            maybe_node: OptionalIndex,
        },

        list: struct {
            el1: Index,
            el2: Index,
        },

        string: String,

        extra: Extra,
    };

    pub const Index = enum(u32) {
        _,

        pub fn toOptional(ind: Index) OptionalIndex {
            const result: OptionalIndex = @enumFromInt(@intFromEnum(ind));
            assert(result != .none);
            return result;
        }
    };

    pub const OptionalIndex = enum(u32) {
        none = std.math.maxInt(u32),
        _,

        pub fn unwrap(opt: OptionalIndex) ?Index {
            if (opt == .none) return null;
            return @enumFromInt(@intFromEnum(opt));
        }
    };

    comptime {
        if (!std.debug.runtime_safety) {
            assert(@sizeOf(Data) == 8);
        }
    }
};

pub const Extra = enum(u32) {
    _,
};

pub const Map = struct {
    map_len: u32,

    pub const Entry = struct {
        key: Token.Index,
        maybe_node: Node.OptionalIndex,
    };
};

pub const List = struct {
    list_len: u32,

    pub const Entry = struct {
        node: Node.Index,
    };
};

pub const String = struct {
    index: Index,
    len: u32,

    pub const Index = enum(u32) {
        _,
    };

    pub fn slice(str: String, tree: Tree) []const u8 {
        return tree.string_bytes[@intFromEnum(str.index)..][0..str.len];
    }
};

pub const LineCol = struct {
    line: u32,
    col: u32,
};

pub const TokenWithLineCol = struct {
    token: Token,
    line_col: LineCol,
};

test {
    std.testing.refAllDecls(@This());
}
