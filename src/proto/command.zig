//! Server-side substitution of bound parameters into a SQL statement string.
//!
//! The binary wire protocol lets a client send a parameterised statement
//! (`... WHERE id = $1 AND name = $2`) together with a positional list of
//! argument values. kaidb's SQL parser does not itself carry placeholders all
//! the way through the executor, so the protocol layer resolves the placeholders
//! into concrete SQL literals BEFORE the text reaches the parser. This module is
//! that resolution step: [`substituteParams`] rewrites every `$N` into the
//! literal form of the Nth argument.
//!
//! Because the rewrite happens by string splicing, it is also the security
//! boundary against SQL injection. The safety does not come from scanning for
//! dangerous substrings (a blocklist); it comes from CLASSIFYING each argument
//! and emitting it in a form the SQL grammar cannot escape:
//!
//!   * [`ParamClass.text`] values are wrapped in single quotes with every
//!     embedded `'` doubled, so the argument can only ever be one string literal
//!     no matter what it contains (the classic `x'; DROP TABLE users; --` stays
//!     inert inside the quotes).
//!   * [`ParamClass.numeric`] values are emitted RAW (unquoted), so they are
//!     validated by [`isNumericLiteral`] first and a non-numeric argument is
//!     rejected with `error.InvalidNumericParam` rather than spliced in. This is
//!     the one class where an attacker-controlled string would otherwise land
//!     outside quotes.
//!   * [`ParamClass.boolean`] values are canonicalised to the keywords `true` or
//!     `false`, so nothing of the argument survives into the SQL except one of
//!     two fixed tokens.
//!
//! A `null` argument (represented as a `null` slice) becomes the SQL keyword
//! `NULL` regardless of class. Placeholders are 1-based; `$N` where `N` is out
//! of range for the argument list is left untouched in the output (it is treated
//! as literal statement text, not an error), which keeps a stray `$` in the SQL
//! from being mangled.
//!
//! The function is allocation-owning: it returns a freshly allocated `[]u8` the
//! caller must free, and it frees its own working buffer on any error via
//! `errdefer`.

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

/// Failure modes of [`substituteParams`].
///
/// `InvalidNumericParam` is raised when a value tagged [`ParamClass.numeric`]
/// is not a valid numeric literal (see [`isNumericLiteral`]), refusing to
/// splice it unquoted is what stops a fake-numeric argument from injecting SQL.
/// `ParamIndexOutOfRange` is part of the declared set for callers/future use;
/// the current splicing path leaves an out-of-range `$N` as literal text rather
/// than raising it. `Allocator.Error` is folded in because the output and its
/// backing buffer are heap-allocated.
pub const SubstituteError = error{ InvalidNumericParam, ParamIndexOutOfRange } || Allocator.Error;

/// How a bound parameter's value is rendered into SQL text.
///
/// The class chosen for each argument determines the escaping rule and therefore
/// the injection guarantee; see the module header for the per-class contract.
pub const ParamClass = enum {
    /// Rendered as a single-quoted string literal with `'` doubled; safe for
    /// arbitrary bytes.
    text,
    /// Rendered raw/unquoted; the value must pass [`isNumericLiteral`] or the
    /// substitution fails closed.
    numeric,
    /// Rendered as the keyword `true` or `false`; any non-truthy spelling maps
    /// to `false`.
    boolean,
};

/// Rewrites every `$N` placeholder in `sql` with the literal form of the Nth
/// value in `params`, returning a newly allocated statement string.
///
/// `params[k]` supplies the value for `$k+1` (placeholders are 1-based) and
/// `classes[k]` its [`ParamClass`]; a `null` entry in `params` renders as
/// `NULL`. If `classes` is shorter than the referenced index the class defaults
/// to [`ParamClass.text`] (the safe, always-quoted choice). A `$` not followed
/// by a digit, or `$N` whose `N` is 0 or greater than `params.len`, is copied
/// through verbatim so ordinary text containing `$` is preserved.
///
/// The returned slice is owned by the caller (free with the same allocator).
/// Fails with `error.InvalidNumericParam` if a numeric-classed value is not a
/// valid numeric literal, or with an allocator error; on any failure the
/// partially built buffer is released and nothing leaks. Delegates the
/// per-value rendering to [`appendParam`].
pub fn substituteParams(
    a: Allocator,
    sql: []const u8,
    params: []const ?[]const u8,
    classes: []const ParamClass,
) SubstituteError![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(a);
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '$' and i + 1 < sql.len and std.ascii.isDigit(sql[i + 1])) {
            var j = i + 1;
            var idx: usize = 0;
            while (j < sql.len and std.ascii.isDigit(sql[j])) : (j += 1) {
                idx = idx * 10 + (sql[j] - '0');
            }
            if (idx >= 1 and idx <= params.len) {
                const class = if (idx - 1 < classes.len) classes[idx - 1] else .text;
                try appendParam(a, &out, params[idx - 1], class);
                i = j;
                continue;
            }
        }
        try out.append(a, ch);
        i += 1;
    }
    return out.toOwnedSlice(a);
}

/// Appends the SQL literal form of one bound value `p` to `out`.
///
/// Implements the per-class escaping rules described in the module header: a
/// `null` `p` appends `NULL`; [`ParamClass.text`] appends a single-quoted
/// literal with every interior `'` doubled (SQL's own escape, so the value can
/// never break out of the string); [`ParamClass.numeric`] validates via
/// [`isNumericLiteral`] and appends the digits raw, failing with
/// `error.InvalidNumericParam` otherwise; [`ParamClass.boolean`] appends `true`
/// only when `p` spells a recognised truthy token (`true`/`t`/`1`) and `false`
/// for anything else. Callers rely on the numeric branch being the sole place an
/// unquoted argument reaches the output.
fn appendParam(a: Allocator, out: *std.ArrayList(u8), p: ?[]const u8, class: ParamClass) SubstituteError!void {
    const v = p orelse {
        try out.appendSlice(a, "NULL");
        return;
    };
    switch (class) {
        .text => {
            try out.append(a, '\'');
            for (v) |c| {
                if (c == '\'') try out.append(a, '\'');
                try out.append(a, c);
            }
            try out.append(a, '\'');
        },
        .numeric => {
            if (!isNumericLiteral(v)) return error.InvalidNumericParam;
            try out.appendSlice(a, v);
        },
        .boolean => {
            const is_true = mem.eql(u8, v, "true") or mem.eql(u8, v, "t") or mem.eql(u8, v, "1");
            try out.appendSlice(a, if (is_true) "true" else "false");
        },
    }
}

/// Reports whether `v` is safe to splice unquoted as a numeric literal.
///
/// This is a whitelist, not a blocklist: it accepts only the characters that can
/// appear in an integer, decimal, or scientific-notation number (`0`-`9`, `.`,
/// `e`/`E`, and sign characters `+`/`-`) and requires at least one digit to be
/// present, so an empty string or a value carrying letters, quotes, whitespace,
/// or statement separators is rejected. It does not attempt to validate the
/// grammar (multiple dots or misplaced signs pass); its job is only to guarantee
/// that no character capable of ending the literal and starting new SQL survives,
/// which is what makes raw insertion by [`appendParam`] injection-safe.
fn isNumericLiteral(v: []const u8) bool {
    if (v.len == 0) return false;
    var seen_digit = false;
    for (v, 0..) |c, i| {
        switch (c) {
            '0'...'9' => seen_digit = true,
            '.', 'e', 'E' => {},
            '+', '-' => {},
            else => return false,
        }
        _ = i;
    }
    return seen_digit;
}

const testing = std.testing;

test "text params are quoted and escaped" {
    const a = testing.allocator;
    const params = [_]?[]const u8{"O'Brien"};
    const classes = [_]ParamClass{.text};
    const out = try substituteParams(a, "SELECT * FROM t WHERE name = $1", &params, &classes);
    defer a.free(out);
    try testing.expectEqualStrings("SELECT * FROM t WHERE name = 'O''Brien'", out);
}

test "numeric params inserted raw; NULL becomes NULL" {
    const a = testing.allocator;
    const params = [_]?[]const u8{ "42", null };
    const classes = [_]ParamClass{ .numeric, .text };
    const out = try substituteParams(a, "WHERE id = $1 AND note = $2", &params, &classes);
    defer a.free(out);
    try testing.expectEqualStrings("WHERE id = 42 AND note = NULL", out);
}

test "injection via a fake-numeric param is rejected" {
    const a = testing.allocator;
    const params = [_]?[]const u8{"1; DROP TABLE users"};
    const classes = [_]ParamClass{.numeric};
    try testing.expectError(error.InvalidNumericParam, substituteParams(a, "WHERE id = $1", &params, &classes));
}

test "injection via a text param is neutralized (stays a string literal)" {
    const a = testing.allocator;
    const params = [_]?[]const u8{"x'; DROP TABLE users; --"};
    const classes = [_]ParamClass{.text};
    const out = try substituteParams(a, "WHERE name = $1", &params, &classes);
    defer a.free(out);
    try testing.expectEqualStrings("WHERE name = 'x''; DROP TABLE users; --'", out);
}

test "multi-digit and repeated params" {
    const a = testing.allocator;
    const params = [_]?[]const u8{ "a", "b" };
    const classes = [_]ParamClass{ .text, .text };
    const out = try substituteParams(a, "$1 $2 $1", &params, &classes);
    defer a.free(out);
    try testing.expectEqualStrings("'a' 'b' 'a'", out);
}
