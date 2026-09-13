//! Recursive-descent SQL parser: turns a SQL string into a NovaDB AST.
//!
//! This is the front half of the SQL engine. It sits between the [`Lexer`]
//! (which turns raw text into a flat `[]Token`) and the query executor (which
//! walks the AST this file produces). The grammar it accepts is the SQL subset
//! NovaDB actually runs: DML (`SELECT`/`INSERT`/`UPDATE`/`DELETE`), DDL
//! (`CREATE`/`DROP`/`ALTER TABLE`, indexes), transaction control
//! (`BEGIN`/`COMMIT`/`ROLLBACK`/`SAVEPOINT`/`RELEASE`), bulk `IMPORT`/`EXPORT`,
//! and access control (`CREATE USER`/`ROLE`, `LOGIN`, `GRANT`/`REVOKE`).
//!
//! ## Design decisions and invariants
//!
//! **Two-phase, not streaming.** [`Parser.init`] eagerly runs the whole lexer to
//! completion and stores the resulting token slice, terminated by an `EOF`
//! token. The parser then walks that slice with a single cursor [`Parser.pos`].
//! Because the token stream is fully materialised and always ends in `EOF`,
//! lookahead never has to guard against running off the end mid-stream: the
//! only bound check is in [`Parser.peek`], which clamps to the final (`EOF`)
//! token so a one-token lookahead at the very end is still safe.
//!
//! **Arena-owned AST, borrowed identifiers.** Every AST node is allocated from
//! [`Parser.arena`], so the entire tree is freed in one shot by
//! [`Parser.deinit`] and individual nodes are never freed piecemeal. Identifier
//! and string-literal payloads, however, are *borrowed* slices of the original
//! `source` text (via [`Parser.sliceText`]); they are not copied. The critical
//! consequence for callers: the `source` buffer must outlive the AST, because
//! table names, column names and string literals all point back into it. The
//! exceptions are the few identifiers this parser has to synthesise, for
//! example a qualified `table.column` name or an upper-cased function name,
//! which are `allocPrint`ed into the arena and so live with the tree.
//!
//! **Precedence by cascade.** Expression parsing is a classic
//! recursive-descent precedence ladder, lowest binding at the top:
//! [`Parser.parseExpr`] (`OR`) -> [`Parser.parseAnd`] (`AND`) ->
//! [`Parser.parseNot`] (prefix `NOT`) -> [`Parser.parsePredicate`] (comparisons,
//! `IS NULL`, `IN`, `LIKE`, `BETWEEN`) -> [`Parser.parseAdditive`] (`+ -`) ->
//! [`Parser.parseMul`] (`* /`) -> [`Parser.parsePrimary`] (literals, columns,
//! function calls, `CASE`, parenthesised sub-expressions and scalar subqueries).
//! Each level loops left-associatively over its own operators and defers to the
//! next tighter level for its operands.
//!
//! **Error model.** Parsing returns Zig errors, never partial trees. The common
//! failures are `error.UnexpectedToken` (the token the grammar demanded was not
//! there) and the more specific `error.ExpectedColumn`,
//! `error.ExpectedDefaultValue`, `error.ExpectedFormatKind`. Numeric literal
//! conversions use `catch unreachable` on purpose: the lexer only emits an
//! `INTEGER`/`FLOAT` token for text it already validated as numeric, so a parse
//! failure there would be a lexer bug, not malformed input.
//!
//! Keyword matching is largely token-driven (the lexer recognises reserved
//! words as dedicated token types), but a handful of contextual words that are
//! not reserved (`IF`, `EXISTS`, `TO`, `ASC`, `DESC`, `DATABASE`) arrive as
//! plain `IDENTIFIER` tokens and are matched here by case-insensitive text
//! comparison.

const std = @import("std");
/// Lexer that produces the token stream this parser consumes; driven to `EOF`
/// once up front in [`Parser.init`].
const Lexer = @import("lexer.zig").Lexer;
/// A single lexed token: a [`TokenType`] tag plus a `start`/`len` span into the
/// source text (see [`Parser.sliceText`]).
const Token = @import("lexer.zig").Token;
/// The token-kind enum matched throughout the grammar (keywords, punctuation,
/// literal categories).
const TokenType = @import("lexer.zig").TokenType;
/// The AST node definitions this parser builds (`Statement`, `Expr`, and the
/// per-statement structs).
const ast = @import("ast.zig");

/// Stateful recursive-descent parser over a pre-lexed token stream.
///
/// One `Parser` parses one `source` buffer. It holds the materialised token
/// slice, a cursor into it, and an arena that owns every AST node it produces.
/// Because AST identifiers borrow from `source` (see the module header), the
/// `source` passed to [`Parser.init`] must stay alive for as long as the
/// returned AST is used. Call [`Parser.deinit`] to release both the token slice
/// and the whole AST arena.
pub const Parser = struct {
    /// The full token stream, lexed once in [`Parser.init`] and terminated by an
    /// `EOF` token. Owned by this struct (heap-allocated via `toOwnedSlice`) and
    /// freed in [`Parser.deinit`].
    tokens: []Token,
    /// Cursor index into [`Parser.tokens`]; advanced by [`Parser.eat`]. Never
    /// moves past the terminating `EOF` because every grammar rule stops on it.
    pos: usize,
    /// The original SQL text. Token spans index into this, so borrowed AST
    /// identifiers alias it; it must outlive the AST.
    source: []const u8,
    /// Backing allocator, used for the token slice and as the arena's parent.
    /// Only [`Parser.tokens`] is freed through it directly; AST nodes go through
    /// [`Parser.arena`].
    allocator: std.mem.Allocator,

    /// Arena owning every AST node and every synthesised identifier. Freed
    /// wholesale by [`Parser.deinit`]; individual nodes are never freed.
    arena: std.heap.ArenaAllocator,

    /// Lexes `source` fully and returns a ready-to-parse [`Parser`].
    ///
    /// Runs [`Lexer`] to completion up front, appending every token (including
    /// the closing `EOF`) into a temporary list, then transfers ownership of
    /// that list into [`Parser.tokens`]. The temporary list is freed via
    /// `defer`; only the owned slice survives. Propagates any lexer error and
    /// any allocation failure. The returned parser borrows `source`, which the
    /// caller must keep alive for the lifetime of the resulting AST.
    pub fn init(allocator: std.mem.Allocator, source: []const u8) !Parser {
        var lexer = Lexer.init(source);
        var token_list = std.ArrayList(Token).empty;
        defer token_list.deinit(allocator);
        while (true) {
            const tok = try lexer.nextToken();
            try token_list.append(allocator, tok);
            if (tok.type == .EOF) break;
        }
        const tokens = try token_list.toOwnedSlice(allocator);
        return Parser{
            .tokens = tokens,
            .pos = 0,
            .source = source,
            .allocator = allocator,
            .arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    /// Frees the token slice and the entire AST arena.
    ///
    /// After this returns, every AST node produced by this parser is invalid.
    /// Borrowed identifiers pointed into `source`, which this does not own, so
    /// `source` itself is untouched.
    pub fn deinit(self: *Parser) void {
        self.allocator.free(self.tokens);
        self.arena.deinit();
    }

    /// Returns the token at the cursor without advancing.
    ///
    /// Safe to call unconditionally: the cursor never advances past the `EOF`
    /// token, so this always indexes a valid element.
    fn current(self: *Parser) Token {
        return self.tokens[self.pos];
    }

    /// Returns the token one past the cursor for a single-token lookahead.
    ///
    /// Clamps to the last token (always `EOF`) when the cursor is already at the
    /// end, so a lookahead at end-of-stream reports `EOF` rather than reading
    /// out of bounds. Used where the grammar needs two tokens to decide, for
    /// example distinguishing `NOT IN`/`NOT LIKE`/`NOT BETWEEN` from a prefix
    /// `NOT` in [`Parser.parsePredicate`].
    fn peek(self: *Parser) Token {
        if (self.pos + 1 >= self.tokens.len) return self.tokens[self.tokens.len - 1];
        return self.tokens[self.pos + 1];
    }

    /// Advances the cursor by one token unconditionally.
    ///
    /// Callers use this after they have already inspected [`Parser.current`];
    /// it does no bounds or type checking of its own.
    fn eat(self: *Parser) void {
        self.pos += 1;
    }

    /// Consumes the current token if it matches `expected`, otherwise errors.
    ///
    /// Returns the consumed token (callers often need its text span) and
    /// advances the cursor. Returns `error.UnexpectedToken` without advancing if
    /// the current token is not of type `expected`; this is the workhorse for
    /// asserting required grammar tokens like `LPAREN`, `FROM` or `RPAREN`.
    fn expect(self: *Parser, expected: TokenType) !Token {
        const tok = self.current();
        if (tok.type != expected) {
            return error.UnexpectedToken;
        }
        self.eat();
        return tok;
    }

    /// Returns the source text a token spans, as a borrowed slice of `source`.
    ///
    /// The returned slice aliases the `source` buffer (no copy), so it is only
    /// valid while `source` lives. This is how identifiers and string literals
    /// reach the AST cheaply; see the module header on borrowed-vs-synthesised
    /// identifiers.
    fn sliceText(self: *Parser, tok: Token) []const u8 {
        return self.source[tok.start..(tok.start + tok.len)];
    }

    /// Parses a full expression, the lowest-precedence entry point (`OR`).
    ///
    /// Top of the precedence cascade: parses an `AND`-expression, then folds any
    /// number of `OR`-separated `AND`-expressions into a left-associative chain
    /// of `binary_op` nodes. `anyerror` because the cascade is mutually
    /// recursive (through `parseAnd` down to `parsePrimary`, which can recurse
    /// back into `parseExpr` for parenthesised sub-expressions), so the error
    /// set cannot be inferred locally.
    pub fn parseExpr(self: *Parser) anyerror!*ast.Expr {
        var left = try self.parseAnd();
        while (self.current().type == .OR) {
            self.eat();
            const right = try self.parseAnd();
            const node = try self.arena.allocator().create(ast.Expr);
            node.* = .{ .binary_op = .{ .left = left, .op = .OR, .right = right } };
            left = node;
        }
        return left;
    }

    /// Parses `AND`-joined predicates, one precedence level below `OR`.
    ///
    /// Left-associative: folds `a AND b AND c` into nested `binary_op` nodes.
    /// Operands come from [`Parser.parseNot`], so a `NOT` binds tighter than the
    /// `AND` that joins two of them.
    fn parseAnd(self: *Parser) !*ast.Expr {
        var left = try self.parseNot();
        while (self.current().type == .AND) {
            self.eat();
            const right = try self.parseNot();
            const node = try self.arena.allocator().create(ast.Expr);
            node.* = .{ .binary_op = .{ .left = left, .op = .AND, .right = right } };
            left = node;
        }
        return left;
    }

    /// Parses an optional prefix `NOT`, then a predicate.
    ///
    /// Recurses on itself so `NOT NOT x` is accepted (double negation). When
    /// there is no leading `NOT` it falls straight through to
    /// [`Parser.parsePredicate`]. `anyerror` for the same mutual-recursion
    /// reason as [`Parser.parseExpr`]. Note this handles the *prefix* `NOT`;
    /// the infix `NOT IN`/`NOT LIKE`/`NOT BETWEEN` forms are disambiguated
    /// separately inside [`Parser.parsePredicate`].
    fn parseNot(self: *Parser) anyerror!*ast.Expr {
        if (self.current().type == .NOT) {
            self.eat();
            const inner = try self.parseNot();
            const node = try self.arena.allocator().create(ast.Expr);
            node.* = .{ .unary_not = inner };
            return node;
        }
        return self.parsePredicate();
    }

    /// Parses a single comparison/membership predicate.
    ///
    /// Parses a left operand (an additive expression), then dispatches on the
    /// following token into one of the predicate forms:
    /// comparison (`= <> > < >= <=`), `IS [NOT] NULL`, `[NOT] IN (list |
    /// subquery)`, `[NOT] LIKE pattern`, or `[NOT] BETWEEN lo AND hi`. If none of
    /// those follow, the bare left operand is returned unchanged (an expression
    /// need not be a predicate).
    ///
    /// The infix `NOT` is handled with one token of lookahead: a `NOT` is only
    /// consumed as a predicate negator when the token after it is `IN`, `LIKE`
    /// or `BETWEEN` (see [`Parser.peek`]); otherwise it is left for a higher
    /// level. The resulting `negated` flag is threaded into the `in_*`, `like`
    /// and `between` nodes. `IN (` peeks for a `SELECT` to choose between an
    /// `in_subquery` and a literal `in_list` node.
    fn parsePredicate(self: *Parser) !*ast.Expr {
        const left = try self.parseAdditive();

        var negated = false;
        if (self.current().type == .NOT and
            (self.peek().type == .IN or self.peek().type == .LIKE or self.peek().type == .BETWEEN))
        {
            self.eat();
            negated = true;
        }

        switch (self.current().type) {
            .EQ, .NE, .GT, .LT, .GTE, .LTE => {
                const op: ast.OpType = switch (self.current().type) {
                    .EQ => .EQ,
                    .NE => .NE,
                    .GT => .GT,
                    .LT => .LT,
                    .GTE => .GTE,
                    .LTE => .LTE,
                    else => unreachable,
                };
                self.eat();
                const right = try self.parseAdditive();
                const node = try self.arena.allocator().create(ast.Expr);
                node.* = .{ .binary_op = .{ .left = left, .op = op, .right = right } };
                return node;
            },
            .IS => {
                self.eat();
                var is_neg = false;
                if (self.current().type == .NOT) {
                    self.eat();
                    is_neg = true;
                }
                _ = try self.expect(.NULL);
                const node = try self.arena.allocator().create(ast.Expr);
                node.* = .{ .is_null = .{ .operand = left, .negated = is_neg } };
                return node;
            },
            .IN => {
                self.eat();
                _ = try self.expect(.LPAREN);
                if (self.current().type == .SELECT) {
                    const sub = try self.parseSubquerySelect();
                    _ = try self.expect(.RPAREN);
                    const node = try self.arena.allocator().create(ast.Expr);
                    node.* = .{ .in_subquery = .{ .operand = left, .subquery = sub, .negated = negated } };
                    return node;
                }
                var items = std.ArrayList(*ast.Expr).empty;
                if (self.current().type != .RPAREN) {
                    try items.append(self.arena.allocator(), try self.parseAdditive());
                    while (self.current().type == .COMMA) {
                        self.eat();
                        try items.append(self.arena.allocator(), try self.parseAdditive());
                    }
                }
                _ = try self.expect(.RPAREN);
                const node = try self.arena.allocator().create(ast.Expr);
                node.* = .{ .in_list = .{ .operand = left, .items = try items.toOwnedSlice(self.arena.allocator()), .negated = negated } };
                return node;
            },
            .LIKE => {
                self.eat();
                const pat = try self.parseAdditive();
                const node = try self.arena.allocator().create(ast.Expr);
                node.* = .{ .like = .{ .operand = left, .pattern = pat, .negated = negated } };
                return node;
            },
            .BETWEEN => {
                self.eat();
                const lo = try self.parseAdditive();
                _ = try self.expect(.AND);
                const hi = try self.parseAdditive();
                const node = try self.arena.allocator().create(ast.Expr);
                node.* = .{ .between = .{ .operand = left, .lo = lo, .hi = hi, .negated = negated } };
                return node;
            },
            else => return left,
        }
    }

    /// Parses `+`/`-` additive arithmetic, left-associative.
    ///
    /// Operands come from [`Parser.parseMul`], so multiplication and division
    /// bind tighter. Folds a run like `a + b - c` into left-leaning `binary_op`
    /// nodes.
    fn parseAdditive(self: *Parser) !*ast.Expr {
        var left = try self.parseMul();
        while (self.current().type == .PLUS or self.current().type == .MINUS) {
            const op: ast.OpType = if (self.current().type == .PLUS) .PLUS else .MINUS;
            self.eat();
            const right = try self.parseMul();
            const node = try self.arena.allocator().create(ast.Expr);
            node.* = .{ .binary_op = .{ .left = left, .op = op, .right = right } };
            left = node;
        }
        return left;
    }

    /// Parses `*`/`/` multiplicative arithmetic, left-associative.
    ///
    /// The tightest-binding binary level. Operands are primaries, so a
    /// parenthesised sub-expression is the only way to lower precedence back
    /// down within an arithmetic term.
    fn parseMul(self: *Parser) !*ast.Expr {
        var left = try self.parsePrimary();
        while (self.current().type == .STAR or self.current().type == .SLASH) {
            const op: ast.OpType = if (self.current().type == .STAR) .STAR else .SLASH;
            self.eat();
            const right = try self.parsePrimary();
            const node = try self.arena.allocator().create(ast.Expr);
            node.* = .{ .binary_op = .{ .left = left, .op = op, .right = right } };
            left = node;
        }
        return left;
    }

    /// Parses an atom: the base case of the expression cascade.
    ///
    /// Handles, by leading token:
    /// - `IDENTIFIER` -> a function call `name(args)` if followed by `(` (the
    ///   name is upper-cased into the arena so lookups are case-insensitive), a
    ///   qualified `table.column` reference if followed by `.`, or a plain
    ///   `column_ref` otherwise.
    /// - the aggregate keywords `COUNT`/`SUM`/`MIN`/`MAX`/`AVG` -> a `func_call`
    ///   named by the token tag; an optional leading `DISTINCT` is consumed and
    ///   `*` is accepted as a no-argument form (as in `COUNT(*)`).
    /// - `CASE ... WHEN ... THEN ... [ELSE ...] END` -> a `case_expr` with a
    ///   list of when/then arms and an optional else.
    /// - `INTEGER`/`FLOAT`/`STRING` literals and `?` placeholders. Numeric
    ///   conversions `catch unreachable` because the lexer guarantees the token
    ///   text is well-formed.
    /// - `LPAREN` -> either a scalar `subquery` (when `SELECT` follows) or a
    ///   parenthesised expression, whose inner node is returned directly (no
    ///   wrapper node, so parentheses leave no AST trace beyond grouping).
    ///
    /// A node is allocated up front and overwritten in each arm; the parenthesis
    /// arm is the one path that returns a *different* pointer and leaves that
    /// pre-allocated node unused (harmless: the arena reclaims it in bulk).
    /// Returns `error.UnexpectedToken` for any other leading token.
    fn parsePrimary(self: *Parser) !*ast.Expr {
        const tok = self.current();
        const node = try self.arena.allocator().create(ast.Expr);
        switch (tok.type) {
            .IDENTIFIER => {
                self.eat();
                var text = self.sliceText(tok);
                if (self.current().type == .LPAREN) {
                    self.eat();
                    var args = std.ArrayList(*ast.Expr).empty;
                    if (self.current().type != .RPAREN) {
                        try args.append(self.arena.allocator(), try self.parseExpr());
                        while (self.current().type == .COMMA) {
                            self.eat();
                            try args.append(self.arena.allocator(), try self.parseExpr());
                        }
                    }
                    _ = try self.expect(.RPAREN);
                    const upper = try std.ascii.allocUpperString(self.arena.allocator(), text);
                    node.* = .{ .func_call = .{ .name = upper, .args = try args.toOwnedSlice(self.arena.allocator()) } };
                    return node;
                }
                if (self.current().type == .DOT) {
                    self.eat();
                    const col_tok = try self.expect(.IDENTIFIER);
                    const col_text = self.sliceText(col_tok);
                    text = try std.fmt.allocPrint(self.arena.allocator(), "{s}.{s}", .{text, col_text});
                }
                node.* = .{ .column_ref = text };
                return node;
            },
            .COUNT, .SUM, .MIN, .MAX, .AVG => {
                const kname = @tagName(tok.type);
                self.eat();
                _ = try self.expect(.LPAREN);
                if (self.current().type == .DISTINCT) self.eat();
                var args = std.ArrayList(*ast.Expr).empty;
                if (self.current().type == .STAR) {
                    self.eat();
                } else if (self.current().type != .RPAREN) {
                    try args.append(self.arena.allocator(), try self.parseExpr());
                }
                _ = try self.expect(.RPAREN);
                node.* = .{ .func_call = .{ .name = kname, .args = try args.toOwnedSlice(self.arena.allocator()) } };
                return node;
            },
            .CASE => {
                self.eat();
                var whens = std.ArrayList(ast.CaseWhen).empty;
                while (self.current().type == .WHEN) {
                    self.eat();
                    const cond = try self.parseExpr();
                    _ = try self.expect(.THEN);
                    const result = try self.parseExpr();
                    try whens.append(self.arena.allocator(), ast.CaseWhen{ .cond = cond, .result = result });
                }
                var else_result: ?*ast.Expr = null;
                if (self.current().type == .ELSE) {
                    self.eat();
                    else_result = try self.parseExpr();
                }
                _ = try self.expect(.END);
                node.* = .{ .case_expr = .{ .whens = try whens.toOwnedSlice(self.arena.allocator()), .else_result = else_result } };
                return node;
            },
            .INTEGER => {
                self.eat();
                const val = std.fmt.parseInt(i64, self.sliceText(tok), 10) catch unreachable;
                node.* = .{ .literal_int = val };
                return node;
            },
            .FLOAT => {
                self.eat();
                const val = std.fmt.parseFloat(f64, self.sliceText(tok)) catch unreachable;
                node.* = .{ .literal_float = val };
                return node;
            },
            .STRING => {
                self.eat();
                node.* = .{ .literal_text = self.sliceText(tok) };
                return node;
            },
            .PLACEHOLDER => {
                self.eat();
                node.* = .{ .placeholder = 0 };
                return node;
            },
            .LPAREN => {
                self.eat();
                if (self.current().type == .SELECT) {
                    const sub = try self.parseSubquerySelect();
                    _ = try self.expect(.RPAREN);
                    node.* = .{ .subquery = sub };
                    return node;
                }
                const expr = try self.parseExpr();
                _ = try self.expect(.RPAREN);
                return expr;
            },
            else => {
                return error.UnexpectedToken;
            },
        }
    }

    /// Parses a nested `SELECT` and returns it heap-boxed for embedding.
    ///
    /// Used where a subquery appears inside an expression (`IN (SELECT ...)`, a
    /// scalar `(SELECT ...)`). It calls [`Parser.parseSelect`] and copies the
    /// resulting `SelectStmt` into a fresh arena allocation so it can be
    /// referenced by pointer from the enclosing `Expr` node. Note it takes only
    /// the `.select` variant: a subquery is a single SELECT, not a `UNION`.
    fn parseSubquerySelect(self: *Parser) !*ast.SelectStmt {
        const stmt = try self.parseSelect();
        const sub = try self.arena.allocator().create(ast.SelectStmt);
        sub.* = stmt.select;
        return sub;
    }

    /// Parses one complete SQL statement: the public entry point.
    ///
    /// Dispatches on the leading keyword token to the matching `parse*` routine,
    /// covering DML, DDL, transaction control, `IMPORT`/`EXPORT`, and access
    /// control. Returns `error.UnexpectedToken` if the statement does not start
    /// with a recognised keyword. Does not itself consume a trailing `;`; the
    /// caller drives statement boundaries.
    pub fn parseStatement(self: *Parser) !ast.Statement {
        const tok = self.current();
        return switch (tok.type) {
            .SELECT => try self.parseSelectOrUnion(),
            .INSERT => try self.parseInsert(),
            .UPDATE => try self.parseUpdate(),
            .DELETE => try self.parseDelete(),
            .CREATE => try self.parseCreate(),
            .DROP => try self.parseDrop(),
            .EXPORT => try self.parseExport(),
            .IMPORT => try self.parseImport(),
            .BEGIN => try self.parseBegin(),
            .COMMIT => try self.parseCommit(),
            .ROLLBACK => try self.parseRollback(),
            .SAVEPOINT => try self.parseSavepoint(),
            .RELEASE => try self.parseRelease(),
            .LOGIN => try self.parseLogin(),
            .ALTER => try self.parseAlter(),
            .ANALYZE => try self.parseAnalyze(),
            .BACKUP => try self.parseBackup(),
            .GRANT => try self.parseGrant(),
            .REVOKE => try self.parseRevoke(),
            else => error.UnexpectedToken,
        };
    }

    /// Parses `BEGIN [TRANSACTION]`, opening a transaction.
    ///
    /// The optional `TRANSACTION` keyword is accepted and discarded; both spellings
    /// produce the same empty `begin` statement.
    fn parseBegin(self: *Parser) !ast.Statement {
        self.eat();
        if (self.current().type == .TRANSACTION) {
            self.eat();
        }
        return .{ .begin = .{} };
    }

    /// Parses `COMMIT [TRANSACTION]`, ending a transaction successfully.
    ///
    /// The optional `TRANSACTION` keyword is accepted and discarded.
    fn parseCommit(self: *Parser) !ast.Statement {
        self.eat();
        if (self.current().type == .TRANSACTION) {
            self.eat();
        }
        return .{ .commit = .{} };
    }

    /// Parses `ROLLBACK`, either whole-transaction or to a savepoint.
    ///
    /// Three shapes: `ROLLBACK TO [SAVEPOINT] name` yields a `rollback` carrying
    /// `to_savepoint` (partial rollback within a subtransaction); `ROLLBACK
    /// [TRANSACTION]` yields a plain full rollback. The `TO` branch is checked
    /// first, and its `SAVEPOINT` keyword is optional.
    fn parseRollback(self: *Parser) !ast.Statement {
        self.eat();
        if (self.current().type == .TO) {
            self.eat();
            if (self.current().type == .SAVEPOINT) self.eat();
            const name_tok = try self.expect(.IDENTIFIER);
            return .{ .rollback = .{ .to_savepoint = self.sliceText(name_tok) } };
        }
        if (self.current().type == .TRANSACTION) {
            self.eat();
        }
        return .{ .rollback = .{} };
    }

    /// Parses `SAVEPOINT name`, marking a rollback point in a transaction.
    ///
    /// The name is a required identifier, borrowed from `source`. See
    /// [`Parser.parseRollback`] (`ROLLBACK TO`) and [`Parser.parseRelease`] for
    /// the operations that reference it.
    fn parseSavepoint(self: *Parser) !ast.Statement {
        self.eat();
        const name_tok = try self.expect(.IDENTIFIER);
        return .{ .savepoint = .{ .name = self.sliceText(name_tok) } };
    }

    /// Parses `RELEASE [SAVEPOINT] name`, discarding a savepoint.
    ///
    /// Releasing a savepoint merges its subtransaction into the enclosing one;
    /// it can no longer be rolled back to. The `SAVEPOINT` keyword is optional.
    fn parseRelease(self: *Parser) !ast.Statement {
        self.eat();
        if (self.current().type == .SAVEPOINT) self.eat();
        const name_tok = try self.expect(.IDENTIFIER);
        return .{ .release_savepoint = .{ .name = self.sliceText(name_tok) } };
    }

    /// Parses an aggregate call `KIND([DISTINCT] (* | column))` in a projection.
    ///
    /// `kind` is the already-classified aggregate (`COUNT`/`SUM`/...); this
    /// consumes that keyword token, the parentheses, an optional `DISTINCT`, and
    /// the argument. The argument is either `*` (`.star`, the `COUNT(*)` form) or
    /// a possibly-qualified `table.column`, which is joined into one arena string
    /// when a `.` follows. This is the projection-list aggregate; the
    /// expression-context aggregate lives in [`Parser.parsePrimary`].
    fn parseAggregateCall(self: *Parser, kind: ast.AggregateKind) !ast.AggregateCall {
        self.eat();
        _ = try self.expect(.LPAREN);
        var distinct = false;
        if (self.current().type == .DISTINCT) {
            self.eat();
            distinct = true;
        }
        var arg: ast.AggregateArg = undefined;
        if (self.current().type == .STAR) {
            self.eat();
            arg = .star;
        } else {
            const col_tok = try self.expect(.IDENTIFIER);
            var col_name = self.sliceText(col_tok);
            if (self.current().type == .DOT) {
                self.eat();
                const col_tok2 = try self.expect(.IDENTIFIER);
                col_name = try std.fmt.allocPrint(self.arena.allocator(), "{s}.{s}", .{ col_name, self.sliceText(col_tok2) });
            }
            arg = .{ .column = col_name };
        }
        _ = try self.expect(.RPAREN);
        return .{ .kind = kind, .distinct = distinct, .argument = arg };
    }

    /// Parses one `SELECT`, then folds any trailing `UNION [ALL]` chain.
    ///
    /// Fast path: if no `UNION` follows the first select, returns it directly as
    /// a `.select` statement (no wrapper allocation). Otherwise collects all the
    /// selects into one `union_query`, with a parallel `all` bool list recording
    /// whether each `UNION` was `UNION ALL` (duplicate-preserving) or plain
    /// `UNION` (duplicate-eliminating). The `all` list has one fewer entry than
    /// `selects`, since it describes the joins *between* selects.
    fn parseSelectOrUnion(self: *Parser) !ast.Statement {
        const first = try self.parseSelect();
        if (self.current().type != .UNION) return first;
        var selects = std.ArrayList(ast.SelectStmt).empty;
        var alls = std.ArrayList(bool).empty;
        try selects.append(self.arena.allocator(), first.select);
        while (self.current().type == .UNION) {
            self.eat();
            var is_all = false;
            if (self.current().type == .ALL) {
                is_all = true;
                self.eat();
            }
            try alls.append(self.arena.allocator(), is_all);
            const nxt = try self.parseSelect();
            try selects.append(self.arena.allocator(), nxt.select);
        }
        return .{ .union_query = ast.UnionStmt{
            .selects = try selects.toOwnedSlice(self.arena.allocator()),
            .all = try alls.toOwnedSlice(self.arena.allocator()),
        } };
    }

    /// Parses a single `SELECT` query into a `select` statement.
    ///
    /// Consumes the full SELECT grammar in clause order: optional `DISTINCT`;
    /// the projection list (`*`, aggregates, or possibly-qualified columns, each
    /// with an optional `AS alias`); the mandatory `FROM table` (with optional
    /// `schema.table` qualification); zero or more `JOIN` clauses via
    /// [`Parser.parseSelect`]'s inline join loop (`[INNER|LEFT|RIGHT|FULL]
    /// [OUTER] JOIN t ON expr`); optional `WHERE`, `GROUP BY`, `HAVING`, `ORDER
    /// BY` (per-column `ASC`/`DESC`, default ascending), and `LIMIT`/`OFFSET`.
    ///
    /// `LIMIT`/`OFFSET` values are parsed as `u32` and `catch unreachable` on
    /// conversion because the token is a lexer-validated `INTEGER`. Every list
    /// (projections, joins, group columns, order keys) is arena-owned. `ORDER
    /// BY` direction is read as a following `IDENTIFIER` compared
    /// case-insensitively, since `ASC`/`DESC` are not reserved words. Returns
    /// `error.ExpectedColumn` if a projection item is none of `*`, an aggregate,
    /// or an identifier.
    fn parseSelect(self: *Parser) !ast.Statement {
        self.eat();
        var distinct = false;
        if (self.current().type == .DISTINCT) {
            self.eat();
            distinct = true;
        }
        var projections = std.ArrayList(ast.Projection).empty;
        while (true) {
            const tok = self.current();
            var expr: ast.ProjectionExpr = undefined;
            if (tok.type == .STAR) {
                self.eat();
                expr = .star;
            } else if (tok.type == .COUNT or tok.type == .SUM or tok.type == .MIN or tok.type == .MAX or tok.type == .AVG) {
                const kind: ast.AggregateKind = switch (tok.type) {
                    .COUNT => .COUNT,
                    .SUM => .SUM,
                    .MIN => .MIN,
                    .MAX => .MAX,
                    .AVG => .AVG,
                    else => unreachable,
                };
                expr = .{ .aggregate = try self.parseAggregateCall(kind) };
            } else if (tok.type == .IDENTIFIER) {
                self.eat();
                var name = self.sliceText(tok);
                if (self.current().type == .DOT) {
                    self.eat();
                    const col_tok = try self.expect(.IDENTIFIER);
                    const col_name = self.sliceText(col_tok);
                    name = try std.fmt.allocPrint(self.arena.allocator(), "{s}.{s}", .{ name, col_name });
                }
                expr = .{ .column = name };
            } else {
                return error.ExpectedColumn;
            }

            var alias: ?[]const u8 = null;
            if (self.current().type == .AS) {
                self.eat();
                const alias_tok = try self.expect(.IDENTIFIER);
                alias = self.sliceText(alias_tok);
            }

            try projections.append(self.arena.allocator(), .{ .expr = expr, .alias = alias });

            if (self.current().type != .COMMA) break;
            self.eat();
        }

        _ = try self.expect(.FROM);
        var table_name = self.sliceText(try self.expect(.IDENTIFIER));
        if (self.current().type == .DOT) {
            self.eat();
            const suffix_tok = try self.expect(.IDENTIFIER);
            const suffix = self.sliceText(suffix_tok);
            table_name = try std.fmt.allocPrint(self.arena.allocator(), "{s}.{s}", .{table_name, suffix});
        }

        var joins = std.ArrayList(ast.JoinExpr).empty;
        while (true) {
            const current_tok = self.current();
            var join_type: ?ast.JoinType = null;
            if (current_tok.type == .JOIN) {
                self.eat();
                join_type = .inner;
            } else if (current_tok.type == .INNER) {
                self.eat();
                _ = try self.expect(.JOIN);
                join_type = .inner;
            } else if (current_tok.type == .LEFT) {
                self.eat();
                if (self.current().type == .OUTER) self.eat();
                _ = try self.expect(.JOIN);
                join_type = .left;
            } else if (current_tok.type == .RIGHT) {
                self.eat();
                if (self.current().type == .OUTER) self.eat();
                _ = try self.expect(.JOIN);
                join_type = .right;
            } else if (current_tok.type == .FULL) {
                self.eat();
                if (self.current().type == .OUTER) self.eat();
                _ = try self.expect(.JOIN);
                join_type = .full;
            }

            if (join_type) |jt| {
                const right_tok = try self.expect(.IDENTIFIER);
                const right_table = self.sliceText(right_tok);
                _ = try self.expect(.ON);
                const on_expr = try self.parseExpr();
                try joins.append(self.arena.allocator(), .{
                    .join_type = jt,
                    .right_table = right_table,
                    .on_expr = on_expr,
                });
            } else {
                break;
            }
        }

        var where_expr: ?*ast.Expr = null;
        if (self.current().type == .WHERE) {
            self.eat();
            where_expr = try self.parseExpr();
        }

        var group_by: ?[]const []const u8 = null;
        if (self.current().type == .GROUP) {
            self.eat();
            _ = try self.expect(.BY);
            var group_cols = std.ArrayList([]const u8).empty;
            while (true) {
                const col_tok = try self.expect(.IDENTIFIER);
                var cname = self.sliceText(col_tok);
                if (self.current().type == .DOT) {
                    self.eat();
                    const col_tok2 = try self.expect(.IDENTIFIER);
                    cname = try std.fmt.allocPrint(self.arena.allocator(), "{s}.{s}", .{ cname, self.sliceText(col_tok2) });
                }
                try group_cols.append(self.arena.allocator(), cname);
                if (self.current().type != .COMMA) break;
                self.eat();
            }
            group_by = try group_cols.toOwnedSlice(self.arena.allocator());
        }

        var having_expr: ?*ast.Expr = null;
        if (self.current().type == .HAVING) {
            self.eat();
            having_expr = try self.parseExpr();
        }

        var order_by: ?[]const ast.OrderKey = null;
        if (self.current().type == .ORDER) {
            self.eat();
            _ = try self.expect(.BY);
            var order_cols = std.ArrayList(ast.OrderKey).empty;
            while (true) {
                const col_tok = try self.expect(.IDENTIFIER);
                var desc = false;
                if (self.current().type == .IDENTIFIER) {
                    const dir = self.sliceText(self.current());
                    if (std.ascii.eqlIgnoreCase(dir, "DESC")) {
                        desc = true;
                        self.eat();
                    } else if (std.ascii.eqlIgnoreCase(dir, "ASC")) {
                        self.eat();
                    }
                }
                try order_cols.append(self.arena.allocator(), .{ .column = self.sliceText(col_tok), .desc = desc });
                if (self.current().type != .COMMA) break;
                self.eat();
            }
            order_by = try order_cols.toOwnedSlice(self.arena.allocator());
        }

        var limit: ?u32 = null;
        var offset: ?u32 = null;
        if (self.current().type == .LIMIT) {
            self.eat();
            const num_tok = try self.expect(.INTEGER);
            limit = std.fmt.parseInt(u32, self.sliceText(num_tok), 10) catch unreachable;
        }
        if (self.current().type == .OFFSET) {
            self.eat();
            const num_tok = try self.expect(.INTEGER);
            offset = std.fmt.parseInt(u32, self.sliceText(num_tok), 10) catch unreachable;
        }

        return .{ .select = ast.SelectStmt{
            .table_name = table_name,
            .joins = try joins.toOwnedSlice(self.arena.allocator()),
            .projections = try projections.toOwnedSlice(self.arena.allocator()),
            .where_expr = where_expr,
            .group_by = group_by,
            .having_expr = having_expr,
            .order_by = order_by,
            .limit = limit,
            .offset = offset,
            .distinct = distinct,
        } };
    }

    /// Parses `INSERT INTO table (cols...) VALUES (...), (...)`.
    ///
    /// Requires an explicit column list. Then parses one or more comma-separated
    /// value tuples, each a parenthesised list of primaries (via
    /// [`Parser.parsePrimary`], so values are literals/placeholders, not full
    /// expressions), supporting multi-row inserts. Column and value arity are
    /// *not* checked here; that is the executor's job. Values are stored as
    /// `[]const ast.Expr` per row (dereferenced from the parsed node pointers).
    fn parseInsert(self: *Parser) !ast.Statement {
        self.eat();
        _ = try self.expect(.INTO);
        const table_tok = try self.expect(.IDENTIFIER);
        const table_name = self.sliceText(table_tok);

        _ = try self.expect(.LPAREN);
        var columns = std.ArrayList([]const u8).empty;
        while (true) {
            const col_tok = try self.expect(.IDENTIFIER);
            try columns.append(self.arena.allocator(), self.sliceText(col_tok));
            if (self.current().type != .COMMA) break;
            self.eat();
        }
        _ = try self.expect(.RPAREN);

        _ = try self.expect(.VALUES);
        var rows = std.ArrayList([]const ast.Expr).empty;
        while (true) {
            _ = try self.expect(.LPAREN);
            var values = std.ArrayList(ast.Expr).empty;
            while (true) {
                const val_node = try self.parsePrimary();
                try values.append(self.arena.allocator(), val_node.*);
                if (self.current().type != .COMMA) break;
                self.eat();
            }
            _ = try self.expect(.RPAREN);
            try rows.append(self.arena.allocator(), try values.toOwnedSlice(self.arena.allocator()));
            if (self.current().type != .COMMA) break;
            self.eat();
        }

        return .{ .insert = ast.InsertStmt{
            .table_name = table_name,
            .columns = try columns.toOwnedSlice(self.arena.allocator()),
            .rows = try rows.toOwnedSlice(self.arena.allocator()),
        } };
    }

    /// Parses `UPDATE table SET col = expr, ... [WHERE expr]`.
    ///
    /// Each assignment RHS is an additive expression (via
    /// [`Parser.parseAdditive`], so it may be arithmetic but not a full
    /// predicate). The `WHERE` clause is optional; its absence means the update
    /// applies to every row, which the executor must honour.
    fn parseUpdate(self: *Parser) !ast.Statement {
        self.eat();
        const table_tok = try self.expect(.IDENTIFIER);
        const table_name = self.sliceText(table_tok);

        _ = try self.expect(.SET);
        var assignments = std.ArrayList(ast.Assignment).empty;
        while (true) {
            const col_tok = try self.expect(.IDENTIFIER);
            const col_name = self.sliceText(col_tok);
            _ = try self.expect(.EQ);
            const val_node = try self.parseAdditive();
            try assignments.append(self.arena.allocator(), ast.Assignment{
                .column = col_name,
                .value = val_node.*,
            });
            if (self.current().type != .COMMA) break;
            self.eat();
        }

        var where_expr: ?*ast.Expr = null;
        if (self.current().type == .WHERE) {
            self.eat();
            where_expr = try self.parseExpr();
        }

        return .{ .update = ast.UpdateStmt{
            .table_name = table_name,
            .assignments = try assignments.toOwnedSlice(self.arena.allocator()),
            .where_expr = where_expr,
        } };
    }

    /// Parses `DELETE FROM table [WHERE expr]`.
    ///
    /// A missing `WHERE` deletes all rows; the executor is responsible for that
    /// semantics. The `FROM` keyword is mandatory.
    fn parseDelete(self: *Parser) !ast.Statement {
        self.eat();
        _ = try self.expect(.FROM);
        const table_tok = try self.expect(.IDENTIFIER);
        const table_name = self.sliceText(table_tok);

        var where_expr: ?*ast.Expr = null;
        if (self.current().type == .WHERE) {
            self.eat();
            where_expr = try self.parseExpr();
        }

        return .{ .delete = ast.DeleteStmt{
            .table_name = table_name,
            .where_expr = where_expr,
        } };
    }

    /// Dispatches `CREATE ...` to the matching object parser.
    ///
    /// Routes on the object keyword: `TABLE`, `INDEX`, `UNIQUE INDEX`
    /// (consumes `UNIQUE` then `INDEX` and flags the index unique), `USER`,
    /// `ROLE`. Returns `error.UnexpectedToken` for anything else. See
    /// [`Parser.parseCreateTable`], [`Parser.parseCreateIndex`],
    /// [`Parser.parseCreateUser`], [`Parser.parseCreateRole`].
    fn parseCreate(self: *Parser) !ast.Statement {
        self.eat();
        const tok = self.current();
        return switch (tok.type) {
            .TABLE => try self.parseCreateTable(),
            .INDEX => try self.parseCreateIndex(false),
            .UNIQUE => {
                self.eat();
                _ = try self.expect(.INDEX);
                return try self.parseCreateIndex(true);
            },
            .USER => try self.parseCreateUser(),
            .ROLE => try self.parseCreateRole(),
            else => error.UnexpectedToken,
        };
    }

    /// Dispatches `DROP ...` to the matching object parser.
    ///
    /// Handles inline (no separate helper): `DROP TABLE name`, `DROP INDEX name
    /// ON table` (index drop needs the owning table), and `DROP USER name`.
    /// Returns `error.UnexpectedToken` for any other object keyword.
    fn parseDrop(self: *Parser) !ast.Statement {
        self.eat();
        const tok = self.current();
        return switch (tok.type) {
            .TABLE => {
                self.eat();
                const table_tok = try self.expect(.IDENTIFIER);
                return .{ .drop_table = .{ .table_name = self.sliceText(table_tok) } };
            },
            .INDEX => {
                self.eat();
                const index_tok = try self.expect(.IDENTIFIER);
                const index_name = self.sliceText(index_tok);
                _ = try self.expect(.ON);
                const table_tok = try self.expect(.IDENTIFIER);
                const table_name = self.sliceText(table_tok);
                return .{ .drop_index = .{ .index_name = index_name, .table_name = table_name } };
            },
            .USER => {
                self.eat();
                const user_tok = try self.expect(.IDENTIFIER);
                return .{ .drop_user = .{ .username = self.sliceText(user_tok) } };
            },
            else => error.UnexpectedToken,
        };
    }

    /// Parses `CREATE TABLE [IF NOT EXISTS] name (column-defs...)`.
    ///
    /// `IF NOT EXISTS` is contextual: `IF` and `EXISTS` are not reserved words,
    /// so they arrive as identifiers and are matched by case-insensitive text
    /// (`NOT` is a real token in between). Each column is `name type` followed by
    /// zero or more constraints in any order: `PRIMARY KEY`, `UNIQUE`, `NOT
    /// NULL`/`NULL`, `DEFAULT (string|int|NULL)`, `AUTO_INCREMENT` (accepted and
    /// ignored), and `REFERENCES parent(col)` for a foreign key. An optional
    /// type size/precision `(n)` or `(n, m)` after the type name is parsed and
    /// discarded (NovaDB does not store column widths). Returns
    /// `error.ExpectedDefaultValue` if `DEFAULT` is not followed by a literal.
    fn parseCreateTable(self: *Parser) !ast.Statement {
        self.eat();

        var if_not_exists = false;
        if (self.current().type == .IDENTIFIER and std.ascii.eqlIgnoreCase(self.sliceText(self.current()), "IF")) {
            self.eat();
            _ = try self.expect(.NOT);
            const ex_tok = try self.expect(.IDENTIFIER);
            if (!std.ascii.eqlIgnoreCase(self.sliceText(ex_tok), "EXISTS")) return error.UnexpectedToken;
            if_not_exists = true;
        }

        const table_tok = try self.expect(.IDENTIFIER);
        const table_name = self.sliceText(table_tok);

        _ = try self.expect(.LPAREN);
        var columns = std.ArrayList(ast.CreateColumn).empty;
        while (true) {
            const col_tok = try self.expect(.IDENTIFIER);
            const col_name = self.sliceText(col_tok);
            const type_tok = try self.expect(.IDENTIFIER);
            const type_name = self.sliceText(type_tok);

            if (self.current().type == .LPAREN) {
                self.eat();
                _ = try self.expect(.INTEGER);
                if (self.current().type == .COMMA) {
                    self.eat();
                    _ = try self.expect(.INTEGER);
                }
                _ = try self.expect(.RPAREN);
            }

            var is_primary_key = false;
            var is_nullable = true;
            var is_unique = false;
            var default_value: ?[]const u8 = null;
            var foreign_key_table: ?[]const u8 = null;
            var foreign_key_column: ?[]const u8 = null;

            while (true) {
                const next = self.current();
                if (next.type == .PRIMARY) {
                    self.eat();
                    _ = try self.expect(.KEY);
                    is_primary_key = true;
                } else if (next.type == .UNIQUE) {
                    self.eat();
                    is_unique = true;
                } else if (next.type == .NOT) {
                    self.eat();
                    _ = try self.expect(.NULL);
                    is_nullable = false;
                } else if (next.type == .NULL) {
                    self.eat();
                    is_nullable = true;
                } else if (next.type == .DEFAULT) {
                    self.eat();
                    const def_tok = self.current();
                    if (def_tok.type == .STRING or def_tok.type == .INTEGER) {
                        self.eat();
                        default_value = self.sliceText(def_tok);
                    } else if (def_tok.type == .NULL) {
                        self.eat();
                        default_value = "NULL";
                    } else {
                        return error.ExpectedDefaultValue;
                    }
                } else if (next.type == .AUTO_INCREMENT) {
                    self.eat();
                } else if (next.type == .REFERENCES) {
                    self.eat();
                    const parent_table_tok = try self.expect(.IDENTIFIER);
                    const parent_table = self.sliceText(parent_table_tok);
                    _ = try self.expect(.LPAREN);
                    const parent_col_tok = try self.expect(.IDENTIFIER);
                    const parent_col = self.sliceText(parent_col_tok);
                    _ = try self.expect(.RPAREN);
                    foreign_key_table = parent_table;
                    foreign_key_column = parent_col;
                } else {
                    break;
                }
            }

            try columns.append(self.arena.allocator(), ast.CreateColumn{
                .name = col_name,
                .type_name = type_name,
                .is_primary_key = is_primary_key,
                .is_nullable = is_nullable,
                .is_unique = is_unique,
                .default_value = default_value,
                .foreign_key_table = foreign_key_table,
                .foreign_key_column = foreign_key_column,
            });

            if (self.current().type != .COMMA) break;
            self.eat();
        }
        _ = try self.expect(.RPAREN);

        return .{ .create_table = ast.CreateTableStmt{
            .table_name = table_name,
            .columns = try columns.toOwnedSlice(self.arena.allocator()),
            .if_not_exists = if_not_exists,
        } };
    }

    /// Parses `CREATE [UNIQUE] INDEX name ON table (cols...)`.
    ///
    /// `is_unique` is passed in by [`Parser.parseCreate`], which already
    /// consumed the `UNIQUE` keyword; this routine consumes `INDEX` onward.
    /// Supports multi-column (composite) indexes.
    fn parseCreateIndex(self: *Parser, is_unique: bool) !ast.Statement {
        self.eat();
        const index_tok = try self.expect(.IDENTIFIER);
        const index_name = self.sliceText(index_tok);

        _ = try self.expect(.ON);
        const table_tok = try self.expect(.IDENTIFIER);
        const table_name = self.sliceText(table_tok);

        _ = try self.expect(.LPAREN);
        var columns = std.ArrayList([]const u8).empty;
        while (true) {
            const col_tok = try self.expect(.IDENTIFIER);
            try columns.append(self.arena.allocator(), self.sliceText(col_tok));
            if (self.current().type != .COMMA) break;
            self.eat();
        }
        _ = try self.expect(.RPAREN);

        return .{ .create_index = ast.CreateIndexStmt{
            .index_name = index_name,
            .table_name = table_name,
            .columns = try columns.toOwnedSlice(self.arena.allocator()),
            .is_unique = is_unique,
        } };
    }

    /// Strips the surrounding single quotes from a string-literal span.
    ///
    /// The lexer includes the quote characters in a `STRING` token's span, so
    /// file paths and similar literals need them removed before use. Returns the
    /// inner slice when both quotes are present, or the text unchanged otherwise.
    /// `self` is unused (kept for method-call form).
    fn cleanString(self: *Parser, text: []const u8) []const u8 {
        _ = self;
        if (text.len >= 2 and text[0] == '\'' and text[text.len - 1] == '\'') {
            return text[1 .. text.len - 1];
        }
        return text;
    }

    /// Maps a format keyword token to an [`ast.ExportFormatKind`].
    ///
    /// Accepts `CSV`, `JSON`, `BSON`; returns `error.ExpectedFormatKind`
    /// otherwise. Shared by [`Parser.parseExport`] and [`Parser.parseImport`].
    /// `self` is unused (kept for method-call form).
    fn parseFormatKind(self: *Parser, tok_type: TokenType) !ast.ExportFormatKind {
        _ = self;
        return switch (tok_type) {
            .CSV => .CSV,
            .JSON => .JSON,
            .BSON => .BSON,
            else => error.ExpectedFormatKind,
        };
    }

    /// Parses `EXPORT (TABLE t | ALL) TO fmt 'path'` or `EXPORT MANIFEST 'path'`.
    ///
    /// Three targets: a single `TABLE`, `ALL` tables, or a `MANIFEST` (which
    /// takes only a path, no format). For the table/all forms the word `TO` is
    /// contextual (a plain identifier compared case-insensitively), followed by
    /// a format keyword (via [`Parser.parseFormatKind`]) and a quoted path that
    /// is un-quoted with [`Parser.cleanString`]. Returns `error.UnexpectedToken`
    /// for an unrecognised target or a missing/mismatched `TO`.
    fn parseExport(self: *Parser) !ast.Statement {
        self.eat();
        const next_tok = self.current();
        var target: ast.ExportTarget = undefined;
        var format: ?ast.ExportFormatKind = null;
        var file_path: ?[]const u8 = null;

        if (next_tok.type == .TABLE) {
            self.eat();
            const table_tok = try self.expect(.IDENTIFIER);
            target = .{ .table = self.sliceText(table_tok) };

            const to_tok = try self.expect(.IDENTIFIER);
            if (!std.ascii.eqlIgnoreCase(self.sliceText(to_tok), "TO")) return error.UnexpectedToken;
            const format_tok = self.current();
            format = try self.parseFormatKind(format_tok.type);
            self.eat();

            const path_tok = try self.expect(.STRING);
            file_path = self.cleanString(self.sliceText(path_tok));
        } else if (next_tok.type == .ALL) {
            self.eat();
            target = .all;

            const to_tok = try self.expect(.IDENTIFIER);
            if (!std.ascii.eqlIgnoreCase(self.sliceText(to_tok), "TO")) return error.UnexpectedToken;
            const format_tok = self.current();
            format = try self.parseFormatKind(format_tok.type);
            self.eat();

            const path_tok = try self.expect(.STRING);
            file_path = self.cleanString(self.sliceText(path_tok));
        } else if (next_tok.type == .MANIFEST) {
            self.eat();
            const path_tok = try self.expect(.STRING);
            target = .{ .manifest = self.cleanString(self.sliceText(path_tok)) };
        } else {
            return error.UnexpectedToken;
        }

        return .{ .export_stmt = .{
            .target = target,
            .format = format,
            .file_path = file_path,
        } };
    }

    /// Parses `IMPORT (TABLE t | ALL) FROM fmt 'path'` or `IMPORT MANIFEST 'path'`.
    ///
    /// The mirror of [`Parser.parseExport`], differing in that the table/all
    /// forms use the reserved `FROM` keyword (not the contextual `TO`) before
    /// the format and path. `MANIFEST` again takes only a quoted path.
    fn parseImport(self: *Parser) !ast.Statement {
        self.eat();
        const next_tok = self.current();
        var target: ast.ImportTarget = undefined;
        var format: ?ast.ExportFormatKind = null;
        var file_path: ?[]const u8 = null;

        if (next_tok.type == .TABLE) {
            self.eat();
            const table_tok = try self.expect(.IDENTIFIER);
            target = .{ .table = self.sliceText(table_tok) };

            _ = try self.expect(.FROM);
            const format_tok = self.current();
            format = try self.parseFormatKind(format_tok.type);
            self.eat();

            const path_tok = try self.expect(.STRING);
            file_path = self.cleanString(self.sliceText(path_tok));
        } else if (next_tok.type == .ALL) {
            self.eat();
            target = .all;

            _ = try self.expect(.FROM);
            const format_tok = self.current();
            format = try self.parseFormatKind(format_tok.type);
            self.eat();

            const path_tok = try self.expect(.STRING);
            file_path = self.cleanString(self.sliceText(path_tok));
        } else if (next_tok.type == .MANIFEST) {
            self.eat();
            const path_tok = try self.expect(.STRING);
            target = .{ .manifest = self.cleanString(self.sliceText(path_tok)) };
        } else {
            return error.UnexpectedToken;
        }

        return .{ .import_stmt = .{
            .target = target,
            .format = format,
            .file_path = file_path,
        } };
    }

    /// Parses `CREATE USER name IDENTIFIED BY 'password' [ROLE 'role']`.
    ///
    /// The password is a required string literal; the role clause is optional and
    /// defaults to `"read_write"`. Each `expect` here is wrapped to print a
    /// diagnostic (current token type and text) to stderr before propagating the
    /// error, which is a debugging aid for this specific statement. Note the role
    /// name is stored with its surrounding quotes (unlike paths, it is not passed
    /// through [`Parser.cleanString`]).
    fn parseCreateUser(self: *Parser) !ast.Statement {
        self.eat();
        const user_tok = self.expect(.IDENTIFIER) catch |e| {
            std.debug.print("parseCreateUser: expect IDENTIFIER failed, current={any} slice={s}\n", .{self.current().type, self.sliceText(self.current())});
            return e;
        };
        const username = self.sliceText(user_tok);

        _ = self.expect(.IDENTIFIED) catch |e| {
            std.debug.print("parseCreateUser: expect IDENTIFIED failed, current={any} slice={s}\n", .{self.current().type, self.sliceText(self.current())});
            return e;
        };

        _ = self.expect(.BY) catch |e| {
            std.debug.print("parseCreateUser: expect BY failed, current={any} slice={s}\n", .{self.current().type, self.sliceText(self.current())});
            return e;
        };

        const pwd_tok = self.expect(.STRING) catch |e| {
            std.debug.print("parseCreateUser: expect STRING failed, current={any} slice={s}\n", .{self.current().type, self.sliceText(self.current())});
            return e;
        };
        const password = self.sliceText(pwd_tok);

        var role: []const u8 = "read_write";
        if (self.current().type == .ROLE) {
            self.eat();
            const role_tok = self.expect(.STRING) catch |e| {
                std.debug.print("parseCreateUser: expect ROLE STRING failed, current={any} slice={s}\n", .{self.current().type, self.sliceText(self.current())});
                return e;
            };
            role = self.sliceText(role_tok);
        }

        return .{ .create_user = .{
            .username = username,
            .password = password,
            .role = role,
        } };
    }

    /// Parses `LOGIN name 'password'`, authenticating a session.
    ///
    /// Both the username identifier and the string password are required. Unlike
    /// [`Parser.parseCreateUser`] there is no `IDENTIFIED BY`; the password
    /// follows the name directly.
    fn parseLogin(self: *Parser) !ast.Statement {
        self.eat();
        const user_tok = try self.expect(.IDENTIFIER);
        const username = self.sliceText(user_tok);

        const pwd_tok = try self.expect(.STRING);
        const password = self.sliceText(pwd_tok);

        return .{ .login = .{
            .username = username,
            .password = password,
        } };
    }

    /// Parses `ALTER TABLE name (ADD [COLUMN] ... | RENAME TO new)`.
    ///
    /// Two actions. `ADD [COLUMN] name type [constraints...]` reuses the same
    /// constraint loop as [`Parser.parseCreateTable`] (`PRIMARY KEY`, `UNIQUE`,
    /// `NOT NULL`/`NULL`, `DEFAULT`, `AUTO_INCREMENT`) but without the
    /// `REFERENCES`/foreign-key case, producing an `add_column` action.
    /// `RENAME TO new` produces a `rename_table` action. Returns
    /// `error.UnexpectedToken` for any other action keyword and
    /// `error.ExpectedDefaultValue` for a malformed `DEFAULT`.
    fn parseAlter(self: *Parser) !ast.Statement {
        self.eat();
        _ = try self.expect(.TABLE);
        const table_tok = try self.expect(.IDENTIFIER);
        const table_name = self.sliceText(table_tok);

        const action_tok = self.current();
        if (action_tok.type == .ADD) {
            self.eat();
            if (self.current().type == .COLUMN) {
                self.eat();
            }
            const col_tok = try self.expect(.IDENTIFIER);
            const col_name = self.sliceText(col_tok);
            const type_tok = try self.expect(.IDENTIFIER);
            const type_name = self.sliceText(type_tok);

            var is_primary_key = false;
            var is_nullable = true;
            var is_unique = false;
            var default_value: ?[]const u8 = null;

            while (true) {
                const next = self.current();
                if (next.type == .PRIMARY) {
                    self.eat();
                    _ = try self.expect(.KEY);
                    is_primary_key = true;
                } else if (next.type == .UNIQUE) {
                    self.eat();
                    is_unique = true;
                } else if (next.type == .NOT) {
                    self.eat();
                    _ = try self.expect(.NULL);
                    is_nullable = false;
                } else if (next.type == .NULL) {
                    self.eat();
                    is_nullable = true;
                } else if (next.type == .DEFAULT) {
                    self.eat();
                    const def_tok = self.current();
                    if (def_tok.type == .STRING or def_tok.type == .INTEGER) {
                        self.eat();
                        default_value = self.sliceText(def_tok);
                    } else if (def_tok.type == .NULL) {
                        self.eat();
                        default_value = "NULL";
                    } else {
                        return error.ExpectedDefaultValue;
                    }
                } else if (next.type == .AUTO_INCREMENT) {
                    self.eat();
                } else {
                    break;
                }
            }

            return .{ .alter_table = .{
                .table_name = table_name,
                .action = .{ .add_column = .{
                    .name = col_name,
                    .type_name = type_name,
                    .is_primary_key = is_primary_key,
                    .is_nullable = is_nullable,
                    .is_unique = is_unique,
                    .default_value = default_value,
                } },
            } };
        } else if (action_tok.type == .RENAME) {
            self.eat();
            _ = try self.expect(.TO);
            const new_name_tok = try self.expect(.IDENTIFIER);
            const new_name = self.sliceText(new_name_tok);

            return .{ .alter_table = .{
                .table_name = table_name,
                .action = .{ .rename_table = new_name },
            } };
        } else {
            return error.UnexpectedToken;
        }
    }

    /// Parses `ANALYZE TABLE name`, requesting statistics collection.
    ///
    /// Produces an `analyze_table` statement; the executor gathers the
    /// cardinality/statistics the cost-based optimiser uses. Both `TABLE` and the
    /// table name are required.
    fn parseAnalyze(self: *Parser) !ast.Statement {
        self.eat();
        _ = try self.expect(.TABLE);
        const name_tok = try self.expect(.IDENTIFIER);
        const name = self.sliceText(name_tok);
        return .{ .analyze_table = .{ .table_name = name } };
    }

    /// Parses `BACKUP [DATABASE] TO 'path'`.
    ///
    /// `DATABASE` is contextual (a plain identifier compared case-insensitively)
    /// and optional. The path is a required, still-quoted string literal (not
    /// passed through [`Parser.cleanString`]).
    fn parseBackup(self: *Parser) !ast.Statement {
        self.eat();
        if (self.current().type == .IDENTIFIER and std.ascii.eqlIgnoreCase(self.sliceText(self.current()), "DATABASE")) {
            self.eat();
        }
        _ = try self.expect(.TO);
        const path_tok = try self.expect(.STRING);
        const path = self.sliceText(path_tok);
        return .{ .backup = .{ .backup_path = path } };
    }

    /// Parses `CREATE ROLE name`, defining a new access-control role.
    ///
    /// Called from [`Parser.parseCreate`] after the `ROLE` keyword; consumes the
    /// role-name identifier and returns a `create_role` statement.
    fn parseCreateRole(self: *Parser) !ast.Statement {
        self.eat();
        const role_tok = try self.expect(.IDENTIFIER);
        const role_name = self.sliceText(role_tok);
        return .{ .create_role = .{ .role_name = role_name } };
    }

    /// Parses `GRANT priv (TO principal | ON object TO principal)`.
    ///
    /// The privilege token may be a plain identifier or one of the reserved
    /// words `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`ALL` (which are otherwise
    /// keywords, so they are matched explicitly and their text taken verbatim).
    /// Two shapes follow: `TO principal` grants a database-wide privilege
    /// (`object` is null), or `ON object TO principal` scopes it to one object.
    /// Returns `error.UnexpectedToken` for a bad privilege token or a missing
    /// `TO`/`ON`. Mirrors [`Parser.parseRevoke`].
    fn parseGrant(self: *Parser) !ast.Statement {
        self.eat();
        const tok = self.current();
        const priv = switch (tok.type) {
            .IDENTIFIER, .SELECT, .INSERT, .UPDATE, .DELETE, .ALL => s: {
                self.eat();
                break :s self.sliceText(tok);
            },
            else => return error.UnexpectedToken,
        };

        if (self.current().type == .TO) {
            self.eat();
            const principal_tok = try self.expect(.IDENTIFIER);
            const principal = self.sliceText(principal_tok);
            return .{ .grant = .{
                .privilege = priv,
                .object = null,
                .to_principal = principal,
            } };
        } else if (self.current().type == .ON) {
            self.eat();
            const obj_tok = try self.expect(.IDENTIFIER);
            const obj = self.sliceText(obj_tok);

            _ = try self.expect(.TO);
            const principal_tok = try self.expect(.IDENTIFIER);
            const principal = self.sliceText(principal_tok);
            return .{ .grant = .{
                .privilege = priv,
                .object = obj,
                .to_principal = principal,
            } };
        } else {
            return error.UnexpectedToken;
        }
    }

    /// Parses `REVOKE priv (FROM principal | ON object FROM principal)`.
    ///
    /// The inverse of [`Parser.parseGrant`], with `FROM` where grant uses `TO`.
    /// The privilege token is matched the same way (plain identifier or a
    /// reserved `SELECT`/`INSERT`/`UPDATE`/`DELETE`/`ALL`), and `ON object`
    /// optionally scopes the revocation to one object (`object` null when
    /// absent). Returns `error.UnexpectedToken` on a bad privilege token or a
    /// missing `FROM`/`ON`.
    fn parseRevoke(self: *Parser) !ast.Statement {
        self.eat();
        const tok = self.current();
        const priv = switch (tok.type) {
            .IDENTIFIER, .SELECT, .INSERT, .UPDATE, .DELETE, .ALL => s: {
                self.eat();
                break :s self.sliceText(tok);
            },
            else => return error.UnexpectedToken,
        };

        if (self.current().type == .FROM) {
            self.eat();
            const principal_tok = try self.expect(.IDENTIFIER);
            const principal = self.sliceText(principal_tok);
            return .{ .revoke = .{
                .privilege = priv,
                .object = null,
                .from_principal = principal,
            } };
        } else if (self.current().type == .ON) {
            self.eat();
            const obj_tok = try self.expect(.IDENTIFIER);
            const obj = self.sliceText(obj_tok);

            _ = try self.expect(.FROM);
            const principal_tok = try self.expect(.IDENTIFIER);
            const principal = self.sliceText(principal_tok);
            return .{ .revoke = .{
                .privilege = priv,
                .object = obj,
                .from_principal = principal,
            } };
        } else {
            return error.UnexpectedToken;
        }
    }
};
