//! Hand-written SQL tokeniser: the first stage of kaidb's SQL front end.
//!
//! This module turns a flat `[]const u8` of SQL text into a stream of
//! [`Token`]s that the parser pulls one at a time via [`Lexer.nextToken`].
//! It is a classic single-pass, character-at-a-time scanner: there is no
//! regex engine and no lookahead beyond a single character ([`Lexer.peekNext`]),
//! which is all the SQL grammar kaidb accepts requires.
//!
//! Design decisions and invariants worth knowing:
//!
//!   * **Zero-copy tokens.** A [`Token`] never owns text. It records only a
//!     `(start, len)` byte span into the caller's `source` buffer plus the
//!     token kind. The parser and later stages slice `source[start..start+len]`
//!     to recover the lexeme. This means the `source` slice MUST outlive every
//!     token produced from it, and the lexer allocates nothing on the hot path
//!     (the `allocator` field is vestigial, see [`Lexer.init`]).
//!
//!   * **Keywords are recognised, not reserved at the scanner level.** Every
//!     alphabetic run is first scanned as an identifier, then looked up in
//!     [`Lexer.keywordToType`]. If it matches a known SQL keyword the token
//!     takes that keyword's [`TokenType`]; otherwise it stays an
//!     `.IDENTIFIER`. Keyword matching is ASCII case-insensitive, so `SELECT`,
//!     `select` and `Select` are the same token.
//!
//!   * **Positional placeholders.** A bare `?` becomes a [`TokenType.PLACEHOLDER`],
//!     which is how bound-parameter queries reach the binary wire protocol; the
//!     parser assigns each `?` its ordinal.
//!
//!   * **Numbers.** [`Lexer.readInteger`] scans an integer and only promotes it
//!     to `.FLOAT` when a `.` is immediately followed by a digit. A trailing dot
//!     with no digit (e.g. `t.`) is therefore left for the `.DOT` rule, so
//!     `table.column` tokenises correctly rather than eating the dot as a
//!     malformed float.
//!
//!   * **Failure mode.** The only errors this stage raises are
//!     `error.UnexpectedCharacter` for a byte that starts no known token, and
//!     it returns them from [`Lexer.nextToken`]. An unterminated string is NOT
//!     an error: [`Lexer.readString`] simply runs to end-of-input. Line numbers
//!     ([`Token.line`]) are tracked purely for diagnostics.
//!
//! This file sits directly under the SQL parser in kaidb's `sql/` layer; the
//! parser owns a `Lexer`, and the tokens feed the query executor and catalog.

const std = @import("std");

/// The kind of a lexical token. Backed by `u8` so a [`Token`] stays small.
///
/// The variants fall into three groups: SQL keywords (`SELECT` ... `REVOKE`,
/// including aggregate functions `COUNT`/`AVG`/`MIN`/`MAX`/`SUM`), literal and
/// name classes ([`IDENTIFIER`], [`INTEGER`], [`FLOAT`], [`STRING`],
/// [`PLACEHOLDER`]), and punctuation/operators (`STAR` ... `DOT`), terminated by
/// the synthetic [`EOF`]. Keyword variants are produced only through
/// [`Lexer.keywordToType`]; anything alphabetic that is not a keyword becomes
/// [`IDENTIFIER`].
pub const TokenType = enum(u8) {
    /// `SELECT` keyword: begins a query.
    SELECT,
    /// `INSERT` keyword: begins a row-insertion statement.
    INSERT,
    /// `UPDATE` keyword: begins a row-mutation statement.
    UPDATE,
    /// `DELETE` keyword: begins a row-removal statement.
    DELETE,
    /// `FROM` keyword: introduces the source table of a query.
    FROM,
    /// `WHERE` keyword: introduces a row-filter predicate.
    WHERE,
    /// `ORDER` keyword: paired with [`BY`] to sort results.
    ORDER,
    /// `BY` keyword: follows `ORDER`/`GROUP`.
    BY,
    /// `LIMIT` keyword: caps the number of returned rows.
    LIMIT,
    /// `CREATE` keyword: begins a DDL creation (table/index/sequence/user/role).
    CREATE,
    /// `DROP` keyword: begins a DDL removal.
    DROP,
    /// `EXPORT` keyword: kaidb extension to dump data to a file format.
    EXPORT,
    /// `IMPORT` keyword: kaidb extension to load data from a file format.
    IMPORT,
    /// `CSV` keyword: file format for [`EXPORT`]/[`IMPORT`].
    CSV,
    /// `JSON` keyword: file format for [`EXPORT`]/[`IMPORT`].
    JSON,
    /// `BSON` keyword: file format for [`EXPORT`]/[`IMPORT`].
    BSON,
    /// `MANIFEST` keyword: kaidb extension used with export/backup manifests.
    MANIFEST,
    /// `ALL` keyword: e.g. `UNION ALL`, or select-all qualifiers.
    ALL,
    /// `TABLE` keyword: the object kind in `CREATE/DROP/ALTER TABLE`.
    TABLE,
    /// `INDEX` keyword: the object kind in `CREATE/DROP INDEX`.
    INDEX,
    /// `SEQUENCE` keyword: the object kind for auto-increment sequences.
    SEQUENCE,
    /// `UNIQUE` keyword: a uniqueness constraint or unique-index qualifier.
    UNIQUE,
    /// `INTO` keyword: follows `INSERT` to name the target table.
    INTO,
    /// `VALUES` keyword: introduces the row tuples of an `INSERT`.
    VALUES,
    /// `SET` keyword: introduces column assignments in `UPDATE` (and session SET).
    SET,
    /// `AND` keyword: boolean conjunction / `BETWEEN` separator.
    AND,
    /// `OR` keyword: boolean disjunction.
    OR,
    /// `NOT` keyword: boolean negation (`NOT NULL`, `NOT IN`, `NOT LIKE`).
    NOT,
    /// `IN` keyword: set-membership predicate.
    IN,
    /// `LIKE` keyword: pattern-match predicate.
    LIKE,
    /// `BETWEEN` keyword: range predicate, used with [`AND`].
    BETWEEN,
    /// `IS` keyword: null test (`IS NULL` / `IS NOT NULL`).
    IS,
    /// `OFFSET` keyword: skips rows before applying [`LIMIT`].
    OFFSET,
    /// `HAVING` keyword: post-aggregation filter on grouped rows.
    HAVING,
    /// `CASE` keyword: begins a `CASE ... WHEN ... END` expression.
    CASE,
    /// `WHEN` keyword: a branch condition inside a [`CASE`].
    WHEN,
    /// `THEN` keyword: the result of a matched [`WHEN`] branch.
    THEN,
    /// `ELSE` keyword: the default branch of a [`CASE`].
    ELSE,
    /// `END` keyword: closes a [`CASE`] expression.
    END,
    /// `PRIMARY` keyword: paired with [`KEY`] for a primary-key constraint.
    PRIMARY,
    /// `KEY` keyword: follows `PRIMARY`/`FOREIGN`.
    KEY,
    /// `FOREIGN` keyword: paired with [`KEY`] for a foreign-key constraint.
    FOREIGN,
    /// `NULL` keyword: the null literal / column nullability.
    NULL,
    /// `DEFAULT` keyword: a column default-value clause.
    DEFAULT,
    /// `AUTO_INCREMENT` keyword: auto-generated sequential column value.
    AUTO_INCREMENT,
    /// `BEGIN` keyword: starts an explicit transaction.
    BEGIN,
    /// `COMMIT` keyword: commits the current transaction.
    COMMIT,
    /// `ROLLBACK` keyword: aborts the current transaction (or to a savepoint).
    ROLLBACK,
    /// `TRANSACTION` keyword: the optional noun after `BEGIN`/`COMMIT`.
    TRANSACTION,
    /// `USER` keyword: the object kind in `CREATE USER` (authn).
    USER,
    /// `IDENTIFIED` keyword: introduces a user's credential (`IDENTIFIED BY`).
    IDENTIFIED,
    /// `ROLE` keyword: the object kind in role management (authz).
    ROLE,
    /// `LOGIN` keyword: a user attribute controlling connect permission.
    LOGIN,
    /// `ALTER` keyword: begins a DDL alteration.
    ALTER,
    /// `RENAME` keyword: an `ALTER` sub-action.
    RENAME,
    /// `ADD` keyword: an `ALTER TABLE ADD` sub-action (e.g. add column).
    ADD,
    /// `COLUMN` keyword: the object kind in `ALTER TABLE ... COLUMN`.
    COLUMN,
    /// `TO` keyword: target of a `RENAME ... TO` (or `GRANT ... TO`).
    TO,
    /// `REFERENCES` keyword: the referenced table of a foreign key.
    REFERENCES,
    /// `JOIN` keyword: begins a join clause.
    JOIN,
    /// `INNER` keyword: inner-join qualifier.
    INNER,
    /// `LEFT` keyword: left-outer-join qualifier.
    LEFT,
    /// `RIGHT` keyword: right-outer-join qualifier.
    RIGHT,
    /// `FULL` keyword: full-outer-join qualifier.
    FULL,
    /// `OUTER` keyword: the optional noun in `LEFT/RIGHT/FULL OUTER JOIN`.
    OUTER,
    /// `ON` keyword: introduces a join predicate.
    ON,
    /// `ANALYZE` keyword: refreshes planner statistics for a table.
    ANALYZE,
    /// `BACKUP` keyword: kaidb extension to snapshot the database.
    BACKUP,
    /// `GRANT` keyword: grants a privilege (authz).
    GRANT,
    /// `REVOKE` keyword: revokes a privilege (authz).
    REVOKE,

    /// `COUNT` aggregate function.
    COUNT,
    /// `AVG` aggregate function.
    AVG,
    /// `MIN` aggregate function.
    MIN,
    /// `MAX` aggregate function.
    MAX,
    /// `SUM` aggregate function.
    SUM,
    /// `AS` keyword: introduces a column or table alias.
    AS,
    /// `GROUP` keyword: paired with [`BY`] for aggregation grouping.
    GROUP,
    /// `DISTINCT` keyword: de-duplicates a result set or aggregate input.
    DISTINCT,
    /// `UNION` keyword: set-union of two queries.
    UNION,
    /// `SAVEPOINT` keyword: names a rollback point within a transaction.
    SAVEPOINT,
    /// `RELEASE` keyword: discards a previously declared [`SAVEPOINT`].
    RELEASE,

    /// A user-supplied name (table, column, alias): any alphanumeric run that
    /// is not a recognised keyword. See [`Lexer.readIdentifier`].
    IDENTIFIER,
    /// An integer literal: a run of digits with no fractional part.
    INTEGER,
    /// A floating-point literal: digits, a `.`, and further digits. See
    /// [`Lexer.readInteger`], which promotes an integer to this only when a `.`
    /// is followed by a digit.
    FLOAT,
    /// A single-quoted string literal; its span EXCLUDES the surrounding
    /// quotes. See [`Lexer.readString`].
    STRING,
    /// A positional bind parameter, the bare `?`. Ordinals are assigned by the
    /// parser; this feeds bound-parameter queries over the binary wire protocol.
    PLACEHOLDER,

    /// `*`: the select-all star, or the multiplication operator.
    STAR,
    /// `,`: list separator.
    COMMA,
    /// `;`: statement terminator.
    SEMICOLON,
    /// `(`: opening parenthesis.
    LPAREN,
    /// `)`: closing parenthesis.
    RPAREN,
    /// `=`: equality comparison (SQL uses a single `=`, not `==`).
    EQ,
    /// `<>` or `!=`: inequality comparison. Both spellings map here.
    NE,
    /// `>`: greater-than comparison.
    GT,
    /// `<`: less-than comparison.
    LT,
    /// `>=`: greater-than-or-equal comparison.
    GTE,
    /// `<=`: less-than-or-equal comparison.
    LTE,
    /// `+`: addition operator.
    PLUS,
    /// `-`: subtraction / unary-minus operator.
    MINUS,
    /// `/`: division operator.
    SLASH,
    /// `.`: member access, e.g. `table.column`.
    DOT,

    /// Synthetic end-of-input marker returned once `pos` reaches `source.len`;
    /// signals the parser to stop.
    EOF,
};

/// One lexical token: a kind plus a zero-copy span into the source text.
///
/// A `Token` deliberately carries no owned bytes. To recover the lexeme, slice
/// `source[start .. start + len]` against the exact buffer the producing
/// [`Lexer`] was initialised with; the token is only meaningful while that
/// buffer is alive. Single-character punctuation has `len == 1`, two-character
/// operators (`>=`, `<=`, `<>`, `!=`) have `len == 2`, and [`TokenType.EOF`]
/// has `len == 0`.
pub const Token = struct {
    /// The lexical category of this token.
    type: TokenType,
    /// Byte offset of the lexeme's first character within `source`.
    start: usize,
    /// Length in bytes of the lexeme. For a [`TokenType.STRING`] this counts
    /// the inner text only, not the surrounding quotes.
    len: usize,
    /// 1-based source line the token starts on, tracked for diagnostics only.
    line: u32,
};

/// Single-pass SQL scanner over a borrowed source buffer.
///
/// The lexer holds a cursor (`pos`) into `source` and is driven by repeated
/// calls to [`Lexer.nextToken`], each of which skips whitespace and emits the
/// next [`Token`] (or [`TokenType.EOF`] once the input is exhausted). It keeps
/// no token buffer and no dynamic state beyond the cursor and line counter, so
/// it is cheap to construct and copy.
pub const Lexer = struct {
    /// The SQL text being scanned. Borrowed, never owned; every emitted
    /// [`Token`] indexes into this slice, so it must outlive the tokens.
    source: []const u8,
    /// Cursor: byte offset of the next character to examine.
    pos: usize,
    /// Current 1-based line number, incremented by [`Lexer.advance`] on `\n`.
    line: u32,
    /// Present only to satisfy callers that expect an allocator field; the
    /// scanner allocates nothing and this is left `undefined` by
    /// [`Lexer.init`]. Do not read it.
    allocator: std.mem.Allocator,

    /// Creates a lexer positioned at the start of `source`, line 1.
    ///
    /// Does not copy `source`; the caller must keep it alive for as long as any
    /// token is used. `allocator` is intentionally left `undefined` because the
    /// scan path performs no allocation (see the `allocator` field).
    pub fn init(source: []const u8) Lexer {
        return Lexer{
            .source = source,
            .pos = 0,
            .line = 1,
            .allocator = undefined,
        };
    }

    /// Returns the character under the cursor without consuming it, or `0` at
    /// end of input. The scanner treats `0` as a synthetic terminator, which is
    /// safe because SQL source is not expected to contain NUL bytes.
    fn peek(self: *Lexer) u8 {
        if (self.pos >= self.source.len) return 0;
        return self.source[self.pos];
    }

    /// Returns the character one past the cursor without consuming it, or `0`
    /// if that would run off the end. This is the lexer's entire lookahead: it
    /// lets [`Lexer.readInteger`] decide whether a `.` starts a fraction.
    fn peekNext(self: *Lexer) u8 {
        if (self.pos + 1 >= self.source.len) return 0;
        return self.source[self.pos + 1];
    }

    /// Consumes one character, advancing the cursor and bumping [`Lexer.line`]
    /// when it steps over a newline. A no-op at end of input, so it is always
    /// safe to call.
    fn advance(self: *Lexer) void {
        if (self.pos < self.source.len) {
            if (self.source[self.pos] == '\n') self.line += 1;
            self.pos += 1;
        }
    }

    /// Advances the cursor past any run of spaces, tabs, newlines and carriage
    /// returns. SQL is whitespace-insensitive between tokens, so this runs
    /// before every [`Lexer.nextToken`] emission. Note there is no comment
    /// handling here: `--` and `/* */` are not skipped.
    fn skipWhitespace(self: *Lexer) void {
        while (true) {
            const c = self.peek();
            if (c == ' ' or c == '\t' or c == '\n' or c == '\r') {
                self.advance();
            } else {
                break;
            }
        }
    }

    /// Scans a single-quoted string literal starting at the opening quote.
    ///
    /// Consumes the leading `'`, records the span of the inner text (quotes
    /// excluded), then consumes the closing `'` if present. An unterminated
    /// string is tolerated, not an error: the loop stops at end of input and
    /// the token covers everything up to there. There is currently no escape or
    /// doubled-quote (`''`) handling, so the first inner `'` always ends the
    /// literal. Returns a [`TokenType.STRING`] token; the error union exists to
    /// match [`Lexer.nextToken`]'s signature and is never actually returned.
    fn readString(self: *Lexer) !Token {
        self.advance();
        const start = self.pos;
        while (self.peek() != 0 and self.peek() != '\'') {
            self.advance();
        }
        const token = Token{
            .type = .STRING,
            .start = start,
            .len = self.pos - start,
            .line = self.line,
        };
        if (self.peek() == '\'') self.advance();
        return token;
    }

    /// Scans an identifier or keyword starting at the current position.
    ///
    /// Consumes a maximal run of alphanumerics plus `_` and `$` (so `user_id`
    /// and `$1`-style names scan whole), then classifies it with
    /// [`Lexer.keywordToType`]: a keyword match yields that keyword's
    /// [`TokenType`], everything else becomes [`TokenType.IDENTIFIER`]. The
    /// caller (`nextToken`) guarantees the first character is alphabetic or
    /// `_`, so a leading digit never reaches here (that goes to
    /// [`Lexer.readInteger`]).
    fn readIdentifier(self: *Lexer) Token {
        const start = self.pos;
        while (true) {
            const c = self.peek();
            if (std.ascii.isAlphanumeric(c) or c == '_' or c == '$') {
                self.advance();
            } else {
                break;
            }
        }
        const text = self.source[start..self.pos];
        const token_type = keywordToType(text) orelse .IDENTIFIER;
        return Token{
            .type = token_type,
            .start = start,
            .len = self.pos - start,
            .line = self.line,
        };
    }

    /// Maps an identifier lexeme to its keyword [`TokenType`], or `null` if it
    /// is not a reserved word (in which case the caller keeps it as an
    /// [`TokenType.IDENTIFIER`]).
    ///
    /// Matching is ASCII case-insensitive via `std.ascii.eqlIgnoreCase`, so
    /// keywords may be written in any case. The lookup is a linear chain of
    /// comparisons rather than a hash table; the keyword set is small and this
    /// keeps the scanner allocation-free and dependency-free. The order of the
    /// checks does not affect correctness because the keyword set is disjoint.
    fn keywordToType(text: []const u8) ?TokenType {
        if (std.ascii.eqlIgnoreCase(text, "SELECT")) return .SELECT;
        if (std.ascii.eqlIgnoreCase(text, "INSERT")) return .INSERT;
        if (std.ascii.eqlIgnoreCase(text, "UPDATE")) return .UPDATE;
        if (std.ascii.eqlIgnoreCase(text, "DELETE")) return .DELETE;
        if (std.ascii.eqlIgnoreCase(text, "FROM")) return .FROM;
        if (std.ascii.eqlIgnoreCase(text, "WHERE")) return .WHERE;
        if (std.ascii.eqlIgnoreCase(text, "ORDER")) return .ORDER;
        if (std.ascii.eqlIgnoreCase(text, "BY")) return .BY;
        if (std.ascii.eqlIgnoreCase(text, "LIMIT")) return .LIMIT;
        if (std.ascii.eqlIgnoreCase(text, "OFFSET")) return .OFFSET;
        if (std.ascii.eqlIgnoreCase(text, "HAVING")) return .HAVING;
        if (std.ascii.eqlIgnoreCase(text, "CREATE")) return .CREATE;
        if (std.ascii.eqlIgnoreCase(text, "DROP")) return .DROP;
        if (std.ascii.eqlIgnoreCase(text, "TABLE")) return .TABLE;
        if (std.ascii.eqlIgnoreCase(text, "INDEX")) return .INDEX;
        if (std.ascii.eqlIgnoreCase(text, "SEQUENCE")) return .SEQUENCE;
        if (std.ascii.eqlIgnoreCase(text, "FOREIGN")) return .FOREIGN;
        if (std.ascii.eqlIgnoreCase(text, "UNIQUE")) return .UNIQUE;
        if (std.ascii.eqlIgnoreCase(text, "INTO")) return .INTO;
        if (std.ascii.eqlIgnoreCase(text, "VALUES")) return .VALUES;
        if (std.ascii.eqlIgnoreCase(text, "SET")) return .SET;
        if (std.ascii.eqlIgnoreCase(text, "AND")) return .AND;
        if (std.ascii.eqlIgnoreCase(text, "OR")) return .OR;
        if (std.ascii.eqlIgnoreCase(text, "NOT")) return .NOT;
        if (std.ascii.eqlIgnoreCase(text, "IN")) return .IN;
        if (std.ascii.eqlIgnoreCase(text, "LIKE")) return .LIKE;
        if (std.ascii.eqlIgnoreCase(text, "BETWEEN")) return .BETWEEN;
        if (std.ascii.eqlIgnoreCase(text, "IS")) return .IS;
        if (std.ascii.eqlIgnoreCase(text, "CASE")) return .CASE;
        if (std.ascii.eqlIgnoreCase(text, "WHEN")) return .WHEN;
        if (std.ascii.eqlIgnoreCase(text, "THEN")) return .THEN;
        if (std.ascii.eqlIgnoreCase(text, "ELSE")) return .ELSE;
        if (std.ascii.eqlIgnoreCase(text, "END")) return .END;
        if (std.ascii.eqlIgnoreCase(text, "PRIMARY")) return .PRIMARY;
        if (std.ascii.eqlIgnoreCase(text, "KEY")) return .KEY;
        if (std.ascii.eqlIgnoreCase(text, "NULL")) return .NULL;
        if (std.ascii.eqlIgnoreCase(text, "DEFAULT")) return .DEFAULT;
        if (std.ascii.eqlIgnoreCase(text, "AUTO_INCREMENT")) return .AUTO_INCREMENT;
        if (std.ascii.eqlIgnoreCase(text, "COUNT")) return .COUNT;
        if (std.ascii.eqlIgnoreCase(text, "AVG")) return .AVG;
        if (std.ascii.eqlIgnoreCase(text, "MIN")) return .MIN;
        if (std.ascii.eqlIgnoreCase(text, "MAX")) return .MAX;
        if (std.ascii.eqlIgnoreCase(text, "SUM")) return .SUM;
        if (std.ascii.eqlIgnoreCase(text, "AS")) return .AS;
        if (std.ascii.eqlIgnoreCase(text, "GROUP")) return .GROUP;
        if (std.ascii.eqlIgnoreCase(text, "DISTINCT")) return .DISTINCT;
        if (std.ascii.eqlIgnoreCase(text, "UNION")) return .UNION;
        if (std.ascii.eqlIgnoreCase(text, "SAVEPOINT")) return .SAVEPOINT;
        if (std.ascii.eqlIgnoreCase(text, "RELEASE")) return .RELEASE;
        if (std.ascii.eqlIgnoreCase(text, "EXPORT")) return .EXPORT;
        if (std.ascii.eqlIgnoreCase(text, "IMPORT")) return .IMPORT;
        if (std.ascii.eqlIgnoreCase(text, "CSV")) return .CSV;
        if (std.ascii.eqlIgnoreCase(text, "JSON")) return .JSON;
        if (std.ascii.eqlIgnoreCase(text, "BSON")) return .BSON;
        if (std.ascii.eqlIgnoreCase(text, "MANIFEST")) return .MANIFEST;
        if (std.ascii.eqlIgnoreCase(text, "ALL")) return .ALL;
        if (std.ascii.eqlIgnoreCase(text, "BEGIN")) return .BEGIN;
        if (std.ascii.eqlIgnoreCase(text, "COMMIT")) return .COMMIT;
        if (std.ascii.eqlIgnoreCase(text, "ROLLBACK")) return .ROLLBACK;
        if (std.ascii.eqlIgnoreCase(text, "TRANSACTION")) return .TRANSACTION;
        if (std.ascii.eqlIgnoreCase(text, "USER")) return .USER;
        if (std.ascii.eqlIgnoreCase(text, "IDENTIFIED")) return .IDENTIFIED;
        if (std.ascii.eqlIgnoreCase(text, "ROLE")) return .ROLE;
        if (std.ascii.eqlIgnoreCase(text, "LOGIN")) return .LOGIN;
        if (std.ascii.eqlIgnoreCase(text, "ALTER")) return .ALTER;
        if (std.ascii.eqlIgnoreCase(text, "RENAME")) return .RENAME;
        if (std.ascii.eqlIgnoreCase(text, "ADD")) return .ADD;
        if (std.ascii.eqlIgnoreCase(text, "COLUMN")) return .COLUMN;
        if (std.ascii.eqlIgnoreCase(text, "TO")) return .TO;
        if (std.ascii.eqlIgnoreCase(text, "REFERENCES")) return .REFERENCES;
        if (std.ascii.eqlIgnoreCase(text, "JOIN")) return .JOIN;
        if (std.ascii.eqlIgnoreCase(text, "INNER")) return .INNER;
        if (std.ascii.eqlIgnoreCase(text, "LEFT")) return .LEFT;
        if (std.ascii.eqlIgnoreCase(text, "RIGHT")) return .RIGHT;
        if (std.ascii.eqlIgnoreCase(text, "FULL")) return .FULL;
        if (std.ascii.eqlIgnoreCase(text, "OUTER")) return .OUTER;
        if (std.ascii.eqlIgnoreCase(text, "ON")) return .ON;
        if (std.ascii.eqlIgnoreCase(text, "ANALYZE")) return .ANALYZE;
        if (std.ascii.eqlIgnoreCase(text, "BACKUP")) return .BACKUP;
        if (std.ascii.eqlIgnoreCase(text, "GRANT")) return .GRANT;
        if (std.ascii.eqlIgnoreCase(text, "REVOKE")) return .REVOKE;
        return null;
    }

    /// Scans a numeric literal, returning either an [`TokenType.INTEGER`] or a
    /// [`TokenType.FLOAT`] token.
    ///
    /// Consumes the leading digit run, then promotes the token to a float ONLY
    /// when the next character is `.` AND the one after it is a digit (checked
    /// via [`Lexer.peekNext`]). This is the subtle bit: a `.` with no following
    /// digit is left unconsumed so that `t.col` and a trailing `1.` tokenise as
    /// number-then-[`TokenType.DOT`] rather than a malformed float. There is no
    /// exponent (`1e9`) or sign handling here; a leading `-` is a separate
    /// [`TokenType.MINUS`] token.
    fn readInteger(self: *Lexer) Token {
        const start = self.pos;
        while (std.ascii.isDigit(self.peek())) {
            self.advance();
        }
        var is_float = false;
        if (self.peek() == '.' and std.ascii.isDigit(self.peekNext())) {
            is_float = true;
            self.advance();
            while (std.ascii.isDigit(self.peek())) {
                self.advance();
            }
        }
        return Token{
            .type = if (is_float) .FLOAT else .INTEGER,
            .start = start,
            .len = self.pos - start,
            .line = self.line,
        };
    }

    /// Produces the next token in the stream, the lexer's sole public entry
    /// point after [`Lexer.init`].
    ///
    /// Skips leading whitespace, then dispatches on the first character:
    /// single-character punctuation and operators are emitted directly;
    /// `>`/`<`/`!` look one character ahead to form the two-character operators
    /// `>=`, `<=`, `<>` and `!=`; `'` delegates to [`Lexer.readString`], `?`
    /// becomes a [`TokenType.PLACEHOLDER`], a digit goes to
    /// [`Lexer.readInteger`], and a letter or `_` to [`Lexer.readIdentifier`].
    /// At end of input it returns a [`TokenType.EOF`] token.
    ///
    /// Returns `error.UnexpectedCharacter` for a lone `!` not followed by `=`,
    /// and for any byte that begins no known token. Note that `=` is a single
    /// token: kaidb SQL has no `==`.
    pub fn nextToken(self: *Lexer) !Token {
        self.skipWhitespace();
        if (self.pos >= self.source.len) {
            return Token{ .type = .EOF, .start = self.pos, .len = 0, .line = self.line };
        }

        const c = self.peek();
        switch (c) {
            '*' => {
                self.advance();
                return Token{ .type = .STAR, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            ',' => {
                self.advance();
                return Token{ .type = .COMMA, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            ';' => {
                self.advance();
                return Token{ .type = .SEMICOLON, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '(' => {
                self.advance();
                return Token{ .type = .LPAREN, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            ')' => {
                self.advance();
                return Token{ .type = .RPAREN, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '+' => {
                self.advance();
                return Token{ .type = .PLUS, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '-' => {
                self.advance();
                return Token{ .type = .MINUS, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '/' => {
                self.advance();
                return Token{ .type = .SLASH, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '.' => {
                self.advance();
                return Token{ .type = .DOT, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '\'' => {
                return try self.readString();
            },
            '?' => {
                self.advance();
                return Token{ .type = .PLACEHOLDER, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '=' => {
                self.advance();
                return Token{ .type = .EQ, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '>' => {
                self.advance();
                if (self.peek() == '=') {
                    self.advance();
                    return Token{ .type = .GTE, .start = self.pos - 2, .len = 2, .line = self.line };
                }
                return Token{ .type = .GT, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '<' => {
                self.advance();
                if (self.peek() == '=') {
                    self.advance();
                    return Token{ .type = .LTE, .start = self.pos - 2, .len = 2, .line = self.line };
                } else if (self.peek() == '>') {
                    self.advance();
                    return Token{ .type = .NE, .start = self.pos - 2, .len = 2, .line = self.line };
                }
                return Token{ .type = .LT, .start = self.pos - 1, .len = 1, .line = self.line };
            },
            '!' => {
                self.advance();
                if (self.peek() == '=') {
                    self.advance();
                    return Token{ .type = .NE, .start = self.pos - 2, .len = 2, .line = self.line };
                }
                return error.UnexpectedCharacter;
            },
            else => {
                if (std.ascii.isDigit(c)) {
                    return self.readInteger();
                } else if (std.ascii.isAlphabetic(c) or c == '_') {
                    return self.readIdentifier();
                } else {
                    return error.UnexpectedCharacter;
                }
            },
        }
    }
};
