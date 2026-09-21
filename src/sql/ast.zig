//! Abstract syntax tree for kaidb's SQL dialect.
//!
//! This file is the shared vocabulary between the two ends of the SQL layer:
//! the parser produces these nodes from a query string, and the query executor
//! walks them to plan and run against the B+Tree engine. It contains no logic at
//! all, only the data shapes, so that "what the parser can express" and "what the
//! executor must handle" are pinned to one authoritative definition and the two
//! sides cannot drift apart.
//!
//! The root of every parse is a single [`Statement`], a tagged union that spans
//! the whole dialect: DML (`SELECT`/`INSERT`/`UPDATE`/`DELETE`), DDL
//! (`CREATE`/`DROP`/`ALTER` for tables, indexes, sequences and foreign keys),
//! transaction control (`BEGIN`/`COMMIT`/`ROLLBACK` plus savepoints), bulk
//! import/export, and the security surface (users, roles, `GRANT`/`REVOKE`,
//! `LOGIN`). Value expressions are a separate recursive union, [`Expr`], reached
//! from the clauses that carry predicates or projected values.
//!
//! Ownership and lifetime: every `[]const u8` here (identifiers, string literals)
//! borrows from the original query text or the parser's arena, and every `*Expr`
//! child is arena-allocated, so a tree is only valid for as long as the arena
//! that built it. Nothing in this file frees anything; the arena owns the whole
//! tree and is dropped in one shot when the statement is done. That is why the
//! recursive expression nodes ([`BinaryOp`], [`InList`], [`CaseExpr`], ...) hold
//! raw pointers rather than owned slices: there is no per-node destruction to
//! sequence.
//!
//! Design notes worth knowing before touching a node:
//!   - `SELECT` and its pieces are the widest surface: joins, `WHERE`,
//!     `GROUP BY`/`HAVING`, `ORDER BY`, `LIMIT`/`OFFSET`, `DISTINCT`, aggregates
//!     and subqueries all live under [`SelectStmt`].
//!   - Subqueries embed a [`SelectStmt`] by pointer ([`Expr.subquery`],
//!     [`InSubquery`]), so the tree is mutually recursive across the
//!     statement/expression boundary and `UNION` ([`UnionStmt`]) is a flat list
//!     of selects rather than a binary tree.
//!   - Negatable predicates (`IN`, `LIKE`, `BETWEEN`, `IS NULL`) carry a
//!     `negated` flag instead of wrapping in a separate NOT node, which keeps the
//!     executor's dispatch flat.

const std = @import("std");

/// The root node of any parsed SQL command: one tagged union arm per statement
/// kind the dialect accepts.
///
/// The parser returns exactly one of these per input statement and the executor
/// switches on the active tag to choose an execution path. New statement support
/// is added by adding an arm here plus its payload struct below; the executor's
/// exhaustive switch then forces every new arm to be handled.
pub const Statement = union(enum) {
    /// A `SELECT` query. See [`SelectStmt`] for the full clause set.
    select: SelectStmt,
    /// An `INSERT INTO ... VALUES ...`, possibly multi-row. See [`InsertStmt`].
    insert: InsertStmt,
    /// An `UPDATE ... SET ... WHERE ...`. See [`UpdateStmt`].
    update: UpdateStmt,
    /// A `DELETE FROM ... WHERE ...`. See [`DeleteStmt`].
    delete: DeleteStmt,
    /// A `CREATE TABLE`. See [`CreateTableStmt`].
    create_table: CreateTableStmt,
    /// A `CREATE INDEX`. See [`CreateIndexStmt`].
    create_index: CreateIndexStmt,
    /// A `CREATE SEQUENCE`. See [`CreateSequenceStmt`].
    create_sequence: CreateSequenceStmt,
    /// A standalone `CREATE`/`ADD` foreign-key constraint. See
    /// [`CreateForeignKeyStmt`].
    create_foreign_key: CreateForeignKeyStmt,
    /// A `CREATE FUNCTION ... LANGUAGE wasm AS '<hex>'` (embed-wasm.md M1).
    create_function: CreateFunctionStmt,
    /// A `DROP FUNCTION name`.
    drop_function: DropFunctionStmt,
    /// A `DROP TABLE`. See [`DropTableStmt`].
    drop_table: DropTableStmt,
    /// A `DROP INDEX`. See [`DropIndexStmt`].
    drop_index: DropIndexStmt,
    /// A `DROP SEQUENCE`. See [`DropSequenceStmt`].
    drop_sequence: DropSequenceStmt,
    /// A `DROP` of a named foreign-key constraint. See [`DropForeignKeyStmt`].
    drop_foreign_key: DropForeignKeyStmt,
    /// A bulk `EXPORT` of one table, all tables, or a manifest. See
    /// [`ExportStmt`].
    export_stmt: ExportStmt,
    /// A bulk `IMPORT` counterpart to `EXPORT`. See [`ImportStmt`].
    import_stmt: ImportStmt,
    /// `BEGIN`, open an explicit transaction. See [`BeginStmt`].
    begin: BeginStmt,
    /// `COMMIT`, durably apply the open transaction. See [`CommitStmt`].
    commit: CommitStmt,
    /// `ROLLBACK`, either the whole transaction or to a savepoint. See
    /// [`RollbackStmt`].
    rollback: RollbackStmt,
    /// `SAVEPOINT name`, mark a sub-transaction point. See [`SavepointStmt`].
    savepoint: SavepointStmt,
    /// `RELEASE SAVEPOINT name`, discard a savepoint. See
    /// [`ReleaseSavepointStmt`].
    release_savepoint: ReleaseSavepointStmt,
    /// `CREATE USER` with a password and role. See [`CreateUserStmt`].
    create_user: CreateUserStmt,
    /// `DROP USER`. See [`DropUserStmt`].
    drop_user: DropUserStmt,
    /// `ALTER USER ... IDENTIFIED BY`. See [`AlterUserStmt`].
    alter_user: AlterUserStmt,
    /// `LOGIN`, authenticate a session. See [`LoginStmt`].
    login: LoginStmt,
    /// `ALTER TABLE`, add a column or rename the table. See [`AlterTableStmt`].
    alter_table: AlterTableStmt,
    /// `ANALYZE`, refresh a table's planner statistics. See
    /// [`AnalyzeTableStmt`].
    analyze_table: AnalyzeTableStmt,
    /// `BACKUP` the database to a path. See [`BackupStmt`].
    backup: BackupStmt,
    /// `CREATE ROLE`. See [`CreateRoleStmt`].
    create_role: CreateRoleStmt,
    /// `GRANT` a privilege to a principal. See [`GrantStmt`].
    grant: GrantStmt,
    /// `REVOKE` a privilege from a principal. See [`RevokeStmt`].
    revoke: RevokeStmt,
    /// A `UNION`/`UNION ALL` of two or more selects. See [`UnionStmt`].
    union_query: UnionStmt,
};

/// A `UNION` chain flattened into parallel arrays rather than a binary tree.
///
/// The dialect represents `A UNION B UNION ALL C` as one node so the executor
/// runs each branch and merges results in one pass. `selects[i]` is the i-th
/// branch; `all[i]` says whether the operator that JOINS branch `i` to the
/// previous one was `UNION ALL` (keep duplicates) rather than plain `UNION`
/// (de-duplicate). The two slices are index-aligned.
pub const UnionStmt = struct {
    /// The branch queries in textual order; `selects[0]` is the leftmost.
    selects: []const SelectStmt,
    /// Per-branch duplicate policy: `true` for `UNION ALL`, `false` for `UNION`.
    /// Index-aligned with [`UnionStmt.selects`].
    all: []const bool,
};

/// A SQL value expression: the recursive core reached from `WHERE`, `HAVING`,
/// projections, `ON` clauses, `IN` lists, assignments and function arguments.
///
/// Composite arms hold `*Expr` children allocated in the parser's arena, making
/// this a mutually recursive tree with the statement types (a [`Expr.subquery`]
/// points back at a [`SelectStmt`]). Nothing here is owned in the RAII sense; the
/// arena frees the whole tree at once.
pub const Expr = union(enum) {
    /// A bare column reference by name, e.g. `price`. Qualification/resolution is
    /// the executor's job.
    column_ref: []const u8, // e.g., "age"
    /// An integer literal, stored as `i64` regardless of the column's width.
    literal_int: i64,
    /// A floating-point literal.
    literal_float: f64, // 3.5, 12.25 (approximate numeric / decimal literal)
    /// A string literal, borrowing the unquoted text from the source.
    literal_text: []const u8,
    /// The SQL `NULL` literal (no payload).
    literal_null,
    /// A positional bind parameter; the `u8` is its 1-based ordinal, so the
    /// executor can substitute a value supplied over the binary wire protocol.
    placeholder: u8, // '?' -> 0, '$1' -> 1, etc.
    /// A binary operator applied to two sub-expressions. See [`BinaryOp`].
    binary_op: BinaryOp,
    /// Logical `NOT` of the pointed-to expression.
    unary_not: *Expr, // NOT <expr>
    /// An `IS NULL` / `IS NOT NULL` test. See [`IsNull`].
    is_null: IsNull, // <expr> IS [NOT] NULL
    /// An `IN (v1, v2, ...)` / `NOT IN (...)` test against a literal list. See
    /// [`InList`].
    in_list: InList, // <expr> [NOT] IN (<item>, ...)
    /// A `LIKE` / `NOT LIKE` pattern match. See [`Like`].
    like: Like, // <expr> [NOT] LIKE <pattern>
    /// A `BETWEEN lo AND hi` / `NOT BETWEEN` range test. See [`Between`].
    between: Between, // <expr> [NOT] BETWEEN <lo> AND <hi>
    /// A scalar function call such as `UPPER(name)`. See [`FuncCall`].
    func_call: FuncCall, // NAME(arg, ...) scalar function (UPPER/LOWER/LENGTH/ABS/COALESCE/...)
    /// A `CASE WHEN ... THEN ... ELSE ... END` expression. See [`CaseExpr`].
    case_expr: CaseExpr, // CASE WHEN cond THEN val ... [ELSE val] END
    /// A scalar subquery: a `SELECT` used where a single value is expected.
    subquery: *SelectStmt,
    /// An `IN (SELECT ...)` / `NOT IN (SELECT ...)` membership test. See
    /// [`InSubquery`].
    in_subquery: InSubquery,
};

/// The `operand IN (SELECT ...)` form, distinct from [`InList`] because the right
/// side is a query rather than a static value list.
pub const InSubquery = struct {
    /// The value being tested for membership.
    operand: *Expr,
    /// The subquery whose result rows form the candidate set.
    subquery: *SelectStmt,
    /// `true` for `NOT IN`; the executor inverts the membership result.
    negated: bool = false,
};

/// A scalar function invocation like `LENGTH(name)` or `COALESCE(a, b)`.
///
/// Only the syntactic shape is recorded here; which names are valid functions
/// and their arity/typing rules are enforced by the executor, not the parser.
pub const FuncCall = struct {
    /// The function name as written (case handling is the executor's concern).
    name: []const u8, // upper-cased at parse time
    /// Argument expressions in call order; may be empty.
    args: []*Expr,
};

/// One `WHEN cond THEN result` branch of a [`CaseExpr`].
pub const CaseWhen = struct {
    /// The predicate evaluated to decide whether this branch fires.
    cond: *Expr,
    /// The value produced when [`CaseWhen.cond`] is true.
    result: *Expr,
};

/// A searched `CASE WHEN ... THEN ... [ELSE ...] END` expression.
///
/// This is the searched form (each branch carries its own boolean condition),
/// not the simple `CASE expr WHEN value` form. Branches are tried in order and
/// the first true one wins.
pub const CaseExpr = struct {
    /// The ordered branches; the first whose condition is true supplies the
    /// value.
    whens: []CaseWhen,
    /// The `ELSE` value, or `null` when no `ELSE` was given (in which case an
    /// unmatched `CASE` yields SQL `NULL`).
    else_result: ?*Expr,
};

/// An `operand IS NULL` / `operand IS NOT NULL` test.
pub const IsNull = struct {
    /// The expression whose nullness is tested.
    operand: *Expr,
    /// `true` for `IS NOT NULL`.
    negated: bool = false, // IS NOT NULL
};

/// An `operand IN (item, item, ...)` test against a literal expression list.
///
/// For membership against a subquery instead of a static list, see
/// [`InSubquery`].
pub const InList = struct {
    /// The value tested for membership.
    operand: *Expr,
    /// The candidate expressions on the right-hand side of `IN`.
    items: []*Expr,
    /// `true` for `NOT IN`.
    negated: bool = false, // NOT IN
};

/// An `operand LIKE pattern` / `NOT LIKE` string-pattern match.
pub const Like = struct {
    /// The string expression being matched.
    operand: *Expr,
    /// The pattern expression (`%` and `_` wildcards are interpreted by the
    /// executor).
    pattern: *Expr,
    /// `true` for `NOT LIKE`.
    negated: bool = false, // NOT LIKE
};

/// An `operand BETWEEN lo AND hi` / `NOT BETWEEN` inclusive range test.
pub const Between = struct {
    /// The value being range-checked.
    operand: *Expr,
    /// The inclusive lower bound.
    lo: *Expr,
    /// The inclusive upper bound.
    hi: *Expr,
    /// `true` for `NOT BETWEEN`.
    negated: bool = false, // NOT BETWEEN
};

/// A binary operator node joining a left and right sub-expression under one
/// [`OpType`].
///
/// Covers both arithmetic (`+ - * /`) and comparison/logical (`= <> < > AND OR`)
/// operators; precedence and associativity were already resolved by the parser
/// when this tree was built, so the executor evaluates it structurally.
pub const BinaryOp = struct {
    /// Left operand.
    left: *Expr,
    /// The operator. See [`OpType`].
    op: OpType,
    /// Right operand.
    right: *Expr,
};

/// The set of binary operators a [`BinaryOp`] can carry.
///
/// Backed by `u8` so it is cheap to store and switch on. Groups: equality/
/// comparison (`EQ NE GT LT GTE LTE`), logical (`AND OR`), and arithmetic
/// (`PLUS MINUS STAR SLASH`).
pub const OpType = enum(u8) {
    /// `=` equality.
    EQ,
    /// `<>` / `!=` inequality.
    NE,
    /// `>` greater-than.
    GT,
    /// `<` less-than.
    LT,
    /// `>=` greater-than-or-equal.
    GTE,
    /// `<=` less-than-or-equal.
    LTE,
    /// Logical `AND`.
    AND,
    /// Logical `OR`.
    OR,
    /// `+` addition.
    PLUS,
    /// `-` subtraction.
    MINUS,
    /// `*` multiplication (as an operator, not the `SELECT *` star).
    STAR,
    /// `/` division.
    SLASH,
};


/// An `INSERT INTO table (cols...) VALUES (...), (...)` statement.
///
/// Multi-row inserts are represented directly: `rows` is a list of value tuples,
/// each of which must be positionally aligned with [`InsertStmt.columns`]. A
/// single-row insert is just `rows.len == 1`.
pub const InsertStmt = struct {
    /// Target table name.
    table_name: []const u8,
    /// The explicit column list; each row's values are matched to these by
    /// position.
    columns: []const []const u8,
    /// One value tuple per row; each inner slice is index-aligned with
    /// [`InsertStmt.columns`].
    rows: []const []const Expr,
};

/// An `UPDATE table SET ... WHERE ...` statement.
pub const UpdateStmt = struct {
    /// Target table name.
    table_name: []const u8,
    /// The `SET` clauses; see [`Assignment`].
    assignments: []const Assignment,
    /// The `WHERE` predicate, or `null` to update every row.
    where_expr: ?*Expr = null,
};

/// A single `column = value` clause within an [`UpdateStmt`].
pub const Assignment = struct {
    /// The column being written.
    column: []const u8,
    /// The new value expression (evaluated per row).
    value: Expr,
};

/// A `DELETE FROM table WHERE ...` statement.
pub const DeleteStmt = struct {
    /// Target table name.
    table_name: []const u8,
    /// The `WHERE` predicate, or `null` to delete every row.
    where_expr: ?*Expr = null,
};

/// A `CREATE TABLE` statement with its column definitions.
pub const CreateTableStmt = struct {
    /// Name of the table to create.
    table_name: []const u8,
    /// The ordered column definitions. See [`CreateColumn`].
    columns: []const CreateColumn,
    /// `true` when `IF NOT EXISTS` was given, so an existing table is a no-op
    /// rather than an error.
    if_not_exists: bool = false,
};

/// One column definition inside a [`CreateTableStmt`] (also reused by
/// `ALTER TABLE ADD COLUMN` via [`AlterTableAction`]).
///
/// The nullable fields carry inline constraints parsed from the column
/// definition. A per-column `REFERENCES other(col)` is captured by the two
/// `foreign_key_*` fields together (both present or both `null`).
pub const CreateColumn = struct {
    /// The column name.
    name: []const u8,
    /// The declared type as written (e.g. `INT`, `TEXT`, `DECIMAL`); the catalog
    /// interprets it.
    type_name: []const u8, // e.g., "INT", "TEXT", "BOOL"
    /// `true` if this column is (part of) the primary key.
    is_primary_key: bool = false,
    /// Whether the column permits `NULL`; defaults to nullable unless `NOT NULL`
    /// was given.
    is_nullable: bool = true,
    /// `true` if a `UNIQUE` constraint was declared on the column.
    is_unique: bool = false,
    /// The `DEFAULT` expression's raw text, or `null` if no default.
    default_value: ?[]const u8 = null,
    /// For an inline `REFERENCES`, the referenced table; paired with
    /// [`CreateColumn.foreign_key_column`].
    foreign_key_table: ?[]const u8 = null,
    /// For an inline `REFERENCES`, the referenced column; paired with
    /// [`CreateColumn.foreign_key_table`].
    foreign_key_column: ?[]const u8 = null,
};

/// A `CREATE INDEX` statement over one or more columns of a table.
pub const CreateIndexStmt = struct {
    /// Name of the index to create.
    index_name: []const u8,
    /// The table the index is built on.
    table_name: []const u8,
    /// The indexed columns, in key order.
    columns: []const []const u8,
    /// `true` for `CREATE UNIQUE INDEX`, enforcing distinctness of the key.
    is_unique: bool = false,
};

/// A `DROP TABLE table` statement.
pub const DropTableStmt = struct {
    /// The table to drop.
    table_name: []const u8,
};

/// `CREATE FUNCTION name [LANGUAGE wasm] AS '<hex>'` registers a wasm scalar UDF
/// (embed-wasm.md M1). The module bytes are given inline as a hex string literal.
pub const CreateFunctionStmt = struct {
    /// The SQL function name (upper-cased by the executor to match call sites).
    name: []const u8,
    /// The wasm module bytes as a hex string (borrowed from the SQL text).
    wasm_hex: []const u8,
    /// Whether the function returns text (`RETURNS TEXT`); otherwise a numeric result.
    returns_string: bool = false,
};

/// `DROP FUNCTION name` unregisters a wasm scalar UDF.
pub const DropFunctionStmt = struct {
    name: []const u8,
};

/// A `DROP INDEX` statement.
///
/// Both names are kept because an index name is only unique within its table, so
/// the executor needs the table to locate the index in the catalog.
pub const DropIndexStmt = struct {
    /// The index to drop.
    index_name: []const u8,
    /// The table the index belongs to.
    table_name: []const u8,
};

/// A standalone add-foreign-key statement (as opposed to the inline
/// `REFERENCES` captured on [`CreateColumn`]).
pub const CreateForeignKeyStmt = struct {
    /// The constraint name.
    name: []const u8,
    /// The table the constraint is declared on.
    table_name: []const u8,
    /// The constrained columns.
    columns: []const []const u8,
};

/// A `CREATE SEQUENCE` statement for a table-scoped auto-increment source.
pub const CreateSequenceStmt = struct {
    /// The sequence name.
    name: []const u8,
    /// The table the sequence is associated with.
    table_name: []const u8,
};

/// A `DROP` of a named foreign-key constraint from a table.
pub const DropForeignKeyStmt = struct {
    /// The constraint name to drop.
    name: []const u8,
    /// The table the constraint was declared on.
    table_name: []const u8,
};

/// A `DROP SEQUENCE` statement.
pub const DropSequenceStmt = struct {
    /// The sequence name to drop.
    name: []const u8,
    /// The table the sequence was associated with.
    table_name: []const u8,
};

/// One output item of a `SELECT` list: an expression plus an optional `AS` alias.
///
/// The output name is `alias` when present, otherwise derived from the
/// projection expression. See [`ProjectionExpr`] for the kinds of thing that can
/// be projected.
pub const Projection = struct {
    /// The `AS name` alias, or `null` to use the expression's implicit name.
    alias: ?[]const u8 = null, // e.g., "AS total_sales"
    /// What is being projected. See [`ProjectionExpr`].
    expr: ProjectionExpr,
};

/// The aggregate functions the `SELECT` list understands, backed by `u8`.
pub const AggregateKind = enum(u8) { COUNT, AVG, MIN, MAX, SUM };

/// The three shapes a projection can take in the `SELECT` list.
///
/// Kept deliberately narrow (a bare column, `*`, or an aggregate) rather than a
/// full [`Expr`], because the executor's projection/grouping paths only support
/// these forms; richer per-row computation is done in `WHERE`/`HAVING`.
pub const ProjectionExpr = union(enum) {
    /// A single named column, e.g. `SELECT name`.
    column: []const u8, // e.g., "name"
    /// `SELECT *`, all columns of the row.
    star, // *
    /// An aggregate such as `COUNT(*)` or `SUM(price)`. See [`AggregateCall`].
    aggregate: AggregateCall, // e.g., COUNT(*), SUM(price)
};

/// An aggregate invocation in the `SELECT` list, e.g. `COUNT(DISTINCT id)`.
pub const AggregateCall = struct {
    /// Which aggregate function. See [`AggregateKind`].
    kind: AggregateKind,
    /// `true` for `COUNT(DISTINCT ...)` and similar, so duplicates are collapsed
    /// before aggregation.
    distinct: bool = false,
    /// What the aggregate is applied to. See [`AggregateArg`].
    argument: AggregateArg,
};

/// The argument to an [`AggregateCall`].
///
/// `star` exists so `COUNT(*)` (count rows, ignore nullness) is distinguishable
/// from `COUNT(column)` (count non-null values); `expression` allows aggregating
/// a computed value.
pub const AggregateArg = union(enum) {
    /// The `*` argument, as in `COUNT(*)`.
    star, // COUNT(*)
    /// A single column argument, as in `SUM(price)`.
    column: []const u8, // COUNT(age)
    /// An arbitrary expression argument, as in `AVG(price * qty)`.
    expression: *Expr, // For complex stuff later (e.g., SUM(price * quantity))
};

/// The join flavours the executor supports, backed by `u8`.
pub const JoinType = enum(u8) {
    /// `INNER JOIN`, only matching pairs.
    inner,
    /// `LEFT [OUTER] JOIN`, all left rows, nulls for unmatched right.
    left,
    /// `RIGHT [OUTER] JOIN`, all right rows, nulls for unmatched left.
    right,
    /// `FULL [OUTER] JOIN`, all rows from both sides.
    full,
};

/// One `JOIN` clause of a [`SelectStmt`]: a joined table with its `ON`
/// predicate.
///
/// Only equi/theta joins with an explicit `ON` expression are represented; the
/// driving (left) table is [`SelectStmt.table_name`], and multiple joins chain
/// left-to-right through [`SelectStmt.joins`].
pub const JoinExpr = struct {
    /// The kind of join. See [`JoinType`].
    join_type: JoinType,
    /// The table being joined in on the right side.
    right_table: []const u8,
    /// The optional `[AS] alias` for the right table (e.g. `JOIN orders o`), or
    /// `null`. Captured so a qualified reference like `o.total` can be resolved;
    /// the executor's single-table normalisation currently uses only the base
    /// table's alias, so join-side alias resolution remains a follow-up.
    right_alias: ?[]const u8 = null,
    /// The `ON` predicate relating the two sides.
    on_expr: *Expr,
};

/// One `ORDER BY` key: a column and its sort direction.
pub const OrderKey = struct {
    /// The column to sort on.
    column: []const u8,
    /// `true` for `DESC`, `false` (default) for `ASC`.
    desc: bool = false,
};

/// A fully parsed `SELECT`, the widest node in the tree.
///
/// This gathers every optional clause the dialect allows. The executor reads them
/// in the conventional logical order (from/join, where, group/having, distinct,
/// order, limit/offset). Because a [`SelectStmt`] is also reachable by pointer
/// from [`Expr.subquery`] and [`InSubquery`], the same struct serves both
/// top-level queries and nested subqueries.
pub const SelectStmt = struct {
    /// The driving table in the `FROM` clause.
    table_name: []const u8,
    /// The optional `[AS] alias` on the driving table (e.g. `FROM orders o`), or
    /// `null`. When present, a qualifier matching it (or `table_name`) on a column
    /// reference is stripped by the executor's single-table normalisation.
    table_alias: ?[]const u8 = null,
    /// Zero or more `JOIN` clauses applied left-to-right; empty by default. See
    /// [`JoinExpr`].
    joins: []JoinExpr = &.{},
    /// The output list. See [`Projection`].
    projections: []Projection, // <-- CHANGED from []const []const u8
    /// The `WHERE` predicate, or `null` for no filter.
    where_expr: ?*Expr = null,
    /// The `GROUP BY` column list, or `null` when absent.
    group_by: ?[]const []const u8 = null, // <-- ADD THIS
    /// The `HAVING` predicate (post-aggregation filter), or `null` when absent.
    having_expr: ?*Expr = null,
    /// The `ORDER BY` keys, or `null` when unordered. See [`OrderKey`].
    order_by: ?[]const OrderKey = null,
    /// The `LIMIT` row cap, or `null` for unlimited.
    limit: ?u32 = null,
    /// The `OFFSET` to skip, or `null` for none.
    offset: ?u32 = null, // B-4: OFFSET n (skip after ORDER BY, before LIMIT)
    /// `true` for `SELECT DISTINCT`, collapsing duplicate output rows.
    distinct: bool = false, // B-4: SELECT DISTINCT
};

/// The serialisation formats `EXPORT`/`IMPORT` can move data in, backed by `u8`.
pub const ExportFormatKind = enum(u8) {
    /// Comma-separated values.
    CSV,
    /// JSON documents.
    JSON,
    /// Binary BSON.
    BSON,
};

/// What an `EXPORT` targets: a single table, everything, or a named manifest.
pub const ExportTarget = union(enum) {
    /// A single table by name.
    table: []const u8,
    /// The whole database (all tables).
    all: void,
    /// A named manifest describing a set of objects to export.
    manifest: []const u8,
};

/// An `EXPORT` statement moving data out to a file.
///
/// `format` and `file_path` are optional so the executor can apply defaults
/// (infer the format from the path, or write to a default location).
pub const ExportStmt = struct {
    /// What to export. See [`ExportTarget`].
    target: ExportTarget,
    /// The chosen format, or `null` to let the executor default/infer it. See
    /// [`ExportFormatKind`].
    format: ?ExportFormatKind = null,
    /// The destination path, or `null` for a default.
    file_path: ?[]const u8 = null,
};

/// What an `IMPORT` targets. Mirrors [`ExportTarget`] on the read side.
pub const ImportTarget = union(enum) {
    /// A single table by name.
    table: []const u8,
    /// The whole database (all tables).
    all: void,
    /// A named manifest describing a set of objects to import.
    manifest: []const u8,
};

/// An `IMPORT` statement loading data in from a file. Symmetric with
/// [`ExportStmt`].
pub const ImportStmt = struct {
    /// What to import into. See [`ImportTarget`].
    target: ImportTarget,
    /// The source format, or `null` to default/infer. See [`ExportFormatKind`]
    /// (the same format enum is reused for import).
    format: ?ExportFormatKind = null,
    /// The source path, or `null` for a default.
    file_path: ?[]const u8 = null,
};

/// `BEGIN`, opens an explicit transaction. Carries no payload; the keyword is
/// the whole statement.
pub const BeginStmt = struct {};

/// `COMMIT`, durably applies the open transaction. Carries no payload.
pub const CommitStmt = struct {};

/// `ROLLBACK`, undoes the transaction, or unwinds to a named savepoint.
///
/// When `to_savepoint` is set the statement is `ROLLBACK TO SAVEPOINT name`,
/// which discards work back to that point but keeps the transaction open;
/// otherwise it aborts the whole transaction.
pub const RollbackStmt = struct {
    /// The savepoint to unwind to, or `null` for a full rollback.
    to_savepoint: ?[]const u8 = null,
};

/// `SAVEPOINT name`, marks a named point within the current transaction that a
/// later [`RollbackStmt`] can return to.
pub const SavepointStmt = struct {
    /// The savepoint name.
    name: []const u8,
};

/// `RELEASE SAVEPOINT name`, forgets a savepoint (merges its work into the
/// enclosing transaction) without rolling anything back.
pub const ReleaseSavepointStmt = struct {
    /// The savepoint name to release.
    name: []const u8,
};

/// `CREATE USER`, creates an authentication principal with a role.
pub const CreateUserStmt = struct {
    /// The new user's name.
    username: []const u8,
    /// The user's password (as supplied in the statement text).
    password: []const u8,
    /// The role granted to the user at creation.
    role: []const u8,
};

/// `DROP USER`, removes an authentication principal.
pub const DropUserStmt = struct {
    /// The user to remove.
    username: []const u8,
};

/// `ALTER USER name IDENTIFIED BY 'password'`, rotates a user's password.
/// The role is preserved; only the stored credential changes.
pub const AlterUserStmt = struct {
    /// The user whose password is being changed.
    username: []const u8,
    /// The new password (as supplied in the statement text).
    password: []const u8,
};

/// `LOGIN`, authenticates the current session as a user.
pub const LoginStmt = struct {
    /// The user logging in.
    username: []const u8,
    /// The supplied password to verify.
    password: []const u8,
};

/// The mutations an `ALTER TABLE` can perform in this dialect.
///
/// Deliberately limited to the two the executor implements: add a column, or
/// rename the table.
pub const AlterTableAction = union(enum) {
    /// `ADD COLUMN` with a full column definition. See [`CreateColumn`].
    add_column: CreateColumn,
    /// `RENAME TO new_name`; the payload is the new table name.
    rename_table: []const u8,
};

/// An `ALTER TABLE table action` statement.
pub const AlterTableStmt = struct {
    /// The table being altered.
    table_name: []const u8,
    /// The change to apply. See [`AlterTableAction`].
    action: AlterTableAction,
};

/// `ANALYZE table`, recomputes the planner statistics for a table so the
/// cost-based optimiser has fresh cardinality/selectivity estimates.
pub const AnalyzeTableStmt = struct {
    /// The table to analyse.
    table_name: []const u8,
};

/// `BACKUP`, writes a consistent copy of the database to a path.
pub const BackupStmt = struct {
    /// The destination path for the backup.
    backup_path: []const u8,
};

/// `CREATE ROLE`, defines a named role that privileges can be granted to.
pub const CreateRoleStmt = struct {
    /// The role name.
    role_name: []const u8,
};

/// `GRANT privilege ON object TO principal`, adds an access-control grant.
pub const GrantStmt = struct {
    /// The privilege being granted (e.g. `SELECT`, `INSERT`).
    privilege: []const u8,
    /// The object the privilege applies to, or `null` for a database-wide grant.
    object: ?[]const u8,
    /// The user or role receiving the privilege.
    to_principal: []const u8,
};

/// `REVOKE privilege ON object FROM principal`, the inverse of [`GrantStmt`].
pub const RevokeStmt = struct {
    /// The privilege being revoked.
    privilege: []const u8,
    /// The object the privilege applied to, or `null` for a database-wide
    /// revoke.
    object: ?[]const u8,
    /// The user or role losing the privilege.
    from_principal: []const u8,
};
