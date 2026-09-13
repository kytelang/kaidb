# NovaDB SQL92 compliance and correctness audit

Status: analysis (2026-08-12). Scope: `src/sql/{lexer,parser,ast}.zig`, `src/query/{query_executor,iterator}.zig`,
`src/schema/types.zig`. This is the relational-engine track. It must be addressed BEFORE the multi-model work
(`multi-model-design.md`), because a shaky relational engine should not spawn document and graph engines on the
same base.

## Headline

NovaDB implements a small operational subset of SQL, not SQL92. The expression grammar is one comparison of two
primaries (`parser.zig:97`), and the lexer has no tokens for most SQL92 predicates and operators. But the more
serious problem is not the missing features: it is that several PARSED features are silently ignored or
miscompiled at execution time, so applications get wrong results with no error. Those correctness defects are
worse than a parse failure and must be fixed first.

## Tracking table

Status values: `not started`, `in progress`, `blocked`, `done`, `deferred`. "Master" cross-references the
consolidated plan at `../../PLATFORM-PLAN.md`. Scope for this push is only what the acceptance-slice app's
queries need; the full SQL92 matrix is not in scope.

| ID | Item | Master | Priority | Status |
|----|------|--------|----------|--------|
| B-1a | Execute ORDER BY (parsed, never run) | B-1 | P0 | done (AST OrderKey + ASC/DESC parse + executor sort-before-limit, numeric-aware; slice.sh check 7 green) |
| B-1b | Stop silent type-to-TEXT collapse for the app's types | B-1 | P0 | done for the integer families (BIGINT/SMALLINT/INT2/4/8/TINYINT/SERIAL -> INT64, round-trip + numeric range verified). Float/decimal/temporal still map to TEXT on purpose: mapping them to FLOAT64/TIMESTAMP without the INSERT value codec makes the row silently drop (verified), so they wait on the codec work |
| B-1c | Bind `?` placeholders in WHERE (currently false) | B-1 | P0 | handled upstream (command.substituteParams replaces bound placeholders with literals before parse; the evaluator never sees a real bound `?`) |
| B-1d | Reconcile the two divergent WHERE evaluators | B-1 | P0 | done (executor UPDATE/DELETE compareValues now delegates to the numeric-aware iterator.compareValues; verified salary>1000 UPDATE hits only the right rows) |
| B-1e | Honour COUNT(DISTINCT) | B-1 | P1 | done (per-accumulator dedup set; COUNT(DISTINCT salary)=3 vs COUNT(salary)=4 verified) |
| B-2 | Expression engine (parser + evaluator + 3VL): IN, LIKE, BETWEEN, IS NULL, NOT, arithmetic | B-2 | P0 | done (precedence-correct parser OR/AND/NOT/predicate/additive/mul; iterator.eval3 three-valued evaluator shared by SELECT/UPDATE/DELETE; live-verified IN/NOT IN/BETWEEN/LIKE %,_/NOT/arithmetic/AND-OR; scalar functions still TODO) |
| B-3 | Guarded UPDATE ... WHERE revision = ? (atomic CAS primitive) | B-3 | P1 | done (SET RHS is now a scalar expression so `revision = revision + 1` works; a single guarded UPDATE runs under the per-table exclusive lock = atomic read-modify-write; rows_affected=1 on match, 0 on stale revision. Verified. BEGIN/COMMIT already exist) |
| B-4 | Remaining clauses the app uses (OFFSET with working ORDER BY, DISTINCT) | B-4 | P1 | done (SELECT DISTINCT dedup + OFFSET n, applied as DISTINCT->OFFSET->LIMIT after the ORDER BY sort; pagination LIMIT/OFFSET verified) |
| B-5 | Subqueries, set ops, HAVING, DDL completeness, real type storage | none | P3 | deferred |

## Part 1: correctness defects (silent wrong results), FIX FIRST

These accept valid-looking SQL and return wrong data with no warning. They are more dangerous than the missing
features and should be the first work item.

1. **`ORDER BY` is parsed then never executed.** The parser reads the column list (`parser.zig:366-379`) but
   the executor sorts nothing: the only use of the select tail is `sel.limit` (`query_executor.zig:1429`), and
   `order_by` appears nowhere in `query/`. Every list and pagination query returns rows in physical order with
   no error. This blocks essentially every application and ORM. Highest severity.
2. **Real data types silently collapse to TEXT.** `CREATE TABLE t (amt DECIMAL(10,2), d DATE)` succeeds, but
   the type mapping (`query_executor.zig:979-985`) only recognises INT/CHAR/VARCHAR/BOOL and defaults everything
   else to TEXT. DATE, TIME, TIMESTAMP, DECIMAL, NUMERIC, FLOAT, DOUBLE, BLOB, SMALLINT, BIGINT are stored and
   compared as strings, which breaks range queries and sorting. No DDL error is raised. Column length and
   precision or scale are also discarded (`parser.zig:554-562`; `size` is hardcoded to 255 at `:988`).
3. **`COUNT(DISTINCT col)` is accepted but not deduplicated.** The `distinct` flag is set (`parser.zig:215-218`)
   but the aggregate accumulators never dedupe. Silent wrong counts.
4. **A `?` placeholder in a WHERE filter evaluates to false.** `iterator.zig:125` returns `false` for a
   placeholder, so a parameterised filter matches nothing. Prepared-statement filters silently return empty.
5. **Two divergent WHERE evaluators exist.** `iterator.zig:64-118` compares numbers correctly, but a secondary
   `compareValues` (`query_executor.zig:2538-2564`) does lexicographic comparison (wrong for numbers). Confirm
   which is live and delete or fix the other to prevent a regression.

## Part 2: compliance matrix (condensed)

Legend: `ok` supported, `~` partial or parsed-but-not-honoured, `NO` missing.

| Area | Supported | Partial / silently-wrong | Missing |
|---|---|---|---|
| DDL | CREATE/ALTER(add col, rename table)/DROP TABLE, column PK, NOT NULL, single-col FK REFERENCES, CREATE/DROP INDEX | UNIQUE (flag dropped), DEFAULT (literals only), FK actions parsed-never (`types.zig:71` enums exist, `parser.zig:603` ignores them), type precision/scale (discarded) | table-level constraints, composite PK, CHECK, CREATE VIEW, schemas, DROP..IF EXISTS/CASCADE, RENAME/DROP/ALTER COLUMN |
| DML | INSERT single row, UPDATE, DELETE | INSERT needs an explicit column list; UPDATE/INSERT RHS is a primary only (no `col = col+1`) | multi-row INSERT, INSERT..SELECT, expression values |
| SELECT projection | column, `*`, the five aggregates, `AS` alias | COUNT(DISTINCT) not honoured | expressions/arithmetic/functions in the list, `t.*`, table aliases, aggregate on expression |
| SELECT WHERE | `= <> != > < >= <=`, AND, OR | placeholder `?` -> false; NULL -> false (no 3VL) | `IN`, `LIKE`, `BETWEEN`, `IS NULL`, unary `NOT`, arithmetic, functions, subqueries |
| SELECT joins | INNER/LEFT/RIGHT/FULL JOIN ON (equality) | ON is a single comparison (AND-chained ok) | CROSS, NATURAL, USING, comma joins, subquery in FROM |
| SELECT other | GROUP BY, LIMIT | ORDER BY parsed-never-sorted; COUNT(DISTINCT) ignored | HAVING (AST field unused), DISTINCT, OFFSET, ASC/DESC, NULLS ordering, UNION/INTERSECT/EXCEPT, CASE, COALESCE, NULLIF, CAST |
| Data types | INT(->INT64), CHAR/VARCHAR/TEXT(->TEXT), BOOL | none | DATE/TIME/TIMESTAMP, DECIMAL/NUMERIC, REAL/DOUBLE/FLOAT, BLOB, SMALLINT/BIGINT (all -> TEXT) |
| Transactions | BEGIN, COMMIT, ROLLBACK (engine has MVCC) | none | SET TRANSACTION ISOLATION LEVEL, SAVEPOINT/RELEASE/ROLLBACK TO |

## Part 3: gap list, ranked by how much it blocks real applications

1. ORDER BY silently ignored (correctness defect 1). CRITICAL.
2. No `IN` predicate (no lexer token). Blocks most ORMs (`WHERE id IN (?, ?)`, eager loads). CRITICAL.
3. No `IS NULL` / `IS NOT NULL` and no three-valued logic. CRITICAL.
4. No subqueries of any kind (scalar, IN, EXISTS, correlated, derived tables). CRITICAL.
5. No expressions in the SELECT list and no WHERE arithmetic or functions. CRITICAL.
6. No `LIKE`. HIGH.
7. No `DISTINCT`; COUNT(DISTINCT) ignored. HIGH.
8. No set operations (UNION/UNION ALL/INTERSECT/EXCEPT). HIGH for reporting.
9. No `OFFSET`; with defect 1, pagination is unusable. HIGH.
10. Real data types collapse to TEXT (correctness defect 2). HIGH.
11. No `HAVING`. MEDIUM-HIGH.
12. No multi-row INSERT, no INSERT..SELECT. MEDIUM-HIGH.
13. FK referential actions not parseable. MEDIUM.
14. No CASE, COALESCE, NULLIF, CAST. MEDIUM.
15. No table-level constraints, composite PK, CHECK, CREATE VIEW, schemas. MEDIUM.
16. No BETWEEN, logical NOT, CROSS/NATURAL/USING joins, comma joins. MEDIUM.
17. UNIQUE not enforced; `?` in a filter not bound (correctness defect 4). MEDIUM.
18. No SAVEPOINT, no SQL-level isolation control (engine has MVCC, not exposed). LOW-MEDIUM.

## Part 4: the single highest-leverage fix, a real expression engine

Most of the CRITICAL and HIGH gaps share one root: the parser has no expression tree. `parseComparison`
(`parser.zig:97-114`) builds exactly one comparison of two primaries, `parsePrimary`'s `(` branch parses an
expression not a SELECT, and there is no function-call grammar and no arithmetic rule (the `OpType` enum has
PLUS/MINUS/STAR/SLASH but no parser rule ever builds them). Replacing this with a proper precedence-climbing
(Pratt) expression parser plus a matching evaluator, over a real `Expr` tree with three-valued logic, unblocks
in one change: arithmetic, function calls, `IN`, `LIKE`, `BETWEEN`, `IS NULL`, `CASE`, `COALESCE`, `NULLIF`,
`CAST`, unary `NOT`, and expressions in the SELECT list and ON clauses. Subqueries then become an `Expr` leaf
that runs a nested plan. This is the one investment that most moves the compliance number.

## Part 5: recommended sequencing (relational engine to a real bar)

1. Fix the correctness defects (Part 1): execute ORDER BY, map real data types (stop the silent TEXT
   fallthrough), honour COUNT(DISTINCT), bind `?` in filters, and reconcile the two WHERE evaluators. These are
   the dangerous silent-wrong items and several are small.
2. Build the real expression engine (Part 4): the Pratt parser + evaluator + three-valued logic + a
   function-call grammar. This unblocks `IN`, `LIKE`, `BETWEEN`, `IS NULL`, arithmetic, functions, and CASE
   together.
3. Add the remaining high-value clauses: DISTINCT, OFFSET (with the now-working ORDER BY), HAVING, set
   operations, multi-row INSERT and INSERT..SELECT.
4. Add subqueries (scalar, IN, EXISTS, correlated, derived tables) once the expression engine and a nested
   plan runner exist.
5. Add the DDL completeness items: table-level constraints, composite PK, CHECK, real type storage, FK
   actions, and views.

Every real type stored correctly (step 1) and the expression engine (step 2) are the two that turn NovaDB from
"runs hand-written toy queries" into "runs typical ORM-generated SQL". Only after this relational bar is met
should the document and graph engines in `multi-model-design.md` begin.
