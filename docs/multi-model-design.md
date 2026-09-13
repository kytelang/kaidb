# NovaDB multi-model design: document, GraphQL, graph

Status: design (2026-08-12). This deepens `multi-model-feasibility.md` into a concrete, file-level plan. It
corrects one load-bearing claim in that note (see below) and sequences the work with effort estimates and
touch points. Prerequisite: the relational engine must reach the bar in `sql92-compliance.md` first. A shaky
relational engine should not spawn two more engines on the same core.

## Two corrections to the feasibility note

1. **BSON is not in the NovaDB codebase, but a pure-Zig BSON library exists and should be vendored.** In this
   repo, BSON is only a SQL keyword token (`src/sql/lexer.zig:20,229`, `ast.zig:200`, `parser.zig:681`) with a
   single export branch treating JSON and BSON identically (`query_executor.zig:1496`); there is no encoder or
   decoder here. HOWEVER, a complete pure-Zig BSON library exists at `/Users/kamlesh/plancksystems/bson`
   (`.name = .bson`, v0.1.0): `encode`/`decode`/`encodeFast`, a `BsonDocument` with typed getters including
   `getNestedField(field_path)`, `BsonArray`, `ObjectId`, `Decimal128`, and `json_interop`. Vendor it as a Zig
   module dependency (like `yaml`/`tls`/`utils` in `build.zig.zon`) and store documents as native BSON. Its
   `getNestedField` also supplies the document-path value getter that stage D-4 below assumed had to be written
   by hand. `std.json` remains a fine bring-up codec, but BSON is the target and it is a dependency-away.
2. **The engine is already document-ready in a stronger sense than the note claims.** The relational engine
   already stores rows as JSON documents with embedded MVCC on one code path. `getVisibleVersion`
   (`query_executor.zig:2612`) branches on `actual_bytes[0] == '{'` and reads an embedded
   `{"versions":[{"xmin","xmax","data"}]}` array; `writeNewVersion` (`:2722`) already accepts a
   `std.json.ObjectMap`. A collection is therefore the existing JSON-value-in-a-B+Tree path with the fixed
   column schema removed, not a new storage format.

## Tracking table

Status values: `not started`, `in progress`, `blocked`, `done`, `deferred`. "Master" cross-references the
consolidated plan at `../../PLATFORM-PLAN.md`. This whole document is a P3 non-goal for the current push: it
starts only after the relational slice is certified. Every row is therefore `deferred` today.

| ID | Item | Master | Priority | Status |
|----|------|--------|----------|--------|
| D-1 | Collection = B+Tree keyed by doc-id (reuse JSON-in-B+Tree path) | ND-DOC | P3 | deferred |
| D-2 | Catalog generalization via free-form `sys.objects` type ("COLLECTION"/"DOC_INDEX") | ND-DOC | P3 | deferred |
| D-3 | Field/path indexes reuse the SQL index machinery | ND-DOC | P3 | deferred |
| D-4 | CRUD + Mongo-filter surface into the existing executor (FilterIterator) | ND-DOC | P3 | deferred |
| D-5 | Document opcode under existing wire negotiation | ND-DOC | P3 | deferred |
| D-6 | MVCC/WAL/recovery seams (the two that are not free) | ND-DOC | P3 | deferred |
| GQL-1 | GraphQL as a Nova layer over the Connection seam (no engine change) | ND-GQL | P3 | deferred |
| G-1 | Graph adjacency encoding (composite key + rangeScan) | ND-GRAPH | P3 | deferred |
| G-2 | Traversal over the Volcano operator framework | ND-GRAPH | P3 | deferred |
| G-3 | Graph catalog extension + Cypher-subset language | ND-GRAPH | P3 | deferred |

## Part 1: document (NoSQL) model

Feasibility HIGH, effort MEDIUM (~3 to 4 focused weeks), risk LOW to MEDIUM (the risk is the recovery and
replication seams, item 6 below).

### 1.1 Collection = one B+Tree keyed by doc-id

`BPlusTree` (`storage/btree.zig:179`) is a pure ordered KV store: keys and values are `[]const u8`, `insert`
(`:276`) spills any value over `PAGE_SIZE/8` into an overflow chain and `search` reassembles it. So a
collection is one B+Tree keyed by doc-id with a serialised document value, and large documents overflow for
free.

### 1.2 Catalog generalization, using the hook that already exists

`SystemCatalog` (`schema/catalog.zig:6`) is relational-shaped (tables, indexes, foreign_keys, sequences), and
every table carries a fixed `columns` and `fixed_size` (`schema/table.zig:5-9`). But objects are registered in
a generic master registry `sys.objects` where each entry is `ObjectMetadata{ id, name, type: []const u8,
root_page_id }` and `type` is a FREE-FORM STRING (`schema/types.zig:323-375`), today only "TABLE"/"INDEX".
`loadCatalog` dispatches on that string (`schema/database.zig:2481`). So "COLLECTION" and "DOC_INDEX" slot in
with no page-1 format change. Add a `Collection` struct + list to the catalog, a `CollectionMetadata`
(serialize/deserialize) beside `TableMetadata` (`types.zig:251`), `sys.collections` + `sys.doc_indexes` system
trees in `ensureSystemCatalogTables` (`database.zig:2367`), and `createCollection`/`getCollectionTree`
mirroring `createTable`/`getTableTree` (`:2682`/`:2876`) minus the column plumbing.

### 1.3 Field/path indexes reuse the SQL index machinery verbatim

The SQL secondary index is already a `(key : "")` B+Tree: creation `query_executor.zig:1026-1068`, maintenance
on insert `:1135-1162` (`std.mem.join(":", ...)` then `idx_tree.insert(key, "")`), on update `:1881-1907`, and
the read-side `IndexScanIterator` (`iterator.zig:261`) chosen at `:516`. A document index over path `age` is
byte-identical (`field-value ‖ doc-id`). The path value getter is provided by the vendored BSON library's
`BsonDocument.getNestedField("a.b.c")` (or a `std.json.Value` walk on the bring-up codec), so the only
genuinely new code is an order-preserving key encoding for non-string types, modelled on
`Index.packKey` (`schema/table.zig:148`, big-endian). The SQL path cheats by stringifying everything; documents
need correct numeric ordering.

### 1.4 CRUD and the filter surface plug into the existing executor

The executor already has a small JSON expression evaluator (`evaluateExpr`/`getValue`/`compareValues`,
`query_executor.zig:2509-2564`). Document CRUD reuses the MVCC write/read seams: `insertDoc` reuses
`writeNewVersion` (`:2722`), `findDocs` reuses `getVisibleVersion` (`:2612`) for MVCC-correct reads and the
`IndexScanIterator` when the filter matches an indexed path, and update/delete mirror the SQL MVCC + index
blocks (`:1881`, `:1935`). The filter surface is a small compiler from a Mongo-style filter document
(`{age:{$gt:30}}` as `std.json.Value`) into the existing `ast.Expr` predicate tree, so it flows straight into
the existing `FilterIterator` (`iterator.zig:348`). This reuses the whole Volcano pipeline rather than building
a second one.

### 1.5 Wire protocol: a document opcode under the existing negotiation

The live framing is Postgres-style in `proto/wire.zig` (`Frontend` opcodes at `:17`), dispatched by the
session loop switch (`proto/session.zig:169-253`); the wire is already versioned and negotiated
(`proto/protocol.zig:20-38`). Add a `doc_op` opcode to `wire.Frontend`, a decode/encode pair, and a `.doc_op`
arm in the session switch that calls a new `executor.executeDoc`. Give it a `model` tag byte so the graph model
can reuse the same envelope later (the D10 point). This is additive under `negotiate()`, so no protocol break.
There is precedent for out-of-band commands: `SET FENCE EPOCH` and `SET DURABLE COMMIT` are string-prefix
dispatched at `query_executor.zig:674,688`.

### 1.6 MVCC / WAL / recovery: mostly free, two seams are not

A collection write is a `BPlusTree.insert`/`writeNewVersion` + a WAL append, so the per-statement txn wrapper
(`query_executor.zig:848`), WAL (`logWalRecordWithLsn`, `:2688`), visibility (`:2649`), and locking
(`db.rw_lock` + per-table `GroupLock`, `:918/:944`) all work on a generic `table_name` string and come free.
Two dispatch seams hard-code the relational object types and MUST get collection branches or collections
silently vanish:
- Recovery Phase-1 catalog replay dispatches on literal `"sys.objects"`/`"sys.tables"`/`"sys.indexes"`
  (`database.zig:1143,1415,1426,1694`) and `loadCatalog` on the `"TABLE"`/`"INDEX"` type strings (`:2481`).
- Replication follower re-creates tables/indexes from the leader stream (`applyTableCreateFromLeader` `:1255`,
  `applyIndexCreateFromLeader` `:1300`, driven from `applyStream` `:1378`). A collection needs an
  `applyCollectionCreateFromLeader` peer or it will not propagate to replicas.

### 1.7 Staged plan (document)

| Stage | Work | Files | Effort |
|---|---|---|---|
| D-1 | Collection catalog + storage | `catalog.zig:6`, `types.zig:251`, `database.zig:2367/2682/2876` | M (~3-4 d) |
| D-2 | Document CRUD in the executor | `query_executor.zig` (near `:636`, reuse `:2722/:2612/:848`) | M (~4-5 d) |
| D-3 | Filter surface (Mongo-JSON -> ast.Expr) | new `query/doc_filter.zig`, reuse `iterator.zig:348` | M (~3 d) |
| D-4 | Document field/path indexes | JSON-path getter + order-preserving key (model `table.zig:148`); hooks from `query_executor.zig:1135/1881/516` | M (~4 d) |
| D-5 | Wire protocol doc opcode | `proto/wire.zig:17`, `proto/session.zig:169` | S-M (~2-3 d) |
| D-6 | Durability + replication seams | `database.zig:1143/1415/1694/2481`, `:1255/1378/2140` | M (~3-4 d), HIGHEST risk |

Biggest risks: the false BSON assumption (build on std.json); the ~6 recovery/replication sites that hard-code
relational types (over-test with the Stage-3 concurrency fuzzer and STRESS cases); two MVCC storage encodings
coexisting (embedded-JSON versions array vs packed-row + undo chain, `:2623` vs `:2656` -- pin collections to
the JSON path); order-preserving key encoding for typed fields; and the global concurrency ceiling
(`db.rw_lock`) bounding document writes exactly as it bounds SQL.

## Part 2: GraphQL as a Nova layer (does NOT need the graph engine)

The graph DATA model and GraphQL the API share only a name. GraphQL resolves over the model-agnostic
`Connection` seam (`lang/src/std/data/db.nova:591-616`: `async fn query(sql, params): ResultSet`, and
`query<T>` at `:674`), which the NovaDB driver already implements. A `user { posts { comments } }` selection is
three resolver levels, each a `conn.query` call, over relational or document. So GraphQL needs NO native graph
engine and touches zero engine code. Every building block already exists in Nova:

- HTTP entry: `POST /graphql` is one route (`web/routing.nova:120` or `web/app.nova:181`).
- Request envelope `{query, variables, operationName}`: the existing `serde.json.parse` (`serde/json.nova:318`)
  and `json.stringify` (`:323`) for the response.
- Resolvers call the DB through `conn.query` / `db.query<T>` exactly as the ORM does (`orm.nova:73`).
- Async batching: `Connection.query` is `async` and Nova has `spawn`/`await`/`when_all`, so a DataLoader fires
  N sibling queries concurrently or coalesces N `WHERE key = ?` into one `WHERE key IN (...)` and demuxes the
  `ResultSet.rows` back by key. This is the N+1 fix.
- Per-request connection scope: `app.useServices` + the mediator per-request scope (`web/app.nova:108-113`).

There is no GraphQL code anywhere today (greenfield over a complete toolkit).

### 2.1 Staged plan (GraphQL in Nova), effort MEDIUM (~6 to 8 weeks), zero engine change

| Stage | Work | Files | Effort |
|---|---|---|---|
| GQL-1 | GraphQL document lexer + parser (selection sets, fields, args, variables, fragments) -> AST | new `lang/src/std/web/graphql/{lexer,ast,parser}.nova` | M (~2 wk) |
| GQL-2 | Schema + resolver registry (Nova structs + a resolver map; derive base types from ORM structs) | new `graphql/schema.nova` | M (~1-1.5 wk) |
| GQL-3 | Executor / resolver dispatch: walk the selection set, invoke resolvers, assemble a JsonValue | new `graphql/executor.nova` | M (~2 wk) |
| GQL-4 | DataLoader batching (`DataLoader<K,V>`, key coalescing, `IN (...)` batch, async fan-out) | new `graphql/dataloader.nova` | M (~1 wk) |
| GQL-5 | HTTP binding `POST /graphql` (+ error shape, variables) | `web/routing.nova:120` | S (~2-3 d) |

## Part 3: graph data model in the engine

Feasibility MEDIUM, effort LARGE (~2 to 3 engineer-months). The storage maps directly and the traversal
operators reuse the existing framework; the graph query LANGUAGE dominates the cost.

### 3.1 Adjacency encoding is free

The engine already builds composite keys (`std.mem.join(":", parts)` at `query_executor.zig:1064`, and
`Index.packKey` fixed-width big-endian at `table.zig:148-211`), and `rangeScan` (`btree.zig:452`) does an
`O(log n)` seek then sequential sibling-link walk with a byte-order end bound. So:
- Node tree: key = node-id (big-endian), value = property map (JSON, later BSON).
- Out-edge tree: key = `src-id ‖ edge-type ‖ dst-id`; value = edge properties.
- In-edge (reverse) tree: key = `dst-id ‖ edge-type ‖ src-id`.
"Out-edges of N" = `rangeScan(N, N+1)`; "out-edges of N of type T" = `rangeScan(N‖T, N‖T+1)`. This is exactly
the `IndexScanIterator` access pattern.

### 3.2 Traversal reuses the Volcano operator framework

The executor is already a Volcano iterator engine (`iterator.zig`; chained at `query_executor.zig:558-630`).
Graph traversal is new iterators in the same shape: `AdjacencyScanIterator` (a `rangeScan` over the edge tree,
near-identical to `IndexScanIterator`); `ExpandIterator` (for each child row, open an adjacency scan on that
node, emit (row, neighbor) pairs -- structurally a nested-loop join with an adjacency inner side);
`VarLengthExpandIterator` (BFS/DFS to depth k: a frontier worklist, an adjacency scan per step, a visited-set).
MVCC, the per-tree `structure_lock`, WAL, and recovery are inherited because traversal is pure reads through
the same `pool.fetchPage`.

### 3.3 The cost is the catalog extension and the query language

Catalog needs node labels, edge types, and the three tree roots per graph (the `table_roots` name->root
mechanism at `query_executor.zig:2137` already maps logical objects to roots, so graph trees register the same
way). The LANGUAGE is the bulk: a Cypher subset (`MATCH (a)-[:T]->(b) WHERE ... RETURN ...`) needs its own
lexer + AST + parser modelled on `src/sql/*`, plus a pattern-match planner (join ordering across pattern
segments) that has no SQL analog to reuse, though it targets the physical operators from 3.2.

### 3.4 Staged plan (graph)

| Stage | Work | Files | Effort |
|---|---|---|---|
| G-1 | Key codec (node-id + `src‖type‖dst`), prefix-range helpers | new `graph/keycodec.zig` (model `table.zig:148`) | S (~2-3 d) |
| G-2 | Graph catalog (labels, edge types, 3 roots) | `schema/catalog.zig:6`, `database.zig` root registration | M (~1 wk) |
| G-3 | Node/edge CRUD (maintain out + in trees in one txn) | new `graph/store.zig` over `btree.zig` | M (~1-1.5 wk) |
| G-4 | Traversal iterators (Adjacency/Expand/VarLengthExpand) | extend `iterator.zig`, wire `query_executor.zig:558` | M (~2 wk) |
| G-5 | Cypher-subset language (lexer/ast/parser/planner) | new `graph/{lexer,ast,parser,planner}.zig` | L (~4-6 wk, dominant) |
| G-6 | Wire model tag on the command envelope | `proto/command.zig`, `proto/protocol.zig` | S-M |

## Part 4: sequencing and cross-cutting

Order: **document model first, GraphQL-over-existing-models next, native graph engine last.** This refines the
feasibility note, which listed GraphQL fourth; GraphQL's only dependency is "something to resolve against",
which the document model satisfies, so it should ship right after document rather than waiting on the Cypher
engine.

1. Fix the relational engine to the `sql92-compliance.md` bar (correctness defects, then the expression
   engine). Do not start multi-model on an uncertified base.
2. Document model (Part 1). Lowest effort per unit value; forces the catalog generalization and the wire
   model-tag that graph will also need, paying those once.
3. GraphQL in Nova (Part 2). Pure Nova, zero engine change, resolves over relational + the new document model.
   Delivers a user-facing API win without waiting ~3 months for the graph engine.
4. Native graph engine (Part 3). Highest effort, least leveraged by prior stages (the operator reuse helps the
   executor but the Cypher language is standalone), and the riskiest engine, so it sits on the most-proven core
   last.

Cross-cutting, do UP FRONT (before step 2): version the wire protocol with a model tag
(`proto/command.zig`, `proto/protocol.zig`) so neither the document nor the graph addition forces a second
breaking change. On concurrency (updated to the Stage-3 model, 2026-08-08): there is no single global
write lock. `db.rw_lock` gates only DDL and FK/join; per-table `GroupLock` + per-tree `structure_lock`
carry the write path, and every model inherits that same machinery, so finer-grained locking later helps
all engines at once rather than any one model being singled out.
