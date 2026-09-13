# Multi-model direction: NoSQL (document) + graph, feasibility note

> This is the high-level feasibility rationale. The CONCRETE, file-level plan (with effort estimates and touch
> points) lives in `multi-model-design.md`, which also corrects one claim below: BSON is NOT already in the
> codebase (it is only a SQL keyword token); the document model is built on `std.json`, which the engine
> already uses with embedded MVCC. Relational-engine prerequisites are tracked in `sql92-compliance.md`.
>
> **Re-verified against the current source (2026-08-28).** The load-bearing mechanisms this note relies on
> still hold: the catalog's free-form object type (`schema/types.zig` `ObjectMetadata.type: []const u8`), the
> JSON-value-in-B+Tree MVCC path (`query/query_executor.zig` `getVisibleVersion` branches on
> `actual_bytes[0] == '{'`, `writeNewVersion` takes a `std.json.ObjectMap`), overflow spill for large values
> and `rangeScan` (`storage/btree.zig`), and versioned wire negotiation (`proto/protocol.zig` `isCompatible`).
> Two corrections applied below. (1) **Storage and the value codec.** Rows are stored BINARY at the row
> level: `schema/row.zig` packs columns as fixed-width little-endian values plus a heap section, read back by
> `RowReader`/`reconstructVersionChain` with an undo chain. A SECOND encoding coexists: `writeNewVersion`
> stores a JSON `{"versions":[{xmin,xmax,data}]}` blob and `getVisibleVersion` auto-detects it
> (`bytes[0] == '{'` -> JSON path, else -> packed-binary path). For the DOCUMENT model, BSON is not in the
> NovaDB codebase (only a SQL keyword token), but a complete pure-Zig BSON library exists at
> `/Users/kamlesh/plancksystems/bson` (`.name = .bson`): encoder + decoder, `BsonDocument` with typed getters
> including `getNestedField(field_path)`, `BsonArray`, `ObjectId`, `Decimal128`, and `json_interop`. Vendor it
> as a Zig module dependency the way NovaDB already vendors `yaml`/`tls`/`utils`, and store documents as native
> BSON from the start (its nested-path getter also serves document field indexing directly). std.json remains a
> fine bring-up value codec, but BSON is the target and it is a library-away, not a build-from-scratch. (2) The
> concurrency claim is updated to the Stage-3 model (per-table `GroupLock` + per-tree `structure_lock`, no
> global write lock). The design doc's line numbers have drifted as the executor grew (for example
> `writeNewVersion` is now near `query_executor.zig:3902`, not `:2722`); the mechanisms are unchanged.


## The ask

Make NovaDB multi-model. Today it is relational only (SQL parser, executor, schema/catalog). We want
it to also serve a NoSQL (document) model and a graph model. "GraphQL" is treated below as two separate
things, because they are: the graph DATA model (nodes and edges with traversals), and GraphQL the API/
query language. They have very different homes in the stack.

Status: NOTE + feasibility. Not scheduled. This is a major surface expansion and must sequence AFTER the
D-row hardening (D1-D10). A shaky relational engine should not spawn two more shaky engines on the same
base; the foundation-first rule in db-orch-lang-prod-readiness.md applies.

## Why the foundation already suits this

The important point: underneath the SQL layer, NovaDB is an ordered key-value store with the hard parts
already built. A slotted-page B+Tree with variable-length cells and overflow pages, a buffer pool/pager,
MVCC, an undo log, a WAL, and crash recovery. That is exactly the substrate real multi-model databases
layer document and graph engines on top of (FoundationDB layers, ArangoDB on RocksDB, and so on). The
relational engine is simply the FIRST model engine over that substrate; document and graph would be more
model engines sharing the same storage, transaction, and durability core.

So the question is not "can the storage hold it" (it can, it is a KV store), but "how much new surface
does each model's catalog, query, index, and wire path add".

## Document / NoSQL model, feasibility: HIGH (medium effort)

A document store is collections of schema-flexible JSON/BSON documents keyed by an id.

- Storage maps cleanly. A collection is a B+Tree keyed by document id; the value is the serialised
  document, and overflow pages already handle large values. The engine already carries two value
  encodings (a binary packed row via `schema/row.zig`, and a JSON embedded-versions blob via
  `writeNewVersion`, auto-detected on read), so adding documents is choosing a value codec, not inventing
  storage. Use the pure-Zig BSON library at `/Users/kamlesh/plancksystems/bson` as that codec: store
  documents as native BSON (compact, typed, `ObjectId`/`Decimal128` native), vendored as a module the way
  `yaml`/`tls`/`utils` already are. std.json works for bring-up, but BSON is the right target and it is a
  ready library, not new code. Pin collections to ONE encoding so the read path stays simple.
- Secondary indexes on document FIELDS/PATHS reuse the existing index machinery: a second B+Tree keyed
  by `(field-value, doc-id)`, so `find({age: 30})` becomes an index range scan, the same primitive the
  SQL WHERE path already uses.
- The new work is a document CATALOG (collections, not typed columns), a query surface (a Mongo-style
  filter document, or a JSON-path predicate), and index maintenance on writes. All bounded, well-trodden.
- MVCC/WAL/recovery come for free because writes still go through the same storage and log.

Effort: MEDIUM. Risk: LOW. This is the natural first step and the highest value per unit work.

## Graph model, feasibility: MEDIUM (large effort)

A property graph is nodes and edges, each with properties, plus traversals.

- Storage maps well, and the B+Tree's RANGE SCAN is the key enabler. Nodes are KV by node-id. Edges are
  stored as adjacency: a key like `(src-id, edge-type, dst-id)` so "all out-edges of N" is a single
  range scan (`O(log n)` to the first, then sequential), which is exactly the traversal primitive a
  graph engine needs. A reverse index `(dst-id, edge-type, src-id)` gives in-edges.
- The cost is NOT storage, it is the query/traversal ENGINE: BFS/DFS, variable-length paths, pattern
  matching, and then a query LANGUAGE for it (Cypher-like or Gremlin-like) with its own parser and
  planner. That is a substantial parser + executor effort on the scale of the SQL layer itself.
- Transactions/durability again come for free from the shared core.

Effort: LARGE (storage mapping is medium; the traversal engine + a graph query language is the bulk).
Risk: MEDIUM. Best done AFTER the document model proves the multi-engine pattern on the shared substrate.

## GraphQL (the API), feasibility: it belongs in Nova, not the engine

GraphQL is a query/API language over a schema with resolvers; it is NOT a storage model. The right home
is the NOVA web tier, where a GraphQL endpoint's resolvers call the database (relational, document, or
graph) through the existing driver. Baking GraphQL into the storage engine would couple an API surface
to the pager, which is the wrong layer. Recommendation: implement GraphQL as a package/module in the
Nova web framework, resolving to DB operations, once at least the document model exists to resolve
against. Effort there: MEDIUM, and independent of the engine.

## Cross-cutting work any new model needs

- A generalised CATALOG. Today the catalog is relational-shaped (tables, typed columns). Document and
  graph need their own catalog notions (collections; node/edge labels + property indexes), either via a
  generalised catalog or per-model catalogs over the same page-backed metadata.
- The WIRE PROTOCOL and the Nova driver need per-model commands (or one generic command envelope with a
  model tag). This pairs with D10 (version the wire protocol) and the "naive protocol" note: design the
  versioned protocol with multi-model in mind so it does not need another breaking change later.
- The CONCURRENCY MODEL is inherited by every engine, and it is NO LONGER a single global write lock. As
  of Stage 3 (2026-08-08) the db-wide `db.rw_lock` gates only DDL (exclusive) and FK/join statements; per
  user table there is a `GroupLock` (`common/sync.zig`: SELECT = shared, INSERT = concurrent writers,
  UPDATE/DELETE = exclusive), and concurrent writers on one tree are made safe by a per-tree
  `structure_lock` in `btree.zig`. Document and graph engines get the same protection for free, since they
  key everything by a logical object name and go through the same trees. This does not block multi-model;
  it means each model's write throughput is bounded by the same per-table/per-tree machinery, and any
  future finer-grained locking benefits all models at once.
- LEAKS/HYGIENE/DURABILITY gates (D3/D4/D7/D8) must hold for the new engines too; they inherit the same
  bar, which is the reason to sequence after the D-rows.

## Recommendation

Feasible, and the architecture is unusually well-suited because the core is already an ordered KV store
with MVCC + WAL. Sequence it as:

1. Finish D1-D10 (relational engine to the production bar). Do NOT start multi-model on an uncertified
   base.
2. Add the DOCUMENT model first (high value, medium effort, low risk) and use it to prove the
   shared-substrate multi-engine pattern and a generalised catalog.
3. Add the GRAPH model next (adjacency over range scans; the traversal engine + graph query language is
   the real cost).
4. Add GraphQL in the Nova web tier as resolvers over the above, not in the engine.

Design the versioned wire protocol (D10) up front to carry a model tag, so multi-model does not force a
second protocol break.
