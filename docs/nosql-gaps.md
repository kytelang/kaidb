# NovaDB document model: gap register and beta plan

Status as of the D-1 to D-5b slices (document storage, catalog, filter surface,
secondary index, wire opcode, driver API). This document lists every gap we know
about between what is built and what a beta-quality document database needs, then
proposes a sequenced plan for the remaining release window.

## The one root cause behind most blockers

The document path was built as a storage and query layer that sits **beside**
NovaDB's durability, transaction, replication, and security machinery, not
**inside** it. `DocumentStore.insert` calls `tree.insert` on the raw B+Tree
directly, whereas the SQL executor routes every write through WAL logging, MVCC
version records, the undo log, per-table locks, and the security manager.

Because of that, the biggest gaps (crash durability, replication, isolation,
authorization, and a usable index path) are all facets of the same thing: the
document writes do not travel the engine spine that the SQL writes travel. The
plan below is mostly about putting them on that spine.

Verified against the code while writing this: `src/document/store.zig` (raw
`tree.insert`, no WAL, no MVCC, no lock), `src/proto/doc_command.zig` (no
security check, builds a store per request with no index), `src/query/
query_executor.zig` (the SQL write path that DOES log WAL and write versions).

---

## Severity tiers

- **Blocker**: data loss, silent inconsistency, security exposure, or a feature
  so basic its absence disqualifies "beta". Must land, or the document model must
  be explicitly quarantined out of the beta guarantees.
- **Functional**: expected of a usable document store; absence is a real
  limitation but not a data or safety risk.
- **Robustness**: operational maturity, coverage, and proof.

---

## Blockers

> **Update:** G1, G2, G3, G5, and G6 are CLOSED (slices D-6, D-6b,
> G3-a1/G3-a2/G3-b, G5-delete/G5-update, D-9). Document writes now travel the WAL,
> survive a crash, replicate to followers, are permission-checked, are stored as
> MVCC version chains with READ-COMMITTED reads (not snapshot isolation: a reader
> sees the committed set at each read, and an uncommitted transaction's versions
> stay invisible until it commits, so a concurrent find never observes a partial
> batch) plus atomic multi-document transactions, and support update
> ($set/$unset/$inc) and delete by filter. The
> All six blockers (G1-G6) are now CLOSED. G4 (server-path indexes) is done:
> collection indexes are catalog objects, rebuilt from the collection on each open
> (derived, crash-safe without per-entry WAL), maintained on writes, and used by
> the find planner. Known follow-ons: index page reclamation across restarts, and
> precise entry removal on update/delete (stale entries are currently filtered by
> the find re-check, so results stay correct).

### G1. Document writes are not crash-durable [CLOSED - D-6]
`DocumentStore.insert` writes to the B+Tree with no WAL record. A clean
`Database.close()` flushes dirty pages, so documents survive an orderly shutdown
(this is what the D-2 reopen test proves), but a crash before the next checkpoint
loses every unflushed document write. The SQL path survives `kill -9` by replaying
its WAL (the D7 tests); the document path has nothing to replay.

Worse, `createCollection` **is** WAL-logged, so after a crash you can get a
collection that exists but is empty: the catalog change replays, the data does
not. That is a silent inconsistency, not merely lost data.

Fix direction: log a logical WAL record per document write (insert, and later
update and delete), keyed by collection and `_id`, and add a recovery replay
branch that re-applies it. This is slice D-6.

### G2. Document writes do not replicate [CLOSED - D-6b]
Followers catch up by applying shipped WAL. No WAL for document data means no
replication: every read replica and HA follower silently misses all documents,
and the RPO=0 guarantee the SQL path advertises does not hold for collections.
Fixed for free once G1 lands (the same WAL ships to followers), plus a follower
apply branch for the document record type.

### G3. No MVCC or transactional isolation [CLOSED - G3-a1/G3-a2/G3-b]
Document storage is single-version, last-writer-wins. Document operations never
enter the transaction manager or the undo log, so there is no atomic multi-document
write, no rollback, and no snapshot: a `find` running concurrently with a writer
can observe a partially applied batch. `createCollection` does use a transaction,
but the data inserts inside a collection do not.

This is the hardest blocker to close fully. See the beta cut line below: if full
MVCC cannot land in the window, the honest fallback is to document the isolation
limitation precisely rather than imply snapshot semantics we do not provide.

### G4. The secondary index is unreachable on the real path [CLOSED - G4]
`doc_command.handleDocRequest` builds a fresh `DocumentStore` per request and never
calls `addIndex`, so every `find` over the wire is a full collection scan.
Separately, `addIndex` is a runtime, in-process call whose index tree is not a
catalog object, so indexes do not survive a restart and cannot be reached over the
wire at all. The D-4 and D-4b index code is correct but has no path to production.

Fix direction: register a collection index as a catalog object (like a SQL index),
open it in `populateTreeCaches`, maintain it on every write, and have the wire find
handler consult the registered indexes when planning. This is slice D-8.

### G5. No update or delete [CLOSED - G5-delete/G5-update]
Only `insert` and `find` exist. A document cannot be modified or removed over the
wire or from the driver: no `$set`/`$unset`/`$inc`, no `deleteOne`/`deleteMany`, no
upsert. A store you cannot mutate in place is not beta. Slice D-7, and it depends on
G1 so the mutations are durable.

### G6. No authorization on document operations [CLOSED - D-9]
`handleDocRequest` never consults `security_manager`. The SQL path attributes and
permission-checks every statement against the authenticated session; the document
path lets any connected client read or write any collection, ignoring the security
model entirely. This is a cross-tenant exposure. Slice D-9, small and independent,
so it can land early.

---

## Functional gaps

> **Update (query-completeness pass):** G7, G8, G9, G10, G11, G13, and G16 are
> CLOSED, and G15 is partly closed. The document query surface now has:
> server-side `_id` generation (insert accepts a document with no `_id`);
> insert-if-absent via `DocumentStore.insertUnique` (duplicate `_id` is
> `DocError.DuplicateKey`); the full logical/array/regex operator set
> (`$and`/`$or`/`$nor`/`$not`, `$regex`+`$options`, `$type`, `$all`/`$elemMatch`/
> `$size`, and multikey array matching); projection; sort/skip/limit and `count`;
> a cursor (`find_cursor`/`findBatch`) that pages a large collection;
> binary/decimal128 comparison (within-type, by bytes); an aggregation pipeline
> (`$match`/`$group`/`$sort`/`$skip`/`$limit`/`$project`/`$count` with
> `$sum`/`$avg`/`$min`/`$max`); upsert and a mixed `bulkWrite`; and unique +
> compound secondary indexes. These are reachable over the wire (doc opcodes
> `find_query`=11, `count`=12, `insert_many`=13, `find_cursor`=14, `upsert`=15,
> `bulk_write`=16, `aggregate`=17, `insert_unique`=18, `doc_stats`=19,
> `create_unique_index`=20) and from the driver (`findQueryDocs`, `countDocs`,
> `insertManyDocs`, `findCursor`, `updateDocs`, `deleteDocs`, `upsertDocs`,
> `bulkWrite`, `aggregateDocs`, `insertUniqueDoc`, `docStats`, `createIndex`,
> `createUniqueIndex`).
>
> **Adversarial-review fixes (second pass):** the review found several items that
> were only store-level or unsound; these are now genuinely closed:
> - **G8** insert-if-absent is reachable: the `insert_unique` op fails on a
>   duplicate `_id` (server insert still overwrites by design; `insert_unique`
>   is the fail-on-dup path).
> - **G7** insert returns the (possibly generated) `_id` in the response tag
>   (`INSERT-DOC 1 <hex>`); the driver exposes `parseInsertedId`.
> - **G14** UNIQUE single-field indexes are reachable and persisted
>   (`create_unique_index`): a colliding write fails `23505`, and building over an
>   existing duplicate is refused. (COMPOUND indexes remain store-level.)
> - **G16** decimal128 is no longer byte-compared (that gave wrong answers for
>   equal values with different cohorts); it is now deliberately not-comparable
>   (sound). Binary comparison stays (byte-exact by definition).
> - **G17** the counters are reachable via the `doc_stats` op.
> - **G4** index entries are now removed precisely on update and delete (no more
>   unbounded append-only growth), on both the live and follower/recovery paths.
>
> Remaining honest caveats: `$regex` is a documented subset (no alternation/
> backreferences/`{m,n}`); decimal128 comparison is unsupported (needs a real
> decimal library); aggregation is a focused subset (no `$unwind`/`$lookup`/
> `$facet`); COMPOUND indexes are store-level (not yet a wire option); the G3
> reads are read-committed (not snapshot isolation).

### G7. No server-side `_id` generation [CLOSED]
`insert` errors if `_id` is absent (`DocError.MissingId`); clients must mint their
own ObjectIds. MongoDB auto-assigns one. We should generate an ObjectId server-side
when `_id` is omitted and return it in the response.

### G8. Insert is always overwrite
`tree.insert` overwrites any existing document with the same `_id`
(last-writer-wins). There is no insert-if-absent (duplicate-`_id` detection) and no
upsert. We need distinct insert (fail on duplicate) and upsert semantics.

### G9. Query-language depth
Present: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`, `$in`, `$nin`, `$exists`.
Missing: logical `$and`, `$or`, `$not`; `$regex`; `$type`; array operators `$all`,
`$elemMatch`, `$size`; and dotted-path matching into arrays (multikey).

### G10. No projection
`find` always returns whole documents. There is no way to select a subset of
fields, which matters for wire size and for hiding fields.

### G11. No sort, limit, skip, or count
No ordering or pagination primitives on the query surface.

### G12. No aggregation pipeline [CLOSED - basic pipeline]
No group, sum, count-by, or the rest of an aggregation stage set. This is often the
reason a team reaches for a document database, so its absence is notable, though it
is acceptable to defer past the first beta.

### G13. No cursor or result streaming
`find` materialises every match into a single wire frame. A large collection blows
the 64 MB frame cap and the client's memory. We need batched delivery with a cursor,
mirroring how the SQL path streams rows.

### G14. Index breadth [PARTIAL - unique done]
Unique single-field indexes now exist: `DocumentStore.addUniqueIndex` enforces a
unique constraint on a field's value (a colliding insert is `DocError.DuplicateKey`,
an overwrite of the same `_id` is allowed, and backfilling over an existing
duplicate fails). Multikey (array) matching in filters is also supported. Still
open: compound (multi-field) indexes, text and TTL indexes, and wiring the unique
index through the wire/driver as a `createIndex` option.

### G15. No bulk operations [CLOSED - insertMany/bulkWrite/upsert]
No `insertMany` or `bulkWrite`. Each write is a separate round trip.

---

## Robustness and operational gaps

> **Update:** G17, G18, and G19 are CLOSED. The document path now has
> observability counters (`Database.docMetrics`: finds/inserts/deletes/updates
> plus `docs_scanned`/`docs_matched` scan volume), a document wire-decoder fuzz
> and a serial model-consistency fuzz (`src/document/fuzz.zig`), and a concurrent
> overflow-document overwrite test. G16 (binary/decimal128 comparison) is closed
> for within-type ordering; the cross-type numeric comparison of decimal128 stays
> open by design. G20 is now CLOSED: beyond the two-ended codec verification (the
> Zig `doc_command` round-trip tests and the Nova driver codec tests over the same
> byte layout), the document ops now round-trip over a real kernel TCP socket in
> `root.zig` ("G20: document ops over a live TCP socket end-to-end"): a server
> thread runs the true `session.run` loop while the test drives a client through
> startup, `create_collection`, `insert`, `find`, `find_query`, `count`, and
> `delete` over the wire.

### G16. Filter type coverage [CLOSED for within-type ordering]
The matcher compares numbers, strings, bool, ObjectId, and datetime. `decimal128`
and `binary` are not comparable yet. Mixed-type comparisons fall through to "no
match", which is defensible but should be deliberate and documented.

### G17. No observability for document operations
No counters or latency for insert and find, and no visibility into scan volume,
which matters a great deal while G4 (no index on the real path) stands: every query
is a full scan and nothing reports it.

### G18. No document-specific fuzz or soak coverage
The Stage-3 concurrency fuzzer exercises the SQL trees. There is no document
concurrent fuzzer, and the document wire decoder, though bounds-checked, is not part
of the D9 decode fuzz harness.

### G19. Concurrent overflow-document overwrite is untested
Large documents that spill to overflow chains work single-threaded. The behaviour of
delete-plus-reinsert of an overflow document under concurrent writers has no
guardrail test.

### G20. Driver interop is not live-verified [CLOSED]
The document ops now round-trip over a real kernel TCP loopback socket, not just
through the in-memory codec. `root.zig`'s "G20: document ops over a live TCP socket
end-to-end" binds an OS-assigned loopback port, runs the genuine `session.run` loop
on a server thread, and drives a client that speaks the protocol by hand: the
startup handshake, then `create_collection`, two `insert`s, `find` (both docs),
`find_query` with a `$gt` filter (one doc), `count` (`{n:2}`), and a filtered
`delete` (leaving one doc). This exercises the full byte path, frame headers and
partial reads included, so the wire-format agreement between the engine and the
driver is now proven end to end. The Nova driver's own codec tests remain the
offline half of the same proof.

---

## Not gaps (considered, by design)

- Schemaless storage with no schema validation is intentional for a document model.
- The single-reactor server model is a deliberate architecture choice; scale is
  horizontal, behind the proxy.
- `_id`-keyed primary access needs no secondary index; the collection tree is keyed
  by `_id` already.

---

## Proposed slice plan

Sequenced so the blockers that risk data or security land first, and so each slice
builds on a durable, tested base. Efforts are rough and assume the established
per-slice rhythm (implement, unit test, gate, commit).

| Slice | Scope | Closes | Depends on | Effort |
|-------|-------|--------|------------|--------|
| D-6 | WAL logging + crash recovery for document writes | G1, G2 | | L |
| D-7 | Update and delete (store, wire, driver) + `$set`/`$unset`/`$inc` | G5 | D-6 | M |
| D-8 | Persist collection indexes as catalog objects; auto-use in wire find; maintain on write | G4 | D-7 | M |
| D-9 | Authorization on document ops (thread session/security through the handler) | G6 | | S |
| D-10 | Server-side `_id` generation; insert-if-absent and upsert | G7, G8 | D-7 | S |
| D-11 | Query completeness: `$and`/`$or`/`$not`, projection, sort/limit/skip, count | G9, G10, G11 | | M |
| D-12 | Cursor and result streaming for large finds | G13 | | M |
| D-13 | MVCC snapshot reads + atomic multi-document writes | G3 | D-6 | L/XL |

Deferred past the first beta unless time allows: aggregation pipeline (G12), index
breadth (G14), bulk operations (G15), decimal128/binary compare (G16), richer
observability (G17), document fuzz and soak (G18, G19). The live interop proof (G20)
is small and should be run as soon as D-6 lands.

## Beta cut line

Given a couple of weeks, the realistic and honest target is:

**Must land for the document model to be inside the beta guarantees:**
D-6 (durability and replication), D-9 (authorization), D-7 (update and delete),
D-8 (indexes reachable and persisted), D-10 (`_id` generation and insert
semantics), and the G20 live interop proof.

**Should land, strong stretch:** D-11 (query completeness) and D-12 (cursors).

**May not land:** D-13 (full MVCC). If it does not, we do not ship a snapshot
isolation claim for collections. We document precisely that document reads are
read-committed-ish over a live tree and that multi-document writes are not atomic,
and we keep single-document writes durable, replicated, and authorized. Many
document stores began exactly there. That is a defensible v0.1 as long as it is
stated plainly and not implied otherwise.

**If the must-land set cannot be met:** quarantine the document model out of the
beta guarantees. Keep the surface available but labelled experimental, and state in
the release notes that it is single-node, not crash-durable, unreplicated, and
unauthorized. Cheap and honest, and far better than shipping G1, G2, or G6 silently.
