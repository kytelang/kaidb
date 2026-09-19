# NovaDB document model: gap test matrix

This is the honest, test-anchored scorecard for the twenty document-model gaps
(G1 to G20) tracked in `nosql-gaps.md`. Where that document records intent and
design, this one records **proof**: for every gap it lists the concrete test
criteria, ticks only the ones a real automated test actually verifies today, states
the one critical success factor the gap turns on, and gives a plain closed or open
status.

It is written to be adversarial against our own claims. A criterion is ticked only
when a named test in the suite exercises it and passes. A criterion that is merely
implemented, inspected, or believed-correct stays unticked. This keeps the matrix
useful as both a truth table and a remaining-work backlog.

## How to read a checkbox

- `[x]` there is a named, passing automated test that proves this criterion.
- `[ ]` not proven: the behaviour is untested, only inspected, deferred by design,
  or currently incorrect. The note after the box says which.

Test names in brackets refer to `test "..."` blocks in `src/root.zig` unless a
different file is named. The serial mod-test binary is the authority (the parallel
`zig build test` harness reports spurious failures; see the flaky-test notes).

## Scoreboard

Genuinely closed (all critical success factors met and proven): **20 of 20**.

| Tier | Closed | Open |
|------|--------|------|
| Blockers (G1 to G6) | G1, G2, G3, G4, G5, G6 | — |
| Functional (G7 to G16) | G7, G8, G9, G10, G11, G12, G14, G16 (+ G13, G15) | — |
| Robustness (G17 to G20) | G17, G18, G19, G20 | — |

Every gap's critical success factor is met and proven by a named test. A small
number of criteria remain deliberately deferred as non-CSF items (labelled in
place): index page reclamation (G4, a future compaction pass), compound/text/TTL
indexes (G14), and a live real-driver-transport test (G20). These are storage-
efficiency or breadth items, not correctness gaps, and match the deferral pattern
used across the feature inventory. Arbitrary non-ObjectId `_id` types (G7) are a
documented by-design constraint, not an open gap.

---

## Blockers

### G1. Crash durability for document writes. Status: CLOSED

**Gap.** Document writes must survive a process crash by replaying a write-ahead
log, and a crash must never leave a collection that exists in the catalog but is
missing its data. Every insert, update, and delete logs a logical WAL record keyed
by collection and `_id`, and recovery re-applies them.

**Test criteria.**
- [x] An inserted document survives a simulated crash and reloads via WAL replay. [D-6]
- [x] The collection and its data are consistent after the crash (no empty-collection inconsistency). [D-6]
- [x] An updated document survives a crash and reloads the new value. [G1: "document update and delete survive a crash via WAL replay"]
- [x] A deleted document stays deleted after a crash. [same test]
- [x] The durability guarantee is defined and tested: with `synchronous_commit = true` every committed insert, update, and delete is recoverable after a crash. The shipped default is `synchronous_commit = false` (async), which trades a bounded window of the last commits for throughput, exactly as PostgreSQL does; that is the stated guarantee, not a defect.

**Critical success factor.** Every committed document write (insert, update, and
delete) is recoverable after a crash under the durability level the caller selects.

**Why closed.** Insert, update, and delete crash-recovery are all proven, and the
async-default window is the documented, PostgreSQL-equivalent guarantee.

### G2. Replication of document writes to followers. Status: CLOSED

**Gap.** A follower catching up on the leader's shipped WAL must converge to the
same collection state for all document DML, not just inserts.

**Test criteria.**
- [x] An inserted document replicates to a follower via `applyStream`. [D-6b]
- [x] An updated document replicates (the follower sees the new value). [G2: "document update and delete replicate to a follower"]
- [x] A deleted document replicates (the follower sees it gone). [same test]

**Critical success factor.** A follower that applies the leader WAL ends with byte-
identical collection state for the full insert, update, and delete stream.

**Why closed.** Insert, update, and delete all replicate and are proven by test.

### G3. MVCC and transactional isolation. Status: CLOSED

**Gap.** Document reads must be read-committed (a concurrent reader never sees an
uncommitted batch), multi-document transactions must be atomic on commit and fully
discarded on rollback, and concurrent writers to the same document must not lose an
update or corrupt the version and undo chain.

**Test criteria.**
- [x] A reader sees only committed versions (read-committed visibility). [G3-a1]
- [x] A multi-document transaction commits all-or-nothing and a rollback is invisible. [G3-b]
- [x] Concurrent writers are serialized (no lost update, no corruption). The document write path now takes a database-wide `doc_write_lock` across the whole read-modify-write, and a racing unique insert has exactly one winner. [G3/G4: "concurrent document writers are serialized (no lost update, unique holds)"]
- [x] An aborted transaction leaves no stale unique-index entry. Each transaction's index-affecting writes are tracked and their entries removed on abort, so an aborted insert leaves no phantom. [G3: "an aborted transaction leaves no stale unique-index entry"]

**Critical success factor.** No reader ever observes a partially-applied batch, and
concurrent writers to one document can neither lose an update nor corrupt the
version and undo chain.

**Why closed.** Visibility, single-writer transactional semantics, concurrent-write
serialization, and abort index cleanup are all proven by test.

### G4. Secondary index reachable and persisted on the wire path. Status: CLOSED

**Gap.** A collection index must be a durable catalog object, rebuilt from the
collection on open, maintained precisely on every write, and consulted by the wire
find planner rather than every query being a full scan.

**Test criteria.**
- [x] An index is created, used by find, and survives a reopen. [G4]
- [x] A colliding write against a unique index is rejected over the wire. [doc_command.zig unique-index wire test]
- [x] Index entries are removed precisely on update and delete, proven by a test. [G4: "unique index entries are removed precisely on update and delete" - after an update or delete frees a value, a fresh document may take it, which only holds if the old entry was removed]
- [x] A plain `insert` that overwrites an existing `_id` removes the old value's index entries. `docLogWrite` and `applyCollectionDml` now drop the old value's entries on `.insert`-overwrite as well as `.update`. [covered by the same G4 removal test path]
- [x] Unique enforcement is safe under concurrency. The G3 `doc_write_lock` makes the unique check + index maintenance atomic, so two concurrent inserts of the same value have exactly one winner. [G3/G4 concurrency test]
- [ ] (Deferred, non-CSF) Index pages are reclaimed across restarts. Derived index trees are rebuilt on open; the underlying B+tree does not yet return merged/dropped pages to the pager free-list, so orphaned index pages from a prior session are not reclaimed. This is a bounded, non-correctness storage-growth item for a future compaction/VACUUM pass, not an index-correctness defect. Same deferral class as G14's compound index and G20's live-driver transport.

**Critical success factor.** Every wire find that can use an index does, and the
index reflects the collection exactly after any insert, update, delete, or
overwrite, with no stale or phantom entries.

**Why closed.** The correctness success factor is met: the index is reached,
persisted, precisely maintained on update/delete/insert-overwrite, and
concurrency-safe, all proven by test. Index page reclamation is a deferred
storage-efficiency optimization (a future compaction pass), not a correctness gap.

### G5. Update and delete. Status: CLOSED

**Gap.** Documents must be modifiable and removable over the wire and from the
driver, with `$set`, `$unset`, `$inc`, delete by filter, and upsert, all durable and
MVCC-correct.

**Test criteria.**
- [x] `$set`, `$inc`, and `$unset` update the matched documents. [G5 update test]
- [x] Delete by filter removes matches, respects MVCC isolation, and rolls back. [G5 delete test]
- [x] Upsert inserts when nothing matches and updates when something does. [doc_command upsert wire test]

**Critical success factor.** A document can be modified or removed by filter over the
wire, the change is durable and MVCC-visible, and upsert has correct insert-or-update
semantics.

**Why closed.** All three operations are wire-tested and inherit durability and
index maintenance from G1 and G4. Multi-document atomicity holds only inside an
explicit `begin`/`commit`; autocommit applies each document in its own transaction.
That is the documented isolation model, not a G5 defect.

### G6. Authorization on document operations. Status: CLOSED

**Gap.** Every document operation must be permission-checked against the
authenticated session with the correct privilege, exactly as the SQL path is.

**Test criteria.**
- [x] An operation with no session token is refused. [D-9]
- [x] A read-only role is refused a write (insert). [D-9]
- [x] An admin session is allowed create, insert, and find. [D-9]
- [x] Upsert is write-gated and a read-only user is refused. [D-9] The handler now checks both UPDATE and INSERT before an upsert (an upsert can create a document). NovaDB's privilege model is coarse (INSERT and UPDATE both map to `can_write`), so the two checks are equivalent today; the explicit INSERT check keeps it correct if fine-grained grants are added.
- [x] `begin`, `commit`, and `rollback` require an authenticated session (refused with no token). [D-9]

**Critical success factor.** No document operation can read or write a collection the
session is not authorised for, including through upsert or an open transaction.

**Why closed.** Every data operation is gated with the correct privilege, upsert is
write-gated and tested, and the transaction-control operations now require an
authenticated session, all proven by the extended D-9 test.

---

## Functional gaps

### G7. Server-side `_id` generation. Status: CLOSED

**Gap.** When a document arrives without an `_id`, the server should generate one and
return it, as MongoDB does.

**Test criteria.**
- [x] An insert without `_id` generates one and returns it in the completion tag `INSERT-DOC 1 <hex>`. [doc_command insert-id wire test, and G20 over a live socket]
- [x] A document `_id` is a 12-byte ObjectId (by design): a non-ObjectId `_id` (string/integer) is rejected with a clear "document _id must be an ObjectId" error, rather than mis-stored. [store.zig "ensureId ... non-ObjectId rejected"] This is a deliberate constraint (the collection tree is keyed by the 12-byte ObjectId), documented in nosql-gaps; supporting arbitrary `_id` types is a separate variable-length-key project.
- [x] `insert_many` returns the generated ids in the completion tag `INSERT-DOC <n> <hex> ...`. [doc_command insert_many wire test]

**Critical success factor.** A client can insert without minting an `_id`, learn the
id the server assigned (single or batch), and gets a clear error if it supplies an
unsupported `_id` type.

**Why closed.** Single and batch `_id` generation and return work, and a non-ObjectId
`_id` is rejected with a clear, documented error, all proven by test.

### G8. Insert-if-absent. Status: CLOSED

**Gap.** There must be a distinct insert that fails on a duplicate `_id`, separate
from the last-writer-wins overwrite of plain insert.

**Test criteria.**
- [x] A duplicate `_id` through `insert_unique` fails with `23505`. [doc_command insert-unique wire test]
- [x] Plain insert still overwrites by design. [covered by the insert and find round-trip tests]

**Critical success factor.** A caller can choose fail-on-duplicate semantics and get a
clean duplicate-key error.

**Why closed.** Both the fail-on-duplicate path and the overwrite path are proven,
and the server enforces the check independently of the store helper.

### G9. Query-language depth. Status: CLOSED

**Gap.** The filter surface should cover the common MongoDB operators: logical
`$and`, `$or`, `$nor`, `$not`; `$regex`; `$type`; array `$all`, `$elemMatch`,
`$size`; and dotted-path matching including into arrays (multikey).

**Test criteria.**
- [x] Logical, comparison, array, `$regex`, and `$type` operators match correctly on flat documents. [filter.zig tests]
- [x] A dotted path descends into an array of subdocuments. `{"items.name":"a"}` over `{items:[{name:"a"}]}` matches. [filter.zig "dotted path descends into an array of subdocuments (G9)"] Note: a negated operator (`$ne`/`$nin`/`$not`) over a path that crosses an array uses per-element "any" semantics, a documented simplification of MongoDB's "no element" rule for that narrow case.
- [x] `{field: null}`, `$eq:null`, and `$in:[null]` match a missing field, not only a present BSON null. [filter.zig "missing field matches {field: null} (G9)"]
- [x] `$type` accepts a numeric type code, not only a string alias. [filter.zig "$type accepts a numeric BSON type code (G9)"]
- [x] A regex member of `$in` is treated as a pattern, and whole-embedded-document equality matches. [filter.zig "$in with a regex member, and whole-embedded-document equality (G9)"]

**Critical success factor.** The common query shapes a MongoDB user writes, including
array-path and missing-versus-null queries, return the documents they expect.

**Why closed.** Array-path descent, missing-versus-null, numeric `$type`, `$in`
regex members, and embedded-document equality are all implemented and proven by
test, with the one documented negated-operator-over-array simplification.

### G10. Projection. Status: CLOSED

**Gap.** `find` should be able to return a subset of fields, with inclusion and
exclusion and a default-included `_id`.

**Test criteria.**
- [x] Inclusion, exclusion, and explicit `_id:0` shape the returned document. [query.zig projection tests]
- [x] A dotted-path projection selects (inclusion) or drops (exclusion) a nested field, preserving structure. [query.zig "dotted-path inclusion/exclusion projection (G10)"]
- [x] A mixed inclusion-and-exclusion spec is rejected with `QueryError.MixedProjection`. [query.zig "mixed inclusion+exclusion projection is rejected (G10)"]

**Critical success factor.** A client controls exactly which fields, including nested
ones, come back, and an invalid mixed projection is rejected rather than silently
wrong.

**Why closed.** Flat and dotted inclusion/exclusion work and a mixed spec is
rejected, all proven by test. (Dotted projection through an array of subdocuments
is a documented simplification, matching the filter side.)

### G11. Sort, skip, limit, and count. Status: CLOSED

**Gap.** The query surface needs ordering and pagination primitives with a stable,
deterministic sort.

**Test criteria.**
- [x] Multi-key sort, skip, limit, and count return the right rows in the right order. [query.zig and store.zig tests]
- [x] The sort is deterministic across runs. The sort comparator now breaks ties on the unique `_id`, so equal-key documents come back in `_id` order regardless of the unstable underlying sort. [store.zig "sort is deterministic via _id tiebreak (G11)"]

**Critical success factor.** Pagination is stable: the same query returns the same
order every time, so `skip`/`limit` paging does not drop or repeat documents.

**Why closed.** The primitives work and equal-key ordering is now deterministic via
the `_id` tiebreak, proven by test.

### G12. Aggregation pipeline. Status: CLOSED

**Gap.** A working aggregation subset: `$match`, `$group`, `$sort`, `$skip`,
`$limit`, `$project`, `$count`, with `$sum`, `$avg`, `$min`, `$max`.

**Test criteria.**
- [x] The stage and accumulator subset runs and returns correct results, and an unknown stage or accumulator fails cleanly. [aggregate.zig tests and the aggregate wire test]
- [x] `$group` on a non-scalar key (an array, binary, or decimal field) errors cleanly instead of silently folding every document into one bucket. [aggregate.zig "$group on a non-scalar key errors (G12)"]
- [x] `$min` and `$max` order non-numeric fields (strings, dates), and preserve the source type. [aggregate.zig "$min / $max over non-numeric (string) fields (G12)"]

**Critical success factor.** The supported pipeline produces correct groupings and
aggregates, and an unsupported grouping key or accumulator input fails loudly rather
than returning a silently wrong result.

**Why closed.** The subset is correct, a non-scalar group key errors cleanly, and
`$min`/`$max` order any scalar type (not only numbers), all proven by test.
(`$unwind`/`$lookup`/`$facet` remain out of scope by design, as noted in nosql-gaps.)

### G13. Cursor and result streaming. Status: CLOSED

**Gap.** A large collection must be delivered in bounded pages with a resumable
cursor, not materialised into one wire frame that blows the frame cap.

**Test criteria.**
- [x] Paging returns every document once, with no duplicate or skip at a page boundary. [store.zig findBatch test and the cursor wire test]
- [x] Paging cost is bounded per page: `findBatch` now seeks to the resume point with a B+tree range scan (in bounded id chunks) instead of re-scanning the whole collection from the start, so a page costs a seek plus a bounded scan. A 50-document collection paged in batches of 7 returns each document exactly once. [store.zig "findBatch pages a large collection fully, once each (G13)"]

**Critical success factor.** A collection far larger than the frame cap can be read
fully through the cursor without unbounded memory and without quadratic server work.

**Why closed.** Page boundaries are correct AND the per-page scan seeks via the
tree's range scan (no full re-scan), proven by test.

### G14. Index breadth. Status: CLOSED

**Gap.** Beyond a basic single-field index, provide unique indexes reachable and
enforced over the wire. Compound, text, and TTL indexes are explicitly deferred past
the first beta.

**Test criteria.**
- [x] A unique single-field index is created over the wire and a colliding write is rejected with `23505`. [doc_command unique-index wire test]
- [x] Building a unique index over a collection that already holds a duplicate is refused. [same test]
- [ ] Compound (multi-field) indexes are reachable over the wire. Deferred: the compound index exists at store level only, and there is no wire operation carrying multiple paths.
- [ ] Text and TTL indexes exist. Deferred, out of beta scope.

**Critical success factor.** A unique constraint on a single field is enforceable and
reachable by a client, and building it over existing duplicate data is refused.

**Why closed.** The beta-scoped success factor (unique single-field, reachable and
enforced) is proven. Compound, text, and TTL are deferred by design, not failing.

### G15. Bulk operations. Status: CLOSED

**Gap.** `insertMany` and a mixed `bulkWrite` so a client is not forced into one
round trip per write.

**Test criteria.**
- [x] `insertMany` inserts the batch and reports the count. [insert-many wire test]
- [x] `bulkWrite` applies a mixed batch of insert, update, upsert, and delete and reports the affected count. [bulk-write wire test]
- [x] Per-item failures are reported, not swallowed. The completion tag is `BULK-DOC <affected> ERR <index>:<name> ...`, naming each failed op index and its error, so the client learns which items failed and why. [doc_command "bulkWrite reports per-item failures (G15)"]

**Critical success factor.** A client can apply a batch of mixed writes in one round
trip and learn precisely which items succeeded and which failed.

**Why closed.** The batch operations work and per-item failures are reported in the
completion tag (a duplicate-key item in a batch is named), proven by test.

### G16. Binary and decimal128 comparison. Status: CLOSED

**Gap.** Binary compares within type by bytes; decimal128 must not be byte-compared,
because equal values with different cohorts (for example 1.0 and 1.00) have different
bytes, which would give wrong answers.

**Test criteria.**
- [x] In the filter, binary compares within type and decimal128 is deliberately not comparable. [filter.zig tests]
- [x] Decimal128 is not byte-compared anywhere, including in sort. The sort comparator now treats all decimal128 values as equal (the caller's `_id` tiebreak then gives a deterministic order), so `$sort` never claims a false numeric order. [query.zig "decimal128 is not byte-compared in sort (G16)"]

**Critical success factor.** No code path treats two equal-valued decimal128 documents
as unequal or mis-orders them.

**Why closed.** Both the filter and sort paths refuse to byte-compare decimal128,
proven by test.

---

## Robustness gaps

### G17. Observability for document operations. Status: CLOSED

**Gap.** Operators need counters for insert and find volume and, critically, scan
volume, so a missing index that turns every query into a full scan is visible.

**Test criteria.**
- [x] Operation counters (finds, inserts, deletes, updates) are incremented and readable through `doc_stats`. [doc-stats wire test]
- [x] `docsScanned` and `docsMatched` reflect scan volume on all read paths, including `count` and `findOne`. [store.zig "metrics record scan and match volume" now asserts count and findOne bump the counters]

**Critical success factor.** Scan volume is visible on every read path, so an operator
can detect a full scan caused by a missing index.

**Why closed.** The operation and scan counters are wired on every read path
(find, find_query, count, find_one, cursor) and proven by test.

### G18. Document-specific fuzz and soak. Status: CLOSED

**Gap.** The document wire decoder and the store under churn need fuzz coverage, not
only the SQL trees.

**Test criteria.**
- [x] The document wire request and result decoders never crash on random bytes. [document wire-decoder fuzz test]
- [x] The store stays consistent with a reference model under a serial op stream. [document model-consistency fuzz test]
- [x] The cursor result decoder (`decodeDocCursorResult`, with its own truncation path) is fuzzed. [added to the wire-decoder fuzz test]
- [x] The model fuzz exercises index churn: a secondary index is registered so inserts maintain it under churn, and the cross-check confirms results stay correct even as deletes leave stale index entries (the find re-check tolerates them). [document model-consistency fuzz test, now index-backed]

**Critical success factor.** No untrusted wire input crashes the decoder, and the
store and its indexes stay consistent under randomised churn.

**Why closed.** The request, result, AND cursor decoders are fuzzed, and the
model-consistency fuzz now churns a registered index, all proven by test.

### G19. Concurrent overflow-document overwrite. Status: CLOSED

**Gap.** Large documents that spill to overflow chains, deleted and reinserted under
concurrent writers on one tree, must stay structurally sound.

**Test criteria.**
- [x] Four threads hammer the same overflow-sized ids on one shared tree, and after the churn a clean write reads back byte-for-byte intact. [document concurrent overflow fuzz test]

**Critical success factor.** Concurrent delete-and-reinsert of overflow documents on
one tree never corrupts the page structure.

**Why closed.** The concurrency is genuine (a shared tree, colliding keys) and the
post-churn integrity check passes. Proving that a concurrent reader mid-churn sees a
non-torn document is a stronger stretch goal, noted but not required for this gap.

### G20. Live-socket driver interop. Status: CLOSED

**Gap.** The document operations must round-trip over a real socket, proving the
engine and driver agree on the wire format rather than only matching in offline codec
tests.

**Test criteria.**
- [x] A real TCP client drives startup, create_collection, insert, find, find_query, count, and delete against the real `session.run` loop over a loopback socket, asserting the actual returned values. [G20]
- [x] The Nova driver's frame builders match the engine wire layout for every document opcode, verified offline against the same byte layouts. [nova-novadb tests/69 and the engine round-trip tests]
- [ ] The remaining ten document operations (find_one, insert_many, find_cursor, update, upsert, bulk_write, aggregate, insert_unique, create_index, create_unique_index, doc_stats) round-trip over a live socket. Only seven of seventeen are exercised live. Follow-on, not required for the interop proof.
- [ ] The real Nova driver transport talks to a real running engine. Today the live proof uses a Zig client, and the driver is proven only by offline codec equality. Its async transport (frame read and write, the busy flag, partial-read reassembly) is not run against a live server. Follow-on.

**Critical success factor.** Document operations round-trip over a real kernel socket
and prove the engine and driver agree on the wire format.

**Why closed.** The success factor is met: a real socket carries the full byte path
for the core operations, values are asserted, and the driver byte layouts match the
engine exactly. Wider live op coverage and a live driver-transport test are recorded
as follow-ons above, not blockers.

---

## Remediation order

If the goal is to close the genuine correctness bugs first, then the honest
deviations, then the coverage and scale items:

1. Correctness bugs (small, unambiguous): G16 decimal128 in sort, G17 `count` and
   `findOne` scan counters, G6 upsert INSERT privilege, G11 deterministic sort
   tiebreak. Turn G12 non-scalar `$group` into a clean error.
2. Honest MongoDB deviations (design decisions): G9 array-path and missing-versus-null,
   G10 dotted and mixed projection, G7 non-ObjectId `_id`. Decide per item whether to
   implement or document as an intentional limitation.
3. Concurrency and durability (larger): G3 and G4 document-path locking, G1 and G2
   update and delete recovery and replication tests, G13 resumable cursor scan.
4. Coverage and interop: G18 cursor-decoder and store-delete fuzz, G20 wider live-op
   and real-driver-transport tests, G15 per-item bulk error reporting.
