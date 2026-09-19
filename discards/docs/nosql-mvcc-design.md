# Document MVCC design (gap G3)

Goal: give the document model the same snapshot isolation the SQL engine has, so a
`find` never sees a partially applied multi-document write, and multiple document
writes can commit atomically. This closes gap G3 in `nosql-gaps.md`.

## Principle: reuse the SQL version machinery, do not reinvent it

The SQL engine already stores every row as a version chain and reads it through a
transaction snapshot. The version format is opaque about payload bytes, so a
document is just a version whose payload is its BSON. Concretely:

- A document is stored as `row.packVersions([DecodedVersion])`, exactly like a row,
  with `fixed = BSON bytes` and `heap = ""`. Older versions live in the undo log,
  chained by `roll_ptr`, identical to rows.
- Writes go through `Database.updateRowMVCC(tree, id, version)` unchanged. It pushes
  the prior version to the undo log and packs the new one, stamping `xmin`.
- Reads reconstruct the chain with `Database.reconstructVersionChain` and pick the
  first version visible to the reader via `txn_manager.isVisible` (read-committed,
  live set) or `txn_manager.isVisibleIn` (a captured snapshot).
- Transactions reuse `TransactionManager.begin/commit/abort`, `captureSnapshot`, and
  the WAL `begin`/`commit` records already used by D-6.

What we do NOT reuse (they assume a typed row): `writeNewVersion` (serialises via a
column-typed `RowBuilder`) and the materialisation half of `getVisibleVersion` (it
rebuilds a row from `table.columns` and auto-detects a JSON envelope with
`bytes[0]=='{'`, which would misfire on a BSON length prefix). For documents we
write the version directly and return `version.fixed` (the BSON) as-is.

## Slices

- **G3-a1 (this slice): the versioned document primitive, isolated.** New
  `Database.docWriteVersion(tree, id, bson, tx)` and
  `Database.docReadVisible(tree, id, current_tx)` over a collection tree, plus a
  unit test proving read-committed visibility: an uncommitted writer's version is
  invisible to a concurrent reader; once committed it is visible; a newer
  uncommitted version does not hide the committed older one. Non-breaking: nothing
  else calls these yet.
- **G3-a2: route the live path through versions.** `docInsert` writes a version;
  `findById`/`find` read the visible version; recovery `applyCollectionDml` and the
  follower apply reconstruct versions (reuse `updateRowMVCC`) instead of a plain
  key replace. The D-6/D-6b crash and replication tests must stay green.
- **G3-b: explicit multi-document transactions over the wire.** Carry an open
  `current_tx` (and a captured snapshot for snapshot isolation) on the session, add
  document `BEGIN`/`COMMIT`, and make several document writes commit atomically; a
  `find` inside a snapshot transaction sees a stable snapshot.

Delete and update land in D-7 and must be version-aware from the start (a delete is
a new version whose `xmax` is the deleter, mirroring `deleteRowVersion`).

## Known limitation to document if G3-b slips the release

If only G3-a lands, single-document writes are durable, replicated, authorized, and
read-committed-consistent (a reader never sees an uncommitted write), but there is
no atomic multi-document transaction and no repeatable-read snapshot across
statements. That is a defensible v0.1 as long as it is stated plainly.
