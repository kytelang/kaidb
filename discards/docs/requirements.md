## Requirements (historical, superseded)

This file captured the very first, rough requirements sketch for what became
NovaDB. It is kept only as a record of the original intent. Almost all of the
specifics below have since changed, so do NOT treat this as current guidance.

For the authoritative, up-to-date picture use these instead:

- `../CLAUDE.md` for what NovaDB is today, its scoped role, and how to build and run it.
- `../architecture.md` for the design spec (page layout, B+Tree, concurrency protocols).

### What the original sketch asked for

- A single master B+Tree holding all system metadata (table, index, constraint,
  sequence and system-table catalogues), persisted on disk.
- Every user table and every index as its own B+Tree, all sharing one pager and
  one buffer pool inside a single file.
- A SQL parser and a wire protocol that translates SQL into work against the B+Trees.
- Durability with truncation of the log once the tree is flushed to disk.
- A CLI client talking to the server over the wire protocol.

### What has since changed (so the sketch is stale)

- The folder and product were renamed from `btree` to `novadb`; the binaries are
  `novadb` and `novadb-cli`. Old `.../nova-lang/btree/...` paths no longer exist.
- The WASM / wasmer embedding idea was dropped entirely; there is no wasm code path.
- The server speaks a binary wire protocol (with an optional HTTP surface), not
  an https-only endpoint.
- NovaDB is now scoped as the orchestrator's control-plane store, not a
  general-purpose relational database at scale (see `../CLAUDE.md`).

Design decisions and open work are tracked in `../architecture.md` and the
project memory, not here.
