# kaidb

kaidb is a transactional database engine written in Zig. It ships as two binaries:

- **`kaidb`** the database server.
- **`kai`** the command-line client.

It speaks both a SQL surface and a document (Mongo-style) surface over a single binary wire protocol, and it is built to be correct under crashes and concurrent writers.

## Features

- **Clustered, index-organised storage.** Rows live in a B+tree keyed by the primary key, so primary-key lookups and range scans read straight from the leaf pages with no separate heap.
- **MVCC.** Readers see a consistent snapshot and never block writers. Each transaction reads its own version without taking row locks for reads.
- **Durability.** A write-ahead log plus a doublewrite buffer protect against torn pages, and recovery replays the log on restart to bring the database back to a consistent state after a crash or a `kill -9`.
- **Secondary indexes.** Order-preserving keys give indexed range scans and indexed `ORDER BY`. Covering (included-column) indexes answer queries from the index alone. Index maintenance runs as part of the write.
- **A real query planner.** Index range scans, AND-index selection, index-only counts, `OR` to index-union rewrites, MIN/MAX endpoint reads, and pushed-down `LIMIT` are all chosen by the planner rather than left to a full scan.
- **Document store.** Insert, find, and update documents with a filter language, BSON storage, secondary indexes on document fields, atomic multi-document transactions, and the same MVCC visibility as the SQL side.
- **Read replicas.** A primary ships its writes to followers asynchronously, so read traffic can be served from replicas.
- **Binary protocol with TLS.** Clients talk to the server over a compact binary protocol, optionally wrapped in TLS.

## Building

kaidb builds with Zig 0.16.0.

```sh
zig build            # builds the kaidb server and the kai client into zig-out/bin
zig build run        # builds and runs the server
zig build test       # runs the test suite
zig build cross      # cross-compiles every supported OS/arch into zig-out/cross/<triple>/
```

Supported targets: macOS (arm64, x86_64), Linux (x86_64, arm64), and Windows (x86_64, arm64).

## Using the client

```sh
kai --help           # list commands and connection flags
```

`kai` connects to a running `kaidb` server and runs SQL or inspects the database from the terminal.

## Releases

Pushing a version tag (`vX.Y.Z`) builds and publishes the release bundles from CI. Each tag ships the `kaidb` server and the `kai` client for every supported OS/arch, alongside `kde` (Kyte Data Explorer, the desktop database GUI). See `.github/workflows/release.yml`.

## Project layout

- `src/` the engine, server, and CLI sources.
- `src/main.zig` the server entry point.
- `src/cli.zig` the `kai` client entry point.
- `build.zig` the build graph, including the `cross` and `test` steps.
