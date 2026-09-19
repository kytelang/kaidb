# Removing JSON from NovaDB (kaidb): what exists, what is missing, and the plan

Goal: no `std.json` usage anywhere in the database **except the config file** (`db.json`,
parsed in `src/common/config.zig`, which stays JSON by design). This document is the
deep analysis: it inventories every JSON use, maps each to the binary machinery that
**already exists** so we reuse it rather than reinventing, and calls out the one place
where "no JSON" collides with the "leave disk formats" boundary.

Scope note: this is analysis and a staged plan, not a landed change. The only JSON-related
change already made is unrelated to this doc: single-table `SELECT *` numeric result cells
are now always binary (the `NOVADB_BINARY_RESULTS` gate was removed in `query_executor.zig`).

## 1. Binary machinery that already exists (reuse it)

### 1.1 SQL result wire (fully binary-capable today)
- Framing: `src/proto/protocol.zig` `MessageType` (`connect`/`query`/`query_resp`/`fetch`/`err`), length-prefixed with a `NOVA` magic and a `stream_id`.
- Row shape: `src/proto/wire.zig` builds `RowDescription` (one field per column, each with a `format` code: `text = 0`, `binary = 1`) followed by positional `DataRow`s (`u16` cell count, then `i32`-len-prefixed cells, `-1` = NULL).
- Numeric encoder: `query_executor.encodeBinaryCell(ct, cell)` emits fixed-width big-endian bytes for `oidmap.isBinaryType` columns; `session.zig` (lines ~577/631) stamps `field.format = .binary` when the response is binary.
- Driver decode: `kyte-kaidb/src/codec.ky` `decodeDataRowBuf` is **fully positional** and per-column format-aware (`decodeCellBinary` for `format==1`, text parse otherwise). So the client can already decode any number of binary columns; **no driver work is needed to move a query shape onto binary cells**, only a `RowDescription` that lists the columns.

Implication: any SELECT shape can ship as positional binary cells today. The only shapes that do not are the fallbacks in section 3.

### 1.2 Document wire (binary opcodes)
- `src/proto/wire.zig` defines a binary op protocol: `insert=1`, `find=2`, `insert_many=13`, `aggregate=17`, `count=12`, etc. Requests/responses are framed binary; the payloads are document bytes.

### 1.3 Typed cell value (the native value already in place for rows)
- `src/query/iterator.zig` `Cell` is a native tagged union (`null/text/int/uint/float/float32/boolean`) and `TableRow` is a positional set of `Cell`s. Row **storage** is already JSON-free. `std.json.Value` only survives as the *transient evaluator scalar* (`Cell.scalar()` returns it) and for the document / catalog / result-fallback shapes below.

## 2. Full JSON inventory (excluding config)

| File | Uses | Role |
|---|--:|---|
| `src/query/query_executor.zig` | 82 | evaluator scalar type; join / aggregate / non-catalog result fallbacks; catalog (`sys.*`) synthesis; parsing stored document JSON |
| `src/query/iterator.zig` | 22 | `Cell.scalar()`/`getScalar` return `std.json.Value`; `cloneJsonValue`/`freeJsonValue` for document values |
| `src/schema/database.zig` | 11 | `cloneJsonValue`/`freeJsonValue`; foreign-key definition and table-meta (de)serialise |
| `src/common/proto.zig` | 4 | replication `Operation` payload serialised as JSON |
| `src/query/replication.zig` | 2 | replication apply path over the JSON `Operation` |
| `src/durability/write_ahead_log.zig` | 2 | checkpoint / control record JSON (on disk) |
| `src/durability/checkpoint.zig` | 1 | `CheckpointRecord` JSON (on disk) |
| `src/query/stats.zig` | 1 | one stats row built as a JSON object |
| `src/main.zig` | 2 | HTTP `POST /query` convenience endpoint (JSON in/out) |
| `src/cli.zig` | 2 | CLI client renders the JSON body it receives |

## 3. Per-site migration plan

### Group A: SQL result encoding (query engine)  — the perf-relevant one
- **Single-table `SELECT *` numeric cells**: DONE (always binary, gate removed).
- **Multi-table join / non-catalog `SELECT *`** (`query_executor.zig` ~3389, ~3912): today the whole row is collapsed into **one JSON-object text cell** ("the single JSON-object blob the client expects"). Move: emit **positional binary cells** (one field per projected column) exactly like the single-table path. The driver already decodes positional binary rows, so this is a server-side change plus dropping the JSON blob contract for these shapes.
- **Aggregate / GROUP BY result** (`~4286`, `~4444`, `~4492`): same move, build positional cells not a JSON object.
- **`stats.zig`** (1): the stats row → positional cells.
- Effort: medium. Risk: the join/non-catalog "row is one JSON blob" is a **client contract**; changing it must land server + driver together (both are ours).

### Group B: the evaluator scalar type (query engine)  — DONE
- Added a native `Scalar` union (`null/bool/integer/float/string`) in `iterator.zig` and swapped the whole eval seam (`Cell.scalar`, `getScalar`/`getValCol`/`getVal`, `RowResolver`/`JsonResolver`, `evalScalar`/`evalScalarJson`, and the executor helpers `scalarTextInto`/`orderKeyString`/`scalarText`) off `std.json.Value`. Variant names match, so switch arms/literals were unchanged; the type checker flagged (and I fixed) the now-dead `else` arms and the one boundary where the eval scalar meets the Group A JSON blob (bridged with `Scalar.toJsonValue()`, to be removed when Group A lands).
- Verified: `zig build test` = 119/120 (only the pre-existing mutual-TLS replication test fails, on the clean tree too). `iterator.zig` `std.json`: down to 7 (all in the `cloneJson`/`freeClonedJson` document helpers = Group C).

### Group C-write: the write-path row image (INSERT / UPDATE / COPY)  — DONE
- The live write path built each new row as a `std.json.ObjectMap` (a `std.json.Value{.string=...}` per column), passed it to `writeNewVersion`, and the unique/foreign-key validators read it back as `v.string`. This is the per-row allocation that made the 1M-row load path expensive (it is the load half of the "17k/s vs 81k/s" complaint), and it is entirely in-memory (no disk-format coupling).
- Done: introduced `CatalogCellMap = std.StringArrayHashMapUnmanaged(query_iter.Cell)` and moved `writeNewVersion`, `UpdateTask.row_obj`, the three constraint validators, and all row-image builders (INSERT value builder, CSV/COPY builder, both UPDATE builders, and `stats.zig`'s stats row) off `std.json.Value` onto native `query_iter.Cell{.text=...}`. Reads switched `v.string`->`v.text`; the errdefer free loops switched their `.string` arm to `.text`. No JSON object is constructed on the write path any more.
- Verified: `zig build` clean; `zig build test` = 119/120 (only the pre-existing mutual-TLS replication test fails, and it fails on the clean tree too and is untouched by this change). This is distinct from the document at-rest JSON below, which remains disk-coupled.

### Group C: document values (query engine)  — the structural blocker
- Documents (MVCC versioned values) are persisted as **JSON text on disk** and parsed with `std.json.parseFromSlice` into `std.json.Value` (`query_executor.zig` ~5449; the versioned `{versions:[{xmin,xmax,data}]}` shape). `cloneJsonValue`/`freeJsonValue` (iterator + database) clone/free that in-memory value. BSON was stripped from kaidb, so there is no binary document form today.
- To remove JSON from this path you must change how a document is **stored and decoded**: either
  1. store documents in a binary encoding (reintroduce BSON, or a native length-prefixed doc format) and decode straight into native cells / a native doc value — this is a **disk-format change** (crosses the "leave disk formats" boundary; needs a version bump + migration or dual-read), or
  2. keep documents as JSON text on disk and accept that decoding them requires a parser (which is JSON) — i.e. this path cannot be fully JSON-free without a disk change.
- Recommendation: treat this as a separate, explicitly-approved workstream because it changes on-disk bytes. Everything else (Groups A, B, D, E) can be done without touching disk.

### Group D: catalog synthesis (query engine)  — DONE
- `sys.objects`/`sys.tables`/... rows were built as `std.json.ObjectMap`s then passed through `tableRowFromObject`.
- Done: added native `tableRowFromCellMap` (backed by `std.StringArrayHashMapUnmanaged(Cell)`, no `std.json`) and converted all 9 catalog row-builders in `buildCatalogRows` to it. `tableRowFromObject` remains only for the document at-rest path (Group C). Compiles clean; `zig build test` = 119/120 (the 1 failure is a pre-existing mutual-TLS replication test, unrelated and failing on the clean tree too). `std.json` in `query_executor.zig`: 82 -> 29.

### Group E: replication payload
- `src/common/proto.zig` `Packet.deserialize` parses the replication `Operation` from JSON; `replication.zig` applies it.
- Move: encode the `Operation` with a binary codec (the config-store already has a binary-safe codec pattern to mirror). Effort: medium. Risk: this is an **inter-node wire format**; primary and replica must upgrade together (version the replication handshake).

### Group F: leave as-is (not query engine / out of scope)
- `config.zig` — config file, explicitly excluded.
- `durability/checkpoint.zig`, `write_ahead_log.zig` — **on-disk** checkpoint/control records; disk format, leave (or a separate disk-format workstream).
- `main.zig` HTTP `/query` — a convenience HTTP API, JSON by design; not the binary wire.
- `cli.zig` — the CLI client renders whatever the wire sends; it follows the driver, no independent JSON once the wire is binary.

## 3.1 Driver (kyte-kaidb) status: already JSON-free

The `kyte-kaidb` driver contains **no JSON at all** (verified: zero `json` tokens, no
`import json`, no object parse/stringify). It decodes the wire through `codec.ky`
(frame parsing) and `typemap.ky` (per-type cell decode: text numerics via
`string.parseI64`/`parseFloat`, binary via `ieeeBeToStr`). Its `decodeDataRowBuf` is
positional and format-aware, so it already accepts binary cells for any shape.

The only JSON that reaches the driver boundary today is the server's join / non-catalog
`SELECT *` blob: the server sends the whole row as one JSON-object **text cell**, and the
driver passes it through as an opaque `DbValue` string (it does not parse it). When the
server's Group A change lands (positional binary cells for those shapes), that blob string
is gone and the driver receives typed cells only. So no driver change is required to remove
JSON; it is entirely a server-side job.

## 4. Suggested order (each step builds + `zig build test` green, no disk change until F is approved)

1. Group D (catalog synthesis → positional cells): smallest, self-contained, no wire/disk change.
2. Group A (join/aggregate/non-catalog results → positional binary cells) + driver: removes JSON from the SELECT wire for every shape.
3. Group B (evaluator scalar → native `Scalar`): mechanical once A/D remove the object/array pressure.
4. Group E (replication payload → binary): version the replication handshake.
5. Group C (document storage → binary): **disk-format change, needs explicit go-ahead + migration**; only after this is the query engine 100% JSON-free.

## 5. Bottom line

- The binary SQL wire and positional-cell driver already exist; Groups A, B, D are server-side refactors with no disk impact and are the bulk of the `std.json` count.
- The query engine cannot become **100%** JSON-free without changing how documents are stored on disk (Group C), because documents are JSON text today. That is the single hard dependency on a disk-format change, and it is called out separately so it is a decision, not a surprise.
