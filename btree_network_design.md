# NovaDB Networking & Protocol Design

_Design date: 2026-07-14. Replaces the current naive/dual wire protocols (see btree_readiness_plan.md
§5, §7) with a single standardized binary protocol, two transports (TCP binary-native + HTTP JSON
adapter), a full HTTP server (schnell-modeled), and a built-in SSE hub (ssehub-modeled) for WASM
server-side events._

---

## 1. Goals & shape

- **One canonical execution path.** Every request — whether it arrives as native binary over TCP or
  as JSON over HTTP — decodes to the *same* `Command` and runs through the *same* engine path.
- **A standardized binary wire protocol** modeled structurally on PostgreSQL (typed length-framed
  messages, startup/auth handshake, simple + extended/prepared query, typed row description, `-1`
  NULLs, streaming cursors, structured errors). This alone fixes every gap the audit found: no param
  binding → **Parse/Bind**; `"NULL"` sentinel → **`-1` length**; stubbed cursors → **Execute row-limit
  / PortalSuspended**; hardcoded/broken auth → **real handshake**; naive errors → **ErrorResponse**.
- **TCP is the heavy path** (binary native). **HTTP(S) is light** (a workbench/tooling front): it
  accepts JSON, turns it into a `Command`, runs it, returns JSON. But the HTTP *server* is
  full-fledged (TLS, sessions, keep-alive, streaming) — schnell-modeled.
- **Built-in SSE hub** so WASM apps (running in the embedded wasmer runtime) push server-side events to
  browser `EventSource`s — ssehub-modeled, reused nearly verbatim.

```
        ┌──────────────── TCP transport ────────────────┐
 client │ [type|len|payload] binary frames  ⇄  Session  │
        └───────────────────────┬────────────────────────┘
                                 │  Command / Result
        ┌───────────────────────┴────────────────────────┐
        │            Protocol core (canonical)            │
        │  message codec · type codecs · session/portal   │
        │  state machine · Command (decoded op) · Result  │
        └───────────────────────┬────────────────────────┘
                                 │  Command / Result
        ┌───────────────────────┴────────────────────────┐
 tools  │  HTTP(S) transport (schnell-modeled)            │
 browser│   POST /query  {sql,params} → Command → JSON    │
        │   GET  /events?topic=…  → SSE subscriber        │────┐
        │   GET  /            static workbench UI          │    │
        └──────────────────────────────────────────────────┘    │
                                                                 │ publish
        ┌──────────────── SSE Hub (ssehub) ──────────────────────┘
        │  EventBus · Subscriber(bounded queue) · ReplayRing · wire
        └───────▲─────────────────────────────────────────────────
                │ bus.publish(topic, {event,data})
        ┌───────┴───────────────┐        ┌───────────────────────┐
        │ WASM host import       │        │ DB commit hook        │
        │ sse_publish(topic,…)   │        │ (WAL/MVCC → change    │
        │ (wasmer guest calls)   │        │  stream, free)        │
        └────────────────────────┘        └───────────────────────┘
```

---

## 2. The binary wire protocol (PostgreSQL-modeled)

### 2.1 Framing
Uniform for every message (network byte order / big-endian):
```
[ msg_type : u8 ] [ length : u32 ]  [ payload : length-4 bytes ]
                    ^ length includes the 4 length bytes, excludes msg_type (pg convention)
```
Magic/version is negotiated in the Startup message (below), so the framing stays uniform. Max frame
size is enforced (config) to bound memory.

### 2.2 Connection lifecycle (state machine)
```
  (connect) → Startup → [AuthRequest ⇄ AuthResponse]* → AuthOk
            → ParameterStatus* → BackendKeyData → ReadyForQuery(Idle)
            → { SimpleQuery | ExtendedQuery }*  (each ends with ReadyForQuery)
            → Terminate
```
`ReadyForQuery(status)` carries txn status `I`dle / `T`ransaction / `E`rror — the client turn-taking
signal (fixes the current "just send bytes" model).

### 2.3 Message set

**Frontend (client → server)**
| Type | Name | Payload |
|---|---|---|
| — | Startup | proto_version(u16.u16) + key/value params (user, database, application_name) |
| `p` | AuthResponse | opaque credential bytes (password / SASL step) |
| `Q` | Query | SQL text (simple query) |
| `P` | Parse | stmt_name, sql, param_type_oids[] |
| `B` | Bind | portal, stmt_name, param_formats[], param_values[] (len i32, -1=NULL), result_formats[] |
| `D` | Describe | 'S'tatement|'P'ortal + name |
| `E` | Execute | portal, max_rows (0 = all) |
| `S` | Sync | — (ends an extended-query batch → ReadyForQuery) |
| `C` | Close | 'S'|'P' + name |
| `X` | Terminate | — |

**Backend (server → client)**
| Type | Name | Payload |
|---|---|---|
| `R` | AuthRequest | method (Ok / CleartextPassword / SASL(SCRAM-SHA-256)) |
| `S` | ParameterStatus | key, value (server_version, integer_datetimes, …) |
| `K` | BackendKeyData | pid(u32), secret(u32) — for out-of-band cancel |
| `Z` | ReadyForQuery | txn_status(u8: I/T/E) |
| `T` | RowDescription | field_count(u16) + per field {name, table_id(u32), col_no(u16), type_oid(u32), type_size(i16), type_mod(i32), format(u16 0=text/1=binary)} |
| `D` | DataRow | field_count(u16) + per field {len(i32, -1=NULL), bytes} |
| `C` | CommandComplete | tag (e.g. "SELECT 3", "INSERT 0 1", "UPDATE 2") |
| `1`/`2`/`3` | Parse/Bind/CloseComplete | — |
| `s` | PortalSuspended | (Execute hit max_rows; client re-Executes to resume — streaming cursor) |
| `I` | EmptyQueryResponse | — |
| `E` | ErrorResponse | fields: severity, code(SQLSTATE-like 5-char), message, detail?, hint?, position? |
| `N` | NoticeResponse | same shape as ErrorResponse (non-fatal) |

### 2.4 Types
A small type-OID registry with **binary and text** codecs, mapping the engine's `types.zig`
(`BOOL, INT32/64, UINT32/64, FLOAT32/64, TEXT, BLOB, DECIMAL(128), TIMESTAMP`) to OIDs. Binary format
is canonical for the TCP path; text format is used by the JSON/HTTP adapter. NULL is length `-1`
(never a value) — **kills the `"NULL"` sentinel bug**.

### 2.5 What this fixes vs today (readiness §5)
Parameter binding (Parse/Bind) → no more injection-only; typed RowDescription → no stringly-typed
cells; `-1` NULL → real nulls; Execute max_rows/PortalSuspended → real streaming cursors (FETCH stub
gone); Startup+Auth handshake + BackendKeyData → real sessions + cancel; ErrorResponse w/ codes →
standardized errors; single protocol → the JSON/binary duality is gone.

---

## 3. Transports

### 3.1 TCP (binary-native, the heavy path)
Accept loop → per-connection **`Session`** (Planck-modeled, readiness §7b): owns the socket, read/
write buffers, auth state, txn state, **prepared-statement map + portal map**, activity timestamp;
pooled via a `SessionPool` (reset-on-reuse). The session runs the §2.2 state machine: frame in →
decode to `Command` → execute → encode `Result`/rows/error → frame out. Keep-alive; idle-timeout reap;
`max_sessions` backpressure. TLS optional (same `tls` package as HTTP).

### 3.2 HTTP(S) (JSON adapter, the light path)
Full HTTP/1.1 server (schnell-modeled — see §5, pending the transport study). Routes:
- `POST /query` — body `{ "sql": "...", "params": [...], "tx": "..."? }`. The handler builds the
  **same `Command`** the binary Parse/Bind/Execute produces (params typed via the same codecs), runs
  it, and serializes `Result` to JSON `{columns:[{name,type}], rows:[[...]], tag, rows_affected}`.
  → "HTTP sends JSON which becomes the binary operation and executes": the `Command` *is* the decoded
  binary op; we skip a redundant re-encode to bytes (recommended), or optionally synthesize real
  binary frames and feed the same decoder (strict-uniformity variant — a config/impl choice).
- `GET /events?topic=…` — SSE subscription (§4).
- `GET /…` — static workbench UI (an in-browser SQL console).
Auth via a session token / bearer; light traffic, so simplicity over throughput.

### 3.3 Shared session model
Both transports produce `Command`s into one engine executor and share the `Session` concept (auth,
txn, activity). TCP sessions are binary/stateful (portals); HTTP sessions are request-scoped + a token.

---

## 4. Built-in SSE hub (ssehub-modeled) + WASM integration

**Reuse verbatim** (zero external coupling, all on `std.Io`): `event_bus.zig` (topic registry +
publish + per-topic replay + heartbeat fiber + bounded-queue back-pressure), `subscriber.zig`
(bounded `Io.Queue` + `runWriter` drain fiber + `write_mutex` socket serialization), `replay_ring.zig`
(Last-Event-ID history), `wire.zig` (SSE frame + header writers), `metrics.zig`.

**Delete** `watch_client.zig` (its whole job is polling an *external* planck watch — meaningless for a
local source). Replace the producer seam with:
- **WASM host import** `sse_publish(topic, event, data)` — the wasmer guest calls it; the native impl
  reads guest memory and calls `bus.publish(topic, .{event, data})`. The bus owns per-topic monotonic
  `id` (keeps Last-Event-ID resume correct) — WASM does not supply ids.
- **DB commit hook** (optional, powerful): the commit path calls `bus.publish("table:orders", …)` →
  table **change-streams** to browsers for free (same shape as the WASM publish).

**Endpoint contract** (`GET /events?topic=…`): `wire.writeHeaders` (text/event-stream, no-cache,
`X-Accel-Buffering: no`, CORS) → `wire.writeRetry` → parse `Last-Event-ID` → `Subscriber.init(writer)`
→ `bus.subscribe(topic, sub, {last_event_id})` (drains replay ring first, then live) → `sub.runWriter()`
(blocks the request fiber, draining the queue and writing `id:`/`event:`/`data:` frames until the
connection dies). This requires the HTTP server to hand the handler a **long-lived writer it can keep
writing to** — the streaming seam the schnell study is confirming (§5).

**Concurrency note:** `EventBus.init(io)` takes the DB server's `Io`; publish is fiber-safe via
registry + per-topic + per-subscriber mutexes. If the wasmer runtime executes off the `Io` scheduler,
`sse_publish` must marshal onto an `Io` fiber (or rely on the `Io.Threaded` mutexes being thread-safe).

---

## 5. HTTP server internals (schnell-modeled)

**NovaDB already vendors schnell** — `main.zig` builds `schnell.Server` and uses `setRawHandler`
(`src/common/schnell.zig`, 566 lines, a trimmed copy). But it's degraded (per-byte `std.debug.print`,
readiness §7a). So this is an **upgrade to the full schnell server**, not a from-scratch build.

**Architecture (adopt as-is):**
- **`Io.Group` task-per-connection** over `std.Io.Threaded`: single accept loop → `group.async(io,
  handleConnection, …)` per socket; atomic `active_connections` cap; graceful drain (`drain_timeout`
  then `group.cancel`); `TCP_NODELAY`/`SO_KEEPALIVE` set per socket. Each connection is a cheap fiber
  → many concurrent SSE subscribers cost one fiber each, not a thread.
- **Transport-agnostic reader/writer seam (the pattern to copy):** TLS is presence-based (cert+key
  files non-empty) via the vendored pure-Zig `tls` package; plain and TLS **both funnel into one
  `runRequestLoop(reader: anytype, writer: anytype)`** that only touches the `std.Io.Reader`/`Writer`
  interface — so **HTTPS is free** and the request machinery never knows if bytes are encrypted.
- **Per-connection:** keep-alive loop, **per-request arena allocator** (freed in one shot),
  `max_requests_per_connection` cap.
- **Request parsing:** byte-by-byte header read to CRLFCRLF (413 on overflow) + a hardened
  **`validateFraming`** pass — **copy wholesale** (rejects `Transfer-Encoding`, duplicate
  `Content-Length`, obsolete line-folding, whitespace-before-colon; reads body strictly by
  Content-Length with header→body carryover). Anti-smuggling, 9 tests included.
- **Response:** buffered serialize-then-write, with a Content-Length-framed streaming fallback for
  oversized bodies.

**The SSE streaming seam (already first-class in schnell — this is our SSE mechanism):**
`Response.streaming_handler: ?*const fn(ctx, alloc, *Request, *std.Io.Writer) anyerror!void`. When set,
the server hands the handler the **raw `*std.Io.Writer` (TLS-agnostic) and stops managing the
connection** — the handler writes its own `text/event-stream` headers then pushes `data:` frames with
`flush()` indefinitely, and the loop `return`s (no keep-alive) when it ends. So the `GET /events`
handler (§4) = a streaming route whose ctx is the SSE `Subscriber`; `sub.runWriter()` blocks that
connection fiber draining the bounded queue. Client-disconnect is detected via write error → the
ssehub heartbeat fiber reaps dead subscribers.

**Gaps in schnell to fix before shipping** (per the study):
1. **Enforce `idle_timeout_ms`** — the read loop blocks in `peekByte` with no timeout (slowloris
   risk). Wrap header/body reads in `Io.Select` against a sleep (the schnell *client* already shows
   the pattern).
2. **`Expect: 100-continue`** — absent; add if HTTP accepts large bodies (bulk load).
3. **Real HTTP chunked output** — current streaming is Content-Length-only; route large/streamed
   results through the streaming seam or add true chunked.
4. **IPv4-only bind** — add IPv6/hostname if needed.
5. Strip the per-byte `std.debug.print` in the vendored copy (throughput killer).

**Optional:** schnell's `SessionStore` (cookie/token, TTL, LRU, timing-safe lookup, prune fiber) —
pull in only if the workbench UI needs cookie auth; independent of transport.

---

## 6. THE key decision — Postgres wire-*compatible* vs Postgres-*inspired*
- **Wire-compatible:** psql, DBeaver/pgAdmin, and every language's Postgres driver + ORMs work
  **out of the box** — an enormous tooling/adoption win (this is why CockroachDB/Yugabyte/QuestDB do
  it). Cost: must faithfully implement pg's exact message set, type OIDs, SCRAM auth, and SQLSTATE
  codes, and constrain the wire type system to pg's (the 128-bit DECIMAL and some types need mapping
  choices). More work; some semantics are pinned to pg expectations.
- **Postgres-inspired (recommended default):** adopt pg's proven *structure* (framing, handshake,
  extended query, RowDescription/DataRow, ErrorResponse) with our own OIDs and a clean, tailored
  message set. Needs our own driver — which we're **already building for Nova** — and the workbench
  speaks JSON over HTTP anyway. Simpler, fully tailored, no compat burden.

**DECISION (2026-07-14): Postgres-INSPIRED.** Clean custom protocol on pg's structure, our own
OIDs + message set, targeted by the Nova driver. (Original recommendation below.)

Recommendation: **Postgres-inspired**, unless "any Postgres client works instantly" is a product
requirement — in which case go wire-compatible and treat pg-compat as the spec. This choice gates the
protocol module's message/type tables and the driver.

---

## 7. Phased build plan
1. **Protocol core** — `Command`/`Result`, message codec + framing, type codecs (binary+text),
   session/portal state machine, `ErrorResponse`. (Fixes param binding, NULL, types, cursors, errors.)
2. **TCP transport** — Session/SessionPool (Planck-modeled), the frame loop, TLS. Wire to the engine.
3. **Nova driver** — target the new binary protocol (replaces the "target JSON for now" stopgap).
4. **HTTP server** — schnell-modeled; `POST /query` JSON→Command bridge + static workbench.
5. **SSE hub** — vendor ssehub's 5 modules, delete WatchClient, add the `GET /events` handler + the
   `sse_publish` WASM host import + (optional) DB commit-hook publisher.
6. **Cancel/auth hardening** — BackendKeyData cancel, SCRAM (or chosen auth), rate limiting.
