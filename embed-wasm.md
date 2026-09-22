# Embedding a WebAssembly execution engine in kaidb

Status: design, pre-implementation. Deep analysis for review before any code lands.

## 1. Why we are doing this

kaidb today is a competent single-node relational engine: clustered B+Tree storage,
MVCC, WAL, doublewrite, covering indexes, read replicas. All of that is table stakes.
Every serious database has it. Nothing in that list makes a developer choose kaidb over
PostgreSQL, SQLite, or InnoDB.

The differentiator we are chasing is **programmable, deterministic, in-database compute**:
a user uploads a compiled WebAssembly module, registers it as a function, and kaidb runs
it sandboxed, metered, and deterministically, right next to the data, with no network
round trip. The interesting cases are not scalar toys, they are user defined filters and
aggregates that get pushed into a scan, and derived values that we compute once and
persist as ordinary columns or index keys.

The moat is not "we can run WebAssembly". Anyone can link a runtime. The moat is that we
run it with the guarantees a database needs and a generic host does not provide:

- **Determinism** so a UDF produces the same output on the primary and every replica, and
  on replay during recovery. This is what makes it safe to persist UDF output.
- **Metering** (fuel plus a wall clock backstop plus hard memory caps) so an adversarial
  or buggy module cannot stall or exhaust the server.
- **A tight, audited host ABI** so a module can read the current row read only and emit a
  result, and can do nothing else. No filesystem, no clock, no network, no randomness.
- **Persistence** so the lowered form of a module and its deterministic outputs survive
  restarts.

This document is the design for that subsystem. It covers the decision to fork the zware
engine into our tree, the execution layer that instantiates uploaded modules, the
marshalling layer (the "drivers" that write request frames into linear memory and read
responses back out), metering, determinism, persistence, the SQL surface, the threat
model, perf tuning, and a staged roadmap.

## 2. Baseline: what zware gives us and where it falls short

We evaluated `malcolmstill/zware` (MIT, Zig, roughly 412 stars, maintained through mid
2026). It is a strong base and a natural fit for kaidb, which is pure Zig with no Rust in
the build and ships a single binary. Reading the source rather than the README, the
relevant facts are:

**What is already good.**

- **Tail-call threaded dispatch.** The interpreter core in `src/instance/vm.zig` dispatches
  with `@call(.always_tail, lookup[opcode], ...)`. Each opcode is a function that tail
  calls the next. This is the fastest interpreter technique short of a JIT, close to
  computed goto. There is also a `fast_call` opcode for pre-resolved direct calls.
- **Decode and validate are separate from instantiate.** `Module.decode()` parses and
  there is a dedicated `src/module/validator.zig`. Validation can therefore be a hard gate
  before any code executes.
- **The Module / Instance / Store split we want.** `Module.parsed_code` is the immutable
  lowered program. An `Instance` plus `Store` hold the mutable per-execution state
  (linear memory, globals, stacks). This is exactly the compile once, run many model.
- **A flat, pointer free lowered form.** The lowered instruction type `Rr` (in `src/rr.zig`)
  is a `union(RrOpcode)` whose payloads are only integers and instruction-index branch
  targets (`branch_target: u32` and similar). There are no pointers or slices in the
  payloads. This is decisive for persistence: `parsed_code` serialises to a page almost as
  a raw byte blob, with no relocation pass. It is the position independent bytecode we
  would otherwise have had to design ourselves.
- **Stack overflow safety.** The VM guards the operand, frame, and label stacks
  (`OperandStackOverflow`, `ControlStackOverflow`, `CheckStackSpace`). Stack sizes are set
  by the host when the VM is initialised.
- **A usable host and memory API.** A host import is `fn(*VirtualMachine, usize)`. It pops
  its integer arguments off the operand stack and pushes results. `Memory` exposes
  `read(T, offset, address)`, `write(T, offset, address)`, `memory() []u8`, and a `check()`
  bounds guard. That is precisely the substrate the marshalling layer needs.
- **Coverage.** WebAssembly 2.0 minus SIMD, the official test suite passes, and there is a
  fuzzer in `test/fuzzer`.

**Where it falls short for a database.**

- **No metering at all.** A search for fuel, gas, deadline, epoch, or instruction count
  across the VM returns nothing. A `loop ... br 0` runs forever. This is the single biggest
  gap and the first thing we add.
- **Memory cap is soft.** `store/memory.zig` `grow()` honours a module's declared `max`,
  but when a module declares no maximum it allows growth up to the full wasm32 limit
  (`MAX_PAGES`, 64K pages, 4 GiB). We must impose our own per-call cap irrespective of what
  the module declares.
- **No determinism guarantees.** NaN canonicalisation and the exclusion of nondeterministic
  imports are not addressed, because a general purpose runtime does not need them. We do.
- **Alpha quality, by the author's own description.** Fine as a base, not fine to trust as
  is on a server that runs untrusted modules. It needs a hardening and test layer that is
  ours.
- **A build constraint.** Tail-call dispatch requires the LLVM backend. zware issues a
  `@compileError` on Zig's self-hosted x86 backend. kaidb builds ReleaseFast with LLVM on
  macOS arm64 and Linux, so this is satisfied, but it means the WASM path cannot be built
  with the self-hosted backend. We document this and gate it in `build.zig`.

## 3. Decision: fork into `src/wasm/`, do not vendor

We will **absorb the engine into kaidb's own source tree** under `src/wasm/engine/`, not
pull it as a package dependency. Rationale:

- We are going to diverge heavily. Metering lives in the dispatch core, determinism
  touches the float opcodes, the host ABI is kaidb specific, and caching is bound to our
  page store. A vendored dependency with a wrapper on the side would fight all of that.
- Owning the files lets us fit them to kaidb's allocator conventions, error set, logging,
  and `src/proto` codec, and lets us delete the parts we do not want (WASI, the demo
  tooling) rather than carry them.
- Upstream sync value is low. The engine is alpha and we will be ahead of it on everything
  that matters to us. We would be porting our fixes upstream more than pulling theirs down.
- One obligation: MIT requires that we preserve the copyright and licence notice. We keep
  Malcolm Still's copyright header in the forked files and record the fork in
  `src/wasm/engine/NOTICE`, with the upstream commit hash we forked from. Our additions
  carry the kaidb licence.

The forked engine becomes a normal kaidb module. From that point on it is our code, held
to our gate (`gate.sh`), our fuzzers, and our ASAN runs.

### 3.1 Proposed layout

```
src/wasm/
  engine/            forked zware, trimmed to what we use
    vm.zig           interpreter core (+ our fuel hook, + NaN canonicalisation)
    rr.zig           lowered instruction type (unchanged shape, widths pinned)
    module.zig       decode
    validator.zig    validation (+ our proposal-surface restrictions)
    store/           memory, table, global, function, data, elem
    NOTICE           fork provenance + MIT attribution
  meter.zig          fuel model, cost table, deadline flag, memory + output caps
  host.zig           the deterministic host import set (the only imports a UDF sees)
  marshal.zig        request/response framing to and from linear memory (the "drivers")
  abi.zig            the versioned ABI contract (exports the guest must provide, imports we offer)
  instance_pool.zig  per-thread pool of reset-between-calls instances
  cache.zig          parsed_code serialisation, checksum, format-version tag, page storage
  catalog.zig        CREATE/DROP FUNCTION, system tables, module versioning
  udf.zig            the executor entry points (scalar, row-facing filter, aggregate)
```

The engine subtree stays as close to a leaf as possible. Everything database specific
lives in the sibling files so that engine internals have exactly two intentional
modifications (the fuel hook and NaN canonicalisation), both of which are small and
upstreamable.

## 4. Execution layer: from an uploaded module to a running call

This is the heart of the second half of the request. There are two lifecycles that must
not be conflated: the **module lifecycle** (once per uploaded module) and the **call
lifecycle** (once per row or per query, potentially billions of times).

### 4.1 Module lifecycle (expensive, done once, cached)

1. **Upload.** A module arrives as raw `.wasm` bytes, through `CREATE FUNCTION ...
   LANGUAGE wasm AS <bytes>` over the wire protocol, or through a dedicated admin path for
   large modules. The raw bytes are stored in the catalog and are the authoritative source
   of truth. They go through WAL and doublewrite like any other schema object, so a module
   definition survives a crash.
2. **Decode.** `engine/module.zig` parses the binary into structured sections.
3. **Validate.** `engine/validator.zig` type checks the stack, bounds checks every index,
   verifies control structure, and (our addition) rejects any proposal outside the allowed
   surface (see section 8). Validation is a hard gate: nothing that fails validation is
   ever lowered or executed.
4. **Lower.** The validated module is lowered to `parsed_code` (`[]Rr`) with branch targets
   resolved to instruction indices. This is the reusable "interpreted output".
5. **Signature check.** We confirm the module exports the ABI functions we require
   (section 6) and imports only host functions we provide. A module that imports anything
   unknown is rejected at registration, not at call time.
6. **Cache.** The lowered `parsed_code` plus a small amount of module metadata is placed in
   an in-memory module cache keyed by function identity and version, and optionally
   serialised to a page-backed cache (section 7). The compiled module is immutable and is
   shared across all query threads.

The expensive work (decode, validate, lower) happens once at registration and once more
per process start if the page cache is cold. It never happens per row.

### 4.2 Call lifecycle (cheap, per row or per query)

1. **Acquire an instance.** From `instance_pool.zig`, take a pooled `Instance` plus `Store`
   for this module, or create one if the pool is empty. An instance owns a fresh (or reset)
   linear memory, fresh globals initialised from the module's data segments, and the three
   VM stacks.
2. **Set limits.** Install this call's fuel budget, wall-clock deadline, memory cap, and
   output cap into the meter (section 5).
3. **Marshal the request.** Write the arguments or the current row into the instance's
   linear memory as a frame (section 6). Get back the guest pointer.
4. **Invoke.** Call the exported entry point with the frame pointer and length. The VM runs
   the tail-threaded loop over `parsed_code`, decrementing fuel per instruction.
5. **Read the response.** The entry point returns a packed pointer and length (or writes to
   an out region). The host bounds-checks that region against the guest memory size and
   reads the response frame back out (section 6).
6. **Recycle.** Reset the instance (zero or discard the dirty memory, reset globals and
   stack pointers, refund nothing, fuel is per call) and return it to the pool.

For a scalar pure UDF the instance is tiny, a small memory plus stacks, so reset and reuse
is close to free and there is no per-row allocation. This is why an interpreter is fast
enough here: the per-instruction cost is real but the per-call setup cost is amortised
away by pooling, and the decode and validate cost is amortised away by the module cache.

### 4.3 Concurrency model

- **Module (`parsed_code`) is immutable, therefore shared** across kaidb's query threads
  with no lock. It is read only after lowering.
- **Instance and Store are mutable, therefore never shared.** Each concurrent call needs
  its own. We keep a small pool per worker thread to avoid cross-thread synchronisation and
  false sharing. An instance is checked out, used by exactly one call, then returned.
- We need a confirmation pass that the forked VM holds no process-global mutable state. If
  any is found (for example a shared scratch buffer), it moves into the per-instance state.
- This composes cleanly with kaidb's execution model. Scalar and filter UDFs run
  synchronously inside the query operator that calls them, on that operator's thread, with
  that thread's instance pool. No new reactor, no new thread pool.

## 5. Metering: fuel, deadline, memory, output

Metering is the property that turns "we can run arbitrary code" into "we can safely run
untrusted code". Four independent limits, all set per call.

### 5.1 Fuel (instruction budget)

- A `fuel: u64` counter on the meter. Every instruction costs at least one unit. The
  decrement is placed at the single dispatch chokepoint in `vm.zig` (the tail-call
  `dispatch`, plus the `invoke` entry and the `miscDispatch` secondary table). Because
  dispatch is centralised, this is one insertion point, not a change to every opcode.
- When fuel reaches zero the VM traps with `error.FuelExhausted`, which surfaces to SQL as
  a UDF resource error, aborting only that statement, not the server.
- A **cost table** lets us charge more than one unit for expensive opcodes (`memory.grow`,
  `call_indirect`, division). v1 can charge one unit uniformly and refine later. Costs are
  fixed constants (not timings) so that fuel accounting is itself deterministic.
- Fuel budgets are policy: a per-call default, overridable per function at `CREATE
  FUNCTION` time, capped by a server maximum in `db.json`.

### 5.2 Wall-clock deadline (backstop)

Fuel bounds work but not time (a module could execute a legitimate but enormous number of
cheap instructions). A coarse deadline flag, checked in the same dispatch hook every N
instructions, traps with `error.Deadline`. This is a backstop, not the primary control,
and it does not affect determinism because it only ever aborts, it never changes a
successful result.

### 5.3 Memory cap

- Impose a hard per-call page cap at instantiation, independent of the module's declared
  maximum, by setting `Memory.max` to `min(policy_cap, module_declared_max_or_policy)`.
  This closes the soft-cap gap in section 2.
- `memory.grow` beyond the cap returns the wasm-defined failure value to the guest (it does
  not trap), which is spec compliant and deterministic.

### 5.4 Output cap

The response frame a UDF emits is bounded. A module cannot force the host to read back a
multi-gigabyte result. Exceeding the cap is a trap.

### 5.5 Stack caps

Already present in the engine as fixed stack slices. We expose their sizes as policy so an
operator can tune call depth and operand-stack depth, with safe defaults.

## 6. The host and guest ABI: writing frames to linear memory, reading them back

This is the "drivers" the request calls out. WebAssembly can only exchange scalars
(`i32`, `i64`, `f32`, `f64`) plus a shared linear memory. Every structured value (a string,
a row, a decimal, a null, a multi-column argument list) must be **serialised into linear
memory** as a byte frame, with pointers passed as `i32` offsets into that memory. The ABI
is the contract for how that framing works. It must be explicit, versioned, and bounds
safe, because the host will dereference offsets that the guest supplies.

### 6.1 The two directions and the allocation problem

The host cannot simply pick an address in the guest's memory and write there, because that
region might hold live guest data. The guest must own allocation inside its own memory.
Therefore the ABI requires the module to **export an allocator**:

```
(export "kaidb_alloc"  (func (param i32) (result i32)))   ;; size -> ptr, or 0 on failure
(export "kaidb_free"   (func (param i32 i32)))            ;; ptr, size
(export "memory"       (memory ...))
```

Request path (host to guest):

1. Host serialises the request frame into a host-side buffer.
2. Host calls the guest's `kaidb_alloc(len)` to get a guest pointer `p`.
3. Host writes the frame bytes into guest memory at `p` using `Memory.write` /
   `uncheckedCopy`, after a `check()` that `p + len <= memory size`.
4. Host calls the entry point with `(p, len)`.

Response path (guest to host):

1. The guest builds its response frame in its own memory (using its own allocator) and
   returns a packed `i64` result: `(ptr << 32) | len`, or writes `ptr` and `len` to a small
   host-provided out region.
2. Host unpacks `ptr` and `len`, bounds-checks `ptr + len <= memory size`, and copies the
   response bytes out with `Memory.read` over the slice.
3. Host optionally calls `kaidb_free(ptr, len)`. For a pooled instance that is about to be
   reset this can be skipped, the reset reclaims everything.

The entry point signature for a scalar UDF is therefore:

```
(export "kaidb_udf_invoke" (func (param i32 i32) (result i64)))  ;; (req_ptr, req_len) -> packed(ptr,len)
```

### 6.2 Frame encoding: reuse the kaidb wire codec

We do not invent a new serialisation. kaidb already has a binary wire protocol in
`src/proto/wire.zig` and `src/proto/protocol.zig`, with encoders for the value types the
engine speaks (integers, floats, text, bytes, decimal, timestamps, null). The marshalling
layer encodes the request frame and decodes the response frame with that same codec. This
gives us three things for free: a single source of truth for value encoding, automatic
coverage of the full `DbValue` type set, and a byte layout we already fuzz and version.

A request frame for a scalar UDF is: a small header (frame ABI version, argument count),
then each argument encoded as `(type tag, length, bytes)`. A response frame is: a header
(status, type tag), then the encoded return value, or an encoded error. Nulls are a
first-class tag, not a sentinel, so `NULL` in and `NULL` out are unambiguous.

### 6.3 Two host-ABI styles: push versus pull

For **scalar UDFs** the push model above is right: serialise the arguments into guest
memory once, invoke, read the result. Simple and deterministic.

For **row-facing UDFs** (a filter or aggregate applied across a scan) there is a choice:

- **Push the whole row.** Marshal every column of the current row into a frame each call.
  One marshalling cost per row, simple, but wasteful when the UDF touches only one column
  of a wide row.
- **Pull columns on demand.** Instead of pre-serialising the row, expose host imports the
  guest calls when it actually needs a value:

  ```
  (import "kaidb" "col_i64"  (func (param i32) (result i64)))   ;; column index -> value
  (import "kaidb" "col_f64"  (func (param i32) (result f64)))
  (import "kaidb" "col_bytes"(func (param i32 i32) (result i32))) ;; idx, dst_ptr -> len (into guest mem)
  (import "kaidb" "col_is_null" (func (param i32) (result i32)))
  (import "kaidb" "emit"     (func (param i32 i32)))            ;; ptr,len: emit a result row/value
  ```

  These imports receive `*VirtualMachine`, pop their integer arguments, read the bound
  row from kaidb's executor (read only, against the MVCC snapshot), and for byte-typed
  columns write into a guest buffer the guest supplied. Fewer copies for wide rows, at the
  cost of a host-call per accessed column.

The plan is push for scalar (v1), pull for row-facing (later milestone), sharing the same
`marshal.zig` encoders. The pull imports are the surface that has to be audited hardest,
because they hand guest code a window onto live storage.

**Status (M3, done).** The pull model is implemented in `src/wasm/host.zig`. A module that
imports any of `kaidb.col_count`, `kaidb.col_i64`, `kaidb.col_f64`, `kaidb.col_is_null`, or
`kaidb.col_bytes` (and nothing else) is classified as row-facing at `CREATE FUNCTION` time; an
import outside that allowlist, a non-function import, or a foreign namespace is rejected there,
so a UDF that could reach a clock, randomness, or WASI never registers. The executor
(`iterator.zig`) binds the current scan row as a read-only `RowCtx` and invokes the module's
zero-argument `kaidb_udf` export; the host functions read that bound row and, for `col_bytes`,
bounds-check the guest destination pointer before writing (section 6.4). `col_bytes` on a NULL
cell returns length -1 so the guest can tell NULL from an empty string. The predicate is
evaluated in the WHERE/filter path exactly like a scalar UDF, so `WHERE rowfn() = 1` pushes a
pull-model filter into the scan. Columns are addressed positionally in the projected row's
order; forcing full-row materialisation when a row-facing predicate is present (so the guest
always sees every column in schema order, regardless of projection pushdown) is a follow-up.
The aggregate accumulate/finalise surface is implemented in M5 (custom aggregates via
`CREATE AGGREGATE`; see the roadmap). `emit` and `merge` remain for later slices.

### 6.4 Bounds safety is the whole game

Every offset and length the guest hands the host is untrusted. Before the host reads or
writes a single byte at a guest-supplied `(ptr, len)`, it validates `ptr + len` against the
current memory size and rejects on overflow or overrun. zware's `Memory.check` gives us the
primitive; the rule is that no host-side marshalling code touches guest memory without it.
A guest that lies about a pointer gets a trap, never an out-of-bounds host read. This
invariant is a hard review item and a fuzz target.

### 6.5 Value-type mapping

`marshal.zig` owns the mapping between `DbValue` and the frame encoding:

- integers and booleans: direct.
- floats: IEEE 754, with NaN canonicalised on the way out of the guest (section 7).
- text and bytes: length-prefixed, UTF-8 validated on the way in for text columns.
- decimal128: kaidb's exact decimal encoding, not a lossy float.
- timestamps and dates: kaidb's existing encoding from `common/datetime.zig`.
- null: a dedicated tag at the frame level, orthogonal to type.

The guest side of this mapping is provided as a small guest library (for Kyte first, since
Kyte compiles to wasm; C and Rust guests can implement the same frame format). That library
is how a UDF author reads arguments and returns a value without hand-rolling the byte
layout.

## 7. Determinism specification

Persisting UDF output and replicating UDF-bearing statements are only sound if a module is
a pure deterministic function of its declared inputs. The engine must enforce that.

- **NaN canonicalisation.** WebAssembly permits nondeterministic NaN bit patterns from some
  float operations. We audit every float opcode in `vm.zig` and force the canonical NaN
  bit pattern on any NaN result. Without this, two replicas can compute bitwise different
  floats from the same inputs. This is the second intentional engine modification.
- **No nondeterministic imports.** The only imports a UDF can bind are the ones `host.zig`
  offers. That set excludes clocks, wall time, randomness, the filesystem, the network, and
  all of WASI. A module that imports anything else fails validation at registration.
- **No shared memory or atomics.** The threads/atomics proposal is rejected at validation
  (section 8), so there is no data-race-derived nondeterminism.
- **Deterministic resource behaviour.** `memory.grow` failure is deterministic (spec
  defined). Fuel accounting uses fixed integer costs, not measured time, so it does not
  perturb results. The wall-clock deadline only ever aborts; it cannot change a produced
  value.
- **Stable float mode.** No fast-math, no contraction that would vary by host. The
  interpreter evaluates float ops exactly per the wasm spec.

A module that passes validation and runs to completion under these rules is, by
construction, a deterministic function from (arguments, bound row) to (result). That is the
precondition for persisting its output as a computed column or index key, and for replaying
it during recovery.

## 8. Proposal surface: what we accept and what we reject

Smaller surface, smaller attack surface, fewer determinism holes. The validator enforces:

- **Accept:** the MVP core (integers, floats, locals, globals, calls, control flow, linear
  memory load/store), sign-extension ops, and bulk-memory (useful for the guest allocator
  and frame copies).
- **Reject at validation:** threads and atomics (nondeterminism, races), multi-memory,
  reference types and tables beyond what the guest allocator needs, and, at least
  initially, SIMD (it is WIP upstream and its determinism needs its own audit). Rejecting is
  a hard error at registration, so an unsupported module never reaches execution.

Each rejection is a named validator rule with a test, so the accepted surface is explicit
and cannot drift silently.

## 9. Persistence and caching

Three distinct things could persist. They have different answers (this mirrors the earlier
analysis and now becomes concrete).

1. **Module source (`.wasm` bytes): persist, authoritative.** Stored in the catalog,
   WAL-backed, doublewrite-protected. Survives crash and restart. This is the source of
   truth.
2. **Lowered `parsed_code`: persist optionally, as a regenerable cache.** Because `Rr` is
   pointer free, we can serialise `parsed_code` (plus the small `br_table` range side-table)
   into a page-backed blob keyed by `(function_id, wasm_hash, engine_format_version)`, with
   a checksum. On load, if the checksum fails or the format-version tag does not match the
   running engine, we discard the blob and recompile from the stored `.wasm`. The cache is
   therefore never trusted and never load-bearing for correctness; it only saves decode and
   validate work at process start. One portability nit: pin the widths in the serialised
   form (for example `call: usize` in `Rr` is arch-width in memory; the on-disk form uses a
   fixed width) so a cache written on one arch is not misread on another. For a first
   implementation we may skip the page cache entirely and recompile at start, since for a
   handful of small modules that is microseconds to low milliseconds. The point is that the
   design does not preclude the cache, and the flat `Rr` makes it cheap when we want it.
3. **Per-call instance memory: never persist.** Ephemeral by construction.

The high-value persistence is not the code, it is **deterministic UDF output**: computed
columns whose value is `f(row)` for a wasm `f`, and secondary indexes keyed by a wasm
expression. Those are stored as ordinary durable data through the normal page store, WAL,
and doublewrite, and are safe precisely because section 7 guarantees `f` is deterministic.
This is the feature that makes the subsystem more than a novelty: compute once, persist,
reuse across every query and every restart.

**Status (M4, done for computed columns).** A column can be declared
`CREATE TABLE t (..., c TYPE AS fn(args))`. The generating expression is stored as durable
catalog metadata (a new `computed_by` field on `ColumnMetadata`, serialised under flag bit
`16`; a catalog written by an older engine reads back with the field absent, so the format
is backward compatible). At insert and update time the executor evaluates `fn(args)` over the
sibling cells through the same scalar-UDF path predicates use (`computeGeneratedCell` in
`query_executor.zig`, hooked into the single `writeNewVersion` column loop shared by INSERT
and UPDATE), coercing each text cell to its column's declared type so a wasm UDF receives a
correctly typed argument. The result is written through the ordinary `RowBuilder`, so it rides
the normal page store, WAL, and doublewrite and needs no new storage machinery: the computed
value is stored, not recomputed on read. `Database.open` restores the `computed_by` expression
through `loadCatalog`, so the derived-data definition survives a restart and a fresh insert
after reopen recomputes through it. The optional page-backed `parsed_code` cache is
deliberately skipped (section 9 item 2 permits this); modules recompile at start. Wasm-keyed
secondary indexes reuse the same evaluated-at-write value and are a follow-up.

## 10. SQL and catalog surface

- `CREATE FUNCTION name(param types) RETURNS type LANGUAGE wasm AS <bytes> [WITH (fuel=..,
  memory_pages=..)]`. Registration runs decode, validate, lower, signature check, and stores
  the module. A failure here is a DDL error, not a runtime surprise later.
- `DROP FUNCTION name`. Removes the module and any cache blob. Computed columns or indexes
  that depend on it block the drop (or cascade, to be decided).
- **Versioning.** A function has a version; re-`CREATE` bumps it. The module cache and any
  persisted output are keyed by version so an update never silently reuses stale lowered
  code or stale derived data.
- **System tables.** `kaidb_functions` (name, version, signature, wasm hash, limits, source
  size) and, if we ship it, `kaidb_function_cache` (cache presence, format version).
- **Planner integration.** Three call sites, added in order of value:
  1. **Scalar** in projection: `SELECT discount(price) FROM orders`.
  2. **Filter pushdown**: `WHERE my_pred(col)` evaluated during the scan, so the UDF sees
     rows through the same MVCC snapshot the scan uses.
  3. **Aggregate**: a custom accumulate/merge/finalise triple, evaluated as the operator
     streams rows.
- **Computed columns and wasm-keyed indexes** build on the scalar path plus the persistence
  in section 9.

### 10.1 Integration seam (found during M1)

Reconnaissance of kaidb's SQL layer produced one correction and one genuine shortcut:

- **Correction: kaidb has no scalar-expression projections.** `ast.ProjectionExpr` has only
  three variants, `column`, `star`, and `aggregate`. So `SELECT UPPER(col)` is not supported
  today at all: `func_call` appears only in `WHERE`/`HAVING` predicates and aggregate
  arguments, never as a projected output column. Adding `SELECT fn(col)` therefore means first
  adding general scalar-expression projections to kaidb (a new `ProjectionExpr` variant, parser
  support, and handling in every projection drain), a real SQL feature, before any wasm hook.
- **Shortcut: the WHERE/filter predicate path already funnels through the one choke point.**
  A predicate like `WHERE fn(col) = 1` parses `fn(col)` into `ast.FuncCall`, and predicate
  evaluation runs `evalExpr` to `eval3` to `evalScalar` to `evalFunc` in `src/query/iterator.zig`.
  `evalFunc` is the single place built-in scalars are dispatched. So the tractable first UDF
  integration is **filter pushdown** (design section 10, item 2), not projection: hook
  `evalFunc` once, no parser change, no projection-drain surgery.
- **The single evaluation hook is `evalFunc(fc, ctx)` in `src/query/iterator.zig`.** That
  function dispatches built-in scalar functions by name and returns a `Scalar` value
  (`.integer` / `.float` / `.string`). The wasm path is a check at the top of `evalFunc`: if
  `fc.name` matches a registered wasm function, evaluate its args with `evalScalar`, map them
  to the `u64` slots, call `WasmScalarFn.callI64` / `callRaw` (section on M1), and return the
  result as a `Scalar`. Numeric UDFs need nothing more; string UDFs wait on the frame codec.
- **What remains, then, is not parsing but plumbing and lifetime:** a wasm-function registry
  (name to `WasmScalarFn`) reachable from `evalFunc`'s `ctx` (today `anytype`), which means
  threading a registry reference from the executor down into the iterator context. Plus the
  `CREATE FUNCTION ... LANGUAGE wasm AS <bytes>` DDL (a new `ast.Statement` variant, a lexer
  keyword, catalog storage, and WAL persistence) to populate that registry, and `DROP
  FUNCTION`. The scalar evaluation itself is small; the registry threading and the DDL/catalog
  are the real work.

The recommended order is therefore: (1) thread a registry through the executor context and
hook `evalFunc`, proving `SELECT wasmfn(intcol)` end to end with a programmatically registered
function; then (2) add the `CREATE FUNCTION` / `DROP FUNCTION` DDL and catalog persistence so
registration happens through SQL; then (3) filter pushdown and aggregates.

### 10.2 Why the executor hook is genuinely invasive (deeper look)

A closer read of `query_executor.zig` (roughly 5000 lines) shows the projection scalar
evaluation is **not funnelled through a single choke point**. Different execution paths build
output cells with their own `switch (proj.expr)` blocks: the simple-scan projection, the
grouped/aggregate drain (which currently handles only `.aggregate` / `.column` / `.star`), and
the join drains each have separate cases. `evalFunc` in `iterator.zig` is the choke point for
the *scan/predicate* path, but the aggregate and join drains do not route through it. So wiring
UDFs everywhere means either touching each `switch` or, better, a small refactor first:

- **Preparatory refactor (recommended before hooking):** route every projection's scalar
  (non-aggregate) evaluation through one function that ends at `evalScalar`/`evalFunc`, so the
  wasm hook is added in exactly one place and all paths (scan, group, join) inherit it. This
  is the clean way in, and it is a change to kaidb's most intricate file, so it wants its own
  focused pass with the query test harness, not a late-session edit.
- **Registry ownership + threading:** the `Registry` lives on `Database` (one per database);
  `QueryExecutor` already holds `db: *Database`, so `evalFunc` reaches it once the resolver
  (`RowResolver`) carries a `db`/registry pointer. `RowResolver` is constructed at few sites,
  so that part is small; the projection-path unification above is the larger part.

Net: the engine, the call seam, and the catalog are done and tested; the remaining executor
integration is a deliberate refactor-then-hook in the query engine's hottest, most complex
file, best done as its own slice rather than piecemeal.

## 11. In-process mode: kaidb as host, the Kyte app as guest, transport-agnostic driver

Everything above treats a UDF as a small function called during a query. There is a larger
prize sitting on the same machinery: run an **entire Kyte application inside kaidb** as a
wasm guest, so a stored procedure or a data-heavy handler executes in the database process
with no network hop at all. The kaidb wire protocol already is the serialisation between an
application and the engine. The only thing that changes in-process is the transport: instead
of a TCP socket, the driver moves the same frames through linear memory and a host call.

### 11.1 The transport-agnostic driver and the `wasm` DSN flag

The `kyte-kaidb` driver speaks the kaidb binary protocol over a socket today. We make the
transport a strategy, selected by a connection-string parameter:

```
kaidb://user:pass@host:3009/db                 # normal: TCP socket
kaidb://localhost/db?wasm=true                 # in-process: linear-memory host calls
```

When `wasm=true`:

- The driver does **not** open a socket. There is no `host:port` to reach; the driver is
  running as a wasm guest inside the kaidb process.
- To send a request, the driver encodes the same protocol frame it would have written to a
  socket (via the shared `src/proto/wire.zig` codec), places it in its own guest linear
  memory, and calls a host import, for example `kaidb_exec(ptr, len) -> i64` (packed
  response `ptr`/`len`).
- kaidb, as the host, reads the frame out of the guest memory (bounds-checked exactly as in
  section 6.4), executes it against storage on the calling thread and the current
  transaction, encodes the response, writes it back into guest memory (through the guest
  allocator), and returns the pointer and length.
- The driver decodes the response frame with the same codec and returns rows to the
  application, identical result shapes to the socket path.

The important property: **the wire format is unchanged.** Socket mode and in-process mode
share one codec and one protocol. The driver has two back ends behind one interface, and
the DSN flag chooses. Nothing above the transport in the driver, connection handling, query
building, row binding, the ORM, needs to know which transport is in use.

### 11.2 Why this is synchronous, and why that matters

Over a socket, a query is asynchronous: the driver awaits bytes from the network on the
reactor. In-process, the host call is **synchronous**: `kaidb_exec` traps into kaidb, which
services the request inline and returns. There is no waiting, no reactor, no suspension. The
guest calls a function and gets an answer, the same way a normal function call returns.

This is the linchpin that connects to section 12: because the in-process transport is a
plain synchronous host call, the driver's `wasm=true` path contains no `await` and needs no
async runtime. A Kyte program that uses only synchronous data access can therefore be
compiled to wasm and run inside kaidb without solving the async-in-wasm problem at all. The
hardest blocker for Kyte-to-wasm (the reactor and stack switching) simply does not appear on
this path, because the thing it existed to do (wait for I/O) is now an immediate host call.

### 11.3 Trust boundary

An in-process guest is still untrusted code and runs under the full section 5 metering, the
section 6.4 bounds discipline, and the section 7 determinism rules. The difference from a
scalar UDF is only that its host-import set additionally includes `kaidb_exec` (and its
companions), which lets it issue queries. Those queries run under the same authorisation and
transaction the guest was invoked with, so an in-process guest cannot reach data its caller
could not. Nested queries from a guest are serviced synchronously and re-enter the executor
on the same thread, so re-entrancy and transaction visibility must be handled explicitly
(an open question, section 16).

## 12. Compiling Kyte to wasm: the synchronous subset

For the guest side of section 11 to exist, a Kyte program must compile to a wasm module.
This is more feasible than it looks, because the capability was built once and only fenced
off, not removed.

### 12.1 Current state of the Kyte wasm back end

An audit of the compiler (September 2026) finds:

- **The wasm32 code generator is fully intact.** `backend/codegen/llvm_codegen.zig` still
  forces the `wasm32-unknown-unknown` triple under an `is_wasm` flag, sets pointer size to
  4, disables debug info and SIMD for that target, and carries `is_wasm` branches through
  pointer arithmetic, conditionals, and lambda lowering. The `is_wasm` flag is threaded
  through the whole pipeline.
- **The only barrier is a superficial CLI gate.** `builder.zig` and `tester.zig` print
  "WebAssembly is not a supported target" and return early when `--wasm` or `--target wasm`
  is seen (a decision taken 2026-07-28 when native became the sole shipping target). In
  `builder.zig` the internal `is_wasm` is additionally hardcoded to `false`. There is also a
  wasm link path (`linkWasmInProcess`) still referenced in `pipeline.zig`.
- **Host-import helpers already exist** (`__log_i32`, `__log_bool`, `__read_string`) for the
  old minimal host model.

So the code path from Kyte source to a `.wasm` module is present; it is gated, not gone.

### 12.2 Why the synchronous subset should work

The reason wasm was demoted was the runtime, specifically async: the single reactor and its
coroutine stack switching do not map onto wasm's single call stack. That blocker is
irrelevant to a guest that never awaits:

- No `async`/`await` means no coroutines, no reactor, no stack switching, so the one thing
  wasm genuinely cannot express is never emitted.
- ARC is pure pointer arithmetic over a linear-memory heap and a guest allocator, and works
  in wasm unchanged.
- Data access, the usual reason to await, becomes the synchronous `kaidb_exec` host call of
  section 11, so even database work stays synchronous on this path.
- The remaining language surface (values, structs, enums, traits, generics, collections,
  strings, exact decimals, error handling) is ordinary computation that the existing wasm32
  code generator already targets.

The honest boundary: any use of the async runtime, spawning, channels, direct sockets, TLS,
or the reactor cannot compile to this target. The guest programming model for in-process
kaidb is therefore "synchronous Kyte plus the kaidb driver in `wasm=true` mode". That is a
real and useful subset, and it is exactly what a stored procedure or an in-database handler
needs.

### 12.3 The M0 spike (run, passing)

The spike has been run and it passes. The changes were smaller than the retirement notice
suggested: the wasm32 code generator was fully intact, so re-enabling amounted to four edits
in `builder.zig` and one in `pipeline.zig`:

1. Map `--wasm` and `--target wasm` to the `--wasm` target string instead of rejecting them.
2. Derive `is_wasm` from that target and pass it to `llvm_codegen.compile` (it was hardcoded
   `false`).
3. Widen the codegen and link dispatch, which was gated on `--native`, to also run for
   `--wasm`.
4. For the wasm target, stop after emitting the wasm32 object rather than driving the native
   clang link (the in-compiler wasm link path was retired), and do the final link out of
   process with `wasm-ld`.
5. Make `deriveTargetInfo` wasm-aware (wasm32, 4-byte pointers) so the `platform` constants
   match the codegen, and skip the native-only `string_builder` prelude for wasm (it pulls in
   `kyte_str_alloc`, a native runtime symbol).

The compiler rebuilds clean (ReleaseFast, ~42s) and produces valid modules. Results, each
compiled with `kyte build --file x.ky -o out --target wasm`, linked with
`wasm-ld --no-entry --export-all`, validated with `wasm-validate`, and run under both
`wasmtime` and `wasmer`:

| Program | Expected | Result |
| --- | --- | --- |
| `return 40 + 2` | 42 | 42, valid |
| direct call `sq(7)` | 49 | 49, valid |
| value struct `p.x + p.y` | 42 | 42, valid |
| `while` loop sum 0..9 | 45 | 45, valid |
| generic `id<int>(42)` | 42 | 42, valid |

The exported symbols include `__kyte_main` (the entry point) and, notably, the **ARC runtime
compiled to wasm**: `kyte_retain`, `kyte_release`, `kyte_bytes_alloc`, `kyte_bytes_free`, with
a linear-memory heap (`__heap_base`, `heap_ptr`, `free_list`). So reference counting and the
byte heap already work in wasm; the integer, call, struct, control-flow, and generic paths all
lower and run correctly.

The one known gap is exactly the one predicted: **`string` does not compile to wasm yet**,
because the string prelude calls `kyte_str_alloc`, a native-only runtime symbol with no wasm
host import. Since kaidb's marshalling and any KYX view need strings, the immediate next slice
after M0 is a wasm string prelude: either a pure-Kyte string implementation over the
already-working `kyte_bytes_alloc` heap, or a small set of wasm string host imports. Structs,
generics, control flow, direct calls, and ARC are all confirmed working, so that slice is
bounded and well understood.

This confirms the core claim: **synchronous Kyte compiles to and runs as WebAssembly today.**
The `wasm=true` driver transport (section 11), the KYX-from-database apex (section 12.4), and
the whole in-process guest model rest on a validated foundation, with the string prelude as
the single identified follow-up.

### 12.4 The apex: rendering hypermedia in the database with KYX

Once an entire synchronous Kyte program compiles to a wasm guest (section 12) and that guest
can read rows (section 6.3) and issue queries in-process (section 11), one capability falls
out that no other database has: **the query returns rendered HTML, not rows.**

Kyte has KYX, a JSX-like view syntax where a view is a plain function returning an `Html`
string and expressions embed with `{...}`, auto-escaped. A KYX view is ordinary synchronous
Kyte code. It compiles to wasm on exactly the same path as any other function. So a UDF can
be a *view*:

```
// a KYX view, compiled into a wasm guest registered in kaidb
pub fn productRow(p: Product): Html {
    return <tr>
        <td>{p.name}</td>
        <td class="num">{p.price}</td>
    </tr>;
}
```

Registered as a function and invoked over a scan, this makes kaidb emit an HTML fragment per
row (or a single assembled fragment for the whole result), returned through the same response
frame as any other value, just with a text/html payload tag:

```
SELECT render('productRow') FROM products WHERE category = 'tools';
-- returns rendered <tr>...</tr> markup, ready to swap into the page
```

Why this matters, and why it is uniquely a Kyte plus kaidb move:

- **It closes the hypermedia loop at the data layer.** Kyte's whole thesis is that the
  server renders HTML the browser swaps in. Today that render happens in the web tier, which
  first pulls rows out of the database. With KYX-in-kaidb, the render can happen *in the
  database*, so the engine returns the fragment directly. The row-to-HTML step no longer
  needs a round trip or a separate process.
- **It is safe because it is deterministic and sandboxed.** The view is a pure function of
  its row (section 7), metered (section 5), and escaped by KYX's own rule, so it cannot run
  away, cannot reach beyond its row, and cannot open an injection hole. The escaping boundary
  that already makes KYX safe in the web tier is the same one here.
- **It composes with everything above.** A view fragment can be a persisted computed column
  (section 9): render once on write, store the HTML, serve it on read with no compute at all.
  Or it can be produced live during a scan for freshness. The choice is the same
  compute-once-versus-compute-live decision as any other derived value.
- **It does not turn kaidb into a web server.** kaidb still speaks its binary protocol and
  returns a payload; that payload just happens to be markup. The web tier (or an edge cache,
  or htmx fetching directly) decides what to do with it. kaidb stays a database that can, when
  asked, hand back rendered hypermedia instead of raw columns.

This is the differentiator stated at its sharpest. Other databases return rows and leave
presentation to an application. kaidb, with a KYX guest, can return the presentation itself,
deterministically, sandboxed, and optionally persisted. It is the reason the whole
subsystem is worth building, and it is only reachable because Kyte compiles to wasm and KYX
is just Kyte. It is a later milestone (it needs M0, the row-facing ABI of M3, and the KYX
guest support library), recorded here so the earlier milestones are built with it in view.

### 12.5 M0-a: strings compile to and run as self-contained wasm (done)

Strings were the one construct M0 could not lower. The reason was concrete: the string
prelude and the value-optional and byte-copy paths call native runtime symbols
(`kyte_str_alloc`, `kyte_bytes_copy`, `kyte_valopt_box`/`unbox`,
`kyte_bytes_alloc_persistent_nz`, `kyte_i64_to_string`) that had no wasm implementation and
so tripped the "native-only" guard or linked as undefined host imports.

The fix was to give each of these a **self-contained wasm body**, emitted right next to the
existing wasm heap intrinsics (`kyte_bytes_alloc`, `kyte_retain`, `kyte_release`), so they
build on the linear-memory bump heap that already worked:

- `kyte_str_alloc(len)`: allocate `len+1` bytes on the byte heap, NUL-terminate at `[len]`,
  and rewrite the i32 ARC length header at `ptr-4` to the logical length. This mirrors the
  native runtime exactly, so the string layout is identical on both targets.
- `kyte_bytes_copy(dst, src, len)`: a byte-copy loop over linear memory.
- `kyte_valopt_box`/`kyte_valopt_unbox`: value-optional heap boxing (allocate 8 bytes, store;
  null-check then load).
- `kyte_bytes_alloc_persistent_nz`: routed to the byte heap (the wasm bump heap does not zero
  on allocation).
- `kyte_i64_to_string`: a self-contained base-10 formatter (digits written backwards into a
  scratch buffer, then copied into an exact-length string), so numbers render with no host
  call. This is what KYX interpolation needs.
- The old test and panic host imports (`kyte_test_fail`, `kyte_panic`, and friends) became
  trivial no-op bodies, so a non-test module needs no host to instantiate.

Results, each compiled with `--target wasm`, linked with `wasm-ld --no-entry --export-all`,
validated, and run under `wasmtime`:

| Program | Expected | Result |
| --- | --- | --- |
| `"n=" + "5"` length | 3 | 3 |
| `"hello, " + "world"` length | 12 | 12 |
| `` `count=${42}` `` length | 8 | 8 |
| `` `v=${123}` `` length | 5 | 5 |
| `` `x=${-7}` `` length | 4 | 4 |

The linked module has **zero function imports**: it is fully self-contained. The native
string path is unchanged (the native runtime symbols are still declared as externs for
native and link against libkytecore; only the wasm target uses the new bodies), verified by
running the same interpolation program natively.

The one deliberately deferred piece is the rest of the formatter family (`f64`, `bool`,
`decimal` to-string), still declared for the host; they get bodies when a guest needs them.
With strings working, the marshalling codec (which is text-and-bytes heavy) and KYX views
(concatenation and interpolation of escaped fragments) both have the language support they
need.

## 13. Threat model and hardening

The engine will run modules we did not write. The hardening layer treats every module as
hostile.

- **Resource exhaustion:** bounded by fuel, deadline, memory cap, output cap, and stack
  caps (section 5). An adversarial `loop`, a memory bomb, a deep recursion, and a giant
  `br_table` must each trap within limits, never crash or hang the server.
- **Memory safety at the boundary:** every host dereference of a guest pointer is
  bounds-checked (section 6.4). This is the highest-risk surface and the primary fuzz
  target.
- **Validation before execution:** invariant that no unvalidated or partially validated
  module is ever lowered or run. Malformed modules are rejected at registration.
- **Isolation:** a UDF sees only its arguments and (for row-facing UDFs) a read-only view of
  the current row against the snapshot. It cannot read other rows, other tables, server
  memory, or the filesystem. It cannot write storage except through the transaction.
- **Determinism as a security property:** because output is deterministic and inputs are
  constrained, a UDF cannot exfiltrate host state through nondeterministic side channels
  (there is no clock or RNG to leak into).

Test and hardening plan:

- _Remaining:_ fork zware's `test/testrunner` and run the official WebAssembly test suite
  (minus the rejected proposals) in kaidb CI. This needs a WAST parser/runner and is a
  separate, larger task.
- _Done:_ a fuzzer wired into `gate.sh` (`src/wasm/fuzz_test.zig`) over decode, validate, and
  execute, with an adversarial corpus (infinite loops, a memory bomb, deep recursion). Every
  input terminates in a trap or a valid result, never in a crash, hang, or leak: the fuel
  budget and memory-page cap bound time and space, and the leak-checking test allocator bounds
  memory. A fixed PRNG seed makes any failure reproducible. Lying-pointer inputs are already
  covered by the bounds-safety tests in `udf_test.zig` (the host bounds-checks every guest
  pointer before use, section 6.4). Huge branch tables are a corpus addition for later.
- _Done:_ the subsystem runs under the safety-checked optimized build (`zig test -OReleaseSafe`)
  in `gate.sh`, Zig's ASAN-equivalent for pure-Zig code (bounds, overflow, and use-after-free
  poisoning on the release codegen path).
- _Done:_ a **determinism differential test** runs a float module (including a NaN path) across
  independent instances and independent re-decodes and asserts bit-identical output. A
  cross-process primary/replica harness remains.
- _Done:_ a **replay test** re-decodes a module from its stored source bytes (simulating
  recovery) and asserts the replayed derived value matches what was recorded before. A
  full crash/recover of persisted derived data is bounded by kaidb's separate row-durability
  behaviour (noted under M4).

## 14. Performance tuning

- **Keep the tail-threaded dispatch.** It is the main reason to start from zware rather than
  write a naive switch interpreter. The fuel decrement must stay a single cheap add in the
  hot path; we measure its overhead and keep it well under the cost of the average opcode.
- **Instance pooling** so there is no per-row allocation. Reset is memory zeroing or
  discard-and-reallocate, whichever benchmarks better for the typical small UDF memory.
- **Zero-copy marshalling where safe:** for row-facing pull-model UDFs, avoid serialising
  columns the UDF never reads.
- **`fast_call`:** keep and exercise zware's pre-resolved direct-call path.
- **A later JIT tier is explicitly out of scope for now.** If a specific UDF proves too slow
  under interpretation, the escape hatch is a JIT backend (LLVM or Cranelift), but that is a
  future decision, justified by measurement, not a v1 goal. The interpreter with pooling and
  amortised compilation is the target for the first releases.
- **Benchmark plan:** a scalar UDF over 1M and 10M rows versus the equivalent built-in
  expression, a filter UDF pushdown versus a native predicate, and a computed-column build.
  We track per-row overhead and set a budget before optimising.

## 15. Roadmap

- **M0: prove synchronous Kyte compiles to and runs as wasm. DONE (section 12.3).** The wasm
  target was re-enabled with five small edits; integers, calls, value structs, control flow,
  generics, and the ARC runtime all compile to valid wasm and run correctly under wasmtime and
  wasmer.
- **M0-a: wasm string prelude. DONE (section 12.5).** Strings now compile to and run as
  self-contained wasm. The string runtime primitives that were native-only were given
  self-contained wasm bodies emitted next to the existing heap intrinsics: `kyte_str_alloc`
  (over the working byte heap), `kyte_bytes_copy` (a memcpy loop), `kyte_valopt_box`/`unbox`,
  `kyte_bytes_alloc_persistent_nz`, and a self-contained base-10 `kyte_i64_to_string`; the
  test/panic host imports became trivial no-op bodies. String literals, concatenation, and
  number interpolation (positive and negative) run correctly, and the linked module needs
  **zero host imports**. This unblocks marshalling and any KYX view (M7). Remaining formatters
  (`f64`/`bool`/`decimal` to-string) are still declared for the host and get bodies when needed.
- **M1: scalar pure UDF, end to end.** _Foundation landed:_ zware is forked into
  `src/wasm/engine/` (self-contained, std-only, MIT NOTICE retained), compiles under kaidb's
  Zig 0.16, and runs, a smoke test (`src/wasm/smoke_test.zig`) decodes, instantiates, and
  invokes a trivial module returning 42. Upstream's inline tests were stripped (we add our
  own) and the WASI host-function surface (`wasi/`) was removed so UDFs get no filesystem,
  clock, or randomness (section 8).
  _Fuel metering: done._ A `fuel: u64` counter on the VM, charged one unit per instruction at
  the single dispatch chokepoint (and the invoke entry), trapping with `error.FuelExhausted`
  when spent; a `fuel` field on `VirtualMachineOptions` threads a per-call budget through
  `Instance.invoke`. Verified in `smoke_test.zig`: an infinite `loop br 0` traps under a
  bounded budget instead of hanging, and normal functions still run under a budget.
  _Hard memory cap: done._ `Instance.limitMemoryPages(max_pages)` clamps every memory's growth
  ceiling independent of the module's declared maximum, so a module with no declared maximum
  cannot grow to 4 GiB. Verified: growth past the cap returns the wasm failure value, growth
  within it succeeds.
  _NaN canonicalisation: done._ Implemented as canonical-NaN mode at the single float
  operand-push point (`pushOperand`/`pushOperandNoCheck`): any NaN put on the operand stack is
  forced to the canonical quiet NaN (`0x7FC00000` / `0x7FF8000000000000`), so float results are
  bit-identical across hosts and replicas. This is the determinism mode mature runtimes offer,
  and a single auditable enforcement point rather than ~28 scattered edits. `min`/`max` already
  produced the canonical NaN. Verified: `0.0/0.0` and `sqrt(-1)` both yield the canonical NaN.
  _Scalar UDF wrapper: done._ `src/wasm/udf.zig` (`WasmScalarFn`) is the seam the SQL executor
  will call. It follows the module/instance split: a module is decoded once and held
  immutably, and every call spins up a fresh Store + Instance (clean linear memory and stacks),
  rejects any module that needs host imports, applies the memory-page cap and fuel budget, and
  runs the exported function. Numeric scalar UDFs need no linear-memory marshalling, zware
  passes `u64` args and results directly, so `callI64` / `callRaw` cover int and float scalar
  UDFs now. Verified: `add(40,2)=42`, the compiled module reused across calls, and a runaway
  `spin` traps under the fuel budget.
  _Function registry: done._ `src/wasm/registry.zig` (`Registry`) is the catalog map that
  `CREATE FUNCTION` will populate and `evalFunc` will look up: `register`/`get`/`drop`,
  decode-and-validate at registration, and a per-function version that bumps on re-registration
  (so a re-`CREATE` never reuses stale lowered code or version-keyed derived data, section 9).
  In-memory now; WAL-backed persistence of the source bytes is a later slice. Verified:
  register, call through the registry, version bump on re-register, and drop.
  _Executor hook: done (evaluation path)._ `evalFunc` in `src/query/iterator.zig` now checks a
  module-level `active_wasm_registry` first: if the function name is registered, it evaluates
  the integer arguments, runs the module sandboxed and metered via `WasmScalarFn`, and returns
  the result, shadowing built-ins of the same name (the module-level pointer follows kaidb's
  existing scalar-eval state pattern and avoids threading through every operator). The wasm
  engine now compiles as part of the main kaidb build (`zig build` green, `kaidb`/`kai`
  binaries produced), and an end-to-end test in the kaidb suite proves a registered UDF
  `DBL(21)` evaluates to 42 through the real `evalScalarJson` to `evalScalar` to `evalFunc`
  path, with an unregistered name falling through to the built-ins. This is the tractable
  filter-pushdown path (`WHERE fn(col) = ...`); scalar projections (`SELECT fn(col)`) still need
  the separate scalar-projection SQL feature (section 10.1).
  _Reachable from SQL text: done._ `Database` owns the `Registry` (`wasm_functions`), the
  executor points the eval hook at it, and the `CREATE FUNCTION name [LANGUAGE wasm] AS '<hex>'`
  / `DROP FUNCTION name` DDL registers and drops UDFs (new `ast.Statement` variants, parser
  dispatch on the non-reserved `FUNCTION` identifier, executor handlers that hex-decode the
  inline module and register under the upper-cased name). An end-to-end test in the kaidb suite
  proves the whole path from SQL text: `CREATE FUNCTION DBL LANGUAGE wasm AS '<hex>'`, then
  `SELECT x FROM t WHERE DBL(x) = 42` returns exactly the one row (`x = 21`) via the wasm
  predicate evaluated per row through the real executor, then `DROP FUNCTION DBL` makes the same
  query return no rows. **Filter pushdown with a wasm UDF now works from SQL.**
  _Concurrency: already safe._ `CREATE FUNCTION` / `DROP FUNCTION` are DDL, which
  `executeStatement` runs under the database `rw_lock` held exclusively; a `SELECT` that reads
  the registry holds the same lock shared. The two are therefore mutually exclusive, so
  mutating the registry never races a query that reads it. No extra latch is required.
  _Deadline backstop: covered by fuel for now._ Fuel already bounds execution deterministically
  and per instruction; a wall-clock backstop would need clock access the pure interpreter does
  not have (it would take a watchdog-set interrupt flag), so it is deferred as the optional
  control the design already calls it. The output-size cap is not yet relevant: scalar UDFs
  return a single numeric value, so there is nothing unbounded to cap until the string/bytes
  frame codec lands.

  **M1 is functionally complete: a wasm scalar UDF is authored (Kyte -> wasm), registered from
  SQL, and used in a metered, sandboxed, deterministic filter predicate, with the engine built
  into the kaidb binary.** What remains are later milestones, not M1 leftovers:
  - **Persistence: done (file-backed).** A registered module is written to
    `<base_dir>/udf/<NAME>.wasm` (`Database.persistWasmFunction`), removed by `DROP FUNCTION`
    (`removeWasmFunction`), and reloaded into the registry on open after `loadCatalog`
    (`loadWasmFunctions`, non-fatal so a bad file cannot block startup). Verified: a
    `CREATE FUNCTION` then a full close and reopen leaves the UDF registered and usable in a
    `WHERE` predicate with no re-`CREATE`. This survives a restart but is deliberately simple:
    it is not WAL-consistent with table data and is not replicated. Moving the modules into the
    catalog (so they ride the WAL and replicate) is the follow-up; it is gated on `rw_lock`
    non-reentrancy (`CREATE FUNCTION` already holds the exclusive lock, so it must use the
    lower-level table API rather than `self.execute`).
  - **M2 marshalling: done (both directions, mixed args, string results).**
    `WasmScalarFn.call(args, out_buf)` is the general scalar-UDF path: each argument maps to
    wasm parameters (an int is one i64; a string is written into the guest via its exported
    `kaidb_alloc` and passed as a `(ptr, len)` i32 pair, all bounds-checked, host never trusts
    a guest pointer, section 6.4). The result is an i64 numeric value, or, for a
    `CREATE FUNCTION ... RETURNS TEXT` function, a packed `(ptr, len)` string whose bytes are
    copied out of guest memory into a threadlocal scratch buffer (the buffer size is the
    output-size cap). `evalFunc` builds the arg list from the SQL scalars, calls `call`, and
    returns `.integer` or `.string`. The DDL parses `RETURNS TEXT|INT`, and the result type is
    persisted (a `.twasm` extension) so it survives restart. Verified end to end from SQL:
    `SELECT name FROM w WHERE SUMB(name) = 198` (string in, int out) and
    `CREATE FUNCTION UP RETURNS TEXT ... WHERE UP(name) = 'ABC'` (string in, string out) both
    select exactly the matching rows. Remaining M2 (deferred): a fully typed wire frame on
    `src/proto/wire.zig` for the richer `DbValue` set (decimal, timestamps, explicit NULL) and
    for very large results beyond the scratch cap; the common int/string cases work now.
  - **Scalar-expression projections (`SELECT fn(col)`).** kaidb has no scalar projections
    today (section 10.1); this is a separate SQL feature.
  - **The KYX-from-database apex (M7, section 12.4).**
  push-model frame codec on `src/proto/wire.zig` for variable-length (string/bytes) arguments,
  and the `CREATE FUNCTION ... LANGUAGE wasm` / `SELECT fn(col)` wiring into kaidb's lexer,
  parser, catalog, and executor (the larger SQL-surface slice). Demo target: a Kyte function
  compiled to `.wasm`, registered, called in SQL, metered and sandboxed.
- **M2: the marshalling drivers and ABI hardening.** Finalise `abi.zig` (exports the guest
  must provide, imports we offer), the guest support library for Kyte, full `DbValue` type
  coverage in `marshal.zig`, and the bounds-safety invariant with its fuzzer. Also add the
  `wasm=true` transport back end to the `kyte-kaidb` driver (section 11): the same wire
  codec over `kaidb_exec` host calls instead of a socket, selected by the DSN flag.
- **M3: row-facing UDFs.** _Done._ Pull-model column imports (`kaidb.col_i64` / `col_f64` /
  `col_bytes` / `col_is_null` / `col_count`) in `src/wasm/host.zig`, an import allowlist that
  classifies a module as row-facing at registration and rejects everything else, and filter
  pushdown into the scan: a zero-argument row UDF reads the bound scan row read-only through the
  host functions and its result drives the WHERE predicate. Bounds-checked `col_bytes` writes,
  a trap on a bad column index, and end-to-end SQL plus engine-level tests. Remaining for later:
  forcing full-row materialisation under a row-facing predicate (columns are currently read from
  the projected row), and `emit`/aggregate surface (folded into M5).
- **M4: persistence.** _Done for computed columns._ `CREATE TABLE t (..., c TYPE AS fn(args))`
  parses and stores the generating expression as durable catalog metadata (`computed_by` on
  `ColumnMetadata`, flag bit `16`, backward compatible); the value is computed at insert/update
  through the scalar-UDF path (`computeGeneratedCell`, hooked into the shared `writeNewVersion`
  column loop) and written as an ordinary column, so it rides the page store, WAL, and
  doublewrite. `loadCatalog` restores the expression on reopen. End-to-end SQL test. Remaining:
  wasm-keyed secondary indexes, and the optional page-backed `parsed_code` cache with checksum
  and format-version fallback (deliberately skipped for now; modules recompile at start).
- **M5: aggregates.** _Done (accumulate/finalise)._ `CREATE AGGREGATE name LANGUAGE wasm AS
  '<hex>'` registers a module exporting `kaidb_agg_accumulate(i64)` and `kaidb_agg_finalize()
  -> i64` (plus optional `kaidb_agg_init`), validated at registration. `SELECT name(col) FROM
  t [GROUP BY g]` folds each row through a *persistent* per-group guest instance (its linear
  memory and globals carry the running state across rows, unlike the fresh-per-call scalar
  instance), finalising once per group. Wired into kaidb's existing GROUP BY choke points
  (`foldOneAgg`/`formatAggregate`), gated out of the index-only fast path, with a parallel
  `AggRegistry` and file-backed (`.wagg`) persistence reloaded on open. Engine-level and
  end-to-end SQL tests (whole-table, per-group, case-insensitive, DROP). Remaining: `merge`
  (only needed for parallel/partitioned aggregation), a wasm aggregate in HAVING, and
  non-integer accumulator values.
- **M6: hardening to production.** _Largely done._ The engine's own suites (metering,
  determinism, marshalling, and an adversarial-input test) are gated in `gate.sh`, and M6 adds
  `src/wasm/fuzz_test.zig`: a fuzzer over decode/validate/execute (thousands of random-byte
  inputs, thousands of single-bit mutations of a valid module, and an adversarial corpus of
  unbounded recursion and a `memory.grow` bomb), plus determinism-differential and replay
  tests. The fuzzer's seed is fixed so a failure reproduces; the leak-checking test allocator,
  the per-call fuel budget, and the memory-page cap enforce "never leak / hang / blow memory"
  by construction. The determinism test asserts float results (including the canonical NaN) are
  bit-identical across independent instances and re-decodes; the replay test re-decodes a module
  from its stored bytes and asserts the derived value reproduces. The whole engine suite also
  runs under `zig test -OReleaseSafe` in `gate.sh` (Zig's ASAN-equivalent for pure-Zig code:
  runtime safety, including use-after-free poisoning, on the optimized codegen path). _Remaining:_
  the official WebAssembly conformance suite, which needs vendoring a WAST runner (a separate,
  larger task), and a cross-process primary/replica differential harness.
- **M7: hypermedia from the database (KYX).** _Done (kaidb side)._ The apex use-case of section
  12.4. On the kaidb side a KYX view is a row-facing `RETURNS TEXT` UDF: it reads the current
  row through the M3 column ABI and returns an HTML `<tr>` fragment. Both delivery paths work:
  - **Live render:** `SELECT viewname() FROM t` invokes the view against each scanned row and
    emits its fragment as the cell (a new `render` projection variant, wired into the
    non-aggregate projection drain; projection pushdown is forced to the whole row so the view
    sees every column). The database returns rendered HTML, not columns.
  - **Persisted render (compute-once):** `CREATE TABLE ... (html TEXT AS viewname())` renders
    the fragment at insert time and stores it as ordinary durable column data (M7 composed with
    M4 and M3), so a read serves the markup with no compute at all.
  The view is deterministic, metered, and sandboxed (sections 5 and 7), and KYX's own escaping
  makes the fragment injection-safe. A hand-authored view module gates this end to end; a
  compiler path that emits the view from real KYX source (M0-a already proved Kyte strings
  compile to and run as wasm) and a distinct text/html wire payload tag (the fragment is carried
  as a TEXT column today) are the remaining pieces. This is the differentiator; the earlier
  milestones were built with it in view.

## 16. Open questions and risks

- **Guest allocator contract.** Do we require every module to export `kaidb_alloc`/
  `kaidb_free`, or do we support a simpler "host owns a fixed scratch region" mode for
  modules that only read a small fixed request? The export contract is cleaner and more
  general; the fixed-region mode is simpler for tiny scalar UDFs. Likely support both, with
  the export contract as the default.
- **DROP semantics** when computed columns or indexes depend on a function: block or cascade.
- **Fuel calibration.** Turning an instruction budget into a defensible default limit needs
  measurement so the default neither kills legitimate UDFs nor lets pathological ones run
  too long.
- **Replica format skew.** If the engine format version differs across a primary and a
  replica mid-upgrade, the persisted cache is per-node regenerable (fine), but we must
  confirm that UDF output stays bit-identical across engine versions, or gate UDF execution
  on a matched engine version during rolling upgrades.
- **In-process re-entrancy.** When an in-process guest (section 11) issues a query through
  `kaidb_exec`, that query re-enters the executor on the same thread and inside the guest's
  transaction. We must define the visibility (does the guest see its own uncommitted writes),
  the recursion depth limit, and whether a guest query may itself invoke another wasm guest,
  before this path ships.
- **Self-hosted backend.** The tail-call requirement pins us to the LLVM backend. If kaidb
  ever wants the self-hosted backend for faster debug builds, the WASM path needs a fallback
  dispatch (a plain switch) behind a build flag. Not needed now, noted.

## 18. Production-hardening candidates

M1 through M7, plus stored procedures (`CALL`) and DML triggers, are functionally complete and
gated. What follows is the honest list of what must harden before this subsystem is a
production feature, ranked by how load-bearing it is. Nothing here is a functional gap in what
was built; these are the durability, concurrency, security, and resource-safety properties a
shipped feature needs.

**P0, correctness and durability blockers:**

1. **Catalog/WAL persistence, not loose files.** Functions, aggregates, procedures (`.wasm` /
   `.twasm` / `.wagg` / `.wproc`) and triggers (`.wtrig`) persist as loose files under
   `<base_dir>/udf/`, outside the catalog, the WAL, and doublewrite. So registration is not
   crash-consistent (a crash between the in-memory register and the file write diverges them),
   not transactional (a rolled-back statement still leaves the file), and not replicated
   (followers never receive a UDF or trigger over the WAL ship path). The design (section 9,
   item 1) already calls for module source to live in the catalog, WAL-backed and
   doublewrite-protected. This is the single biggest item: it is a durability and HA
   correctness gap, and the `.wtrig` binary format additionally has no version tag or checksum.
2. **Concurrency: guard the registries and the global registry pointer.** DDL
   (`CREATE`/`DROP FUNCTION`/`AGGREGATE`/`PROCEDURE`/`TRIGGER`, `registerTrigger`) mutates the
   in-memory registries and the trigger list with no lock, while queries read them
   concurrently. A `StringHashMap` resize during a concurrent read invalidates the borrowed
   `*WasmScalarFn`. And `query_iter.active_wasm_registry` is a process-global set at the top of
   every `execute`; two executor threads stomp it. Both need the catalog latch (DDL takes it
   exclusive, reads shared) and the registry pointer needs to move onto the executor/request,
   not a global. This is a soundness and crash risk under the threaded server.
3. **Trigger recursion and fan-out limits.** A trigger firing a function that (once in-process
   DML lands, section 11) inserts into another table can fire another trigger, unbounded. Even
   today, nothing caps trigger depth or the number of triggers per event. A per-statement
   trigger-depth guard and a documented ceiling are required before triggers are safe on a
   busy table.

**P1, resource safety and security:**

4. **Authorization on UDF/trigger DDL.** Any user who can run DDL can register a function and a
   `BEFORE INSERT` trigger, which is a persistent, always-on code-execution hook on every write
   to a table. With no privilege check this is a privilege-escalation vector. kaidb already has
   `GRANT`/`REVOKE`; `CREATE FUNCTION`/`PROCEDURE`/`TRIGGER` must require a dedicated privilege.
5. **Aggregate instance cap and per-statement fuel.** A `GROUP BY` over a wasm aggregate
   heap-allocates one `Aggregator` per group, uncapped: a query with millions of groups
   allocates millions of guest instances. And fuel is per-call only, so a UDF over 10M rows
   runs 10M independently-budgeted calls with no total ceiling. Both need a per-statement bound.
6. **Result-buffer truncation.** String and HTML results are copied into a fixed 512-byte
   thread-local scratch buffer and silently truncated past that (`fn_scratch_a`/`b` in
   `iterator.zig`). A KYX fragment longer than 512 bytes is silently cut. This needs a growable
   result buffer or an explicit over-cap error, not silent truncation.
7. **Module-size and registry-count limits.** Nothing bounds a registered module's size (a huge
   module can exhaust memory at decode) or the number of registered UDFs (unbounded registry
   growth). Both need configured caps enforced at `CREATE`.

**P1, correctness completeness:**

8. **Fire UPDATE and DELETE triggers.** Triggers are parsed and persisted for all three events,
   but only INSERT is fired today. A `BEFORE UPDATE`/`DELETE` validation trigger silently does
   nothing, which is a correctness and security surprise. The UPDATE and DELETE executor paths
   need the same `fireRowTriggers` hook (with OLD-row context for DELETE and OLD/NEW for
   UPDATE).
9. **Positional row ABI versus schema evolution.** The row ABI is positional (`col_i64(0)`), so
   a UDF is bound to a specific column layout. `ALTER TABLE ADD`/`DROP COLUMN` silently shifts
   the indices, so a view, filter, or trigger reads the wrong column with no error. Bind columns
   by name, or stamp a schema version the UDF is validated against.
10. **`CALL` and procedures are inert.** A procedure cannot perform DML and `CALL` accepts only
    literal arguments, because the in-process query host imports (section 11) are not built. An
    `AFTER` trigger has the same limit: it can read the row and veto (BEFORE) but cannot act.
    Until section 11 lands, procedures and AFTER triggers are validation/compute only, which
    should be stated in user docs.

**P2, observability and validation:**

11. **System catalog views.** There is no `sys.*` view listing registered functions,
    aggregates, procedures, or triggers (the design's `kaidb_functions` table). Operators cannot
    introspect what code is registered and firing.
12. **Surface persistence failures.** A failed `.w*` write is logged as a warning, so a
    `CREATE` that "succeeded" but did not persist silently fails to survive a restart. Once
    persistence moves into the WAL (item 1) this closes; until then it should at least warn the
    client.
13. **Extend fuzzing and the WASM conformance suite.** The M6 fuzzer covers the engine
    (decode/validate/execute); it does not yet cover the trigger and procedure SQL paths, and
    the official WebAssembly conformance suite (a vendored WAST runner) is still outstanding.

## 17. Licence and attribution

The forked engine files retain Malcolm Still's copyright header and the MIT licence text.
`src/wasm/engine/NOTICE` records that these files derive from `malcolmstill/zware` at a
named commit, the changes we made (fuel hook, NaN canonicalisation, validator restrictions,
trimming), and the MIT terms. Our own files under `src/wasm/` carry the kaidb licence.
