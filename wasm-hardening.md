# kaidb WebAssembly UDF subsystem: production-hardening assessment

kaidb can run sandboxed, deterministic WebAssembly modules registered over SQL: scalar
functions, custom aggregates, stored procedures, DML triggers, and KYX view renders. The feature
set (design and roadmap in `embed-wasm.md`, milestones M1 to M7 plus procedures and triggers) is
functionally complete and gated by the engine's own test suites and a fuzzer.

This document is the honest list of what must harden before the subsystem is a production
feature. Nothing here is a functional gap in what was built; these are the durability,
concurrency, security, and resource-safety properties a shipped feature needs. It mirrors section
18 of `embed-wasm.md` and is kept here as a standalone operator-facing reference.

## Status snapshot

| Property | State today |
| --- | --- |
| Sandbox (fuel, memory cap, no host imports for scalars) | Done, gated |
| Determinism (canonical NaN, no clock/RNG/WASI) | Done, gated (differential + replay tests) |
| Fuzzing of decode / validate / execute | Done, gated; SQL trigger/proc paths and the official WASM conformance suite are not yet fuzzed |
| Per-call resource metering | Done; no per-statement or per-query ceiling |
| Persistence of modules and triggers | File-backed (`.wasm` / `.twasm` / `.wagg` / `.wproc` / `.wtrig`), reloaded on open. NOT catalog / WAL / doublewrite, NOT replicated |
| Concurrency (registry mutation vs concurrent reads) | Mutation-vs-read is serialised by the database `rw_lock` (DDL exclusive, reads shared); the registry pointer is now `threadlocal` (P0-2 done). Moving it fully off ambient state remains |
| Authorization on UDF / trigger DDL | None |
| Trigger events | INSERT, UPDATE, and DELETE all fire (BEFORE can veto). Recursion/fan-out limits still pending |

## Ranked hardening candidates

### P0, correctness and durability blockers

1. **Catalog / WAL persistence, not loose files.** Functions, aggregates, procedures, and
   triggers persist as loose files under `<base_dir>/udf/`, outside the catalog, the WAL, and
   doublewrite. So registration is not crash-consistent (a crash between the in-memory register
   and the file write diverges them), not transactional (a rolled-back statement still leaves the
   file), and not replicated (a follower never receives a UDF or trigger over the WAL ship path).
   The design (`embed-wasm.md` section 9, item 1) already calls for module source to live in the
   catalog, WAL-backed and doublewrite-protected. This is the single biggest item. The `.wtrig`
   binary format additionally has no version tag or checksum.

2. **Concurrency: guard the registries and the global registry pointer.** _Done._ Verified that
   the mutation-versus-read race cannot occur: DDL (`CREATE` / `DROP FUNCTION` / `AGGREGATE` /
   `PROCEDURE` / `TRIGGER`, and trigger registration) runs as a write and holds the database
   `rw_lock` exclusively, while a query reading a registry holds it shared, and the two are
   mutually exclusive, so a `StringHashMap` resize can never invalidate a borrowed function
   pointer mid-read. The remaining bug was `active_wasm_registry`, a process global that two
   executor threads could stomp; it is now `threadlocal`, so each thread sets and reads its own
   pointer. A fuller refactor that carries the registry on the executor or request (rather than
   any ambient thread-local) is still worthwhile but no longer a correctness blocker.

3. **Trigger recursion and fan-out limits.** _Done (guard in place)._ A thread-local trigger-depth
   counter bounds nesting at `MAX_TRIGGER_DEPTH` (8) and returns `error.TriggerRecursionTooDeep`
   past it. A trigger cannot yet cause another DML statement (in-process query imports are
   unbuilt), so the depth is 1 today; this is the guard that keeps a future trigger web from
   running away or blowing the stack when in-process DML lands.

### P1, resource safety and security

4. **Authorization on UDF / trigger DDL.** Any user who can run DDL can register a function and a
   `BEFORE INSERT` trigger, which is a persistent, always-on code-execution hook on every write to
   a table. With no privilege check this is a privilege-escalation vector. kaidb already has
   `GRANT` / `REVOKE`; UDF and trigger DDL must require a dedicated privilege.

5. **Aggregate instance cap and per-statement fuel.** _Instance cap done._ A per-statement counter
   bounds wasm aggregator instances at `MAX_WASM_AGG_INSTANCES` (100k) and returns
   `error.TooManyAggregateInstances` past it, so a `GROUP BY` with a pathological number of groups
   cannot allocate unbounded guest instances. A per-statement fuel ceiling (fuel is still per-call,
   so a UDF over 10M rows runs 10M independently-budgeted calls) remains.

6. **Result-buffer truncation.** _Done._ The thread-local string-result scratch buffers were
   raised to 64 KiB so realistic text and HTML fragments are not truncated, and the copy in
   `WasmScalarFn.call` / `callRow` now returns `error.OutputTooLarge` for a result that does not
   fit, so an over-cap result fails loudly instead of returning silently truncated bytes. A fully
   growable, unbounded result buffer is a later refinement.

7. **Module-size and registry-count limits.** _Done._ A module larger than `MAX_MODULE_BYTES`
   (4 MiB) is rejected in `WasmScalarFn.init` / `WasmAggFn.init` before decode, and a registry that
   already holds `MAX_REGISTERED` (1024) entries rejects a new name (replacing an existing name is
   always allowed). Making the caps configurable from server config is a later refinement.

### P1, correctness completeness

8. **Fire UPDATE and DELETE triggers.** _Done._ The UPDATE path fires BEFORE/AFTER triggers
   against the NEW row (a BEFORE veto aborts the update); the DELETE path fires them against the
   OLD row (a BEFORE veto aborts the delete). AFTER on the scan-based delete fires at match time
   because the two-phase delete frees the row before the delete loop and an AFTER DELETE trigger
   is read-only today; this moves post-delete when in-process DML lands. Full OLD/NEW pairs for an
   UPDATE trigger (it currently sees only the NEW row) are a later refinement.

9. **Positional row ABI versus schema evolution.** The row ABI is positional (`col_i64(0)`), so a
   UDF is bound to a specific column layout. `ALTER TABLE ADD` / `DROP COLUMN` silently shifts the
   indices, so a view, filter, or trigger reads the wrong column with no error. Bind columns by
   name, or stamp a schema version the UDF is validated against.

10. **`CALL` and procedures are inert.** A procedure cannot perform DML and `CALL` accepts only
    literal arguments, because the in-process query host imports (design section 11) are not
    built. An `AFTER` trigger has the same limit: it can read the row and veto (BEFORE) but cannot
    act. Until section 11 lands, procedures and AFTER triggers are validation and compute only,
    which should be stated in user docs.

### P2, observability and validation

11. **System catalog views.** There is no `sys.*` view listing registered functions, aggregates,
    procedures, or triggers. Operators cannot introspect what code is registered and firing.

12. **Surface persistence failures.** A failed persistence write is logged as a warning, so a
    `CREATE` that "succeeded" but did not persist silently fails to survive a restart. Once
    persistence moves into the WAL (item 1) this closes; until then it should at least warn the
    client.

13. **Extend fuzzing and the WASM conformance suite.** The fuzzer covers the engine
    (decode / validate / execute); it does not yet cover the trigger and procedure SQL paths, and
    the official WebAssembly conformance suite (a vendored WAST runner) is still outstanding.

## Suggested order of work

The two changes that most move the subsystem from "works in a single-connection demo" to "safe
under a real server" are **P0-1 (move persistence into the catalog and WAL)** and **P0-2 (registry
concurrency)**. Everything else is valuable but layers on top of those two.
