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
| Concurrency (registry mutation vs concurrent reads) | Not locked; a process-global registry pointer is shared across threads |
| Authorization on UDF / trigger DDL | None |
| Trigger events | INSERT fires; UPDATE / DELETE are parsed and persisted but not fired |

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

2. **Concurrency: guard the registries and the global registry pointer.** DDL
   (`CREATE` / `DROP FUNCTION` / `AGGREGATE` / `PROCEDURE` / `TRIGGER`, and trigger registration)
   mutates the in-memory registries and the trigger list with no lock, while queries read them
   concurrently. A `StringHashMap` resize during a concurrent read invalidates the borrowed
   function pointer. And `active_wasm_registry` is a process global set at the top of every
   `execute`; two executor threads stomp it. Both need the catalog latch (DDL exclusive, reads
   shared) and the registry pointer needs to move onto the executor or request, not a global.

3. **Trigger recursion and fan-out limits.** Nothing caps trigger depth or the number of triggers
   per event. Once in-process DML lands (design section 11), a trigger that inserts into another
   table can fire another trigger, unbounded. A per-statement trigger-depth guard and a documented
   ceiling are required before triggers are safe on a busy table.

### P1, resource safety and security

4. **Authorization on UDF / trigger DDL.** Any user who can run DDL can register a function and a
   `BEFORE INSERT` trigger, which is a persistent, always-on code-execution hook on every write to
   a table. With no privilege check this is a privilege-escalation vector. kaidb already has
   `GRANT` / `REVOKE`; UDF and trigger DDL must require a dedicated privilege.

5. **Aggregate instance cap and per-statement fuel.** A `GROUP BY` over a wasm aggregate
   heap-allocates one aggregator instance per group, uncapped: a query with millions of groups
   allocates millions of guest instances. And fuel is per-call only, so a UDF over 10M rows runs
   10M independently-budgeted calls with no total ceiling. Both need a per-statement bound.

6. **Result-buffer truncation.** String and HTML results are copied into a fixed 512-byte
   thread-local scratch buffer and silently truncated past that. A KYX fragment longer than 512
   bytes is silently cut. This needs a growable result buffer or an explicit over-cap error, not
   silent truncation.

7. **Module-size and registry-count limits.** Nothing bounds a registered module's size (a huge
   module can exhaust memory at decode) or the number of registered UDFs. Both need configured
   caps enforced at `CREATE`.

### P1, correctness completeness

8. **Fire UPDATE and DELETE triggers.** Triggers are parsed and persisted for all three events,
   but only INSERT is fired. A `BEFORE UPDATE` / `DELETE` validation trigger silently does
   nothing, which is a correctness and security surprise. The UPDATE and DELETE executor paths
   need the same firing hook, with OLD-row context for DELETE and OLD / NEW for UPDATE.

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
