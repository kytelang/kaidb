//! Schema subsystem facade: the single import surface for NovaDB's logical
//! data model.
//!
//! Everything that describes *what* the database stores, as opposed to the
//! byte-level storage that holds it, lives under `schema/` and is re-exported
//! from here. The split is deliberate: the storage engine (`storage/btree.zig`,
//! `pager.zig`, `page.zig`) deals only in opaque variable-length cells and keys,
//! while this subsystem gives those bytes meaning: columns and their types, how
//! a row is packed into and read back out of a cell, the tables and indexes that
//! group rows, and the system catalog that persists all of that metadata. The
//! SQL executor and the wire protocol both program against this module rather
//! than reaching into the individual `schema/*.zig` files, so keeping every
//! public name in one place keeps those call sites stable when the internal file
//! layout moves.
//!
//! The pieces layer bottom-up:
//!
//!   1. [`types`], the vocabulary: [`ColumnType`], [`Column`], key/index kinds,
//!      foreign-key and sequence descriptors, and the persisted metadata records
//!      ([`TableMetadata`], [`IndexMetadata`], ...) that the catalog serialises.
//!   2. [`row`], the row codec: [`RowBuilder`] packs typed column values into a
//!      cell payload and [`RowReader`] decodes it back, including the MVCC
//!      version stamps ([`DecodedVersion`], [`packVersions`], [`unpackVersions`])
//!      that let a reader see the correct committed version of a row.
//!   3. [`table`], the logical container: a [`Table`] binds a column layout to a
//!      B+Tree, and an [`Index`] binds a secondary key to it; the `*Builder`
//!      types assemble those definitions.
//!   4. [`catalog`], [`SystemCatalog`], the persistent registry of every table,
//!      index and sequence, itself stored in the engine like any other data.
//!   5. [`database`], [`Database`], the top-level handle that ties a catalog to
//!      an open storage file and hands out tables to the executor.
//!
//! This file adds no logic of its own; it is purely the aggregation point, so
//! importers write `schema.Table` instead of chasing the definition across five
//! files.

const std = @import("std");

/// The type vocabulary submodule: column types, key/index kinds, foreign-key
/// and sequence descriptors, and the persisted metadata records. Every other
/// schema submodule builds on the definitions here.
pub const types = @import("schema/types.zig");

/// Order-preserving, delimiter-safe binary index-key token codec.
pub const index_key = @import("schema/index_key.zig");
/// The row codec submodule: packs typed column values into a B+Tree cell
/// payload and decodes them back, including MVCC version stamps.
pub const row = @import("schema/row.zig");
/// The logical-table submodule: binds a column layout to a B+Tree and defines
/// secondary indexes over it.
pub const table = @import("schema/table.zig");
/// The system-catalog submodule: the persistent registry of tables, indexes
/// and sequences.
pub const catalog = @import("schema/catalog.zig");
/// The database-handle submodule: ties a catalog to an open storage file.
pub const database = @import("schema/database.zig");

/// The set of scalar/logical column types a value can have (INT, TEXT, etc.);
/// drives packing width and comparison semantics. See [`types`].
pub const ColumnType = types.ColumnType;
/// A single column definition: name, [`ColumnType`], and constraints such as
/// nullability and default. The ordered list of these is a table's layout.
pub const Column = types.Column;
/// A resolved foreign-key relationship attached to a table, pairing local
/// columns to a referenced table's key plus its on-delete/on-update actions.
pub const ForeignKey = types.ForeignKey;
/// The parsed-but-unresolved form of a foreign key as written in DDL, before
/// the catalog links it to a concrete referenced [`Table`].
pub const ForeignKeyDefinition = types.ForeignKeyDefinition;
/// A named auto-increment sequence: its current value and step, persisted in
/// the catalog so `SERIAL`-style columns keep allocating unique ids across
/// restarts.
pub const Sequence = types.Sequence;
/// The persisted per-column metadata record the catalog serialises (name,
/// type, flags); the on-disk counterpart of a [`Column`].
pub const ColumnMetadata = types.ColumnMetadata;
/// The persisted per-table metadata record: its column list, root page, and
/// constraints. The catalog stores one of these per user table.
pub const TableMetadata = types.TableMetadata;
/// The persisted metadata for a schema object generally (the umbrella record
/// the catalog uses to discriminate tables, indexes and sequences).
pub const ObjectMetadata = types.ObjectMetadata;
/// The persisted per-index metadata record: which columns it covers, its
/// [`IndexKind`], and the B+Tree root that backs it.
pub const IndexMetadata = types.IndexMetadata;
/// The action taken on a child row when its referenced parent row is deleted
/// (e.g. CASCADE, RESTRICT, SET NULL); half of a [`ForeignKey`]'s policy.
pub const OnDeleteAction = types.OnDeleteAction;
/// The action taken on a child row when its referenced parent key is updated;
/// the update-side counterpart to [`OnDeleteAction`].
pub const OnUpdateAction = types.OnUpdateAction;

/// Builder that packs typed column values into a single B+Tree cell payload,
/// producing the byte layout [`RowReader`] later decodes. See [`row`].
pub const RowBuilder = row.RowBuilder;
/// Reader that decodes a packed cell payload back into typed column values,
/// the inverse of [`RowBuilder`].
pub const RowReader = row.RowReader;
/// A row's decoded MVCC version stamps (the transaction ids that bound its
/// visibility), produced by [`unpackVersions`].
pub const DecodedVersion = row.DecodedVersion;
/// Encodes a row's MVCC version stamps into the fixed header bytes of its cell
/// so readers can test visibility without decoding the whole row.
pub const packVersions = row.packVersions;
/// Decodes the MVCC version header packed by [`packVersions`] into a
/// [`DecodedVersion`].
pub const unpackVersions = row.unpackVersions;

/// A logical table: a named column layout bound to a B+Tree root, plus its
/// indexes and constraints. The unit the SQL executor reads and writes. See
/// [`table`].
pub const Table = table.Table;
/// Builder that assembles a [`Table`] definition (columns, keys, constraints)
/// before it is registered in the catalog and materialised on disk.
pub const TableBuilder = table.TableBuilder;
/// The kind of a secondary index (e.g. unique vs non-unique / primary),
/// governing whether duplicate keys are rejected. See [`types`].
pub const IndexKind = types.IndexKind;
/// The value an index entry maps its key to (typically the primary-key or row
/// locator used to fetch the full row from the base table).
pub const IndexValue = types.IndexValue;
/// A secondary index over a [`Table`]: a separate B+Tree keyed on chosen
/// columns whose entries point back at base rows via an [`IndexValue`].
pub const Index = table.Index;
/// Builder that assembles an [`Index`] definition (covered columns, kind)
/// before it is created against its base table.
pub const IndexBuilder = table.IndexBuilder;

/// The persistent registry of all tables, indexes and sequences, itself stored
/// in the engine. Alias of [`SystemCatalog`]; the short name callers prefer.
pub const Catalog = catalog.SystemCatalog;
/// The system catalog type in full: loads, mutates and persists schema
/// metadata as ordinary rows in reserved system tables. See [`catalog`].
pub const SystemCatalog = catalog.SystemCatalog;

/// The top-level database handle: pairs a [`SystemCatalog`] with an open
/// storage file and hands out [`Table`]s to the executor. See [`database`].
pub const Database = database.Database;
