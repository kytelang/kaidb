//! In-memory system catalog: the engine's record of every schema object.
//!
//! This module holds the one authoritative, in-memory view of the database
//! schema. Where the slotted-page B+Tree ([`storage/btree.zig`]) stores the
//! rows and the WAL ([`durability/write_ahead_log.zig`]) stores the durable
//! change history, the catalog stores the *shape* of the data: which tables
//! exist, what indexes cover them, which foreign keys constrain them, and which
//! sequences generate their auto-increment values. Every layer that needs to
//! interpret raw pages consults it, the SQL parser resolves a table name to a
//! [`table.Table`] here, the planner asks [`SystemCatalog.getIndexesForTable`]
//! which indexes it may use, and the executor reads column layouts to encode
//! and decode cells.
//!
//! The design is deliberately simple: four flat [`std.ArrayList`]s (tables,
//! indexes, foreign keys, sequences) with linear scans for lookup. A catalog is
//! small (tens to hundreds of objects) and read far more often than mutated, so
//! a hash index would add complexity without a measurable win at this scale; if
//! that ever changes, the lookup helpers are the single place to add one.
//!
//! Ownership and lifetime are the key invariants a caller must respect. The
//! catalog *owns* every object appended to it: [`SystemCatalog.addTable`] and
//! its siblings take the object by value and take responsibility for freeing it
//! in [`SystemCatalog.deinit`], which walks each list and calls the object's
//! own `deinit` before releasing the list backing store. This has two
//! consequences. First, every list uses the unmanaged `ArrayList` API, so the
//! catalog's [`SystemCatalog.allocator`] is threaded explicitly into every
//! `append`/`deinit` call. Second, the *getter* methods return objects **by
//! value** ([`table.Table`] is a small struct of an id, a borrowed name slice
//! and an owned column list), so a returned `Table` aliases the catalog's
//! interior storage: it stays valid only while the catalog is alive and
//! untouched, and the caller must NOT call `deinit` on it. The one exception is
//! [`SystemCatalog.getIndexesForTable`], which allocates a fresh owned slice the
//! caller must free.
//!
//! The catalog is not internally synchronised. In kaidb the db-wide lock gates
//! DDL exclusively (see kaidb's `architecture.md`), so schema mutation happens
//! single-threaded under that lock while readers are excluded; concurrent
//! append and scan is therefore never expected here.

const std = @import("std");
const Allocator = std.mem.Allocator;
const table_mod = @import("table.zig");
const types = @import("types.zig");

/// The database's complete in-memory schema, owning every catalog object.
///
/// One instance describes one database. It aggregates the four kinds of schema
/// object into parallel lists and provides name/id lookups over them. See the
/// module header for the ownership contract: the catalog frees everything it
/// holds in [`SystemCatalog.deinit`], and value-returning getters hand back
/// aliases into interior storage, not copies to be freed.
pub const SystemCatalog = struct {
    /// Allocator threaded into every list operation; also frees owned objects.
    ///
    /// The lists are the unmanaged `ArrayList` variant, so this allocator is
    /// passed explicitly to each `append`/`deinit` and must be the same one for
    /// the catalog's whole life, mixing allocators corrupts the heap on free.
    allocator: Allocator,
    /// Every table defined in the database, in creation order.
    ///
    /// Each [`table.Table`] owns its column list; the catalog owns the `Table`s
    /// themselves and destructs them in [`SystemCatalog.deinit`].
    tables: std.ArrayList(table_mod.Table),
    /// Every index across all tables, flat rather than grouped per table.
    ///
    /// Membership is recovered by matching `Index.table_id`; see
    /// [`SystemCatalog.getIndexesForTable`].
    indexes: std.ArrayList(table_mod.Index),
    /// Every foreign-key constraint, used to enforce referential integrity.
    foreign_keys: std.ArrayList(types.ForeignKey),
    /// Every sequence (auto-increment/serial generator) in the database.
    sequences: std.ArrayList(types.Sequence),

    /// Creates an empty catalog bound to `allocator`.
    ///
    /// No allocation happens here: each list starts as `.empty` and grows
    /// lazily on the first `add*` call. The returned value must eventually be
    /// released with [`SystemCatalog.deinit`], which uses this same allocator.
    pub fn init(allocator: Allocator) SystemCatalog {
        return SystemCatalog{
            .allocator = allocator,
            .tables = std.ArrayList(table_mod.Table).empty,
            .indexes = std.ArrayList(table_mod.Index).empty,
            .foreign_keys = std.ArrayList(types.ForeignKey).empty,
            .sequences = std.ArrayList(types.Sequence).empty,
        };
    }

    /// Registers `table`, transferring ownership of it (and its columns) to the
    /// catalog.
    ///
    /// The `Table` is appended by value; the caller must not `deinit` it after
    /// this call, as [`SystemCatalog.deinit`] now owns its destruction. No
    /// uniqueness check is performed here, callers are expected to have
    /// resolved name/id conflicts before registering. Returns
    /// `error.OutOfMemory` if the list cannot grow.
    pub fn addTable(self: *SystemCatalog, table: table_mod.Table) !void {
        try self.tables.append(self.allocator, table);
    }

    /// Registers `index`, transferring ownership to the catalog.
    ///
    /// Indexes are stored flat and associated with their table by the
    /// `table_id` they carry, not by position; see
    /// [`SystemCatalog.getIndexesForTable`]. Returns `error.OutOfMemory` on
    /// allocation failure.
    pub fn addIndex(self: *SystemCatalog, index: table_mod.Index) !void {
        try self.indexes.append(self.allocator, index);
    }

    /// Registers foreign-key constraint `fk`, transferring ownership to the
    /// catalog.
    ///
    /// Returns `error.OutOfMemory` on allocation failure.
    pub fn addForeignKey(self: *SystemCatalog, fk: types.ForeignKey) !void {
        try self.foreign_keys.append(self.allocator, fk);
    }

    /// Registers sequence `seq`, transferring ownership to the catalog.
    ///
    /// Returns `error.OutOfMemory` on allocation failure.
    pub fn addSequence(self: *SystemCatalog, seq: types.Sequence) !void {
        try self.sequences.append(self.allocator, seq);
    }

    /// Looks up a table by its numeric id, or returns `null` if none matches.
    ///
    /// This is the id-keyed counterpart to [`SystemCatalog.getTable`] and does
    /// a linear scan of [`SystemCatalog.tables`]. The returned [`table.Table`]
    /// is a value copy that aliases the catalog's owned column storage: valid
    /// only while the catalog is alive and unmodified, and must NOT be
    /// `deinit`ed by the caller.
    pub fn getTableById(self: SystemCatalog, id: u32) ?table_mod.Table {
        for (self.tables.items) |tbl| {
            if (tbl.id == id) return tbl;
        }
        return null;
    }

    /// Looks up a table by name (exact, case-sensitive byte match), or `null`.
    ///
    /// This is the primary resolution path from a SQL statement's table name to
    /// its schema. Matching is a raw [`std.mem.eql`] over the name bytes, so it
    /// does no case-folding or quoting normalisation, the caller must pass the
    /// name exactly as stored. Like [`SystemCatalog.getTableById`], the returned
    /// [`table.Table`] aliases interior storage and must not be `deinit`ed.
    pub fn getTable(self: SystemCatalog, name: []const u8) ?table_mod.Table {
        for (self.tables.items) |tbl| {
            if (std.mem.eql(u8, tbl.name, name)) return tbl;
        }
        return null;
    }

    /// Returns a freshly allocated slice of every index belonging to `table_id`.
    ///
    /// Because indexes are stored flat, this filters [`SystemCatalog.indexes`]
    /// by `table_id` into a new owned slice, unlike the table getters, the
    /// result is a caller-owned copy that must be freed with the catalog's
    /// allocator. The returned [`table.Index`] values still alias the catalog's
    /// interior index storage, so the *slice* is owned but its elements are not
    /// deep copies.
    ///
    /// Failure is swallowed rather than propagated: if appending a match fails
    /// it is skipped (`catch continue`), and if the final ownership transfer
    /// fails an empty slice (`&.{}`) is returned. Callers therefore cannot
    /// distinguish "no indexes" from "allocation failed", acceptable here
    /// because a missing index only costs the planner a fallback scan, never
    /// correctness.
    pub fn getIndexesForTable(self: SystemCatalog, table_id: u32) []table_mod.Index {
        var result = std.ArrayList(table_mod.Index).empty;
        defer result.deinit();
        for (self.indexes.items) |idx| {
            if (idx.table_id == table_id) result.append(self.allocator, idx) catch continue;
        }
        return result.toOwnedSlice(self.allocator) catch &.{};
    }

    /// Destroys the catalog and every schema object it owns.
    ///
    /// Walks each of the four lists calling the element's own `deinit` first
    /// (so a table's columns, an index's key parts, and so on are released),
    /// then frees each list's backing store. After this call the catalog and
    /// every value previously returned from a getter are dangling and must not
    /// be used. Uses [`SystemCatalog.allocator`], which must be the allocator
    /// passed to [`SystemCatalog.init`].
    pub fn deinit(self: *SystemCatalog) void {
        for (self.tables.items) |*t| t.deinit();
        self.tables.deinit(self.allocator);
        for (self.indexes.items) |*i| i.deinit();
        self.indexes.deinit(self.allocator);
        for (self.foreign_keys.items) |*f| f.deinit();
        self.foreign_keys.deinit(self.allocator);
        for (self.sequences.items) |*s| s.deinit();
        self.sequences.deinit(self.allocator);
    }
};
