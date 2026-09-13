//! Server-startup configuration, parsed from the on-disk `db.json`.
//!
//! This module defines the single source of truth for how a NovaDB server
//! instance is wired up at boot: which data model it serves, which address and
//! port it binds, where its data and WAL live, how durable commits are, pool
//! sizing, per-query memory, TLS, and replication. Everything the server needs
//! before it can accept a connection is a field on [`Config`], and every field
//! here is actually read by the server, the config IS the schema and unused
//! knobs are not carried.
//!
//! Design decisions worth knowing:
//!
//!   * **JSON is the schema.** [`Config`] is deserialised directly by
//!     `std.json` from `db.json`, so the struct layout IS the config format and
//!     the field defaults ARE the shipped defaults. A missing file is treated
//!     as `"{}"`, i.e. an all-defaults config, so a fresh install boots without
//!     any `db.json` at all.
//!
//!   * **Fail closed, fail loud.** Parsing runs in strict mode
//!     (`std.json` rejects unknown keys), so a stray or mistyped key aborts
//!     startup rather than being silently ignored. After a successful parse,
//!     [`Config.validate`] enforces the cross-field invariants that JSON typing
//!     alone cannot (non-zero port, non-empty `base_dir`, a replica port when
//!     replication is on). Both paths surface `error.InvalidConfig` so the
//!     operator sees exactly why the server refused to start.
//!
//!   * **Borrowed strings.** All `[]const u8` fields point into the parsed
//!     document, whose lifetime is owned by the returned `std.json.Parsed`. A
//!     `Config` is therefore only valid while that `Parsed` (or the arena
//!     behind it) is alive; callers keep the `Parsed` for the life of the
//!     server.
//!
//! It fits at the very top of the boot sequence: `main` calls [`Config.load`]
//! once, then hands the resulting `Config` down to the pager (`base_dir`,
//! `pool_size`), the WAL/durability layer (`durability`), the wire server
//! (`address`, `port`, `max_sessions`, `tls`, `mode`), and the replication
//! subsystem (`replica`).

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const schnell = @import("schnell.zig");
const page = @import("../storage/page.zig");

/// Scoped logger used to report configuration failures (bad `db.json`,
/// validation errors) with a `config` tag so they stand out at startup.
const log = std.log.scoped(.config);

/// Which data model a server instance exposes to clients. An instance runs ONE
/// surface so a driver/app opens a single connection with a single contract; a
/// mixed instance would force apps to hold both a SQL and a NoSQL connection.
/// `std.json` deserialises this from the lowercase member name (`"relational"`
/// or `"document"`).
pub const ServerMode = enum {
    /// SQL only. Document (`doc_op`) requests are rejected. Default, matching
    /// NovaDB's role as the orchestrator's relational control-plane store.
    relational,
    /// Document (NoSQL) only. SQL query/parse/bind/describe/execute frames and
    /// the HTTP `/query` endpoint are rejected.
    document,
};

/// The complete, deserialised server configuration.
///
/// This is the shape `db.json` must match: `std.json` maps the document onto
/// these fields, defaults fill anything omitted, and any key NOT named here is
/// a hard error (strict parsing). Every `[]const u8` and slice borrows from the
/// parsed document, so a `Config` outlives nothing, keep the owning
/// `std.json.Parsed` alive for as long as the config is used. Construct one
/// only via [`load`], which also runs [`validate`].
pub const Config = struct {
    /// The single data model this instance serves (see [`ServerMode`]); defaults
    /// to relational (SQL-only) so document ops must be explicitly opted into.
    mode: ServerMode = .relational,
    /// Interface address the server binds and listens on.
    address: []const u8 = "127.0.0.1",
    /// TCP port for the binary wire protocol; must be non-zero (see
    /// [`validate`]).
    port: u16 = 3009,
    /// Whether this node starts as the replication primary (writable). A
    /// replica sets this false and follows a primary.
    primary: bool = true,
    /// Hard cap on concurrent client sessions. `u31` so it always fits a
    /// signed count; must be greater than zero (see [`validate`]).
    max_sessions: u31 = 100,
    /// Directory holding the database files (pages, catalog); must be non-empty
    /// (see [`validate`]). Relative paths resolve against the process CWD.
    base_dir: []const u8 = "data",

    /// Write-ahead-log and commit-durability settings.
    ///
    /// [`synchronous_commit`] trades latency for durability: when false, a
    /// commit returns as soon as the WAL record is buffered and the background
    /// flusher pushes it to disk; when true, the commit waits for the fsync so a
    /// crash cannot lose an acknowledged transaction.
    durability: struct {
        /// Master switch for WAL-backed durability.
        enabled: bool = true,
        /// When true, a commit blocks until its WAL record is fsynced;
        /// when false it returns after buffering and relies on the flusher.
        synchronous_commit: bool = false,
        /// Directory to which retired WAL segments are archived before deletion.
        /// When non-empty, checkpoints and the age-based GC copy each segment
        /// here first, preserving the complete WAL history for point-in-time
        /// recovery. Empty (the default) means no archiving: segments are simply
        /// removed once a checkpoint has made them redundant.
        wal_archive_dir: []const u8 = "",
    } = .{},
    /// Size of the buffer pool in pages. `0` (the default) means **auto**: the
    /// server sizes the pool from physical RAM at startup (see
    /// [`effectivePoolPages`]) so the hot working set stays resident instead of
    /// thrashing a fixed 160 MB pool. Set a non-zero value to pin an explicit
    /// page count (e.g. for a memory-constrained container or a test harness).
    /// Each page is `page.PAGE_SIZE` bytes and the whole pool is committed at
    /// startup, so this is the server's resident memory floor.
    pool_size: u32 = 0,
    /// Hard cap on the memory a single query may allocate while it runs, in
    /// bytes. When a query would exceed this, it fails with "Query Memory Limit
    /// Exceeded" rather than growing the process until the OS OOM-kills the
    /// server. Defaults to 512 MiB, which comfortably fits every LIMIT-bounded
    /// result while stopping a runaway unindexed sort/scan from taking the
    /// server down. Set to 0 to disable the cap (unlimited). A per-request or
    /// per-session limit still overrides this.
    query_memory_limit_bytes: usize = 512 * 1024 * 1024,
    /// Page-mgmt (mmap reads): serve clean page reads borrowed from a read-only
    /// mmap of the data file (no `pread` copy) instead of copying into the slab
    /// window. Off by default. Enabled EARLY (before recovery and tree-cache
    /// population, see `Database.openWithMmap`) so startup reads borrow rather than
    /// materialise. HONEST NOTE (measured, 1M orders, see
    /// `match-page-mgmt-with-sqlite.md`): this is NOT a query speed-up over a pool
    /// sized to the working set, and NOT a steady-state RAM saving - the working
    /// set has to be resident either way. Its only real benefit is that mmap's
    /// residency is reclaimable clean file cache rather than private dirty pool
    /// pages, which the OS can drop and re-fault under memory pressure or
    /// co-tenancy. POSIX only; ignored on Windows.
    mmap_reads: bool = false,
    /// TLS settings for encrypted client connections.
    tls: struct {
        /// Whether to require TLS; when true both cert and key must be set.
        enabled: bool = false,
        /// PEM certificate file path.
        cert_file: []const u8 = "",
        /// PEM private-key file path.
        key_file: []const u8 = "",
    } = .{},
    /// Embedded HTTP server configuration, delegated to the `schnell` module.
    http: schnell.ServerConfig = .{},
    /// Security-feature toggle block.
    security: struct {
        /// Master switch for the security features. When true the server issues
        /// the password challenge at startup and authenticates connections
        /// against `sys.users` (an `admin` account is bootstrapped on a fresh
        /// database with the password `admin` — CHANGE IT before exposing the
        /// server; the engine logs a warning while it is unchanged).
        enabled: bool = false,
        /// Enforce authentication: reject any query/exec on a connection that has
        /// not authenticated. Implies `enabled`. With it off (default) the server
        /// keeps the first-run-friendly behaviour (open access when no users, or
        /// challenge-but-do-not-require when users exist).
        require_auth: bool = false,
        /// Only offer the cleartext-password login over a TLS connection; refuse
        /// a password login on a plaintext link (SQLSTATE 28000). Implies
        /// `require_auth` (hence `enabled`). Defaults false. When true, `tls`
        /// must be configured or the server refuses to start (no silent lockout).
        require_tls_for_auth: bool = false,
        /// Refuse to start while the bootstrap `admin` account still has its
        /// default password (`admin`). Forces an operator to rotate the seeded
        /// credential before the server will serve. Rotate it offline first with
        /// `novadb passwd admin '<newpw>'` (the server need not be running), then
        /// start with this on. Defaults false to keep first-run/dev friction low.
        require_admin_password_change: bool = false,
    } = .{},
    /// Replication settings for a follower (or the link a primary offers).
    ///
    /// When [`enabled`] is true, [`validate`] additionally requires a non-zero
    /// [`port`] so the replica has a concrete endpoint to connect to.
    replica: struct {
        /// Whether replication is active on this node.
        enabled: bool = false,
        /// How often the replica pulls/syncs from the primary, in
        /// milliseconds.
        sync_interval_ms: u32 = 1000,
        /// Address of the replication peer (the primary, for a follower).
        address: []const u8 = "127.0.0.1",
        /// Port of the replication peer; must be non-zero when
        /// [`enabled`] (see [`validate`]).
        port: u16 = 3010,
        /// Identity of this replica, used to authenticate to the primary.
        uid: []const u8 = "",
        /// Shared secret/key for the replication link.
        key: []const u8 = "",
    } = .{},

    /// Loads and validates the configuration from `db.json` in `dir`.
    ///
    /// Reads `db.json` if present; a `FileNotFound` is not an error but is
    /// treated as an empty object `"{}"`, so a server with no config file boots
    /// on all-defaults. Any other read error propagates.
    ///
    /// Parsing is strict (`allocate = .alloc_always`, unknown keys rejected):
    /// a stray key, a wrong value type, or malformed JSON is logged and turned
    /// into `error.InvalidConfig` so startup fails loudly rather than silently
    /// dropping a setting. On a good parse it runs [`validate`]; if validation
    /// fails the `errdefer` frees the parsed arena before returning.
    ///
    /// Returns a `std.json.Parsed(Config)` that OWNS the backing arena for all
    /// the borrowed string fields; the caller must keep it alive for the life
    /// of the config and eventually `deinit` it.
    pub fn load(allocator: std.mem.Allocator, io: std.Io, dir: Io.Dir) !std.json.Parsed(Config) {
        const content: ?[]u8 = Io.Dir.readFileAlloc(dir, io, "db.json", allocator, .unlimited) catch |err| switch (err) {
            error.FileNotFound => null,
            else => return err,
        };
        defer if (content) |c| allocator.free(c);
        const text = content orelse "{}";

        const parsed = std.json.parseFromSlice(Config, allocator, text, .{ .allocate = .alloc_always }) catch |err| {
            log.err("invalid db.json ({any}) -- a stray/mistyped key, wrong value type, or malformed JSON fails startup", .{err});
            return error.InvalidConfig;
        };
        errdefer parsed.deinit();
        try parsed.value.validate();
        return parsed;
    }

    /// Enforces the cross-field invariants that JSON typing cannot express.
    ///
    /// Called by [`load`] after a successful parse. Rejects a configuration
    /// that would parse cleanly but leave the server unable to run: an empty
    /// bind address, a zero port, a zero `max_sessions` or `pool_size`, an
    /// empty `base_dir`, or a `replica` that is enabled without a port to
    /// dial. Each failure logs a specific reason via [`invalidConfig`] and
    /// returns `error.InvalidConfig`.
    pub fn validate(self: *const Config) !void {
        if (self.address.len == 0) return invalidConfig("address is empty");
        if (self.port == 0) return invalidConfig("port must be non-zero");
        if (self.max_sessions == 0) return invalidConfig("max_sessions must be > 0");
        // pool_size == 0 is valid and means "auto" (sized from RAM at startup by
        // effectivePoolPages); a non-zero value is an explicit page count.
        if (self.base_dir.len == 0) return invalidConfig("base_dir must not be empty");
        if (self.replica.enabled and self.replica.port == 0) return invalidConfig("replica.port must be non-zero when replica.enabled is true");
    }

    /// The buffer-pool size in pages to actually open with: the explicit
    /// [`pool_size`] when non-zero, otherwise an auto value sized from physical
    /// RAM (see [`autoPoolPages`]). Call this (not the raw field) at server
    /// startup.
    pub fn effectivePoolPages(self: *const Config) u32 {
        return if (self.pool_size != 0) self.pool_size else autoPoolPages();
    }

    /// Logs `msg` under the `config` scope and returns `error.InvalidConfig`.
    ///
    /// A small helper so each [`validate`] check reads as a one-liner while
    /// still producing a specific, operator-facing reason for the rejection.
    fn invalidConfig(msg: []const u8) error{InvalidConfig} {
        log.err("invalid db.json: {s}", .{msg});
        return error.InvalidConfig;
    }
};

/// Historic fixed default (160 MB at a 16 KB page): the floor for auto-sizing
/// so a RAM probe failure or a tiny box never drops below the old behaviour.
const AUTO_POOL_FLOOR_PAGES: u32 = 10000;
/// Absolute ceiling for auto-sizing (8 GB of pages), so a single instance on a
/// very large host does not commit an unbounded resident pool. Operators who
/// want more set an explicit `pool_size`.
const AUTO_POOL_CAP_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Reserve at least this much RAM for the OS, the query working memory, and the
/// rest of the process, so the committed pool never starves the host.
const AUTO_POOL_HEADROOM_BYTES: u64 = 1024 * 1024 * 1024;

/// Auto buffer-pool size in pages: ~50% of physical RAM (a dedicated database
/// server should keep the hot working set resident rather than thrash a small
/// pool), but always leaving [`AUTO_POOL_HEADROOM_BYTES`] for the OS and query
/// memory, capped at [`AUTO_POOL_CAP_BYTES`], and never below
/// [`AUTO_POOL_FLOOR_PAGES`]. The whole pool is committed at startup, so this is
/// the resident-memory floor for the instance.
fn autoPoolPages() u32 {
    const ram = physicalRamBytes();
    if (ram == 0) return AUTO_POOL_FLOOR_PAGES;
    var bytes: u64 = ram / 2; // target ~50% of RAM for a dedicated server
    // Never take so much that less than the headroom is left for everything else.
    if (ram > AUTO_POOL_HEADROOM_BYTES and bytes > ram - AUTO_POOL_HEADROOM_BYTES) {
        bytes = ram - AUTO_POOL_HEADROOM_BYTES;
    }
    if (bytes > AUTO_POOL_CAP_BYTES) bytes = AUTO_POOL_CAP_BYTES;
    var pages: u64 = bytes / page.PAGE_SIZE;
    if (pages < AUTO_POOL_FLOOR_PAGES) pages = AUTO_POOL_FLOOR_PAGES;
    return std.math.cast(u32, pages) orelse std.math.maxInt(u32);
}

/// Best-effort physical-RAM probe. Returns 0 when it cannot be determined (the
/// caller then falls back to the floor). Linux reads `sysinfo`; macOS reads the
/// `hw.memsize` sysctl; other targets return 0.
fn physicalRamBytes() u64 {
    switch (builtin.os.tag) {
        .linux => {
            var info: std.os.linux.Sysinfo = undefined;
            if (std.os.linux.sysinfo(&info) == 0) {
                const unit: u64 = if (info.mem_unit == 0) 1 else info.mem_unit;
                return @as(u64, info.totalram) * unit;
            }
            return 0;
        },
        .macos, .ios, .tvos, .watchos => {
            var memsize: u64 = 0;
            var len: usize = @sizeOf(u64);
            const rc = std.c.sysctlbyname("hw.memsize", &memsize, &len, null, 0);
            return if (rc == 0) memsize else 0;
        },
        else => return 0,
    }
}

test "D2: config JSON maps nested replica + rejects unknown keys" {
    const a = std.testing.allocator;

    {
        const json =
            \\{ "port": 4000, "replica": { "enabled": true, "port": 4010, "address": "10.0.0.5" } }
        ;
        const p = try std.json.parseFromSlice(Config, a, json, .{ .allocate = .alloc_always });
        defer p.deinit();
        try std.testing.expectEqual(@as(u16, 4000), p.value.port);
        try std.testing.expect(p.value.replica.enabled);
        try std.testing.expectEqual(@as(u16, 4010), p.value.replica.port);
        try std.testing.expectEqualStrings("10.0.0.5", p.value.replica.address);
        try std.testing.expectEqual(@as(u32, 0), p.value.pool_size); // 0 = auto (sized from RAM)
        try p.value.validate();
    }

    {
        const p = try std.json.parseFromSlice(Config, a, "{}", .{ .allocate = .alloc_always });
        defer p.deinit();
        try std.testing.expectEqual(@as(u16, 3009), p.value.port);
        try std.testing.expectEqualStrings("data", p.value.base_dir);
        try std.testing.expectEqual(ServerMode.relational, p.value.mode);
        try p.value.validate();
    }

    {
        // A removed/unread key (e.g. the dropped `cache` block) is now rejected.
        const bad = "{ \"cache\": { \"enabled\": true } }";
        try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(Config, a, bad, .{ .allocate = .alloc_always }));
    }

    {
        const bad = "{ \"prot\": 3009 }";
        try std.testing.expectError(error.UnknownField, std.json.parseFromSlice(Config, a, bad, .{ .allocate = .alloc_always }));
    }

    {
        const bad = "{ \"port\": \"nope\" }";
        const r = std.json.parseFromSlice(Config, a, bad, .{ .allocate = .alloc_always });
        try std.testing.expect(std.meta.isError(r));
    }
}
