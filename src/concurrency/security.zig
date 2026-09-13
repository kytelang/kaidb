//! Authentication, authorisation, and session management for NovaDB.
//!
//! This module is the security gate that sits in front of the SQL engine. Every
//! connection that arrives over the binary wire protocol is authenticated here,
//! handed a [`Session`] token, and then re-checked against that session on each
//! statement before the executor is allowed to touch a table. The single
//! long-lived owner is [`SecurityManager`]; the rest of the types
//! ([`User`], [`Role`], [`Permission`], [`Privilege`], [`Session`]) are the
//! records it keeps in memory.
//!
//! ## Where the data lives
//!
//! The authoritative copies of users, roles, and privileges are ordinary rows
//! in the `sys.users`, `sys.roles`, and `sys.privileges` system tables, stored
//! in the same MVCC B+Tree as user data. [`SecurityManager`] holds an in-memory
//! cache of those rows for fast lookups on the authentication hot path;
//! [`SecurityManager.loadUsers`] and [`SecurityManager.loadRolesAndPrivileges`]
//! rebuild the cache by scanning those trees, reconstructing each row's version
//! chain and taking the latest committed version (one whose `xmax` is still 0,
//! i.e. not deleted). Because the source of truth is MVCC rows, the cache can be
//! reloaded at any time without a schema of its own.
//!
//! ## Two-tier authorisation model
//!
//! Authorisation is checked at two granularities:
//!
//!   1. Coarse role capabilities ([`Permission`]) derived from a user's
//!      [`Role`]. `admin` can do everything, `read_write` reads and writes but
//!      cannot administer, `read_only` only reads, `none` can do nothing. This
//!      is the fast path checked by [`SecurityManager.checkPermission`].
//!   2. Fine-grained per-object grants ([`Privilege`]) checked by
//!      [`SecurityManager.checkObjectPermission`] via
//!      [`SecurityManager.hasObjectPrivilege`]. Grants can be made directly to a
//!      user or to a role the user belongs to, and roles can be granted to other
//!      roles, so membership is resolved transitively by
//!      [`SecurityManager.resolveRoles`]. `sys.*` tables are never reachable
//!      through object grants: only an admin session may touch them.
//!
//! ## Password storage and login throttling
//!
//! Passwords are never stored in plaintext. Each user row carries an
//! Argon2id-derived 32-byte hash and a 32-byte salt (see
//! [`SecurityManager.hashKey`]), persisted as `hex(hash):hex(salt)`. Verification
//! is constant-time (`std.crypto.timing_safe.eql`), and a failed lookup still
//! runs a throwaway hash so that a missing username and a wrong password take
//! the same time, closing a user-enumeration timing side channel.
//!
//! Brute force is bounded by a per-key lockout table
//! ([`LoginAttemptInfo`]): after `max_failed_attempts` failures the key is locked
//! out, and each further failure past the threshold multiplies the lockout
//! window (capped at one hour). The throttle key is the peer IP when known (so
//! one attacker cannot lock out a victim's account by name), except for loopback
//! addresses which are never throttled; it falls back to the username when no IP
//! is available.
//!
//! ## Concurrency
//!
//! [`SecurityManager`] is shared across all connection handlers, so each cache
//! (`users`, `sessions`, `login_attempts`, `roles`, `privileges`) has its own
//! [`std.Io.Mutex`]. The mutexes are independent and are taken for the shortest
//! possible span; note that [`SecurityManager.hasObjectPrivilege`] holds
//! `privileges_mutex` while it walks the grant list, and reads the `users` cache
//! under `users_mutex` first, so callers must not already hold either lock.
//! Locks are acquired with `lockUncancelable` because releasing security state
//! partway through a cancellation would be unsafe.
//!
//! When authentication is disabled (`enabled == false`) the whole layer is a
//! no-op: [`SecurityManager.authenticate`] and
//! [`SecurityManager.validateSession`] hand back a synthetic anonymous admin
//! session, so the engine runs unguarded. This is the embedded/single-user mode.

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const Mutex = std.Io.Mutex;
const Database = @import("../schema/database.zig").Database;
const BPlusTree = @import("../storage/btree.zig").BPlusTree;
const RowReader = @import("../schema/row.zig").RowReader;
const unpackVersions = @import("../schema/row.zig").unpackVersions;

/// A coarse authorisation level assigned to a [`User`].
///
/// The order is not a privilege lattice the code relies on; each variant maps to
/// a concrete [`Permission`] set via [`Permission.fromRole`]. `none` is the
/// safe default for an unknown or unmapped role string (see [`Role.fromString`]).
pub const Role = enum {
    /// Full control: read, write, delete, and administer. Bypasses object-level
    /// privilege checks in [`SecurityManager.hasObjectPrivilege`].
    admin,
    /// Read, write, and delete on user tables, but no administrative rights.
    read_write,
    /// Read-only access; no writes, deletes, or administration.
    read_only,
    /// No capabilities at all. Also the fallback for an unrecognised role name.
    none,

    /// Parses a stored role string into a [`Role`], defaulting to `.none`.
    ///
    /// Any string that is not exactly one of the three known role names maps to
    /// `.none` rather than erroring, so a corrupt or unknown `role` column in a
    /// `sys.users` row fails closed to zero privileges.
    pub fn fromString(str: []const u8) Role {
        if (std.mem.eql(u8, str, "admin")) return .admin;
        if (std.mem.eql(u8, str, "read_write")) return .read_write;
        if (std.mem.eql(u8, str, "read_only")) return .read_only;
        return .none;
    }

    /// Renders a [`Role`] back to its canonical stored string.
    ///
    /// Inverse of [`Role.fromString`]. Used both to persist the role and, in
    /// [`SecurityManager.hasObjectPrivilege`], to treat the user's default role
    /// as a grant principal named by this string.
    pub fn toString(self: Role) []const u8 {
        return switch (self) {
            .admin => "admin",
            .read_write => "read_write",
            .read_only => "read_only",
            .none => "none",
        };
    }
};

/// The four capability flags a [`Session`] carries, derived from a [`Role`].
///
/// This is the coarse, role-level authorisation used on the fast path
/// ([`SecurityManager.checkPermission`]); finer per-object rights are checked
/// separately against [`Privilege`] rows. All flags default to `false` so a
/// zero-initialised value denies everything.
pub const Permission = struct {
    /// May run `SELECT` / read statements.
    can_read: bool = false,
    /// May run `INSERT` / `UPDATE` (and, at role level, delete) statements.
    can_write: bool = false,
    /// May run `DELETE` statements.
    can_delete: bool = false,
    /// May run administrative / DDL operations and bypass object grants.
    can_admin: bool = false,

    /// Maps a [`Role`] to its concrete capability flags.
    ///
    /// Note that `read_write` is granted `can_delete = true` here even though
    /// the flag is nominally the delete capability: at role granularity a
    /// read-write user may delete rows. `none` yields the all-false default.
    pub fn fromRole(role: Role) Permission {
        return switch (role) {
            .admin => Permission{
                .can_read = true,
                .can_write = true,
                .can_delete = true,
                .can_admin = true,
            },
            .read_write => Permission{
                .can_read = true,
                .can_write = true,
                .can_delete = true,
                .can_admin = false,
            },
            .read_only => Permission{
                .can_read = true,
                .can_write = false,
                .can_delete = false,
                .can_admin = false,
            },
            .none => Permission{},
        };
    }
};

/// An in-memory cached account, mirroring one row of `sys.users`.
///
/// Held in [`SecurityManager`]'s `users` map keyed by username. The `key_hash`
/// and `key_salt` are the Argon2id verifier material; the plaintext password is
/// never stored. Owns its `username` allocation (see [`User.deinit`]).
pub const User = struct {
    /// Login name. Heap-owned; freed by [`User.deinit`].
    username: []const u8,
    /// Argon2id-derived 32-byte password verifier, computed by
    /// [`SecurityManager.hashKey`] over the password and `key_salt`.
    key_hash: [32]u8,
    /// Per-user 32-byte salt fed into the Argon2id KDF.
    key_salt: [32]u8,
    /// The account's coarse authorisation level.
    role: Role,
    /// Whether the account may authenticate at all; a disabled user is rejected
    /// with `error.UserDisabled` even with correct credentials.
    enabled: bool,

    /// Frees the owned `username` slice. Does not touch the fixed-size arrays.
    pub fn deinit(self: *User, allocator: Allocator) void {
        allocator.free(self.username);
    }
};

/// One fine-grained grant, mirroring a row of `sys.privileges`.
///
/// Interpreted two ways by [`SecurityManager`]: when `object_name` is empty the
/// row is a role membership (grantee belongs to the role named in `privilege`,
/// walked by [`SecurityManager.resolveRoles`]); otherwise it is an object grant
/// (grantee may perform `privilege`, or `ALL`, on `object_name`). All three
/// slices are heap-owned (see [`Privilege.deinit`]).
pub const Privilege = struct {
    /// The principal receiving the grant: a username or a role name.
    grantee: []const u8,
    /// The object the grant applies to, or empty for a role-membership row.
    object_name: []const u8,
    /// The privilege verb (e.g. `SELECT`, `ALL`), or, for a membership row, the
    /// role being granted to `grantee`.
    privilege: []const u8,

    /// Frees all three owned slices.
    pub fn deinit(self: *Privilege, allocator: Allocator) void {
        allocator.free(self.grantee);
        allocator.free(self.object_name);
        allocator.free(self.privilege);
    }
};

/// A live authenticated session, minted by [`SecurityManager.authenticate`].
///
/// The `token` is the opaque 32-byte bearer credential the client presents on
/// each subsequent request; it keys the `sessions` map. `permissions` is a
/// snapshot of the user's role capabilities taken at login time, so a role
/// change does not affect sessions already open until they expire. Owns its
/// `username` allocation (see [`Session.deinit`]).
pub const Session = struct {
    /// Random 32-byte bearer token identifying this session.
    token: [32]u8,
    /// The authenticated username. Heap-owned; freed by [`Session.deinit`].
    username: []const u8,
    /// Unix epoch milliseconds at which the session was created.
    created_at: i64,
    /// Unix epoch milliseconds after which the session is expired and rejected.
    expires_at: i64,
    /// Capability snapshot taken from the user's [`Role`] at login.
    permissions: Permission,

    /// Returns whether the session has not yet expired at `current_time`.
    ///
    /// A pure time comparison against `expires_at`; revocation
    /// ([`SecurityManager.revokeSession`]) is handled separately by removing the
    /// session from the map.
    pub fn isValid(self: *const Session, current_time: i64) bool {
        return current_time < self.expires_at;
    }

    /// Frees the owned `username` slice.
    pub fn deinit(self: *Session, allocator: Allocator) void {
        allocator.free(self.username);
    }
};

/// Per-throttle-key failure bookkeeping backing the brute-force lockout.
///
/// Stored in [`SecurityManager`]'s `login_attempts` map, keyed by peer IP (or
/// username when no IP is available). Updated by
/// [`SecurityManager.recordFailedAttempt`] and cleared on success.
const LoginAttemptInfo = struct {
    /// Number of consecutive failed authentications for this key.
    failed_count: u32,
    /// Unix epoch milliseconds of the most recent failure.
    last_failed_at: i64,
    /// Unix epoch milliseconds until which this key is locked out, or 0 if not
    /// currently locked.
    lockout_until: i64,
};

/// The action a statement wants to take, checked against a [`Session`]'s
/// [`Permission`] by [`SecurityManager.checkPermission`].
pub const PermissionType = enum {
    /// A read / `SELECT` operation. Requires `can_read`.
    read,
    /// A write / `INSERT` / `UPDATE` operation. Requires `can_write`.
    write,
    /// A `DELETE` operation. Requires `can_delete`.
    delete,
    /// An administrative / DDL operation. Requires `can_admin`.
    admin,
};

/// The process-wide security state: account cache, active sessions, lockout
/// table, and grant catalogue.
///
/// Heap-allocated and shared by every connection handler, so each cache is
/// guarded by its own mutex (see the module-level concurrency notes). Construct
/// with [`SecurityManager.init`] and tear down with [`SecurityManager.deinit`].
/// When `enabled` is false the whole layer short-circuits to an anonymous admin
/// (embedded single-user mode).
pub const SecurityManager = struct {
    /// Allocator owning every cached slice and this struct itself.
    allocator: Allocator,
    /// Master switch. When false, authentication and checks are bypassed and a
    /// synthetic admin session is returned everywhere.
    enabled: bool,
    /// When true, a connection that has not authenticated is refused at the
    /// query/exec boundary (not just challenged at startup). Set from
    /// `config.security.require_auth`; implies `enabled`. Defaults false so the
    /// embedded/first-run path is unchanged.
    require_auth: bool = false,
    /// When true, the cleartext-password challenge is only offered over a TLS
    /// connection; a password login attempt on a plaintext connection is refused
    /// (SQLSTATE 28000) before any password is read. Set from
    /// `config.security.require_tls_for_auth`; implies `require_auth` (hence
    /// `enabled`). Defaults false. The server refuses to start with this on but
    /// no TLS configured, so it can never silently lock every client out.
    require_tls_for_auth: bool = false,
    /// The async I/O context used for the clock, CSPRNG, mutexes, and the
    /// Argon2 KDF.
    io: Io,

    /// Username to [`User`] cache, the source for the authentication hot path.
    /// Rebuilt from `sys.users` by [`SecurityManager.loadUsers`].
    users: std.StringHashMap(User),
    /// Guards `users`. Held while reading/rebuilding the account cache.
    users_mutex: Mutex = .init,

    /// Active sessions keyed by their 32-byte [`Session.token`].
    sessions: std.AutoHashMap([32]u8, Session),
    /// Guards `sessions`.
    sessions_mutex: Mutex = .init,

    /// Brute-force lockout table keyed by throttle key (peer IP or username).
    login_attempts: std.StringHashMap(LoginAttemptInfo),
    /// Guards `login_attempts`.
    login_attempts_mutex: Mutex = .init,

    /// Set of known role names (value-less map). Seeded with the three built-in
    /// roles and extended from `sys.roles`.
    roles: std.StringHashMap(void),
    /// Guards `roles`.
    roles_mutex: Mutex = .init,

    /// Flat list of all [`Privilege`] grants and role memberships, scanned
    /// linearly during authorisation checks.
    privileges: std.ArrayList(Privilege),
    /// Guards `privileges`. Also held across the grant walk in
    /// [`SecurityManager.hasObjectPrivilege`].
    privileges_mutex: Mutex = .init,

    /// Session lifetime in milliseconds; a new session expires this far past its
    /// creation time. Default one hour.
    session_timeout_ms: i64 = 3600 * 1000,
    /// Consecutive failures allowed before a throttle key is locked out.
    max_failed_attempts: u32 = 5,
    /// Base lockout window in milliseconds once the threshold is crossed.
    /// Default fifteen minutes.
    lockout_duration_ms: i64 = 900_000,
    /// Factor by which the lockout window grows for each failure past the
    /// threshold (exponential backoff, capped at one hour).
    lockout_multiplier: u32 = 2,

    /// Allocates and initialises a [`SecurityManager`], seeding the three
    /// built-in roles (`admin`, `read_write`, `read_only`).
    ///
    /// Returns a heap pointer the caller owns and must release with
    /// [`SecurityManager.deinit`]. Errors if any allocation fails. The account,
    /// session, and privilege caches start empty; call
    /// [`SecurityManager.loadUsers`] and
    /// [`SecurityManager.loadRolesAndPrivileges`] to populate them from the
    /// system tables.
    pub fn init(allocator: Allocator, enabled: bool, io: Io) !*SecurityManager {
        const mgr = try allocator.create(SecurityManager);
        mgr.* = SecurityManager{
            .allocator = allocator,
            .enabled = enabled,
            .io = io,
            .users = std.StringHashMap(User).init(allocator),
            .sessions = std.AutoHashMap([32]u8, Session).init(allocator),
            .login_attempts = std.StringHashMap(LoginAttemptInfo).init(allocator),
            .roles = std.StringHashMap(void).init(allocator),
            .privileges = std.ArrayList(Privilege).empty,
        };
        try mgr.roles.put(try allocator.dupe(u8, "admin"), {});
        try mgr.roles.put(try allocator.dupe(u8, "read_write"), {});
        try mgr.roles.put(try allocator.dupe(u8, "read_only"), {});
        return mgr;
    }

    /// Frees every cached record (users, sessions, lockout keys, role names,
    /// privileges) and then the manager itself.
    ///
    /// Must be called exactly once on a pointer returned by
    /// [`SecurityManager.init`]. Each map's owned keys and each record's owned
    /// slices are released individually because the maps hold duplicated key
    /// strings alongside the value payloads.
    pub fn deinit(self: *SecurityManager) void {
        var user_iter = self.users.iterator();
        while (user_iter.next()) |entry| {
            var u = entry.value_ptr.*;
            u.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.users.deinit();

        var session_iter = self.sessions.iterator();
        while (session_iter.next()) |entry| {
            var s = entry.value_ptr.*;
            s.deinit(self.allocator);
        }
        self.sessions.deinit();

        var attempts_iter = self.login_attempts.iterator();
        while (attempts_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.login_attempts.deinit();

        var roles_iter = self.roles.iterator();
        while (roles_iter.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
        }
        self.roles.deinit();

        for (self.privileges.items) |*p| {
            p.deinit(self.allocator);
        }
        self.privileges.deinit(self.allocator);

        self.allocator.destroy(self);
    }

    /// Rebuilds the `users` cache by scanning the `sys.users` B+Tree.
    ///
    /// For each key it reconstructs the MVCC version chain and takes the latest
    /// version, skipping rows that have been deleted (`xmax > 0`) or whose chain
    /// is empty. The stored `password_hash` column is expected in
    /// `hex(hash):hex(salt)` form; a row without the `:` separator is silently
    /// skipped rather than cached. Returns early (leaving the cache untouched)
    /// if the `sys.users` table or its root page is absent, e.g. on a fresh
    /// database. Takes `users_mutex` and clears the old cache before repopulating,
    /// so it is safe to call repeatedly to refresh.
    pub fn loadUsers(self: *SecurityManager, db: *Database) !void {
        const user_table = db.catalog.getTable("sys.users") orelse return;
        const users_root = db.table_roots.get("sys.users") orelse return;
        var tree = try BPlusTree.init(db.pool, users_root, db.allocator);
        defer tree.deinit();

        var it = try tree.iterator();
        defer it.deinit();

        self.users_mutex.lockUncancelable(self.io);
        defer self.users_mutex.unlock(self.io);

        var user_iter = self.users.iterator();
        while (user_iter.next()) |entry| {
            var u = entry.value_ptr.*;
            u.deinit(self.allocator);
            self.allocator.free(entry.key_ptr.*);
        }
        self.users.clearRetainingCapacity();

        while (try it.next()) |cell| {
            const versions = try db.reconstructVersionChain(cell.value, self.allocator);
            defer {
                for (versions) |*v| v.deinit(self.allocator);
                self.allocator.free(versions);
            }

            if (versions.len == 0) continue;
            const latest = versions[versions.len - 1];
            if (latest.xmax > 0) continue;

            const reader = RowReader.init(user_table, latest.fixed, latest.heap);
            const username = try reader.readToString(self.allocator, "username");
            errdefer self.allocator.free(username);
            const password_hash = try reader.readToString(self.allocator, "password_hash");
            defer self.allocator.free(password_hash);
            const role_str = try reader.readToString(self.allocator, "role");
            defer self.allocator.free(role_str);

            const role = Role.fromString(role_str);

            if (std.mem.indexOfScalar(u8, password_hash, ':')) |colon_pos| {
                const hex_hash = password_hash[0..colon_pos];
                const hex_salt = password_hash[colon_pos + 1 ..];

                const hash_bytes = try hexDecode(self.allocator, hex_hash);
                defer self.allocator.free(hash_bytes);
                const salt_bytes = try hexDecode(self.allocator, hex_salt);
                defer self.allocator.free(salt_bytes);

                var key_hash: [32]u8 = undefined;
                var key_salt: [32]u8 = undefined;
                @memcpy(&key_hash, hash_bytes[0..32]);
                @memcpy(&key_salt, salt_bytes[0..32]);

                const user = User{
                    .username = username,
                    .key_hash = key_hash,
                    .key_salt = key_salt,
                    .role = role,
                    .enabled = true,
                };
                const user_key = try self.allocator.dupe(u8, username);
                try self.users.put(user_key, user);
            }
        }
    }

    /// Verifies credentials and, on success, mints and stores a new [`Session`].
    ///
    /// When the manager is disabled this returns a synthetic anonymous admin
    /// session without checking anything. Otherwise the flow is:
    ///
    ///   1. Pick a throttle key: the `peer_ip` if given and not loopback, else
    ///      the `username`. If that key is currently locked out, fail with
    ///      `error.AccountLockedOut`; if a stale lockout has elapsed, clear it.
    ///   2. Look up the user. A missing user still runs a throwaway
    ///      [`SecurityManager.hashKey`] so the failure is not distinguishable by
    ///      timing from a wrong password, then records a failed attempt and
    ///      returns `error.InvalidCredentials`.
    ///   3. Reject a disabled account with `error.UserDisabled`.
    ///   4. Hash the password with the user's salt and compare in constant time;
    ///      a mismatch records a failed attempt and returns
    ///      `error.InvalidCredentials`.
    ///   5. On success clear the failed-attempt record, generate a random token,
    ///      build a session expiring `session_timeout_ms` from now, insert it,
    ///      and return it.
    ///
    /// Acquires `login_attempts_mutex`, then `users_mutex` for the whole check,
    /// then `sessions_mutex` to insert; callers must hold none of these.
    pub fn authenticate(self: *SecurityManager, username: []const u8, password: []const u8, peer_ip: ?[]const u8) !Session {
        if (!self.enabled) {
            return Session{
                .token = [_]u8{0} ** 32,
                .username = "anonymous",
                .created_at = std.Io.Clock.now(.real, self.io).toMilliseconds(),
                .expires_at = std.math.maxInt(i64),
                .permissions = Permission.fromRole(.admin),
            };
        }

        const now = std.Io.Clock.now(.real, self.io).toMilliseconds();
        const throttle_key = if (peer_ip) |ip|
            (if (isLoopbackIp(ip)) null else ip)
        else
            username;

        if (throttle_key) |tk| {
            self.login_attempts_mutex.lockUncancelable(self.io);
            defer self.login_attempts_mutex.unlock(self.io);
            if (self.login_attempts.get(tk)) |info| {
                if (now < info.lockout_until) {
                    return error.AccountLockedOut;
                }
                if (info.lockout_until > 0 and now >= info.lockout_until) {
                    self.clearFailedAttemptsLocked(tk);
                }
            }
        }

        self.users_mutex.lockUncancelable(self.io);
        defer self.users_mutex.unlock(self.io);

        const user = self.users.get(username) orelse {
            _ = self.hashKey(password, [_]u8{0} ** 32) catch {};
            if (throttle_key) |tk| self.recordFailedAttempt(tk, now);
            return error.InvalidCredentials;
        };

        if (!user.enabled) {
            return error.UserDisabled;
        }

        const computed = try self.hashKey(password, user.key_salt);
        if (!std.crypto.timing_safe.eql([32]u8, computed, user.key_hash)) {
            if (throttle_key) |tk| self.recordFailedAttempt(tk, now);
            return error.InvalidCredentials;
        }

        if (throttle_key) |tk| self.clearFailedAttempts(tk);

        var token: [32]u8 = undefined;
        std.Io.random(self.io, &token);

        const session = Session{
            .token = token,
            .username = try self.allocator.dupe(u8, username),
            .created_at = now,
            .expires_at = now + self.session_timeout_ms,
            .permissions = Permission.fromRole(user.role),
        };

        self.sessions_mutex.lockUncancelable(self.io);
        defer self.sessions_mutex.unlock(self.io);
        try self.sessions.put(token, session);

        return session;
    }

    /// Resolves a bearer `token` to its live [`Session`], enforcing expiry.
    ///
    /// When disabled, returns the synthetic anonymous admin session. Otherwise
    /// looks the token up under `sessions_mutex` and returns
    /// `error.InvalidSession` if unknown or `error.SessionExpired` if past its
    /// `expires_at`. Note that an expired session is left in the map here (it is
    /// only removed by [`SecurityManager.revokeSession`]); this call just
    /// refuses to honour it.
    pub fn validateSession(self: *SecurityManager, token: [32]u8) !Session {
        if (!self.enabled) {
            return Session{
                .token = [_]u8{0} ** 32,
                .username = "anonymous",
                .created_at = std.Io.Clock.now(.real, self.io).toMilliseconds(),
                .expires_at = std.math.maxInt(i64),
                .permissions = Permission.fromRole(.admin),
            };
        }

        self.sessions_mutex.lockUncancelable(self.io);
        defer self.sessions_mutex.unlock(self.io);

        const session = self.sessions.get(token) orelse return error.InvalidSession;
        const now = std.Io.Clock.now(.real, self.io).toMilliseconds();

        if (!session.isValid(now)) {
            return error.SessionExpired;
        }

        return session;
    }

    /// Enforces a coarse [`PermissionType`] against a session's capabilities.
    ///
    /// Re-checks expiry first (`error.Unauthenticated` if the session has lapsed
    /// since it was validated), then maps the requested action to the matching
    /// [`Permission`] flag and returns `error.PermissionDenied` if it is not set.
    /// This is the role-level gate; per-object grants are checked by
    /// [`SecurityManager.checkObjectPermission`]. Lock-free: reads only the
    /// caller-supplied session snapshot and the clock.
    pub fn checkPermission(self: *SecurityManager, session: *const Session, permission_type: PermissionType) !void {
        const now = std.Io.Clock.now(.real, self.io).toMilliseconds();
        if (!session.isValid(now)) return error.Unauthenticated;
        const has_permission = switch (permission_type) {
            .read => session.permissions.can_read,
            .write => session.permissions.can_write,
            .delete => session.permissions.can_delete,
            .admin => session.permissions.can_admin,
        };

        if (!has_permission) {
            return error.PermissionDenied;
        }
    }

    /// Removes a session by token (logout), freeing its owned storage.
    ///
    /// A no-op if the token is not present. Takes `sessions_mutex`. After this,
    /// [`SecurityManager.validateSession`] on the same token returns
    /// `error.InvalidSession`.
    pub fn revokeSession(self: *SecurityManager, token: [32]u8) !void {
        self.sessions_mutex.lockUncancelable(self.io);
        defer self.sessions_mutex.unlock(self.io);

        if (self.sessions.fetchRemove(token)) |kv| {
            var session = kv.value;
            session.deinit(self.allocator);
        }
    }

    /// Records one failed login for `key` and applies exponential lockout.
    ///
    /// Increments (or creates) the [`LoginAttemptInfo`] for the throttle key.
    /// Once `failed_count` reaches `max_failed_attempts`, sets `lockout_until`
    /// to now plus `lockout_duration_ms` multiplied by `lockout_multiplier` once
    /// per failure beyond the threshold, clamped to one hour. To bound memory
    /// under a distributed attack it refuses to insert a NEW key once the table
    /// holds 100,000 entries (existing keys still update). Takes
    /// `login_attempts_mutex`. Allocation failures are swallowed (the attempt is
    /// simply not recorded) so a login path never fails for this reason.
    fn recordFailedAttempt(self: *SecurityManager, key: []const u8, now: i64) void {
        self.login_attempts_mutex.lockUncancelable(self.io);
        defer self.login_attempts_mutex.unlock(self.io);
        if (self.login_attempts.count() >= 100_000 and !self.login_attempts.contains(key)) return;
        const result = self.login_attempts.getOrPut(key) catch return;
        if (!result.found_existing) {
            result.key_ptr.* = self.allocator.dupe(u8, key) catch return;
            result.value_ptr.* = LoginAttemptInfo{
                .failed_count = 1,
                .last_failed_at = now,
                .lockout_until = 0,
            };
        } else {
            result.value_ptr.failed_count += 1;
            result.value_ptr.last_failed_at = now;
        }

        const count = result.value_ptr.failed_count;
        if (count >= self.max_failed_attempts) {
            const excess = count - self.max_failed_attempts;
            var multiplied_duration = self.lockout_duration_ms;
            for (0..excess) |_| {
                multiplied_duration = @min(
                    multiplied_duration * self.lockout_multiplier,
                    3600_000,
                );
            }
            result.value_ptr.lockout_until = now + multiplied_duration;
        }
    }

    /// Clears the lockout/failure record for `key`, taking the lock itself.
    ///
    /// Called on a successful login. Wrapper around
    /// [`SecurityManager.clearFailedAttemptsLocked`] that acquires
    /// `login_attempts_mutex`; use the `Locked` variant when the lock is already
    /// held.
    fn clearFailedAttempts(self: *SecurityManager, key: []const u8) void {
        self.login_attempts_mutex.lockUncancelable(self.io);
        defer self.login_attempts_mutex.unlock(self.io);
        self.clearFailedAttemptsLocked(key);
    }

    /// Removes `key` from `login_attempts`, freeing its duplicated key string.
    ///
    /// Caller must already hold `login_attempts_mutex`. A no-op if the key is
    /// absent. Invoked both on success and when a stale lockout window has
    /// elapsed inside [`SecurityManager.authenticate`].
    fn clearFailedAttemptsLocked(self: *SecurityManager, key: []const u8) void {
        if (self.login_attempts.fetchRemove(key)) |entry| {
            self.allocator.free(entry.key);
        }
    }

    /// Reports whether `ip` is a loopback address, which is exempt from
    /// throttling.
    ///
    /// Matches the IPv4 `127.0.0.0/8` prefix and both the compact (`::1`) and
    /// fully-expanded IPv6 loopback forms. Local connections are trusted, so a
    /// process on the same host cannot be locked out by repeated bad passwords.
    fn isLoopbackIp(ip: []const u8) bool {
        return std.mem.startsWith(u8, ip, "127.") or
            std.mem.eql(u8, ip, "::1") or
            std.mem.eql(u8, ip, "00000000000000000000000000000001");
    }

    /// Derives a 32-byte Argon2id verifier from a password and salt.
    ///
    /// Uses Argon2id with `t = 2` passes, `m = 65536` KiB (64 MiB) of memory,
    /// and `p = 1` lane. These parameters are the cost of every login (and every
    /// throwaway hash on a failed lookup), so they deliberately dominate the
    /// authentication latency to make offline cracking expensive. The memory
    /// cost means concurrent logins are memory-heavy; this is intentional.
    /// Returns the raw hash bytes; the caller compares them in constant time.
    pub fn hashKey(self: *SecurityManager, key: []const u8, salt: [32]u8) ![32]u8 {
        var hash: [32]u8 = undefined;
        // Production uses a deliberately expensive 64 MiB Argon2id so offline
        // cracking is costly. The test build runs this on every Database.open
        // (to seed the default admin), so a 64 MiB KDF per open makes the whole
        // suite slow enough to trip the build runner. Under `builtin.is_test`
        // use cheap parameters: the KDF is exercised for correctness, not cost.
        const params: std.crypto.pwhash.argon2.Params = if (builtin.is_test)
            .{ .t = 1, .m = 64, .p = 1 }
        else
            .{ .t = 2, .m = 65536, .p = 1 };
        try std.crypto.pwhash.argon2.kdf(
            self.allocator,
            &hash,
            key,
            &salt,
            params,
            .argon2id,
            self.io,
        );
        return hash;
    }

    /// Rebuilds the `roles` and `privileges` caches from the system tables.
    ///
    /// Scans `sys.roles` and `sys.privileges` independently; each block is
    /// skipped if its table root is absent. As with
    /// [`SecurityManager.loadUsers`], only the latest, non-deleted
    /// (`xmax == 0`) MVCC version of each row is taken. The three built-in role
    /// names are re-seeded before the `sys.roles` scan so they always exist even
    /// if the table is empty. Takes `roles_mutex` then `privileges_mutex` (one
    /// at a time) and replaces the corresponding cache contents. Safe to call
    /// repeatedly to refresh after `GRANT` / `REVOKE`.
    pub fn loadRolesAndPrivileges(self: *SecurityManager, db: *Database) !void {
        if (db.table_roots.get("sys.roles")) |roles_root| {
            var tree = try BPlusTree.init(db.pool, roles_root, db.allocator);
            defer tree.deinit();

            var it = try tree.iterator();
            defer it.deinit();

            self.roles_mutex.lockUncancelable(self.io);
            defer self.roles_mutex.unlock(self.io);

            var roles_iter = self.roles.iterator();
            while (roles_iter.next()) |entry| {
                self.allocator.free(entry.key_ptr.*);
            }
            self.roles.clearRetainingCapacity();

            try self.roles.put(try self.allocator.dupe(u8, "admin"), {});
            try self.roles.put(try self.allocator.dupe(u8, "read_write"), {});
            try self.roles.put(try self.allocator.dupe(u8, "read_only"), {});

            while (try it.next()) |cell| {
                const versions = try db.reconstructVersionChain(cell.value, self.allocator);
                defer {
                    for (versions) |*v| v.deinit(self.allocator);
                    self.allocator.free(versions);
                }

                if (versions.len == 0) continue;
                const latest = versions[versions.len - 1];
                if (latest.xmax > 0) continue;

                const role_table = db.catalog.getTable("sys.roles") orelse continue;
                const reader = RowReader.init(role_table, latest.fixed, latest.heap);
                const role_name = try reader.readToString(self.allocator, "role_name");
                errdefer self.allocator.free(role_name);

                const role_key = try self.allocator.dupe(u8, role_name);
                try self.roles.put(role_key, {});
                self.allocator.free(role_name);
            }
        }

        if (db.table_roots.get("sys.privileges")) |privs_root| {
            var tree = try BPlusTree.init(db.pool, privs_root, db.allocator);
            defer tree.deinit();

            var it = try tree.iterator();
            defer it.deinit();

            self.privileges_mutex.lockUncancelable(self.io);
            defer self.privileges_mutex.unlock(self.io);

            for (self.privileges.items) |*p| {
                p.deinit(self.allocator);
            }
            self.privileges.clearRetainingCapacity();

            while (try it.next()) |cell| {
                const versions = try db.reconstructVersionChain(cell.value, self.allocator);
                defer {
                    for (versions) |*v| v.deinit(self.allocator);
                    self.allocator.free(versions);
                }

                if (versions.len == 0) continue;
                const latest = versions[versions.len - 1];
                if (latest.xmax > 0) continue;

                const priv_table = db.catalog.getTable("sys.privileges") orelse continue;
                const reader = RowReader.init(priv_table, latest.fixed, latest.heap);
                const grantee = try reader.readToString(self.allocator, "grantee");
                errdefer self.allocator.free(grantee);
                const object_name = try reader.readToString(self.allocator, "object_name");
                errdefer {
                    self.allocator.free(grantee);
                    self.allocator.free(object_name);
                }
                const privilege = try reader.readToString(self.allocator, "privilege");
                errdefer {
                    self.allocator.free(grantee);
                    self.allocator.free(object_name);
                    self.allocator.free(privilege);
                }

                try self.privileges.append(self.allocator, Privilege{
                    .grantee = grantee,
                    .object_name = object_name,
                    .privilege = privilege,
                });
            }
        }
    }

    /// Transitively collects every role `grantee` belongs to into `resolved`.
    ///
    /// Walks role-membership rows (those [`Privilege`] entries whose
    /// `object_name` is empty, treating `privilege` as the parent role) depth
    /// first. The `resolved` set doubles as the visited guard, so a cycle in the
    /// role graph terminates instead of recursing forever. The caller seeds
    /// `resolved` and owns it; on return it contains `grantee` plus all
    /// reachable roles. Expects `privileges_mutex` to be held by the caller
    /// (it reads `self.privileges` without locking).
    pub fn resolveRoles(self: *SecurityManager, grantee: []const u8, resolved: *std.StringHashMap(void)) !void {
        if (resolved.contains(grantee)) return;
        try resolved.put(grantee, {});

        for (self.privileges.items) |p| {
            if (std.mem.eql(u8, p.grantee, grantee) and p.object_name.len == 0) {
                try self.resolveRoles(p.privilege, resolved);
            }
        }
    }

    /// Reports whether `username` may perform `required_privilege` on
    /// `object_name`.
    ///
    /// Returns true immediately when the manager is disabled, or when the user's
    /// [`Role`] is `admin`. Otherwise it resolves the user's full set of
    /// principals (the username, all roles reachable from it via
    /// [`SecurityManager.resolveRoles`], and the user's default role name) and
    /// scans `privileges` for any grant to one of those principals on the exact
    /// object with either the exact privilege or `ALL`. A grant match returns
    /// true; nothing matching returns false.
    ///
    /// Reads the `users` cache under `users_mutex` (released before the grant
    /// walk), then holds `privileges_mutex` for the resolution and scan. On any
    /// internal allocation failure it fails closed (returns false). Callers must
    /// hold neither lock.
    pub fn hasObjectPrivilege(self: *SecurityManager, username: []const u8, object_name: []const u8, required_privilege: []const u8) bool {
        if (!self.enabled) return true;

        self.users_mutex.lockUncancelable(self.io);
        const user = self.users.get(username);
        self.users_mutex.unlock(self.io);

        if (user) |u| {
            if (u.role == .admin) return true;
        }

        var resolved = std.StringHashMap(void).init(self.allocator);
        defer resolved.deinit();

        self.privileges_mutex.lockUncancelable(self.io);
        defer self.privileges_mutex.unlock(self.io);

        self.resolveRoles(username, &resolved) catch return false;

        if (user) |u| {
            const default_role_str = u.role.toString();
            if (!std.mem.eql(u8, default_role_str, "none")) {
                if (!resolved.contains(default_role_str)) {
                    resolved.put(default_role_str, {}) catch return false;
                    self.resolveRoles(default_role_str, &resolved) catch {};
                }
            }
        }



        var key_it = resolved.keyIterator();
        while (key_it.next()) |k| {
            const principal = k.*;
            for (self.privileges.items) |p| {
                if (std.mem.eql(u8, p.grantee, principal)) {
                    if (std.mem.eql(u8, p.object_name, object_name)) {
                        if (std.mem.eql(u8, p.privilege, required_privilege) or std.mem.eql(u8, p.privilege, "ALL")) {
                            return true;
                        }
                    }
                }
            }
        }

        return false;
    }

    /// Enforces per-object authorisation for a statement on `object_name`.
    ///
    /// The full authorisation gate, layering object grants over role
    /// capabilities:
    ///
    ///   1. `error.Unauthenticated` if the session has expired.
    ///   2. Allow immediately if the session has `can_admin`.
    ///   3. `error.PermissionDenied` for any `sys.*` object: system tables are
    ///      reachable only by admins, never through grants.
    ///   4. Allow if [`SecurityManager.hasObjectPrivilege`] finds a matching
    ///      grant for the session's user on this object.
    ///   5. Otherwise fall back to the coarse role capability: `SELECT` needs
    ///      `can_read`, and `INSERT` / `UPDATE` / `DELETE` need `can_write`; any
    ///      other verb has no global fallback. `error.PermissionDenied` if the
    ///      required capability is absent.
    pub fn checkObjectPermission(self: *SecurityManager, session: *const Session, object_name: []const u8, privilege: []const u8) !void {
        const now = std.Io.Clock.now(.real, self.io).toMilliseconds();
        if (!session.isValid(now)) return error.Unauthenticated;

        if (session.permissions.can_admin) return;

        if (std.mem.startsWith(u8, object_name, "sys.")) {
            return error.PermissionDenied;
        }

        if (self.hasObjectPrivilege(session.username, object_name, privilege)) {
            return;
        }

        const has_global = if (std.mem.eql(u8, privilege, "SELECT"))
            session.permissions.can_read
        else if (std.mem.eql(u8, privilege, "INSERT") or std.mem.eql(u8, privilege, "UPDATE") or std.mem.eql(u8, privilege, "DELETE"))
            session.permissions.can_write
        else
            false;

        if (!has_global) {
            return error.PermissionDenied;
        }
    }
};

/// Encodes bytes as a lowercase hexadecimal string.
///
/// Allocates and returns `data.len * 2` bytes the caller owns. Used to serialise
/// the password hash and salt into the `hex(hash):hex(salt)` form stored in the
/// `sys.users` row; inverse of [`hexDecode`].
pub fn hexEncode(allocator: Allocator, data: []const u8) ![]u8 {
    const hex_chars = "0123456789abcdef";
    const result = try allocator.alloc(u8, data.len * 2);
    for (data, 0..) |byte, i| {
        result[i * 2] = hex_chars[byte >> 4];
        result[i * 2 + 1] = hex_chars[byte & 0x0f];
    }
    return result;
}

/// Decodes a hexadecimal string into its raw bytes.
///
/// Allocates and returns `hex.len / 2` bytes the caller owns. Returns
/// `error.InvalidHexLength` for an odd-length input and `error.InvalidHexChar`
/// for any non-hex digit (via [`hexCharToValue`]). Used by
/// [`SecurityManager.loadUsers`] to recover the stored hash and salt; inverse of
/// [`hexEncode`].
pub fn hexDecode(allocator: Allocator, hex: []const u8) ![]u8 {
    if (hex.len % 2 != 0) return error.InvalidHexLength;
    const result = try allocator.alloc(u8, hex.len / 2);
    errdefer allocator.free(result);
    for (0..hex.len / 2) |i| {
        const high = hexCharToValue(hex[i * 2]) orelse return error.InvalidHexChar;
        const low = hexCharToValue(hex[i * 2 + 1]) orelse return error.InvalidHexChar;
        result[i] = (high << 4) | low;
    }
    return result;
}

/// Maps a single hex digit to its 0-15 value, or null if not a hex digit.
///
/// Accepts both upper- and lowercase `a`-`f`. The null return is what lets
/// [`hexDecode`] and [`parseTokenHex`] surface `error.InvalidHexChar` on bad
/// input.
fn hexCharToValue(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

/// Parses a 64-character hex string into a fixed 32-byte session token.
///
/// Unlike [`hexDecode`] this allocates nothing and returns a fixed array, since
/// a [`Session.token`] is always exactly 32 bytes. Returns
/// `error.InvalidTokenLength` unless the input is exactly 64 characters, or
/// `error.InvalidHexChar` on a bad digit. Used to turn the client's presented
/// hex token back into the key for the `sessions` map.
pub fn parseTokenHex(hex: []const u8) ![32]u8 {
    if (hex.len != 64) return error.InvalidTokenLength;
    var token: [32]u8 = undefined;
    for (0..32) |i| {
        const high = hexCharToValue(hex[i * 2]) orelse return error.InvalidHexChar;
        const low = hexCharToValue(hex[i * 2 + 1]) orelse return error.InvalidHexChar;
        token[i] = (high << 4) | low;
    }
    return token;
}
