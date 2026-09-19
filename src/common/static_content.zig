//! In-memory static-content store for the kaidb server.
//!
//! kaidb is primarily a binary-protocol database, but the server can also
//! serve a small bundle of static web assets (an admin/status UI, favicons,
//! scripts, stylesheets) directly over HTTP. This module is that asset store:
//! it slurps a whole directory tree into RAM once at start-up and then answers
//! request-path lookups from the in-memory map, with no per-request disk I/O.
//!
//! The design is deliberately "load once, serve many". [`StaticContentStore`]
//! owns every byte it hands out, file contents, normalised path keys, and
//! pre-computed ETag strings are all heap copies owned by the store's
//! allocator and freed together in [`StaticContentStore.deinit`]. The only
//! borrowed data is the MIME-type string, which is always a compile-time
//! constant returned by [`getMimeType`], so it needs no ownership tracking.
//!
//! Two representation decisions matter for callers:
//!
//!   * **Path normalisation.** Keys are stored web-style: a leading `/` is
//!     prepended and every OS path separator (`\` on Windows) is rewritten to
//!     `/`, so a file at `assets\app.css` on disk is served as `/assets/app.css`
//!     regardless of host platform. [`StaticContentStore.lookup`] mirrors this
//!     by mapping `/` to `/index.html` and stripping any `?query` or `#fragment`
//!     before matching, so a raw request-target string can be looked up as-is.
//!
//!   * **ETags are content hashes.** Each file's ETag is a quoted hex Wyhash of
//!     its bytes, computed once at load time. That lets the HTTP layer answer
//!     conditional requests (`If-None-Match`) with a stable, content-derived
//!     validator without re-reading the file. The hash is not cryptographic; it
//!     is a cache validator, not an integrity check.
//!
//! Loading is best-effort and non-fatal: a missing content directory logs a
//! warning and leaves the store empty, and an individual file that fails to
//! read is logged and skipped rather than aborting the whole load. This keeps
//! the database server starting even when the optional web bundle is absent.

const std = @import("std");
/// Alias for the Zig async/blocking I/O interface used to open the content
/// directory and read files ([`StaticContentStore.loadDir`] takes an `Io`).
const Io = std.Io;
/// Alias for the standard allocator interface; the store keeps one to own all
/// of its copied file bytes, keys, and ETag strings.
const Allocator = std.mem.Allocator;

/// One cached static asset: its content type, its bytes, and its ETag.
///
/// A `StaticFile` is a plain value returned by [`StaticContentStore.lookup`].
/// The `data` and `etag` slices are owned by the enclosing
/// [`StaticContentStore`] and stay valid only until that store is
/// [`StaticContentStore.deinit`]'d; the `mime_type` slice is a static constant
/// from [`getMimeType`] and outlives the store.
pub const StaticFile = struct {
    /// HTTP `Content-Type` value for this asset, including any `; charset=`
    /// parameter. Always a compile-time constant from [`getMimeType`], so it is
    /// borrowed, not owned.
    mime_type: []const u8,
    /// The raw file bytes, served verbatim as the response body. Heap-owned by
    /// the store's allocator and freed in [`StaticContentStore.deinit`].
    data: []const u8,
    /// Pre-computed ETag: a quoted hex Wyhash of `data` (e.g. `"1a2b3c"`).
    /// Used as an HTTP cache validator. Heap-owned by the store's allocator.
    etag: []const u8,
};

/// Owns the in-memory bundle of static files and serves them by request path.
///
/// Populate it once via [`StaticContentStore.loadDir`], then answer lookups
/// with [`StaticContentStore.lookup`]. The store owns every key and value it
/// holds; call [`StaticContentStore.deinit`] to release them. It is not
/// internally synchronised: build it fully before sharing it, then treat it as
/// read-only across request handlers.
pub const StaticContentStore = struct {
    /// Allocator that owns all copied file bytes, normalised path keys, and
    /// ETag strings. The same allocator must be passed for the store's whole
    /// lifetime; [`StaticContentStore.deinit`] frees everything through it.
    allocator: Allocator,
    /// Maps a normalised web path (e.g. `/index.html`) to its cached
    /// [`StaticFile`]. Both the key strings and the value's `data`/`etag` are
    /// heap-owned; the map itself is the unmanaged variant, so the allocator is
    /// threaded through each mutating call.
    files: std.StringHashMapUnmanaged(StaticFile) = .{},

    /// Creates an empty store bound to `allocator`.
    ///
    /// No files are loaded yet; call [`StaticContentStore.loadDir`] to populate
    /// it. The returned value must eventually be passed to
    /// [`StaticContentStore.deinit`] to avoid leaking loaded content.
    pub fn init(allocator: Allocator) StaticContentStore {
        return .{
            .allocator = allocator,
            .files = .{},
        };
    }

    /// Frees every heap allocation the store owns and destroys the map.
    ///
    /// Walks each entry releasing the normalised key string, the file `data`,
    /// and the `etag` (the `mime_type` is a static constant and must NOT be
    /// freed), then tears down the hash map's own storage. After this the store
    /// must not be used again.
    pub fn deinit(self: *StaticContentStore) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.data);
            self.allocator.free(entry.value_ptr.etag);
        }
        self.files.deinit(self.allocator);
    }

    /// Recursively loads every regular file under `dir_path` into the store.
    ///
    /// Walks the directory tree, and for each file: reads the whole content
    /// into an allocation, derives the MIME type from the extension via
    /// [`getMimeType`], computes a quoted-hex Wyhash ETag, and normalises the
    /// relative path to a web path (leading `/`, backslashes rewritten to `/`)
    /// before inserting it into [`StaticContentStore.files`].
    ///
    /// Failure handling is intentionally lenient so an optional asset bundle
    /// never blocks server start-up: if `dir_path` cannot be opened the store is
    /// left empty and a warning is logged, and an individual file that fails to
    /// read is logged and skipped while the rest still load. The `errdefer`s on
    /// `data`, `etag`, and `norm_path` free those buffers if a later step in the
    /// SAME iteration errors before the successful `files.put` takes ownership,
    /// avoiding a leak on the propagating-error path.
    ///
    /// Directory entries that are not regular files (subdirectories, symlinks)
    /// are skipped; the walker still descends into subdirectories to find files.
    pub fn loadDir(self: *StaticContentStore, io: Io, dir_path: []const u8) !void {
        var dir = Io.Dir.openDir(.cwd(), io, dir_path, .{ .iterate = true }) catch |err| {
            std.log.warn("Static content directory '{s}' not found: {any}. Skipping static loading.", .{ dir_path, err });
            return;
        };
        defer dir.close(io);

        var walker = try dir.walk(self.allocator);
        defer walker.deinit();

        while (try walker.next(io)) |entry| {
            if (entry.kind != .file) continue;

            const file_path = entry.path;
            const data = Io.Dir.readFileAlloc(dir, io, file_path, self.allocator, .unlimited) catch |err| {
                std.log.err("Failed to read static file '{s}': {any}", .{ file_path, err });
                continue;
            };
            errdefer self.allocator.free(data);

            const mime = getMimeType(file_path);

            var h = std.hash.Wyhash.init(0);
            h.update(data);
            const etag = try std.fmt.allocPrint(self.allocator, "\"{x}\"", .{h.final()});
            errdefer self.allocator.free(etag);

            const norm_path = try std.fmt.allocPrint(self.allocator, "/{s}", .{file_path});
            errdefer self.allocator.free(norm_path);
            for (norm_path) |*c| {
                if (c.* == '\\') c.* = '/';
            }

            try self.files.put(self.allocator, norm_path, .{
                .mime_type = mime,
                .data = data,
                .etag = etag,
            });
            std.log.info("Loaded static file: {s} ({s}, {d} bytes)", .{ norm_path, mime, data.len });
        }
    }

    /// Looks up a cached file by request path, or returns `null` if none.
    ///
    /// Applies the request-side normalisation that mirrors how keys are stored:
    /// a bare `/` is treated as `/index.html` (the directory-index default), and
    /// any `?query` or `#fragment` suffix is stripped so a raw HTTP request
    /// target matches the stored path. The returned [`StaticFile`] borrows from
    /// the store and is only valid until [`StaticContentStore.deinit`].
    ///
    /// Takes `self` by const pointer: lookups do not mutate the store and may
    /// run concurrently once loading is complete.
    pub fn lookup(self: *const StaticContentStore, path: []const u8) ?StaticFile {
        var lookup_path = path;
        if (std.mem.eql(u8, lookup_path, "/")) {
            lookup_path = "/index.html";
        }
        if (std.mem.indexOfScalar(u8, lookup_path, '?')) |idx| {
            lookup_path = lookup_path[0..idx];
        }
        if (std.mem.indexOfScalar(u8, lookup_path, '#')) |idx| {
            lookup_path = lookup_path[0..idx];
        }
        return self.files.get(lookup_path);
    }
};

/// Maps a file path's extension to an HTTP `Content-Type` string.
///
/// Returns a compile-time constant (so the result is borrowed, never owned),
/// with a `; charset=utf-8` parameter on the text-based types. Recognises the
/// common web asset extensions (HTML, CSS, JS/MJS, JSON, images, SVG, ICO,
/// TXT); anything unrecognised falls back to `application/octet-stream`,
/// the safe generic binary type. Used by [`StaticContentStore.loadDir`] to
/// stamp each cached [`StaticFile.mime_type`].
pub fn getMimeType(path: []const u8) []const u8 {
    const ext = std.fs.path.extension(path);
    if (std.mem.eql(u8, ext, ".html") or std.mem.eql(u8, ext, ".htm")) return "text/html; charset=utf-8";
    if (std.mem.eql(u8, ext, ".css")) return "text/css; charset=utf-8";
    if (std.mem.eql(u8, ext, ".js") or std.mem.eql(u8, ext, ".mjs")) return "application/javascript; charset=utf-8";
    if (std.mem.eql(u8, ext, ".json")) return "application/json; charset=utf-8";
    if (std.mem.eql(u8, ext, ".png")) return "image/png";
    if (std.mem.eql(u8, ext, ".jpg") or std.mem.eql(u8, ext, ".jpeg")) return "image/jpeg";
    if (std.mem.eql(u8, ext, ".gif")) return "image/gif";
    if (std.mem.eql(u8, ext, ".svg")) return "image/svg+xml; charset=utf-8";
    if (std.mem.eql(u8, ext, ".ico")) return "image/x-icon";
    if (std.mem.eql(u8, ext, ".txt")) return "text/plain; charset=utf-8";
    return "application/octet-stream";
}
