//! Orders benchmark: a NovaDB port of the MongoDB `mongodb-perf-compare` test.
//!
//! It generates N synthetic orders from the same source data (employees,
//! customers, products), loads them into NovaDB in either SQL or document mode,
//! indexes EmployeeID / CustomerID / TotalDue, and runs the same ten queries the
//! reference runs against MongoDB (point/range filters, sort+limit, `$in`,
//! range-between, count, and a group-by average). The point is a like-for-like
//! SQL-vs-document comparison on the identical data and query set.
//!
//! Data source directory (the reference's `json/` folder) defaults to
//! `DEFAULT_DATA_DIR`; override with `--data_dir=...`. Order count comes from
//! `--record_count` (default whatever the config carries).

const std = @import("std");
const Io = std.Io;
const btree = @import("btree");
const bson = btree.bson_pkg;
const config_mod = @import("config.zig");
const Client = @import("client.zig").Client;

pub const DEFAULT_DATA_DIR = "/Users/kamlesh/zsh/mongodb-perf-compare/json";

pub const Mode = enum { sql, doc };

const Detail = struct {
    SalesOrderDetailID: i32,
    ProductID: i64,
    OrderQty: i32,
    UnitPrice: f64,
    UnitPriceDiscount: f64,
    LineTotal: f64,
};

/// One generated order in document shape (also the source for the SQL columns).
const OrderDoc = struct {
    OrderDate: []const u8,
    DueDate: []const u8,
    ShipDate: []const u8,
    EmployeeID: i64,
    CustomerID: i64,
    SubTotal: f64,
    TaxAmt: f64,
    Freight: f64,
    TotalDue: f64,
    SalesOrderDetails: []const Detail,
};

const Sources = struct {
    emp_ids: []i64,
    cust_ids: []i64,
    prod_ids: []i64,
    prod_prices: []f64,
};

fn readFile(a: std.mem.Allocator, io: Io, path: []const u8) ![]u8 {
    return Io.Dir.readFileAlloc(.cwd(), io, path, a, .unlimited);
}

/// Pull an integer out of a JSON value that may be `.integer` or `.float`.
fn jsonInt(v: std.json.Value) i64 {
    return switch (v) {
        .integer => |i| i,
        .float => |fl| @intFromFloat(fl),
        else => 0,
    };
}

fn jsonFloat(v: std.json.Value) f64 {
    return switch (v) {
        .integer => |i| @floatFromInt(i),
        .float => |fl| fl,
        else => 0,
    };
}

fn collectField(a: std.mem.Allocator, arr: std.json.Array, field: []const u8) ![]i64 {
    var out = std.ArrayList(i64).empty;
    for (arr.items) |item| {
        if (item != .object) continue;
        if (item.object.get(field)) |v| try out.append(a, jsonInt(v));
    }
    return out.toOwnedSlice(a);
}

fn loadSources(a: std.mem.Allocator, io: Io, dir: []const u8) !Sources {
    const emp_path = try std.fmt.allocPrint(a, "{s}/employees.json", .{dir});
    defer a.free(emp_path);
    const cust_path = try std.fmt.allocPrint(a, "{s}/customers.json", .{dir});
    defer a.free(cust_path);
    const prod_path = try std.fmt.allocPrint(a, "{s}/products.json", .{dir});
    defer a.free(prod_path);

    const emp_bytes = try readFile(a, io, emp_path);
    defer a.free(emp_bytes);
    const cust_bytes = try readFile(a, io, cust_path);
    defer a.free(cust_bytes);
    const prod_bytes = try readFile(a, io, prod_path);
    defer a.free(prod_bytes);

    const emp_p = try std.json.parseFromSlice(std.json.Value, a, emp_bytes, .{});
    defer emp_p.deinit();
    const cust_p = try std.json.parseFromSlice(std.json.Value, a, cust_bytes, .{});
    defer cust_p.deinit();
    const prod_p = try std.json.parseFromSlice(std.json.Value, a, prod_bytes, .{});
    defer prod_p.deinit();

    const emp_ids = try collectField(a, emp_p.value.array, "EmployeeID");
    const cust_ids = try collectField(a, cust_p.value.array, "CustomerID");

    var pids = std.ArrayList(i64).empty;
    var prices = std.ArrayList(f64).empty;
    for (prod_p.value.array.items) |item| {
        if (item != .object) continue;
        const pid = item.object.get("ProductID") orelse continue;
        const price = item.object.get("ListPrice") orelse continue;
        try pids.append(a, jsonInt(pid));
        try prices.append(a, jsonFloat(price));
    }
    return .{
        .emp_ids = emp_ids,
        .cust_ids = cust_ids,
        .prod_ids = try pids.toOwnedSlice(a),
        .prod_prices = try prices.toOwnedSlice(a),
    };
}

fn round4(x: f64) f64 {
    return @round(x * 10000.0) / 10000.0;
}

/// Generate one order into `details_buf` (up to 8 items) and return the doc.
fn genOrder(rng: std.Random, s: Sources, details_buf: []Detail, date_buf: []u8) OrderDoc {
    const emp = s.emp_ids[rng.uintLessThan(usize, s.emp_ids.len)];
    const cust = s.cust_ids[rng.uintLessThan(usize, s.cust_ids.len)];
    const num_items = rng.intRangeAtMost(usize, 1, 8);
    var sub_total: f64 = 0;
    for (0..num_items) |i| {
        const pi = rng.uintLessThan(usize, s.prod_ids.len);
        const price = s.prod_prices[pi];
        const qty = rng.intRangeAtMost(i32, 1, 10);
        const discount: f64 = if (rng.float(f64) < 0.2) round4(0.01 + rng.float(f64) * 0.24) else 0;
        const line = round4(price * @as(f64, @floatFromInt(qty)) * (1.0 - discount));
        sub_total += line;
        details_buf[i] = .{
            .SalesOrderDetailID = @intCast(i + 1),
            .ProductID = s.prod_ids[pi],
            .OrderQty = qty,
            .UnitPrice = price,
            .UnitPriceDiscount = discount,
            .LineTotal = line,
        };
    }
    const tax = round4(sub_total * 0.08);
    const freight = round4(sub_total * 0.025);
    // A plausible date string; dates are never queried so the exact value is
    // irrelevant, only that the column/field is populated and realistically sized.
    const mo = rng.intRangeAtMost(u32, 1, 12);
    const day = rng.intRangeAtMost(u32, 1, 28);
    const yr = rng.intRangeAtMost(u32, 2011, 2014);
    const ds = std.fmt.bufPrint(date_buf, "{d}/{d}/{d}", .{ mo, day, yr }) catch date_buf[0..0];
    return .{
        .OrderDate = ds,
        .DueDate = ds,
        .ShipDate = ds,
        .EmployeeID = emp,
        .CustomerID = cust,
        .SubTotal = round4(sub_total),
        .TaxAmt = tax,
        .Freight = freight,
        .TotalDue = round4(sub_total + tax + freight),
        .SalesOrderDetails = details_buf[0..num_items],
    };
}

const milliTimestamp = @import("metrics.zig").milliTimestamp;

fn nowMs() i64 {
    return milliTimestamp();
}

const CREATE_SQL =
    \\CREATE TABLE orders (id INTEGER PRIMARY KEY, order_date TEXT, due_date TEXT, ship_date TEXT, employee_id INTEGER, customer_id INTEGER, sub_total DOUBLE, tax_amt DOUBLE, freight DOUBLE, total_due DOUBLE, details TEXT)
;

pub fn run(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig, mode: Mode, data_dir: []const u8, do_load: bool) !void {
    var threaded: Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();

    const sources = try loadSources(allocator, io, data_dir);
    std.debug.print("[{s}] sources: {d} employees, {d} customers, {d} products\n", .{ @tagName(mode), sources.emp_ids.len, sources.cust_ids.len, sources.prod_ids.len });

    switch (mode) {
        .sql => try runSql(allocator, io, cfg, sources, do_load),
        .doc => return error.DocumentModelRemoved,
    }
}

fn runSql(allocator: std.mem.Allocator, io: Io, cfg: config_mod.BenchmarkConfig, s: Sources, do_load: bool) !void {
    const client = try Client.init(allocator, io, cfg.host, cfg.port, cfg.tls);
    defer client.deinit();
    try client.login(cfg.uid, cfg.key);

    if (do_load) {
        _ = client.execute("DROP TABLE orders") catch {};
        const cr = try client.execute(CREATE_SQL);
        allocator.free(cr);

        const n = cfg.record_count;
        const batch: u64 = 100;
        var prng = std.Random.DefaultPrng.init(0xABCDEF12);
        const rng = prng.random();
        var details: [8]Detail = undefined;
        var date_buf: [16]u8 = undefined;

        const t0 = nowMs();
        var i: u64 = 0;
        var sb = std.ArrayList(u8).empty;
        defer sb.deinit(allocator);
        while (i < n) {
            sb.clearRetainingCapacity();
            try sb.appendSlice(allocator, "INSERT INTO orders (id,order_date,due_date,ship_date,employee_id,customer_id,sub_total,tax_amt,freight,total_due,details) VALUES ");
            const upto = @min(i + batch, n);
            var first = true;
            while (i < upto) : (i += 1) {
                const o = genOrder(rng, s, &details, &date_buf);
                if (!first) try sb.append(allocator, ',');
                first = false;
                // details as a compact JSON string (never queried; realistic size)
                var dj = std.ArrayList(u8).empty;
                defer dj.deinit(allocator);
                var eltbuf: [128]u8 = undefined;
                try dj.append(allocator, '[');
                for (o.SalesOrderDetails, 0..) |d, k| {
                    if (k != 0) try dj.append(allocator, ',');
                    const elt = try std.fmt.bufPrint(&eltbuf, "{{\"ProductID\":{d},\"OrderQty\":{d},\"UnitPrice\":{d:.4},\"LineTotal\":{d:.4}}}", .{ d.ProductID, d.OrderQty, d.UnitPrice, d.LineTotal });
                    try dj.appendSlice(allocator, elt);
                }
                try dj.append(allocator, ']');
                var rowbuf: [512]u8 = undefined;
                const head = try std.fmt.bufPrint(&rowbuf, "({d},'{s}','{s}','{s}',{d},{d},{d:.4},{d:.4},{d:.4},{d:.4},'", .{ i, o.OrderDate, o.DueDate, o.ShipDate, o.EmployeeID, o.CustomerID, o.SubTotal, o.TaxAmt, o.Freight, o.TotalDue });
                try sb.appendSlice(allocator, head);
                try sb.appendSlice(allocator, dj.items);
                try sb.appendSlice(allocator, "')");
            }
            const rep = try client.execute(sb.items);
            allocator.free(rep);
            if (i % 100000 < batch) std.debug.print("  loaded {d}/{d}\n", .{ i, n });
        }
        const load_ms = nowMs() - t0;
        std.debug.print("[sql] loaded {d} orders in {d} ms ({d:.0}/s)\n", .{ n, load_ms, @as(f64, @floatFromInt(n)) * 1000.0 / @as(f64, @floatFromInt(@max(load_ms, 1))) });

        const it0 = nowMs();
        for ([_][]const u8{
            "CREATE INDEX idx_emp ON orders (employee_id)",
            "CREATE INDEX idx_cust ON orders (customer_id)",
            "CREATE INDEX idx_total ON orders (total_due)",
            // Composite covering index for Q3 (WHERE employee_id = ? [AND
            // total_due > ?] ORDER BY total_due): the planner scans just the
            // employee_id prefix in total_due order, so it streams to LIMIT
            // instead of collecting ~590k rows and sorting.
            "CREATE INDEX idx_emp_total ON orders (employee_id, total_due)",
        }) |ddl| {
            const r = client.execute(ddl) catch |e| {
                std.debug.print("  index failed ({s}): {any}\n", .{ ddl, e });
                continue;
            };
            allocator.free(r);
        }
        std.debug.print("[sql] indexes created in {d} ms\n", .{nowMs() - it0});
    }

    const queries = [_]struct { name: []const u8, sql: []const u8 }{
        .{ .name = "Q1  EmployeeID=279 limit 10000", .sql = "SELECT * FROM orders WHERE employee_id = 279 LIMIT 10000" },
        .{ .name = "Q2  EmployeeID=279 TotalDue>10000 limit 10000", .sql = "SELECT * FROM orders WHERE employee_id = 279 AND total_due > 10000 LIMIT 10000" },
        .{ .name = "Q3  Q2 + sort TotalDue desc", .sql = "SELECT * FROM orders WHERE employee_id = 279 AND total_due > 10000 ORDER BY total_due DESC LIMIT 10000" },
        .{ .name = "Q4  TotalDue>50000 limit 5000", .sql = "SELECT * FROM orders WHERE total_due > 50000 LIMIT 5000" },
        .{ .name = "Q5  Q4 + sort TotalDue desc", .sql = "SELECT * FROM orders WHERE total_due > 50000 ORDER BY total_due DESC LIMIT 5000" },
        .{ .name = "Q6  CustomerID=1045 limit 10000", .sql = "SELECT * FROM orders WHERE customer_id = 1045 LIMIT 10000" },
        .{ .name = "Q7  EmployeeID in (279,281,283) limit 10000", .sql = "SELECT * FROM orders WHERE employee_id IN (279,281,283) LIMIT 10000" },
        .{ .name = "Q8  TotalDue 10000..50000 limit 10000", .sql = "SELECT * FROM orders WHERE total_due > 10000 AND total_due < 50000 LIMIT 10000" },
        .{ .name = "Q9  count EmployeeID=279", .sql = "SELECT COUNT(*) FROM orders WHERE employee_id = 279" },
        .{ .name = "Q10 avg TotalDue by EmployeeID top 5", .sql = "SELECT employee_id, AVG(total_due) AS a FROM orders GROUP BY employee_id ORDER BY a DESC LIMIT 5" },
    };

    std.debug.print("\n=== SQL queries ===\n", .{});
    for (queries) |q| {
        const t = nowMs();
        const rows = client.queryCount(q.sql) catch |e| {
            std.debug.print("{s}\n  -> ERROR {any}\n", .{ q.name, e });
            continue;
        };
        std.debug.print("{s}\n  -> {d} rows | {d} ms\n", .{ q.name, rows, nowMs() - t });
    }
}

/// Emit the EXACT same-seed dataset as the SQL loader, but as a standalone
/// `.sql` file (MySQL/InnoDB-ready) instead of executing it over the NovaDB
/// wire. This lets a peer engine be loaded with byte-identical rows for an
/// apples-to-apples comparison. Uses the same generator, seed (`0xABCDEF12`),
/// schema and indexes as `runSql`'s load path, so the produced rows match the
/// kaidb scratch data row-for-row.
pub fn dumpRun(allocator: std.mem.Allocator, cfg: config_mod.BenchmarkConfig, data_dir: []const u8, out_path: []const u8) !void {
    var threaded: Io.Threaded = .init(allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const s = try loadSources(allocator, io, data_dir);

    const file = try Io.Dir.createFile(.cwd(), io, out_path, .{});
    defer file.close(io);
    var wbuf: [128 * 1024]u8 = undefined;
    var fw = file.writer(io, &wbuf);
    const w = &fw.interface;

    try w.writeAll(
        \\DROP TABLE IF EXISTS orders;
        \\CREATE TABLE orders (id INT PRIMARY KEY, order_date TEXT, due_date TEXT, ship_date TEXT, employee_id INT, customer_id INT, sub_total DOUBLE, tax_amt DOUBLE, freight DOUBLE, total_due DOUBLE, details TEXT) ENGINE=InnoDB;
        \\SET autocommit=0;
        \\SET unique_checks=0;
        \\SET foreign_key_checks=0;
        \\
    );

    const n = cfg.record_count;
    const batch: u64 = 100;
    var prng = std.Random.DefaultPrng.init(0xABCDEF12);
    const rng = prng.random();
    var details: [8]Detail = undefined;
    var date_buf: [16]u8 = undefined;
    var i: u64 = 0;
    var sb = std.ArrayList(u8).empty;
    defer sb.deinit(allocator);
    while (i < n) {
        sb.clearRetainingCapacity();
        try sb.appendSlice(allocator, "INSERT INTO orders (id,order_date,due_date,ship_date,employee_id,customer_id,sub_total,tax_amt,freight,total_due,details) VALUES ");
        const upto = @min(i + batch, n);
        var first = true;
        while (i < upto) : (i += 1) {
            const o = genOrder(rng, s, &details, &date_buf);
            if (!first) try sb.append(allocator, ',');
            first = false;
            var dj = std.ArrayList(u8).empty;
            defer dj.deinit(allocator);
            var eltbuf: [128]u8 = undefined;
            try dj.append(allocator, '[');
            for (o.SalesOrderDetails, 0..) |d, k| {
                if (k != 0) try dj.append(allocator, ',');
                const elt = try std.fmt.bufPrint(&eltbuf, "{{\"ProductID\":{d},\"OrderQty\":{d},\"UnitPrice\":{d:.4},\"LineTotal\":{d:.4}}}", .{ d.ProductID, d.OrderQty, d.UnitPrice, d.LineTotal });
                try dj.appendSlice(allocator, elt);
            }
            try dj.append(allocator, ']');
            var rowbuf: [512]u8 = undefined;
            const head = try std.fmt.bufPrint(&rowbuf, "({d},'{s}','{s}','{s}',{d},{d},{d:.4},{d:.4},{d:.4},{d:.4},'", .{ i, o.OrderDate, o.DueDate, o.ShipDate, o.EmployeeID, o.CustomerID, o.SubTotal, o.TaxAmt, o.Freight, o.TotalDue });
            try sb.appendSlice(allocator, head);
            try sb.appendSlice(allocator, dj.items);
            try sb.appendSlice(allocator, "')");
        }
        try sb.appendSlice(allocator, ";\n");
        try w.writeAll(sb.items);
        if (i % 100000 < batch) std.debug.print("  dumped {d}/{d}\n", .{ i, n });
    }
    try w.writeAll(
        \\COMMIT;
        \\CREATE INDEX idx_emp ON orders (employee_id);
        \\CREATE INDEX idx_cust ON orders (customer_id);
        \\CREATE INDEX idx_total ON orders (total_due);
        \\CREATE INDEX idx_emp_total ON orders (employee_id, total_due);
        \\
    );
    try fw.flush();
    std.debug.print("[dump] wrote {d} orders to {s}\n", .{ n, out_path });
}



