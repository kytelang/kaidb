# orders-compare

The Q1..Q10 "orders" benchmark, run head to head across **kaidb**, **PostgreSQL**
and **MySQL** from a single Kyte program.

It generates a synthetic orders dataset with a seeded RNG, so every engine loads
byte-identical rows, then loads it into each engine, builds the same four
indexes, and runs the same ten queries against each. Because the data and the SQL
are identical, the result row counts match across engines and only the timings
differ. Each query is run twice and the warm (second) pass is reported as the
full client round-trip time (server executes plus all result rows returned).

This replaces the old Zig YCSB harness (`benchmark/ycsb`). Where that measured
kaidb alone and PostgreSQL was timed separately by hand with `psql`, this runs
all three engines in one program through the Kyte driver packages.

## Schema and queries

Table `orders(id PK, order_date, due_date, ship_date, employee_id, customer_id,
sub_total, tax_amt, freight, total_due, details)`, with secondary indexes on
`employee_id`, `customer_id`, `total_due`, and a composite `(employee_id,
total_due)`. Money columns are stored as whole-number doubles so the generator
needs no float formatting.

| Query | SQL |
|---|---|
| Q1  | `SELECT * FROM orders WHERE employee_id = 279 LIMIT 10000` |
| Q2  | `... WHERE employee_id = 279 AND total_due > 10000 LIMIT 10000` |
| Q3  | Q2 `ORDER BY total_due DESC` |
| Q4  | `... WHERE total_due > 50000 LIMIT 5000` |
| Q5  | Q4 `ORDER BY total_due DESC` |
| Q6  | `... WHERE customer_id = 1045 LIMIT 10000` |
| Q7  | `... WHERE employee_id IN (279,281,283) LIMIT 10000` |
| Q8  | `... WHERE total_due > 10000 AND total_due < 50000 LIMIT 10000` |
| Q9  | `SELECT COUNT(*) ... WHERE employee_id = 279` |
| Q10 | `SELECT employee_id, AVG(total_due) ... GROUP BY employee_id ORDER BY a DESC LIMIT 5` |

## Build

The three driver packages are symlinked under `packages/` (kyte-postgres,
kyte-mysql, kyte-kaidb). From this directory:

```sh
kyte build
codesign -s - -f build/debug/bin/orders-compare   # macOS only
```

## Run

Everything is configured by environment variable (all optional):

| Variable | Default | Meaning |
|---|---|---|
| `ORDERS_ROWS` | `1000000` | rows to generate and load |
| `ORDERS_ENGINES` | `kaidb,postgres,mysql` | comma list of engines to run, in order |
| `ORDERS_BATCH` | `500` | rows per multi-row INSERT |
| `ORDERS_SEED` | `12345` | RNG seed (same seed gives identical data) |
| `ORDERS_PIPELINE` | `64` | kaidb only: INSERT batches in flight during load. Set `1` to load kaidb through the same synchronous path as PostgreSQL/MySQL (apples-to-apples load). |
| `ORDERS_TABLE` | `orders` | base table name |
| `ORDERS_OUT` | `orders_compare_report.md` | markdown report path |
| `KAIDB_URL` | `admin:admin@127.0.0.1:3009?db=default&tls=false` | kaidb DSN |
| `PG_URL` | `postgresql://postgres@127.0.0.1:5432/novabench?sslmode=disable` | PostgreSQL DSN |
| `MYSQL_URL` | `mysql://root@127.0.0.1:3306/novabench?sslmode=disable` | MySQL DSN |

```sh
# full default run (1M rows, all three engines)
./build/debug/bin/orders-compare

# quick smoke, kaidb and MySQL only
ORDERS_ROWS=50000 ORDERS_ENGINES=kaidb,mysql ./build/debug/bin/orders-compare
```

The report is printed to the console and written to `ORDERS_OUT`. It has three
tables: load and index-build times, per-query warm timings, and a result-row
cross-check (identical data must give identical counts across engines, which is
the benchmark's built-in correctness gate).

## Safety

The harness only ever creates, drops and queries the `orders` table in the
database named in each engine's URL. **Point the SQL engines at a throwaway
database** (the defaults use `novabench`), never at an application database.
An engine that cannot actually store the data (for example a PostgreSQL server
in read-only recovery mode) is detected by the post-load row-count check and
reported as skipped rather than producing bogus zero-millisecond "wins".

## kaidb note (engine bug worked around here)

On kaidb, `DROP TABLE` does not reclaim the table's storage, and neither
`DROP`+`CREATE` of the same name nor `DELETE FROM` clears the table's
primary-key index. A reload of the same ids therefore fails with
`KeyAlreadyExists` even though `COUNT(*)` reads zero. To stay re-runnable, the
harness gives kaidb a fresh, run-unique table name (`orders_<timestamp>`) each
run. That leaks the previous table inside `nova.db`; restart kaidb with a clean
data directory to reclaim the space. PostgreSQL and MySQL reclaim on `DROP`, so
they reuse the base name.
