# GraniteDB

GraniteDB is a compact relational core implemented in Go. It focuses on the fundamentals of page-based storage, a tiny SQL surface, and a clean modular design. Stage 6 introduces explicit transactions, Read Committed isolation, and a lock manager alongside the existing indexing, constraint, grouping, aggregation, and ordering features.

## Quick start

GraniteDB requires Go 1.21 or newer.

```bash
cd engine
go build ./...
```

### Creating a database

```bash
cd engine
./granitectl new demo.gdb
```

### Running commands

```bash
cd engine
./granitectl exec -q "CREATE TABLE people(id INT NOT NULL, name VARCHAR(50), PRIMARY KEY(id));" demo.gdb
./granitectl exec -q "INSERT INTO people(id, name) VALUES (1, 'Ada');" demo.gdb
./granitectl exec -q "INSERT INTO people(id, name) VALUES (2, 'Grace');" demo.gdb
./granitectl exec -q "SELECT * FROM people;" demo.gdb
./granitectl exec -q "CREATE INDEX idx_people_name ON people(name);" demo.gdb
./granitectl explain -q "SELECT * FROM people WHERE name = 'Ada';" demo.gdb
```

Foreign keys now protect parent/child relationships immediately:

```bash
cd engine
./granitectl exec -q "CREATE TABLE customers(id INT PRIMARY KEY, name VARCHAR(50));" demo.gdb
./granitectl exec -q "CREATE TABLE orders(id INT PRIMARY KEY, customer_id INT REFERENCES customers(id) ON DELETE RESTRICT ON UPDATE RESTRICT);" demo.gdb
./granitectl exec -q "INSERT INTO customers VALUES (1,'Ada');" demo.gdb
./granitectl exec -q "INSERT INTO orders VALUES (100,1);" demo.gdb
./granitectl exec -q "DELETE FROM customers WHERE id=1;" demo.gdb   # foreign key violation on "fk_orders_1": referenced by "orders" ...
```

The CLI surfaces friendly error messages when constraints are violated, making it easy to spot the offending key values.
```

Expected output:

```
id | name 
-- | -----
1  | Ada  
2  | Grace
(2 row(s))
```

To inspect the schema, use:

```bash
cd engine
./granitectl dump demo.gdb
```

Explicit transactions are now available and default to autocommit when not in use:

```bash
cd engine
./granitectl exec -q "BEGIN; UPDATE orders SET total = total + 10 WHERE id = 100; COMMIT;" demo.gdb
./granitectl exec -q "BEGIN; UPDATE orders SET total = total + 100 WHERE id = 100; ROLLBACK;" demo.gdb
```

Conflicting writers wait for the holder to finish before returning a clear error such as `lock timeout on table orders` once the two second timeout expires.

## New in Stage 6

Stage 6 introduces explicit transaction control and a lock manager that provides
Read Committed isolation. Statements continue to run in autocommit mode unless
wrapped by `BEGIN`/`COMMIT` or `ROLLBACK`. Conflicting readers and writers now
wait on table and row locks, with clear timeout errors when contention persists.
All previously shipped features, including secondary indexes and immediate
foreign key enforcement, remain available. A few examples:

```bash
./granitectl exec -q "SELECT c.name, COUNT(o.id) AS orders, SUM(o.total) AS spend FROM customers c LEFT JOIN orders o ON c.id=o.customer_id GROUP BY c.name HAVING SUM(o.total) IS NOT NULL ORDER BY spend DESC, c.name ASC;" demo.gdb
```

```
name | orders | spend
---- | ------ | -----
Grace| 2      | 99.99
Ada  | 2      | 49.75
(2 row(s))
```

```bash
./granitectl exec -q "SELECT customer_id, AVG(total) AS avg_total FROM orders GROUP BY customer_id ORDER BY customer_id;" demo.gdb
./granitectl explain -q "SELECT * FROM orders WHERE total > 50;" demo.gdb
```

```
customer_id | avg_total
----------- | ---------
1           | 24.88
2           | 99.99
(2 row(s))
```

Expression projections, arithmetic, joins, and scalar functions from the
previous stages remain available and continue to work without modification.

### Running scripts

You can execute a file containing semicolon-terminated statements. The runner stops at the first error by default, but the `--continue-on-error` flag keeps processing subsequent statements.

```bash
cd engine
./granitectl exec -f ./seed.sql demo.gdb
```

### Exporting results

For quick CSV exports, change the output format when running ad-hoc commands or scripts:

```bash
cd engine
./granitectl exec -q "SELECT * FROM people;" --format csv demo.gdb
```

### Explaining a query

The `explain` verb prints a lightweight operator tree and JSON payload for integration with tooling.

```bash
cd engine
./granitectl explain -q "SELECT * FROM people;" demo.gdb
```

## Features

* 4 KB slotted pages with a freelist allocator.
* Heap files for table storage with automatic page chaining.
* System catalogue capturing table definitions, column metadata, row counts, and secondary indexes.
* Minimal SQL subset (CREATE/DROP TABLE, CREATE/DROP INDEX, INSERT, SELECT with expression projections, filtering, grouping, aggregation, ordering, and joins).
* Fixed-precision `DECIMAL` columns with precision/scale enforcement across inserts and scans.
* B⁺-tree indexes shared by primary and secondary keys with optional uniqueness enforcement.
* Cost-free planner heuristics that recognise equality and range predicates and choose index scans automatically.
* Immediate foreign key enforcement with RESTRICT/NO ACTION behaviour for both column-level and table-level declarations.
* Explicit transactions with Read Committed isolation, shared/exclusive table locks, row-level exclusive locks, and descriptive lock timeouts.
* Write-ahead logging (REDO) underpinning transaction durability.
* Command-line client for database lifecycle management, transaction-aware query execution, script running, CSV exports, and plan inspection.

## Current limitations

* Joins are limited to left-deep chains of INNER and LEFT joins. No USING, RIGHT/FULL joins, or join reordering.
* Isolation is limited to Read Committed and enforced via locking with timeout-based deadlock avoidance.
* Single database file – no replication or clustering.
* Foreign keys currently support only `RESTRICT`/`NO ACTION` referential actions. `CASCADE`, `SET NULL`, `SET DEFAULT`, and deferrable constraints are not yet available.
* Only literal VALUES clauses are accepted in INSERT statements.

## Testing

```bash
cd engine
go test ./...
```

## Roadmap

Future work will focus on richer join strategies, index cost estimation,
stronger isolation levels, and observability enhancements.

## Licence

GraniteDB is released under the Apache 2.0 Licence. See [LICENCE](LICENSE).
