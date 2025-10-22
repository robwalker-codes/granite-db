# GraniteDB

GraniteDB is a compact relational core implemented in Go. It focuses on the fundamentals of page-based storage, a tiny SQL surface, and a clean modular design. Stage 2 introduces two-table joins on top of the Stage 1 expression engine.

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

## New in Stage 2

Stage 2 extends the SELECT pipeline with table aliases and two-table joins in
addition to the Stage 1 expression work. A few examples:

```bash
./granitectl exec -q "SELECT c.name, o.total FROM customers c JOIN orders o ON c.id=o.customer_id ORDER BY o.id;" demo.gdb
```

```
name | total
---- | -----
Ada  | 4250
Ada  |  725
Grace| 9999
(3 row(s))
```

```bash
./granitectl exec -q "SELECT c.id, c.name, o.id AS order_id FROM customers c LEFT JOIN orders o ON c.id=o.customer_id ORDER BY c.id, order_id;" demo.gdb
```

```
id | name  | order_id
-- | ----- | --------
1  | Ada   | 100
1  | Ada   | 101
2  | Grace | 200
3  | Lin   | NULL
(4 row(s))
```

Expression projections, arithmetic, and built-in functions from Stage 1 remain
available and continue to work without modification.

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
* System catalogue capturing table definitions, column metadata, and row counts.
* Minimal SQL subset (CREATE TABLE, DROP TABLE, INSERT, SELECT with expression projections, filtering, ordering, and two-table joins).
* Command-line client for database lifecycle management, query execution, script running, CSV exports, and plan inspection.

## Current limitations

* Joins are limited to left-deep chains of INNER and LEFT joins. No USING, RIGHT/FULL joins, or join reordering.
* No transactions, WAL, or concurrent access safety.
* Single database file – no replication or clustering.
* Constraints beyond `NOT NULL` and `PRIMARY KEY` are not enforced.
* Only literal VALUES clauses are accepted in INSERT statements.

## Testing

```bash
cd engine
go test ./...
```

## Roadmap

Future work will focus on richer joins, secondary indexes, and transaction
infrastructure alongside CLI and observability enhancements.

## Licence

GraniteDB is released under the Apache 2.0 Licence. See [LICENCE](LICENSE).
