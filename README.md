# GraniteDB

GraniteDB is a compact relational core implemented in Go. It focuses on the fundamentals of page-based storage, a tiny SQL surface, and a clean modular design. Stage 4 introduces secondary indexes and planner heuristics that build upon the Stage 3 grouping, aggregation, and ordering work.

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

## New in Stage 4

Stage 4 adds secondary indexes, uniqueness enforcement, and planner heuristics
that automatically choose useful access paths. Grouping, aggregation, and
multi-key ordering from Stage 3 remain available. A few examples:

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
* Write-ahead logging (REDO) with autocommit execution.
* Command-line client for database lifecycle management, query execution, script running, CSV exports, and plan inspection.

## Current limitations

* Joins are limited to left-deep chains of INNER and LEFT joins. No USING, RIGHT/FULL joins, or join reordering.
* No multi-statement transactions or concurrent access safety beyond WAL-based crash recovery.
* Single database file – no replication or clustering.
* Constraints beyond `NOT NULL`, `PRIMARY KEY`, and `UNIQUE` indexes are not enforced.
* Only literal VALUES clauses are accepted in INSERT statements.

## Testing

```bash
cd engine
go test ./...
```

## Roadmap

Future work will focus on richer join strategies, index cost estimation,
multi-statement transactions, and observability enhancements.

## Licence

GraniteDB is released under the Apache 2.0 Licence. See [LICENCE](LICENSE).
