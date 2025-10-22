# GraniteDB

GraniteDB is a compact relational core implemented in Go. It focuses on the fundamentals of page-based storage, a tiny SQL surface, and a clean modular design. This repository currently ships Phase 1A of the roadmap with incremental tooling improvements.

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

## New in Stage 1

Stage 1 introduces expression projections in `SELECT` lists with full type
inference, aliases, and NULL-aware evaluation. A few examples:

```bash
./granitectl exec -q "SELECT id+1 AS next, UPPER(name) AS uname, COALESCE(nick,name) AS display FROM people ORDER BY id;" demo.gdb
```

```
next | uname | display
---- | ----- | -------
2    | ADA   | Ada
3    | GRACE | G
(2 row(s))
```

```bash
./granitectl exec -q "SELECT 1+2*3 AS a, (1+2)*3 AS b;" demo.gdb
```

```
a | b
- | -
7 | 9
(1 row(s))
```

```bash
./granitectl exec -q "SELECT LENGTH(name) FROM people;" demo.gdb
```

```
LENGTH(name)
------------
3
5
(2 row(s))
```

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
* Minimal SQL subset (CREATE TABLE, DROP TABLE, INSERT, SELECT with expression projections and filtering).
* Command-line client for database lifecycle management, query execution, script running, CSV exports, and plan inspection.

## Current limitations

* Single-table queries only; JOINs, GROUP BY, and subqueries are not yet supported.
* No transactions, WAL, or concurrent access safety.
* Single database file – no replication or clustering.
* Constraints beyond `NOT NULL` and `PRIMARY KEY` are not enforced.
* Only literal VALUES clauses are accepted in INSERT statements.

## Testing

```bash
cd engine
go test ./...
```

## Phase 1B roadmap

Phase 1B will focus on:

* Extending SELECT to support simple filters and ordering.
* Adding basic transaction semantics with a write-ahead log.
* Introducing secondary indexes to accelerate lookups.
* Improving the CLI with script execution and formatted exports.
* Enhancing observability with page inspection tools.

## Licence

GraniteDB is released under the Apache 2.0 Licence. See [LICENCE](LICENSE).
