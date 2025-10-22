# GraniteDB

GraniteDB is a compact relational core implemented in Go. It focuses on the fundamentals of page-based storage, a tiny SQL surface, and a clean modular design. This repository currently ships Phase 1A of the roadmap.

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
./granitectl exec demo.gdb -q "CREATE TABLE people(id INT NOT NULL, name VARCHAR(50), PRIMARY KEY(id));"
./granitectl exec demo.gdb -q "INSERT INTO people(id, name) VALUES (1, 'Ada');"
./granitectl exec demo.gdb -q "INSERT INTO people(id, name) VALUES (2, 'Grace');"
./granitectl exec demo.gdb -q "SELECT * FROM people;"
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

## Features

* 4 KB slotted pages with a freelist allocator.
* Heap files for table storage with automatic page chaining.
* System catalogue capturing table definitions, column metadata, and row counts.
* Minimal SQL subset (CREATE TABLE, DROP TABLE, INSERT, SELECT *).
* Command-line client for database lifecycle management and query execution.

## Current limitations

* No WHERE, ORDER BY, LIMIT, or JOIN support.
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
