# Architecture overview

GraniteDB follows a clean architecture split between the storage engine, the SQL
front-end, and the command-line tooling. Each layer exposes narrow interfaces so
that the system stays modular as new features arrive. Stage 6 builds upon the
foreign key work by adding explicit transactions, a lock manager, and session
aware execution whilst retaining the reusable B⁺-trees and index-aware
heuristics.

## Storage engine

The storage engine combines slotted heap pages, a write-ahead log, and
B⁺-tree indexes. Tables continue to live in heap files whilst primary and
secondary indexes share the same reusable tree implementation under
`internal/storage/bptree`.

The tree stores keys as composite byte tuples and values as row identifiers.
Leaf pages chain together to support range scans. The page layout is shared by
primary keys and all secondary indexes.

```
+-----------------------+       +-----------------------+
|       Root page       |       |     Internal page     |
|  fan-out directory    |   ┌──>|   separator keys +    |
|  points at children   |   |   |   child page pointers |
+-----------------------+   |   +-----------------------+
            |               |               |
            |               |               v
            |               |   +-----------------------+
            |               |   |       Leaf page       |
            |               └──>|   key tuple | RID list|
            v                   |   key tuple | RID list|
+-----------------------+       |   ...                   |
|     Leaf page         |       |   next leaf pointer →   |
|   key tuple | RID list|       +-----------------------+
|   key tuple | RID list|
|   next leaf pointer → |
+-----------------------+
```

* Root pages track the top-level fan-out and are the only entry point into the
  tree.
* Internal pages carry separator keys and child page numbers.
* Leaf pages store ordered key tuples alongside one or more row identifiers.
  Non-unique indexes therefore share leaf slots between multiple rows.

Each modification is recorded in the write-ahead log before the corresponding
page changes land on disk. On restart the REDO log replays structural updates so
that heap and index state stay in sync.

## Planner flow

The logical planner remains rule-driven. Stage 4 introduces a heuristic that
considers secondary indexes alongside the existing scan and join rules. The
planner inspects predicates from the `WHERE` clause and join conditions, then
selects an index whenever the leftmost prefix of a candidate matches an equality
or range filter.

```
+---------------------------+
| Collect predicates per    |
|   base table / join side  |
+-------------+-------------+
              |
              v
+-------------+-------------+
| Do predicates match an    |
| index prefix?             |
+------+------+-------------+
       |      |
       |      v
       |  +---+--------------------------+
       |  | Choose IndexScan node        |
       |  | - record index + bounds      |
       |  | - attach residual filter     |
       |  +------------------------------+
       |
       v
+------+--------------------+
| Fall back to sequential   |
| scan with filter pushdown |
+---------------------------+
```

When a join predicate references an indexed right-hand table the planner can
produce an index nested loop join, probing the tree for each outer row. Any
remaining predicates are left as residual filters that the executor evaluates
once the index lookup materialises heap rows.

## Command-line tooling

`granitectl` wraps the engine for interactive and scripted use. Stage 4 adds new
`CREATE INDEX` / `DROP INDEX` statements and extends the `dump` sub-command so
that it lists indexes next to tables. `EXPLAIN` output now records the chosen
index name and highlights residual predicates, making it easy to confirm that a
query uses the intended access path.

## Foreign key enforcement

Foreign keys are stored alongside tables in the catalogue. Each entry records
the child columns, parent table, parent key order, and the supported referential
actions. During DML the executor uses this metadata to gate modifications:

```
+----------------------+      +-----------------------------+
| Child INSERT/UPDATE  |----->| Build key (skip all-NULL)   |
+----------------------+      +-------------+---------------+
                                     |
                                     v
                           +-----------------------------+
                           | Probe parent unique index   |
                           |  - Use PK/UNIQUE definition |
                           |  - Heap scan fallback       |
                           +-------------+---------------+
                                     |
                                     v
                           +-----------------------------+
                           | Reject when parent missing  |
                           +-----------------------------+

+----------------------+      +-----------------------------+
| Parent DELETE/UPDATE |----->| Locate referencing children |
+----------------------+      |  - Prefer child FK index    |
                                     |  - Heap scan fallback |
                                     +-------------+---------+
                                                   |
                                                   v
                                       +---------------------+
                                       | Reject when child   |
                                       | rows still present  |
                                       +---------------------+
```

All checks are immediate and wrapped around the existing heap/index writes, so
foreign key errors are surfaced before any WAL entries are emitted.

## Transaction management

Transactions are coordinated by a dedicated manager in `internal/txn`. Each
session obtains a transaction identifier when it issues `BEGIN` (or one of its
synonyms). Autocommit statements still exist: when no transaction is active the
API requests a temporary transaction, marks it as autocommit, and commits or
rolls back automatically after the statement finishes. Explicit transactions
remain tied to the session until `COMMIT` or `ROLLBACK` releases the identifier.

Every transaction records its held locks so that the manager can release them on
completion. Rollbacks reapply captured undo actions to restore heap rows and
index entries to their pre-statement state.

Lock coordination happens inside the new lock manager. It tracks table-level
shared/exclusive locks and row-level exclusive locks. Requests block until they
are compatible with existing holders or until a timeout expires. The following
matrix summarises compatibility:

```
Lock compatibility (request vs held)

            | Held S | Held X
------------+--------+--------
Request S   |   ✓    |   ✗
Request X   |   ✗    |   ✗
```

Shared locks allow concurrent readers, whereas exclusive locks wait for all
other holders to finish. Conflicts beyond the two second timeout surface as
errors such as `lock timeout on row orders[1:1]` so that callers can retry or
abort their work.
