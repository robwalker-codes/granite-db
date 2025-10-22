# SQL support

GraniteDB offers a compact SQL surface aimed at analytical tinkering. Stage 3
builds upon the Stage 2 join work by introducing grouping, aggregation, and
multi-key ordering while retaining the existing expression grammar and join
pipeline.

## Projection expressions

Each `SELECT` item may be an arbitrary scalar expression with an optional alias
(`expr [AS] alias`). The default column name is derived from the expression text
when no alias is supplied. The `*` shorthand remains supported as the sole item
in the projection list. When multiple tables participate in the query the
expanded column names are qualified with their source alias to avoid ambiguity.

Constant projections without a `FROM` clause are allowed for evaluating literal
expressions once per query.

Supported operands and operators include:

* Literals: `INT`, `BIGINT`, `DECIMAL`, `VARCHAR`, `BOOLEAN`, `DATE`,
  `TIMESTAMP`, and `NULL`.
* Column references, optionally qualified by table name (`table.column`).
* Unary operators: `+`, `-`, `NOT`.
* Binary arithmetic: `+`, `-`, `*`, `/`, `%` with numeric promotion rules
  (`INT → BIGINT → DECIMAL`).
* Binary comparison: `=`, `<>`, `<`, `<=`, `>`, `>=`.
* Boolean connectives: `AND`, `OR` with three-valued logic semantics.
* Parentheses for explicit precedence control.

### Built-in functions

| Function | Description | Return type |
| --- | --- | --- |
| `LOWER(text)` | Convert text to lower case | `VARCHAR` |
| `UPPER(text)` | Convert text to upper case | `VARCHAR` |
| `LENGTH(text)` | Character length (Unicode aware) | `INT` |
| `COALESCE(a, b)` | Return the first non-NULL argument | Type of arguments |

`COALESCE` requires both arguments to share the same data type; the resulting
column is nullable only if both inputs are nullable.

## Filters, ordering, and limits

`WHERE` clauses accept the same expression grammar as projections. Comparisons
and boolean connectives honour SQL's three-valued logic, treating `NULL`
predicates as unknown and therefore false for filtering. In LEFT JOIN queries
the filter is applied after the join: predicates referencing the right-hand
table may therefore collapse the LEFT JOIN back into an INNER join when they
reject `NULL` rows.

`ORDER BY` supports multi-column keys with optional `ASC`/`DESC` modifiers per
expression. Expressions may reference projection aliases or be re-evaluated in
place, and `NULL` values are always placed last. `LIMIT ... OFFSET ...` retains
its existing semantics.

## Grouping and aggregation

`GROUP BY` clauses collect rows into groups using any deterministic expression
over the input columns. Each projection must either be an aggregate function or
be composed entirely from grouped expressions. The following aggregate
functions are available:

* `COUNT(*)`
* `COUNT(expr)` (ignores `NULL` values)
* `SUM(expr)`
* `AVG(expr)`
* `MIN(expr)`
* `MAX(expr)`

Aggregates infer result types based on their arguments. Integer inputs widen to
`DECIMAL` to avoid overflow, whilst `DECIMAL(p,s)` inputs widen to
`DECIMAL(p+10, s)`. Aggregate results honour SQL `NULL` semantics: `COUNT` never
returns `NULL`, whereas `SUM`/`AVG` return `NULL` for all-null groups. `HAVING`
filters are evaluated after aggregation and may reference group keys or
aggregate outputs.

## FROM clause and joins

The `FROM` clause accepts either a single table reference or a left-deep chain
of two-table joins. Each table may carry an optional alias declared with `AS`
or directly after the table name. Supported join forms are:

```
FROM table [AS alias]
FROM left [INNER | LEFT [OUTER]] JOIN right [AS alias] ON <boolean expression>
```

`INNER JOIN` may omit the `INNER` keyword. `LEFT` and `LEFT OUTER` are
synonymous. ON clauses reuse the main expression grammar; the system rejects
`USING` syntax.

Name resolution honours aliases first and falls back to base table names where
no alias is specified. Unqualified column references must be unambiguous across
the join sources; otherwise validation reports an error listing the competing
candidates (for example `ambiguous column "id" (candidates: c.id, o.id)`).

Join planning splits equality predicates into hash join keys wherever possible
and applies remaining conditions (including non-equality predicates) as
residual filters. Both INNER and LEFT joins are supported. Multi-way joins are
evaluated left-to-right without reordering.

## Known limitations

* Mixing `*` with other projection expressions is not yet supported.
* Joins are limited to left-deep chains of INNER and LEFT joins. No USING,
  RIGHT/FULL joins, or join reordering are available.
* Aggregate functions do not support `DISTINCT`, window functions, or grouping
  sets.
* No user-defined scalar functions beyond the listed built-ins.

