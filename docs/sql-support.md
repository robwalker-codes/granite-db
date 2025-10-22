# SQL support

GraniteDB offers a compact SQL surface aimed at single-table workloads. Stage 1
adds expression projections to the `SELECT` list alongside the existing
filtering, ordering, and limiting features.

## Projection expressions

Each `SELECT` item may be an arbitrary scalar expression with an optional alias
(`expr [AS] alias`). The default column name is derived from the expression text
when no alias is supplied. The `*` shorthand remains supported but cannot yet
be mixed with other expressions.

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
predicates as unknown and therefore false for filtering.

`ORDER BY` continues to support single column keys with `ASC`/`DESC` modifiers.
`LIMIT ... OFFSET ...` retains its existing semantics.

## Known limitations

* Mixing `*` with other projection expressions is not yet supported.
* Only single-table queries are available; joins and table aliases remain out of
  scope for Stage 1.
* No user-defined functions or additional scalar built-ins beyond the list
  above.
* ORDER BY expressions are limited to base columns.

