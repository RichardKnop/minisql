---
name: Index Selection Rules
description: Which operators can use indexes, index priority, composite prefix matching, and the sequential scan override guard
type: standard
---

# Index Selection Rules

`PlanQuery` calls `setIndexScans` to decide whether to replace the default sequential scan with index scans.

## Operator eligibility

| Can use index | Cannot use index (always sequential) |
|---|---|
| `=`, `IN` (equality / point scan) | `!=`, `NOT IN` |
| `>`, `>=`, `<`, `<=` (range scan) | `IS NULL`, `IS NOT NULL` |
| | `LIKE`, `NOT LIKE` |
| | `BETWEEN`, `NOT BETWEEN` |

## Index priority (no ANALYZE stats)

Primary Key → Unique Index → Secondary Index

With ANALYZE stats, selectivity wins: higher selectivity (more distinct values) is preferred.

## Composite index matching

- Columns must match left-to-right prefix: index on `(a, b, c)` can match `a`, `a+b`, or `a+b+c`.
- `IN` is only allowed on the **last matched column** — earlier columns must use `=`.
- Partial prefix match → range scan with synthesised upper bound (`incrementValue`); if upper bound cannot be incremented, residual filters are kept.

## Override guard

`setIndexScans` only replaces the default sequential scan if at least one resulting scan uses a real index. If all OR groups fall back to sequential, the original single sequential scan is preserved.
