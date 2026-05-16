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
| MATCH() on full-text index | `LIKE`, `NOT LIKE` |
| JSON_CONTAINS() on inverted index | `BETWEEN`, `NOT BETWEEN` |

`ScanTypeFullText` and `ScanTypeInverted` are separate scan types, not the standard point/range paths — their evaluation is handled by `full_text_search.go` and the JSON inverted index executor respectively. Covering index (`CoveringIndex = true`) is never set for these scan types.

## Index priority (no ANALYZE stats)

Primary Key → Unique Index → Secondary Index → Expression Index

With ANALYZE stats, selectivity wins: higher selectivity (more distinct values) is preferred.

## Composite index matching

- Columns must match left-to-right prefix: index on `(a, b, c)` can match `a`, `a+b`, or `a+b+c`.
- `IN` is only allowed on the **last matched column** — earlier columns must use `=`.
- Partial prefix match → range scan with synthesised upper bound (`incrementValue`); if upper bound cannot be incremented, residual filters are kept.

## Expression index matching

`FindExpressionIndex(expr *Expr)` in `expr_index.go` checks whether a WHERE condition's LHS expression structurally matches any expression index via `exprEqual`. If a match is found, `setIndexScans` substitutes an index point/range scan instead of sequential. The index key is evaluated via `evalExprIndexKey` at DML time.

## Partial index implication check

`partialIndexImplied(queryWhere, indexWhere OneOrMore)` performs a conservative syntactic containment check: every condition in the index's WHERE clause must appear verbatim in the query's WHERE clause. Semantically equivalent but textually different conditions are not recognised. When the check fails, the partial index is skipped and a sequential scan (or other index) is used.

## Override guard

`setIndexScans` only replaces the default sequential scan if at least one resulting scan uses a real index. If all OR groups fall back to sequential, the original single sequential scan is preserved.
