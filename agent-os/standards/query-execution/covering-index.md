---
name: Covering Index Eligibility
description: When markCoveringIndexes sets CoveringIndex=true; SELECT-only restriction, disqualifiers, COUNT(*) shortcut
type: standard
---

# Covering Index Eligibility

`markCoveringIndexes` sets `Scan.CoveringIndex = true` when the index columns
cover every column the query needs, allowing the executor to skip the table row fetch entirely.

## SELECT-only

`CoveringIndex` is only set for `stmt.Kind == Select`.
DELETE and UPDATE always fetch the full table row for index maintenance — covering index
is never considered, even when an index scan is used to locate rows.

## Automatic disqualifiers

| Condition | Why |
|---|---|
| `SELECT *` | All table columns needed |
| `IS NULL` / `IS NOT NULL` in WHERE | Null-keyed rows may be absent from the index |
| `ScanTypeSequential`, `IndexIntersect`, `FullText`, `Inverted` | Never set on these scan types |

## Special case: SELECT COUNT(*)

`SELECT COUNT(*)` is always eligible for covering index — the executor only needs
to count index entries, no column values required.

## Coverage check

Every column in SELECT fields, WHERE conditions, ORDER BY, GROUP BY, and aggregate
functions must appear in the index's column list. A single uncovered column disqualifies
the entire scan.
