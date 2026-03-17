---
name: DNF-to-Scans Fanout
description: How each OR group in the DNF WHERE clause becomes one Scan, and how residual filters work
type: standard
---

# DNF-to-Scans Fanout

The WHERE clause is normalised to DNF (`[][]Condition`) by the parser. Each inner slice (AND group) becomes exactly one `Scan` in `QueryPlan.Scans`.

```
WHERE (pk=1 AND a='foo') OR (pk=2 AND b='bar')
  → Scan{IndexPoint, key=1, Filters=[a='foo']}
  → Scan{IndexPoint, key=2, Filters=[b='bar']}
```

All scans write rows to the **same** `filteredPipe`. The consumer (streaming or sort) sees a merged, unordered stream.

## Scan.Filters (residual conditions)

Conditions that cannot be satisfied by the index are stored in `Scan.Filters` and applied row-by-row after the index retrieves the row (`Scan.FilterRow`).

## Rules

- One OR group = one Scan. Never merge groups.
- If all OR groups would fall back to sequential scan, `setIndexScans` keeps a single sequential scan (not N identical ones) — the original default `plan.Scans[0]` already holds the full DNF filter.
- Adding a new operator that can use an index: add it to `isEquality` or `tryRangeScan`, never skip residual filter handling.
