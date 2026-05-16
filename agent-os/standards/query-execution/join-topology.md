---
name: Join Topology — LeftScanIndex Resolution
description: flattenJoinTree + scanIndexByAlias pattern for chain joins; why LeftScanIndex is never hardcoded
type: standard
---

# Join Topology — LeftScanIndex Resolution

Joins are flattened into a flat `QueryPlan.Scans` array by `flattenJoinTree` (DFS).
`JoinPlan.LeftScanIndex` is NOT always 0 — it is the scan slot of whichever table
the join is FROM, looked up via `scanIndexByAlias`.

## Why

Star-schema: all joins are from the base table → LeftScanIndex is always 0.
Chain join (a→b→c): the b→c join has b as its left side → LeftScanIndex = 1 (slot of b).
Hardcoding 0 silently fetches the wrong row when executing chain joins.

## scanIndexByAlias

```go
// Base table always starts at slot 0.
scanIndexByAlias := map[string]int{stmt.TableAlias: 0}
// flattenJoinTree adds each joined alias as it appends a Scan:
scanIndexByAlias[join.TableAlias] = len(plan.Scans) - 1
```

## Rules

- Never hardcode `LeftScanIndex = 0`.
- When adding a new scan to `plan.Scans`, register its alias in `scanIndexByAlias` before any sub-join that references it.
- `validateJoinTree` is recursive — new join validation must also be recursive.
