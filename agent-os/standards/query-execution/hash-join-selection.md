---
name: Hash Join Algorithm Selection
description: When the planner chooses hash join vs nested-loop; the 1M threshold and RIGHT JOIN exception
type: standard
---

# Hash Join Algorithm Selection

`planJoinQuery` chooses one of two join algorithms per join:

| Condition | Algorithm |
|---|---|
| Index exists on inner join column | `JoinAlgorithmNestedLoop` (index point lookup) |
| No index + build side ≤ 1M rows | `JoinAlgorithmHash` |
| No index + build side > 1M rows | `JoinAlgorithmNestedLoop` |
| No index + RIGHT JOIN (any size) | `JoinAlgorithmNestedLoop` |

## Threshold: `hashJoinMaxBuildRows = 1_000_000`

The build side is the inner (right) table. `estimatedRowCount()` returns `-1` when stats are
unavailable — this is treated as "small" and hash join is chosen.

## RIGHT JOIN exception

RIGHT JOIN requires an unmatched-row pass after the main loop. This cannot be done
with the current hash probe path, so RIGHT JOIN always falls back to nested-loop
regardless of table size.

## Rules

- Index always wins over hash join — check `findIndexOnColumns` first.
- Do not raise `hashJoinMaxBuildRows` without profiling memory impact.
- New outer-join types (FULL OUTER) must explicitly set `JoinAlgorithmNestedLoop`
  until a hash-based implementation exists.
