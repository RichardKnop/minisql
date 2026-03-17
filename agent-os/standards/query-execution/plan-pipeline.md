---
name: Query Plan Pipeline
description: How Select streams rows through a channel from scanner goroutine to consumer
type: standard
---

# Query Plan Pipeline

`Table.Select` follows a fixed pipeline:

```
PlanQuery  →  plan.Execute (goroutine)  →  filteredPipe (chan Row, buf=128)
                                                  ↓
                                  selectStreaming  OR  selectWithSort
```

## Rules

- `plan.Execute` runs in a `wg.Go` goroutine; a second goroutine closes `filteredPipe` when the WaitGroup drains.
- `filteredPipe` buffer = 128. `limitedPipe` (inside streaming path) buffer = 64. Do not reduce these — they exist to reduce goroutine blocking.
- The two consumer paths are mutually exclusive: `plan.SortInMemory = true` → `selectWithSort`; otherwise `selectStreaming`.
- `errorsPipe` is buffered to `len(plan.Scans)` so scan goroutines never block on error delivery.
- New query operations that need to materialise rows (e.g. aggregation, GROUP BY) should follow the `selectWithSort` pattern: collect from `filteredPipe`, transform, then build a `StatementResult` with an `Iterator`.
