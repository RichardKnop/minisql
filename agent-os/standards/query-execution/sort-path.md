---
name: Sort Path Selection
description: How ORDER BY chooses between index scan, heap-based, and full in-memory sort; DISTINCT interaction
type: standard
---

# Sort Path Selection

ORDER BY is handled in one of three ways, chosen during `PlanQuery`:

| Condition | Path |
|---|---|
| ORDER BY columns match an index (same order, uniform direction) | `ScanTypeIndexAll` — rows arrive pre-sorted; no in-memory sort |
| ORDER BY with mixed ASC/DESC, or no matching index | `SortInMemory = true` — `selectWithSort` |
| No ORDER BY | `selectStreaming` |

## selectWithSort internals

- **LIMIT + ORDER BY (no DISTINCT):** uses a fixed-size min/max heap (`rowHeap`, size = `offset + limit`). Only keeps top N rows in memory.
- **Everything else (no LIMIT, or DISTINCT):** collects all rows into a slice, sorts with `sortRows`.
- **DISTINCT** always materialises all rows before deduplication, even when LIMIT is present — heap optimisation is disabled.

## Rules

- Never apply OFFSET/LIMIT before sorting and deduplication.
- DISTINCT deduplication (`deduplicateRows`) runs after sort, before OFFSET/LIMIT slicing.
- Mixed ASC/DESC ORDER BY always triggers in-memory sort regardless of indexes.
