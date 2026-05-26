---
name: Row Size Presets
description: testColumns / testMediumColumns / testBigColumns and when to use each for B+ tree and overflow testing
type: standard
---

# Row Size Presets

Three column sets in `minisql_test.go` exist to trigger different B+ tree behaviours deterministically.

| Preset | Columns | Rows per page | Purpose |
|---|---|---|---|
| `testColumns` | 6 standard columns | Many | Normal tests |
| `testMediumColumns` | 6 + extra varchars | Exactly 5 | Tests that trigger splits without overflow |
| `testBigColumns` | 6 + extra varchars | Exactly 1 | Tests that require overflow pages |

The sizes are computed at init time using `appendUntilSize`, which appends `VARCHAR` columns until the row fills the target fraction of a page.

## Self-describing cell overhead

Every leaf cell now carries a `1-byte ColumnCount` field followed by `N TypeCode bytes` (one per column). This overhead must be subtracted from the usable-page-size formula when computing how many rows fit per page, or how large `testBigColumns` can be.

For a page holding R rows each with N columns, the total TypeCode overhead is `R × (1 + N)` bytes.

The `testMediumColumns` and `testBigColumns` formulas in `minisql_test.go` subtract this overhead explicitly:

```go
// testMediumColumns — 5 rows per page, 8 columns per row (6 base + 2 added varchars)
// TypeCode overhead: 5 rows × (1 ColumnCount + 8 TypeCodes) = 45 bytes
int((PageSize - uint32(RootPageConfigSize) -
    7 -      // base header
    8 -      // leaf header
    5*8 -    // 5 keys
    5*8 -    // 5 null bitmasks
    5*1 -    // 5 ColumnCount bytes
    5*8 -    // 5×8 TypeCode bytes (N=8, self-consistent fixed point)
    5*mediumRowBaseSize) / 5)

// testBigColumns — 1 row per page, 21 columns (6 base + 15 added varchars)
// TypeCode overhead: 1 row × (1 ColumnCount + 21 TypeCodes) = 22 bytes
int(PageSize - uint32(RootPageConfigSize) -
    7 -   // base header
    8 -   // leaf header
    8 -   // 1 key
    8 -   // 1 null bitmask
    1 -   // 1 ColumnCount byte
    21 -  // 21 TypeCode bytes
    bigRowBaseSize)
```

**Self-consistency:** The N in the TypeCode subtraction is the total column count after `appendUntilSize` runs, which depends on the target size — which depends on N. Both N=8 and N=21 are verified fixed points: assuming N produces a target that yields exactly N total columns.

## Rules

- Use `testColumns` for most tests.
- Use `testMediumColumns` when you need to test B+ tree node splits (5 rows fills one page, so inserting 6+ triggers a split).
- Use `testBigColumns` when you need to test overflow pages (row is too large to fit in a single page).
- Do not hard-code row sizes in tests — derive them from `testRowSize` or the `testXxxColumns` slice sizes.
- When changing the cell format (adding header bytes), re-derive the TypeCode overhead constants in `testMediumColumns` and `testBigColumns` and verify self-consistency.
