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

## Rules

- Use `testColumns` for most tests.
- Use `testMediumColumns` when you need to test B+ tree node splits (5 rows fills one page, so inserting 6+ triggers a split).
- Use `testBigColumns` when you need to test overflow pages (row is too large to fit in a single page).
- Do not hard-code row sizes in tests — derive them from `testRowSize` or the `testXxxColumns` slice sizes.
