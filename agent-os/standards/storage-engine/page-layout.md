---
name: Page Layout
description: 4KB page tagged-union structure, page 0 special handling, and usable space calculation
type: standard
---

# Page Layout

Every piece of data lives in a fixed-size **4 KB page** (`PageSize = 4096`).

## Page tagged union

`Page` is a tagged union — exactly **one** field is non-nil:

| Field | Used for |
|---|---|
| `LeafNode` | B+ tree leaf (row data) |
| `InternalNode` | B+ tree internal (routing) |
| `OverflowPage` | Continuation of a row that doesn't fit one page |
| `FreePage` | Free-list entry pointing to the next free page |
| `IndexNode` | B+ tree node for a secondary/unique index |
| `IndexOverflowNode` | Overflow for non-unique index row ID lists |

All marshal/unmarshal and `Clone` code switches on which field is set. Never set more than one field.

## Page 0 (root page)

Page 0 is special: its first `RootPageConfigSize` bytes hold the `DatabaseHeader`. The rest of the page is a normal B+ tree node. `Flush` writes the header separately before writing the rest of the page content.

## Usable space

`UsablePageSize = 4096 - 7 - 8 - 8 - 8` — page size minus base header, node header, key, and null bitmask overhead.
