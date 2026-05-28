---
name: Page Layout
description: 4KB page tagged-union structure, page 0 database header, and usable space calculation
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

Page 0 is special: its first `RootPageConfigSize` bytes hold the on-disk `DatabaseHeader`. The rest of the page is a normal B+ tree node. `Flush` writes the header separately before writing the rest of the page content.

## Database header format

The current database header layout is:

| Offset | Size | Field |
|---|---:|---|
| `0` | `8` | magic (`minisql\0`) |
| `8` | `4` | file format version (`1`) |
| `12` | `4` | page size (`4096`) |
| `16` | `4` | first free page |
| `20` | `4` | free page count |
| `24` | `76` | reserved |

Rules:

- New database files must always write the full header, not just the free-list fields.
- Opening a database requires valid header magic, version, and page size.
- The reserved bytes are part of the format contract; do not reuse them casually.
- If the header format changes, update docs/tests/standards in the same change.

## Page checksum

The **last 4 bytes** of every page hold a CRC32-IEEE checksum (`pageChecksumSize = 4`). The checksum covers all preceding bytes in the page (`buf[:PageSize-4]`).

- Written at flush time in `pager.go` (`writePageChecksum` / `writeRootPageChecksum`).
- Also embedded in WAL frames at serialization time in `serializeWritesForWAL` (`transaction_manager.go`), so the WAL checkpoint path copies frames verbatim without recomputing checksums.
- Verified on every read from the database file (`verifyPageChecksum` in `GetPage`). A mismatch returns `pkg/errors.PageChecksumError{PageIndex}`, which wraps the sentinel `ErrPageChecksumMismatch`.
- Page 0 is two-piece on disk (header + B-tree data); `writeRootPageChecksum` hashes both pieces together and stores the result at the end of the page so `verifyPageChecksum` on the full 4096-byte read sees a consistent checksum.
- All capacity constants (`UsablePageSize`, `InternalNodeMaxCells`, `RootInternalNodeMaxCells`, `MaxOverflowPageData`, inverted-index body sizes) are derived with `pageChecksumSize` already subtracted. Do not add `pageChecksumSize` again in new code — read the constant definitions in `page.go`, `internal_node.go`, and `overflow_page.go` for the exact formulas.

## Usable space

`UsablePageSize = 4096 - 7 - 8 - 8 - 8 - 4` — page size minus base header, node header, key, null bitmask overhead, and the 4-byte CRC32-IEEE checksum.

## Self-describing cell format (leaf nodes)

Every leaf cell is self-describing: it encodes its own column count and per-column type codes so the decoder never needs the table schema to parse byte widths. The on-disk layout for a single cell is:

```
[8 bytes NullBitmask] [8 bytes Key] [1 byte ColumnCount] [ColumnCount bytes TypeCodes] [packed values]
```

| Field | Size | Notes |
|---|---:|---|
| `NullBitmask` | 8 | Bit N=1 means column N is NULL (no value bytes written for it) |
| `Key` | 8 | Internal row ID (B+ tree key) |
| `ColumnCount` | 1 | Number of TypeCode bytes that follow; 0 means no TypeCodes (legacy/empty) |
| `TypeCodes[0..N-1]` | N | One `TypeCode` byte per column (see table below) |
| packed values | variable | Each column's value bytes, in order; NULLs and `TypeCodeNull` slots consume 0 bytes |

### TypeCode values

| Constant | Value | Byte width | Notes |
|---|---:|---:|---|
| `TypeCodeNull` | 0 | 0 | Dropped-column placeholder; occupies no bytes in the value area |
| `TypeCodeBool` | 1 | 1 | |
| `TypeCodeInt4` | 2 | 4 | |
| `TypeCodeInt8` | 3 | 8 | |
| `TypeCodeReal` | 4 | 4 | |
| `TypeCodeDouble` | 5 | 8 | |
| `TypeCodeTimestamp` | 6 | 8 | Microseconds since 2000-01-01 UTC |
| `TypeCodeUUID` | 7 | 16 | Fixed 16-byte UUID |
| `TypeCodeText` | 8 | 4 + N | 4-byte length prefix then data; covers VARCHAR, TEXT, JSON |

### Lazy ADD COLUMN

When a row was written before a column was added to the schema, `cell.ColumnCount < len(schema.Columns)`. A `RowView` returns the column's declared `Default` value for positions ≥ `ColumnCount`. The B+ tree is **never rewritten** — zero migration cost.

### Tombstone DROP COLUMN

When a column is marked `Deleted = true` in the schema:

- **Old rows** (written before the DROP): still contain real bytes at the dropped position. The `TypeCode` at that position tells the decoder how many bytes to skip, so decoding remains correct without the schema.
- **New rows** (written after the DROP): write `TypeCodeNull` at the dropped position (0 bytes) and set the corresponding `NullBitmask` bit.

This means DROP COLUMN is also a zero-rebuild operation at the storage level.

### Rules

- When constructing a `Cell` in tests, always set both `TypeCodes` and `ColumnCount` — an empty/nil `TypeCodes` with `ColumnCount = 0` makes `RowView` treat every column as lazily-added (all NULLs). Use the `makeTestCell(key, nullBitmask, data, columns)` helper (package `minisql`) or populate inline for external packages.
- `TypeCodesFromColumns(columns []Column)` builds the correct `[]byte` for a column list; deleted columns get `TypeCodeNull`.
- `Cell.Unmarshal` produces `nil` TypeCodes (not `[]byte{}`) when `ColumnCount == 0` to preserve round-trip equality with zero-value `Cell{}` structs.
