---
name: Biased Leaf Split
description: Sequential-insert optimisation in LeafNodeSplitInsert; when to use biased vs even split; two latent bugs it exposed
type: standard
---

# Biased Leaf Split

`LeafNodeSplitInsert` in `cursor.go` chooses between two split strategies based
on whether the new key is greater than all existing keys on the page.

## Split selection

```go
if key > originalMaxKey {
    // Biased split: pack all existing cells on the left, put only the new key right
    leftSplitCount = leafNodeMaxCells
    rightSplitCount = 1
    saveToCell(ctx, newPage.LeafNode, 0, key, row)
} else {
    // Even split: distribute cells evenly (≈ N/2 left, N/2 right)
    rightSplitCount = (leafNodeMaxCells + 1) / 2
    leftSplitCount  = leafNodeMaxCells + 1 - rightSplitCount
    // ... copy loop ...
}
```

## Why biased split for sequential inserts

Table RowIDs are engine-managed and strictly monotone-increasing
(`SeekNextRowID` always returns `max+1`). Every INSERT adds a key greater than
all existing keys, so the biased path is taken on every leaf split.

Result: each leaf page is filled to 100% capacity instead of ~50%, roughly
halving the total page count and WAL frame count for sequential-insert
workloads.

## Interaction with the rightmost-leaf cache

The rightmost-leaf cache (`rightmostTablePage`) is updated inside
`LeafNodeSplitInsert` when the new page has `NextLeaf == 0`:

```go
if newPage.LeafNode.Header.NextLeaf == 0 {
    c.Table.rightmostTablePage.Store(int64(newPage.Index))
}
```

After a biased split, `newPage` (with 1 cell) is the new rightmost leaf and is
cached. The next 4 inserts (until `newPage` fills up) hit the O(1) fast path
without any tree traversal.

## Even-split pre-allocation rule

**The even-split copy loop indexes `newPage.LeafNode.Cells` by position before
`saveToCell` extends the slice.** This is safe only if the slice is
pre-allocated to `rightSplitCount` elements before the loop:

```go
for uint32(len(newPage.LeafNode.Cells)) < rightSplitCount {
    newPage.LeafNode.Cells = append(newPage.LeafNode.Cells, Cell{})
}
```

Without this pre-allocation, any even-split where the new key is **not** at the
rightmost position (e.g. the delete-and-reinsert path in `cursor.update`) will
panic with `index out of range`.

The bug was latent pre-biased-splits because sequential inserts always placed
the new key at position `leafNodeMaxCells` (the very first iteration of the
copy loop calls `saveToCell`, which extends the slice before any direct
assignment). Once biased splits exposed the update delete-and-reinsert path
(see below), the even split could receive a non-rightmost `c.CellIdx`.

## In-place update size check

`cursor.update` checks whether an updated row fits in-place before falling back
to delete-and-reinsert:

```go
// correct: trigger reinsert when net growth exceeds available space
if row.Size() > page.LeafNode.AvailableSpace() + oldRow.Size() {
    // delete old row, reinsert new
}
```

**Do NOT write `AvailableSpace() - oldRow.Size()`.** With biased splits, fully
packed pages have only ~11 bytes of free space (for a typical medium-row
schema), while `oldRow.Size()` is ~53 bytes. The subtraction wraps around in
uint64, making the condition always false and allowing the page to silently
overflow its 4 KB boundary.

The correct condition reads as: "the new row doesn't fit in the space that
would be freed by removing the old row". Available space after removal =
`AvailableSpace() + oldRow.Size()`; trigger reinsert when `row.Size()` exceeds
that.

## When even-split is used

Even split fires for non-sequential workloads:
- Random-key inserts
- Update delete-and-reinsert (same RowID, larger row) when the row can't fit
  in the freed space on a nearly-full page
- Any secondary/unique index insert where keys are not monotone

Even split for the **table** B+ tree is rare in practice (RowIDs are always
sequential), but it is exercised in tests and must be correct.
