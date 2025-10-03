package minisql

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Insert(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		aRow   = gen.Row()
		aTable = NewTable(testLogger, "foo", testColumns, aPager, 0)
	)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testColumns...),
		Inserts:   [][]OptionalValue{aRow.Values},
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))

	actualRow := NewRow(aRow.Columns)
	err = UnmarshalRow(aPager.pages[0].LeafNode.Cells[0].Value, &actualRow)
	require.NoError(t, err)
	assert.Equal(t, aRow, actualRow)
}

func TestTable_Insert_MultiInsert(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	var (
		ctx                = context.Background()
		aRow, aRow2, aRow3 = gen.Row(), gen.Row(), gen.Row()
		aTable             = NewTable(testLogger, "foo", testColumns, aPager, 0)
	)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testColumns...),
		Inserts:   [][]OptionalValue{aRow.Values, aRow2.Values, aRow3.Values},
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	assert.Equal(t, 3, int(aPager.pages[0].LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))
	assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Cells[1].Key))
	assert.Equal(t, 2, int(aPager.pages[0].LeafNode.Cells[2].Key))

	actualRow := NewRow(aRow.Columns)
	err = UnmarshalRow(aPager.pages[0].LeafNode.Cells[0].Value, &actualRow)
	require.NoError(t, err)
	assert.Equal(t, aRow, actualRow)

	actualRow2 := NewRow(aRow.Columns)
	err = UnmarshalRow(aPager.pages[0].LeafNode.Cells[1].Value, &actualRow2)
	require.NoError(t, err)
	assert.Equal(t, aRow2, actualRow2)

	actualRow3 := NewRow(aRow.Columns)
	err = UnmarshalRow(aPager.pages[0].LeafNode.Cells[2].Value, &actualRow3)
	require.NoError(t, err)
	assert.Equal(t, aRow3, actualRow3)
}

func TestTable_Insert_SplitRootLeaf(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		rows   = gen.MediumRows(6)
		aTable = NewTable(testLogger, "foo", testMediumColumns, aPager, 0)
	)

	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testMediumColumns...),
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	require.NoError(t, printTree(aTable))

	assert.Equal(t, 1, int(aPager.pages[0].InternalNode.Header.KeysNum))
	assert.True(t, aPager.pages[0].InternalNode.Header.IsRoot)
	assert.True(t, aPager.pages[0].InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aPager.pages[0].InternalNode.Header.RightChild))
	assert.Equal(t, 2, int(aPager.pages[0].InternalNode.ICells[0].Child))
	assert.Equal(t, 2, int(aPager.pages[0].InternalNode.ICells[0].Key))

	// Assert left leaf
	leftLeaf := aPager.pages[2]
	assert.False(t, leftLeaf.LeafNode.Header.IsRoot)
	assert.False(t, leftLeaf.LeafNode.Header.IsInternal)
	assert.Equal(t, 0, int(leftLeaf.LeafNode.Header.Parent))
	assert.Equal(t, 3, int(leftLeaf.LeafNode.Header.Cells))
	assert.Equal(t, 1, int(leftLeaf.LeafNode.Header.NextLeaf))

	// // Assert keys in the left leaf
	assert.Equal(t, 0, int(leftLeaf.LeafNode.Cells[0].Key))
	assert.Equal(t, 1, int(leftLeaf.LeafNode.Cells[1].Key))
	assert.Equal(t, 2, int(leftLeaf.LeafNode.Cells[2].Key))

	// Assert right leaf
	rightLeaf := aPager.pages[1]
	assert.False(t, rightLeaf.LeafNode.Header.IsRoot)
	assert.False(t, rightLeaf.LeafNode.Header.IsInternal)
	assert.Equal(t, 0, int(rightLeaf.LeafNode.Header.Parent))
	assert.Equal(t, 3, int(rightLeaf.LeafNode.Header.Cells))
	assert.Equal(t, 0, int(rightLeaf.LeafNode.Header.NextLeaf))

	// // Assert keys in the right leaf
	assert.Equal(t, 3, int(rightLeaf.LeafNode.Cells[0].Key))
	assert.Equal(t, 4, int(rightLeaf.LeafNode.Cells[1].Key))
	assert.Equal(t, 5, int(rightLeaf.LeafNode.Cells[2].Key))

	assert.Equal(t, 3, int(aPager.TotalPages()))
}

func TestTable_Insert_SplitLeaf(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	var (
		ctx    = context.Background()
		rows   = gen.BigRows(4)
		aTable = NewTable(testLogger, "foo", testBigColumns, aPager, 0)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testBigColumns...),
		Inserts:   [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	require.NoError(t, printTree(aTable))

	// Assert root node
	aRootPage := aPager.pages[0]
	assert.Equal(t, 3, int(aRootPage.InternalNode.Header.KeysNum))
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 4, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, 2, int(aRootPage.InternalNode.ICells[0].Child))
	assert.Equal(t, 0, int(aRootPage.InternalNode.ICells[0].Key))
	assert.Equal(t, 1, int(aRootPage.InternalNode.ICells[1].Child))
	assert.Equal(t, 1, int(aRootPage.InternalNode.ICells[1].Key))
	assert.Equal(t, 3, int(aRootPage.InternalNode.ICells[2].Child))
	assert.Equal(t, 2, int(aRootPage.InternalNode.ICells[2].Key))

	// Assert leaf nodes // in order from left to right
	leafs := aPager.pages[1:]
	leafs[0], leafs[1] = leafs[1], leafs[0] // switch 1st and 2nd leaf as a result of split
	for i, aLeaf := range leafs {
		assert.False(t, aLeaf.LeafNode.Header.IsRoot)
		assert.False(t, aLeaf.LeafNode.Header.IsInternal)
		assert.Equal(t, 0, int(aLeaf.LeafNode.Header.Parent))
		assert.Equal(t, 1, int(aLeaf.LeafNode.Header.Cells))
		assert.Equal(t, i, int(aLeaf.LeafNode.Cells[0].Key))
	}
}

func TestTable_Insert_SplitInternalNode_CreateNewRoot(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize, SchemaTableName)
	require.NoError(t, err)

	/*
		In this test we are trying to simulate an internal node split. We will create
		a new tree and start inserting big rows (each row is big enough to take all
		space in a page and will require a new leaf node).
		Internal node has maximum of 340 keys therefor it can have 341 children leafs.
		That's why we need to try to insert 342 new rows, 342th row should cause the
		root node to split into two child internal nodes, each inheriting half of leaf
		nodes.
	*/
	var (
		ctx     = context.Background()
		numRows = InternalNodeMaxCells + 2
		rows    = gen.BigRows(numRows)
		aTable  = NewTable(testLogger, "foo", testBigColumns, aPager, 0)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:      Insert,
		TableName: "foo",
		Fields:    columnNames(testBigColumns...),
		Inserts:   [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	// Assert root node
	aRootPage := aPager.pages[0]
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.Equal(t, 343, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, 344, int(aRootPage.InternalNode.ICells[0].Child))
	assert.Equal(t, 170, int(aRootPage.InternalNode.ICells[0].Key))

	// New left internal node should have 171 cells (170 + right child page).
	// First two pages should be switched (2, 1) as a result of root leaf split
	// but after that it continues as 3, 4, ... 171. Keys are 0, 1, ... 170
	aNewLeftInternal := aPager.pages[344]
	assert.Equal(t, 170, int(aNewLeftInternal.InternalNode.Header.KeysNum))
	assert.False(t, aNewLeftInternal.InternalNode.Header.IsRoot)
	assert.True(t, aNewLeftInternal.InternalNode.Header.IsInternal)
	assert.Equal(t, 0, int(aNewLeftInternal.InternalNode.ICells[0].Key))
	assert.Equal(t, 2, int(aNewLeftInternal.InternalNode.ICells[0].Child))
	assert.Equal(t, 1, int(aNewLeftInternal.InternalNode.ICells[1].Key))
	assert.Equal(t, 1, int(aNewLeftInternal.InternalNode.ICells[1].Child))
	for i := 2; i < 170; i++ {
		assert.Equal(t, i, int(aNewLeftInternal.InternalNode.ICells[i].Key))
		assert.Equal(t, i+1, int(aNewLeftInternal.InternalNode.ICells[i].Child))
	}
	assert.Equal(t, 171, int(aNewLeftInternal.InternalNode.Header.RightChild))

	// New right internal node will have 169 cells.
	// Children go from 172, 173, ... 342 and keys from 171, 172, ... 341
	aNewRightInternal := aPager.pages[343]
	assert.Equal(t, 170, int(aNewRightInternal.InternalNode.Header.KeysNum))
	assert.False(t, aNewRightInternal.InternalNode.Header.IsRoot)
	assert.True(t, aNewRightInternal.InternalNode.Header.IsInternal)
	var (
		firstRightKey   = 171
		firstRightChild = 172
	)
	for i := 0; i < 170; i++ {
		assert.Equal(t, firstRightKey, int(aNewRightInternal.InternalNode.ICells[i].Key))
		assert.Equal(t, firstRightChild, int(aNewRightInternal.InternalNode.ICells[i].Child))
		firstRightKey += 1
		firstRightChild += 1
	}
	assert.Equal(t, 340, int(aNewRightInternal.InternalNode.ICells[169].Key))
	assert.Equal(t, 341, int(aNewRightInternal.InternalNode.ICells[169].Child))
	assert.Equal(t, 342, int(aNewRightInternal.InternalNode.Header.RightChild))

	assert.Len(t, aPager.pages, 345)
	leafs := aPager.pages[1:343]
	leafs[0], leafs[1] = leafs[1], leafs[0] // switch 1st and 2nd leaf as a result of split
	for i, aLeaf := range leafs {
		assert.Equal(t, 1, int(aLeaf.LeafNode.Header.Cells))
		assert.Equal(t, i, int(aLeaf.LeafNode.Cells[0].Key))
		if i < 171 {
			assert.Equal(t, 344, int(aLeaf.LeafNode.Header.Parent), fmt.Sprintf("parent not 344 %d", i))
		} else {
			assert.Equal(t, 343, int(aLeaf.LeafNode.Header.Parent), fmt.Sprintf("parent not 343 %d", i))
		}
	}
}
