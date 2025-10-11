package minisql

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/RichardKnop/minisql/pkg/bitwise"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testTableName = "test_table"

func TestTable_Insert(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := aPager.ForTable(Row{Columns: testColumns}.Size())

	var (
		ctx    = context.Background()
		rows   = gen.Rows(2)
		aTable = NewTable(testLogger, testTableName, testColumns, tablePager, 0)
	)

	t.Run("Insert row with all NOT NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  columnNames(testColumns...),
			Inserts: [][]OptionalValue{rows[0].Values},
		}

		err = aTable.Insert(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))

		actualRow := NewRow(rows[0].Columns)
		err = UnmarshalRow(aPager.pages[0].LeafNode.Cells[0], &actualRow)
		require.NoError(t, err)
		assert.Equal(t, rows[0], actualRow)
		assert.Equal(t, rows[0].NullBitmask(), actualRow.NullBitmask())
		assert.Equal(t, uint64(0), actualRow.NullBitmask())
	})

	t.Run("Insert row with NULL value", func(t *testing.T) {
		rows[1].Values[1] = OptionalValue{Valid: false} // set second column to NULL
		stmt := Statement{
			Kind:    Insert,
			Fields:  columnNames(testColumns...),
			Inserts: [][]OptionalValue{rows[1].Values},
		}

		err = aTable.Insert(ctx, stmt)
		require.NoError(t, err)

		assert.Equal(t, 2, int(aPager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))
		assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Cells[1].Key))

		actualRow := NewRow(rows[1].Columns)
		err = UnmarshalRow(aPager.pages[0].LeafNode.Cells[1], &actualRow)
		require.NoError(t, err)
		assert.Equal(t, rows[1], actualRow)
		assert.Equal(t, rows[1].NullBitmask(), actualRow.NullBitmask())
		assert.Equal(t, bitwise.Set(uint64(0), 1), actualRow.NullBitmask())
	})
}

func TestTable_Insert_MultiInsert(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := aPager.ForTable(Row{Columns: testColumns}.Size())

	var (
		ctx    = context.Background()
		rows   = gen.Rows(3)
		aTable = NewTable(testLogger, testTableName, testColumns, tablePager, 0)
	)

	stmt := Statement{
		Kind:   Insert,
		Fields: columnNames(testColumns...),
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	assert.Equal(t, 3, int(aPager.pages[0].LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))
	assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Cells[1].Key))
	assert.Equal(t, 2, int(aPager.pages[0].LeafNode.Cells[2].Key))

	checkRows(ctx, t, aTable, rows)
}

func TestTable_Insert_SplitRootLeaf(t *testing.T) {
	t.Parallel()

	tempFile, err := os.CreateTemp("", "testdb")
	require.NoError(t, err)
	defer os.Remove(tempFile.Name())
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := aPager.ForTable(Row{Columns: testMediumColumns}.Size())

	var (
		ctx    = context.Background()
		rows   = gen.MediumRows(6)
		aTable = NewTable(testLogger, testTableName, testMediumColumns, tablePager, 0)
	)

	stmt := Statement{
		Kind:   Insert,
		Fields: columnNames(testMediumColumns...),
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	//require.NoError(t, printTree(aTable))

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
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := aPager.ForTable(Row{Columns: testBigColumns}.Size())

	var (
		ctx    = context.Background()
		rows   = gen.BigRows(4)
		aTable = NewTable(testLogger, testTableName, testBigColumns, tablePager, 0)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testBigColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	//require.NoError(t, printTree(aTable))

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
	aPager, err := NewPager(tempFile, PageSize)
	require.NoError(t, err)
	tablePager := aPager.ForTable(Row{Columns: testBigColumns}.Size())

	/*
		In this test we are trying to simulate an internal node split. We will create
		a new tree and start inserting big rows (each row is big enough to take all
		space in a page and will require a new leaf node).
		Internal node has maximum of 340 keys therefor it can have 341 children leafs.
		However, for root node, it has less space available since 100 bytes is reserved.
		That's why we need to try to insert (340-100/12-1)=331 + 2 = 333 new rows, 333th
		row should cause the root node to split into two child internal nodes,
		each inheriting half of leaf nodes.
	*/
	var (
		ctx     = context.Background()
		aTable  = NewTable(testLogger, testTableName, testBigColumns, tablePager, 0)
		numRows = aTable.maxICells(0) + 2
		rows    = gen.BigRows(numRows)
	)

	require.Equal(t, 333, numRows)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testBigColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err = aTable.Insert(ctx, stmt)
	require.NoError(t, err)

	//require.NoError(t, printTree(aTable))
	checkRows(ctx, t, aTable, rows)

	// Assert root node
	aRootPage := aPager.pages[0]
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.True(t, aRootPage.InternalNode.Header.IsRoot)
	assert.True(t, aRootPage.InternalNode.Header.IsInternal)
	assert.Equal(t, 1, int(aRootPage.InternalNode.Header.KeysNum))
	assert.Equal(t, 334, int(aRootPage.InternalNode.Header.RightChild))
	assert.Equal(t, 335, int(aRootPage.InternalNode.ICells[0].Child))
	assert.Equal(t, 165, int(aRootPage.InternalNode.ICells[0].Key))

	// New left internal node should have 166 children (165 keys + right child page).
	// First two pages should be switched (2, 1) as a result of root leaf split
	// but after that it continues as 3, 4, ... 165. Keys are 0, 1, ... 164
	aNewLeftInternal := aPager.pages[335]
	assert.Equal(t, 165, int(aNewLeftInternal.InternalNode.Header.KeysNum))
	assert.False(t, aNewLeftInternal.InternalNode.Header.IsRoot)
	assert.True(t, aNewLeftInternal.InternalNode.Header.IsInternal)
	assert.Equal(t, 0, int(aNewLeftInternal.InternalNode.ICells[0].Key))
	assert.Equal(t, 2, int(aNewLeftInternal.InternalNode.ICells[0].Child))
	assert.Equal(t, 1, int(aNewLeftInternal.InternalNode.ICells[1].Key))
	assert.Equal(t, 1, int(aNewLeftInternal.InternalNode.ICells[1].Child))
	for i := 2; i < 165; i++ {
		assert.Equal(t, i, int(aNewLeftInternal.InternalNode.ICells[i].Key))
		assert.Equal(t, i+1, int(aNewLeftInternal.InternalNode.ICells[i].Child))
	}
	numberOfIcells := int(aNewLeftInternal.InternalNode.Header.KeysNum)
	assert.Equal(t, 164, int(aNewLeftInternal.InternalNode.ICells[numberOfIcells-1].Key))
	assert.Equal(t, 165, int(aNewLeftInternal.InternalNode.ICells[numberOfIcells-1].Child))
	assert.Equal(t, 166, int(aNewLeftInternal.InternalNode.Header.RightChild))

	// New right internal node will have 167 children (166 keys + right child page).
	// Children go from 167, 168, ... 333 and keys from 166, 167, ... 331
	aNewRightInternal := aPager.pages[334]
	assert.Equal(t, 166, int(aNewRightInternal.InternalNode.Header.KeysNum))
	assert.False(t, aNewRightInternal.InternalNode.Header.IsRoot)
	assert.True(t, aNewRightInternal.InternalNode.Header.IsInternal)
	var (
		firstRightKey   = 166
		firstRightChild = 167
	)
	for i := 0; i < 166; i++ {
		assert.Equal(t, firstRightKey, int(aNewRightInternal.InternalNode.ICells[i].Key))
		assert.Equal(t, firstRightChild, int(aNewRightInternal.InternalNode.ICells[i].Child))
		firstRightKey += 1
		firstRightChild += 1
	}
	numberOfIcells = int(aNewRightInternal.InternalNode.Header.KeysNum)
	assert.Equal(t, 331, int(aNewRightInternal.InternalNode.ICells[numberOfIcells-1].Key))
	assert.Equal(t, 332, int(aNewRightInternal.InternalNode.ICells[numberOfIcells-1].Child))
	assert.Equal(t, 333, int(aNewRightInternal.InternalNode.Header.RightChild))

	// 333 leafs nodes should have been created, each with 1 cell,
	// plust root internal node and two new internal nodes
	require.Equal(t, 336, int(aPager.TotalPages()))

	leafs := aPager.pages[1:334]
	require.Len(t, leafs, 333)

	leafs[0], leafs[1] = leafs[1], leafs[0] // switch 1st and 2nd leaf as a result of split
	for i, aLeaf := range leafs {
		assert.Equal(t, 1, int(aLeaf.LeafNode.Header.Cells))
		assert.Equal(t, i, int(aLeaf.LeafNode.Cells[0].Key))
		if i < 166 {
			assert.Equal(t, 335, int(aLeaf.LeafNode.Header.Parent), fmt.Sprintf("parent not 335 %d", i))
		} else {
			assert.Equal(t, 334, int(aLeaf.LeafNode.Header.Parent), fmt.Sprintf("parent not 334 %d", i))
		}
	}
}
