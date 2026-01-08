package minisql

import (
	"context"
	"fmt"
	"testing"

	"github.com/RichardKnop/minisql/pkg/bitwise"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Insert(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		rows           = gen.Rows(2)
		tablePager     = aPager.ForTable(testColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable         = NewTable(testLogger, txPager, txManager, testTableName, testColumns, 0)
	)

	t.Run("Insert row with all NOT NULL values", func(t *testing.T) {
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(testColumns...),
			Inserts: [][]OptionalValue{rows[0].Values},
		}

		mustInsert(t, ctx, aTable, txManager, stmt)

		assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))

		aCursor, err := aTable.SeekLast(ctx, 0)
		require.NoError(t, err)
		actualRow, err := aCursor.fetchRow(ctx, false, fieldsFromColumns(testColumns...)...)
		require.NoError(t, err)

		require.NoError(t, err)
		assert.Equal(t, rows[0], actualRow)
		assert.Equal(t, rows[0].NullBitmask(), actualRow.NullBitmask())
		assert.Equal(t, uint64(0), actualRow.NullBitmask())
	})

	t.Run("Insert row with NULL value", func(t *testing.T) {
		rows[1].Values[1] = OptionalValue{Valid: false} // set second column to NULL
		stmt := Statement{
			Kind:    Insert,
			Fields:  fieldsFromColumns(testColumns...),
			Inserts: [][]OptionalValue{rows[1].Values},
		}

		mustInsert(t, ctx, aTable, txManager, stmt)

		assert.Equal(t, 2, int(aPager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))
		assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Cells[1].Key))

		aCursor, err := aTable.SeekLast(ctx, 0)
		require.NoError(t, err)
		actualRow, err := aCursor.fetchRow(ctx, false, fieldsFromColumns(testColumns...)...)
		require.NoError(t, err)

		assert.Equal(t, rows[1], actualRow)
		assert.Equal(t, rows[1].NullBitmask(), actualRow.NullBitmask())
		assert.Equal(t, bitwise.Set(uint64(0), 1), actualRow.NullBitmask())
	})
}

func TestTable_Insert_MultiInsert(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		rows           = gen.Rows(3)
		tablePager     = aPager.ForTable(testColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable         = NewTable(testLogger, txPager, txManager, testTableName, testColumns, 0)
	)

	stmt := Statement{
		Kind:   Insert,
		Fields: fieldsFromColumns(testColumns...),
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	mustInsert(t, ctx, aTable, txManager, stmt)

	assert.Equal(t, 3, int(aPager.pages[0].LeafNode.Header.Cells))
	assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Cells[0].Key))
	assert.Equal(t, 1, int(aPager.pages[0].LeafNode.Cells[1].Key))
	assert.Equal(t, 2, int(aPager.pages[0].LeafNode.Cells[2].Key))

	checkRows(ctx, t, aTable, rows)
}

func TestTable_Insert_SplitRootLeaf(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		rows           = gen.MediumRows(6)
		tablePager     = aPager.ForTable(testMediumColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0)
	)

	stmt := Statement{
		Kind:   Insert,
		Fields: fieldsFromColumns(testMediumColumns...),
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	mustInsert(t, ctx, aTable, txManager, stmt)

	//require.NoError(t, aTable.print())

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
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		rows           = gen.BigRows(4)
		tablePager     = aPager.ForTable(testBigColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable         = NewTable(testLogger, txPager, txManager, testTableName, testBigColumns, 0)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testBigColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	mustInsert(t, ctx, aTable, txManager, stmt)

	//require.NoError(t, aTable.print())

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
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testBigColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable         = NewTable(testLogger, txPager, txManager, testTableName, testBigColumns, 0)
		numRows        = aTable.maxICells(0) + 2
		rows           = gen.BigRows(numRows)
	)

	require.Equal(t, 333, numRows)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testBigColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	mustInsert(t, ctx, aTable, txManager, stmt)

	//require.NoError(t, aTable.print())
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

func TestTable_Insert_Overflow(t *testing.T) {
	var (
		aPager, dbFile = initTest(t)
		ctx            = context.Background()
		tablePager     = aPager.ForTable(testOverflowColumns)
		txManager      = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), aPager, nil)
		txPager        = NewTransactionalPager(tablePager, txManager, testTableName, "")
		aTable         = NewTable(testLogger, txPager, txManager, testTableName, testOverflowColumns, 0)
		rows           = []Row{
			gen.OverflowRow(MaxInlineVarchar),
			gen.OverflowRow(MaxInlineVarchar + 100),
			gen.OverflowRow(MaxOverflowPageData + 100),
		}
	)

	t.Run("First insert non overflow text", func(t *testing.T) {
		stmt := Statement{
			Kind:   Insert,
			Fields: fieldsFromColumns(testOverflowColumns...),
		}
		stmt.Inserts = append(stmt.Inserts, rows[0].Values)

		mustInsert(t, ctx, aTable, txManager, stmt)

		require.Equal(t, 1, int(aPager.TotalPages()))
		assert.NotNil(t, aPager.pages[0].LeafNode)
	})

	t.Run("Now insert a text that will overflow to a single page", func(t *testing.T) {
		stmt := Statement{
			Kind:   Insert,
			Fields: fieldsFromColumns(testOverflowColumns...),
		}
		stmt.Inserts = append(stmt.Inserts, rows[1].Values)

		mustInsert(t, ctx, aTable, txManager, stmt)

		require.Equal(t, 2, int(aPager.TotalPages()))
		assert.NotNil(t, aPager.pages[0].LeafNode)
		assert.NotNil(t, aPager.pages[1].OverflowPage)
		assert.Equal(t, 0, int(aPager.pages[1].OverflowPage.Header.NextPage))
		assert.Equal(t, 355, int(aPager.pages[1].OverflowPage.Header.DataSize))
	})

	t.Run("Now insert a text that will overflow to a 2 pages", func(t *testing.T) {
		stmt := Statement{
			Kind:   Insert,
			Fields: fieldsFromColumns(testOverflowColumns...),
		}
		stmt.Inserts = append(stmt.Inserts, rows[2].Values)

		mustInsert(t, ctx, aTable, txManager, stmt)

		require.Equal(t, 4, int(aPager.TotalPages()))
		assert.NotNil(t, aPager.pages[0].LeafNode)
		assert.NotNil(t, aPager.pages[1].OverflowPage)
		assert.NotNil(t, aPager.pages[2].OverflowPage)
		assert.NotNil(t, aPager.pages[3].OverflowPage)

		assert.Equal(t, aPager.pages[3].Index, aPager.pages[2].OverflowPage.Header.NextPage)
		assert.Equal(t, 0, int(aPager.pages[3].OverflowPage.Header.NextPage))

		assert.Equal(t, MaxOverflowPageData, int(aPager.pages[2].OverflowPage.Header.DataSize))
		assert.Equal(t, 100, int(aPager.pages[3].OverflowPage.Header.DataSize))
	})
}

func mustInsert(t *testing.T, ctx context.Context, aTable *Table, txManager *TransactionManager, stmt Statement) {
	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		_, err := aTable.Insert(ctx, stmt)
		return err
	})
	require.NoError(t, err)
}

func checkRows(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
	selectResult, err := aTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: fieldsFromColumns(aTable.Columns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.Value.(int64)] = struct{}{}
	}

	var actual []Row
	for selectResult.Rows.Next(ctx) {
		aRow := selectResult.Rows.Row()
		actual = append(actual, aRow)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[aRow.Values[0].Value.(int64)]
			assert.True(t, ok)
		}
	}
	require.NoError(t, selectResult.Rows.Err())

	require.Len(t, actual, len(expectedRows))
	for i := range len(expectedRows) {
		assert.Equal(t, expectedRows[i].Key, actual[i].Key, "row key %d does not match expected %d", i)
		assert.Equal(t, expectedRows[i].Columns, actual[i].Columns, "row columns %d does not match expected", i)
		// Compare values, for text values, we don't want to compare pointers to overflow pages
		for j, aValue := range expectedRows[i].Values {
			tp, ok := aValue.Value.(TextPointer)
			if ok {
				assert.Equal(t, int(tp.Length), int(actual[i].Values[j].Value.(TextPointer).Length), "row %d text pointer length %d does not match expected", i, j)
				assert.Equal(t, tp.Data, actual[i].Values[j].Value.(TextPointer).Data, "row %d text pointer data %d does not match expected", i, j)
			} else {
				assert.Equal(t, actual[i].Values[j], expectedRows[i].Values[j], "row %d value %d does not match expected", i, j)
			}
		}
		assert.Equal(t, expectedRows[i].NullBitmask(), actual[i].NullBitmask(), "row %d null bitmask does not match expected", i)
	}
}
