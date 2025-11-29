package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTable_Delete_RootLeafNode(t *testing.T) {
	/*
		In this test we will be deleting from a root leaf node only tree.
	*/
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		numRows    = 5
		rows       = gen.MediumRows(numRows)
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(testMediumColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testMediumColumns, 0)
	)

	// Set some values to NULL so we can test selecting/filtering on NULLs
	rows[1].Values[2] = OptionalValue{Valid: false}
	rows[3].Values[5] = OptionalValue{Valid: false}
	rows[4].Values[5] = OptionalValue{Valid: false}

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, stmt)
	}, aPager)
	require.NoError(t, err)

	t.Run("Delete rows with NULL values when no rows match", func(t *testing.T) {
		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: NewOneOrMore(Conditions{FieldIsNull("id")}),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 0, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows)
	})

	t.Run("Delete one row", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, id.Value.(int64)),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[1:])
	})

	t.Run("Delete rows with NULL values", func(t *testing.T) {
		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: NewOneOrMore(Conditions{FieldIsNull("age")}),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[2:])
	})

	t.Run("Delete rows with NOT NULL values", func(t *testing.T) {
		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: NewOneOrMore(Conditions{FieldIsNotNull("test_double")}),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[3:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind: Delete,
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 2, aResult.RowsAffected)
		checkRows(ctx, t, aTable, nil)
	})

	// Root page is never recycled
	assert.Equal(t, 0, int(aPager.dbHeader.FirstFreePage))
	assert.Equal(t, 0, int(aPager.dbHeader.FreePageCount))
}

func TestTable_Delete_LeafNodeRebalancing(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		numRows    = 20
		rows       = gen.MediumRows(numRows)
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(testMediumColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testMediumColumns, 0)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, stmt)
	}, aPager)
	require.NoError(t, err)

	/*
		Initial state of the tree:

		           +------------------------------------------------+
		           |   2,       5,       8,         11,        14   |
		           +------------------------------------------------+
		          /       /         /        /             /         \
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
		| 0,1,2 |  | 3,4,5 |  | 6,7,8 |  | 9,10,11 |  | 12,13,14 |  | 15,16,17,18,19 |
		+-------+  +-------+  +-------+  +---------+  +----------+  +----------------+
	*/

	//require.NoError(t, aTable.print())

	// Check the root page
	assert.Equal(t, 5, int(aPager.pages[0].InternalNode.Header.KeysNum))
	assert.Equal(t, []uint64{2, 5, 8, 11, 14}, aPager.pages[0].InternalNode.Keys())
	// Check the leaf pages
	assert.Equal(t, []uint64{0, 1, 2}, aPager.pages[2].LeafNode.Keys())
	assert.Equal(t, []uint64{3, 4, 5}, aPager.pages[1].LeafNode.Keys())
	assert.Equal(t, []uint64{6, 7, 8}, aPager.pages[3].LeafNode.Keys())
	assert.Equal(t, []uint64{9, 10, 11}, aPager.pages[4].LeafNode.Keys())
	assert.Equal(t, []uint64{12, 13, 14}, aPager.pages[5].LeafNode.Keys())
	assert.Equal(t, []uint64{15, 16, 17, 18, 19}, aPager.pages[6].LeafNode.Keys())

	t.Run("Delete first row to force merging of first two leaves", func(t *testing.T) {
		ids := rowIDs(rows[0])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[1:])

		/*
				          +----------------------------------------------+
				          |      5,        8,         11,        14      |
				          +----------------------------------------------+
				         /           /          /            /           \
			+-----------+     +-------+    +---------+    +----------+     +----------------+
			| 1,2,3,4,5 |     | 6,7,8 |    | 9,10,11 |    | 12,13,14 |     | 15,16,17,18,19 |
			+-----------+     +-------+    +---------+    +----------+     +----------------+
		*/

		//require.NoError(t, aTable.print())

		// Check the root page
		assert.Equal(t, 4, int(aPager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []uint64{5, 8, 11, 14}, aPager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []uint64{1, 2, 3, 4, 5}, aPager.pages[2].LeafNode.Keys())
		// leafs[1] has been merged into leafs[0]
		assert.Equal(t, []uint64{6, 7, 8}, aPager.pages[3].LeafNode.Keys())
		assert.Equal(t, []uint64{9, 10, 11}, aPager.pages[4].LeafNode.Keys())
		assert.Equal(t, []uint64{12, 13, 14}, aPager.pages[5].LeafNode.Keys())
		assert.Equal(t, []uint64{15, 16, 17, 18, 19}, aPager.pages[6].LeafNode.Keys())
		// Check that leafs[1] is now a free page
		assert.NotNil(t, aPager.pages[1].FreePage)
		assert.Nil(t, aPager.pages[1].LeafNode)
		assert.Nil(t, aPager.pages[1].InternalNode)
		assert.Equal(t, 0, int(aPager.pages[1].FreePage.NextFreePage))
		assert.Equal(t, int(aPager.pages[1].Index), int(aPager.dbHeader.FirstFreePage))
		assert.Equal(t, 1, int(aPager.dbHeader.FreePageCount))
	})

	t.Run("Delete last three rows to force merging of last two leaves", func(t *testing.T) {
		ids := rowIDs(rows[17], rows[18], rows[19])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 3, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[1:17])

		/*
				          +------------------------------------+
				          |      5,        8,         11,      |
				          +------------------------------------+
				         /           /          /               \
			+-----------+     +-------+    +---------+    +----------------+
			| 1,2,3,4,5 |     | 6,7,8 |    | 9,10,11 |    | 12,13,14,15,16 |
			+-----------+     +-------+    +---------+    +----------------+
		*/

		//require.NoError(t, aTable.print())

		// Check the root page
		assert.Equal(t, 3, int(aPager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []uint64{5, 8, 11}, aPager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []uint64{1, 2, 3, 4, 5}, aPager.pages[2].LeafNode.Keys())
		assert.Equal(t, []uint64{6, 7, 8}, aPager.pages[3].LeafNode.Keys())
		assert.Equal(t, []uint64{9, 10, 11}, aPager.pages[4].LeafNode.Keys())
		assert.Equal(t, []uint64{12, 13, 14, 15, 16}, aPager.pages[5].LeafNode.Keys())
		// Check that leafs[6] is now a free page
		assert.NotNil(t, aPager.pages[6].FreePage)
		assert.Nil(t, aPager.pages[6].LeafNode)
		assert.Nil(t, aPager.pages[6].InternalNode)
		assert.Equal(t, int(aPager.pages[1].Index), int(aPager.pages[6].FreePage.NextFreePage))
		assert.Equal(t, int(aPager.pages[6].Index), int(aPager.dbHeader.FirstFreePage))
		assert.Equal(t, 2, int(aPager.dbHeader.FreePageCount))
	})

	t.Run("Keep deleting more rows, another merge", func(t *testing.T) {
		ids := rowIDs(rows[2], rows[4], rows[6])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 3, aResult.RowsAffected)
		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[3], rows[5], rows[7], rows[8],
			rows[9], rows[10], rows[11],
			rows[12], rows[13], rows[14], rows[15], rows[16],
		})

		/*
			           +----------------------------+
			           |        8,         11,      |
			           +----------------------------+
			          /              /               \
			+-----------+      +---------+      +----------------+
			| 1,3,5,7,8 |      | 9,10,11 |      | 12,13,14,15,16 |
			+-----------+      +---------+      +----------------+
		*/

		//require.NoError(t, aTable.print())

		// Check the root page
		assert.Equal(t, 2, int(aPager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []uint64{8, 11}, aPager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []uint64{1, 3, 5, 7, 8}, aPager.pages[2].LeafNode.Keys())
		assert.Equal(t, []uint64{9, 10, 11}, aPager.pages[4].LeafNode.Keys())
		assert.Equal(t, []uint64{12, 13, 14, 15, 16}, aPager.pages[5].LeafNode.Keys())
		// Check that leafs[3] is now a free page
		assert.NotNil(t, aPager.pages[3].FreePage)
		assert.Nil(t, aPager.pages[3].LeafNode)
		assert.Nil(t, aPager.pages[3].InternalNode)
		assert.Equal(t, int(aPager.pages[6].Index), int(aPager.pages[3].FreePage.NextFreePage))
		assert.Equal(t, int(aPager.pages[3].Index), int(aPager.dbHeader.FirstFreePage))
		assert.Equal(t, 3, int(aPager.dbHeader.FreePageCount))
	})

	t.Run("Keep deleting more rows, no merge", func(t *testing.T) {
		ids := rowIDs(rows[9], rows[11], rows[13], rows[15])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 4, aResult.RowsAffected)
		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[3], rows[5],
			rows[7], rows[8], rows[10],
			rows[12], rows[14], rows[16],
		})

		/*
			           +----------------------------+
			           |        5,         11,      |
			           +----------------------------+
			          /              /               \
			+--------+           +--------+          +----------+
			| 1,3,5, |           | 7,8,10 |          | 12,14,16 |
			+--------+           +--------+          +----------+
		*/

		//require.NoError(t, aTable.print())

		// Check the root page
		assert.Equal(t, 2, int(aPager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []uint64{5, 11}, aPager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []uint64{1, 3, 5}, aPager.pages[2].LeafNode.Keys())
		assert.Equal(t, []uint64{7, 8, 10}, aPager.pages[4].LeafNode.Keys())
		assert.Equal(t, []uint64{12, 14, 16}, aPager.pages[5].LeafNode.Keys())
	})

	t.Run("Keep deleting more rows, another merge and borrow", func(t *testing.T) {
		ids := rowIDs(rows[3], rows[12], rows[5])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 3, aResult.RowsAffected)
		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[7], rows[8],
			rows[10], rows[14], rows[16],
		})

		/*
		           +-------------+
		           |      8      |
		           +-------------+
		          /               \
		 +-------+                +----------+
		 | 1,7,8 |                | 10,14,16 |
		 +-------+                +----------+
		*/

		//require.NoError(t, aTable.print())

		// Check the root page
		assert.Equal(t, 1, int(aPager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []uint64{8}, aPager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []uint64{1, 7, 8}, aPager.pages[2].LeafNode.Keys())
		assert.Equal(t, []uint64{10, 14, 16}, aPager.pages[5].LeafNode.Keys())
		// Check that leafs[4] is now a free page
		assert.NotNil(t, aPager.pages[4].FreePage)
		assert.Nil(t, aPager.pages[4].LeafNode)
		assert.Nil(t, aPager.pages[4].InternalNode)
		assert.Equal(t, int(aPager.pages[3].Index), int(aPager.pages[4].FreePage.NextFreePage))
		assert.Equal(t, int(aPager.pages[4].Index), int(aPager.dbHeader.FirstFreePage))
		assert.Equal(t, 4, int(aPager.dbHeader.FreePageCount))
	})

	t.Run("Delete one more time, we are left with only root leaf node", func(t *testing.T) {
		ids := rowIDs(rows[14])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, []Row{
			rows[1], rows[7], rows[8],
			rows[10], rows[16],
		})

		/*
		   +-----------------+
		   | 1, 7, 8, 10, 16 |
		   +-----------------+
		*/

		//require.NoError(t, aTable.print())

		assert.Nil(t, aPager.pages[0].InternalNode)
		assert.Equal(t, 5, int(aPager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Header.Parent))
		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Header.NextLeaf))
		assert.Equal(t, []uint64{1, 7, 8, 10, 16}, aPager.pages[0].LeafNode.Keys())
		// Check there are two more free pages (6 in total now)
		assert.NotNil(t, aPager.pages[5].FreePage)
		assert.Nil(t, aPager.pages[5].LeafNode)
		assert.Nil(t, aPager.pages[5].InternalNode)
		assert.Equal(t, int(aPager.pages[2].Index), int(aPager.pages[5].FreePage.NextFreePage))
		assert.Equal(t, int(aPager.pages[5].Index), int(aPager.dbHeader.FirstFreePage))
		assert.Equal(t, 6, int(aPager.dbHeader.FreePageCount))
	})

	t.Run("Delete all remaining rows", func(t *testing.T) {
		ids := rowIDs(rows[1], rows[7], rows[8], rows[10], rows[16])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 5, aResult.RowsAffected)
		checkRows(ctx, t, aTable, nil)

		//require.NoError(t, aTable.print())

		assert.Equal(t, 0, int(aPager.pages[0].LeafNode.Header.Cells))
	})

	assert.Equal(t, 7, int(aPager.TotalPages()))
	// Root page cannot be recycled so there should still be just 6 free pages
	assert.NotNil(t, aPager.pages[5].FreePage)
	assert.Nil(t, aPager.pages[5].LeafNode)
	assert.Nil(t, aPager.pages[5].InternalNode)
	assert.Nil(t, aPager.pages[5].OverflowPage)
	assert.Equal(t, int(aPager.pages[2].Index), int(aPager.pages[5].FreePage.NextFreePage))
	assert.Equal(t, int(aPager.pages[5].Index), int(aPager.dbHeader.FirstFreePage))
	assert.Equal(t, 6, int(aPager.dbHeader.FreePageCount))
}

func TestTable_Delete_InternalNodeRebalancing(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		numRows    = 100
		rows       = gen.MediumRows(numRows)
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(testMediumColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testMediumColumns, 0)
	)
	aTable.maximumICells = 5 // for testing purposes only, normally 340

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		stmt.Inserts = append(stmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, stmt)
	}, aPager)
	require.NoError(t, err)

	//fmt.Println("BEFORE")
	//require.NoError(t, aTable.print())

	checkRows(ctx, t, aTable, rows)
	assert.Equal(t, 47, int(aPager.TotalPages()))

	var aResult StatementResult
	err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		var err error
		aResult, err = aTable.Delete(ctx, Statement{
			Kind: Delete,
		})
		return err
	}, aPager)
	require.NoError(t, err)

	assert.Equal(t, len(rows), aResult.RowsAffected)

	//fmt.Println("AFTER")
	//require.NoError(t, aTable.print())

	checkRows(ctx, t, aTable, nil)

	assert.Equal(t, 47, int(aPager.TotalPages()))
	assert.Equal(t, 46, int(aPager.dbHeader.FreePageCount))
}

func TestTable_Delete_Overflow(t *testing.T) {
	var (
		aPager     = initTest(t)
		ctx        = context.Background()
		txManager  = NewTransactionManager()
		tablePager = NewTransactionalPager(
			aPager.ForTable(testOverflowColumns),
			txManager,
		)
		aTable = NewTable(testLogger, tablePager, txManager, testTableName, testOverflowColumns, 0)
		rows   = gen.OverflowRows(3, []uint32{
			MaxInlineVarchar,          // inline text
			MaxInlineVarchar + 100,    // text overflows to 1 page
			MaxOverflowPageData + 100, // text overflows to multiple pages
		})
	)

	// Batch insert test rows
	insertStmt := Statement{
		Kind:    Insert,
		Fields:  columnNames(testOverflowColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, aRow := range rows {
		insertStmt.Inserts = append(insertStmt.Inserts, aRow.Values)
	}

	err := txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
		return aTable.Insert(ctx, insertStmt)
	}, aPager)
	require.NoError(t, err)

	require.Equal(t, 4, int(aPager.TotalPages()))

	t.Run("Delete inline non overflowing row", func(t *testing.T) {
		ids := rowIDs(rows[0])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 1, aResult.RowsAffected)
		checkRows(ctx, t, aTable, rows[1:])

		require.Equal(t, 4, int(aPager.TotalPages()))
		assertFreePages(t, tablePager, nil)
	})

	t.Run("Delete overflowing rows", func(t *testing.T) {
		ids := rowIDs(rows[1], rows[2])

		var aResult StatementResult
		err = txManager.ExecuteInTransaction(ctx, func(ctx context.Context) error {
			var err error
			aResult, err = aTable.Delete(ctx, Statement{
				Kind:       Delete,
				Conditions: FieldIsInAny("id", OperandInteger, ids...),
			})
			return err
		}, aPager)
		require.NoError(t, err)

		assert.Equal(t, 2, aResult.RowsAffected)
		checkRows(ctx, t, aTable, nil)

		require.Equal(t, 4, int(aPager.TotalPages()))
		assertFreePages(t, tablePager, []PageIndex{3, 2, 1})
	})
}

func rowIDs(rows ...Row) []any {
	ids := make([]any, 0, len(rows))
	for _, r := range rows {
		id, ok := r.GetValue("id")
		if ok {
			ids = append(ids, id.Value.(int64))
		}
	}
	return ids
}

func checkRows(ctx context.Context, t *testing.T, aTable *Table, expectedRows []Row) {
	selectResult, err := aTable.Select(ctx, Statement{
		Kind:   Select,
		Fields: columnNames(testColumns...),
	})
	require.NoError(t, err)

	expectedIDMap := map[int64]struct{}{}
	for _, r := range expectedRows {
		id, ok := r.GetValue("id")
		require.True(t, ok)
		expectedIDMap[id.Value.(int64)] = struct{}{}
	}

	var actual []Row
	aRow, err := selectResult.Rows(ctx)
	for ; err == nil; aRow, err = selectResult.Rows(ctx) {
		actual = append(actual, aRow)
		if len(expectedIDMap) > 0 {
			_, ok := expectedIDMap[aRow.Values[0].Value.(int64)]
			assert.True(t, ok)
		}
	}

	require.Len(t, actual, len(expectedRows))
	for i := range len(expectedRows) {
		assert.Equal(t, actual[i].Key, expectedRows[i].Key, "row key %d does not match expected", i)
		assert.Equal(t, actual[i].Columns, expectedRows[i].Columns, "row columns %d does not match expected", i)
		// Compare values, for text values, we don't want to compare pointers to overflow pages
		for j, aValue := range expectedRows[i].Values {
			tp, ok := aValue.Value.(TextPointer)
			if ok {
				assert.Equal(t, tp.Length, actual[i].Values[j].Value.(TextPointer).Length, "row %d text pointer length %d does not match expected", i, j)
				assert.Equal(t, tp.Data, actual[i].Values[j].Value.(TextPointer).Data, "row %d text pointer data %d does not match expected", i, j)
			} else {
				assert.Equal(t, actual[i].Values[j], expectedRows[i].Values[j], "row %d value %d does not match expected", i, j)
			}
		}
		assert.Equal(t, actual[i].NullBitmask(), expectedRows[i].NullBitmask(), "row %d null bitmask does not match expected", i)
	}
}
