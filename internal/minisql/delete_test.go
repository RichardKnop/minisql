package minisql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestTable_Delete_RootLeafNode(t *testing.T) {
	/*
		In this test we will be deleting from a root leaf node only tree.
	*/
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		numRows       = 5
		rows          = gen.MediumRows(numRows)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)

	// Set some values to NULL so we can test selecting/filtering on NULLs
	rows[1].Values[2] = MakeNull()
	rows[3].Values[5] = MakeNull()
	rows[4].Values[5] = MakeNull()

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	t.Run("Delete rows with NULL values when no rows match", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind:       Delete,
			Conditions: NewOneOrMore(Conditions{FieldIsNull(Field{Name: "id"})}),
		})

		assert.Equal(t, 0, result.RowsAffected)
		checkRows(ctx, t, table, rows)
	})

	t.Run("Delete one row", func(t *testing.T) {
		id, ok := rows[0].GetValue("id")
		require.True(t, ok)

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsEqual(Field{Name: "id"}, OperandInteger, id.AsInt8()),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])
	})

	t.Run("Delete rows with NULL values", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind:       Delete,
			Conditions: NewOneOrMore(Conditions{FieldIsNull(Field{Name: "age"})}),
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[2:])
	})

	t.Run("Delete rows with NOT NULL values", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind:       Delete,
			Conditions: NewOneOrMore(Conditions{FieldIsNotNull(Field{Name: "created"})}),
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[3:])
	})

	t.Run("Delete all rows", func(t *testing.T) {
		result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

		assert.Equal(t, 2, result.RowsAffected)
		checkRows(ctx, t, table, nil)
	})

	// Root page is never recycled
	assert.Equal(t, 0, int(pager.dbHeader.FirstFreePage))
	assert.Equal(t, 0, int(pager.dbHeader.FreePageCount))
}

func TestTable_Delete_LeafNodeRebalancing(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		numRows       = 20
		rows          = gen.MediumRows(numRows)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	/*
		Initial state of the tree (biased splits pack existing cells left, new key goes right):

		               +--------------------------------------+
		               |        4,        9,        14.       |
		               +--------------------------------------+
		              /             /           \              \
		+-----------+    +-----------+    +----------------+    +--------------+
		| 0,1,2,3,4 |    | 5,6,7,8,9 |    | 10,11,12,13,14 |    |15,16,17,18,19|
		+-----------+    +-----------+    +----------------+    +--------------+
		  page 2           page 1           page 3                page 4
	*/

	// Check the root page
	assert.Equal(t, 3, int(pager.pages[0].InternalNode.Header.KeysNum))
	assert.Equal(t, []RowID{4, 9, 14}, pager.pages[0].InternalNode.Keys())
	// Check the leaf pages
	assert.Equal(t, []RowID{0, 1, 2, 3, 4}, pager.pages[2].LeafNode.Keys())
	assert.Equal(t, []RowID{5, 6, 7, 8, 9}, pager.pages[1].LeafNode.Keys())
	assert.Equal(t, []RowID{10, 11, 12, 13, 14}, pager.pages[3].LeafNode.Keys())
	assert.Equal(t, []RowID{15, 16, 17, 18, 19}, pager.pages[4].LeafNode.Keys())

	t.Run("Delete 3 rows from first leaf to trigger borrow from right", func(t *testing.T) {
		ids := rowIDs(rows[0], rows[1], rows[2])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, rows[3:])

		/*
			page2 underflows (2 cells) → borrows row 5 from right neighbour page1 (5 cells → can lend).

			               +-----------------------------------+
			               |     5,        9,        14        |
			               +-----------------------------------+
			              /           /           \             \
			 +-----------+    +-----------+   +-----------+      +-----------+
			 |   3,4,5   |    |  6,7,8,9  |   | 10,...,14 |      | 15,...,19 |
			 +-----------+    +-----------+   +-----------+      +-----------+
		*/

		// Check the root page
		assert.Equal(t, 3, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{5, 9, 14}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{3, 4, 5}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{6, 7, 8, 9}, pager.pages[1].LeafNode.Keys())
		assert.Equal(t, []RowID{10, 11, 12, 13, 14}, pager.pages[3].LeafNode.Keys())
		assert.Equal(t, []RowID{15, 16, 17, 18, 19}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, 0, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete 3 rows from last leaf to trigger borrow from left", func(t *testing.T) {
		ids := rowIDs(rows[17], rows[18], rows[19])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, rows[3:17])

		/*
			page4 underflows (2 cells) → borrows row 14 from left neighbour page3 (5 cells → can lend).

			               +----------------------------------+
			               |      5,       9,       13        |
			               +----------------------------------+
			              /           /          \             \
			 +-----------+    +-----------+    +-----------+    +-----------+
			 |   3,4,5   |    |  6,7,8,9  |    |10,11,12,13|    | 14,15,16  |
			 +-----------+    +-----------+    +-----------+    +-----------+
		*/

		// Check the root page
		assert.Equal(t, 3, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{5, 9, 13}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{3, 4, 5}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{6, 7, 8, 9}, pager.pages[1].LeafNode.Keys())
		assert.Equal(t, []RowID{10, 11, 12, 13}, pager.pages[3].LeafNode.Keys())
		assert.Equal(t, []RowID{14, 15, 16}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, 0, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete 3 rows to trigger merge of second leaf into first", func(t *testing.T) {
		ids := rowIDs(rows[6], rows[7], rows[8])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, append(append([]Row(nil), rows[3:6]...), rows[9:17]...))

		/*
			page1 underflows (1 cell) → left neighbour page2 has only 3 cells (cannot lend) → merge.

			               +-------------------+
			               |   9,       13     |
			               +-------------------+
			              /         |           \
			 +-----------+    +-------------+    +-----------+
			 |  3,4,5,9  |    | 10,11,12,13 |    | 14,15,16  |
			 +-----------+    +-------------+    +-----------+
			  page 2           page 3             page 4
		*/

		// Check the root page
		assert.Equal(t, 2, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{9, 13}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{3, 4, 5, 9}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{10, 11, 12, 13}, pager.pages[3].LeafNode.Keys())
		assert.Equal(t, []RowID{14, 15, 16}, pager.pages[4].LeafNode.Keys())
		// page1 is now a free page
		assert.NotNil(t, pager.pages[1].FreePage)
		assert.Nil(t, pager.pages[1].LeafNode)
		assert.Nil(t, pager.pages[1].InternalNode)
		assert.Equal(t, 0, int(pager.pages[1].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[1].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 1, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete rows 9 and 10, no rebalancing", func(t *testing.T) {
		ids := rowIDs(rows[9], rows[10])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 2, result.RowsAffected)
		checkRows(ctx, t, table, append(append([]Row(nil), rows[3:6]...), rows[11:17]...))

		/*
			Row 9 removed from page2 (still 3 cells, no underflow).
			Row 10 removed from page3 (still 3 cells, no underflow).

			               +--------------------+
			               |    5,       13     |
			               +--------------------+
			              /         |           \
			 +-----------+     +----------+     +----------+
			 |   3,4,5   |     | 11,12,13 |     | 14,15,16 |
			 +-----------+     +----------+     +----------+
		*/

		// Check the root page
		assert.Equal(t, 2, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{5, 13}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{3, 4, 5}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{11, 12, 13}, pager.pages[3].LeafNode.Keys())
		assert.Equal(t, []RowID{14, 15, 16}, pager.pages[4].LeafNode.Keys())
		assert.Equal(t, 1, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete rows 11 and 12 to trigger another merge", func(t *testing.T) {
		ids := rowIDs(rows[11], rows[12])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 2, result.RowsAffected)
		checkRows(ctx, t, table, append(append([]Row(nil), rows[3:6]...), rows[13:17]...))

		/*
			Row 11 leaves page3 with 2 cells → underflow → page2 cannot lend (3 cells) → merge.
			Row 12 is then deleted from the merged page2 = [3,4,5,12,13] → [3,4,5,13].

			               +-----------+
			               |    13     |
			               +-----------+
			              /             \
			+-----------+              +-----------+
			|  3,4,5,13 |              | 14,15,16  |
			+-----------+              +-----------+
			  page 2                     page 4
		*/

		// Check the root page
		assert.Equal(t, 1, int(pager.pages[0].InternalNode.Header.KeysNum))
		assert.Equal(t, []RowID{13}, pager.pages[0].InternalNode.Keys())
		// Check the leaf pages
		assert.Equal(t, []RowID{3, 4, 5, 13}, pager.pages[2].LeafNode.Keys())
		assert.Equal(t, []RowID{14, 15, 16}, pager.pages[4].LeafNode.Keys())
		// page3 is now also a free page
		assert.NotNil(t, pager.pages[3].FreePage)
		assert.Nil(t, pager.pages[3].LeafNode)
		assert.Nil(t, pager.pages[3].InternalNode)
		assert.Equal(t, int(pager.pages[1].Index), int(pager.pages[3].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[3].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 2, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete rows 3, 4, 5 to collapse to a single root leaf", func(t *testing.T) {
		ids := rowIDs(rows[3], rows[4], rows[5])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, rows[13:17])

		/*
			Row 3 leaves page2 with 3 cells — no underflow.
			Row 4 leaves page2 with 2 cells → underflow → right neighbour page4 has 3 cells
			    (cannot lend) → merge page4 into page2 → root collapses to leaf [5,13,14,15,16].
			Row 5 is then deleted from the root leaf → [13,14,15,16].

			   +----------------+
			   | 13, 14, 15, 16 |  (root leaf)
			   +----------------+
		*/

		assert.Nil(t, pager.pages[0].InternalNode)
		assert.Equal(t, 4, int(pager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.Parent))
		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.NextLeaf))
		assert.Equal(t, []RowID{13, 14, 15, 16}, pager.pages[0].LeafNode.Keys())
		// page2 and page4 are now free (page2 freed first during merge collapse, then page4)
		assert.NotNil(t, pager.pages[2].FreePage)
		assert.Nil(t, pager.pages[2].LeafNode)
		assert.Nil(t, pager.pages[2].InternalNode)
		assert.NotNil(t, pager.pages[4].FreePage)
		assert.Nil(t, pager.pages[4].LeafNode)
		assert.Nil(t, pager.pages[4].InternalNode)
		assert.Equal(t, int(pager.pages[2].Index), int(pager.pages[4].FreePage.NextFreePage))
		assert.Equal(t, int(pager.pages[4].Index), int(pager.dbHeader.FirstFreePage))
		assert.Equal(t, 4, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete one more row, root leaf shrinks", func(t *testing.T) {
		ids := rowIDs(rows[16])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[13:16])

		/*
		   +------------+
		   | 13, 14, 15 |  (root leaf)
		   +------------+
		*/

		assert.Nil(t, pager.pages[0].InternalNode)
		assert.Equal(t, 3, int(pager.pages[0].LeafNode.Header.Cells))
		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.Parent))
		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.NextLeaf))
		assert.Equal(t, []RowID{13, 14, 15}, pager.pages[0].LeafNode.Keys())
		assert.Equal(t, 4, int(pager.dbHeader.FreePageCount))
	})

	t.Run("Delete all remaining rows", func(t *testing.T) {
		ids := rowIDs(rows[13], rows[14], rows[15])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 3, result.RowsAffected)
		checkRows(ctx, t, table, nil)

		assert.Equal(t, 0, int(pager.pages[0].LeafNode.Header.Cells))
	})

	assert.Equal(t, 5, int(pager.TotalPages()))
	// Root page cannot be recycled, so there should still be 4 free pages
	assert.NotNil(t, pager.pages[4].FreePage)
	assert.Nil(t, pager.pages[4].LeafNode)
	assert.Nil(t, pager.pages[4].InternalNode)
	assert.Nil(t, pager.pages[4].OverflowPage)
	assert.Equal(t, int(pager.pages[2].Index), int(pager.pages[4].FreePage.NextFreePage))
	assert.Equal(t, int(pager.pages[4].Index), int(pager.dbHeader.FirstFreePage))
	assert.Equal(t, 4, int(pager.dbHeader.FreePageCount))
}

func TestTable_Delete_InternalNodeRebalancing(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		numRows       = 1000
		rows          = gen.MediumRows(numRows)
		tablePager    = pager.ForTable(testMediumColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testMediumColumns, 0, nil)
	)
	// maximumICells is normally ~340; set to 5 in production code to stress-test splits.

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testMediumColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	checkRows(ctx, t, table, rows)
	assert.Equal(t, 201, int(pager.TotalPages()))

	result := mustDelete(ctx, t, table, txManager, pager, Statement{Kind: Delete})

	assert.Equal(t, len(rows), result.RowsAffected)

	checkRows(ctx, t, table, nil)

	assert.Equal(t, 201, int(pager.TotalPages()))
	assert.Equal(t, 200, int(pager.dbHeader.FreePageCount))
}

func TestTable_Delete_Overflow(t *testing.T) {
	var (
		pager, dbFile = initTest(t)
		ctx           = context.Background()
		tablePager    = pager.ForTable(testOverflowColumns)
		txManager     = NewTransactionManager(zap.NewNop(), dbFile.Name(), mockPagerFactory(tablePager), pager, nil)
		txPager       = NewTransactionalPager(tablePager, txManager, testTableName, "")
		table         = NewTable(testLogger, txPager, txManager, testTableName, testOverflowColumns, 0, nil)
		rows          = gen.OverflowRows(3, []uint32{
			MaxInlineVarchar,          // inline text
			MaxInlineVarchar + 100,    // text overflows to 1 page
			MaxOverflowPageData + 100, // text overflows to multiple pages
		})
	)

	// Batch insert test rows
	stmt := Statement{
		Kind:    Insert,
		Fields:  fieldsFromColumns(testOverflowColumns...),
		Inserts: [][]OptionalValue{},
	}
	for _, row := range rows {
		stmt.Inserts = append(stmt.Inserts, row.Values)
	}

	mustInsert(ctx, t, table, txManager, stmt)

	require.Equal(t, 4, int(pager.TotalPages()))

	t.Run("Delete inline non overflowing row", func(t *testing.T) {
		ids := rowIDs(rows[0])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 1, result.RowsAffected)
		checkRows(ctx, t, table, rows[1:])

		require.Equal(t, 4, int(pager.TotalPages()))
		assertFreePages(t, tablePager, nil)
	})

	t.Run("Delete overflowing rows", func(t *testing.T) {
		ids := rowIDs(rows[1], rows[2])

		result := mustDelete(ctx, t, table, txManager, pager, Statement{
			Kind: Delete,
			Conditions: OneOrMore{
				{
					FieldIsInAny(Field{Name: "id"}, ids...),
				},
			},
		})

		assert.Equal(t, 2, result.RowsAffected)
		checkRows(ctx, t, table, nil)

		require.Equal(t, 4, int(pager.TotalPages()))
		assertFreePages(t, tablePager, []PageIndex{3, 2, 1})
	})
}

func rowIDs(rows ...Row) []any {
	ids := make([]any, 0, len(rows))
	for _, r := range rows {
		id, ok := r.GetValue("id")
		if ok {
			ids = append(ids, id.AsInt8())
		}
	}
	return ids
}
